use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::{Receiver, channel};

use crate::{Message, SendFut};

enum ReadEvent {
    NoResponse,
    RetryLater,
    Data(Bytes),
}

/// Asynchronous reader half for a serial port managed by the background worker.
pub struct Reader {
    sender: Sender<Message>,
    sending: Option<SendFut>,
    receiver: Option<Receiver<io::Result<Bytes>>>,
    buffered: Bytes,
}

impl Reader {
    /// Creates a reader half backed by the worker command channel.
    pub(crate) fn new(sender: Sender<Message>) -> Self {
        Self {
            sender,
            sending: None,
            receiver: None,
            buffered: Bytes::new(),
        }
    }

    fn copy_buffered_or_complete_empty_read(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        if self.buffered.is_empty() && buf.remaining() != 0 {
            return false;
        }

        let bytes_to_copy = self.buffered.len().min(buf.remaining());
        let bytes = self.buffered.split_to(bytes_to_copy);
        buf.put_slice(&bytes);
        true
    }

    fn poll_send_request(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(mut sending) = self.sending.take() else {
            return Poll::Ready(Ok(()));
        };

        match sending.as_mut().poll(cx) {
            Poll::Ready(result) => {
                if result.is_err() {
                    self.receiver.take();
                    Poll::Ready(Err(ErrorKind::BrokenPipe.into()))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            Poll::Pending => {
                self.sending.replace(sending);
                Poll::Pending
            }
        }
    }

    fn poll_response(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<ReadEvent>> {
        let Some(mut receiver) = self.receiver.take() else {
            return Poll::Ready(Ok(ReadEvent::NoResponse));
        };

        match Pin::new(&mut receiver).poll(cx) {
            Poll::Ready(result) => {
                let Ok(result) = result else {
                    return Poll::Ready(Err(ErrorKind::BrokenPipe.into()));
                };

                match result {
                    Ok(bytes) if bytes.is_empty() => Poll::Ready(Ok(ReadEvent::RetryLater)),
                    Ok(bytes) => Poll::Ready(Ok(ReadEvent::Data(bytes))),
                    Err(error) => Poll::Ready(Err(error)),
                }
            }
            Poll::Pending => {
                self.receiver.replace(receiver);
                Poll::Pending
            }
        }
    }

    fn start_read(&mut self, bytes_to_read: usize) {
        let (tx, rx) = channel();
        let bytes = BytesMut::zeroed(bytes_to_read);
        let sender = self.sender.clone();
        let fut = async move {
            sender
                .send(Message::Read {
                    bytes,
                    response: tx,
                })
                .await
        };

        self.sending.replace(Box::pin(fut));
        self.receiver.replace(rx);
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let reader = self.as_mut().get_mut();

        loop {
            if reader.copy_buffered_or_complete_empty_read(buf) {
                return Poll::Ready(Ok(()));
            }

            match reader.poll_send_request(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }

            match reader.poll_response(cx) {
                Poll::Ready(Ok(ReadEvent::NoResponse | ReadEvent::RetryLater)) => {
                    reader.start_read(buf.remaining());
                }
                Poll::Ready(Ok(ReadEvent::Data(bytes))) => {
                    reader.buffered = bytes;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Debug for Reader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("sender", &self.sender)
            .field("is_sending", &self.sending.is_some())
            .field("has_receiver", &self.receiver.is_some())
            .field("buffered", &self.buffered)
            .finish()
    }
}
