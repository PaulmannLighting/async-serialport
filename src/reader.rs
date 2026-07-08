use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::{Receiver, channel};

use crate::Message;
use crate::message::ReadResponse;

type SendFut = Pin<Box<dyn Future<Output = Result<(), SendError<Message>>> + Send + 'static>>;

/// Asynchronous reader half for a serial port managed by the background worker.
pub struct Reader {
    sender: Sender<Message>,
    sending: Option<SendFut>,
    receiver: Option<Receiver<io::Result<ReadResponse>>>,
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

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let reader = self.as_mut().get_mut();

        loop {
            if !reader.buffered.is_empty() || buf.remaining() == 0 {
                let bytes_to_copy = reader.buffered.len().min(buf.remaining());
                let bytes = reader.buffered.split_to(bytes_to_copy);
                buf.put_slice(&bytes);
                return Poll::Ready(Ok(()));
            }

            if let Some(mut sending) = reader.sending.take() {
                match sending.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        if result.is_err() {
                            reader.receiver.take();
                            return Poll::Ready(Err(ErrorKind::BrokenPipe.into()));
                        }
                    }
                    Poll::Pending => {
                        reader.sending.replace(sending);
                        return Poll::Pending;
                    }
                }
            }

            if let Some(mut receiver) = reader.receiver.take() {
                match Pin::new(&mut receiver).poll(cx) {
                    Poll::Ready(result) => {
                        let Ok(result) = result else {
                            return Poll::Ready(Err(ErrorKind::BrokenPipe.into()));
                        };

                        match result {
                            Ok(ReadResponse::Data(bytes)) => {
                                reader.buffered = bytes;
                            }
                            Ok(ReadResponse::RetryLater) => {}
                            Err(error) => {
                                return Poll::Ready(Err(error));
                            }
                        }
                    }
                    Poll::Pending => {
                        reader.receiver.replace(receiver);
                        return Poll::Pending;
                    }
                }

                continue;
            }

            let (tx, rx) = channel();
            let sender = reader.sender.clone();
            let fut = async move { sender.send(Message::Read(tx)).await };
            reader.sending.replace(Box::pin(fut));
            reader.receiver.replace(rx);
        }
    }
}
