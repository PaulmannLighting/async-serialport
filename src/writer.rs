use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::{Receiver, channel};

use crate::Message;

type SendFut = Pin<Box<dyn Future<Output = Result<(), SendError<Message>>> + Send + 'static>>;

enum Operation {
    Flush,
    Write { bytes: usize },
}

struct PendingOperation {
    sending: Option<SendFut>,
    receiver: Receiver<io::Result<()>>,
    operation: Operation,
}

/// Asynchronous writer half for a serial port managed by the background worker.
pub struct Writer {
    sender: Sender<Message>,
    pending: Option<PendingOperation>,
}

impl Writer {
    /// Creates a writer half backed by the worker command channel.
    pub(crate) const fn new(sender: Sender<Message>) -> Self {
        Self {
            sender,
            pending: None,
        }
    }
}

impl Writer {
    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Operation>> {
        let Some(mut pending) = self.pending.take() else {
            return Poll::Ready(Err(ErrorKind::InvalidInput.into()));
        };

        if let Some(mut sending) = pending.sending.take() {
            match sending.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    if result.is_err() {
                        return Poll::Ready(Err(ErrorKind::BrokenPipe.into()));
                    }
                }
                Poll::Pending => {
                    pending.sending.replace(sending);
                    self.pending.replace(pending);
                    return Poll::Pending;
                }
            }
        }

        match Pin::new(&mut pending.receiver).poll(cx) {
            Poll::Ready(result) => {
                let Ok(result) = result else {
                    return Poll::Ready(Err(ErrorKind::BrokenPipe.into()));
                };

                match result {
                    Ok(()) => Poll::Ready(Ok(pending.operation)),
                    Err(error) => Poll::Ready(Err(error)),
                }
            }
            Poll::Pending => {
                self.pending.replace(pending);
                Poll::Pending
            }
        }
    }

    fn start_flush(&mut self) {
        let (tx, rx) = channel();
        let sender = self.sender.clone();
        let fut = async move { sender.send(Message::Flush(tx)).await };

        self.pending.replace(PendingOperation {
            sending: Some(Box::pin(fut)),
            receiver: rx,
            operation: Operation::Flush,
        });
    }

    fn start_write(&mut self, buf: &[u8]) {
        let bytes = Bytes::copy_from_slice(buf);
        let bytes_written = bytes.len();
        let (tx, rx) = channel();
        let sender = self.sender.clone();
        let fut = async move {
            sender
                .send(Message::Write {
                    bytes,
                    response: tx,
                })
                .await
        };

        self.pending.replace(PendingOperation {
            sending: Some(Box::pin(fut)),
            receiver: rx,
            operation: Operation::Write {
                bytes: bytes_written,
            },
        });
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let writer = self.as_mut().get_mut();

        loop {
            if writer.pending.is_some() {
                match writer.poll_pending(cx) {
                    Poll::Ready(Ok(Operation::Write { bytes })) => return Poll::Ready(Ok(bytes)),
                    Poll::Ready(Ok(Operation::Flush)) => {}
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if buf.is_empty() {
                return Poll::Ready(Ok(buf.len()));
            }

            writer.start_write(buf);
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let writer = self.as_mut().get_mut();

        loop {
            if writer.pending.is_some() {
                match writer.poll_pending(cx) {
                    Poll::Ready(Ok(Operation::Write { .. })) => {}
                    Poll::Ready(Ok(Operation::Flush)) => return Poll::Ready(Ok(())),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            writer.start_flush();
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
