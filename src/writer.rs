use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use serialport::SerialPort;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::{Receiver, channel};

use crate::message::{PendingSend, send_command};
use crate::{Message, SerialPortGuard};

#[derive(Debug)]
enum Operation {
    Flush,

    Write { bytes: usize },
}

struct PendingOperation {
    sending: Option<PendingSend>,
    receiver: Receiver<io::Result<()>>,
    operation: Operation,
}

/// Asynchronous writer half for a serial port managed by the background worker.
pub struct Writer<T> {
    sender: Sender<Message<T>>,
    pending: Option<PendingOperation>,
}

impl<T> Writer<T>
where
    T: SerialPort + 'static,
{
    /// Creates a writer half backed by the worker command channel.
    pub(crate) const fn new(sender: Sender<Message<T>>) -> Self {
        Self {
            sender,
            pending: None,
        }
    }

    /// Temporarily removes the serial port from the worker.
    ///
    /// While the returned guard exists, asynchronous reads and writes return
    /// [`io::ErrorKind::NotConnected`]. Dropping the guard returns the serial
    /// port to the worker and allows asynchronous I/O to resume.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotConnected`] if another guard currently owns
    /// the port, or [`io::ErrorKind::BrokenPipe`] if the worker has stopped.
    pub async fn take_serial_port(&mut self) -> io::Result<SerialPortGuard<T>> {
        let (response_tx, response_rx) = channel();

        self.sender
            .send(Message::TakePort {
                response: response_tx,
                worker_sender: self.sender.clone(),
            })
            .await
            .map_err(|_| io::Error::from(ErrorKind::BrokenPipe))?;

        response_rx
            .await
            .map_err(|_| io::Error::from(ErrorKind::BrokenPipe))?
    }
}

impl<T> Debug for Writer<T>
where
    T: SerialPort,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let pending_operation = self.pending.as_ref().map(|pending| &pending.operation);

        f.debug_struct("Writer")
            .field("sender", &self.sender)
            .field("pending_operation", &pending_operation)
            .field(
                "is_sending",
                &self
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.sending.is_some()),
            )
            .finish()
    }
}

impl<T> Writer<T>
where
    T: SerialPort + 'static,
{
    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Operation>> {
        let Some(mut pending) = self.pending.take() else {
            return Poll::Ready(Err(ErrorKind::InvalidInput.into()));
        };

        if let Some(mut sending) = pending.sending.take() {
            match sending.as_mut().poll(cx) {
                Poll::Ready(result) => result?,
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

    fn start_flush(&mut self) -> io::Result<()> {
        let (tx, rx) = channel();

        self.pending.replace(PendingOperation {
            sending: send_command(&self.sender, Message::Flush(tx))?,
            receiver: rx,
            operation: Operation::Flush,
        });
        Ok(())
    }

    fn start_write(&mut self, buf: &[u8]) -> io::Result<()> {
        let bytes = Bytes::copy_from_slice(buf);
        let bytes_written = bytes.len();
        let (tx, rx) = channel();

        self.pending.replace(PendingOperation {
            sending: send_command(
                &self.sender,
                Message::Write {
                    bytes,
                    response: tx,
                },
            )?,
            receiver: rx,
            operation: Operation::Write {
                bytes: bytes_written,
            },
        });
        Ok(())
    }
}

impl<T> AsyncWrite for Writer<T>
where
    T: SerialPort + 'static,
{
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

            writer.start_write(buf)?;
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

            writer.start_flush()?;
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
