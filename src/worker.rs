use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use serialport::SerialPort;
use tokio::sync::mpsc::Receiver;

use crate::Message;

const POLLED_AFTER_COMPLETION: &str = "worker polled after completion";

/// Future that owns the blocking serial port and services async I/O requests.
///
/// The future resolves with the owned serial port once all [`crate::Reader`]
/// and [`crate::Writer`] handles have been dropped and the worker command
/// channel closes.
pub struct Worker<T> {
    serial_port: Option<T>,
    inbox: Receiver<Message>,
}

impl<T> Worker<T> {
    /// Creates a worker that owns the provided serial port.
    #[must_use]
    pub(crate) const fn new(serial_port: T, inbox: Receiver<Message>) -> Self {
        Self {
            serial_port: Some(serial_port),
            inbox,
        }
    }

    const fn finish(&mut self) -> T {
        self.serial_port.take().expect(POLLED_AFTER_COMPLETION)
    }

    const fn serial_port_mut(&mut self) -> &mut T {
        self.serial_port.as_mut().expect(POLLED_AFTER_COMPLETION)
    }
}

impl<T> Worker<T>
where
    T: SerialPort,
{
    fn read_available(&mut self, mut bytes: BytesMut) -> io::Result<Bytes> {
        let available = self.serial_port_mut().bytes_to_read()?;
        let available = usize::try_from(available).unwrap_or(usize::MAX);
        let bytes_to_read = bytes.len().min(available);
        bytes.truncate(bytes_to_read);

        if bytes.is_empty() {
            return Ok(bytes.freeze());
        }

        self.serial_port_mut()
            .read_exact(bytes.as_mut())
            .map(|()| bytes.freeze())
    }

    fn process_message(&mut self, message: Message) {
        match message {
            Message::Read { bytes, response } => {
                response
                    .send(self.read_available(bytes))
                    .unwrap_or_else(drop);
            }
            Message::Write { bytes, response } => {
                let result = self.serial_port_mut().write_all(&bytes);
                response.send(result).unwrap_or_else(drop);
            }
            Message::Flush(response) => {
                let result = self.serial_port_mut().flush();
                response.send(result).unwrap_or_else(drop);
            }
        }
    }
}

impl<T> Future for Worker<T>
where
    T: SerialPort,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let worker = self.as_mut().get_mut();

        loop {
            match worker.inbox.poll_recv(cx) {
                Poll::Ready(Some(message)) => worker.process_message(message),
                Poll::Ready(None) => return Poll::Ready(worker.finish()),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T> Unpin for Worker<T> {}
