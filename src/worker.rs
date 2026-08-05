use std::future::Future;
use std::io::{self, ErrorKind, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use serialport::SerialPort;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{OwnedPermit, Receiver, Sender};
use tokio::sync::oneshot;

use crate::{Message, SerialPortGuard};

const POLLED_AFTER_COMPLETION: &str = "worker polled after completion";
const SERIAL_PORT_ALREADY_PRESENT: &str = "worker received a duplicate serial port";
const SERIAL_PORT_UNAVAILABLE: &str = "serial port is temporarily removed from the worker";

type ReserveReturnSlot<T> =
    Pin<Box<dyn Future<Output = Result<OwnedPermit<Message<T>>, SendError<()>>> + Send + 'static>>;

struct PendingTake<T> {
    response: oneshot::Sender<io::Result<SerialPortGuard<T>>>,
    reservation: ReserveReturnSlot<T>,
}

/// Future that owns the blocking serial port and services async I/O requests.
///
/// The future resolves with the owned serial port once all [`crate::Reader`]
/// and [`crate::Writer`] handles have been dropped and the worker command
/// channel closes.
pub struct Worker<T> {
    serial_port: Option<T>,
    inbox: Receiver<Message<T>>,
    pending_take: Option<PendingTake<T>>,
    pending_message: Option<Message<T>>,
}

impl<T> Worker<T> {
    /// Creates a worker that owns the provided serial port.
    #[must_use]
    pub(crate) const fn new(serial_port: T, inbox: Receiver<Message<T>>) -> Self {
        Self {
            serial_port: Some(serial_port),
            inbox,
            pending_take: None,
            pending_message: None,
        }
    }

    const fn finish(&mut self) -> T {
        self.serial_port.take().expect(POLLED_AFTER_COMPLETION)
    }

    fn unavailable_error() -> io::Error {
        io::Error::new(ErrorKind::NotConnected, SERIAL_PORT_UNAVAILABLE)
    }
}

impl<T> Worker<T>
where
    T: SerialPort + 'static,
{
    fn read_available(serial_port: &mut T, mut bytes: BytesMut) -> io::Result<Bytes> {
        let available = serial_port.bytes_to_read()?;
        let available = usize::try_from(available).unwrap_or(usize::MAX);
        let bytes_to_read = bytes.len().min(available);
        bytes.truncate(bytes_to_read);

        if bytes.is_empty() {
            return Ok(bytes.freeze());
        }

        serial_port
            .read_exact(bytes.as_mut())
            .map(|()| bytes.freeze())
    }

    fn process_message(&mut self, message: Message<T>) {
        match message {
            Message::Read { bytes, response } => {
                let result = self.serial_port.as_mut().map_or_else(
                    || Err(Self::unavailable_error()),
                    |port| Self::read_available(port, bytes),
                );
                response.send(result).unwrap_or_else(drop);
            }
            Message::Write { bytes, response } => {
                let result = self.serial_port.as_mut().map_or_else(
                    || Err(Self::unavailable_error()),
                    |port| port.write_all(&bytes),
                );
                response.send(result).unwrap_or_else(drop);
            }
            Message::Flush(response) => {
                let result = self
                    .serial_port
                    .as_mut()
                    .map_or_else(|| Err(Self::unavailable_error()), Write::flush);
                response.send(result).unwrap_or_else(drop);
            }
            Message::TakePort {
                response,
                worker_sender,
            } => self.start_take_serial_port(response, worker_sender),
            Message::ReturnPort(serial_port) => self.return_serial_port(serial_port),
        }
    }

    fn start_take_serial_port(
        &mut self,
        response: oneshot::Sender<io::Result<SerialPortGuard<T>>>,
        worker_sender: Sender<Message<T>>,
    ) {
        if self.serial_port.is_none() || self.pending_take.is_some() {
            response
                .send(Err(Self::unavailable_error()))
                .unwrap_or_else(drop);
            return;
        }

        self.pending_take.replace(PendingTake {
            response,
            reservation: Box::pin(worker_sender.reserve_owned()),
        });
    }

    fn return_serial_port(&mut self, serial_port: T) {
        assert!(self.serial_port.is_none(), "{SERIAL_PORT_ALREADY_PRESENT}");
        self.serial_port.replace(serial_port);
    }

    fn poll_pending_take(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(mut pending_take) = self.pending_take.take() else {
            return Poll::Ready(());
        };

        match pending_take.reservation.as_mut().poll(cx) {
            Poll::Ready(Ok(return_permit)) => {
                let serial_port = self.serial_port.take().expect(POLLED_AFTER_COMPLETION);
                let guard = SerialPortGuard::new(serial_port, return_permit);
                pending_take.response.send(Ok(guard)).unwrap_or_else(drop);
                Poll::Ready(())
            }
            Poll::Ready(Err(_)) => {
                pending_take
                    .response
                    .send(Err(ErrorKind::BrokenPipe.into()))
                    .unwrap_or_else(drop);
                Poll::Ready(())
            }
            Poll::Pending => {
                self.pending_take.replace(pending_take);
                Poll::Pending
            }
        }
    }
}

impl<T> Future for Worker<T>
where
    T: SerialPort + 'static,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let worker = self.as_mut().get_mut();

        loop {
            if worker.pending_take.is_some() {
                match worker.poll_pending_take(cx) {
                    Poll::Ready(()) => {}
                    Poll::Pending if worker.pending_message.is_some() => return Poll::Pending,
                    Poll::Pending => match worker.inbox.poll_recv(cx) {
                        Poll::Ready(Some(message)) => {
                            worker.pending_message.replace(message);
                            continue;
                        }
                        Poll::Ready(None) => return Poll::Ready(worker.finish()),
                        Poll::Pending => return Poll::Pending,
                    },
                }
            }

            if let Some(message) = worker.pending_message.take() {
                worker.process_message(message);
                continue;
            }

            match worker.inbox.poll_recv(cx) {
                Poll::Ready(Some(message)) => worker.process_message(message),
                Poll::Ready(None) => return Poll::Ready(worker.finish()),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T> Unpin for Worker<T> {}
