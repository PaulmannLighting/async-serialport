use std::future::Future;
use std::io::{self, ErrorKind, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use serialport::SerialPort;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{OwnedPermit, Receiver, Sender};
use tokio::sync::oneshot;

use crate::message::ReadResponse;
use crate::read_wait::ReadWait;
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

struct PendingRead {
    capacity: usize,
    response: oneshot::Sender<ReadResponse>,
    ready: Option<oneshot::Receiver<io::Result<()>>>,
}

/// Future that owns the blocking serial port and services async I/O requests.
///
/// The future resolves with the owned serial port once all [`crate::Reader`]
/// and [`crate::Writer`] handles have been dropped and the worker command
/// channel closes.
pub struct Worker<T> {
    // Drop the readiness helper before closing its serial file descriptor.
    read_wait: Option<ReadWait>,
    serial_port: Option<T>,
    inbox: Receiver<Message<T>>,
    pending_read: Option<PendingRead>,
    pending_take: Option<PendingTake<T>>,
    pending_message: Option<Message<T>>,
}

impl<T> Worker<T> {
    /// Creates a worker that owns the provided serial port.
    #[must_use]
    pub(crate) const fn new(serial_port: T, inbox: Receiver<Message<T>>) -> Self {
        Self {
            read_wait: None,
            serial_port: Some(serial_port),
            inbox,
            pending_read: None,
            pending_take: None,
            pending_message: None,
        }
    }

    fn finish(&mut self) -> T {
        self.pending_read.take();
        self.read_wait.take();
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
    fn read_available(serial_port: &mut T, capacity: usize) -> io::Result<Option<Bytes>> {
        let available = serial_port.bytes_to_read()?;
        let available = usize::try_from(available).unwrap_or(usize::MAX);
        let bytes_to_read = capacity.min(available);

        if bytes_to_read == 0 {
            return Ok(None);
        }

        let mut bytes = BytesMut::zeroed(bytes_to_read);
        match serial_port.read(&mut bytes) {
            Ok(0) => Ok(None),
            Ok(read) => {
                bytes.truncate(read);
                Ok(Some(bytes.freeze()))
            }
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Interrupted | ErrorKind::TimedOut | ErrorKind::WouldBlock
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn check_read(&mut self, mut read: PendingRead) {
        let result = self.serial_port.as_mut().map_or_else(
            || Err(Self::unavailable_error()),
            |port| Self::read_available(port, read.capacity),
        );
        if matches!(result, Ok(None)) {
            read.ready = None;
            self.pending_read = Some(read);
        } else {
            read.response.send(result).unwrap_or_else(drop);
        }
    }

    fn arm_read_wait(&mut self) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let port = self
            .serial_port
            .as_ref()
            .ok_or_else(Self::unavailable_error)?;
        if self.read_wait.is_none() {
            self.read_wait = Some(ReadWait::new(port)?);
        }
        self.read_wait
            .as_ref()
            .expect("read waiter is initialized")
            .arm()
    }

    fn poll_pending_read(&mut self, cx: &mut Context<'_>) {
        while let Some(mut read) = self.pending_read.take() {
            if read.response.poll_closed(cx).is_ready() {
                self.read_wait.take();
                return;
            }

            let mut ready = match read.ready.take().map_or_else(|| self.arm_read_wait(), Ok) {
                Ok(ready) => ready,
                Err(error) => {
                    read.response.send(Err(error)).unwrap_or_else(drop);
                    self.read_wait.take();
                    return;
                }
            };
            let readiness = Pin::new(&mut ready)
                .poll(cx)
                .map(|result| result.unwrap_or_else(|_| Err(ErrorKind::BrokenPipe.into())));
            match readiness {
                Poll::Pending => {
                    read.ready = Some(ready);
                    self.pending_read = Some(read);
                    return;
                }
                Poll::Ready(Ok(())) => self.check_read(read),
                Poll::Ready(Err(error)) => {
                    read.response.send(Err(error)).unwrap_or_else(drop);
                    self.read_wait.take();
                }
            }
        }
    }

    fn process_message(&mut self, message: Message<T>) {
        match message {
            Message::Read { capacity, response } => {
                if !response.is_closed() {
                    self.check_read(PendingRead {
                        capacity,
                        response,
                        ready: None,
                    });
                }
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
                if let Some(read) = self.pending_read.take() {
                    read.response.send(Ok(None)).unwrap_or_else(drop);
                }
                self.read_wait.take();
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
                Poll::Pending => {
                    worker.poll_pending_read(cx);
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<T> Unpin for Worker<T> {}
