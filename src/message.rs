use std::io::{ErrorKind, Result};
use std::pin::Pin;

use bytes::Bytes;
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot::Sender;

use crate::SerialPortGuard;

/// A read result, or `None` when taking the port invalidated the in-flight read.
pub type ReadResponse = Result<Option<Bytes>>;

/// A pending send waiting in the worker command channel's capacity queue.
pub type PendingSend = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Internal command sent from async I/O halves to the serial-port worker.
pub enum Message<T> {
    /// Requests a read from the serial port.
    Read {
        /// Maximum number of bytes to read.
        capacity: usize,
        /// Channel used to return the read result.
        response: Sender<ReadResponse>,
    },

    /// Requests that the worker writes the provided bytes to the serial port.
    Write {
        /// Bytes to write.
        bytes: Bytes,
        /// Channel used to return the write result.
        response: Sender<Result<()>>,
    },

    /// Requests that the worker flushes the serial port.
    Flush(Sender<Result<()>>),

    /// Requests temporary ownership of the serial port.
    TakePort {
        /// Channel used to return the serial-port guard.
        response: Sender<Result<SerialPortGuard<T>>>,
        /// Command sender used to reserve space for returning the port.
        worker_sender: MpscSender<Self>,
    },

    /// Returns a temporarily owned serial port to the worker.
    ReturnPort(T),
}

/// Sends a command immediately or returns a future that waits for channel capacity.
pub fn send_command<T>(
    sender: &MpscSender<Message<T>>,
    message: Message<T>,
) -> Result<Option<PendingSend>>
where
    T: Send + 'static,
{
    match sender.try_send(message) {
        Ok(()) => Ok(None),
        Err(TrySendError::Full(message)) => {
            let sender = sender.clone();
            Ok(Some(Box::pin(async move {
                sender
                    .send(message)
                    .await
                    .map_err(|_| ErrorKind::BrokenPipe.into())
            })))
        }
        Err(TrySendError::Closed(_)) => Err(ErrorKind::BrokenPipe.into()),
    }
}
