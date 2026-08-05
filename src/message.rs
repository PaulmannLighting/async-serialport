use std::io::Result;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::oneshot::Sender;

use crate::SerialPortGuard;

/// Internal command sent from async I/O halves to the serial-port worker.
pub enum Message<T> {
    /// Requests a read from the serial port.
    Read {
        /// Bytes to read into.
        bytes: BytesMut,
        /// Channel used to return the read result.
        response: Sender<Result<Bytes>>,
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
