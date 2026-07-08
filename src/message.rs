use std::io::Result;

use bytes::Bytes;
use tokio::sync::oneshot::Sender;

/// Internal command sent from async I/O halves to the serial-port worker.
pub enum Message {
    /// Requests a read from the serial port.
    Read(Sender<Result<Bytes>>),
    /// Requests that the worker writes the provided bytes to the serial port.
    Write {
        /// Bytes to write.
        bytes: Bytes,
        /// Channel used to return the write result.
        response: Sender<Result<()>>,
    },
    /// Requests that the worker flushes the serial port.
    Flush(Sender<Result<()>>),
}
