use std::io::Result;

use bytes::Bytes;
use tokio::sync::oneshot::Sender;

/// Result of a worker read attempt.
pub enum ReadResponse {
    /// Bytes were read from the serial port.
    Data(Bytes),
    /// No bytes were available; the reader should retry the read later.
    RetryLater,
}

/// Internal command sent from async I/O halves to the serial-port worker.
#[expect(variant_size_differences)]
pub enum Message {
    /// Requests a read from the serial port.
    Read(Sender<Result<ReadResponse>>),
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
