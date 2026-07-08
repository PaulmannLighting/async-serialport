//! Asynchronous I/O wrappers for serial ports.
//!
//! The crate exposes [`AsyncSerialPort`], an extension trait that splits a
//! serial port into asynchronous [`Reader`] and [`Writer`] halves. The halves
//! communicate with an internal worker over channels. The worker owns the
//! blocking serial port and performs reads, writes, and flushes on behalf of the
//! async halves.
//!
//! This keeps blocking serial-port operations out of async tasks while still
//! presenting the standard [`tokio::io::AsyncRead`] and
//! [`tokio::io::AsyncWrite`] traits to callers.
//!
//! Call [`AsyncSerialPort::split`] on a serial port to obtain the [`Reader`],
//! [`Writer`], and worker future. Spawn the worker future on the async runtime
//! of your choice.

use std::pin::Pin;

use serialport::SerialPort;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::SendError;

use self::message::Message;
pub use self::reader::Reader;
use self::worker::Worker;
pub use self::writer::Writer;

mod message;
mod reader;
mod worker;
mod writer;

type SendFut =
    Pin<Box<dyn Future<Output = Result<(), SendError<Message>>> + Send + Sync + 'static>>;

/// Future that drives the blocking serial-port worker.
///
/// The future resolves with the owned serial port once all [`Reader`] and
/// [`Writer`] handles have been dropped and the worker command channel closes.
pub type WorkerFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Extension trait for splitting a serial port into asynchronous I/O halves.
pub trait AsyncSerialPort: Sized {
    /// Creates async reader and writer halves plus the worker future.
    ///
    /// The `buffer` argument configures the capacity of the internal command
    /// channel used by the reader and writer halves. The returned worker future
    /// owns the serial port until all command senders are dropped, then resolves
    /// with the serial port.
    fn split(self, buffer: usize) -> (Reader, Writer, WorkerFuture<Self>);
}

impl<T> AsyncSerialPort for T
where
    T: SerialPort + 'static,
{
    fn split(self, buffer: usize) -> (Reader, Writer, WorkerFuture<Self>) {
        let worker = Worker::new(self);
        let (tx, rx) = channel(buffer);
        let worker = Box::pin(worker.run(rx));
        (Reader::new(tx.clone()), Writer::new(tx), worker)
    }
}
