//! Asynchronous `tokio` I/O wrappers for serial ports.
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
//! Call [`AsyncSerialPort::split`] on a serial port to start the background task
//! and obtain the [`Reader`] and [`Writer`] halves.

use serialport::SerialPort;
use tokio::spawn;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

use self::message::Message;
pub use self::reader::Reader;
use self::worker::Worker;
pub use self::writer::Writer;

mod message;
mod reader;
mod worker;
mod writer;

/// Extension trait for splitting a serial port into asynchronous I/O halves.
pub trait AsyncSerialPort: Sized {
    /// Starts the background worker and returns the async reader, writer, and worker task.
    ///
    /// The `buffer` argument configures the capacity of the internal command
    /// channel used by the reader and writer halves. The returned task owns the
    /// serial port until all command senders are dropped, then resolves with the
    /// serial port.
    fn split(self, buffer: usize) -> (Reader, Writer, JoinHandle<Self>);
}

impl<T> AsyncSerialPort for T
where
    T: SerialPort + 'static,
{
    fn split(self, buffer: usize) -> (Reader, Writer, JoinHandle<Self>) {
        let worker = Worker::new(self);
        let (tx, rx) = channel(buffer);
        let handle = spawn(worker.run(rx));
        (Reader::new(tx.clone()), Writer::new(tx), handle)
    }
}
