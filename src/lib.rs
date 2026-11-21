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
//! [`Writer`], and [`Worker`] future. Spawn the worker on the async runtime of
//! your choice. Either half can temporarily take exclusive ownership of the
//! concrete serial port through [`Reader::take_serial_port`] or
//! [`Writer::take_serial_port`]. The resulting [`SerialPortGuard`] returns the
//! port to the worker when dropped.

use std::pin::Pin;

use serialport::SerialPort;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::SendError;

use self::message::Message;
pub use self::port_guard::SerialPortGuard;
pub use self::reader::Reader;
pub use self::worker::Worker;
pub use self::writer::Writer;

mod message;
mod port_guard;
mod read_wait;
mod reader;
mod worker;
mod writer;

const RETURN_SLOT_CAPACITY: usize = 1;
const CHANNEL_CAPACITY_OVERFLOW: &str = "command channel capacity exceeds usize";

type SendFut<T> = Pin<Box<dyn Future<Output = Result<(), SendError<Message<T>>>> + Send + 'static>>;

/// Extension trait for splitting a serial port into asynchronous I/O halves.
pub trait AsyncSerialPort
where
    Self: Sized,
{
    /// Creates async reader and writer halves plus the worker.
    ///
    /// The `buffer` argument configures the capacity available for ordinary
    /// commands on the internal bounded channel. The channel has one additional
    /// slot that a [`SerialPortGuard`] reserves for returning the port. The
    /// returned [`Worker`] owns the serial port until all command senders are
    /// dropped, then resolves with the serial port.
    fn split(self, buffer: usize) -> (Reader<Self>, Writer<Self>, Worker<Self>);
}

impl<T> AsyncSerialPort for T
where
    T: SerialPort + 'static,
{
    fn split(self, buffer: usize) -> (Reader<Self>, Writer<Self>, Worker<Self>) {
        assert_ne!(buffer, 0, "command channel capacity must be non-zero");

        let channel_capacity = buffer
            .checked_add(RETURN_SLOT_CAPACITY)
            .expect(CHANNEL_CAPACITY_OVERFLOW);
        let (tx, rx) = channel(channel_capacity);
        let worker = Worker::new(self, rx);

        (Reader::new(tx.clone()), Writer::new(tx), worker)
    }
}
