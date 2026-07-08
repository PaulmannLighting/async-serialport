//! Asynchronous `tokio` I/O wrappers for serial ports.
//!
//! The crate exposes [`Reader`] and [`Writer`] halves that communicate with a
//! [`Worker`] over channels. The worker owns the blocking serial port and
//! performs reads, writes, and flushes on behalf of the async halves.
//!
//! This keeps blocking serial-port operations out of async tasks while still
//! presenting the standard [`tokio::io::AsyncRead`] and
//! [`tokio::io::AsyncWrite`] traits to callers.
//!
//! Construct a [`Worker`] with [`Worker::new`], then call [`Worker::split`] to
//! start the background task and obtain the [`Reader`] and [`Writer`] halves.

use self::message::Message;
pub use self::reader::Reader;
pub use self::worker::Worker;
pub use self::writer::Writer;

mod message;
mod reader;
mod worker;
mod writer;
