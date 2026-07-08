use std::io;

use bytes::{Bytes, BytesMut};
use serialport::SerialPort;
use tokio::sync::mpsc::Receiver;

use crate::Message;

/// Background worker that owns the blocking serial port.
#[derive(Debug)]
pub struct Worker<T> {
    serial_port: T,
}

impl<T> Worker<T> {
    /// Creates a worker that owns the provided serial port.
    #[must_use]
    pub const fn new(serial_port: T) -> Self {
        Self { serial_port }
    }
}

impl<T> Worker<T>
where
    T: SerialPort,
{
    fn read_available(&mut self, mut bytes: BytesMut) -> io::Result<Bytes> {
        let available = self.serial_port.bytes_to_read()?;
        let available = usize::try_from(available).unwrap_or(usize::MAX);
        let bytes_to_read = bytes.len().min(available);
        bytes.truncate(bytes_to_read);

        if bytes.is_empty() {
            return Ok(bytes.freeze());
        }

        self.serial_port
            .read_exact(bytes.as_mut())
            .map(|()| bytes.freeze())
    }

    /// Processes read, write, and flush commands until all senders are dropped.
    pub async fn run(mut self, mut inbox: Receiver<Message>) -> T {
        while let Some(message) = inbox.recv().await {
            match message {
                Message::Read { bytes, response } => {
                    response
                        .send(self.read_available(bytes))
                        .unwrap_or_else(drop);
                }
                Message::Write { bytes, response } => {
                    response
                        .send(self.serial_port.write_all(&bytes))
                        .unwrap_or_else(drop);
                }
                Message::Flush(response) => {
                    response.send(self.serial_port.flush()).unwrap_or_else(drop);
                }
            }
        }

        self.serial_port
    }
}
