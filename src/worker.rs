use bytes::BytesMut;
use serialport::SerialPort;
use tokio::spawn;
use tokio::sync::mpsc::{Receiver, channel};
use tokio::task::JoinHandle;

use crate::message::ReadResponse;
use crate::{Message, Reader, Writer};

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
    /// Processes read, write, and flush commands until all senders are dropped.
    async fn run(mut self, mut inbox: Receiver<Message>) -> T {
        while let Some(message) = inbox.recv().await {
            match message {
                Message::Read(response) => match self.serial_port.bytes_to_read() {
                    Ok(bytes) => {
                        if bytes == 0 {
                            response
                                .send(Ok(ReadResponse::RetryLater))
                                .unwrap_or_else(drop);
                            continue;
                        }

                        let mut buffer = BytesMut::zeroed(bytes as usize);
                        response
                            .send(
                                self.serial_port
                                    .read_exact(buffer.as_mut())
                                    .map(|()| ReadResponse::Data(buffer.freeze())),
                            )
                            .unwrap_or_else(drop);
                    }
                    Err(error) => {
                        response.send(Err(error.into())).unwrap_or_else(drop);
                    }
                },
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

impl<T> Worker<T>
where
    T: SerialPort + 'static,
{
    /// Splits the worker into async I/O halves and a background worker task.
    ///
    /// The `buffer` argument configures the capacity of the internal command
    /// channel used by the reader and writer halves. The returned task owns the
    /// serial port until all command senders are dropped, then resolves with the
    /// serial port.
    pub fn split(self, buffer: usize) -> (Reader, Writer, JoinHandle<T>) {
        let (tx, rx) = channel(buffer);
        let handle = spawn(self.run(rx));
        (Reader::new(tx.clone()), Writer::new(tx), handle)
    }
}
