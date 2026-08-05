use std::ops::{Deref, DerefMut};

use tokio::sync::mpsc::OwnedPermit;

use crate::Message;

const PORT_ALREADY_RETURNED: &str = "serial port guard no longer owns the serial port";

/// Temporary exclusive ownership of a worker's serial port.
///
/// The guard dereferences to the concrete serial-port type. Dropping it returns
/// the port to the worker so that asynchronous reads and writes can resume.
#[must_use = "dropping the guard immediately returns the serial port to the worker"]
pub struct SerialPortGuard<T> {
    serial_port: Option<T>,
    return_permit: Option<OwnedPermit<Message<T>>>,
}

impl<T> SerialPortGuard<T> {
    /// Creates a guard for a serial port removed from its worker.
    pub(crate) const fn new(serial_port: T, return_permit: OwnedPermit<Message<T>>) -> Self {
        Self {
            serial_port: Some(serial_port),
            return_permit: Some(return_permit),
        }
    }
}

impl<T> Deref for SerialPortGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.serial_port.as_ref().expect(PORT_ALREADY_RETURNED)
    }
}

impl<T> DerefMut for SerialPortGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.serial_port.as_mut().expect(PORT_ALREADY_RETURNED)
    }
}

impl<T> Drop for SerialPortGuard<T> {
    fn drop(&mut self) {
        let serial_port = self.serial_port.take().expect(PORT_ALREADY_RETURNED);
        let return_permit = self.return_permit.take().expect(PORT_ALREADY_RETURNED);
        drop(return_permit.send(Message::ReturnPort(serial_port)));
    }
}
