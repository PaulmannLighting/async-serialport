use std::any::Any;
use std::io::{self, ErrorKind};
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::mpsc::{Sender, channel};
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use filedescriptor::OwnedHandle;
#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
#[cfg(unix)]
use serialport::TTYPort;
use tokio::sync::oneshot;

const RETRY_INTERVAL: Duration = Duration::from_millis(5);

/// Wakes the worker when the port is readable or a fallback retry is due.
pub struct ReadWait {
    requests: Option<Sender<oneshot::Sender<io::Result<()>>>>,
    #[cfg(unix)]
    native: Option<(UnixStream, thread::JoinHandle<()>)>,
}

impl ReadWait {
    /// Starts a helper for the port without requiring an async runtime.
    pub(crate) fn new(port: &dyn Any) -> io::Result<Self> {
        #[cfg(unix)]
        if let Some(port) = port.downcast_ref::<TTYPort>() {
            return Self::native(port);
        }

        #[cfg(not(unix))]
        let _ = port;
        let (requests, receiver) = channel::<oneshot::Sender<io::Result<()>>>();
        thread::Builder::new()
            .name("serial-read-timer".into())
            .spawn(move || {
                for response in receiver {
                    thread::sleep(RETRY_INTERVAL);
                    let _ = response.send(Ok(()));
                }
            })?;

        Ok(Self {
            requests: Some(requests),
            #[cfg(unix)]
            native: None,
        })
    }

    /// Arms one wait. The worker polls its response alongside the command channel.
    pub(crate) fn arm(&self) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let (response, ready) = oneshot::channel();
        self.requests
            .as_ref()
            .expect("read wait has a request sender until dropped")
            .send(response)
            .map_err(|_| io::Error::from(ErrorKind::BrokenPipe))?;
        Ok(ready)
    }

    #[cfg(unix)]
    fn native(port: &TTYPort) -> io::Result<Self> {
        let serial_fd = OwnedHandle::dup(port).map_err(io::Error::other)?;
        let (requests, receiver) = channel::<oneshot::Sender<io::Result<()>>>();
        let (stop, stopped) = UnixStream::pair()?;
        let thread = thread::Builder::new()
            .name("serial-read-ready".into())
            .spawn(move || {
                for response in receiver {
                    let result = wait_readable(serial_fd.as_fd(), &stopped);
                    let _ = response.send(result);
                }
            })?;

        Ok(Self {
            requests: Some(requests),
            native: Some((stop, thread)),
        })
    }
}

impl Drop for ReadWait {
    fn drop(&mut self) {
        drop(self.requests.take());
        #[cfg(unix)]
        if let Some((stop, thread)) = self.native.take() {
            // Close the control socket to interrupt poll before the port can move or close.
            drop(stop);
            let _ = thread.join();
        }
    }
}

#[cfg(unix)]
fn wait_readable(serial_fd: BorrowedFd<'_>, stop: &UnixStream) -> io::Result<()> {
    let mut descriptors = [
        PollFd::new(serial_fd, PollFlags::POLLIN),
        PollFd::new(stop.as_fd(), PollFlags::POLLIN),
    ];

    loop {
        match poll(&mut descriptors, PollTimeout::NONE) {
            Ok(_) => break,
            Err(Errno::EINTR) => (),
            Err(error) => return Err(io::Error::from(error)),
        }
    }

    if descriptors[1].any() == Some(true) {
        return Ok(());
    }

    let events = descriptors[0].revents().unwrap_or_else(PollFlags::empty);
    if events.intersects(PollFlags::POLLHUP | PollFlags::POLLNVAL) {
        Err(ErrorKind::BrokenPipe.into())
    } else if events.contains(PollFlags::POLLIN) && !events.contains(PollFlags::POLLERR) {
        Ok(())
    } else {
        Err(io::Error::other("serial port reported a polling error"))
    }
}
