# async-serialport

`async-serialport` provides asynchronous I/O halves for serial ports.

The crate is built around a background worker that owns the blocking serial
port. Async-facing reader and writer halves communicate with that worker over
channels and expose Tokio's standard I/O traits:

- `AsyncSerialPort` extends serial ports with `split`.
- `Reader` implements `tokio::io::AsyncRead`.
- `Writer` implements `tokio::io::AsyncWrite`.

This design lets async tasks use serial-port reads, writes, and flushes without
performing the blocking serial-port operations directly inside the task.

## Current API

The public API currently exposes the `AsyncSerialPort`, `Reader`, and `Writer`
types. Call `AsyncSerialPort::split` on a serial port to receive the async I/O
halves and the worker future. Spawn the worker future on the async runtime of
your choice.

The worker protocol is internal. Callers should interact with the async halves
through Tokio's `AsyncRead` and `AsyncWrite` extension traits.

```rust
use async_serialport::AsyncSerialPort;

const BAUD_RATE: u32 = 115_200;
const COMMAND_BUFFER: usize = 16;

let serial_port = serialport::new("/dev/ttyUSB0", BAUD_RATE).open()?;
let (reader, writer, worker) = serial_port.split(COMMAND_BUFFER);
```

## Runtime

The core crate does not require Tokio runtime features. It uses Tokio's
runtime-agnostic I/O traits and channels, and returns the worker as a future so
callers can choose how to spawn it.

## Error Handling

Serial-port errors are returned as `std::io::Error` values through the async I/O
traits. If the worker channel closes before a request completes, the async half
returns `std::io::ErrorKind::BrokenPipe`.

When all async halves are dropped, the worker future finishes and returns the
owned serial port.
