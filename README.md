# async-serialport

`async-serialport` provides asynchronous Tokio I/O halves for serial ports.

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
types. Call `AsyncSerialPort::split` on a serial port to start the background
task and receive the async I/O halves.

The worker protocol is internal. Callers should interact with the async halves
through Tokio's `AsyncRead` and `AsyncWrite` extension traits.

```rust
use async_serialport::AsyncSerialPort;

const BAUD_RATE: u32 = 115_200;
const COMMAND_BUFFER: usize = 16;

let serial_port = serialport::new("/dev/ttyUSB0", BAUD_RATE).open()?;
let (reader, writer, worker_task) = serial_port.split(COMMAND_BUFFER);
```

## Runtime

This crate targets Tokio. The worker communicates with the async halves through
Tokio channels, so applications need to run the halves inside a Tokio runtime.

## Error Handling

Serial-port errors are returned as `std::io::Error` values through the async I/O
traits. If the worker channel closes before a request completes, the async half
returns `std::io::ErrorKind::BrokenPipe`.

When all async halves are dropped, the worker task finishes and returns the
owned serial port.
