# async-serialport

`async-serialport` provides asynchronous I/O halves for serial ports.

The crate is built around a background worker that owns the blocking serial
port. Async-facing reader and writer halves communicate with that worker over
channels and expose Tokio's standard I/O traits:

- `AsyncSerialPort` extends serial ports with `split`.
- `Reader<T>` implements `tokio::io::AsyncRead`.
- `Writer<T>` implements `tokio::io::AsyncWrite`.
- `Worker<T>` is the future that owns and drives the blocking serial port.
- `SerialPortGuard<T>` temporarily provides mutable access to that concrete
  serial port.

This design lets async tasks use serial-port reads, writes, and flushes without
performing the blocking serial-port operations directly inside the task.

## Current API

The public API exposes the `AsyncSerialPort`, `Reader<T>`, `Writer<T>`,
`SerialPortGuard<T>`, and `Worker<T>` types. Call `AsyncSerialPort::split` on a
serial port to receive the async I/O halves and the worker future. Spawn the
worker on the async runtime of your choice.

The worker's message protocol is internal. Callers should interact with the
async halves through Tokio's `AsyncRead` and `AsyncWrite` extension traits.

```rust
use async_serialport::AsyncSerialPort;

const BAUD_RATE: u32 = 115_200;
const COMMAND_BUFFER: usize = 16;

let serial_port = serialport::new("/dev/ttyUSB0", BAUD_RATE).open()?;
let (reader, writer, worker) = serial_port.split(COMMAND_BUFFER);
```

## Idle Reads

An idle read stays pending while the worker services writes, flushes, and port
access. On Unix, a concrete `serialport::TTYPort` from `open_native()` uses OS
readability notifications and reports hangups to the reader. Boxed and custom
ports use a 5 ms retry interval because `SerialPort` exposes no readiness handle.
Data arriving just after a fallback check can wait up to one interval plus
scheduling time before it is read.

## Temporary Serial-Port Access

Either async half can temporarily remove the concrete serial port from the
worker. This is useful for serial-port-specific configuration that is not part
of `AsyncRead` or `AsyncWrite`:

```rust
use async_serialport::AsyncSerialPort;
use serialport::SerialPort;

const BAUD_RATE: u32 = 115_200;
const COMMAND_BUFFER: usize = 16;
const CONFIGURED_BAUD_RATE: u32 = 57_600;

async fn configure() -> Result<(), Box<dyn std::error::Error>> {
    let serial_port = serialport::new("/dev/ttyUSB0", BAUD_RATE).open()?;
    let (mut reader, writer, worker) = serial_port.split(COMMAND_BUFFER);
    let worker_task = tokio::spawn(worker);

    let mut serial_port = reader.take_serial_port().await?;
    serial_port.set_baud_rate(CONFIGURED_BAUD_RATE)?;
    drop(serial_port);
    drop(reader);
    drop(writer);
    worker_task.await?;
    Ok(())
}
```

`SerialPortGuard<T>` implements `Deref<Target = T>` and `DerefMut`, preserving
access to the exact concrete port type. Dropping the guard returns the port to
the worker through the same bounded command channel. A reserved return slot
keeps this drop path non-blocking even when the configured command capacity is
full.

While a guard owns the port, reads, writes, flushes, and another
`take_serial_port` call return `std::io::ErrorKind::NotConnected`. A read already
parked when the port is taken can retry after the guard returns.

## Runtime

The core crate does not require Tokio runtime features. It uses Tokio's
runtime-agnostic I/O traits and channels, and returns `Worker<T>` as a future
so callers can choose how to spawn it.

## Error Handling

Serial-port errors are returned as `std::io::Error` values through the async I/O
traits. If the worker channel closes before a request completes, the async half
returns `std::io::ErrorKind::BrokenPipe`.

When a `SerialPortGuard` temporarily owns the port, worker I/O requests return
`std::io::ErrorKind::NotConnected` until the guard is dropped.

When all async halves are dropped, the worker future finishes and returns the
owned serial port. A live guard keeps the worker open until it has returned the
port.
