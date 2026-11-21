//! Integration tests for worker-backed async serial-port halves.

use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use async_serialport::AsyncSerialPort;
use bytes as _;
#[cfg(unix)]
use nix as _;
use serialport::{
    ClearBuffer, DataBits, Error, ErrorKind, FlowControl, Parity, SerialPort, StopBits,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::{Builder, Runtime};
use tokio::time::timeout;

const COMMAND_BUFFER: usize = 4;
const DEFAULT_BAUD_RATE: u32 = 115_200;
const EXPECTED_FLUSH_COUNT: usize = 1;
const MINIMUM_COMMAND_BUFFER: usize = 1;
const UPDATED_BAUD_RATE: u32 = 9_600;
const TEST_PORT_NAME: &str = "fake-serial-port";
const TEST_TIMEOUT: Duration = Duration::from_millis(100);
const TEST_DEADLINE: Duration = Duration::from_secs(5);
const IDLE_POLLS: usize = 2_000;
const FALLBACK_RETRY_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Debug)]
struct FakeSerialPort {
    read_buffer: Arc<Mutex<VecDeque<u8>>>,
    availability_checks: Arc<AtomicUsize>,
    write_buffer: Vec<u8>,
    baud_rate: u32,
    data_bits: DataBits,
    flow_control: FlowControl,
    parity: Parity,
    stop_bits: StopBits,
    timeout: Duration,
    flush_count: usize,
}

impl FakeSerialPort {
    fn new(input: &[u8]) -> Self {
        Self {
            read_buffer: Arc::new(Mutex::new(input.iter().copied().collect())),
            availability_checks: Arc::new(AtomicUsize::new(0)),
            write_buffer: Vec::new(),
            baud_rate: DEFAULT_BAUD_RATE,
            data_bits: DataBits::Eight,
            flow_control: FlowControl::None,
            parity: Parity::None,
            stop_bits: StopBits::One,
            timeout: TEST_TIMEOUT,
            flush_count: 0,
        }
    }

    fn written(&self) -> &[u8] {
        &self.write_buffer
    }

    const fn flush_count(&self) -> usize {
        self.flush_count
    }
}

impl Read for FakeSerialPort {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_buffer = self
            .read_buffer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let bytes_to_read = read_buffer.len().min(buf.len());

        for (byte, next) in buf.iter_mut().zip(read_buffer.drain(..bytes_to_read)) {
            *byte = next;
        }

        drop(read_buffer);
        Ok(bytes_to_read)
    }
}

impl Write for FakeSerialPort {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_count += 1;
        Ok(())
    }
}

impl SerialPort for FakeSerialPort {
    fn name(&self) -> Option<String> {
        Some(TEST_PORT_NAME.to_owned())
    }

    fn baud_rate(&self) -> serialport::Result<u32> {
        Ok(self.baud_rate)
    }

    fn data_bits(&self) -> serialport::Result<DataBits> {
        Ok(self.data_bits)
    }

    fn flow_control(&self) -> serialport::Result<FlowControl> {
        Ok(self.flow_control)
    }

    fn parity(&self) -> serialport::Result<Parity> {
        Ok(self.parity)
    }

    fn stop_bits(&self) -> serialport::Result<StopBits> {
        Ok(self.stop_bits)
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn set_baud_rate(&mut self, baud_rate: u32) -> serialport::Result<()> {
        self.baud_rate = baud_rate;
        Ok(())
    }

    fn set_data_bits(&mut self, data_bits: DataBits) -> serialport::Result<()> {
        self.data_bits = data_bits;
        Ok(())
    }

    fn set_flow_control(&mut self, flow_control: FlowControl) -> serialport::Result<()> {
        self.flow_control = flow_control;
        Ok(())
    }

    fn set_parity(&mut self, parity: Parity) -> serialport::Result<()> {
        self.parity = parity;
        Ok(())
    }

    fn set_stop_bits(&mut self, stop_bits: StopBits) -> serialport::Result<()> {
        self.stop_bits = stop_bits;
        Ok(())
    }

    fn set_timeout(&mut self, timeout: Duration) -> serialport::Result<()> {
        self.timeout = timeout;
        Ok(())
    }

    fn write_request_to_send(&mut self, _level: bool) -> serialport::Result<()> {
        Ok(())
    }

    fn write_data_terminal_ready(&mut self, _level: bool) -> serialport::Result<()> {
        Ok(())
    }

    fn read_clear_to_send(&mut self) -> serialport::Result<bool> {
        Ok(false)
    }

    fn read_data_set_ready(&mut self) -> serialport::Result<bool> {
        Ok(false)
    }

    fn read_ring_indicator(&mut self) -> serialport::Result<bool> {
        Ok(false)
    }

    fn read_carrier_detect(&mut self) -> serialport::Result<bool> {
        Ok(false)
    }

    fn bytes_to_read(&self) -> serialport::Result<u32> {
        self.availability_checks.fetch_add(1, Ordering::Relaxed);
        self.read_buffer
            .lock()
            .map_err(|error| Error::new(ErrorKind::Unknown, error.to_string()))?
            .len()
            .try_into()
            .map_err(|_| Error::new(ErrorKind::Unknown, "read buffer length exceeds u32"))
    }

    fn bytes_to_write(&self) -> serialport::Result<u32> {
        self.write_buffer
            .len()
            .try_into()
            .map_err(|_| Error::new(ErrorKind::Unknown, "write buffer length exceeds u32"))
    }

    fn clear(&self, _buffer_to_clear: ClearBuffer) -> serialport::Result<()> {
        Ok(())
    }

    fn try_clone(&self) -> serialport::Result<Box<dyn SerialPort>> {
        Err(Error::new(
            ErrorKind::Unknown,
            "fake serial port cannot be cloned",
        ))
    }

    fn set_break(&self) -> serialport::Result<()> {
        Ok(())
    }

    fn clear_break(&self) -> serialport::Result<()> {
        Ok(())
    }
}

fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime should build")
}

#[test]
fn idle_read_retries_only_on_timer_and_receives_fragmented_input() {
    const FIRST_FRAGMENT: &[u8] = b"first";
    const SECOND_FRAGMENT: &[u8] = b"second";
    const OUTPUT: &[u8] = b"command";

    runtime().block_on(async {
        let port = FakeSerialPort::new(&[]);
        let input = Arc::clone(&port.read_buffer);
        let checks = Arc::clone(&port.availability_checks);
        let (mut reader, mut writer, mut worker) = port.split(COMMAND_BUFFER);
        let mut buffer = [0; FIRST_FRAGMENT.len() + SECOND_FRAGMENT.len()];
        let mut context = Context::from_waker(Waker::noop());
        assert_eq!(reader.read(&mut []).await.unwrap(), 0);
        assert_eq!(checks.load(Ordering::Relaxed), 0);

        timeout(TEST_DEADLINE, async {
            let mut read = Box::pin(reader.read(&mut buffer));
            let started = Instant::now();
            for _ in 0..IDLE_POLLS {
                assert!(read.as_mut().poll(&mut context).is_pending());
                assert!(Pin::new(&mut worker).poll(&mut context).is_pending());
            }
            let attempts = checks.load(Ordering::Relaxed) as u128;
            let elapsed_intervals =
                started.elapsed().as_nanos() / FALLBACK_RETRY_INTERVAL.as_nanos();
            assert!(
                (1..=1 + elapsed_intervals).contains(&attempts),
                "{attempts} availability checks across {elapsed_intervals} retry intervals"
            );

            let task = tokio::spawn(worker);
            writer.write_all(OUTPUT).await.unwrap();
            writer.flush().await.unwrap();
            assert!(read.as_mut().poll(&mut context).is_pending());

            input.lock().unwrap().extend(FIRST_FRAGMENT);
            assert_eq!(read.await.unwrap(), FIRST_FRAGMENT.len());
            assert_eq!(&buffer[..FIRST_FRAGMENT.len()], FIRST_FRAGMENT);

            input.lock().unwrap().extend(SECOND_FRAGMENT);
            reader
                .read_exact(&mut buffer[FIRST_FRAGMENT.len()..])
                .await
                .unwrap();
            assert_eq!(&buffer[FIRST_FRAGMENT.len()..], SECOND_FRAGMENT);

            drop(reader);
            drop(writer);
            let port = task.await.unwrap();
            assert_eq!(port.written(), OUTPUT);
            assert_eq!(port.flush_count(), EXPECTED_FLUSH_COUNT);
        })
        .await
        .unwrap();
    });
}

#[cfg(target_os = "linux")]
#[test]
fn native_pending_read_handles_port_handoff_and_disconnect() {
    const INPUT: &[u8] = b"after-handoff";
    const OUTPUT: &[u8] = b"serial-command";

    runtime().block_on(async {
        let (mut peer, port) = serialport::TTYPort::pair().unwrap();
        let (mut reader, mut writer, mut worker) = port.split(MINIMUM_COMMAND_BUFFER);
        let mut buffer = [0; INPUT.len()];
        let mut context = Context::from_waker(Waker::noop());
        let mut read = Box::pin(reader.read(&mut buffer));

        assert!(read.as_mut().poll(&mut context).is_pending());
        assert!(Pin::new(&mut worker).poll(&mut context).is_pending());
        drop(read);

        let task = tokio::spawn(worker);
        timeout(TEST_DEADLINE, async {
            let guard = reader.take_serial_port().await.unwrap();
            drop(guard);

            let mut read = Box::pin(reader.read(&mut buffer));
            assert!(read.as_mut().poll(&mut context).is_pending());
            writer.write_all(OUTPUT).await.unwrap();
            writer.flush().await.unwrap();
            assert!(read.as_mut().poll(&mut context).is_pending());
            let mut output = [0; OUTPUT.len()];
            peer.read_exact(&mut output).unwrap();
            assert_eq!(&output, OUTPUT);

            peer.write_all(INPUT).unwrap();
            assert_eq!(read.await.unwrap(), INPUT.len());
            assert_eq!(&buffer, INPUT);

            let mut read = Box::pin(reader.read(&mut buffer));
            assert!(read.as_mut().poll(&mut context).is_pending());
            let guard = writer.take_serial_port().await.unwrap();
            assert_eq!(read.await.unwrap_err().kind(), io::ErrorKind::NotConnected);
            drop(guard);

            let mut read = Box::pin(reader.read(&mut buffer));
            assert!(read.as_mut().poll(&mut context).is_pending());
            writer.flush().await.unwrap();
            drop(peer);
            assert_eq!(read.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);

            drop(reader);
            drop(writer);
            task.await.unwrap();
        })
        .await
        .unwrap();
    });
}

#[cfg(target_os = "linux")]
#[test]
fn worker_returns_port_when_reader_is_dropped_during_native_wait() {
    runtime().block_on(async {
        let (_peer, port) = serialport::TTYPort::pair().unwrap();
        let (mut reader, writer, mut worker) = port.split(COMMAND_BUFFER);
        let mut buffer = [0];
        let mut read = Box::pin(reader.read(&mut buffer));
        let mut context = Context::from_waker(Waker::noop());

        assert!(read.as_mut().poll(&mut context).is_pending());
        assert!(Pin::new(&mut worker).poll(&mut context).is_pending());
        drop(read);
        drop(reader);
        drop(writer);

        timeout(TEST_DEADLINE, worker).await.unwrap();
    });
}

#[test]
fn pending_read_reports_worker_shutdown() {
    runtime().block_on(async {
        let (mut reader, _writer, mut worker) = FakeSerialPort::new(&[]).split(COMMAND_BUFFER);
        let mut buffer = [0];
        let mut read = Box::pin(reader.read(&mut buffer));
        let mut context = Context::from_waker(Waker::noop());

        assert!(read.as_mut().poll(&mut context).is_pending());
        assert!(Pin::new(&mut worker).poll(&mut context).is_pending());
        drop(worker);

        assert_eq!(read.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    });
}

#[test]
fn serial_port_split_spawns_background_task() {
    runtime().block_on(async {
        let (reader, writer, worker) = FakeSerialPort::new(&[]).split(COMMAND_BUFFER);
        let worker_task = tokio::spawn(worker);

        drop(reader);
        drop(writer);

        let port = worker_task.await.expect("worker task should finish");
        assert_eq!(port.name().as_deref(), Some(TEST_PORT_NAME));
    });
}

#[test]
fn reader_reads_from_worker_serial_port() {
    const INPUT: &[u8] = b"serial-input";

    runtime().block_on(async {
        let (mut reader, writer, worker) = FakeSerialPort::new(INPUT).split(COMMAND_BUFFER);
        let worker_task = tokio::spawn(worker);
        let mut buffer = [0_u8; INPUT.len()];

        reader
            .read_exact(&mut buffer)
            .await
            .expect("reader should read seeded input");

        assert_eq!(&buffer, INPUT);

        drop(reader);
        drop(writer);

        let port = worker_task.await.expect("worker task should finish");
        assert_eq!(
            port.bytes_to_read()
                .expect("read buffer length should fit into u32"),
            0,
        );
    });
}

#[test]
fn writer_writes_to_worker_serial_port() {
    const OUTPUT: &[u8] = b"serial-output";

    runtime().block_on(async {
        let (reader, mut writer, worker) = FakeSerialPort::new(&[]).split(COMMAND_BUFFER);
        let worker_task = tokio::spawn(worker);

        writer
            .write_all(OUTPUT)
            .await
            .expect("writer should write output");
        writer.flush().await.expect("writer should flush output");

        drop(reader);
        drop(writer);

        let port = worker_task.await.expect("worker task should finish");
        assert_eq!(port.written(), OUTPUT);
        assert_eq!(port.flush_count(), EXPECTED_FLUSH_COUNT);
    });
}

#[test]
fn reader_temporarily_takes_and_returns_serial_port() {
    const INPUT: &[u8] = b"input-after-return";
    const OUTPUT: &[u8] = b"output-after-return";

    runtime().block_on(async {
        let (mut reader, mut writer, worker) =
            FakeSerialPort::new(INPUT).split(MINIMUM_COMMAND_BUFFER);
        let worker_task = tokio::spawn(worker);
        let mut serial_port = reader
            .take_serial_port()
            .await
            .expect("reader should take the serial port");

        assert_eq!(serial_port.name().as_deref(), Some(TEST_PORT_NAME));
        serial_port
            .set_baud_rate(UPDATED_BAUD_RATE)
            .expect("guard should provide mutable access to the serial port");

        let mut read_buffer = [0_u8; INPUT.len()];
        let read_error = reader
            .read_exact(&mut read_buffer)
            .await
            .expect_err("reads should fail while the guard owns the serial port");
        assert_eq!(read_error.kind(), io::ErrorKind::NotConnected);

        let write_error = writer
            .write_all(OUTPUT)
            .await
            .expect_err("writes should fail while the guard owns the serial port");
        assert_eq!(write_error.kind(), io::ErrorKind::NotConnected);

        let flush_error = writer
            .flush()
            .await
            .expect_err("flushes should fail while the guard owns the serial port");
        assert_eq!(flush_error.kind(), io::ErrorKind::NotConnected);

        drop(serial_port);

        reader
            .read_exact(&mut read_buffer)
            .await
            .expect("reads should resume after the guard is dropped");
        writer
            .write_all(OUTPUT)
            .await
            .expect("writes should resume after the guard is dropped");

        assert_eq!(&read_buffer, INPUT);

        drop(reader);
        drop(writer);

        let serial_port = worker_task.await.expect("worker task should finish");
        assert_eq!(
            serial_port
                .baud_rate()
                .expect("fake serial port should report its baud rate"),
            UPDATED_BAUD_RATE,
        );
        assert_eq!(serial_port.written(), OUTPUT);
    });
}

#[test]
fn writer_takes_serial_port_exclusively() {
    runtime().block_on(async {
        let (mut reader, mut writer, worker) = FakeSerialPort::new(&[]).split(COMMAND_BUFFER);
        let worker_task = tokio::spawn(worker);
        let serial_port = writer
            .take_serial_port()
            .await
            .expect("writer should take the serial port");

        let Err(error) = reader.take_serial_port().await else {
            panic!("only one guard should own the serial port");
        };
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);

        drop(serial_port);

        let serial_port = reader
            .take_serial_port()
            .await
            .expect("reader should take the returned serial port");
        assert_eq!(serial_port.name().as_deref(), Some(TEST_PORT_NAME));

        drop(serial_port);
        drop(reader);
        drop(writer);

        worker_task.await.expect("worker task should finish");
    });
}

#[test]
fn serial_port_guard_keeps_worker_alive() {
    runtime().block_on(async {
        let (mut reader, writer, worker) = FakeSerialPort::new(&[]).split(COMMAND_BUFFER);
        let worker_task = tokio::spawn(worker);
        let serial_port = reader
            .take_serial_port()
            .await
            .expect("reader should take the serial port");

        drop(reader);
        drop(writer);
        tokio::task::yield_now().await;

        assert!(!worker_task.is_finished());

        drop(serial_port);

        let serial_port = worker_task.await.expect("worker task should finish");
        assert_eq!(serial_port.name().as_deref(), Some(TEST_PORT_NAME));
    });
}

#[test]
fn cancelled_take_returns_serial_port_to_worker() {
    runtime().block_on(async {
        let (mut reader, mut writer, worker) = FakeSerialPort::new(&[]).split(COMMAND_BUFFER);
        let worker_task = tokio::spawn(worker);
        let mut take = Box::pin(reader.take_serial_port());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(matches!(take.as_mut().poll(&mut context), Poll::Pending));

        tokio::task::yield_now().await;
        drop(take);

        let serial_port = writer
            .take_serial_port()
            .await
            .expect("cancelling a take should return the serial port");
        assert_eq!(serial_port.name().as_deref(), Some(TEST_PORT_NAME));

        drop(serial_port);
        drop(reader);
        drop(writer);

        worker_task.await.expect("worker task should finish");
    });
}

#[test]
fn serial_port_returns_through_full_command_channel() {
    runtime().block_on(async {
        let (mut reader, mut writer, worker) =
            FakeSerialPort::new(&[]).split(MINIMUM_COMMAND_BUFFER);
        let worker_task = tokio::spawn(worker);
        let serial_port = writer
            .take_serial_port()
            .await
            .expect("writer should take the serial port");
        let mut buffer = [0_u8; 1];
        let mut read = Box::pin(reader.read(&mut buffer));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(matches!(read.as_mut().poll(&mut context), Poll::Pending));

        drop(serial_port);

        let error = read
            .await
            .expect_err("the queued read should observe the absent serial port");
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);

        let serial_port = writer
            .take_serial_port()
            .await
            .expect("the full channel should still return the serial port");

        drop(serial_port);
        drop(reader);
        drop(writer);

        worker_task.await.expect("worker task should finish");
    });
}
