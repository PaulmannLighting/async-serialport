//! Integration tests for worker-backed async serial-port halves.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::time::Duration;

use async_serialport::AsyncSerialPort;
use bytes as _;
use serialport::{
    ClearBuffer, DataBits, Error, ErrorKind, FlowControl, Parity, SerialPort, StopBits,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::{Builder, Runtime};

const COMMAND_BUFFER: usize = 4;
const DEFAULT_BAUD_RATE: u32 = 115_200;
const EXPECTED_FLUSH_COUNT: usize = 1;
const TEST_PORT_NAME: &str = "fake-serial-port";
const TEST_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
struct FakeSerialPort {
    read_buffer: VecDeque<u8>,
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
            read_buffer: input.iter().copied().collect(),
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
        let bytes_to_read = self.read_buffer.len().min(buf.len());

        for byte in buf.iter_mut().take(bytes_to_read) {
            let Some(next) = self.read_buffer.pop_front() else {
                return Err(io::ErrorKind::UnexpectedEof.into());
            };
            *byte = next;
        }

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
        self.read_buffer
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
        .build()
        .expect("test runtime should build")
}

#[test]
fn serial_port_split_spawns_background_task() {
    runtime().block_on(async {
        let (reader, writer, worker_task) = FakeSerialPort::new(&[]).split(COMMAND_BUFFER);

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
        let (mut reader, writer, worker_task) = FakeSerialPort::new(INPUT).split(COMMAND_BUFFER);
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
        let (reader, mut writer, worker_task) = FakeSerialPort::new(&[]).split(COMMAND_BUFFER);

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
