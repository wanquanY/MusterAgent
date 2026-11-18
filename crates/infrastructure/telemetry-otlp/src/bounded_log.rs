use std::io::{self, Write};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded, select};
use musteragent_telemetry::TelemetrySignal;
use tracing_subscriber::fmt::MakeWriter;

use crate::TelemetryHealth;
use crate::bounded_queue::{LinearizedAdmission, Sequenced};

const MAX_LOG_LINE_BYTES: usize = 8 * 1_024;

enum Control {
    Flush {
        cutoff: u64,
        ack: Sender<io::Result<()>>,
    },
    Shutdown {
        cutoff: u64,
        ack: Sender<io::Result<()>>,
    },
}

#[derive(Clone)]
pub(crate) struct BoundedLogMakeWriter {
    admission: LinearizedAdmission<Vec<u8>>,
    health: TelemetryHealth,
}

impl BoundedLogMakeWriter {
    pub(crate) fn emit_line(&self, bytes: Vec<u8>) {
        if bytes.len() > MAX_LOG_LINE_BYTES {
            self.health.dropped_invalid(TelemetrySignal::Log);
            return;
        }
        self.admission.admit(bytes);
    }
}

pub(crate) struct BoundedLogSink {
    admission: LinearizedAdmission<Vec<u8>>,
    controls: Sender<Control>,
    health: TelemetryHealth,
    timeout: Duration,
}

impl BoundedLogSink {
    pub(crate) fn new(
        capacity: usize,
        timeout: Duration,
        health: TelemetryHealth,
    ) -> io::Result<(BoundedLogMakeWriter, Self)> {
        Self::new_with_writer(capacity, timeout, health, io::stdout())
    }

    pub(crate) fn new_with_writer<W: Write + Send + 'static>(
        capacity: usize,
        timeout: Duration,
        health: TelemetryHealth,
        writer: W,
    ) -> io::Result<(BoundedLogMakeWriter, Self)> {
        let (admission, line_rx) =
            LinearizedAdmission::new(capacity, TelemetrySignal::Log, health.clone());
        let (control_tx, control_rx) = bounded(2);
        let worker_health = health.clone();
        let worker_admission = admission.clone();
        std::thread::Builder::new()
            .name("musteragent-structured-log-writer".to_owned())
            .spawn(move || {
                writer_loop(line_rx, control_rx, worker_admission, worker_health, writer);
            })?;
        Ok((
            BoundedLogMakeWriter {
                admission: admission.clone(),
                health: health.clone(),
            },
            Self {
                admission,
                controls: control_tx,
                health,
                timeout,
            },
        ))
    }

    pub(crate) fn force_flush(&self) -> io::Result<()> {
        let cutoff = self
            .admission
            .cutoff()
            .ok_or_else(|| io::Error::other("log sink already shut down"))?;
        match self.exchange_control(|ack| Control::Flush { cutoff, ack }) {
            Ok(result) => result,
            Err(error) => {
                self.health.export_result(TelemetrySignal::Log, 0, false);
                Err(error)
            }
        }
    }

    /// Establishes log readiness with a static, schema-safe process event.
    /// This bypasses application log filtering but uses the same bounded sink
    /// and physical writer as every structured log record.
    pub(crate) fn readiness_handshake(&self) -> io::Result<()> {
        self.admission.admit(
            br#"{"telemetry_internal":"log_sink_readiness","telemetry_schema":"musteragent.runtime.telemetry.v2"}
"#
            .to_vec(),
        );
        self.force_flush()
    }

    pub(crate) fn shutdown(&self) -> io::Result<()> {
        let Some(cutoff) = self.admission.close() else {
            return Ok(());
        };
        match self.exchange_control(|ack| Control::Shutdown { cutoff, ack }) {
            Ok(result) => result,
            Err(error) => {
                self.admission.abort();
                Err(error)
            }
        }
    }

    fn exchange_control(
        &self,
        make: impl FnOnce(Sender<io::Result<()>>) -> Control,
    ) -> io::Result<io::Result<()>> {
        let (ack_tx, ack_rx) = bounded(1);
        self.controls
            .send_timeout(make(ack_tx), self.timeout)
            .map_err(io::Error::other)
            .and_then(|()| ack_rx.recv_timeout(self.timeout).map_err(io::Error::other))
    }
}

impl<'writer> MakeWriter<'writer> for BoundedLogMakeWriter {
    type Writer = PendingLogLine;

    fn make_writer(&'writer self) -> Self::Writer {
        PendingLogLine {
            bytes: Vec::with_capacity(1_024),
            writer: self.clone(),
        }
    }
}

pub(crate) struct PendingLogLine {
    bytes: Vec<u8>,
    writer: BoundedLogMakeWriter,
}

impl Write for PendingLogLine {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for PendingLogLine {
    fn drop(&mut self) {
        if self.bytes.is_empty() {
            return;
        }
        if self.bytes.len() > MAX_LOG_LINE_BYTES {
            self.writer.health.dropped_invalid(TelemetrySignal::Log);
            return;
        }
        let bytes = std::mem::take(&mut self.bytes);
        self.writer.emit_line(bytes);
    }
}

fn writer_loop(
    lines: Receiver<Sequenced<Vec<u8>>>,
    controls: Receiver<Control>,
    admission: LinearizedAdmission<Vec<u8>>,
    health: TelemetryHealth,
    mut writer: impl Write,
) {
    let mut handled = 0_u64;
    loop {
        select! {
            recv(controls) -> control => match control {
                Ok(Control::Flush { cutoff, ack }) => {
                    let result = flush_to(
                        cutoff,
                        &mut handled,
                        &lines,
                        &admission,
                        &mut writer,
                        &health,
                    ).and_then(|()| flush_writer(&mut writer, &health));
                    let _ = ack.send(result);
                }
                Ok(Control::Shutdown { cutoff, ack }) => {
                    let result = flush_to(
                        cutoff,
                        &mut handled,
                        &lines,
                        &admission,
                        &mut writer,
                        &health,
                    ).and_then(|()| flush_writer(&mut writer, &health));
                    let _ = ack.send(result);
                    return;
                }
                Err(_) => return,
            },
            recv(lines) -> line => match line {
                Ok(line) => {
                    handled = line.sequence;
                    write_line(&mut writer, &health, &admission, line);
                }
                Err(_) => return,
            },
        }
    }
}

fn flush_writer(writer: &mut impl Write, health: &TelemetryHealth) -> io::Result<()> {
    let result = writer.flush();
    if result.is_err() {
        health.export_result(TelemetrySignal::Log, 0, false);
    }
    result
}

fn flush_to(
    cutoff: u64,
    handled: &mut u64,
    lines: &Receiver<Sequenced<Vec<u8>>>,
    admission: &LinearizedAdmission<Vec<u8>>,
    writer: &mut impl Write,
    health: &TelemetryHealth,
) -> io::Result<()> {
    while *handled < cutoff {
        let line = lines.recv().map_err(|_| {
            io::Error::other("log flush cutoff exceeded the available accepted queue")
        })?;
        *handled = line.sequence;
        write_line(writer, health, admission, line);
    }
    Ok(())
}

fn write_line(
    writer: &mut impl Write,
    health: &TelemetryHealth,
    admission: &LinearizedAdmission<Vec<u8>>,
    line: Sequenced<Vec<u8>>,
) {
    let sequence = line.sequence;
    let success = !admission.is_aborted() && writer.write_all(&line.value).is_ok();
    if admission.complete(sequence, sequence, 1) {
        health.export_result(TelemetrySignal::Log, 1, success);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_queue::TestAdmissionGate;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, value: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("log capture lock")
                .extend_from_slice(value);
            Ok(value.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FailingFlushWriter {
        fail: Arc<AtomicBool>,
    }

    struct BlockingWriter {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl Write for BlockingWriter {
        fn write(&mut self, value: &[u8]) -> io::Result<usize> {
            self.entered.send(()).expect("blocking writer entered");
            self.release.recv().expect("blocking writer released");
            Ok(value.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Write for FailingFlushWriter {
        fn write(&mut self, value: &[u8]) -> io::Result<usize> {
            Ok(value.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.fail.load(Ordering::Acquire) {
                Err(io::Error::other("flush failed"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn log_signal_is_independent_and_shutdown_flushes_accepted_lines() {
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, false, false);
        health.configure(TelemetrySignal::Metric, false, false);
        health.configure(TelemetrySignal::Log, true, true);
        assert!(!health.ready(), "required log has not exported");
        let capture = SharedWriter::default();
        let (make_writer, sink) = BoundedLogSink::new_with_writer(
            8,
            Duration::from_secs(1),
            health.clone(),
            capture.clone(),
        )
        .expect("start log writer");
        {
            let mut line = make_writer.make_writer();
            line.write_all(b"{\"event\":\"startup\"}\n")
                .expect("write log line");
        }
        sink.shutdown().expect("shutdown flush");
        assert_eq!(
            capture.0.lock().expect("log capture lock").as_slice(),
            b"{\"event\":\"startup\"}\n"
        );
        let snapshot = health.snapshot();
        assert!(snapshot.ready);
        assert_eq!(snapshot.log.exported_items, 1);
        assert_eq!(snapshot.trace.exported_items, 0);
        assert_eq!(snapshot.metric.exported_items, 0);
    }

    #[test]
    fn log_queue_pressure_and_oversize_are_counted_without_blocking_writer() {
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Log, true, false);
        let (admission, _held_receiver) =
            LinearizedAdmission::new(1, TelemetrySignal::Log, health.clone());
        let writer = BoundedLogMakeWriter {
            admission,
            health: health.clone(),
        };
        for value in [b"first".as_slice(), b"second".as_slice()] {
            let mut line = writer.make_writer();
            line.write_all(value).expect("in-memory line");
        }
        let mut oversized = writer.make_writer();
        oversized
            .write_all(&vec![b'x'; MAX_LOG_LINE_BYTES + 1])
            .expect("in-memory oversized line");
        drop(oversized);
        let snapshot = health.snapshot();
        assert_eq!(snapshot.log.accepted, 1);
        assert_eq!(snapshot.log.dropped_queue_full, 1);
        assert_eq!(snapshot.log.dropped_invalid, 1);
    }

    #[test]
    fn shutdown_is_linearized_after_an_inflight_log_admission() {
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Log, true, false);
        let capture = SharedWriter::default();
        let (make_writer, sink) = BoundedLogSink::new_with_writer(
            8,
            Duration::from_secs(1),
            health.clone(),
            capture.clone(),
        )
        .expect("start log writer");
        let sink = Arc::new(sink);
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        make_writer.admission.install_test_gate(TestAdmissionGate {
            entered: entered_tx,
            release: release_rx,
        });

        let producer_writer = make_writer.clone();
        let producer = std::thread::spawn(move || {
            let mut line = producer_writer.make_writer();
            line.write_all(b"pre-close\n").expect("log line");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("log admission paused");
        let closer = Arc::clone(&sink);
        let (closed_tx, closed_rx) = std::sync::mpsc::sync_channel(1);
        let closer = std::thread::spawn(move || {
            closed_tx.send(closer.shutdown()).expect("shutdown result");
        });
        assert!(closed_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release_tx.send(()).expect("release log admission");
        producer.join().expect("log producer");
        closed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown result")
            .expect("shutdown flush");
        closer.join().expect("log closer");

        {
            let mut line = make_writer.make_writer();
            line.write_all(b"post-close\n").expect("log line");
        }
        assert_eq!(
            capture.0.lock().expect("log capture lock").as_slice(),
            b"pre-close\n"
        );
        let snapshot = health.snapshot().log;
        assert_eq!(snapshot.accepted, 1);
        assert_eq!(snapshot.exported_items, 1);
        assert_eq!(snapshot.dropped_shutdown, 1);
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(make_writer.admission.counts(), (1, 1, 0));
    }

    #[test]
    fn flush_failure_marks_required_log_readiness_red() {
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Log, true, true);
        let fail = Arc::new(AtomicBool::new(true));
        let (_make_writer, sink) = BoundedLogSink::new_with_writer(
            8,
            Duration::from_secs(1),
            health.clone(),
            FailingFlushWriter {
                fail: Arc::clone(&fail),
            },
        )
        .expect("start log writer");

        assert!(sink.readiness_handshake().is_err());
        let snapshot = health.snapshot();
        assert!(snapshot.log.has_succeeded, "the write itself succeeded");
        assert!(!snapshot.log.healthy);
        assert!(!snapshot.ready);
        assert_eq!(snapshot.log.export_failures, 1);

        fail.store(false, Ordering::Release);
        sink.shutdown()
            .expect("clean shutdown after fixture recovery");
    }

    #[test]
    fn shutdown_deadline_accounts_a_blocked_log_write_once() {
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Log, true, true);
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (make_writer, sink) = BoundedLogSink::new_with_writer(
            8,
            Duration::from_millis(20),
            health.clone(),
            BlockingWriter {
                entered: entered_tx,
                release: release_rx,
            },
        )
        .expect("start blocking log writer");
        {
            let mut line = make_writer.make_writer();
            line.write_all(b"admitted before shutdown\n")
                .expect("log line");
        }
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer blocked");
        assert!(sink.shutdown().is_err());
        let snapshot = health.snapshot().log;
        assert_eq!(snapshot.accepted, 1);
        assert_eq!(snapshot.exported_items, 0);
        assert_eq!(snapshot.dropped_export_failure, 1);
        assert_eq!(snapshot.export_failures, 1);
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(make_writer.admission.counts(), (1, 1, 0));
        release_tx.send(()).expect("release writer");
    }
}
