use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use musteragent_telemetry::TelemetrySignal;

use crate::TelemetryHealth;

/// Queue item whose sequence is assigned at the same linearization point as
/// bounded-channel publication.
pub(crate) struct Sequenced<T> {
    pub(crate) sequence: u64,
    pub(crate) value: T,
}

struct AdmissionState {
    open: bool,
    aborted: bool,
    accepted: u64,
    completed: u64,
    outstanding: u64,
}

struct AdmissionInner<T> {
    sender: Sender<Sequenced<T>>,
    state: Mutex<AdmissionState>,
    health: TelemetryHealth,
    signal: TelemetrySignal,
    #[cfg(test)]
    gate: Mutex<Option<TestAdmissionGate>>,
}

/// Shared admission owner for bounded trace and log queues.
///
/// The state lock covers the open check, sequence assignment, channel publish,
/// and accepted/outstanding accounting. Closing admission takes the same lock,
/// so its cutoff includes every pre-close publication and excludes every
/// post-close attempt.
pub(crate) struct LinearizedAdmission<T> {
    inner: Arc<AdmissionInner<T>>,
}

impl<T> Clone for LinearizedAdmission<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Debug for LinearizedAdmission<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let state = self.inner.state.lock().expect("telemetry admission lock");
        formatter
            .debug_struct("LinearizedAdmission")
            .field("capacity", &self.inner.sender.capacity())
            .field("open", &state.open)
            .field("accepted", &state.accepted)
            .field("completed", &state.completed)
            .field("outstanding", &state.outstanding)
            .finish()
    }
}

impl<T> LinearizedAdmission<T> {
    pub(crate) fn new(
        capacity: usize,
        signal: TelemetrySignal,
        health: TelemetryHealth,
    ) -> (Self, Receiver<Sequenced<T>>) {
        let (sender, receiver) = bounded(capacity);
        (
            Self {
                inner: Arc::new(AdmissionInner {
                    sender,
                    state: Mutex::new(AdmissionState {
                        open: true,
                        aborted: false,
                        accepted: 0,
                        completed: 0,
                        outstanding: 0,
                    }),
                    health,
                    signal,
                    #[cfg(test)]
                    gate: Mutex::new(None),
                }),
            },
            receiver,
        )
    }

    pub(crate) fn admit(&self, value: T) {
        let mut state = self.inner.state.lock().expect("telemetry admission lock");
        #[cfg(test)]
        self.wait_at_test_gate();
        if !state.open {
            self.inner.health.dropped_shutdown(self.inner.signal);
            return;
        }
        let sequence = state
            .accepted
            .checked_add(1)
            .expect("telemetry sequence overflow");
        match self.inner.sender.try_send(Sequenced { sequence, value }) {
            Ok(()) => {
                state.accepted = sequence;
                state.outstanding = state
                    .outstanding
                    .checked_add(1)
                    .expect("telemetry outstanding overflow");
                self.inner.health.accepted(self.inner.signal);
                self.inner.health.queue_increment(self.inner.signal);
            }
            Err(TrySendError::Full(_)) => self.inner.health.dropped_full(self.inner.signal),
            Err(TrySendError::Disconnected(_)) => {
                state.open = false;
                self.inner.health.dropped_shutdown(self.inner.signal);
            }
        }
    }

    /// Freezes a force-flush cutoff without closing admission.
    pub(crate) fn cutoff(&self) -> Option<u64> {
        let state = self.inner.state.lock().expect("telemetry admission lock");
        state.open.then_some(state.accepted)
    }

    /// Closes admission and returns the exact final accepted cutoff.
    pub(crate) fn close(&self) -> Option<u64> {
        let mut state = self.inner.state.lock().expect("telemetry admission lock");
        if !state.open {
            return None;
        }
        state.open = false;
        Some(state.accepted)
    }

    /// Accounts a contiguous receiver-ordered batch exactly once.
    pub(crate) fn complete(&self, first: u64, last: u64, count: usize) -> bool {
        assert!(count > 0, "cannot complete an empty telemetry batch");
        let count = u64::try_from(count).expect("telemetry batch count fits u64");
        let mut state = self.inner.state.lock().expect("telemetry admission lock");
        if state.aborted {
            assert!(
                last <= state.completed,
                "aborted completion was not accounted"
            );
            return false;
        }
        let expected_first = state
            .completed
            .checked_add(1)
            .expect("telemetry completion sequence overflow");
        assert_eq!(first, expected_first, "telemetry completion reordered");
        assert_eq!(
            last.checked_sub(first)
                .and_then(|delta| delta.checked_add(1)),
            Some(count),
            "telemetry completion batch is not contiguous"
        );
        assert!(
            last <= state.accepted,
            "completed unaccepted telemetry item"
        );
        state.completed = last;
        state.outstanding = state
            .outstanding
            .checked_sub(count)
            .expect("telemetry outstanding underflow");
        self.inner.health.queue_decrement(
            self.inner.signal,
            usize::try_from(count).expect("batch fits usize"),
        );
        true
    }

    /// Accounts every still-outstanding admitted item as a shutdown drop.
    /// Later worker completions for the frozen sequence range become no-ops,
    /// which makes timeout accounting exact instead of double counting.
    pub(crate) fn abort(&self) -> u64 {
        let mut state = self.inner.state.lock().expect("telemetry admission lock");
        if state.aborted {
            return 0;
        }
        state.open = false;
        state.aborted = true;
        let dropped = state.outstanding;
        state.completed = state.accepted;
        state.outstanding = 0;
        if dropped > 0 {
            self.inner.health.queue_decrement(
                self.inner.signal,
                usize::try_from(dropped).expect("outstanding count fits usize"),
            );
        }
        self.inner.health.export_result(
            self.inner.signal,
            usize::try_from(dropped).expect("outstanding count fits usize"),
            false,
        );
        dropped
    }

    pub(crate) fn is_aborted(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("telemetry admission lock")
            .aborted
    }

    #[cfg(test)]
    pub(crate) fn counts(&self) -> (u64, u64, u64) {
        let state = self.inner.state.lock().expect("telemetry admission lock");
        (state.accepted, state.completed, state.outstanding)
    }

    #[cfg(test)]
    pub(crate) fn install_test_gate(&self, gate: TestAdmissionGate) {
        *self.inner.gate.lock().expect("telemetry gate lock") = Some(gate);
    }

    #[cfg(test)]
    fn wait_at_test_gate(&self) {
        let gate = self.inner.gate.lock().expect("telemetry gate lock").take();
        if let Some(gate) = gate {
            gate.entered.send(()).expect("admission gate entered");
            gate.release.recv().expect("admission gate released");
        }
    }
}

#[cfg(test)]
pub(crate) struct TestAdmissionGate {
    pub(crate) entered: std::sync::mpsc::SyncSender<()>,
    pub(crate) release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn close_waits_for_inflight_admission_and_freezes_an_exact_cutoff() {
        let health = TelemetryHealth::default();
        let (queue, receiver) = LinearizedAdmission::new(4, TelemetrySignal::Trace, health.clone());
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        queue.install_test_gate(TestAdmissionGate {
            entered: entered_tx,
            release: release_rx,
        });

        let producer = queue.clone();
        let producer = std::thread::spawn(move || producer.admit(1_u8));
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("producer reached linearization lock");
        let closer = queue.clone();
        let (cutoff_tx, cutoff_rx) = std::sync::mpsc::sync_channel(1);
        let closer = std::thread::spawn(move || cutoff_tx.send(closer.close()).expect("cutoff"));
        assert!(
            cutoff_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "close cannot pass an in-flight admission"
        );
        release_tx.send(()).expect("release admission");
        producer.join().expect("producer");
        assert_eq!(cutoff_rx.recv_timeout(Duration::from_secs(1)), Ok(Some(1)));
        closer.join().expect("closer");

        queue.admit(2_u8);
        let item = receiver.recv().expect("pre-close item");
        assert_eq!(item.sequence, 1);
        assert!(queue.complete(1, 1, 1));
        assert_eq!(queue.counts(), (1, 1, 0));
        let snapshot = health.snapshot();
        assert_eq!(snapshot.trace.accepted, 1);
        assert_eq!(snapshot.trace.dropped_shutdown, 1);
        assert_eq!(snapshot.trace.queue_depth, 0);
    }

    #[test]
    fn abort_accounts_each_outstanding_item_once() {
        let health = TelemetryHealth::default();
        let (queue, receiver) = LinearizedAdmission::new(4, TelemetrySignal::Log, health.clone());
        queue.admit(1_u8);
        queue.admit(2_u8);
        let first = receiver.recv().expect("first admitted item");
        assert_eq!(queue.abort(), 2);
        assert!(!queue.complete(first.sequence, first.sequence, 1));
        assert_eq!(queue.abort(), 0);
        assert_eq!(queue.counts(), (2, 2, 0));
        let snapshot = health.snapshot().log;
        assert_eq!(snapshot.accepted, 2);
        assert_eq!(snapshot.exported_items, 0);
        assert_eq!(snapshot.dropped_export_failure, 2);
        assert_eq!(snapshot.export_failures, 1);
        assert_eq!(snapshot.queue_depth, 0);
    }
}
