use super::*;
use std::sync::Mutex;

struct TemporaryControls {
    calls: AtomicUsize,
    fence: Mutex<Option<AttemptFence>>,
    recover: bool,
}
impl AttemptControlStore for TemporaryControls {
    fn heartbeat(
        &self,
        fence: &AttemptFence,
        _: Duration,
    ) -> ApplicationFuture<'_, Result<LeaseHeartbeat, AttemptControlError>> {
        let mut saved = self.fence.lock().expect("heartbeat fixture");
        if let Some(original) = saved.as_ref() {
            assert_eq!(original, fence);
        } else {
            *saved = Some(fence.clone());
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let result = match call {
            0 => Ok(LeaseHeartbeat {
                database_now: UtcTimestamp::from_unix_millis(0),
                lease_expires_at: UtcTimestamp::from_unix_millis(1_000),
                stop_intent: None,
            }),
            1 => Err(AttemptControlError::DatabaseUnavailable),
            2 if self.recover => Ok(LeaseHeartbeat {
                database_now: UtcTimestamp::from_unix_millis(200),
                lease_expires_at: UtcTimestamp::from_unix_millis(2_000),
                stop_intent: None,
            }),
            _ if self.recover => Err(AttemptControlError::LeaseLost),
            _ => Err(AttemptControlError::DatabaseUnavailable),
        };
        Box::pin(async move { result })
    }
    fn surrender(
        &self,
        _: &AttemptFence,
        _: InterruptionReason,
    ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(start_paused = true)]
async fn temporary_heartbeat_failure_keeps_the_original_running_attempt_until_confirmed_loss() {
    let controls = Arc::new(TemporaryControls {
        calls: AtomicUsize::new(0),
        fence: Mutex::new(None),
        recover: true,
    });
    let claimed = receipt();
    let original = claimed.fence.clone();
    let exit = run_attempt(
        Arc::new(NonCooperativeExecutor),
        controls.clone(),
        Arc::new(RecordingCancellationStore::default()),
        Arc::new(RejectingCoordinationForcedAbortStore),
        WorkerRunnerPolicy {
            heartbeat_interval: Duration::from_millis(100),
            shutdown_drain_timeout: Duration::from_millis(100),
            ..policy()
        },
        claimed,
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        exit,
        AgentLoopExit::Interrupted {
            cause: CancellationCause::LeaseLost
        }
    ));
    assert_eq!(
        controls.calls.load(Ordering::SeqCst),
        4,
        "the first unavailable observation must not drop execution"
    );
    assert_eq!(
        *controls.fence.lock().expect("heartbeat fixture"),
        Some(original)
    );
}

#[tokio::test(start_paused = true)]
async fn persistent_heartbeat_outage_does_not_keep_execution_past_its_original_lease_and_drain() {
    let controls = Arc::new(TemporaryControls {
        calls: AtomicUsize::new(0),
        fence: Mutex::new(None),
        recover: false,
    });
    let start = Instant::now();
    let exit = run_attempt(
        Arc::new(NonCooperativeExecutor),
        controls.clone(),
        Arc::new(RecordingCancellationStore::default()),
        Arc::new(RejectingCoordinationForcedAbortStore),
        WorkerRunnerPolicy {
            heartbeat_interval: Duration::from_millis(100),
            shutdown_drain_timeout: Duration::from_millis(100),
            ..policy()
        },
        receipt(),
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        exit,
        AgentLoopExit::Interrupted {
            cause: CancellationCause::LeaseLost
        }
    ));
    assert_eq!(Instant::now() - start, Duration::from_millis(1_100));
    assert!(controls.calls.load(Ordering::SeqCst) > 2);
}
