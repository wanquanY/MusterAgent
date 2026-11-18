use super::*;
use tokio::sync::Notify;

#[derive(Default)]
struct FaultAndDrainExecutor {
    calls: AtomicUsize,
    both_started: Notify,
    panic_now: Notify,
    draining: Notify,
    drain_now: Notify,
    causes: Mutex<Vec<CancellationCause>>,
}

impl AttemptExecutor for FaultAndDrainExecutor {
    fn execute(
        &self,
        _attempt: DispatchedAttempt,
        _telemetry: AttemptTelemetryContext,
        supervisor: InvocationSupervisor,
    ) -> AttemptFuture<'_> {
        Box::pin(async move {
            let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
            if ordinal == 0 {
                self.panic_now.notified().await;
                panic!("deliberate attempt task failure fixture");
            }
            self.both_started.notify_one();
            let cancelled = supervisor
                .run(CancellationCause::SystemShutdown, pending::<()>())
                .await
                .expect_err("fixture receives process interruption");
            self.causes.lock().expect("causes").push(cancelled.cause);
            self.draining.notify_one();
            // The executor still owns physical cleanup after cancellation. The
            // runner must join it, not just cancel/drop the sibling JoinSet.
            self.drain_now.notified().await;
            AgentLoopExit::Interrupted {
                cause: cancelled.cause,
            }
        })
    }
}

async fn fault_must_join_other_attempts(during_shutdown: bool) {
    let executor = Arc::new(FaultAndDrainExecutor::default());
    let runner = RuntimeWorkerRunner::new(
        policy(),
        Arc::new(FixedControls { stop_intent: None }),
        Arc::new(RecordingCancellationStore::default()),
        executor.clone(),
    )
    .expect("runner");
    let (tx, rx) = mpsc::channel(2);
    tx.send(receipt()).await.expect("first dispatch");
    tx.send(receipt()).await.expect("second dispatch");
    let cancellation = CancellationToken::new();
    let child = cancellation.clone();
    let mut task = tokio::spawn(async move { runner.run(rx, child).await });
    tokio::time::timeout(Duration::from_secs(2), executor.both_started.notified())
        .await
        .expect("both attempts entered");
    if during_shutdown {
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), executor.draining.notified())
            .await
            .expect("sibling begins graceful shutdown");
    }
    executor.panic_now.notify_one();
    if !during_shutdown {
        tokio::select! {
            biased;
            result = &mut task => panic!("runner returned before sibling drain: {result:?}"),
            () = executor.draining.notified() => {},
            () = tokio::time::sleep(Duration::from_secs(2)) => panic!("sibling never receives shutdown"),
        }
    }
    // Give the supervisor a chance to observe the panic while the sibling is
    // deliberately retained at cleanup. This wait must not finish the runner.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut task)
            .await
            .is_err(),
        "failure observation cannot replace physical drain"
    );
    assert_eq!(
        *executor.causes.lock().expect("causes"),
        vec![CancellationCause::SystemShutdown]
    );
    executor.drain_now.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("bounded join")
            .expect("supervisor task"),
        Err(WorkerRunnerError::AttemptTaskLost)
    );
    drop(tx);
}

#[tokio::test]
async fn attempt_panic_still_interrupts_and_joins_its_sibling() {
    fault_must_join_other_attempts(false).await;
}

#[tokio::test]
async fn panic_during_shutdown_does_not_skip_the_remaining_drain() {
    fault_must_join_other_attempts(true).await;
}
