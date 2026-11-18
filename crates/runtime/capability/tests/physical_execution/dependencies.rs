use super::*;
use futures_util::FutureExt as _;
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, LivenessError, StopReason,
};
use musteragent_runtime_capability::CooperativeExecution;
use musteragent_runtime_contract::execution_liveness::ActivitySource;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct Sink {
    binding: ExecutionBinding,
    ends: AtomicUsize,
    progress: AtomicUsize,
}
impl Sink {
    pub(super) fn new(binding: ExecutionBinding) -> Arc<Self> {
        Arc::new(Self {
            binding,
            ends: AtomicUsize::new(0),
            progress: AtomicUsize::new(0),
        })
    }
    pub(super) fn progress(&self) -> usize {
        self.progress.load(Ordering::SeqCst)
    }
}
impl ExecutionActivitySink for Sink {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn accepted(
        &self,
    ) -> Result<musteragent_execution_liveness::ExecutionAcceptance, LivenessError> {
        Ok(musteragent_execution_liveness::ExecutionAcceptance::Observed)
    }
    fn wait_for_acceptance(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<(), musteragent_execution_liveness::LivenessError>,
    > {
        Box::pin(async { Ok(()) })
    }
    fn ended(&self) -> Result<(), LivenessError> {
        self.ends.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn advanced(&self, source: ActivitySource, cursor: NonZeroU64) -> Result<(), LivenessError> {
        assert_eq!(source, ActivitySource::ExecutorWork);
        let previous = self.progress.swap(cursor.get() as usize, Ordering::SeqCst);
        assert_eq!(previous + 1, cursor.get() as usize);
        Ok(())
    }
}
pub(super) fn verify(unknown: bool) {
    let (_, _, binding) = fixture();
    let sink = Arc::new(Sink {
        binding,
        ends: AtomicUsize::new(0),
        progress: AtomicUsize::new(0),
    });
    let operation = Box::new(CooperativeExecution::new(
        binding,
        || false,
        move |context| {
            Box::pin(async move {
                {
                    let mut first = context.dependency();
                    context.completed_stage().expect("actual child work");
                    if !unknown {
                        first.confirm_stopped();
                    }
                }
                {
                    let mut second = context.dependency();
                    context.completed_stage().expect("second child work");
                    second.confirm_stopped();
                }
                context.quiescent();
                true
            })
        },
    ));
    let control = operation.stop_control();
    assert_eq!(operation.execute(sink.clone()).now_or_never(), Some(true));
    let confirmation = control
        .stop(StopReason::ExecutionFailed)
        .now_or_never()
        .expect("settled owner");
    assert_eq!(confirmation.is_ok(), !unknown);
    assert_eq!(sink.ends.load(Ordering::SeqCst), usize::from(!unknown));
    assert_eq!(sink.progress.load(Ordering::SeqCst), 2);
}

#[test]
fn read_stage_cancellation_releases_the_read_before_confirming_stop() {
    let (_, _, binding) = fixture();
    let sink = Sink::new(binding);
    let reads = Arc::new(AtomicUsize::new(0));
    let owner = reads.clone();
    struct ReadGuard(Arc<AtomicUsize>);
    impl Drop for ReadGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let operation = Box::new(CooperativeExecution::new(
        binding,
        || Ok(Ok::<(), ()>(())),
        move |context| {
            Box::pin(async move {
                context
                    .read_stage(async { Ok::<_, ()>(()) })
                    .await
                    .expect("not cancelled")
                    .expect("real completed stage");
                assert!(
                    context
                        .read_stage(async { Err::<(), _>(()) })
                        .await
                        .expect("failed read is not cancellation")
                        .is_err()
                );
                let result = context
                    .read_stage(async {
                        owner.fetch_add(1, Ordering::SeqCst);
                        let _guard = ReadGuard(owner);
                        std::future::pending::<Result<(), ()>>().await
                    })
                    .await;
                context.quiescent();
                result
            })
        },
    ));
    let control = operation.stop_control();
    let execution = operation.execute(sink.clone());
    tokio::pin!(execution);
    assert!(execution.as_mut().now_or_never().is_none());
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(sink.progress(), 1);
    let stopping = control.stop(StopReason::UserCancelled);
    tokio::pin!(stopping);
    assert!(stopping.as_mut().now_or_never().is_none());
    let result = execution
        .as_mut()
        .now_or_never()
        .expect("read cancellation settles");
    let stopped = stopping.now_or_never().expect("stop confirmation");
    assert_eq!(
        result,
        Err(musteragent_runtime_contract::ErrorCode::RunTransitionInvalid)
    );
    assert!(stopped.is_ok());
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(sink.progress(), 1, "cancellation is not work evidence");
}
