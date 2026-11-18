use super::*;
use futures_util::FutureExt;
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, LivenessError, StopReason,
};
use musteragent_runtime_capability::CooperativeExecution;
use musteragent_runtime_contract::execution_liveness::ActivitySource;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct Sink {
    binding: ExecutionBinding,
    ended: AtomicBool,
    cursor: AtomicU64,
}
impl ExecutionActivitySink for Sink {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn accepted(
        &self,
    ) -> Result<musteragent_execution_liveness::ExecutionAcceptance, LivenessError> {
        if self.ended.load(Ordering::SeqCst) {
            return Err(LivenessError::ScopeClosed);
        }
        Ok(musteragent_execution_liveness::ExecutionAcceptance::AlreadyObserved)
    }
    fn wait_for_acceptance(&self) -> ApplicationFuture<'_, Result<(), LivenessError>> {
        Box::pin(async { Ok(()) })
    }
    fn ended(&self) -> Result<(), LivenessError> {
        assert!(!self.ended.swap(true, Ordering::SeqCst));
        Ok(())
    }
    fn advanced(&self, source: ActivitySource, cursor: NonZeroU64) -> Result<(), LivenessError> {
        if self.ended.load(Ordering::SeqCst) {
            return Err(LivenessError::ScopeClosed);
        }
        assert_eq!(source, ActivitySource::ExecutorWork);
        assert_eq!(
            self.cursor.swap(cursor.get(), Ordering::SeqCst) + 1,
            cursor.get()
        );
        Ok(())
    }
}

#[test]
fn process_exit_leaves_copy_and_final_commit_live_without_replaying_progress() {
    let (_, _, binding) = fixture();
    let sink = Arc::new(Sink {
        binding,
        ended: AtomicBool::new(false),
        cursor: AtomicU64::new(0),
    });
    let observed = sink.clone();
    let operation = Box::new(CooperativeExecution::new(
        binding,
        || false,
        move |context| {
            Box::pin(async move {
                context.completed_stage().unwrap(); // verified dependencies
                for _ in 0..2 {
                    let stage = context.stage_activity();
                    assert_eq!(stage.binding(), binding);
                    stage.accepted().unwrap();
                    stage.wait_for_acceptance().await.unwrap();
                    stage
                        .advanced(ActivitySource::Stdout, NonZeroU64::new(10).unwrap())
                        .unwrap();
                    assert!(matches!(
                        stage.advanced(ActivitySource::Stdout, NonZeroU64::new(10).unwrap()),
                        Err(LivenessError::NonAdvancingEvidence)
                    ));
                    stage
                        .advanced(ActivitySource::Stderr, NonZeroU64::new(3).unwrap())
                        .unwrap();
                    stage.ended().unwrap();
                    assert!(!observed.ended.load(Ordering::SeqCst));
                    assert!(matches!(stage.ended(), Err(LivenessError::ScopeClosed)));
                    assert!(matches!(
                        stage.advanced(ActivitySource::Stdout, NonZeroU64::new(11).unwrap()),
                        Err(LivenessError::ScopeClosed)
                    ));
                    context.completed_stage().unwrap(); // validate and copy real output
                }
                context.quiescent();
                true
            })
        },
    ));
    let control = operation.stop_control();
    assert_eq!(operation.execute(sink.clone()).now_or_never(), Some(true));
    assert_eq!(sink.cursor.load(Ordering::SeqCst), 9);
    assert!(sink.ended.load(Ordering::SeqCst));
    assert!(
        control
            .stop(StopReason::ExecutionFailed)
            .now_or_never()
            .unwrap()
            .is_ok()
    );
}

#[test]
fn child_end_does_not_confirm_an_unsettled_dependency() {
    let (_, _, binding) = fixture();
    let sink = Arc::new(Sink {
        binding,
        ended: AtomicBool::new(false),
        cursor: AtomicU64::new(0),
    });
    let operation = Box::new(CooperativeExecution::new(
        binding,
        || false,
        move |context| {
            Box::pin(async move {
                let dependency = context.dependency();
                context.stage_activity().ended().unwrap();
                drop(dependency); // root process exited; descendants are unconfirmed
                context.quiescent();
                true
            })
        },
    ));
    let control = operation.stop_control();
    assert_eq!(operation.execute(sink.clone()).now_or_never(), Some(true));
    assert!(!sink.ended.load(Ordering::SeqCst));
    assert!(
        control
            .stop(StopReason::ExecutionFailed)
            .now_or_never()
            .unwrap()
            .is_err()
    );
}

#[test]
fn a_committed_stop_rejects_late_child_progress_and_end() {
    let (_, _, binding) = fixture();
    let sink = Arc::new(Sink {
        binding,
        ended: AtomicBool::new(false),
        cursor: AtomicU64::new(0),
    });
    let operation = Box::new(CooperativeExecution::new(
        binding,
        || false,
        move |context| {
            Box::pin(async move {
                let stage = context.stage_activity();
                stage
                    .advanced(ActivitySource::Stdout, NonZeroU64::new(1).unwrap())
                    .unwrap();
                context.cancelled().await;
                assert!(matches!(
                    stage.advanced(ActivitySource::Stdout, NonZeroU64::new(2).unwrap()),
                    Err(LivenessError::ScopeClosed)
                ));
                assert!(matches!(stage.ended(), Err(LivenessError::ScopeClosed)));
                context.quiescent();
                true
            })
        },
    ));
    let control = operation.stop_control();
    let execution = operation.execute(sink.clone());
    tokio::pin!(execution);
    assert!(execution.as_mut().now_or_never().is_none());
    let stopping = control.stop(StopReason::UserCancelled);
    tokio::pin!(stopping);
    assert!(stopping.as_mut().now_or_never().is_none());
    assert_eq!(execution.as_mut().now_or_never(), Some(true));
    assert!(stopping.now_or_never().unwrap().is_ok());
    assert_eq!(sink.cursor.load(Ordering::SeqCst), 1);
}
