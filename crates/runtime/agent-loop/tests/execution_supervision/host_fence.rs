use super::*;
use musteragent_execution_liveness::{ExecutionStopControl, LivenessJournalError};
use musteragent_execution_liveness_io::HomeExecutionActivityFactory;
use musteragent_execution_supervision::{HostExecutionFence, HostExecutionPermit};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
    time::Duration,
};
struct Fence {
    denied: bool,
    revoked: bool,
    settled: Arc<AtomicBool>,
}
struct Permit {
    revoked: bool,
    settled: Arc<AtomicBool>,
}
impl HostExecutionPermit for Permit {
    fn check(&self) -> Result<(), LivenessJournalError> {
        Ok(())
    }
    fn revoked(&self) -> ApplicationFuture<'_, ()> {
        Box::pin(async move {
            if !self.revoked {
                pending::<()>().await;
            }
        })
    }
    fn settled(&self) {
        self.settled.store(true, Ordering::SeqCst);
    }
}
impl HostExecutionFence for Fence {
    fn acquire<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: Arc<dyn ExecutionStopControl>,
        _: Duration,
    ) -> ApplicationFuture<'a, Result<Box<dyn HostExecutionPermit>, LivenessJournalError>> {
        Box::pin(async move {
            if self.denied {
                return Err(LivenessJournalError {
                    code: ErrorCode::PolicyDenied,
                    retryable: false,
                    rejection: None,
                });
            }
            Ok(Box::new(Permit {
                revoked: self.revoked,
                settled: self.settled.clone(),
            }) as Box<dyn HostExecutionPermit>)
        })
    }
}
#[tokio::test(start_paused = true)]
async fn host_membership_denied_before_first_poll_never_dispatches() {
    let fixture = fixture(ControlBehavior::Confirm);
    let record = fixture
        .journal
        .record
        .lock()
        .expect("valid test fixture")
        .clone();
    let settled = Arc::new(AtomicBool::new(false));
    let home = musteragent_loop::ExecutionSupervision::new(
        fixture.journal,
        fixture.clock,
        Arc::new(HomeExecutionActivityFactory),
    )
    .with_host_fence(Arc::new(Fence {
        denied: true,
        revoked: false,
        settled: settled.clone(),
    }));
    let (reactor, _) = home
        .attach(fixture.owner, record, fixture.control.clone())
        .expect("valid test fixture");
    let polls = AtomicUsize::new(0);
    let error = reactor
        .run(
            async {
                polls.fetch_add(1, Ordering::SeqCst);
                ExecutionBoundary::Committed(())
            },
            pending::<ControlStop>(),
        )
        .await
        .err()
        .expect("valid test fixture");
    assert_eq!(
        error.origin,
        musteragent_execution_supervision::ExecutionSupervisionFailureOrigin::HostFence
    );
    assert!(matches!(
        error.cause,
        ExecutionSupervisionError::Journal(LivenessJournalError {
            code: ErrorCode::PolicyDenied,
            ..
        })
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .control
            .calls
            .lock()
            .expect("valid test fixture")
            .is_empty()
    );
    assert!(!settled.load(Ordering::SeqCst));
}
#[tokio::test(start_paused = true)]
async fn host_revocation_requires_original_exact_stop_proof_before_settling() {
    for behavior in [
        ControlBehavior::Confirm,
        ControlBehavior::WrongConfirmation,
        ControlBehavior::Unresponsive,
    ] {
        let fixture = fixture(behavior);
        let record = fixture
            .journal
            .record
            .lock()
            .expect("valid test fixture")
            .clone();
        let settled = Arc::new(AtomicBool::new(false));
        let home = musteragent_loop::ExecutionSupervision::new(
            fixture.journal,
            fixture.clock,
            Arc::new(HomeExecutionActivityFactory),
        )
        .with_host_fence(Arc::new(Fence {
            denied: false,
            revoked: true,
            settled: settled.clone(),
        }));
        let (reactor, _) = home
            .attach(fixture.owner, record, fixture.control)
            .expect("valid test fixture");
        let polls = AtomicUsize::new(0);
        let result = reactor
            .run(
                async {
                    polls.fetch_add(1, Ordering::SeqCst);
                    ExecutionBoundary::Committed(())
                },
                pending::<ControlStop>(),
            )
            .await;
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "revoked before start still dispatched"
        );
        let positive = matches!(
            result,
            Ok(ExecutionSupervisionOutcome::Stopped {
                closure: Closure::StopConfirmed(StopReason::AuthorizationLost),
                ..
            })
        );
        assert_eq!(settled.load(Ordering::SeqCst), positive);
        assert_eq!(positive, matches!(behavior, ControlBehavior::Confirm));
    }
}
