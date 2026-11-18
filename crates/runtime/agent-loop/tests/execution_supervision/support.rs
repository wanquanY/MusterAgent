use musteragent_execution_liveness::*;
use musteragent_execution_liveness_io::activity_channel;
use musteragent_identity::{AttemptNumber, LeaseId, UtcTimestamp};
use musteragent_loop::ExecutionReactor;
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode,
    execution_liveness::{ExecutionBinding, ExecutionScope},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
#[allow(dead_code)]
#[path = "../../../execution-liveness/tests/support/mod.rs"]
mod policy_support;

pub struct Clock {
    base: tokio::time::Instant,
    epoch: musteragent_identity::HostClockEpochId,
    pub suspend_ms: AtomicU64,
}
impl LivenessHomeClock for Clock {
    fn sample(&self) -> Result<ClockSample, LivenessError> {
        let elapsed =
            self.base.elapsed() + Duration::from_millis(self.suspend_ms.load(Ordering::SeqCst));
        Ok(ClockSample {
            monotonic: LocalInstant {
                epoch: self.epoch,
                elapsed,
            },
            utc: UtcTimestamp::from_unix_millis(
                i64::try_from(elapsed.as_millis()).expect("test time"),
            ),
        })
    }
}
pub struct Journal {
    pub record: Mutex<ExecutionRecord>,
    pub fail: AtomicBool,
    pub stall: AtomicBool,
    clock: Arc<Clock>,
}
fn error(code: ErrorCode, rejection: Option<LivenessError>) -> LivenessJournalError {
    LivenessJournalError {
        code,
        retryable: code == ErrorCode::DependencyUnavailable,
        rejection,
    }
}
fn rejected(rejection: LivenessError) -> LivenessJournalError {
    error(
        if rejection == LivenessError::RevisionMismatch {
            ErrorCode::IdempotencyConflict
        } else {
            ErrorCode::InvalidCommand
        },
        Some(rejection),
    )
}
impl Journal {
    async fn available(&self) -> Result<(), LivenessJournalError> {
        if self.stall.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            Err(error(ErrorCode::DependencyUnavailable, None))
        } else {
            Ok(())
        }
    }
    pub fn phase(&self) -> Phase {
        let guard = self.record.lock().expect("record");
        let ExecutionRecord::Checkpoint(checkpoint) = &*guard else {
            panic!("checkpoint")
        };
        checkpoint.state().phase()
    }
}
impl FencedExecutionLivenessJournal for Journal {
    type Owner = AttemptContext;
    fn load<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: ExecutionScope,
    ) -> ApplicationFuture<'a, Result<Option<ExecutionRecord>, LivenessJournalError>> {
        Box::pin(async {
            self.available().await?;
            Ok(Some(self.record.lock().expect("record").clone()))
        })
    }
    fn admit<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: ExecutionAdmission,
    ) -> ApplicationFuture<'a, Result<ExecutionRecord, LivenessJournalError>> {
        Box::pin(async { Err(error(ErrorCode::InvalidCommand, None)) })
    }
    fn mutate<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: ExecutionScope,
        revision: u64,
        mutation: RecordMutation,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>> {
        Box::pin(async move {
            self.available().await?;
            let mut record = self.record.lock().expect("record");
            let next = record
                .reduce(revision, mutation, self.clock.sample().map_err(rejected)?)
                .map_err(rejected)?;
            *record = next.next.clone();
            Ok(next)
        })
    }
    fn observe<'a>(
        &'a self,
        _: &'a AttemptContext,
        revision: u64,
        source: &'a mut dyn ExecutionActivitySource,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>> {
        Box::pin(async move {
            self.available().await?;
            let mut record = self.record.lock().expect("record");
            if revision != record.revision() {
                return Err(rejected(LivenessError::RevisionMismatch));
            }
            let ExecutionRecord::Checkpoint(checkpoint) = &*record else {
                return Err(rejected(LivenessError::InvalidPhase));
            };
            let captured = source
                .sample(checkpoint, self.clock.as_ref())
                .map_err(rejected)?;
            let next = record
                .observe(revision, &captured.batch, captured.cutoff)
                .map_err(rejected)?;
            *record = next.next.clone();
            source
                .acknowledge(&captured.batch, &next.next)
                .map_err(rejected)?;
            Ok(next)
        })
    }
}
#[derive(Clone, Copy)]
pub enum ControlBehavior {
    Confirm,
    Unresponsive,
    WrongConfirmation,
}
pub struct Control {
    binding: ExecutionBinding,
    journal: Arc<Journal>,
    behavior: ControlBehavior,
    pub calls: Mutex<Vec<(StopReason, Phase)>>,
}
impl ExecutionStopControl for Control {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop(
        &self,
        reason: StopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("calls")
                .push((reason, self.journal.phase()));
            match self.behavior {
                ControlBehavior::Confirm => Ok(ExecutionStopConfirmation {
                    binding: self.binding,
                }),
                ControlBehavior::Unresponsive => std::future::pending().await,
                ControlBehavior::WrongConfirmation => {
                    let mut binding = self.binding;
                    binding.producer_generation =
                        std::num::NonZeroU64::new(2).expect("wrong generation");
                    Ok(ExecutionStopConfirmation { binding })
                }
            }
        })
    }
}
pub struct Fixture {
    pub owner: AttemptContext,
    pub clock: Arc<Clock>,
    pub journal: Arc<Journal>,
    pub control: Arc<Control>,
    pub sink: Arc<dyn ExecutionActivitySink>,
    pub reactor: ExecutionReactor,
}
pub fn fixture(behavior: ControlBehavior) -> Fixture {
    let rig = policy_support::Rig::new();
    let clock = Arc::new(Clock {
        base: tokio::time::Instant::now(),
        epoch: rig.epoch,
        suspend_ms: AtomicU64::new(0),
    });
    let checkpoint = rig
        .starting()
        .checkpoint(clock.sample().expect("clock"))
        .expect("checkpoint");
    let (sink, inbox) = activity_channel(&checkpoint, clock.clone()).expect("channel");
    let record: ExecutionRecord = checkpoint.into();
    let journal = Arc::new(Journal {
        record: Mutex::new(record.clone()),
        fail: AtomicBool::new(false),
        stall: AtomicBool::new(false),
        clock: clock.clone(),
    });
    let control = Arc::new(Control {
        binding: rig.binding,
        journal: journal.clone(),
        behavior,
        calls: Mutex::new(Vec::new()),
    });
    let owner = AttemptContext {
        attempt_id: rig.binding.owner_attempt_id,
        run_id: rig.binding.run_id,
        attempt_number: AttemptNumber::new(1).expect("attempt"),
        worker_id: WorkerId::new("reactor-fixture").expect("worker"),
        lease_id: LeaseId::generate(),
        fencing_token: rig.binding.owner_fence,
        lease_expires_at: UtcTimestamp::from_unix_millis(1_000_000),
    };
    let reactor = ExecutionReactor::new(
        journal.clone(),
        clock.clone(),
        owner.clone(),
        record,
        Box::new(inbox),
        control.clone(),
    )
    .expect("bound reactor");
    Fixture {
        owner,
        clock,
        journal,
        control,
        sink,
        reactor,
    }
}
pub async fn advance(ms: u64) {
    tokio::time::advance(Duration::from_millis(ms)).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
}
