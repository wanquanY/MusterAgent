use super::*;
use musteragent_execution_liveness::*;
use musteragent_runtime_contract::execution_liveness::{
    ExecutionBinding, ExecutionLivenessPolicy, ExecutionScope as Scope,
};

pub struct HomeClock {
    base: tokio::time::Instant,
    epoch: HostClockEpochId,
}
impl HomeClock {
    fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
            epoch: HostClockEpochId::generate(),
        }
    }
}
impl LivenessHomeClock for HomeClock {
    fn sample(&self) -> Result<ClockSample, LivenessError> {
        Ok(ClockSample {
            monotonic: LocalInstant {
                epoch: self.epoch,
                elapsed: self.base.elapsed(),
            },
            utc: UtcTimestamp::from_unix_millis(self.base.elapsed().as_millis() as i64),
        })
    }
}
pub struct Home {
    records: Mutex<BTreeMap<String, ExecutionRecord>>,
    pub clock: Arc<HomeClock>,
}
impl Home {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(BTreeMap::new()),
            clock: Arc::new(HomeClock::new()),
        }
    }
    pub fn supervision(self: &Arc<Self>) -> ExecutionSupervision {
        ExecutionSupervision::new(
            self.clone(),
            self.clock.clone(),
            Arc::new(musteragent_execution_liveness_io::HomeExecutionActivityFactory),
        )
    }
    pub fn begin(
        &self,
        context: &ExtensionContext,
        id: InvocationId,
        identity: Sha256,
        policy: Arc<ExecutionLivenessPolicy>,
    ) {
        let binding = ExecutionBinding {
            tenant_id: context.run.tenant_id,
            run_id: context.run.run_id,
            scope: Scope::Invocation(id),
            operation_identity: identity,
            owner_attempt_id: context.attempt.attempt_id,
            owner_fence: context.attempt.fencing_token,
            producer_generation: NonZeroU64::MIN,
        };
        let record = ExecutionAdmission {
            binding,
            policy,
            business_deadline: None,
        }
        .into_checkpoint(
            self.clock
                .sample()
                .expect("valid fixture and successful tested operation"),
        )
        .expect("valid fixture and successful tested operation")
        .into();
        assert!(
            lock(&self.records)
                .insert(format!("{:?}", binding.scope), record)
                .is_none()
        );
    }
    pub fn close(&self, id: InvocationId) {
        let mut records = lock(&self.records);
        let Some(record) = records.get_mut(&format!("{:?}", Scope::Invocation(id))) else {
            return;
        };
        if !record.is_closed() {
            *record = record
                .reduce(
                    record.revision(),
                    RecordMutation::Event(Event::TerminalObserved),
                    self.clock
                        .sample()
                        .expect("valid fixture and successful tested operation"),
                )
                .expect("valid fixture and successful tested operation")
                .next;
        }
    }
}
fn error(e: LivenessError) -> LivenessJournalError {
    LivenessJournalError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
        rejection: Some(e),
    }
}
impl FencedExecutionLivenessJournal for Home {
    type Owner = AttemptContext;
    fn load<'a>(
        &'a self,
        owner: &'a AttemptContext,
        scope: Scope,
    ) -> ApplicationFuture<'a, Result<Option<ExecutionRecord>, LivenessJournalError>> {
        Box::pin(async move {
            let record = lock(&self.records).get(&format!("{scope:?}")).cloned();
            if let Some(record) = &record {
                let b = record.binding();
                if b.owner_attempt_id != owner.attempt_id || b.owner_fence != owner.fencing_token {
                    return Err(error(LivenessError::BindingMismatch));
                }
            }
            Ok(record)
        })
    }
    fn admit<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: ExecutionAdmission,
    ) -> ApplicationFuture<'a, Result<ExecutionRecord, LivenessJournalError>> {
        Box::pin(async { Err(error(LivenessError::BindingMismatch)) })
    }
    fn mutate<'a>(
        &'a self,
        _: &'a AttemptContext,
        scope: Scope,
        revision: u64,
        mutation: RecordMutation,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>> {
        Box::pin(async move {
            let mut records = lock(&self.records);
            let record = records
                .get_mut(&format!("{scope:?}"))
                .expect("valid fixture and successful tested operation");
            let transition = record
                .reduce(
                    revision,
                    mutation,
                    self.clock
                        .sample()
                        .expect("valid fixture and successful tested operation"),
                )
                .map_err(error)?;
            *record = transition.next.clone();
            Ok(transition)
        })
    }
    fn observe<'a>(
        &'a self,
        owner: &'a AttemptContext,
        revision: u64,
        source: &'a mut dyn ExecutionActivitySource,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>> {
        Box::pin(async move {
            let mut records = lock(&self.records);
            let record = records
                .values_mut()
                .find(|r| r.binding() == source.binding())
                .expect("matching source binding");
            assert_eq!(record.binding().owner_attempt_id, owner.attempt_id);
            let ExecutionRecord::Checkpoint(checkpoint) = record else {
                return Err(error(LivenessError::BindingMismatch));
            };
            let captured = source
                .sample(checkpoint, self.clock.as_ref())
                .map_err(error)?;
            let transition = record
                .observe(revision, &captured.batch, captured.cutoff)
                .map_err(error)?;
            *record = transition.next.clone();
            source
                .acknowledge(&captured.batch, &transition.next)
                .map_err(error)?;
            Ok(transition)
        })
    }
}
/// Test-only immediate computation has no external resource or work after return.
pub struct LocalControl(pub ExecutionBinding);
impl ExecutionStopControl for LocalControl {
    fn binding(&self) -> ExecutionBinding {
        self.0
    }
    fn stop(
        &self,
        _: musteragent_execution_liveness::StopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async { Ok(ExecutionStopConfirmation { binding: self.0 }) })
    }
}
