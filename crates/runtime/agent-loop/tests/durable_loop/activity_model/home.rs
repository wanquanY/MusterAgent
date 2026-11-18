use super::*;
use musteragent_execution_liveness::*;
use musteragent_identity::HostClockEpochId;
use musteragent_runtime_contract::execution_liveness::{ExecutionBinding, ExecutionScope as Scope};

pub struct Clock {
    base: tokio::time::Instant,
    epoch: HostClockEpochId,
}
impl Clock {
    pub fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
            epoch: HostClockEpochId::generate(),
        }
    }
}
impl LivenessHomeClock for Clock {
    fn sample(&self) -> Result<ClockSample, LivenessError> {
        Ok(ClockSample {
            monotonic: LocalInstant {
                epoch: self.epoch,
                elapsed: self.base.elapsed(),
            },
            utc: UtcTimestamp::from_unix_millis(
                i64::try_from(self.base.elapsed().as_millis()).expect("test time"),
            ),
        })
    }
}
pub struct Home {
    pub record: Mutex<Option<ExecutionRecord>>,
    pub clock: Arc<Clock>,
    pub policy:
        Mutex<Arc<musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy>>,
}
impl Home {
    pub fn begin(&self, iteration: &ModelIteration) {
        let invocation = &iteration.request.invocation;
        let binding = ExecutionBinding {
            tenant_id: invocation.run_context.tenant_id,
            run_id: invocation.run_context.run_id,
            scope: Scope::Invocation(iteration.invocation_id),
            operation_identity: iteration.request_ref.sha256,
            owner_attempt_id: invocation.attempt_context.attempt_id,
            owner_fence: invocation.attempt_context.fencing_token,
            producer_generation: NonZeroU64::MIN,
        };
        let admission = ExecutionAdmission {
            binding,
            policy: lock(&self.policy).clone(),
            business_deadline: None,
        };
        *lock(&self.record) = Some(
            admission
                .into_checkpoint(self.clock.sample().expect("clock"))
                .expect("admission")
                .into(),
        );
    }
    pub fn phase(&self) -> Phase {
        let guard = lock(&self.record);
        match guard.as_ref().expect("admitted") {
            ExecutionRecord::Checkpoint(c) => c.state().phase(),
            _ => panic!("checkpoint"),
        }
    }
    pub fn close(&self) {
        let mut guard = lock(&self.record);
        let record = guard.as_ref().expect("admitted");
        if !record.is_closed() {
            *guard = Some(
                record
                    .reduce(
                        record.revision(),
                        RecordMutation::Event(Event::TerminalObserved),
                        self.clock.sample().expect("clock"),
                    )
                    .expect("terminal")
                    .next,
            );
        }
    }
}
fn rejection(error: LivenessError) -> LivenessJournalError {
    LivenessJournalError {
        code: if error == LivenessError::RevisionMismatch {
            ErrorCode::IdempotencyConflict
        } else {
            ErrorCode::InvalidCommand
        },
        retryable: false,
        rejection: Some(error),
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
            let value = lock(&self.record).clone();
            if let Some(value) = &value {
                let b = value.binding();
                if b.scope != scope
                    || b.owner_attempt_id != owner.attempt_id
                    || b.owner_fence != owner.fencing_token
                {
                    return Err(rejection(LivenessError::BindingMismatch));
                }
            }
            Ok(value)
        })
    }
    fn admit<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: ExecutionAdmission,
    ) -> ApplicationFuture<'a, Result<ExecutionRecord, LivenessJournalError>> {
        Box::pin(async { panic!("Loop must use original model admission") })
    }
    fn mutate<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: Scope,
        revision: u64,
        mutation: RecordMutation,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>> {
        Box::pin(async move {
            let mut guard = lock(&self.record);
            let next = guard
                .as_ref()
                .expect("admitted")
                .reduce(revision, mutation, self.clock.sample().expect("clock"))
                .map_err(rejection)?;
            *guard = Some(next.next.clone());
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
            let mut guard = lock(&self.record);
            let record = guard.as_ref().expect("admitted");
            let ExecutionRecord::Checkpoint(c) = record else {
                panic!("checkpoint")
            };
            let captured = source.sample(c, self.clock.as_ref()).map_err(rejection)?;
            let next = record
                .observe(revision, &captured.batch, captured.cutoff)
                .map_err(rejection)?;
            *guard = Some(next.next.clone());
            source
                .acknowledge(&captured.batch, &next.next)
                .map_err(rejection)?;
            Ok(next)
        })
    }
}
pub struct Journal {
    pub inner: Arc<FixtureJournal>,
    pub home: Arc<Home>,
    pub delay: Duration,
    pub reads: Arc<AtomicUsize>,
    pub first_commit_reads: AtomicUsize,
    pub tool_commit_failure: Mutex<Option<ToolCallId>>,
    pub tool_recovery: Mutex<Option<Arc<super::tool_recovery_gate::OriginalToolFacts>>>,
}
macro_rules! forward {
    ($name:ident($($arg:ident: $ty:ty),*) -> $result:ty) => {
        fn $name(&self, attempt: &AttemptContext, $($arg: $ty),*) -> ApplicationFuture<'_, Result<$result, LoopPortError>> {
            self.inner.$name(attempt, $($arg),*)
        }
    }
}
impl AgentLoopJournal for Journal {
    forward!(recovery_budget() -> musteragent_loop::RecoveryBudget);
    forward!(load_checkpoint() -> DurableLoopCheckpoint);
    forward!(load_model_observation(invocation_id: InvocationId) -> ModelInvocationObservation);
    forward!(recover_checkpoint(checkpoint: DurableLoopCheckpoint, constraints: musteragent_loop::ModelRecoveryConstraints) -> RecoveredLoopState);
    forward!(mark_model_dispatched(invocation_id: InvocationId) -> DurableLoopCheckpoint);
    forward!(stage_model_completion(completion: ModelCompletionCommit) -> DurableLoopCheckpoint);
    fn commit_tool_result(
        &self,
        attempt: &AttemptContext,
        result: ToolResultCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        if lock(&self.tool_commit_failure).as_ref() == Some(&result.call_id) {
            return Box::pin(async {
                Err(LoopPortError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: false,
                })
            });
        }
        self.inner.commit_tool_result(attempt, result)
    }
    forward!(wait_for_reconciliation(invocation_id: InvocationId, observed_at: UtcTimestamp, resume_not_before: UtcTimestamp) -> ());
    fn completion_readiness(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::RunCompletionReadiness, LoopPortError>>
    {
        if let Some(facts) = lock(&self.tool_recovery).as_ref()
            && let Err(error) = facts.require_permit(&self.inner)
        {
            return Box::pin(async move { Err(error) });
        }
        self.inner.completion_readiness(attempt)
    }
    forward!(complete() -> RunCompletionDisposition);
    forward!(fail(code: ErrorCode, retryable: bool, retry_after_ms: Option<u64>, recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot) -> DurableLoopCheckpoint);
    forward!(interrupt_external_outcome(reconciliation_ref: OpaqueExternalRef) -> DurableLoopCheckpoint);
    fn begin_model(
        &self,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        recovery: musteragent_runtime_model::ProviderRecoveryCapability,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        let iteration = iteration.clone();
        Box::pin(async move {
            if let Some(facts) = lock(&self.tool_recovery).as_ref() {
                facts.require_permit(&self.inner)?;
            }
            let result = self
                .inner
                .begin_model(&attempt, &iteration, recovery)
                .await?;
            self.home.begin(&iteration);
            Ok(result)
        })
    }
    fn append_model_events(
        &self,
        attempt: &AttemptContext,
        batch: ModelEventBatch,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            if matches!(self.home.phase(), Phase::Stopping { .. } | Phase::Closed(_)) {
                return Err(port_error(ErrorCode::IdempotencyConflict, false));
            }
            let _ = self.first_commit_reads.compare_exchange(
                0,
                self.reads.load(Ordering::SeqCst),
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            self.inner.append_model_events(&attempt, batch).await
        })
    }
    fn finish_model(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            if matches!(self.home.phase(), Phase::Stopping { .. } | Phase::Closed(_)) {
                return Err(port_error(ErrorCode::IdempotencyConflict, false));
            }
            let result = self.inner.finish_model(&attempt, completion).await?;
            self.home.close();
            Ok(result)
        })
    }
    fn finish_failed_model(
        &self,
        attempt: &AttemptContext,
        commit: ModelFailureCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            if commit.resolution == ModelFailureResolution::ExternalOutcomeUnknown {
                assert!(matches!(
                    self.home.phase(),
                    Phase::Closed(Closure::StopUnconfirmed(_))
                ));
                assert_eq!(
                    self.inner
                        .load_model_observation(&attempt, commit.observation.invocation_id)
                        .await?,
                    commit.observation
                );
                let mut checkpoint = lock(&self.inner.checkpoint);
                checkpoint.phase = LoopPhase::Interrupted;
                return Ok(checkpoint.clone());
            }
            if matches!(
                self.home.phase(),
                Phase::Starting { .. } | Phase::Draining { .. }
            ) {
                // Native terminal errors and local preflight failures close in
                // the original model result transaction as in the real store.
                self.home.close();
            }
            assert!(matches!(self.home.phase(), Phase::Closed(_)));
            self.inner.finish_failed_model(&attempt, commit).await
        })
    }
    fn finish_rejected_model_action(
        &self,
        attempt: &AttemptContext,
        commit: ModelActionRejectionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let native_terminal = matches!(self.home.phase(), Phase::Draining { .. });
        assert!(
            native_terminal
                || matches!(
                    self.home.phase(),
                    Phase::Closed(Closure::StopConfirmed(_) | Closure::StopUnconfirmed(_))
                )
        );
        let attempt = attempt.clone();
        Box::pin(async move {
            let result = self
                .inner
                .finish_rejected_model_action(&attempt, commit)
                .await?;
            // A completed provider response owns a native exit, not a
            // requested stop. Close with the same model-result commit.
            if native_terminal {
                self.home.close();
            }
            Ok(result)
        })
    }
    fn acknowledge_cancellation(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        if observation.is_some() {
            assert!(matches!(
                self.home.phase(),
                Phase::Closed(
                    Closure::StopConfirmed(_)
                        | Closure::StopUnconfirmed(_)
                        | Closure::TerminalObserved
                )
            ));
        }
        self.inner.acknowledge_cancellation(attempt, observation)
    }
    fn acknowledge_coordination_interrupt(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        if observation.is_some() {
            assert!(matches!(
                self.home.phase(),
                Phase::Closed(
                    Closure::StopConfirmed(_)
                        | Closure::StopUnconfirmed(_)
                        | Closure::TerminalObserved
                )
            ));
        }
        self.inner
            .acknowledge_coordination_interrupt(attempt, observation)
    }
}
