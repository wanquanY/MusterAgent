//! Composition of the original Home Journal and its trusted observation clock.
use super::*;
use musteragent_execution_liveness::{ExecutionActivityFactory, ExecutionActivitySink};
use musteragent_runtime_contract::execution_liveness::{ExecutionLivenessPolicy, ExecutionScope};

#[derive(Clone)]
pub struct ExecutionSupervision {
    journal: Arc<dyn FencedExecutionLivenessJournal<Owner = AttemptContext>>,
    clock: Arc<dyn LivenessHomeClock>,
    activities: Arc<dyn ExecutionActivityFactory>,
    host_fence: Option<Arc<dyn HostExecutionFence>>,
}
impl ExecutionSupervision {
    /// Close lost observation using the same durable recovery protocol used by
    /// Model recovery and cancellation; no physical work is redispatched.
    pub async fn close_unobserved(
        &self,
        owner: &AttemptContext,
        scope: ExecutionScope,
        policy: &ExecutionLivenessPolicy,
    ) -> Result<(), ExecutionSupervisionError> {
        crate::close_unobserved_execution(
            self.journal.as_ref(),
            self.clock.as_ref(),
            owner,
            scope,
            policy,
        )
        .await
    }

    /// Admit one retained-result lookup against the original operation. An
    /// already issued ticket is reused; another owner cannot renew its window.
    pub async fn recovery_query_window(
        &self,
        owner: &AttemptContext,
        scope: ExecutionScope,
        policy: &ExecutionLivenessPolicy,
    ) -> Result<Option<Duration>, ExecutionSupervisionError> {
        use musteragent_execution_liveness::RecordMutation;
        let budget = policy.windows().observation_request;
        let mut record = tokio::time::timeout(budget, self.journal.load(owner, scope))
            .await
            .map_err(|_| ExecutionSupervisionError::JournalRequestExpired)??
            .ok_or(LivenessError::InvalidPhase)?;
        if record.binding().scope != scope
            || record.binding().run_id != owner.run_id
            || record.policy() != policy
        {
            return Err(LivenessError::BindingMismatch.into());
        }
        if let ExecutionRecord::Checkpoint(checkpoint) = &record {
            if !matches!(
                checkpoint.state().phase(),
                Phase::Starting { .. } | Phase::Running { .. }
            ) {
                return Ok(None);
            }
            let mut rebound = record.binding();
            rebound.owner_attempt_id = owner.attempt_id;
            rebound.owner_fence = owner.fencing_token;
            record = tokio::time::timeout(
                budget,
                self.journal.mutate(
                    owner,
                    scope,
                    record.revision(),
                    RecordMutation::BeginRecovery {
                        rebound,
                        nonce: fresh_recovery_nonce(),
                    },
                ),
            )
            .await
            .map_err(|_| ExecutionSupervisionError::JournalRequestExpired)??
            .next;
        }
        let ExecutionRecord::Recovering(ticket) = record else {
            return Err(LivenessError::InvalidPhase.into());
        };
        let now = self.clock.sample()?.monotonic;
        if ticket.binding().owner_attempt_id != owner.attempt_id
            || ticket.binding().owner_fence != owner.fencing_token
            || now.epoch != ticket.due().epoch
        {
            return Ok(None);
        }
        let remaining = ticket.due().elapsed.saturating_sub(now.elapsed);
        Ok((!remaining.is_zero()).then_some(remaining.min(budget)))
    }

    pub fn new(
        journal: Arc<dyn FencedExecutionLivenessJournal<Owner = AttemptContext>>,
        clock: Arc<dyn LivenessHomeClock>,
        activities: Arc<dyn ExecutionActivityFactory>,
    ) -> Self {
        Self {
            journal,
            clock,
            activities,
            host_fence: None,
        }
    }

    /// Native composition supplies its original membership/physical effect
    /// owner. This does not change Cloud/Hosted Journal or release semantics.
    pub fn with_host_fence(mut self, fence: Arc<dyn HostExecutionFence>) -> Self {
        self.host_fence = Some(fence);
        self
    }

    /// Load an already committed original begin. No admission or replacement
    /// clock is created here. The frozen request policy owns this short read's
    /// budget; a missing/mismatched record cannot proceed to provider I/O.
    pub async fn load(
        &self,
        owner: &AttemptContext,
        scope: ExecutionScope,
        policy: &ExecutionLivenessPolicy,
    ) -> Result<ExecutionRecord, ExecutionSupervisionError> {
        let record = tokio::time::timeout(
            policy.windows().observation_request,
            self.journal.load(owner, scope),
        )
        .await
        .map_err(|_| ExecutionSupervisionError::JournalRequestExpired)??
        .ok_or(LivenessError::InvalidPhase)?;
        let binding = record.binding();
        if binding.scope != scope
            || binding.run_id != owner.run_id
            || binding.owner_attempt_id != owner.attempt_id
            || binding.owner_fence != owner.fencing_token
            || record.policy() != policy
        {
            return Err(LivenessError::BindingMismatch.into());
        }
        let ExecutionRecord::Checkpoint(checkpoint) = &record else {
            return Err(LivenessError::InvalidPhase.into());
        };
        self.clock
            .sample()?
            .monotonic
            .since(checkpoint.saved_at().monotonic)?;
        Ok(record)
    }

    /// Observe a retained closed call through the current Run owner. This is a
    /// read-only result lookup, not permission to resume or redispatch the old
    /// physical generation. Its clock epoch and owner may predate recovery.
    pub async fn closed_outcome(
        &self,
        owner: &AttemptContext,
        scope: ExecutionScope,
        policy: &ExecutionLivenessPolicy,
    ) -> Result<Option<Closure>, ExecutionSupervisionError> {
        let record = tokio::time::timeout(
            policy.windows().observation_request,
            self.journal.load(owner, scope),
        )
        .await
        .map_err(|_| ExecutionSupervisionError::JournalRequestExpired)??
        .ok_or(LivenessError::InvalidPhase)?;
        if record.binding().scope != scope
            || record.binding().run_id != owner.run_id
            || record.policy() != policy
        {
            return Err(LivenessError::BindingMismatch.into());
        }
        Ok(match record {
            ExecutionRecord::Checkpoint(checkpoint) => match checkpoint.state().phase() {
                Phase::Closed(closure) => Some(closure),
                _ => None,
            },
            ExecutionRecord::Recovering(_) => None,
        })
    }

    /// Attach a resource owner to the exact begin/recovery record. The Runtime
    /// retains the inbox and passes only its bound sink to trusted consumers.
    pub fn attach(
        &self,
        owner: AttemptContext,
        record: ExecutionRecord,
        control: Arc<dyn ExecutionStopControl>,
    ) -> Result<(ExecutionReactor, Arc<dyn ExecutionActivitySink>), LivenessError> {
        let ExecutionRecord::Checkpoint(checkpoint) = &record else {
            return Err(LivenessError::InvalidPhase);
        };
        let channel = self.activities.open(checkpoint, self.clock.clone())?;
        let reactor = ExecutionReactor::new(
            self.journal.clone(),
            self.clock.clone(),
            owner,
            record,
            channel.source,
            control,
        )?;
        Ok((
            reactor.with_host_fence(self.host_fence.clone()),
            channel.sink,
        ))
    }
}

pub(super) fn fresh_recovery_nonce() -> musteragent_content_ref::Sha256 {
    musteragent_content_ref::Sha256::digest(
        musteragent_identity::InvocationId::generate()
            .into_uuid()
            .as_bytes(),
    )
}
