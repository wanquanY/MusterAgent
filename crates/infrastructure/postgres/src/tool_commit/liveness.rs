//! Liveness is composed into the Tool owner's existing transaction.
use super::*;
use crate::execution_liveness::LifecycleChange;
use musteragent_execution_liveness::LivenessHomeClock;
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;

impl PostgresFencedToolInvocationJournal {
    /// Composition supplies the same process-owned Home clock to observers and
    /// every Journal. Legacy-only constructors reject activity admission.
    #[must_use]
    pub fn with_execution_liveness(
        mut self,
        snapshots: Arc<dyn musteragent_context::RuntimeSnapshotDecoder>,
        clock: Arc<dyn LivenessHomeClock>,
    ) -> Self {
        self.receipt_snapshots = Some(snapshots.clone());
        self.liveness = Some(
            crate::PostgresFencedExecutionLivenessJournal::new(
                self.ledger.database().clone(),
                snapshots,
                Arc::clone(&self.encoder),
                Arc::clone(&self.decoder),
                clock,
            )
            .with_application_catalogs(self.application_catalogs.clone()),
        );
        self
    }
    pub(super) async fn prepare_liveness_material(
        &self,
        owner: &AttemptContext,
        key: Option<&musteragent_tool::ToolKey>,
        invocation: Option<InvocationId>,
    ) -> Result<Self, ToolCommitError> {
        let mut prepared = self.clone();
        if let Some(journal) = &self.liveness {
            prepared.liveness = Some(
                journal
                    .prepare_tool_material(owner, key, invocation)
                    .await
                    .map_err(ToolCommitError::Liveness)?,
            );
        }
        Ok(prepared)
    }

    fn liveness_for(
        &self,
        invocation: &ToolInvocation,
    ) -> Result<Option<&crate::PostgresFencedExecutionLivenessJournal>, ToolCommitError> {
        match invocation.timing {
            InvocationExecutionTiming::LegacyFixed { .. } => Ok(None),
            InvocationExecutionTiming::ActivityV1(_) => self
                .liveness
                .as_ref()
                .map(Some)
                .ok_or(ToolCommitError::InvalidTransition),
        }
    }
    pub(super) async fn validate_liveness_intent(
        &self,
        tx: &mut PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: &ToolInvocation,
        retry_contract: musteragent_tool::ToolRetryContract,
    ) -> Result<(), ToolCommitError> {
        if let Some(journal) = self.liveness_for(invocation)? {
            journal
                .validate_tool_intent(tx, state, owner, invocation, retry_contract)
                .await
                .map_err(ToolCommitError::Liveness)?;
        }
        Ok(())
    }
    pub(super) async fn prepare_liveness_start(
        &self,
        tx: &mut PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: &ToolInvocation,
        retry_contract: musteragent_tool::ToolRetryContract,
        mode: ToolStartMode,
    ) -> Result<Option<LifecycleChange>, ToolCommitError> {
        let Some(journal) = self.liveness_for(invocation)? else {
            return Ok(None);
        };
        // Reconciliation/recovery cannot replay a physical operation with a
        // new startup budget. They require an explicit retained-state rebind.
        if mode != ToolStartMode::Initial {
            return Err(ToolCommitError::InvalidTransition);
        }
        journal
            .prepare_tool_start(tx, state, owner, invocation, retry_contract)
            .await
            .map(Some)
            .map_err(ToolCommitError::Liveness)
    }
    pub(super) async fn prepare_liveness_terminal(
        &self,
        tx: &mut PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: &ToolInvocation,
        retry_contract: musteragent_tool::ToolRetryContract,
        outcome: &ToolTerminalOutcome,
    ) -> Result<Option<LifecycleChange>, ToolCommitError> {
        let Some(journal) = self.liveness_for(invocation)? else {
            return Ok(None);
        };
        // Indeterminate is a retained reconciliation obligation, not proof
        // that the physical execution stopped or completed.
        if matches!(outcome, ToolTerminalOutcome::Indeterminate { .. }) {
            return Ok(None);
        }
        journal
            .prepare_tool_terminal(tx, state, owner, invocation, retry_contract, outcome)
            .await
            .map_err(ToolCommitError::Liveness)
    }
    pub(super) async fn prepare_liveness_wait(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &CommitLaneState,
        owner: &AttemptContext,
        invocation: &ToolInvocation,
        wait_record: &ProposedRecord,
    ) -> Result<Option<LifecycleChange>, ToolCommitError> {
        let Some(journal) = self.liveness_for(invocation)? else {
            return Ok(None);
        };
        if wait_record.run_id != Some(owner.run_id)
            || wait_record.attempt_id != Some(owner.attempt_id)
            || Sha256::digest(&wait_record.payload) != wait_record.payload_hash
            || !matches!(
                self.decoder
                    .decode(wait_record.record_kind, &wait_record.payload),
                Ok(LedgerPayload::DomainEvent(DomainEvent::Run(
                    musteragent_run::RunEvent::Waiting { .. }
                )))
            )
        {
            return Err(ToolCommitError::Corrupt);
        }
        let wait = musteragent_runtime_contract::execution_liveness::DurableWaitBinding {
            checkpoint_sequence: wait_record.run_seq.ok_or(ToolCommitError::Corrupt)?,
            identity: wait_record.payload_hash,
        };
        journal
            .prepare_tool_wait(tx, state, owner, invocation, wait)
            .await
            .map_err(ToolCommitError::Liveness)
    }
}
