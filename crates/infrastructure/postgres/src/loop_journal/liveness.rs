//! Supervision shares the original Model Journal transaction and authority.
use super::*;
use crate::execution_liveness::LifecycleChange;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_execution_liveness::{LivenessHomeClock, LivenessJournalError};
use musteragent_identity::InvocationId;
use musteragent_ledger::AppendRequest;

impl PostgresAgentLoopJournal {
    pub(super) async fn recover_model_liveness(
        &self,
        attempt: &AttemptContext,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let state = self.load_state(attempt).await?;
        if let Some(journal) = self.model_liveness(&state)?
            && let LoopPhase::ModelInFlight { invocation_id } = state.checkpoint.phase
            && state
                .model_attempt_id
                .is_some_and(|original| original != attempt.attempt_id)
            && state.provider_dispatched
            && state.terminal_stop_reason.is_none()
        {
            journal
                .recover_orphaned_model(attempt, invocation_id)
                .await?;
        }
        Ok(())
    }

    /// Runtime supervision uses the exact Journal instance and Home clock that
    /// own model admission; callers cannot substitute another clock epoch.
    pub fn execution_supervision(
        &self,
        activities: Arc<dyn musteragent_execution_liveness::ExecutionActivityFactory>,
    ) -> Option<musteragent_loop::ExecutionSupervision> {
        self.liveness
            .as_ref()
            .map(|journal| journal.supervision(activities))
    }

    #[must_use]
    pub fn with_execution_liveness(
        mut self,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
        clock: Arc<dyn LivenessHomeClock>,
    ) -> Self {
        self.liveness = Some(
            crate::PostgresFencedExecutionLivenessJournal::new(
                self.ledger.database().clone(),
                snapshots,
                self.encoder.clone(),
                self.decoder.clone(),
                clock,
            )
            .with_application_catalogs(self.application_catalogs.clone()),
        );
        self
    }
    pub(super) fn model_liveness(
        &self,
        state: &JournalState,
    ) -> Result<Option<&crate::PostgresFencedExecutionLivenessJournal>, PostgresAgentLoopJournalError>
    {
        if !(musteragent_runtime_snapshot::runtime_snapshot_has_activity_liveness(
            state.snapshot_schema.as_str(),
        ) || matches!(
            state.snapshot_schema.as_str(),
            musteragent_runtime_memory::MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION
        )) {
            return Ok(None);
        }
        self.liveness.as_ref().map(Some).ok_or_else(|| {
            PostgresAgentLoopJournalError::Liveness(LivenessJournalError {
                code: ErrorCode::UnsupportedSchemaVersion,
                retryable: false,
                rejection: None,
            })
        })
    }
}
impl PostgresAgentLoopJournal {
    pub(super) async fn validate_model_completion_link(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &JournalState,
        attempt: &AttemptContext,
        invocation: InvocationId,
        content: musteragent_identity::ContentId,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let Some(journal) = self.model_liveness(state)? else {
            return Ok(());
        };
        let live = crate::commit_lane::CommitLaneState {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            activity_id: state.activity_id,
            home_epoch: state.home_epoch,
            last_run_sequence: state.last_run_sequence,
            conversation_offset: state.conversation_offset,
        };
        journal
            .validate_completion_link(tx, &live, attempt, invocation, content)
            .await?;
        Ok(())
    }
    pub(super) async fn validate_model_activity(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &JournalState,
        attempt: &AttemptContext,
        invocation: InvocationId,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let Some(journal) = self.model_liveness(state)? else {
            return Ok(());
        };
        let live = crate::commit_lane::CommitLaneState {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            activity_id: state.activity_id,
            home_epoch: state.home_epoch,
            last_run_sequence: state.last_run_sequence,
            conversation_offset: state.conversation_offset,
        };
        journal
            .validate_model_activity(tx, &live, attempt, invocation)
            .await?;
        Ok(())
    }
    pub(super) async fn stage_recovered_terminal(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &JournalState,
        attempt: &AttemptContext,
        recovery: &musteragent_loop::RecoverableModelInvocation,
    ) -> Result<LifecycleChange, PostgresAgentLoopJournalError> {
        let journal = self
            .model_liveness(state)?
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let live = crate::commit_lane::CommitLaneState {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            activity_id: state.activity_id,
            home_epoch: state.home_epoch,
            last_run_sequence: state.last_run_sequence,
            conversation_offset: state.conversation_offset,
        };
        Ok(journal
            .prepare_recovered_model_terminal(tx, &live, attempt, recovery)
            .await?)
    }

    pub(super) async fn stage_model_begin_liveness(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &JournalState,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        append: &mut AppendRequest,
    ) -> Result<Option<LifecycleChange>, PostgresAgentLoopJournalError> {
        let Some(journal) = self.model_liveness(state)? else {
            return Ok(None);
        };
        let live = crate::commit_lane::CommitLaneState {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            activity_id: state.activity_id,
            home_epoch: state.home_epoch,
            last_run_sequence: state.last_run_sequence,
            conversation_offset: state.conversation_offset,
        };
        let change = journal
            .prepare_model_start(
                tx,
                &live,
                attempt,
                &iteration.request.invocation,
                iteration.request_ref.sha256,
            )
            .await?;
        change.append_to(append, self.encoder.as_ref())?;
        Ok(Some(change))
    }
    pub(super) async fn require_unconfirmed_model_stop(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &JournalState,
        attempt: &AttemptContext,
        invocation: InvocationId,
    ) -> Result<musteragent_runtime_contract::OpaqueExternalRef, PostgresAgentLoopJournalError>
    {
        let journal = self
            .model_liveness(state)?
            .ok_or(PostgresAgentLoopJournalError::CheckpointConflict)?;
        let live = crate::commit_lane::CommitLaneState {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            activity_id: state.activity_id,
            home_epoch: state.home_epoch,
            last_run_sequence: state.last_run_sequence,
            conversation_offset: state.conversation_offset,
        };
        Ok(journal
            .require_unconfirmed_model_stop(tx, &live, attempt, invocation)
            .await?)
    }
    pub(super) async fn stage_model_terminal_liveness(
        &self,
        tx: &mut sqlx::PgConnection,
        state: &JournalState,
        attempt: &AttemptContext,
        invocation: InvocationId,
        allow_confirmed_stop: bool,
        append: &mut AppendRequest,
    ) -> Result<Option<LifecycleChange>, PostgresAgentLoopJournalError> {
        let Some(journal) = self.model_liveness(state)? else {
            return Ok(None);
        };
        let live = crate::commit_lane::CommitLaneState {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            activity_id: state.activity_id,
            home_epoch: state.home_epoch,
            last_run_sequence: state.last_run_sequence,
            conversation_offset: state.conversation_offset,
        };
        let change = journal
            .prepare_model_terminal(tx, &live, attempt, invocation, allow_confirmed_stop)
            .await?;
        if let Some(change) = &change {
            change.append_to(append, self.encoder.as_ref())?;
        }
        Ok(change)
    }
}

impl PostgresAgentLoopJournal {
    pub(super) async fn seal_model_completion(
        &self,
        mut tx: crate::GovernedPostgresTransaction<'static>,
        state: &JournalState,
        attempt: &AttemptContext,
        invocation: InvocationId,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let Some(journal) = self.model_liveness(state)? else {
            tx.commit().await?;
            return Ok(());
        };
        let live = crate::commit_lane::CommitLaneState {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            activity_id: state.activity_id,
            home_epoch: state.home_epoch,
            last_run_sequence: state.last_run_sequence,
            conversation_offset: state.conversation_offset,
        };
        let Some(change) = journal
            .prepare_model_terminal(&mut tx, &live, attempt, invocation, false)
            .await?
        else {
            tx.commit().await?;
            return Ok(());
        };
        let record = self.record(
            state,
            attempt,
            RecordSpec {
                run_sequence: state.next_run_sequence(1)?,
                occurred_at: database_now_ms(&mut tx).await?,
                producer: Producer::Worker,
                payload: LedgerPayload::DomainEvent(change.event()),
                record_id: deterministic_record_id(
                    invocation,
                    u32::MAX,
                    0,
                    b"model-result-sealed",
                )?,
            },
        )?;
        let append =
            state.append_request(format!("model-result-sealed:{invocation}"), vec![record])?;
        let mut prepared = match self.ledger.begin_append_in_transaction(tx, &append).await? {
            BeginAppend::Ready(value) => value,
            BeginAppend::Duplicate(_) => {
                return Err(PostgresAgentLoopJournalError::CheckpointConflict);
            }
        };
        change.persist(prepared.transaction_mut()).await?;
        advance_run_sequence(
            prepared.transaction_mut(),
            attempt.run_id,
            state.next_run_sequence(1)?,
        )
        .await?;
        prepared.commit(&append).await?;
        Ok(())
    }
}
