//! Compression retains its original slot/model and Conversation commit lane.
use super::*;
use musteragent_execution_liveness::LivenessJournalError;
use musteragent_runtime_model::{
    AdmittedModelInvocation, SupervisedModelAdmission, SupervisedModelCompletion,
    SupervisedModelStart,
};

impl PostgresContextCompactionSlotStore {
    #[must_use]
    pub fn with_execution_liveness(
        mut self,
        journal: Arc<crate::PostgresFencedExecutionLivenessJournal>,
    ) -> Self {
        self.liveness = Some(journal);
        self
    }

    pub async fn begin_supervised(
        &self,
        request: &ContextCompactionRequest,
        invocation: &ModelInvocation,
    ) -> Result<SupervisedModelStart, PostgresContextCompactionSlotError> {
        validate_new_invocation(request, invocation)?;
        let admission = SupervisedModelAdmission {
            context: request.invocation.clone(),
            admission: AdmittedModelInvocation {
                parent_invocation_id: None,
                invocation: invocation.clone(),
                route: request.model.route.clone(),
            },
        };
        admission
            .validate()
            .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)?;
        let journal = self
            .liveness
            .as_ref()
            .ok_or(PostgresContextCompactionSlotError::InvalidContract)?;
        let (mut tx, state) =
            crate::commit_lane::acquire(&self.database, &request.invocation.attempt_context)
                .await
                .map_err(|e| {
                    liveness_error(crate::execution_liveness::StoreError::from(e).port())
                })?;
        let row = load_slot_for_update(&mut tx, request)
            .await?
            .ok_or(PostgresContextCompactionSlotError::CorruptState)?;
        verify_slot(&row, request)?;
        let slot = decode_slot(&row, &request.invocation)?;
        if slot.invocation != request.invocation
            || slot.request_ref != invocation.request_ref
            || slot.started_at != invocation.started_at
            || slot.request_hash != invocation.request_hash
        {
            return Err(PostgresContextCompactionSlotError::IdempotencyConflict);
        }
        let duplicate: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.execution_liveness WHERE tenant_id=$1 AND run_id=$2 AND scope_kind=1 AND scope_invocation_id=$3)")
            .bind(invocation.tenant_id.into_uuid()).bind(invocation.run_id.into_uuid()).bind(invocation.invocation_id.into_uuid()).fetch_one(&mut *tx).await?;
        if !duplicate && slot.dispatch_state != ContextCompactionDispatchState::SealedNotDispatched
        {
            return Err(PostgresContextCompactionSlotError::IdempotencyConflict);
        }
        let record = journal
            .start_auxiliary(tx, &state, &admission)
            .await
            .map_err(liveness_error)?;
        Ok(SupervisedModelStart {
            disposition: if duplicate {
                ModelPersistenceDisposition::Duplicate
            } else {
                ModelPersistenceDisposition::Applied
            },
            record,
        })
    }

    pub async fn complete_supervised(
        &self,
        request: SupervisedModelCompletion,
    ) -> Result<ModelPersistenceDisposition, PostgresContextCompactionSlotError> {
        request
            .validate()
            .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)?;
        if request.completion.invocation.purpose != ModelInvocationPurpose::ContextCompaction
            || request.context.parent_invocation_id.is_some()
        {
            return Err(PostgresContextCompactionSlotError::InvalidContract);
        }
        let journal = self
            .liveness
            .as_ref()
            .ok_or(PostgresContextCompactionSlotError::InvalidContract)?;
        let (mut tx, state) =
            crate::commit_lane::acquire(&self.database, &request.context.attempt_context)
                .await
                .map_err(|e| {
                    liveness_error(crate::execution_liveness::StoreError::from(e).port())
                })?;
        let row = sqlx::query("SELECT sealed_deadline_ms,activity_timing,activity_timing_hash,sealed_attempt_id,correlation_id,causation_id,dispatch_state FROM runtime.context_compaction_slots WHERE tenant_id=$1 AND run_id=$2 AND invocation_id=$3 FOR UPDATE")
            .bind(request.context.run_context.tenant_id.into_uuid()).bind(request.context.run_context.run_id.into_uuid()).bind(request.context.invocation_id.into_uuid()).fetch_one(&mut *tx).await?;
        let timing = musteragent_persistence_codec::decode_invocation_timing_columns(
            musteragent_persistence_codec::EncodedInvocationTiming {
                legacy_deadline_ms: row.try_get("sealed_deadline_ms")?,
                activity: row.try_get("activity_timing")?,
                activity_hash: row.try_get("activity_timing_hash")?,
            },
        )
        .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?;
        let outcome = &request.completion.invocation;
        if timing != request.context.timing
            || row.try_get::<uuid::Uuid, _>("sealed_attempt_id")?
                != request.context.attempt_context.attempt_id.into_uuid()
            || row.try_get::<uuid::Uuid, _>("correlation_id")?
                != request.context.correlation_id.into_uuid()
            || row.try_get::<uuid::Uuid, _>("causation_id")?
                != request.context.causation_id.into_uuid()
            || (row.try_get::<i16, _>("dispatch_state")? != DISPATCHED
                && (outcome.status != ModelInvocationStatus::Failed
                    || outcome.usage.is_some()
                    || outcome.provider_request_ref.is_some()
                    || outcome.response_ref.is_some()))
        {
            return Err(PostgresContextCompactionSlotError::IdempotencyConflict);
        }
        let record = journal
            .validate_auxiliary_completion(&mut tx, &state, &request)
            .await
            .map_err(liveness_error)?;
        let disposition = self
            .model
            .complete_in_transaction(&mut tx, request.completion, true)
            .await?;
        journal
            .finish_auxiliary(tx, &state, &request.context, record)
            .await
            .map_err(liveness_error)?;
        Ok(disposition)
    }
}

pub(super) fn liveness_error(error: LivenessJournalError) -> PostgresContextCompactionSlotError {
    use musteragent_runtime_contract::ErrorCode;
    match error.code {
        ErrorCode::AttemptLeaseStale => PostgresContextCompactionSlotError::FenceConflict,
        ErrorCode::IdempotencyConflict => PostgresContextCompactionSlotError::IdempotencyConflict,
        ErrorCode::DependencyUnavailable => {
            PostgresContextCompactionSlotError::SupervisionUnavailable
        }
        _ => PostgresContextCompactionSlotError::InvalidContract,
    }
}
