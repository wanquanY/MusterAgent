//! Canonical Tool terminal commit shared by Task start, goal and control.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_ledger::{DomainEvent, LedgerPayload, ProposedRecord};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_tool::ToolEvent;
impl PostgresWorkItemStartStore {
    pub(super) async fn finish_tool(
        &self,
        mut tx: crate::GovernedPostgresTransaction<'static>,
        state: crate::commit_lane::CommitLaneState,
        attempt: &AttemptContext,
        request: &ResolveWorkItemStart,
        result: &CapabilityResult,
        at: UtcTimestamp,
    ) -> Result<(), DialogueStoreError> {
        let bytes = self
            .encoder
            .encode(LedgerPayload::DomainEvent(DomainEvent::Tool(
                ToolEvent::InvocationSucceeded {
                    invocation_id: request.invocation_id,
                    result_ref: result.result_ref.clone(),
                    result_hash: result.result_ref.sha256,
                    provider_request_ref: None,
                    authorized_content: Vec::new(),
                },
            )));
        let record = ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .map_err(|_| DialogueStoreError::Corrupt)?,
            record_id: LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                &request.invocation_id.into_uuid(),
                b"work-item-tool-terminal@1",
            ))
            .map_err(|_| DialogueStoreError::Corrupt)?,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(state.activity_id),
            run_id: Some(attempt.run_id),
            run_seq: Some(
                RunSeq::new(
                    state
                        .next_sequence(1)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                )
                .map_err(|_| DialogueStoreError::Corrupt)?,
            ),
            attempt_id: Some(attempt.attempt_id),
            participant_id: Some(request.run_context.executor_participant_id),
            producer: Producer::Worker,
            occurred_at: at,
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&bytes),
            payload: bytes,
        };
        let mut append = state
            .append(
                format!("work-item-tool-terminal:{}", request.invocation_id),
                vec![record],
            )
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let liveness = crate::execution_liveness::stage_tool_terminal(
            &mut tx,
            self.clock.as_deref(),
            self.encoder.as_ref(),
            attempt,
            request.invocation_id,
            &mut append,
        )
        .await
        .map_err(|_| DialogueStoreError::Corrupt)?;
        start::fence(&mut tx, attempt, state).await?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(tx, &append)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?
        {
            crate::ledger::BeginAppend::Ready(p) => p,
            crate::ledger::BeginAppend::Duplicate(_) => {
                return Err(DialogueStoreError::IdempotencyConflict);
            }
        };
        crate::artifact::complete_tool(
            prepared.transaction_mut(),
            &self.keys,
            state.tenant_id,
            request.invocation_id,
            result,
            at,
            true,
        )
        .await
        .map_err(|_| DialogueStoreError::Corrupt)?;
        crate::tool_security_audit::append_succeeded_in_transaction(
            &self.audit,
            prepared.transaction_mut(),
            state.tenant_id,
            request.invocation_id,
        )
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
        if let Some(change) = liveness {
            change
                .persist(prepared.transaction_mut())
                .await
                .map_err(|_| DialogueStoreError::Corrupt)?;
        }
        crate::resource_commit::advance_run(
            prepared.transaction_mut(),
            state.tenant_id,
            attempt.run_id,
            state
                .next_sequence(append.records.len() as u64)
                .map_err(|_| DialogueStoreError::Corrupt)?,
        )
        .await
        .map_err(|_| DialogueStoreError::RevisionConflict)?;
        prepared
            .commit(&append)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        Ok(())
    }
}
