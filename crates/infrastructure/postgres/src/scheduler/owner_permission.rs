//! A revoked permission snapshot cannot accept an answer. Settle that passive
//! wait through the original cancellation transaction, including its ledger,
//! Interaction, Tool, checkpoint and public activity. Never restart the model.
use super::*;
use crate::ledger::BeginAppend;
use musteragent_activity::ActivityEvent;
use musteragent_identity::{InvocationId, PrincipalId};
use musteragent_ledger::{CancelMode, CancelRunBatch};
use musteragent_runtime_contract::CancelReason;
use musteragent_tool::ToolEvent;

impl PostgresDispatchStore {
    pub(super) async fn cancel_revoked_dialogue_waits(&self) -> Result<u64, DispatchError> {
        let rows = sqlx::query(
            "SELECT r.tenant_id,r.conversation_id,r.activity_id,r.run_id,r.activity_kind,
                    r.traceparent,r.tracestate,r.home_epoch::text AS home_epoch,
                    r.last_run_sequence::text AS last_run_sequence,
                    stream.last_sequence::text AS conversation_offset,admission.correlation_id,
                    r.wait_interaction_id AS interaction_id,r.wait_invocation_id AS invocation_id,
                    s.owner_id,(SELECT attempt_id FROM runtime.attempts a
                        WHERE a.tenant_id=r.tenant_id AND a.run_id=r.run_id
                        ORDER BY a.attempt_number DESC LIMIT 1) AS last_attempt_id
             FROM runtime.runs r
             JOIN agent.dialogue_sessions s USING(tenant_id,conversation_id)
             JOIN agent.owned_agent_configurations cfg USING(agent_id)
             JOIN event_store.ledger_streams stream USING(tenant_id,conversation_id)
             JOIN event_store.ledger_records admission ON admission.tenant_id=r.tenant_id
                AND admission.run_id=r.run_id AND admission.run_seq=1
             WHERE r.status=3 AND r.wait_reason IN(1,2)
               AND s.permission_revision<cfg.permission_revision
               AND r.execution_home_kind=1 AND r.control_home_kind=1
               AND r.execution_device_id IS NULL AND r.control_device_id IS NULL
               AND r.parent_run_id IS NULL AND r.delegation_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM runtime.leases l WHERE l.run_id=r.run_id)
             ORDER BY r.admitted_at_ms,r.run_id LIMIT $1",
        )
        .bind(CANDIDATE_SCAN_LIMIT)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let mut settled = 0;
        for row in rows {
            let candidate = ExpiredInteractionCandidate {
                identity: decode_identity(&row)?,
                interaction_id: InteractionId::from_uuid(
                    row.try_get("interaction_id").map_err(database_error)?,
                )
                .map_err(|_| contract_error())?,
            };
            let invocation =
                InvocationId::from_uuid(row.try_get("invocation_id").map_err(database_error)?)
                    .map_err(|_| contract_error())?;
            let owner = PrincipalId::from_uuid(row.try_get("owner_id").map_err(database_error)?)
                .map_err(|_| contract_error())?;
            let last_attempt = row
                .try_get::<Option<uuid::Uuid>, _>("last_attempt_id")
                .map_err(database_error)?
                .map(AttemptId::from_uuid)
                .transpose()
                .map_err(|_| contract_error())?;
            let now = database_now(&self.pool).await?;
            let batch =
                self.revoked_wait_batch(&candidate, invocation, owner, last_attempt, now)?;
            match self.settle_revoked_wait(batch, now).await {
                Ok(true) => settled += 1,
                Ok(false) => {}
                Err(error)
                    if is_claim_race(&error)
                        || matches!(error, PostgresExecutionError::RunNotCancellable) => {}
                Err(error) => return Err(map_execution_error(error)),
            }
        }
        Ok(settled)
    }

    async fn settle_revoked_wait(
        &self,
        batch: CancelRunBatch,
        now: UtcTimestamp,
    ) -> Result<bool, PostgresExecutionError> {
        batch.validate().map_err(PostgresExecutionError::Contract)?;
        let mut prepared = match self.ledger.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(_) => return Ok(false),
            BeginAppend::Ready(prepared) => prepared,
        };
        let tx = prepared.transaction_mut();
        // Original lock order: stream -> Run -> account -> configuration.
        sqlx::query("SELECT run_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE")
            .bind(batch.append.tenant_id.into_uuid())
            .bind(batch.run_id.into_uuid())
            .fetch_one(&mut *tx)
            .await?;
        sqlx::query("SELECT space.space_id FROM agent.owner_spaces space
            JOIN agent.owned_agents a USING(space_id)
            JOIN agent.dialogue_sessions s ON s.agent_id=a.agent_id AND s.tenant_id=space.tenant_id AND s.owner_id=space.owner_id
            WHERE s.tenant_id=$1 AND s.conversation_id=$2 AND space.product='musteragent' FOR UPDATE OF space")
            .bind(batch.append.tenant_id.into_uuid()).bind(batch.append.conversation_id.into_uuid()).fetch_one(&mut *tx).await?;
        let revoked: bool = sqlx::query_scalar(
            "SELECT s.permission_revision<cfg.permission_revision
            FROM agent.dialogue_sessions s JOIN agent.owned_agent_configurations cfg USING(agent_id)
            WHERE s.tenant_id=$1 AND s.conversation_id=$2 FOR SHARE OF cfg",
        )
        .bind(batch.append.tenant_id.into_uuid())
        .bind(batch.append.conversation_id.into_uuid())
        .fetch_one(&mut *tx)
        .await?;
        if !revoked {
            return Ok(false);
        }
        crate::execution::apply_cancel_mutation(
            tx,
            &crate::execution::CancelMutation::from_batch(&batch),
            now,
        )
        .await?;
        prepared.commit(&batch.append).await?;
        Ok(true)
    }

    fn revoked_wait_batch(
        &self,
        candidate: &ExpiredInteractionCandidate,
        invocation: InvocationId,
        owner: PrincipalId,
        last_attempt: Option<AttemptId>,
        now: UtcTimestamp,
    ) -> Result<CancelRunBatch, DispatchError> {
        let reason = CancelReason::PolicyRevoked;
        let identity = &candidate.identity;
        let events = [
            DomainEvent::Interaction(InteractionEvent::Cancelled {
                interaction_id: candidate.interaction_id,
                reason,
            }),
            DomainEvent::Tool(ToolEvent::InvocationCancelled {
                invocation_id: invocation,
                reason,
            }),
            DomainEvent::Run(RunEvent::CancelRequested {
                reason,
                requested_by: owner,
            }),
            DomainEvent::Run(RunEvent::Cancelled {
                reason,
                last_attempt_id: last_attempt,
            }),
            DomainEvent::Activity(ActivityEvent::Cancelled { reason }),
        ];
        let mut records = Vec::with_capacity(events.len());
        for (index, event) in events.into_iter().enumerate() {
            let payload = self.encoder.encode_domain_event(event)?;
            records.push(ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1")
                    .map_err(|_| contract_error())?,
                record_id: LedgerRecordId::from_uuid(derived_id(
                    candidate.interaction_id,
                    format!("permission-revoked-{index}").as_bytes(),
                ))
                .map_err(|_| contract_error())?,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(identity.activity_id),
                run_id: Some(identity.run_id),
                run_seq: Some(
                    RunSeq::new(next_run_sequence(
                        identity.last_run_sequence,
                        index as u64 + 1,
                    )?)
                    .map_err(|_| contract_error())?,
                ),
                attempt_id: None,
                participant_id: None,
                producer: Producer::Scheduler,
                occurred_at: now,
                causation_id: None,
                correlation_id: identity.correlation_id,
                payload_hash: Sha256::digest(&payload),
                payload,
            });
        }
        let key = format!(
            "scheduler:owner-permission-revoked:{}",
            candidate.interaction_id
        );
        Ok(CancelRunBatch {
            append: AppendRequest {
                tenant_id: identity.tenant_id,
                conversation_id: identity.conversation_id,
                expected_home_epoch: identity.home_epoch,
                expected_offset: identity.conversation_offset,
                command_fingerprint: Sha256::digest(key.as_bytes()),
                idempotency_key: IdempotencyKey::new(key).map_err(|_| contract_error())?,
                records,
                outbox: Vec::new(),
            },
            run_id: identity.run_id,
            activity_id: identity.activity_id,
            expected_last_run_sequence: identity.last_run_sequence,
            mode: CancelMode::CancelWithoutAttempt,
            reason,
            pending_interaction_id: Some(candidate.interaction_id),
            pending_tool_invocation_id: Some(invocation),
            unresolved_tool_invocation_id: None,
            descendants: Vec::new(),
        })
    }
}
