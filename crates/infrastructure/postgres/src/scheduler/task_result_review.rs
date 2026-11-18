//! The existing scheduler drains historical result-review waits. Its exact
//! Interaction owner closes the question and wakes the original Run; no task
//! success or human consent is synthesized.
use super::*;
impl PostgresDispatchStore {
    pub(super) async fn retire_task_result_reviews(&self) -> Result<u64, DispatchError> {
        let Some(interactions) = &self.interaction_expiry else {
            return Ok(0);
        };
        let rows = sqlx::query(include_str!("task_result_review.sql"))
            .bind(CANDIDATE_SCAN_LIMIT)
            .fetch_all(&self.pool)
            .await
            .map_err(database_error)?;
        let mut retired = 0u64;
        for row in rows {
            let identity = decode_identity(&row)?;
            let interaction =
                InteractionId::from_uuid(row.try_get("interaction_id").map_err(database_error)?)
                    .map_err(|_| contract_error())?;
            let at = database_now(&self.pool).await?;
            let command = self.task_result_retirement_command(identity, interaction, at)?;
            match interactions.retire_task_result_review(command).await {
                Ok(_) => retired = retired.saturating_add(1),
                Err(error) if is_interaction_expiry_race(&error) => (),
                Err(error) => return Err(map_interaction_expiry_error(error)),
            }
        }
        Ok(retired)
    }
    fn task_result_retirement_command(
        &self,
        identity: RunIdentity,
        interaction: InteractionId,
        at: UtcTimestamp,
    ) -> Result<CloseInteractionCommit, DispatchError> {
        let record_id = LedgerRecordId::from_uuid(derived_id(
            interaction,
            b"task-result-review-retired-record@1",
        ))
        .map_err(|_| contract_error())?;
        let payload = self.encoder.encode_domain_event(DomainEvent::Interaction(
            InteractionEvent::Cancelled {
                interaction_id: interaction,
                reason: musteragent_runtime_contract::CancelReason::PolicyRevoked,
            },
        ))?;
        let record = ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .map_err(|_| contract_error())?,
            record_id,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(identity.activity_id),
            run_id: Some(identity.run_id),
            run_seq: Some(
                RunSeq::new(next_run_sequence(identity.last_run_sequence, 1)?)
                    .map_err(|_| contract_error())?,
            ),
            attempt_id: None,
            participant_id: None,
            producer: Producer::Scheduler,
            occurred_at: at,
            causation_id: None,
            correlation_id: identity.correlation_id,
            payload_hash: Sha256::digest(&payload),
            payload,
        };
        let dispatch = format!("run:{}", identity.run_id).into_bytes();
        let key = format!("scheduler:task-result-review-retire@1:{interaction}");
        let append = AppendRequest {
            tenant_id: identity.tenant_id,
            conversation_id: identity.conversation_id,
            expected_home_epoch: identity.home_epoch,
            expected_offset: identity.conversation_offset,
            idempotency_key: IdempotencyKey::new(key.clone()).map_err(|_| contract_error())?,
            command_fingerprint: Sha256::digest(key.as_bytes()),
            records: vec![record],
            outbox: vec![ProposedOutbox {
                message_id: OutboxMessageId::from_uuid(derived_id(
                    interaction,
                    b"task-result-review-retired-dispatch@1",
                ))
                .map_err(|_| contract_error())?,
                topic: DottedKey::new("run.dispatch").map_err(|_| contract_error())?,
                payload_hash: Sha256::digest(&dispatch),
                payload: dispatch,
            }],
        };
        Ok(CloseInteractionCommit {
            tenant_id: identity.tenant_id,
            interaction_id: interaction,
            closure: InteractionClosure::Cancel,
            closed_at: at,
            expected_last_run_sequence: identity.last_run_sequence,
            append,
            semantic_records: InteractionSemanticRecords {
                interaction_record_id: record_id,
                run_record_id: None,
                tool_record_id: None,
            },
        })
    }
}
