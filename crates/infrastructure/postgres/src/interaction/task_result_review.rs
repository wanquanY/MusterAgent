//! A product-policy retirement is a closed original Interaction transition. It
//! never supplies an Approval response and never completes a WorkItem.
use super::*;
impl PostgresInteractionStore {
    pub(crate) async fn retire_task_result_review(
        &self,
        command: CloseInteractionCommit,
    ) -> Result<InteractionCommitReceipt, PostgresInteractionError> {
        self.close_inner(command, None, ClosePurpose::RetireTaskResultReview)
            .await
    }
}
pub(super) async fn retain(
    tx: &mut sqlx::PgConnection,
    decoder: &dyn LedgerPayloadDecoder,
    command: &CloseInteractionCommit,
    interaction: &PendingInteraction,
) -> Result<(), PostgresInteractionError> {
    if command.closure != InteractionClosure::Cancel
        || interaction.kind != InteractionKind::Confirmation
        || command.append.records.len() != 1
    {
        return Err(PostgresInteractionError::IdentityMismatch);
    }
    let record = &command.append.records[0];
    let event = decoder
        .decode(record.record_kind, &record.payload)
        .map_err(|_| PostgresInteractionError::SemanticLedgerMismatch)?;
    if record.producer != Producer::Scheduler
        || record.activity_id != Some(interaction.activity_id)
        || record.run_id != Some(interaction.run_id)
        || record.occurred_at != command.closed_at
        || record.participant_id.is_some()
        || event
            != LedgerPayload::DomainEvent(DomainEvent::Interaction(InteractionEvent::Cancelled {
                interaction_id: command.interaction_id,
                reason: musteragent_runtime_contract::CancelReason::PolicyRevoked,
            }))
    {
        return Err(PostgresInteractionError::SemanticLedgerMismatch);
    }
    let row = sqlx::query(include_str!("task_result_review.sql"))
        .bind(command.tenant_id.into_uuid())
        .bind(command.interaction_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(PostgresInteractionError::IdentityMismatch)?;
    sqlx::query("INSERT INTO approval.task_result_review_retirements(interaction_id,tenant_id,conversation_id,run_id,invocation_id,work_item_id,goal_revision,control_revision,assignment_epoch,producing_run_id,report_content_id,prompt_content_id,ledger_record_id,retired_at_ms,policy) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'direct_task_delivery@1')")
        .bind(command.interaction_id.into_uuid()).bind(command.tenant_id.into_uuid()).bind(interaction.conversation_id.into_uuid()).bind(interaction.run_id.into_uuid())
        .bind(row.try_get::<uuid::Uuid,_>("invocation_id")?).bind(row.try_get::<uuid::Uuid,_>("work_item_id")?).bind(row.try_get::<i64,_>("goal_revision")?).bind(row.try_get::<i64,_>("control_revision")?).bind(row.try_get::<i64,_>("assignment_epoch")?).bind(row.try_get::<uuid::Uuid,_>("producing_run_id")?).bind(row.try_get::<uuid::Uuid,_>("report_content_id")?).bind(row.try_get::<uuid::Uuid,_>("prompt_content_id")?).bind(record.record_id.into_uuid()).bind(command.closed_at.unix_millis()).execute(&mut *tx).await?;
    Ok(())
}
