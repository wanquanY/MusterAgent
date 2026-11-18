//! Dialogue delivery repair counts semantic failures, not physical Tool turns.
//! Only an original committed public completion or an authenticated response to
//! the original published Interaction advances this episode. Plans, read/write
//! receipts, provider retries and worker replacement do not erase its history.
use super::*;
use musteragent_execution_context::{PublicDeliveryContract, PublicDeliveryIssue};
use std::num::NonZeroU32;

pub(super) async fn load(
    pool: &PgPool,
    tenant: TenantId,
    run: RunId,
) -> Result<Option<MaterializedModelActionRejection>, PostgresContextMaterialError> {
    let rows = sqlx::query(
        "WITH dialogue_session AS (
            SELECT session.*,run.run_id FROM agent.dialogue_sessions session
            JOIN runtime.runs run ON run.tenant_id=session.tenant_id AND run.conversation_id=session.conversation_id
            JOIN conversation.conversations conversation ON conversation.tenant_id=session.tenant_id
              AND conversation.conversation_id=session.conversation_id AND conversation.created_by=session.owner_id
              AND conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution')
            WHERE run.tenant_id=$1 AND run.run_id=$2
        ), progress AS (
            SELECT model.started_conversation_seq AS sequence
            FROM dialogue_session JOIN agent.public_delivery_candidates candidate USING(tenant_id,conversation_id)
            JOIN agent.dialogue_publications publication ON publication.delivery_id=candidate.delivery_id
              AND publication.tenant_id=candidate.tenant_id AND publication.source_run_id=candidate.run_id
              AND publication.source_kind='model_delivery'
            JOIN runtime.model_invocations model ON model.tenant_id=candidate.tenant_id
              AND model.run_id=candidate.run_id AND model.invocation_id=candidate.invocation_id AND model.status=2
            WHERE candidate.run_id=$2 AND candidate.kind IN ('message','artifact')
              AND candidate.purpose IN ('reply','result','blocked')
              AND NOT EXISTS(SELECT 1 FROM agent.public_delivery_candidates pending
                  WHERE pending.tenant_id=candidate.tenant_id AND pending.invocation_id=candidate.invocation_id
                    AND NOT EXISTS(SELECT 1 FROM agent.dialogue_publications published
                        WHERE published.tenant_id=pending.tenant_id AND published.delivery_id=pending.delivery_id))
            UNION ALL
            SELECT record.conversation_seq AS sequence
            FROM dialogue_session JOIN approval.interactions interaction USING(tenant_id,conversation_id)
            JOIN agent.dialogue_publications publication ON publication.tenant_id=interaction.tenant_id
              AND publication.source_run_id=interaction.run_id AND publication.source_interaction_id=interaction.interaction_id
              AND publication.source_kind='interaction_owner'
            JOIN approval.interaction_responses response ON response.interaction_id=interaction.interaction_id
              AND response.responded_by=interaction.responded_by AND response.responded_at_ms=interaction.responded_at_ms
              AND response.responded_by=dialogue_session.owner_id
            JOIN event_store.ledger_records record ON record.record_id=response.response_ledger_record_id
              AND record.tenant_id=interaction.tenant_id AND record.run_id=interaction.run_id
              AND record.conversation_id=interaction.conversation_id AND record.record_kind=1
              AND record.payload_hash=response.response_ledger_payload_hash
            JOIN conversation.participants responder ON responder.tenant_id=record.tenant_id
              AND responder.conversation_id=record.conversation_id AND responder.participant_id=record.participant_id
              AND responder.kind=1 AND responder.principal_ref=dialogue_session.owner_id
            WHERE interaction.run_id=$2 AND interaction.status=2
        )
        SELECT rejection.delivery_contract_key,rejection.delivery_issue
        FROM runtime.model_action_rejections rejection
        JOIN runtime.model_invocations invocation ON invocation.invocation_id=rejection.invocation_id
        JOIN dialogue_session ON dialogue_session.tenant_id=invocation.tenant_id AND dialogue_session.run_id=invocation.run_id
        WHERE invocation.tenant_id=$1 AND invocation.run_id=$2 AND invocation.purpose=1 AND invocation.status=3
          AND rejection.rejection_kind='public_delivery' AND rejection.error_code='model_response_invalid'
          AND invocation.terminal_error=convert_to(rejection.error_code,'UTF8')
          AND invocation.started_conversation_seq > COALESCE((SELECT max(sequence) FROM progress),0)
        ORDER BY invocation.started_conversation_seq DESC LIMIT $3"
    ).bind(tenant.into_uuid()).bind(run.into_uuid())
        .bind(i64::from(musteragent_context::MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS))
        .fetch_all(pool).await?;
    let Some(latest) = rows.first() else {
        return Ok(None);
    };
    let key: String = latest.try_get("delivery_contract_key")?;
    if PublicDeliveryContract::from_key(&key).is_none()
        || rows
            .iter()
            .any(|row| row.try_get::<&str, _>("delivery_contract_key").ok() != Some(key.as_str()))
    {
        return Err(PostgresContextMaterialError::CorruptState);
    }
    Ok(Some(
        MaterializedModelActionRejection::InvalidPublicDelivery {
            contract_ref: VersionedKey::new(key)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            issue: PublicDeliveryIssue::parse(latest.try_get("delivery_issue")?)
                .ok_or(PostgresContextMaterialError::CorruptState)?,
            consecutive_count: NonZeroU32::new(u32::try_from(rows.len())?)
                .ok_or(PostgresContextMaterialError::CorruptState)?,
        },
    ))
}
