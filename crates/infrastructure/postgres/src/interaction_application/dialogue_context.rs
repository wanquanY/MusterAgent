//! Metadata-only context read through the original Dialogue interaction binding.
//! No prompts, answers, secure references or UI actions enter model history.
use super::*;
use musteragent_execution_context::{
    DialogueApprovalContextKind as Kind, DialogueApprovalContextState as State,
    DialogueApprovalObservation,
};
use musteragent_identity::{AgentDialogueId, DialogueEntryId, RunId};
use sqlx::PgConnection;

pub(crate) async fn observations(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entries: &[uuid::Uuid],
) -> Result<Vec<DialogueApprovalObservation>, PostgresInteractionError> {
    if entries.is_empty() {
        return Ok(vec![]);
    }
    if actor.principal_kind != PrincipalKind::User
        || entries.len() > musteragent_execution_context::DIALOGUE_APPROVAL_CONTEXT_LIMIT
    {
        return Err(PostgresInteractionError::ActorForbidden);
    }
    // Original Dialogue visibility and active participant requirements. Reuse
    // binding_row for its exact control identity; no prompt/secure bytes read.
    let query=format!("SELECT e.entry_id,e.seq,i.run_id,i.interaction_id,i.conversation_id,i.kind,i.status,
        i.expires_at_ms,i.prompt_content_id,i.response_schema_key,i.response_schema_hash,
        p.content_hash,e.visible_revision,e.author_agent_id,s.permission_revision AS pinned_permission,
        cfg.permission_revision,cfg.permission_mode,r.execution_home_kind,r.execution_device_id,
        r.control_home_kind,r.control_device_id,r.home_epoch::text AS home_epoch,
        r.status AS run_status,r.cancel_requested_at_ms, ({}) AS pending,
        response.response_kind,response.approval_decision
        {} JOIN conversation.participants viewer ON viewer.tenant_id=i.tenant_id AND viewer.conversation_id=i.conversation_id AND viewer.kind=1 AND viewer.principal_ref=$2 AND viewer.left_at_ms IS NULL
        LEFT JOIN approval.interaction_responses response ON response.interaction_id=i.interaction_id
        WHERE {} AND e.dialogue_id=$3 AND e.entry_id=ANY($4) AND e.erasure_job_id IS NULL",
        super::dialogue_pending::PENDING,super::dialogue_pending::FROM,super::dialogue_pending::VISIBLE);
    let rows = sqlx::query(&query)
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(dialogue.into_uuid())
        .bind(entries)
        .fetch_all(&mut *tx)
        .await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let entry = DialogueEntryId::from_uuid(row.try_get("entry_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?;
        let sequence = u64::try_from(row.try_get::<i64, _>("seq")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?;
        let bound = super::dialogue::binding_row(actor, dialogue, entry, &row)?;
        let kind = match crate::interaction::decode_kind(row.try_get("kind")?)? {
            musteragent_approval::InteractionKind::Approval => Kind::Approval,
            musteragent_approval::InteractionKind::Confirmation => Kind::Confirmation,
            _ => continue,
        };
        let state = match crate::interaction::decode_status(row.try_get("status")?)? {
            musteragent_approval::InteractionStatus::Pending => State::Pending,
            musteragent_approval::InteractionStatus::Expired => State::Expired,
            musteragent_approval::InteractionStatus::Cancelled => State::Cancelled,
            musteragent_approval::InteractionStatus::Responded => {
                if row.try_get::<Option<i16>, _>("response_kind")? != Some(1) {
                    return Err(PostgresInteractionError::Corrupt);
                }
                match row.try_get::<Option<i16>, _>("approval_decision")? {
                    Some(1) => State::Approved,
                    Some(2) => State::Denied,
                    Some(3) => State::ResolvedDecisionUnavailable,
                    _ => return Err(PostgresInteractionError::Corrupt),
                }
            }
        };
        result.push(DialogueApprovalObservation {
            entry_id: entry,
            sequence,
            interaction_id: bound.interaction_id,
            run_id: RunId::from_uuid(row.try_get("run_id")?)
                .map_err(|_| PostgresInteractionError::Corrupt)?,
            visible_revision: bound.visible_revision,
            control_revision: bound.control_revision,
            kind,
            state,
        });
    }
    Ok(result)
}
