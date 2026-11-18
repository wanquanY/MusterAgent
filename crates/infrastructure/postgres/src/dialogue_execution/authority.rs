//! Current Dialogue execution identity, shared by resource and account-domain
//! tool owners. Runtime custody is resolved from the invocation, not arguments.
use musteragent_identity::{
    Actor, AgentId, ConversationId, InvocationId, PrincipalKind, RunId, VersionedKey,
};
use sqlx::{PgConnection, Row};

const SOURCE: &str = "SELECT a.agent_id,a.space_id,inv.tool_ref,r.run_id,s.subject_kind FROM tool.tool_invocations inv
 JOIN runtime.runs r ON r.tenant_id=inv.tenant_id AND r.run_id=inv.run_id AND r.conversation_id=inv.conversation_id
 JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
 JOIN conversation.conversations c ON c.tenant_id=s.tenant_id AND c.conversation_id=s.conversation_id
 JOIN conversation.participants p ON p.tenant_id=r.tenant_id AND p.conversation_id=r.conversation_id AND p.participant_id=r.executor_participant_id
 JOIN agent.owned_agents a ON a.agent_id=s.agent_id AND a.dialogue_id=s.dialogue_id
 JOIN agent.owner_spaces space ON space.space_id=a.space_id AND space.tenant_id=s.tenant_id AND space.owner_id=s.owner_id
 JOIN agent.owned_agent_configurations config ON config.agent_id=a.agent_id AND (config.permission_revision=s.permission_revision OR s.subject_kind='goal')
 WHERE inv.tenant_id=$1 AND inv.conversation_id=$2 AND inv.invocation_id=$3 AND s.owner_id=$4
 AND s.purpose IN ('intent_coordination','work_item_execution','work_item_report','goal_execution') AND s.conversation_purpose=c.purpose AND c.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution')
 AND c.created_by=s.owner_id AND c.kind=4 AND c.workspace_id IS NULL AND c.lifecycle=1
 AND c.execution_owner_participant_id=r.executor_participant_id AND p.kind=2 AND p.agent_ref=s.agent_id AND p.left_at_ms IS NULL
 AND a.status='active' AND space.product='musteragent'
 AND (r.parent_run_id IS NULL OR s.subject_kind='goal') AND r.delegation_id IS NULL AND r.status=2 AND r.cancel_requested_at_ms IS NULL
 AND c.home_epoch=r.home_epoch
 AND EXISTS (SELECT 1 FROM runtime.attempts attempt
   JOIN runtime.leases lease ON lease.tenant_id=attempt.tenant_id AND lease.run_id=attempt.run_id AND lease.attempt_id=attempt.attempt_id
   WHERE attempt.tenant_id=r.tenant_id AND attempt.run_id=r.run_id AND attempt.status=1
    AND lease.expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint
    AND ((inv.status=3 AND inv.execution_attempt_id=attempt.attempt_id)
      OR ($5 AND inv.status=6 AND inv.retry_contract=3)))";

/// Does not infer authority from an arbitrary Agent argument. Called only with
/// the invocation identity provided by the original sealed Tool execution.
pub(crate) async fn resolve_execution(
    tx: &mut PgConnection,
    actor: Actor,
    conversation: ConversationId,
    invocation: InvocationId,
) -> Result<Option<(AgentId, VersionedKey)>, sqlx::Error> {
    resolve_execution_on(tx, actor, conversation, invocation, false).await
}

/// Commit owners already hold the Run fence. Reconciliation must retain the
/// same original invocation authority even after its prepared attempt ended.
pub(crate) async fn resolve_commit_execution(
    tx: &mut PgConnection,
    actor: Actor,
    conversation: ConversationId,
    invocation: InvocationId,
) -> Result<Option<(AgentId, VersionedKey)>, sqlx::Error> {
    resolve_execution_on(tx, actor, conversation, invocation, true).await
}

pub(crate) async fn resolve_execution_on(
    tx: &mut PgConnection,
    actor: Actor,
    conversation: ConversationId,
    invocation: InvocationId,
    fenced_reconciliation: bool,
) -> Result<Option<(AgentId, VersionedKey)>, sqlx::Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Ok(None);
    }
    let row = sqlx::query(SOURCE)
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation.into_uuid())
        .bind(invocation.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(fenced_reconciliation)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(row) = row else { return Ok(None) };
    let agent = AgentId::from_uuid(row.try_get("agent_id")?)
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
    if row.try_get::<&str, _>("subject_kind")? == "goal" {
        let run = RunId::from_uuid(row.try_get("run_id")?)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        match crate::dialogue_context::run::run_cut(tx, run, false).await {
            Ok(Some(cut)) if cut.agent_id == agent && cut.owner_id == actor.principal_id => {}
            Err(musteragent_runtime_session::DialogueStoreError::Infrastructure) => {
                return Err(sqlx::Error::Protocol(
                    "Owner-scoped Goal authority unavailable".into(),
                ));
            }
            _ => return Ok(None),
        }
    }
    Ok(Some((
        agent,
        VersionedKey::new(row.try_get::<String, _>("tool_ref")?)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
    )))
}
