//! Only an actual admitted Dialogue Run and its registered invocation can
//! delegate a task. Knowledge of another account's IDs is never authority.
use super::*;
pub(super) async fn load(
    tx: &mut PgConnection,
    r: &ResolveWorkItemStart,
    allow_completed: bool,
) -> Result<WorkItemStartSource, DialogueStoreError> {
    let actual: Option<String> = sqlx::query_scalar("SELECT tool_ref FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2 AND run_id=$3")
        .bind(r.actor.tenant_id.into_uuid()).bind(r.invocation_id.into_uuid())
        .bind(r.run_context.run_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let tool = actual
        .as_deref()
        .filter(|key| {
            matches!(
                *key,
                "tasks.start@1" | "tasks.start@2" | "tasks.start@3" | "tasks.start@4"
            )
        })
        .ok_or(DialogueStoreError::Unavailable)?;
    let source = load_for(tx, r, tool, allow_completed, None).await?;
    if input_actions::target(tx, &source).await?.is_some() {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(source)
}
pub(super) async fn load_for(
    tx: &mut PgConnection,
    r: &ResolveWorkItemStart,
    tool: &str,
    allow_completed: bool,
    task: Option<WorkItemId>,
) -> Result<WorkItemStartSource, DialogueStoreError> {
    require_user(r.actor)?;
    if r.actor.tenant_id != r.run_context.tenant_id {
        return Err(DialogueStoreError::Unavailable);
    }
    let row=sqlx::query("SELECT s.source_intent_id,s.dialogue_id,a.space_id,run.runtime_snapshot_id,i.request_content_id,i.status AS tool_status FROM agent.dialogue_sessions s
 JOIN runtime.runs run ON run.tenant_id=s.tenant_id AND run.conversation_id=s.conversation_id
 JOIN conversation.conversations c ON c.tenant_id=s.tenant_id AND c.conversation_id=s.conversation_id
 JOIN conversation.participants p ON p.tenant_id=run.tenant_id AND p.conversation_id=run.conversation_id AND p.participant_id=run.executor_participant_id
 JOIN agent.owned_agents a ON a.agent_id=s.agent_id AND a.dialogue_id=s.dialogue_id
 JOIN agent.owner_spaces space ON space.space_id=a.space_id AND space.tenant_id=s.tenant_id AND space.owner_id=s.owner_id
 JOIN tool.tool_invocations i ON i.tenant_id=run.tenant_id AND i.run_id=run.run_id AND i.conversation_id=run.conversation_id
 WHERE run.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4 AND i.invocation_id=$5 AND i.tool_call_id=$6 AND (i.tool_ref=$8 OR ($8='tasks.amend@1' AND i.tool_ref='tasks.amend@2'))
 AND ((s.subject_kind='intent' AND s.purpose='intent_coordination' AND c.purpose='dialogue_intent_coordination') OR ($9::uuid IS NOT NULL AND s.subject_kind='work_item_report' AND s.purpose='work_item_report' AND c.purpose='agent_work_item_report' AND s.work_item_id=$9 AND s.requesting_agent_id=s.agent_id))
 AND c.created_by=s.owner_id AND c.kind=4 AND c.workspace_id IS NULL AND c.lifecycle=1
 AND c.execution_owner_participant_id=run.executor_participant_id AND p.kind=2 AND p.agent_ref=s.agent_id AND p.left_at_ms IS NULL
 AND a.status='active' AND space.product='musteragent' AND run.parent_run_id IS NULL AND run.delegation_id IS NULL
 AND ($7 OR (run.status=2 AND run.cancel_requested_at_ms IS NULL))")
 .bind(r.run_context.run_id.into_uuid()).bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(r.requesting_agent_id.into_uuid())
 .bind(r.invocation_id.into_uuid()).bind(r.tool_call_id.as_str()).bind(allow_completed).bind(tool).bind(task.map(WorkItemId::into_uuid)).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    if row
        .try_get::<uuid::Uuid, _>("request_content_id")
        .map_err(db)?
        != r.request_ref.content_id.into_uuid()
    {
        return Err(DialogueStoreError::IdempotencyConflict);
    }

    if !allow_completed {
        crate::dialogue_context::run::run_cut(tx, r.run_context.run_id, false)
            .await?
            .ok_or(DialogueStoreError::Unavailable)?;
    }
    Ok(WorkItemStartSource {
        actor: r.actor,
        scope: OwnedAgentWorkItemScope {
            space_id: OwnerSpaceId::from_uuid(row.try_get("space_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            owner_id: r.actor.principal_id,
            agent_id: r.requesting_agent_id,
        },
        source_dialogue_id: AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        source_intent_id: UserIntentId::from_uuid(row.try_get("source_intent_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        source_run_id: r.run_context.run_id,
        source_invocation_id: r.invocation_id,
        source_snapshot_id: RuntimeSnapshotId::from_uuid(
            row.try_get("runtime_snapshot_id").map_err(db)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?,
    })
}

/// Reads use canonical execution identity without inventing a direct UserIntent
/// for Task/report subjects. The same resource and context owners recheck the
/// actual current permission, invocation lease and Task fence.
pub(super) async fn query_scope(
    tx: &mut PgConnection,
    r: &ResolveWorkItemStart,
    tool: &str,
) -> Result<OwnedAgentWorkItemScope, DialogueStoreError> {
    require_user(r.actor)?;
    crate::agent_resource::resolve_execution(
        tx,
        r.actor,
        r.run_context.conversation_id,
        r.invocation_id,
    )
    .await
    .map_err(db)?
    .filter(|(agent, key)| *agent == r.requesting_agent_id && key.as_str() == tool)
    .ok_or(DialogueStoreError::Unavailable)?;
    crate::dialogue_context::run::run_cut(tx, r.run_context.run_id, false)
        .await?
        .ok_or(DialogueStoreError::Unavailable)?;
    let space:uuid::Uuid=sqlx::query_scalar("SELECT a.space_id FROM agent.owned_agents a JOIN agent.owner_spaces space ON space.space_id=a.space_id JOIN agent.dialogue_sessions s ON s.agent_id=a.agent_id AND s.dialogue_id=a.dialogue_id AND s.tenant_id=space.tenant_id AND s.owner_id=space.owner_id JOIN runtime.runs run ON run.tenant_id=s.tenant_id AND run.conversation_id=s.conversation_id JOIN tool.tool_invocations tool ON tool.tenant_id=run.tenant_id AND tool.run_id=run.run_id WHERE a.agent_id=$1 AND space.tenant_id=$2 AND space.owner_id=$3 AND run.run_id=$4 AND run.conversation_id=$5 AND run.runtime_snapshot_id=$6 AND tool.invocation_id=$7 AND tool.request_content_id=$8 AND tool.tool_call_id=$9")
  .bind(r.requesting_agent_id.into_uuid()).bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(r.run_context.run_id.into_uuid()).bind(r.run_context.conversation_id.into_uuid()).bind(r.run_context.runtime_snapshot_id.into_uuid()).bind(r.invocation_id.into_uuid()).bind(r.request_ref.content_id.into_uuid()).bind(r.tool_call_id.as_str()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    Ok(OwnedAgentWorkItemScope {
        space_id: OwnerSpaceId::from_uuid(space).map_err(|_| DialogueStoreError::Corrupt)?,
        owner_id: r.actor.principal_id,
        agent_id: r.requesting_agent_id,
    })
}

/// Only a committed same-Task report command can survive its own revision fence.
pub(super) async fn retained_authority(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    task: WorkItemId,
) -> Result<(), DialogueStoreError> {
    if let Some(native) = native_source::load(tx, source.actor, source.source_invocation_id).await?
    {
        if native.source != *source {
            return Err(DialogueStoreError::Corrupt);
        }
        return native_source::require_current(tx, &native).await;
    }
    let subject:String=sqlx::query_scalar("SELECT s.subject_kind FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id WHERE r.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4")
 .bind(source.source_run_id.into_uuid()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(source.scope.agent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    match subject.as_str() {
        "intent" => {
            crate::dialogue_context::run::run_cut(tx, source.source_run_id, false)
                .await?
                .ok_or(DialogueStoreError::Unavailable)?;
        }
        "work_item_report" => {
            crate::dialogue_context::run::work_item_command_cut(
                tx,
                source.actor,
                task,
                source.source_run_id,
                source.source_invocation_id,
            )
            .await?;
        }
        _ => return Err(DialogueStoreError::Unavailable),
    }
    Ok(())
}

/// A delegated executor may inspect its assignment and declared prerequisites. It does not
/// gain the requester's Task directory or mutation authority.
pub(super) async fn inspection_scope(
    tx: &mut PgConnection,
    r: &ResolveWorkItemStart,
    own: OwnedAgentWorkItemScope,
    task: WorkItemId,
) -> Result<OwnedAgentWorkItemScope, DialogueStoreError> {
    let source=sqlx::query("SELECT s.subject_kind,s.work_item_id,s.requesting_agent_id,t.space_id,t.agent_id FROM agent.dialogue_sessions s JOIN runtime.runs run ON run.conversation_id=s.conversation_id AND run.tenant_id=s.tenant_id LEFT JOIN collaboration.work_items t ON t.work_item_id=s.work_item_id AND t.tenant_id=s.tenant_id AND t.owner_id=s.owner_id WHERE run.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4")
 .bind(r.run_context.run_id.into_uuid()).bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(r.requesting_agent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if source.try_get::<&str, _>("subject_kind").map_err(db)? != "work_item" {
        return Ok(own);
    }
    let assigned: Option<uuid::Uuid> = source.try_get("work_item_id").map_err(db)?;
    if assigned != Some(task.into_uuid()) {
        let declared: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_items t JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision WHERE t.work_item_id=$1 AND t.tenant_id=$2 AND t.owner_id=$3 AND t.erasure_job_id IS NULL AND $4=ANY(r.dependencies))")
            .bind(assigned).bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(task.into_uuid())
            .fetch_one(&mut *tx).await.map_err(db)?;
        if !declared {
            return Err(DialogueStoreError::Unavailable);
        }
    }
    let requester: uuid::Uuid = source.try_get("requesting_agent_id").map_err(db)?;
    if source.try_get::<uuid::Uuid, _>("agent_id").map_err(db)? != requester
        || source.try_get::<uuid::Uuid, _>("space_id").map_err(db)? != own.space_id.into_uuid()
    {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(OwnedAgentWorkItemScope {
        agent_id: AgentId::from_uuid(requester).map_err(|_| DialogueStoreError::Corrupt)?,
        ..own
    })
}
