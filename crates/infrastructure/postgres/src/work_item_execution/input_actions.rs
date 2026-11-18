//! A selected task input has one immutable mutation slot. Its original command
//! and every physical invocation commit with the WorkItem authority receipt.
use super::*;
use musteragent_content_ref::Sha256;

pub(super) async fn target(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
) -> Result<Option<AgentTaskTarget>, DialogueStoreError> {
    target_at(tx, source, true).await
}
/// Read preparation must not upgrade custody/intent locks out of commit order.
pub(super) async fn target_read(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
) -> Result<Option<AgentTaskTarget>, DialogueStoreError> {
    target_at(tx, source, false).await
}
async fn target_at(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    lock: bool,
) -> Result<Option<AgentTaskTarget>, DialogueStoreError> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    let row=sqlx::query(&format!("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 AND dialogue_id=$4{suffix}"))
        .bind(source.source_intent_id.into_uuid()).bind(source.actor.tenant_id.into_uuid())
        .bind(source.actor.principal_id.into_uuid()).bind(source.source_dialogue_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let intent = intent_row(&row)?;
    selected_target(tx, source.actor, source.scope.agent_id, intent).await
}

/// Resolve stable material identity and reject a redefined committed action
/// before writing immutable goal content. The effect transaction checks again.
pub(super) async fn prepare(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    mutation: &WorkItemMutation,
) -> Result<CommandId, DialogueStoreError> {
    prepare_at(tx, source, mutation, true).await
}
pub(super) async fn prepare_read(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    mutation: &WorkItemMutation,
) -> Result<CommandId, DialogueStoreError> {
    prepare_at(tx, source, mutation, false).await
}
async fn prepare_at(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    mutation: &WorkItemMutation,
    lock: bool,
) -> Result<CommandId, DialogueStoreError> {
    let Some(target) = target_at(tx, source, lock).await? else {
        return Ok(work_item_mutation_command_id(source.source_invocation_id));
    };
    let (action, hash) = identity(source, target, mutation)?;
    let accepted: Option<Vec<u8>> = sqlx::query_scalar("SELECT request_sha256 FROM collaboration.input_task_actions WHERE tenant_id=$1 AND owner_id=$2 AND source_intent_id=$3")
        .bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid())
        .bind(source.source_intent_id.into_uuid()).fetch_optional(tx).await.map_err(db)?;
    if accepted.is_some_and(|value| value != hash) {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    Ok(action.command_id)
}

fn identity(
    source: &WorkItemStartSource,
    target: AgentTaskTarget,
    mutation: &WorkItemMutation,
) -> Result<(musteragent_work_item::TaskActionRef, [u8; 32]), DialogueStoreError> {
    let (goal, control) = mutation.expected();
    if target.work_item_id != mutation.task_id()
        || target.goal_revision != goal.get()
        || target.control_revision != control.get()
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let action = InputActionOrigin::Dialogue {
        tenant_id: source.actor.tenant_id,
        scope: source.scope,
        dialogue_id: source.source_dialogue_id,
        intent_id: source.source_intent_id,
    }
    .action(0);
    let mut bytes = target.canonical_bytes();
    bytes.extend_from_slice(mutation.kind().as_bytes());
    if let WorkItemMutation::Amend { spec, .. } = mutation {
        bytes.extend_from_slice(&work_item_goal_bytes(spec));
        for dependency in &spec.dependencies {
            bytes.extend_from_slice(dependency.into_uuid().as_bytes());
        }
    }
    Ok((action, Sha256::digest(bytes).into_bytes()))
}

pub(super) async fn bind(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    mutation: &WorkItemMutation,
    at: UtcTimestamp,
) -> Result<CommandId, DialogueStoreError> {
    let Some(target) = target(tx, source).await? else {
        return Ok(work_item_mutation_command_id(source.source_invocation_id));
    };
    let (action, hash) = identity(source, target, mutation)?;
    let existing:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.input_task_actions WHERE tenant_id=$1 AND owner_id=$2 AND source_intent_id=$3)")
        .bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(source.source_intent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if !existing {
        let row=sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3")
            .bind(source.source_intent_id.into_uuid()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if let Some(reply) = intent_row(&row)?.reply_binding {
            reply_action::require_current(tx, source.actor, source.scope.agent_id, &reply).await?;
        }
    }
    bind_identity(
        tx,
        source,
        action,
        target.work_item_id,
        mutation.kind(),
        hash,
        at,
    )
    .await
}
pub(super) async fn bind_identity(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    action: musteragent_work_item::TaskActionRef,
    work_item_id: WorkItemId,
    operation: &str,
    hash: [u8; 32],
    at: UtcTimestamp,
) -> Result<CommandId, DialogueStoreError> {
    sqlx::query("INSERT INTO collaboration.input_task_actions
        (tenant_id,owner_id,agent_id,source_intent_id,action_id,work_item_id,operation,
         request_sha256,command_id,source_run_id,source_invocation_id,source_snapshot_id,accepted_at_ms)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        ON CONFLICT (tenant_id,owner_id,source_intent_id) DO NOTHING")
        .bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid())
        .bind(source.scope.agent_id.into_uuid()).bind(source.source_intent_id.into_uuid())
        .bind(action.action_id.into_uuid()).bind(work_item_id.into_uuid())
        .bind(operation).bind(hash.as_slice()).bind(action.command_id.into_uuid())
        .bind(source.source_run_id.into_uuid()).bind(source.source_invocation_id.into_uuid())
        .bind(source.source_snapshot_id.into_uuid()).bind(at.unix_millis())
        .execute(&mut *tx).await.map_err(db)?;
    let row = sqlx::query(
        "SELECT action_id,request_sha256,command_id,work_item_id,operation FROM collaboration.input_task_actions
        WHERE tenant_id=$1 AND owner_id=$2 AND source_intent_id=$3",
    )
    .bind(source.actor.tenant_id.into_uuid())
    .bind(source.actor.principal_id.into_uuid())
    .bind(source.source_intent_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    if row.try_get::<uuid::Uuid, _>("work_item_id").map_err(db)? != work_item_id.into_uuid()
        || row.try_get::<&str, _>("operation").map_err(db)? != operation
        || row.try_get::<uuid::Uuid, _>("action_id").map_err(db)? != action.action_id.into_uuid()
        || row.try_get::<Vec<u8>, _>("request_sha256").map_err(db)? != hash
        || row.try_get::<uuid::Uuid, _>("command_id").map_err(db)? != action.command_id.into_uuid()
    {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    sqlx::query("INSERT INTO collaboration.input_task_action_invocations (tenant_id,action_id,invocation_id)
        VALUES($1,$2,$3) ON CONFLICT (tenant_id,invocation_id) DO NOTHING")
        .bind(source.actor.tenant_id.into_uuid()).bind(action.action_id.into_uuid())
        .bind(source.source_invocation_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
    Ok(action.command_id)
}

/// Read-side binding uses the admitted source; no mutable model-provided ID.
pub(super) async fn query_target(
    tx: &mut PgConnection,
    r: &ResolveWorkItemStart,
) -> Result<Option<AgentTaskTarget>, DialogueStoreError> {
    let row=sqlx::query("SELECT i.* FROM agent.dialogue_sessions s JOIN runtime.runs run ON run.tenant_id=s.tenant_id AND run.conversation_id=s.conversation_id JOIN agent.dialogue_user_intents i ON i.intent_id=s.source_intent_id AND i.tenant_id=s.tenant_id AND i.owner_id=s.owner_id WHERE run.run_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.agent_id=$4 AND s.subject_kind='intent'")
        .bind(r.run_context.run_id.into_uuid()).bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid()).bind(r.requesting_agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    selected_target(tx, r.actor, r.requesting_agent_id, intent_row(&row)?).await
}
pub(super) async fn selected_target(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    intent: UserIntent,
) -> Result<Option<AgentTaskTarget>, DialogueStoreError> {
    let reply_target = intent
        .reply_binding
        .as_ref()
        .and_then(|b| b.task_action())
        .map(|v| v.target);
    if let Some(reply) = reply_target {
        if intent.task_target.is_some_and(|target| target != reply) {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let proof =
            crate::agent_dialogue::capability::verified_root(tx, actor, agent, &intent).await?;
        if !proof.is_direct_user_root {
            return Err(DialogueStoreError::Unavailable);
        }
        crate::dialogue_publication::reply::revalidate(
            tx,
            actor,
            agent,
            intent
                .reply_binding
                .as_ref()
                .ok_or(DialogueStoreError::Corrupt)?,
        )
        .await?;
    }
    Ok(intent.task_target.or(reply_target))
}
