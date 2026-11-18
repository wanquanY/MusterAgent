//! Same Task query authority for Native reads and immutable receipt replay.
use super::*;
use musteragent_runtime_session::{ListAgentWorkItems, WorkItemDirectoryPage};
struct Scope {
    own: OwnedAgentWorkItemScope,
    selected: Option<AgentTaskTarget>,
    assigned: Option<WorkItemId>,
}
async fn scope(
    tx: &mut PgConnection,
    actor: Actor,
    command: &DeviceResourceCommand,
) -> Result<Scope, DialogueStoreError> {
    let b = command.admission();
    let row=if let Some(task)=b.work_item {
        sqlx::query("SELECT t.space_id,t.agent_id FROM collaboration.work_item_execution_reservations x
          JOIN collaboration.work_items t USING(work_item_id)
          JOIN collaboration.work_item_device_custody c ON c.run_id=x.run_id
          JOIN collaboration.work_item_device_targets d ON
            (d.work_item_id,d.goal_revision,d.control_revision,d.assignment_epoch)=
            (c.work_item_id,c.goal_revision,c.control_revision,c.assignment_epoch)
          WHERE x.tenant_id=$1 AND x.owner_id=$2 AND x.run_id=$3 AND x.work_item_id=$4 AND x.goal_revision=$5 AND x.control_revision=$6 AND x.assignment_epoch=$7
          AND (c.work_item_id,c.goal_revision,c.control_revision,c.assignment_epoch)=(x.work_item_id,x.goal_revision,x.control_revision,x.assignment_epoch)
          AND c.admitted_at_ms IS NOT NULL AND t.erasure_job_id IS NULL AND d.device_id=$8 AND d.installation_id=$9")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(b.run_id.into_uuid()).bind(task.work_item_id.into_uuid())
            .bind(integer(task.goal_revision)?).bind(integer(task.control_revision)?).bind(integer(task.assignment_epoch)?)
            .bind(b.device_id.into_uuid()).bind(b.installation_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
    }else{
        sqlx::query("SELECT a.space_id,a.agent_id FROM agent.device_admissions d JOIN agent.owned_agents a USING(agent_id)
            WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.run_id=$3 AND d.intent_id=$4 AND d.device_id=$5 AND d.installation_id=$6
              AND a.status='active' AND d.admitted_at_ms IS NOT NULL")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(b.run_id.into_uuid()).bind(b.intent_id.into_uuid())
            .bind(b.device_id.into_uuid()).bind(b.installation_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
    }.ok_or(DialogueStoreError::Unavailable)?;
    let own = OwnedAgentWorkItemScope {
        owner_id: actor.principal_id,
        space_id: OwnerSpaceId::from_uuid(row.try_get("space_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    };
    let selected = if b.work_item.is_none() {
        let row=sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3")
            .bind(b.intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        let intent = intent_row(&row)?;
        let facts =
            crate::agent_dialogue::capability::verified_root(tx, actor, own.agent_id, &intent)
                .await?;
        input_actions::selected_target(tx, actor, own.agent_id, facts.current_intent).await?
    } else {
        None
    };
    Ok(Scope {
        own,
        selected,
        assigned: b.work_item.map(|t| t.work_item_id),
    })
}
async fn require_task(
    tx: &mut PgConnection,
    actor: Actor,
    scope: &Scope,
    task: WorkItemId,
) -> Result<(), DialogueStoreError> {
    if scope.selected.is_some_and(|v| v.work_item_id != task) {
        return Err(DialogueStoreError::Unavailable);
    }
    if let Some(assigned) = scope.assigned
        && assigned != task
    {
        let permitted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_items t JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision WHERE t.work_item_id=$1 AND t.tenant_id=$2 AND t.owner_id=$3 AND t.erasure_job_id IS NULL AND $4=ANY(r.dependencies))")
                .bind(assigned.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(task.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if !permitted {
            return Err(DialogueStoreError::Unavailable);
        }
    }
    crate::work_item_authority::authorize(tx, actor, scope.own, false)
        .await
        .map_err(authority_error)
}
pub(super) async fn read(
    tx: &mut PgConnection,
    actor: Actor,
    _custody: &DeviceCustody,
    command: &DeviceResourceCommand,
) -> Result<WorkItemReadResult, DialogueStoreError> {
    let scope = scope(tx, actor, command).await?;
    let query = parse_work_item_query(
        command.invocation().tool_key.as_str(),
        command.request_bytes(),
    )?;
    match query {
        WorkItemReadQuery::List { cursor, limit } => {
            if scope.assigned.is_some() {
                return Err(DialogueStoreError::Unavailable);
            }
            if let Some(selected) = scope.selected {
                if cursor.is_some() {
                    return Err(DialogueStoreError::RevisionConflict);
                }
                require_task(tx, actor, &scope, selected.work_item_id).await?;
                let record = crate::work_item_authority::load(
                    tx,
                    actor,
                    scope.own,
                    selected.work_item_id,
                    None,
                )
                .await
                .map_err(authority_error)?
                .ok_or(DialogueStoreError::Unavailable)?;
                Ok(WorkItemReadResult::List(WorkItemDirectoryPage {
                    through: record.directory_seq,
                    records: vec![record],
                    total_count: 1,
                    next: None,
                }))
            } else {
                Ok(WorkItemReadResult::List(
                    crate::work_item_authority::queries::list_in_transaction(
                        tx,
                        ListAgentWorkItems {
                            actor,
                            scope: scope.own,
                            cursor,
                            limit,
                        },
                    )
                    .await
                    .map_err(authority_error)?,
                ))
            }
        }
        WorkItemReadQuery::Inspect { work_item_id } => {
            require_task(tx, actor, &scope, work_item_id).await?;
            Ok(WorkItemReadResult::Inspect(
                crate::work_item_authority::load(tx, actor, scope.own, work_item_id, None)
                    .await
                    .map_err(authority_error)?
                    .ok_or(DialogueStoreError::Unavailable)?,
            ))
        }
        WorkItemReadQuery::Executions { .. } => Err(DialogueStoreError::Unavailable),
    }
}
pub(super) fn references(result: &WorkItemReadResult) -> Vec<ContentRef> {
    match result {
        WorkItemReadResult::Inspect(v) => vec![v.goal_content_ref.clone()],
        WorkItemReadResult::List(v) => v
            .records
            .iter()
            .map(|r| r.goal_content_ref.clone())
            .collect(),
        WorkItemReadResult::Executions(v) => v.authorized_content.clone(),
    }
}
pub(super) async fn validate(
    tx: &mut PgConnection,
    actor: Actor,
    command: &DeviceResourceCommand,
    bytes: &[u8],
    authorized: &[ContentRef],
) -> Result<(), DialogueStoreError> {
    let scope = scope(tx, actor, command).await?;
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
    let rows = match command.invocation().tool_key.as_str() {
        "tasks.inspect@1" => vec![value.get("task").ok_or(DialogueStoreError::Corrupt)?],
        "tasks.list@1" if scope.assigned.is_none() => value
            .get("tasks")
            .and_then(|v| v.as_array())
            .ok_or(DialogueStoreError::Corrupt)?
            .iter()
            .collect(),
        _ => return Err(DialogueStoreError::Unavailable),
    };
    if rows.len() != authorized.len() {
        return Err(DialogueStoreError::Corrupt);
    }
    for row in rows {
        let id = WorkItemId::from_uuid(
            row.get("task_id")
                .and_then(|v| v.as_str())
                .ok_or(DialogueStoreError::Corrupt)?
                .parse()
                .map_err(|_| DialogueStoreError::Corrupt)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?;
        require_task(tx, actor, &scope, id).await?;
        // Current ownership/erasure remains authoritative. The exact old read
        // revision is not silently refreshed when replaying its immutable receipt.
        crate::work_item_authority::load(tx, actor, scope.own, id, None)
            .await
            .map_err(authority_error)?
            .ok_or(DialogueStoreError::Unavailable)?;
        let revision = row
            .get("state_revision")
            .and_then(|v| v.as_u64())
            .filter(|v| *v > 0)
            .ok_or(DialogueStoreError::Corrupt)?;
        let historical = crate::work_item_authority::load(tx, actor, scope.own, id, Some(revision))
            .await
            .map_err(authority_error)?
            .ok_or(DialogueStoreError::Unavailable)?;
        if !authorized.contains(&historical.goal_content_ref) {
            return Err(DialogueStoreError::Corrupt);
        }
        lock_ready_content_reference(tx, actor.tenant_id, &historical.goal_content_ref).await?;
    }
    Ok(())
}
