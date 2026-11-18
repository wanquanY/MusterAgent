//! The selected public result and the accepted action head are distinct facts.
//! Only acceptance may select a head. Mutation validates that frozen selection;
//! neither a model argument nor a later Task revision silently retargets input.
use super::*;
use musteragent_execution_context::AgentTaskActionTarget;

pub(crate) async fn resolve_action_head(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    selected: AgentTaskTarget,
) -> Result<AgentTaskActionTarget, DialogueStoreError> {
    let row=sqlx::query("SELECT w.space_id,coalesce(f.chain_root_id,w.work_item_id) AS root FROM collaboration.work_items w LEFT JOIN collaboration.work_item_followups f ON f.work_item_id=w.work_item_id WHERE w.work_item_id=$1 AND w.tenant_id=$2 AND w.owner_id=$3 AND w.agent_id=$4 AND w.erasure_job_id IS NULL")
        .bind(selected.work_item_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let space_id = OwnerSpaceId::from_uuid(row.try_get("space_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let root = WorkItemId::from_uuid(row.try_get("root").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    crate::work_item_authority::authorize(
        tx,
        actor,
        OwnedAgentWorkItemScope {
            space_id,
            owner_id: actor.principal_id,
            agent_id: agent,
        },
        true,
    )
    .await
    .map_err(authority_error)?;
    // Query again under the same original Agent serialization lock as all Task
    // amendments, successor creation and erasure admission.
    let row=sqlx::query("SELECT w.work_item_id,r.goal_revision,r.control_revision,r.assignment_epoch,coalesce(f.chain_sequence,1) AS chain_sequence
        FROM collaboration.work_items w JOIN collaboration.work_item_revisions r USING(work_item_id,state_revision)
        LEFT JOIN collaboration.work_item_followups f ON f.work_item_id=w.work_item_id
        WHERE w.work_item_id=coalesce((SELECT work_item_id FROM collaboration.work_item_followups WHERE chain_root_id=$1 ORDER BY chain_sequence DESC LIMIT 1),$1)
          AND w.tenant_id=$2 AND w.owner_id=$3 AND w.agent_id=$4 AND w.erasure_job_id IS NULL
          AND EXISTS(SELECT 1 FROM collaboration.work_items selected WHERE selected.work_item_id=$5 AND selected.tenant_id=w.tenant_id AND selected.owner_id=w.owner_id AND selected.agent_id=w.agent_id AND selected.erasure_job_id IS NULL)")
        .bind(root.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid()).bind(selected.work_item_id.into_uuid()).fetch_optional(tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let value = AgentTaskActionTarget {
        target: AgentTaskTarget {
            work_item_id: WorkItemId::from_uuid(row.try_get("work_item_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            goal_revision: unsigned(row.try_get("goal_revision").map_err(db)?)?,
            control_revision: unsigned(row.try_get("control_revision").map_err(db)?)?,
        },
        assignment_epoch: unsigned(
            row.try_get::<Option<i64>, _>("assignment_epoch")
                .map_err(db)?
                .ok_or(DialogueStoreError::Unavailable)?,
        )?,
        chain_root_id: root,
        chain_sequence: unsigned(row.try_get("chain_sequence").map_err(db)?)?,
    };
    value.validate().map_err(|_| DialogueStoreError::Corrupt)?;
    Ok(value)
}

pub(super) async fn require_current(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    reply: &DialogueReplyBinding,
) -> Result<AgentTaskActionTarget, DialogueStoreError> {
    let selected = reply.task_result().ok_or(DialogueStoreError::Unavailable)?;
    let frozen = reply.task_action().ok_or(DialogueStoreError::Unavailable)?;
    if resolve_action_head(tx, actor, agent, selected).await? != frozen {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(frozen)
}
