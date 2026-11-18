//! Exact successful Native starts; model-batch quiescence belongs to SQLite.
use super::*;
pub(super) async fn load(
    tx: &mut PgConnection,
    source: &WorkItemStartSource,
    input: &WorkItemHandoffInput,
) -> Result<WorkItemHandoffMaterial, DialogueStoreError> {
    let starts=sqlx::query("SELECT c.invocation_id,x.work_item_id FROM resource.device_commands c
        LEFT JOIN collaboration.work_item_execution_reservations x ON x.source_home='device' AND x.tenant_id=c.tenant_id AND x.source_run_id=c.run_id AND x.source_invocation_id=c.invocation_id AND x.goal_revision=1 AND x.control_revision=1 AND x.assignment_epoch=1
        WHERE c.tenant_id=$1 AND c.owner_id=$2 AND c.agent_id=$3 AND c.run_id=$4 AND c.tool_key='tasks.start@3' AND c.outcome='succeeded' ORDER BY x.work_item_id")
        .bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(source.scope.agent_id.into_uuid()).bind(source.source_run_id.into_uuid()).fetch_all(&mut *tx).await.map_err(db)?;
    let mut targets = input.tasks.clone();
    targets.sort_by_key(|task| task.task_id);
    if starts.len() != targets.len() {
        return Err(DialogueStoreError::Unavailable);
    }
    let mut proofs = Vec::with_capacity(targets.len());
    for (start, target) in starts.into_iter().zip(targets) {
        if start
            .try_get::<Option<uuid::Uuid>, _>("work_item_id")
            .map_err(db)?
            != Some(target.task_id)
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let proof = sqlx::query(
            "SELECT t.erasure_job_id,r.goal_revision,r.control_revision,r.assignment_epoch,r.status,
                    x.command_id,outbox.signed_admission_rejection
             FROM collaboration.work_items t
             JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision
             JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=t.work_item_id
                AND x.goal_revision=$5 AND x.control_revision=$6 AND x.assignment_epoch=$7
             JOIN collaboration.work_item_outbox outbox ON outbox.work_item_id=x.work_item_id
                AND outbox.state_revision=x.accepted_state_revision AND outbox.event_kind='execution_requested'
             WHERE t.work_item_id=$1 AND t.tenant_id=$2 AND t.owner_id=$3 AND t.agent_id=$4
                AND t.source_intent_id=$8 AND x.requesting_agent_id=t.agent_id",
        ).bind(target.task_id).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid())
         .bind(source.scope.agent_id.into_uuid()).bind(target.expected_goal_revision as i64)
         .bind(target.expected_control_revision as i64).bind(target.expected_assignment_epoch as i64)
         .bind(source.source_intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
         .ok_or(DialogueStoreError::RevisionConflict)?;
        if proof
            .try_get::<Option<uuid::Uuid>, _>("erasure_job_id")
            .map_err(db)?
            .is_some()
        {
            return Err(DialogueStoreError::Inactive);
        }
        if proof.try_get::<i64, _>("goal_revision").map_err(db)?
            != target.expected_goal_revision as i64
            || proof.try_get::<i64, _>("control_revision").map_err(db)?
                != target.expected_control_revision as i64
            || proof
                .try_get::<Option<i64>, _>("assignment_epoch")
                .map_err(db)?
                != Some(target.expected_assignment_epoch as i64)
        {
            return Err(DialogueStoreError::RevisionConflict);
        }
        if !(proof
            .try_get::<Option<Vec<u8>>, _>("signed_admission_rejection")
            .map_err(db)?
            .is_none()
            && matches!(
                proof.try_get::<&str, _>("status").map_err(db)?,
                "queued" | "ready" | "active" | "verifying" | "completed"
            ))
        {
            return Err(DialogueStoreError::Unavailable);
        }
        proofs.push(WorkItemHandoffProof {
            target,
            start_invocation_id: InvocationId::from_uuid(
                start.try_get("invocation_id").map_err(db)?,
            )
            .map_err(|_| DialogueStoreError::Corrupt)?,
            execution_command_id: CommandId::from_uuid(proof.try_get("command_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
        });
    }
    Ok(WorkItemHandoffMaterial {
        source: source.clone(),
        tasks: proofs,
    })
}
