//! Reconstruct the complete current-Run delegation set from original receipts.
use super::*;

pub(super) async fn load(
    tx: &mut PgConnection,
    p: &PrepareWorkItemHandoff,
    replay: bool,
) -> Result<WorkItemHandoffMaterial, DialogueStoreError> {
    let i = &p.invocation;
    let source = start_source::load_for(tx, i, "tasks.handoff@1", replay, None).await?;
    // A handoff cannot share a model batch with further, possibly local work.
    let boundary: Option<(String,String)> = sqlx::query_as(
        "SELECT plan.source_sequence_to::text,plan.committed_sequence::text FROM runtime.pending_tool_calls call
         JOIN runtime.context_plans plan ON plan.run_id=call.run_id AND plan.invocation_id=call.invocation_id
         JOIN runtime.model_invocations model ON model.run_id=plan.run_id AND model.invocation_id=plan.invocation_id AND model.status=2
         WHERE call.run_id=$1 AND call.tool_call_id=$2 AND plan.tenant_id=$3
           AND (SELECT count(*) FROM runtime.pending_tool_calls sibling
                WHERE sibling.run_id=call.run_id AND sibling.invocation_id=call.invocation_id)=1",
    ).bind(source.source_run_id.into_uuid()).bind(i.tool_call_id.as_str())
     .bind(source.actor.tenant_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let (boundary, model_cut) = boundary.ok_or(DialogueStoreError::Unavailable)?;
    if !replay
        && pending(
            tx,
            source.actor.tenant_id,
            source.source_run_id,
            i.invocation_id,
            &boundary,
        )
        .await?
    {
        return Err(DialogueStoreError::Unavailable);
    }
    // Do not start from caller-supplied IDs: every successful start in this Run
    // must have its original reservation, including starts in earlier iterations.
    // A replay uses the immutable model boundary: later steering may already
    // have accepted more tasks, but cannot rewrite this original receipt.
    let starts = sqlx::query(
        "SELECT call.invocation_id,x.work_item_id FROM tool.tool_invocations call
         LEFT JOIN runtime.pending_tool_calls accepted ON accepted.run_id=call.run_id AND accepted.tool_call_id=call.tool_call_id
         LEFT JOIN runtime.context_plans origin ON origin.run_id=accepted.run_id AND origin.invocation_id=accepted.invocation_id
         LEFT JOIN collaboration.work_item_execution_reservations x
           ON x.tenant_id=call.tenant_id AND x.source_run_id=call.run_id
           AND x.source_invocation_id=call.invocation_id
           AND x.goal_revision=1 AND x.control_revision=1 AND x.assignment_epoch=1
         WHERE call.tenant_id=$1 AND call.run_id=$2 AND call.tool_ref IN ('tasks.start@1','tasks.start@2','tasks.start@3') AND call.status=4
           AND (NOT $3 OR origin.committed_sequence<$4::numeric)
         ORDER BY x.work_item_id",
    ).bind(source.actor.tenant_id.into_uuid()).bind(source.source_run_id.into_uuid()).bind(replay).bind(model_cut)
     .fetch_all(&mut *tx).await.map_err(db)?;
    let mut targets = p.input.tasks.clone();
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
        if !replay
            && (proof.try_get::<i64, _>("goal_revision").map_err(db)?
                != target.expected_goal_revision as i64
                || proof.try_get::<i64, _>("control_revision").map_err(db)?
                    != target.expected_control_revision as i64
                || proof
                    .try_get::<Option<i64>, _>("assignment_epoch")
                    .map_err(db)?
                    != Some(target.expected_assignment_epoch as i64))
        {
            return Err(DialogueStoreError::RevisionConflict);
        }
        if !replay
            && (proof
                .try_get::<Option<Vec<u8>>, _>("signed_admission_rejection")
                .map_err(db)?
                .is_some()
                || !matches!(
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
        source,
        tasks: proofs,
    })
}

async fn pending(
    tx: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    invocation: InvocationId,
    boundary: &str,
) -> Result<bool, DialogueStoreError> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=$2 AND invocation_id<>$3 AND status IN (1,2,3,6))
         OR EXISTS(SELECT 1 FROM approval.interactions WHERE tenant_id=$1 AND run_id=$2 AND status=1)
         OR EXISTS(SELECT 1 FROM runtime.delegations WHERE tenant_id=$1 AND parent_run_id=$2 AND kind=1 AND status=1)
         OR EXISTS(SELECT 1 FROM runtime.loop_controls WHERE tenant_id=$1 AND run_id=$2 AND status IN (1,2) AND source_conversation_seq>$4::numeric)",
    ).bind(tenant.into_uuid()).bind(run.into_uuid()).bind(invocation.into_uuid()).bind(boundary)
     .fetch_one(tx).await.map_err(db)
}
