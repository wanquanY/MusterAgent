use super::*;

pub(super) async fn load(
    tx: &mut PgConnection,
    r: &PrepareWorkItemFollowup,
    allow_completed: bool,
) -> Result<WorkItemFollowupMaterial, DialogueStoreError> {
    let input = r.validate()?;
    let source = start_source::load_for(
        tx,
        &r.invocation,
        WORK_ITEM_FOLLOWUP_TOOL,
        allow_completed,
        None,
    )
    .await?;
    // Original accepted input authority, including an installed-capability
    // continuation proved by the same root. Schedule/report cannot create work.
    let proof = crate::agent_dialogue::capability::verified_root_id(
        tx,
        source.actor,
        source.scope.agent_id,
        source.source_intent_id,
    )
    .await?;
    if !proof.is_direct_user_root {
        return Err(DialogueStoreError::Unavailable);
    }
    let intent = proof.current_intent;
    let reply = intent
        .reply_binding
        .ok_or(DialogueStoreError::Unavailable)?;
    // Selected result stays exact and readable even when a newer action exists.
    // Only the separately accepted head chooses what this new instruction edits.
    crate::work_item_authority::authorize(tx, source.actor, source.scope, true)
        .await
        .map_err(authority_error)?;
    crate::dialogue_publication::reply::revalidate(tx, source.actor, source.scope.agent_id, &reply)
        .await?;
    let frozen = reply.task_action().ok_or(DialogueStoreError::Unavailable)?;
    let target = frozen.target;
    let assignment = frozen.assignment_epoch;
    if intent.task_target.is_some_and(|value| value != target) {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let stable_action = InputActionOrigin::Dialogue {
        tenant_id: source.actor.tenant_id,
        scope: source.scope,
        dialogue_id: source.source_dialogue_id,
        intent_id: source.source_intent_id,
    }
    .action(0);
    let stable_task = musteragent_runtime_session::agent_work_item_id(
        source.actor,
        source.scope,
        source.source_intent_id,
        &format!("followup:{}", stable_action.action_id),
    );
    let already:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_followups WHERE work_item_id=$1 AND parent_work_item_id=$2 AND source_intent_id=$3 AND tenant_id=$4 AND owner_id=$5)")
        .bind(stable_task.into_uuid()).bind(target.work_item_id.into_uuid()).bind(source.source_intent_id.into_uuid()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    // A repeat acknowledges the same action even after its own child advanced
    // the head. New inputs must still pass the original accepted-head CAS.
    if !already {
        reply_action::require_current(tx, source.actor, source.scope.agent_id, &reply).await?;
    }
    let parent =
        crate::work_item_authority::load(tx, source.actor, source.scope, target.work_item_id, None)
            .await
            .map_err(authority_error)?
            .ok_or(DialogueStoreError::Unavailable)?;
    if parent.core.status != WorkItemStatus::Completed
        || parent.core.revisions.goal.get() != target.goal_revision
        || parent.core.revisions.control.get() != target.control_revision
        || parent.core.assignment_epoch.map(|v| v.get()) != Some(assignment)
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let row=sqlx::query("SELECT x.run_id,x.executing_agent_id,x.device_placement_captured,x.device_workspace_id FROM collaboration.work_item_execution_reservations x
        JOIN collaboration.work_items w ON w.work_item_id=x.work_item_id AND w.tenant_id=x.tenant_id AND w.owner_id=x.owner_id
        WHERE x.work_item_id=$1 AND x.goal_revision=$2 AND x.control_revision=$3 AND x.assignment_epoch=$4 AND x.tenant_id=$5 AND x.owner_id=$6 AND x.requesting_agent_id=$7 AND w.erasure_job_id IS NULL")
        .bind(target.work_item_id.into_uuid()).bind(integer(target.goal_revision)?).bind(integer(target.control_revision)?).bind(integer(assignment)?)
        .bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(source.scope.agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let executor = AgentId::from_uuid(row.try_get("executing_agent_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let destination = device_target::load(
        tx,
        WorkItemExecutionRequest {
            actor: source.actor,
            work_item_id: target.work_item_id,
            goal_revision: WorkItemGoalRevision::new(target.goal_revision)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            control_revision: WorkItemControlRevision::new(target.control_revision)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            assignment_epoch: AssignmentEpoch::new(assignment)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            report_from_run_id: None,
        },
    )
    .await?;
    let workspace = if matches!(destination, WorkItemExecutionTarget::RemoteDevice(_)) {
        if !row
            .try_get::<bool, _>("device_placement_captured")
            .map_err(db)?
        {
            return Err(DialogueStoreError::Unavailable);
        }
        row.try_get::<Option<uuid::Uuid>, _>("device_workspace_id")
            .map_err(db)?
            .map(|id| WorkspaceId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt))
            .transpose()?
    } else {
        None
    };
    let root = frozen.chain_root_id;
    let sequence = frozen
        .chain_sequence
        .checked_add(1)
        .ok_or(DialogueStoreError::Corrupt)?;
    let mut spec = input.spec;
    spec.follow_up_of = Some(target.work_item_id);
    let material = WorkItemFollowupMaterial {
        source,
        reply,
        input: WorkItemStartInput {
            spec,
            executing_agent_id: Some(executor),
            placement: WorkItemStartPlacement::Cloud,
            coordinator_delivery: None,
        },
        destination,
        device_workspace_id: workspace,
        chain_root_id: root,
        chain_sequence: sequence,
    };
    material.validate()?;
    let existing=sqlx::query("SELECT command_id,request_sha256,work_item_id,operation FROM collaboration.input_task_actions WHERE tenant_id=$1 AND owner_id=$2 AND source_intent_id=$3")
        .bind(material.source.actor.tenant_id.into_uuid()).bind(material.source.actor.principal_id.into_uuid()).bind(material.source.source_intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    if let Some(row) = existing
        && (row.try_get::<uuid::Uuid, _>("command_id").map_err(db)?
            != material.action().command_id.into_uuid()
            || row.try_get::<Vec<u8>, _>("request_sha256").map_err(db)? != action_hash(&material)?
            || row.try_get::<uuid::Uuid, _>("work_item_id").map_err(db)?
                != material.task_id().into_uuid()
            || row.try_get::<&str, _>("operation").map_err(db)? != "followup")
    {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    let successor: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT work_item_id FROM collaboration.work_item_followups WHERE parent_work_item_id=$1",
    )
    .bind(target.work_item_id.into_uuid())
    .fetch_optional(tx)
    .await
    .map_err(db)?;
    if successor.is_some_and(|id| id != material.task_id().into_uuid()) {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(material)
}
pub(super) fn action_hash(m: &WorkItemFollowupMaterial) -> Result<[u8; 32], DialogueStoreError> {
    let mut bytes = b"work-item-followup@1\0".to_vec();
    bytes.extend_from_slice(
        &crate::dialogue_codec::reply_binding_bytes(Some(&m.reply))
            .ok_or(DialogueStoreError::Corrupt)?,
    );
    bytes.extend_from_slice(&work_item_goal_bytes(&m.input.spec));
    bytes.push(match m.input.spec.priority {
        WorkItemPriority::Low => 1,
        WorkItemPriority::Normal => 2,
        WorkItemPriority::High => 3,
        WorkItemPriority::Critical => 4,
    });
    Ok(Sha256::digest(bytes).into_bytes())
}
pub(super) async fn record(
    tx: &mut PgConnection,
    m: &WorkItemFollowupMaterial,
    at: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    let frozen = m.reply.task_action().ok_or(DialogueStoreError::Corrupt)?;
    let target = frozen.target;
    let assignment = frozen.assignment_epoch;
    let producing:uuid::Uuid=sqlx::query_scalar("SELECT run_id FROM collaboration.work_item_execution_reservations WHERE work_item_id=$1 AND goal_revision=$2 AND control_revision=$3 AND assignment_epoch=$4")
        .bind(target.work_item_id.into_uuid()).bind(integer(target.goal_revision)?).bind(integer(target.control_revision)?).bind(integer(assignment)?).fetch_one(&mut *tx).await.map_err(db)?;
    let (selected, selected_assignment, selected_producing) = match &m.reply.subject {
        DialogueReplySubject::WorkItemResult {
            target,
            assignment_epoch,
            producing_run_id,
            ..
        } => (*target, *assignment_epoch, *producing_run_id),
        _ => return Err(DialogueStoreError::Corrupt),
    };
    sqlx::query("INSERT INTO collaboration.work_item_followups(work_item_id,parent_work_item_id,chain_root_id,chain_sequence,tenant_id,owner_id,agent_id,source_intent_id,action_id,source_entry_id,source_visible_revision,parent_goal_revision,parent_control_revision,parent_assignment_epoch,parent_producing_run_id,reply_binding_sha256,created_at_ms,selected_work_item_id,selected_goal_revision,selected_control_revision,selected_assignment_epoch,selected_producing_run_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)")
        .bind(m.task_id().into_uuid()).bind(target.work_item_id.into_uuid()).bind(m.chain_root_id.into_uuid()).bind(integer(m.chain_sequence)?)
        .bind(m.source.actor.tenant_id.into_uuid()).bind(m.source.actor.principal_id.into_uuid()).bind(m.source.scope.agent_id.into_uuid()).bind(m.source.source_intent_id.into_uuid()).bind(m.action().action_id.into_uuid())
        .bind(m.reply.target.entry_id.into_uuid()).bind(integer(m.reply.target.visible_revision)?).bind(integer(target.goal_revision)?).bind(integer(target.control_revision)?).bind(integer(assignment)?)
        .bind(producing).bind(Sha256::digest(crate::dialogue_codec::reply_binding_bytes(Some(&m.reply)).ok_or(DialogueStoreError::Corrupt)?).into_bytes().as_slice()).bind(at.unix_millis()).bind(selected.work_item_id.into_uuid()).bind(integer(selected.goal_revision)?).bind(integer(selected.control_revision)?).bind(integer(selected_assignment)?).bind(selected_producing.into_uuid()).execute(tx).await.map_err(db)?;
    Ok(())
}
