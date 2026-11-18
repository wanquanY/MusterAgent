//! Current source fence for an already admitted Owner-scoped Goal Run. This is the
//! original Goal owner, shared by context, model and Tool authorization.
use super::*;
use musteragent_runtime_snapshot::OwnerGoalSnapshotAuthority;

/// A schedule/child keeps a frozen Goal ceiling without becoming a new pursuit.
/// Historical pursuit completion is allowed; source ownership and permission
/// revocation remain authoritative in the original Goal/Agent stores.
pub(crate) async fn validate_derived_authority(
    tx: &mut PgConnection,
    authority: &OwnerGoalSnapshotAuthority,
    lock: bool,
) -> Result<(), DialogueStoreError> {
    let cut = &authority.context_cut;
    if lock {
        sqlx::query("SELECT goal_id FROM runtime.standing_goals WHERE tenant_id=$1 AND goal_id=$2 FOR SHARE")
            .bind(cut.tenant_id.into_uuid()).bind(authority.goal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    }
    let goal = load_goal_tx(tx, cut.tenant_id, authority.goal_id)
        .await
        .map_err(storage)?
        .ok_or(DialogueStoreError::Unavailable)?;
    let binding = load_binding(tx, &goal)
        .await
        .map_err(storage)?
        .ok_or(DialogueStoreError::Unavailable)?;
    if !goal.allows_existing_work()
        || goal.owner != cut.owner_id
        || binding.revision != authority.binding_revision
        || binding.scope.agent_id != cut.agent_id
        || binding.scope.dialogue_id != cut.dialogue_id
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let current =
        crate::dialogue_execution::agent::load(tx, binding.scope.actor(), cut.dialogue_id, lock)
            .await?;
    if current.agent.status != OwnedAgentStatus::Active
        || current.configuration.permission_revision.get() != authority.permission_revision
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    if crate::dialogue_context::load_cut(
        tx,
        binding.scope.actor(),
        cut.agent_id,
        cut.dialogue_id,
        cut.through,
    )
    .await?
        != *cut
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}

pub(crate) async fn validate_current(
    tx: &mut PgConnection,
    run: RunId,
    authority: &OwnerGoalSnapshotAuthority,
    lock: bool,
) -> Result<(), DialogueStoreError> {
    let cut = &authority.context_cut;
    // Control mutations lock Goal then Pursuit. Their source-Run verification
    // reads immutable evidence and does not acquire the execution commit lane.
    if lock {
        sqlx::query("SELECT goal_id FROM runtime.standing_goals WHERE tenant_id=$1 AND goal_id=$2 FOR SHARE")
            .bind(cut.tenant_id.into_uuid()).bind(authority.goal_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        sqlx::query("SELECT pursuit_id FROM runtime.goal_pursuits WHERE tenant_id=$1 AND pursuit_id=$2 FOR SHARE")
            .bind(cut.tenant_id.into_uuid()).bind(authority.pursuit_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    }
    let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.standing_goals g
        JOIN runtime.goal_pursuits p ON p.tenant_id=g.tenant_id AND p.goal_id=g.goal_id
        JOIN runtime.runs r ON r.tenant_id=p.tenant_id AND r.run_id=$4 AND r.conversation_id=g.conversation_id
        JOIN agent.dialogue_sessions s ON s.tenant_id=g.tenant_id AND s.conversation_id=g.conversation_id
        WHERE g.tenant_id=$1 AND g.goal_id=$2 AND p.pursuit_id=$3 AND r.run_id=$4
          AND g.owner_principal_id=$5 AND g.mode=2 AND g.status IN (2,3)
          AND (g.active_pursuit_id=p.pursuit_id OR g.status=3) AND p.status=2
          AND g.input_batch_first IS NOT NULL
          AND ((r.activity_kind=3 AND r.run_id=p.active_run_id AND g.input_batch_run_id=r.run_id
                AND r.parent_run_id IS NOT DISTINCT FROM p.continuation_source_run_id)
            OR (r.activity_kind=6 AND r.parent_run_id=p.active_run_id AND g.input_batch_run_id=p.active_run_id
                AND EXISTS(SELECT 1 FROM runtime.runs original WHERE original.tenant_id=r.tenant_id
                    AND original.run_id=p.active_run_id AND original.status=5 AND original.erasure_job_id IS NULL)))
          AND r.delegation_id IS NULL AND r.status IN (1,2,4) AND r.cancel_requested_at_ms IS NULL
          AND s.subject_kind='goal' AND s.source_goal_id=g.goal_id AND s.owner_id=g.owner_principal_id
          AND s.agent_id=$6 AND s.dialogue_id=$7 AND s.goal_binding_revision=$8)")
        .bind(cut.tenant_id.into_uuid()).bind(authority.goal_id.into_uuid())
        .bind(authority.pursuit_id.into_uuid()).bind(run.into_uuid()).bind(cut.owner_id.into_uuid())
        .bind(cut.agent_id.into_uuid()).bind(cut.dialogue_id.into_uuid()).bind(authority.binding_revision as i64)
        .fetch_one(&mut *tx).await.map_err(db)?;
    if !current {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let rules = rules::for_run(tx, cut.tenant_id, authority.goal_id, run)
        .await
        .map_err(storage)?
        .ok_or(DialogueStoreError::Corrupt)?;
    if rules.target_agent_id != cut.agent_id {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}
