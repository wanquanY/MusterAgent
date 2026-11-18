use super::*;
use musteragent_identity::{HomeEpoch, LedgerOffset};
use musteragent_runtime_session::OwnerGoalExecutionKey;

/// A repeatable source read now; admission calls the same projection again
/// under the original account/Goal/Pursuit locks before committing any effect.
pub(super) async fn load(
    tx: &mut PgConnection,
    request: OwnerGoalContinuationRequest,
    lock: bool,
) -> Result<OwnerGoalExecutionPreparation, DialogueStoreError> {
    let scope = request.scope;
    if lock {
        crate::dialogue_handoff::progress::lock_account(tx, scope.actor()).await?;
        crate::standing_goal::admission::lock_goal(tx, scope.tenant_id, scope.goal_id)
            .await
            .map_err(storage)?;
        sqlx::query("SELECT pursuit_id FROM runtime.goal_pursuits WHERE tenant_id=$1 AND pursuit_id=$2 FOR UPDATE")
            .bind(scope.tenant_id.into_uuid()).bind(request.pursuit_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    }
    let goal = load_goal_tx(tx, scope.tenant_id, scope.goal_id)
        .await
        .map_err(storage)?
        .ok_or(DialogueStoreError::Unavailable)?;
    let binding = load_binding(tx, &goal)
        .await
        .map_err(storage)?
        .ok_or(DialogueStoreError::Unavailable)?;
    if binding.scope != scope {
        return Err(DialogueStoreError::Unavailable);
    }
    let pursuit = load_pursuit_tx(tx, scope.tenant_id, request.pursuit_id)
        .await
        .map_err(storage)?
        .ok_or(DialogueStoreError::Unavailable)?;
    if !goal.allows_existing_work()
        || goal.revision != request.fence.goal_revision
        || pursuit.revision != request.fence.pursuit_revision
        || pursuit.goal_id != scope.goal_id
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let evaluation_source = match &request.phase {
        OwnerGoalAdmissionPhase::Execution
            if pursuit.status == GoalPursuitStatus::Pending && pursuit.active_run_id.is_none() =>
        {
            None
        }
        OwnerGoalAdmissionPhase::Evaluation { source_run_id, .. }
            if pursuit.status == GoalPursuitStatus::Running
                && pursuit.active_run_id == Some(*source_run_id) =>
        {
            Some(*source_run_id)
        }
        _ => return Err(DialogueStoreError::RevisionConflict),
    };
    if evaluation_source.is_none()
        && !crate::owner_event_sources::required_sources_ready(tx, &goal)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?
    {
        return Err(DialogueStoreError::Unavailable);
    }
    let claim_current: bool = if let Some(source) = evaluation_source {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.goal_evaluation_claims claim
            JOIN runtime.runs r ON r.tenant_id=claim.tenant_id AND r.run_id=claim.run_id
            JOIN runtime.standing_goals g ON g.tenant_id=r.tenant_id AND g.conversation_id=r.conversation_id
            WHERE claim.tenant_id=$1 AND claim.pursuit_id=$2 AND claim.run_id=$3 AND claim.claim_token=$4
              AND claim.claim_lease_expires_at_ms > (extract(epoch FROM transaction_timestamp())*1000)::bigint
              AND g.goal_id=$5 AND g.input_batch_run_id=r.run_id AND r.status=5 AND r.erasure_job_id IS NULL)")
            .bind(scope.tenant_id.into_uuid()).bind(request.pursuit_id.into_uuid()).bind(source.into_uuid())
            .bind(&request.fence.claim_token).bind(scope.goal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?
    } else {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.goal_pursuits p JOIN runtime.standing_goals g
        ON g.tenant_id=p.tenant_id AND g.goal_id=p.goal_id WHERE p.tenant_id=$1 AND p.pursuit_id=$2 AND p.claim_token=$3
        AND p.claim_lease_expires_at_ms > (extract(epoch FROM transaction_timestamp())*1000)::bigint
        AND p.continuation_due_at_ms <= (extract(epoch FROM transaction_timestamp())*1000)::bigint
        AND (g.active_pursuit_id=p.pursuit_id OR g.status=3)
        AND (g.input_batch_run_id IS NULL OR g.input_batch_run_id=p.continuation_source_run_id))")
        .bind(scope.tenant_id.into_uuid()).bind(request.pursuit_id.into_uuid()).bind(&request.fence.claim_token)
        .fetch_one(&mut *tx).await.map_err(db)?
    };
    if !claim_current {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let cursor = input::load_cursor(tx, scope.tenant_id, scope.goal_id)
        .await
        .map_err(storage)?;
    let batch = cursor.batch.ok_or(DialogueStoreError::RevisionConflict)?;
    let processing_rules = rules::for_batch(tx, scope.tenant_id, scope.goal_id, batch)
        .await
        .map_err(storage)?;
    if processing_rules.target_agent_id != scope.agent_id {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let input_refs = input::references(tx, scope.tenant_id, scope.goal_id, batch)
        .await
        .map_err(storage)?;
    let current =
        crate::dialogue_execution::agent::load(tx, scope.actor(), scope.dialogue_id, lock).await?;
    if current.agent.status != OwnedAgentStatus::Active {
        return Err(DialogueStoreError::Inactive);
    }
    let context_cut = crate::dialogue_context::load_cut(
        tx,
        scope.actor(),
        scope.agent_id,
        scope.dialogue_id,
        current.dialogue.head,
    )
    .await?;
    let context_has_input_attachments =
        crate::dialogue_context::has_input_attachments(tx, &context_cut).await?;
    let (private_context_home_epoch, private_context_offset) = private_context(tx, scope).await?;
    let previous_evaluation_summary_ref = if evaluation_source.is_none() {
        let row = sqlx::query("SELECT c.content_id,c.media_type,c.byte_length::text,c.content_hash
            FROM runtime.goal_evaluations e JOIN artifact.content_refs c ON c.tenant_id=e.tenant_id AND c.content_id=e.summary_content_id
            WHERE e.tenant_id=$1 AND e.pursuit_id=$2 AND e.run_id=$3 ORDER BY e.occurred_at_ms DESC,e.evaluation_id DESC LIMIT 1")
            .bind(scope.tenant_id.into_uuid()).bind(request.pursuit_id.into_uuid())
            .bind(pursuit.continuation_source_run_id.map(RunId::into_uuid)).fetch_optional(&mut *tx).await.map_err(db)?;
        row.as_ref()
            .map(|row| evaluation_decode::content_ref(row, ""))
            .transpose()
            .map_err(storage)?
    } else {
        None
    };
    let value = OwnerGoalExecutionPreparation {
        phase: request.phase,
        previous_evaluation_summary_ref,
        private_context_home_epoch,
        private_context_offset,
        standing_authority: load_continuation_authority_tx(tx, scope.tenant_id, request.pursuit_id)
            .await
            .map_err(storage)?,
        key: OwnerGoalExecutionKey {
            scope,
            pursuit_id: request.pursuit_id,
            batch,
            run_ordinal: u64::from(pursuit.budget_usage.runs)
                .checked_add(u64::from(evaluation_source.is_none()))
                .ok_or(DialogueStoreError::Corrupt)?,
        },
        binding_revision: binding.revision,
        fence: request.fence,
        previous_run_id: evaluation_source.or(pursuit.continuation_source_run_id),
        default_agent_id: current.default_agent_id,
        agent: current.agent,
        configuration: current.configuration,
        assistant_identity: current.identity,
        context_cut,
        context_has_input_attachments,
        processing_rules,
        input_refs,
        product_release: musteragent_skill::ProductSkillRelease::for_product(
            musteragent_identity::ProductId::KERNEL,
        )
        .reference(),
    };
    value.validate()?;
    Ok(value)
}

async fn private_context(
    tx: &mut PgConnection,
    scope: OwnerGoalScope,
) -> Result<(HomeEpoch, LedgerOffset), DialogueStoreError> {
    let ids = scope.context_identities();
    let row = sqlx::query("SELECT c.home_epoch::text,COALESCE(s.last_sequence,0)::text AS last_sequence
        FROM conversation.conversations c LEFT JOIN event_store.ledger_streams s
          ON s.tenant_id=c.tenant_id AND s.conversation_id=c.conversation_id
        JOIN conversation.participants p ON p.tenant_id=c.tenant_id AND p.conversation_id=c.conversation_id AND p.participant_id=$4
        JOIN conversation.participants e ON e.tenant_id=c.tenant_id AND e.conversation_id=c.conversation_id AND e.participant_id=$5
        WHERE c.tenant_id=$1 AND c.conversation_id=$2 AND c.created_by=$3 AND c.kind=4 AND c.workspace_id IS NULL
          AND c.purpose='owner_goal_execution' AND c.lifecycle=1 AND c.control_home_kind=1 AND c.control_device_id IS NULL
          AND c.execution_owner_participant_id=e.participant_id AND e.kind=2 AND e.agent_ref=$6 AND e.left_at_ms IS NULL
          AND p.kind=3 AND p.principal_ref IS NULL AND p.agent_ref IS NULL AND p.service_ref='musteragent.owner-goal@1'
          AND p.left_at_ms IS NULL")
        .bind(scope.tenant_id.into_uuid()).bind(ids.conversation_id.into_uuid()).bind(scope.owner_id.into_uuid())
        .bind(ids.requested_by.into_uuid()).bind(ids.executor.into_uuid()).bind(scope.agent_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let home = row
        .try_get::<String, _>("home_epoch")
        .map_err(db)?
        .parse::<u64>()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let offset = row
        .try_get::<String, _>("last_sequence")
        .map_err(db)?
        .parse::<u64>()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    Ok((
        HomeEpoch::new(home).map_err(|_| DialogueStoreError::Corrupt)?,
        LedgerOffset::new(offset),
    ))
}
