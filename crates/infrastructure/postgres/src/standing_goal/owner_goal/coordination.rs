//! Private addresses are resolved under the original account/Goal ownership
//! fence. This creates no Run and issues no execution or tool authority.
use super::*;
use musteragent_runtime_session::GoalCoordinationStoreError as Error;
use musteragent_work_item::AgentCollaborationBinding;

impl PostgresStandingGoalProductStore {
    pub async fn resolve_owner_goal_collaboration(
        &self,
        actor: Actor,
        goal_id: GoalId,
        at: UtcTimestamp,
    ) -> Result<AgentCollaborationBinding, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(|_| Error::Unavailable)?;
        let binding = resolve_on(&mut tx, actor, goal_id, at).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(binding)
    }
}

/// The mailbox commit uses this same-transaction owner boundary. The returned
/// rule version must still match the sending Run's frozen authority; an address
/// lookup cannot upgrade an old Run to a changed objective or new assignee.
pub(crate) async fn resolve_on(
    tx: &mut PgConnection,
    actor: Actor,
    goal_id: GoalId,
    at: UtcTimestamp,
) -> Result<AgentCollaborationBinding, Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(Error::Unauthorized);
    }
    crate::dialogue_handoff::progress::lock_account(tx, actor)
        .await
        .map_err(dialogue_error)?;
    if !account_available(tx, actor.principal_id)
        .await
        .map_err(goal_error)?
    {
        return Err(Error::Unauthorized);
    }
    super::super::admission::lock_goal(tx, actor.tenant_id, goal_id)
        .await
        .map_err(goal_error)?;
    let goal = load_goal_tx(tx, actor.tenant_id, goal_id)
        .await
        .map_err(goal_error)?
        .ok_or(Error::NotFound)?;
    if goal.owner != actor.principal_id {
        return Err(Error::Unauthorized);
    }
    if goal.status != StandingGoalStatus::Active {
        return Err(Error::Conflict);
    }
    let binding = load_binding(tx, &goal)
        .await
        .map_err(goal_error)?
        .ok_or(Error::Unauthorized)?;
    if !independent_input_source_active(tx, &goal)
        .await
        .map_err(goal_error)?
    {
        return Err(Error::Unauthorized);
    }
    let ids = binding.coordinator_context_identities();
    let retained: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT goal_coordinator_conversation_id FROM agent.dialogue_sessions
         WHERE tenant_id=$1 AND conversation_id=$2 FOR UPDATE",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(binding.scope.conversation_id().into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(database_error)?;
    match retained {
        Some(id) if id != ids.conversation_id.into_uuid() => return Err(Error::Corrupt),
        Some(_) => (),
        None => {
            // Forward upgrade of an existing binding. Deterministic membership
            // and the original binding reference commit together, with no chat
            // replay or synthetic UserIntent. A collision is never adopted.
            let requester =
                crate::dialogue_execution::agent::load(tx, actor, binding.source_dialogue_id, true)
                    .await
                    .map_err(dialogue_error)?;
            insert_private_context(
                tx,
                binding.scope,
                ids,
                binding.requesting_agent_id,
                &requester.configuration.profile.display_name,
                musteragent_conversation::ConversationPurpose::OwnerGoalCoordination,
                at.unix_millis(),
            )
            .await
            .map_err(dialogue_error)?;
            let updated = sqlx::query("UPDATE agent.dialogue_sessions SET goal_coordinator_conversation_id=$3
                WHERE tenant_id=$1 AND conversation_id=$2 AND goal_coordinator_conversation_id IS NULL")
                .bind(actor.tenant_id.into_uuid()).bind(binding.scope.conversation_id().into_uuid())
                .bind(ids.conversation_id.into_uuid()).execute(&mut *tx).await.map_err(database_error)?;
            if updated.rows_affected() != 1 {
                return Err(Error::Conflict);
            }
        }
    }
    // Recheck actual private membership and lifecycle, not merely stored UUIDs.
    // Neither a public Conversation nor another Goal's private context qualifies.
    for (conversation, agent, purpose) in [
        (
            binding.scope.conversation_id(),
            binding.scope.agent_id,
            binding.scope.purpose(),
        ),
        (
            ids.conversation_id,
            binding.requesting_agent_id,
            musteragent_conversation::ConversationPurpose::OwnerGoalCoordination,
        ),
    ] {
        let valid: bool = sqlx::query_scalar("SELECT EXISTS(
            SELECT 1 FROM conversation.conversations c
            JOIN conversation.participants executor ON executor.tenant_id=c.tenant_id
                AND executor.conversation_id=c.conversation_id AND executor.participant_id=c.execution_owner_participant_id
            JOIN conversation.participants owner ON owner.tenant_id=c.tenant_id AND owner.conversation_id=c.conversation_id
            WHERE c.tenant_id=$1 AND c.conversation_id=$2 AND c.created_by=$3 AND c.purpose=$4 AND c.lifecycle=1
                AND executor.kind=2 AND executor.agent_ref=$5 AND executor.left_at_ms IS NULL
                AND owner.kind=1 AND owner.principal_ref=$3 AND owner.left_at_ms IS NULL
            FOR SHARE OF c,executor,owner)")
            .bind(actor.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(actor.principal_id.into_uuid())
            .bind(purpose.as_str()).bind(agent.into_uuid()).fetch_one(&mut *tx).await.map_err(database_error)?;
        if !valid {
            return Err(Error::Unauthorized);
        }
    }
    let rules = musteragent_persistence_codec::goal_processing_rules::encode(
        &musteragent_goal::GoalProcessingRules::from_goal(&goal),
    )
    .map_err(|_| Error::Corrupt)?;
    Ok(binding.collaboration_binding(
        musteragent_persistence_codec::goal_processing_rules::version(&rules),
    ))
}

fn database_error(_: sqlx::Error) -> Error {
    Error::Unavailable
}
fn goal_error(error: PostgresStandingGoalError) -> Error {
    match error {
        PostgresStandingGoalError::NotFound => Error::NotFound,
        PostgresStandingGoalError::Corrupt => Error::Corrupt,
        _ => Error::Unavailable,
    }
}
fn dialogue_error(error: DialogueStoreError) -> Error {
    match error {
        DialogueStoreError::Unavailable | DialogueStoreError::Inactive => Error::Unauthorized,
        DialogueStoreError::Corrupt => Error::Corrupt,
        DialogueStoreError::RevisionConflict | DialogueStoreError::IdempotencyConflict => {
            Error::Conflict
        }
        _ => Error::Unavailable,
    }
}
