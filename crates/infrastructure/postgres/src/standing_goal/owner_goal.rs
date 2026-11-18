//! Formal Owner-scoped Goal source on the original Goal and Dialogue session owners.
//! No historical chat Run, UserIntent, synthetic grant or parallel Run queue.
use super::*;
use musteragent_definition::owned_agent::OwnedAgentStatus;
use musteragent_identity::{AgentDialogueId, CommandId, OwnerSpaceId};
use musteragent_runtime_session::{
    CreateOwnerGoalDraft, DialogueStoreError, OwnerGoalAdmissionPhase, OwnerGoalBinding,
    OwnerGoalContinuationRequest, OwnerGoalDraftReceipt, OwnerGoalExecutionPreparation,
    OwnerGoalExecutionSource, OwnerGoalScope,
};
use sqlx::PgConnection;

pub(super) mod admission;
pub(crate) mod coordination;
pub(crate) mod creation;
mod preparation;
pub(crate) mod run;

fn storage(error: PostgresStandingGoalError) -> DialogueStoreError {
    match error {
        PostgresStandingGoalError::NotFound => DialogueStoreError::Unavailable,
        PostgresStandingGoalError::Corrupt => DialogueStoreError::Corrupt,
        PostgresStandingGoalError::AssignmentPending => DialogueStoreError::RevisionConflict,
        _ => DialogueStoreError::Infrastructure,
    }
}
fn db(_: sqlx::Error) -> DialogueStoreError {
    DialogueStoreError::Infrastructure
}

async fn account_available(
    tx: &mut PgConnection,
    owner: PrincipalId,
) -> Result<bool, PostgresStandingGoalError> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owner_spaces WHERE owner_id=$1)")
        .bind(owner.into_uuid())
        .fetch_one(tx)
        .await
        .map_err(PostgresStandingGoalError::from)
}

/// Source ownership, not provider readiness. Required provider readiness is
/// rechecked by admission before any model Run can consume accepted input.
pub(super) async fn independent_input_source_active(
    tx: &mut PgConnection,
    goal: &StandingGoal,
) -> Result<bool, PostgresStandingGoalError> {
    let Some(binding) = load_binding(tx, goal).await? else {
        return Ok(false);
    };
    if !account_available(tx, goal.owner).await? {
        return Ok(false);
    }
    let actor = musteragent_identity::Actor {
        tenant_id: goal.tenant_id,
        principal_id: goal.owner,
        principal_kind: musteragent_identity::PrincipalKind::User,
    };
    for (dialogue, agent) in [
        (binding.scope.dialogue_id, binding.scope.agent_id),
        (binding.source_dialogue_id, binding.requesting_agent_id),
    ] {
        let current = match crate::dialogue_execution::agent::load(tx, actor, dialogue, false).await
        {
            Ok(current) => current,
            Err(DialogueStoreError::Unavailable | DialogueStoreError::Inactive) => {
                return Ok(false);
            }
            Err(DialogueStoreError::Infrastructure) => {
                return Err(PostgresStandingGoalError::DependencyUnavailable);
            }
            Err(_) => return Err(PostgresStandingGoalError::Corrupt),
        };
        if current.agent.status != OwnedAgentStatus::Active
            || current.agent.agent_id != agent
            || current.agent.space_id != binding.scope.space_id
        {
            return Ok(false);
        }
    }
    Ok(true)
}

impl OwnerGoalExecutionSource for PostgresStandingGoalProductStore {
    fn create_draft(
        &self,
        request: CreateOwnerGoalDraft,
    ) -> ApplicationFuture<'_, Result<OwnerGoalDraftReceipt, DialogueStoreError>> {
        Box::pin(creation::create(self, request))
    }

    fn read_draft_receipt(
        &self,
        actor: Actor,
        command_id: CommandId,
    ) -> ApplicationFuture<'_, Result<Option<OwnerGoalDraftReceipt>, DialogueStoreError>> {
        Box::pin(creation::read(self, actor, command_id))
    }

    fn prepare(
        &self,
        request: OwnerGoalContinuationRequest,
    ) -> ApplicationFuture<'_, Result<OwnerGoalExecutionPreparation, DialogueStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Admission,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            let value = preparation::load(&mut tx, request, false).await?;
            tx.commit()
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            Ok(value)
        })
    }
}

pub(crate) async fn load_binding(
    tx: &mut PgConnection,
    goal: &StandingGoal,
) -> Result<Option<OwnerGoalBinding>, PostgresStandingGoalError> {
    let row = sqlx::query("SELECT s.agent_id,s.dialogue_id,s.goal_binding_revision,s.requesting_agent_id,s.source_dialogue_id,a.space_id
        FROM agent.dialogue_sessions s JOIN agent.owned_agents a ON a.agent_id=s.agent_id
        JOIN agent.owner_spaces space ON space.space_id=a.space_id AND space.tenant_id=s.tenant_id AND space.owner_id=s.owner_id
        WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.source_goal_id=$3 AND s.conversation_id=$4 AND s.subject_kind='goal'")
        .bind(goal.tenant_id.into_uuid()).bind(goal.owner.into_uuid()).bind(goal.goal_id.into_uuid())
        .bind(goal.conversation_id.into_uuid()).fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(None) };
    let invalid = |_| PostgresStandingGoalError::Corrupt;
    let value = OwnerGoalBinding {
        scope: OwnerGoalScope {
            tenant_id: goal.tenant_id,
            owner_id: goal.owner,
            goal_id: goal.goal_id,
            space_id: OwnerSpaceId::from_uuid(row.try_get("space_id")?).map_err(invalid)?,
            agent_id: AgentId::from_uuid(row.try_get("agent_id")?).map_err(invalid)?,
            dialogue_id: AgentDialogueId::from_uuid(row.try_get("dialogue_id")?)
                .map_err(invalid)?,
        },
        revision: u64::try_from(row.try_get::<i64, _>("goal_binding_revision")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
        requesting_agent_id: AgentId::from_uuid(row.try_get("requesting_agent_id")?)
            .map_err(invalid)?,
        source_dialogue_id: AgentDialogueId::from_uuid(row.try_get("source_dialogue_id")?)
            .map_err(invalid)?,
    };
    value
        .validate(goal)
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    Ok(Some(value))
}

async fn insert_context(
    tx: &mut PgConnection,
    request: &CreateOwnerGoalDraft,
    name: &str,
    coordinator_name: &str,
) -> Result<(), DialogueStoreError> {
    let scope = request.binding.scope;
    let at = request.goal.created_at.unix_millis();
    insert_private_context(
        tx,
        scope,
        scope.context_identities(),
        scope.agent_id,
        name,
        scope.purpose(),
        at,
    )
    .await?;
    insert_private_context(
        tx,
        scope,
        request.binding.coordinator_context_identities(),
        request.binding.requesting_agent_id,
        coordinator_name,
        musteragent_conversation::ConversationPurpose::OwnerGoalCoordination,
        at,
    )
    .await
}

async fn insert_private_context(
    tx: &mut PgConnection,
    scope: OwnerGoalScope,
    ids: musteragent_runtime_session::OwnerGoalContextIdentities,
    executing_agent: AgentId,
    name: &str,
    purpose: musteragent_conversation::ConversationPurpose,
    at: i64,
) -> Result<(), DialogueStoreError> {
    sqlx::query("INSERT INTO conversation.conversations(conversation_id,tenant_id,workspace_id,kind,title,control_home_kind,control_device_id,home_epoch,lifecycle,created_by,created_at_ms,updated_at_ms,archived_at_ms,execution_owner_participant_id,purpose)
        VALUES($1,$2,NULL,4,NULL,1,NULL,1,1,$3,$4,$4,NULL,$5,$6)")
        .bind(ids.conversation_id.into_uuid()).bind(scope.tenant_id.into_uuid()).bind(scope.owner_id.into_uuid())
        .bind(at).bind(ids.executor.into_uuid()).bind(purpose.as_str()).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,agent_ref,service_ref,display_name,avatar_content_id,joined_at_ms,left_at_ms)
        VALUES($1,$2,$3,3,NULL,NULL,'musteragent.owner-goal@1','Dialogue goal',NULL,$4,NULL),
        ($5,$2,$3,2,NULL,$6,NULL,$7,NULL,$4,NULL),($8,$2,$3,1,$9,NULL,NULL,'User',NULL,$4,NULL)")
        .bind(ids.requested_by.into_uuid()).bind(scope.tenant_id.into_uuid()).bind(ids.conversation_id.into_uuid()).bind(at)
        .bind(ids.executor.into_uuid()).bind(executing_agent.into_uuid()).bind(name).bind(ids.user.into_uuid())
        .bind(scope.owner_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
    Ok(())
}
async fn insert_session(
    tx: &mut PgConnection,
    request: &CreateOwnerGoalDraft,
) -> Result<(), DialogueStoreError> {
    let b = &request.binding;
    let s = b.scope;
    sqlx::query("INSERT INTO agent.dialogue_sessions(conversation_id,tenant_id,owner_id,agent_id,dialogue_id,
        source_intent_id,purpose,context_epoch,context_seq,created_at_ms,pinned_agent_version_id,permission_revision,
        manifest_revision,instructions_revision,conversation_purpose,subject_kind,source_dialogue_id,requesting_agent_id,
        source_goal_id,goal_binding_revision,goal_coordinator_conversation_id)
        VALUES($1,$2,$3,$4,$5,NULL,'goal_execution',NULL,NULL,$6,NULL,NULL,NULL,NULL,'owner_goal_execution','goal',$7,$8,$9,$10,$11)")
        .bind(s.conversation_id().into_uuid()).bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(s.agent_id.into_uuid())
        .bind(s.dialogue_id.into_uuid()).bind(request.goal.created_at.unix_millis()).bind(b.source_dialogue_id.into_uuid())
        .bind(b.requesting_agent_id.into_uuid()).bind(s.goal_id.into_uuid()).bind(b.revision as i64)
        .bind(b.coordinator_context_identities().conversation_id.into_uuid())
        .execute(&mut *tx).await.map_err(db)?;
    Ok(())
}
