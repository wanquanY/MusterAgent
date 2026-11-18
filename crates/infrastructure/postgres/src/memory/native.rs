//! Account Memory authority for an original, authenticated Native invocation.
//! Preparation reuses the Memory owner; only its transaction publishes a head.
use super::*;
use musteragent_identity::Actor;
use musteragent_runtime_capability::{CapabilityInvocation, CapabilityResult};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_memory::{
    BuiltinMemoryTools, MemoryToolContext, MemoryToolError, MemoryToolMutation,
    MemoryToolPreparation, UserAuthoredRunOrigin,
};
use musteragent_runtime_session::{
    DeviceResourceCommand, DeviceResourceError as Error, NativeDialogueInteractionProjection,
};
use std::sync::Arc;

pub(crate) struct NativeMemoryPrepared {
    preparation: MemoryToolPreparation,
    approval: Option<NativeApproval>,
}
struct NativeApproval {
    agent: AgentId,
    actor: Actor,
    command: DeviceResourceCommand,
    projection: NativeDialogueInteractionProjection,
}

pub(crate) fn tool_error(error: MemoryToolError) -> Error {
    use MemoryToolError as E;
    match error {
        E::IdentityMismatch => Error::Forbidden,
        E::PublicationNotReady | E::InactiveMemory => Error::Conflict,
        E::Content | E::Store | E::ProgramUnavailable => Error::Infrastructure,
        E::InvalidResult | E::VersionOverflow | E::Application(_) => Error::Corrupt,
        _ => Error::InvalidArguments,
    }
}
fn storage_error(error: PostgresMemoryEvolutionError) -> Error {
    use PostgresMemoryEvolutionError as E;
    match error {
        E::MemoryConflict
        | E::MemoryVersionConflict
        | E::EvolutionConflict
        | E::DraftConflict
        | E::DraftIdempotencyConflict => Error::Conflict,
        E::ApprovalNotSatisfied | E::MemoryManagementUnauthorized => Error::Forbidden,
        E::ContentNotFound | E::MemoryNotFound | E::EvolutionNotFound => {
            Error::ReferenceUnavailable
        }
        E::Sql(_) | E::Transaction(_) | E::SourceUnavailable => Error::Infrastructure,
        _ => Error::Corrupt,
    }
}

pub(crate) async fn approval_prompt(
    database: crate::PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    invocation: &CapabilityInvocation,
    agent: AgentId,
) -> Result<(MediaType, Vec<u8>), Error> {
    let store = Arc::new(PostgresMemoryEvolutionStore::new(database));
    BuiltinMemoryTools::new(content, store.clone(), store)
        .approval_prompt(invocation, agent)
        .await
        .map_err(tool_error)?
        .ok_or(Error::InvalidArguments)
}

pub(crate) async fn prepare(
    database: crate::PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    invocation: CapabilityInvocation,
    agent: AgentId,
    command: &DeviceResourceCommand,
    now: UtcTimestamp,
) -> Result<NativeMemoryPrepared, Error> {
    let facts = command.invocation().memory.as_ref().ok_or(Error::Corrupt)?;
    let store = Arc::new(PostgresMemoryEvolutionStore::new(database.clone()));
    let tools = BuiltinMemoryTools::new(content, store.clone(), store);
    let approval = if command.operation().has_memory_approval() {
        let id = facts.approval_interaction_id.ok_or(Error::Forbidden)?;
        let mut conn = database
            .pool()
            .acquire()
            .await
            .map_err(|_| Error::Infrastructure)?;
        let (actor, owner_agent, projection) =
            crate::PostgresNativeDialogueInteractionStore::approved_memory_interaction(
                &mut conn, id,
            )
            .await
            .map_err(|_| Error::Infrastructure)?
            .ok_or(Error::Forbidden)?;
        if actor != invocation.actor
            || owner_agent != agent
            || projection.admission != *command.admission()
            || projection.invocation_id != invocation.invocation_id
            || projection.conversation_id != invocation.conversation_id
            || projection
                .interaction
                .responded_at
                .is_none_or(|at| at > now)
        {
            return Err(Error::Forbidden);
        }
        verify_user_source(&mut conn, actor, agent, command).await?;
        drop(conn);
        let (media, detail) = tools
            .approval_prompt(&invocation, agent)
            .await
            .map_err(tool_error)?
            .ok_or(Error::Corrupt)?;
        musteragent_runtime_capability::approval_prompt::verify_approval_capability(
            &projection.interaction.prompt_ref,
            &projection.prompt_bytes,
            invocation.invocation_id,
            match command.operation() {
                musteragent_runtime_session::DeviceResourceOperation::MemoryRemember => {
                    "user_memory_remember"
                }
                _ => "experience__publish__v1",
            },
            &invocation.request_ref,
            command.request_bytes(),
            &media,
            &detail,
        )
        .map_err(|_| Error::Forbidden)?;
        Some(NativeApproval {
            agent,
            actor,
            command: command.clone(),
            projection,
        })
    } else {
        None
    };
    let preparation = tools
        .prepare(MemoryToolContext {
            actor: invocation.actor,
            invocation,
            agent_id: agent,
            run_context: *command.run_context(),
            approval_interaction_id: facts.approval_interaction_id,
            user_authored_origin: facts.user_origin.as_ref().map(|o| UserAuthoredRunOrigin {
                record_id: o.record_id,
                content_ref: o.content_ref.clone(),
            }),
            evidence_record_id: facts.evidence_record_id,
            now,
        })
        .await
        .map_err(tool_error)?;
    Ok(NativeMemoryPrepared {
        preparation,
        approval,
    })
}

impl NativeApproval {
    async fn retain(&self, tx: &mut sqlx::PgConnection) -> Result<(), Error> {
        let projection = &self.projection;
        let interaction = &projection.interaction;
        let (actor, _, current) =
            crate::PostgresNativeDialogueInteractionStore::approved_memory_interaction(
                tx,
                interaction.interaction_id,
            )
            .await
            .map_err(|_| Error::Infrastructure)?
            .ok_or(Error::Forbidden)?;
        if actor != self.actor
            || current.request_fingerprint() != projection.request_fingerprint()
            || current.state_fingerprint() != projection.state_fingerprint()
        {
            return Err(Error::Forbidden);
        }
        verify_user_source(tx, self.actor, self.agent, &self.command).await?;
        let origin = self
            .command
            .invocation()
            .memory
            .as_ref()
            .and_then(|m| m.user_origin.as_ref());
        sqlx::query("INSERT INTO memory.native_tool_approvals(interaction_id,invocation_id,tenant_id,approved_by,approved_at_ms,request_content_id,prompt_content_id,prompt_content_hash,source_user_record_id,source_user_content_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(interaction.interaction_id.into_uuid()).bind(projection.invocation_id.into_uuid())
            .bind(self.actor.tenant_id.into_uuid()).bind(self.actor.principal_id.into_uuid())
            .bind(interaction.responded_at.ok_or(Error::Forbidden)?.unix_millis())
            .bind(self.command.invocation().request_ref.content_id.into_uuid())
            .bind(interaction.prompt_ref.content_id.into_uuid()).bind(interaction.prompt_ref.sha256.into_bytes().to_vec())
            .bind(origin.map(|o| o.record_id.into_uuid())).bind(origin.map(|o| o.content_ref.content_id.into_uuid()))
            .execute(tx).await.map_err(|_| Error::Infrastructure)?;
        Ok(())
    }
}

pub(crate) async fn apply(
    tx: &mut sqlx::PgConnection,
    prepared: NativeMemoryPrepared,
) -> Result<CapabilityResult, Error> {
    let NativeMemoryPrepared {
        preparation,
        approval,
    } = prepared;
    let MemoryToolPreparation::Commit { result, mutation } = preparation else {
        let MemoryToolPreparation::ReadOnly(result) = preparation else {
            unreachable!()
        };
        return Ok(result);
    };
    match *mutation {
        MemoryToolMutation::Remember(command) => {
            let approval = approval.ok_or(Error::Forbidden)?;
            if command.authorization.invocation_id != approval.projection.invocation_id
                || command.authorization.approval_interaction_id
                    != approval.projection.interaction.interaction_id
                || command.authorization.request_ref != approval.command.invocation().request_ref
                || command.actor_principal_id != approval.actor.principal_id
            {
                return Err(Error::Forbidden);
            }
            approval.retain(tx).await?;
            remember_user_transaction(tx, &command)
                .await
                .map_err(storage_error)?;
            if command.retains_current_version() {
                let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM memory.explicit_publications WHERE memory_version_id=$1 AND memory_id=$2 AND tenant_id=$3 AND approved_by=$4)")
                    .bind(command.version.memory_version_id.into_uuid()).bind(command.record.memory_id.into_uuid())
                    .bind(approval.actor.tenant_id.into_uuid()).bind(approval.actor.principal_id.into_uuid())
                    .fetch_one(tx).await.map_err(|_| Error::Infrastructure)?;
                if !exists {
                    return Err(Error::Forbidden);
                }
            } else {
                sqlx::query("INSERT INTO memory.explicit_publications(memory_version_id,memory_id,tenant_id,tool_invocation_id,approval_interaction_id,request_content_id,source_user_record_id,approved_by,approved_at_ms,published_at_ms,execution_home) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'device')")
                    .bind(command.version.memory_version_id.into_uuid()).bind(command.record.memory_id.into_uuid())
                    .bind(approval.actor.tenant_id.into_uuid()).bind(command.authorization.invocation_id.into_uuid())
                    .bind(command.authorization.approval_interaction_id.into_uuid()).bind(command.authorization.request_ref.content_id.into_uuid())
                    .bind(command.authorization.origin.record_id.into_uuid()).bind(approval.actor.principal_id.into_uuid())
                    .bind(approval.projection.interaction.responded_at.ok_or(Error::Forbidden)?.unix_millis())
                    .bind(command.version.created_at.unix_millis()).execute(tx).await.map_err(|_| Error::Infrastructure)?;
            }
        }
        MemoryToolMutation::Propose(candidate) => {
            propose_transaction(tx, &candidate)
                .await
                .map_err(storage_error)?;
        }
        MemoryToolMutation::Publish(command) => {
            let approval = approval.ok_or(Error::Forbidden)?;
            command.validate().map_err(|_| Error::Corrupt)?;
            let binding = command.tool_approval.as_ref().ok_or(Error::Forbidden)?;
            if binding.invocation_id() != approval.projection.invocation_id
                || binding.prompt_ref() != &approval.projection.interaction.prompt_ref
                || command.approval.approved_by != approval.actor.principal_id
                || Some(command.approval.approved_at)
                    != approval.projection.interaction.responded_at
            {
                return Err(Error::Forbidden);
            }
            let mut verified = command.candidate.clone();
            verified.status = CandidateStatus::Verified;
            sqlx::query("SELECT proposal_id FROM evolution.proposals WHERE proposal_id=$1 AND status=3 FOR UPDATE")
                .bind(verified.proposal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(|_| Error::Infrastructure)?.ok_or(Error::Conflict)?;
            if !proposal_matches(tx, &verified)
                .await
                .map_err(storage_error)?
                || !evaluation_matches(tx, &command.evaluation)
                    .await
                    .map_err(storage_error)?
                || !command.evaluation.passed
                || command.evaluation.evaluated_at > command.approval.approved_at
            {
                return Err(Error::Conflict);
            }
            approval.retain(tx).await?;
            crate::memory_evolution_commit::persist_authorized_evolution(tx, &command, true)
                .await
                .map_err(storage_error)?;
        }
    }
    Ok(result)
}

async fn verify_user_source(
    tx: &mut sqlx::PgConnection,
    actor: musteragent_identity::Actor,
    agent: AgentId,
    command: &DeviceResourceCommand,
) -> Result<(), Error> {
    if command.admission().work_item.is_some() {
        return Err(Error::Forbidden);
    }
    let facts = crate::agent_dialogue::capability::verified_root_id(
        tx,
        actor,
        agent,
        command.admission().intent_id,
    )
    .await
    .map_err(|e| match e {
        musteragent_runtime_session::DialogueStoreError::Infrastructure => Error::Infrastructure,
        musteragent_runtime_session::DialogueStoreError::Corrupt => Error::Corrupt,
        _ => Error::Forbidden,
    })?;
    if !facts.is_direct_user_root
        || command
            .invocation()
            .memory
            .as_ref()
            .and_then(|m| m.user_origin.as_ref())
            .is_some_and(|origin| origin.content_ref != facts.current_intent.content_ref)
    {
        return Err(Error::Forbidden);
    }
    Ok(())
}
