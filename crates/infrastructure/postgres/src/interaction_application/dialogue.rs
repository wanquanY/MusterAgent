//! Account authority is derived from a published Dialogue entry and its original
//! Interaction/TaskSession. Caller-provided surface or conversation IDs cannot
//! grant device presence or a session-wide approval.
use super::*;
use musteragent_approval::{ApprovalDecision, InteractionKind, InteractionResponseRef};
use musteragent_content_ref::Sha256;
use musteragent_identity::{AgentDialogueId, DialogueEntryId};
use musteragent_ledger::{AppendDisposition, AppendReceipt};
use musteragent_runtime_session::{
    CommandReplayQuery, DialogueInteractionAction as Action, DialogueInteractionAuthority,
    DialogueInteractionTarget,
};
use sqlx::PgConnection;
use std::num::NonZeroU64;

pub(crate) struct Binding {
    pub interaction_id: InteractionId,
    pub conversation_id: musteragent_identity::ConversationId,
    pub visible_revision: NonZeroU64,
    pub control_revision: Sha256,
    pub actions: Vec<Action>,
}

pub(crate) async fn binding(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
) -> Result<Binding, PostgresInteractionError> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(PostgresInteractionError::ActorForbidden);
    }
    let query = format!("SELECT i.interaction_id,i.conversation_id,i.kind,i.status,
        i.expires_at_ms,i.prompt_content_id,i.response_schema_key,i.response_schema_hash,
        p.content_hash,e.visible_revision,e.author_agent_id,s.permission_revision AS pinned_permission,
        cfg.permission_revision,cfg.permission_mode,r.execution_home_kind,r.execution_device_id,
        r.control_home_kind,r.control_device_id,r.home_epoch::text AS home_epoch,
        r.status AS run_status,r.cancel_requested_at_ms, ({}) AS pending
 {} WHERE {} AND e.dialogue_id=$3 AND e.entry_id=$4",
        super::dialogue_pending::PENDING, super::dialogue_pending::FROM, super::dialogue_pending::VISIBLE);
    let row = sqlx::query(&query)
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(dialogue.into_uuid())
        .bind(entry.into_uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(PostgresInteractionError::ActorForbidden)?;
    binding_row(actor, dialogue, entry, &row)
}
pub(crate) fn binding_row(
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
    row: &sqlx::postgres::PgRow,
) -> Result<Binding, PostgresInteractionError> {
    let mut digest = b"musteragent.dialogue-interaction-control.v1\0".to_vec();
    for id in [
        actor.tenant_id.into_uuid(),
        actor.principal_id.into_uuid(),
        dialogue.into_uuid(),
        entry.into_uuid(),
        row.try_get("interaction_id")?,
        row.try_get("conversation_id")?,
        row.try_get("author_agent_id")?,
        row.try_get("prompt_content_id")?,
    ] {
        digest.extend_from_slice(id.as_bytes());
    }
    for field in ["content_hash", "response_schema_hash"] {
        let bytes: Vec<u8> = row.try_get(field)?;
        digest.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        digest.extend_from_slice(&bytes);
    }
    for field in ["response_schema_key", "permission_mode", "home_epoch"] {
        let value: String = row.try_get(field)?;
        digest.extend_from_slice(&(value.len() as u64).to_be_bytes());
        digest.extend_from_slice(value.as_bytes());
    }
    for field in [
        "visible_revision",
        "pinned_permission",
        "permission_revision",
    ] {
        digest.extend_from_slice(&row.try_get::<i64, _>(field)?.to_be_bytes());
    }
    for field in ["kind", "status", "execution_home_kind", "control_home_kind"] {
        digest.extend_from_slice(&row.try_get::<i16, _>(field)?.to_be_bytes());
    }
    for field in ["execution_device_id", "control_device_id"] {
        let value: Option<uuid::Uuid> = row.try_get(field)?;
        digest.push(u8::from(value.is_some()));
        if let Some(value) = value {
            digest.extend_from_slice(value.as_bytes());
        }
    }
    let expires: Option<i64> = row.try_get("expires_at_ms")?;
    digest.push(u8::from(expires.is_some()));
    if let Some(value) = expires {
        digest.extend_from_slice(&value.to_be_bytes());
    }
    let pending: bool = row.try_get("pending")?;
    let cloud = row.try_get::<i16, _>("execution_home_kind")? == 1
        && row
            .try_get::<Option<uuid::Uuid>, _>("execution_device_id")?
            .is_none();
    let actions = if !pending {
        vec![]
    } else {
        match crate::interaction::decode_kind(row.try_get("kind")?)? {
            InteractionKind::Approval if cloud => vec![Action::ApproveOnce, Action::Deny],
            InteractionKind::Clarification | InteractionKind::Selection => {
                vec![Action::Answer, Action::Cancel]
            }
            InteractionKind::Confirmation => vec![Action::ApproveOnce, Action::Deny],
            InteractionKind::OAuth
                if cloud
                    && row.try_get::<String, _>("response_schema_key")?
                        == musteragent_runtime_interaction::CAPABILITY_RESPONSE_SCHEMA =>
            {
                vec![Action::OpenSecureFlow, Action::Cancel]
            }
            InteractionKind::Credential | InteractionKind::OAuth => vec![Action::Cancel],
            _ => vec![],
        }
    };
    Ok(Binding {
        interaction_id: InteractionId::from_uuid(row.try_get("interaction_id")?)
            .map_err(|_| PostgresInteractionError::Corrupt)?,
        conversation_id: musteragent_identity::ConversationId::from_uuid(
            row.try_get("conversation_id")?,
        )
        .map_err(|_| PostgresInteractionError::Corrupt)?,
        visible_revision: NonZeroU64::new(
            u64::try_from(row.try_get::<i64, _>("visible_revision")?)
                .map_err(|_| PostgresInteractionError::Corrupt)?,
        )
        .ok_or(PostgresInteractionError::Corrupt)?,
        control_revision: Sha256::digest(digest),
        actions,
    })
}

/// Ledger lane is already held. Run is locked before any account row so this
/// has the same lock order as publication, request, cancellation and dispatch.
pub(crate) async fn lock_and_authorize(
    tx: &mut PgConnection,
    actor: Actor,
    target: DialogueInteractionTarget,
    interaction: InteractionId,
    run: musteragent_identity::RunId,
    response: Option<&InteractionResponseRef>,
) -> Result<(), PostgresInteractionError> {
    sqlx::query("SELECT run_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE")
        .bind(actor.tenant_id.into_uuid())
        .bind(run.into_uuid())
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("SELECT space_id FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent' FOR UPDATE")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await?
        .ok_or(PostgresInteractionError::ActorForbidden)?;
    sqlx::query("SELECT a.agent_id FROM agent.owned_agents a JOIN agent.owned_agent_configurations cfg USING(agent_id) WHERE a.dialogue_id=$1 FOR SHARE OF a,cfg")
        .bind(target.dialogue_id.into_uuid()).fetch_optional(&mut *tx).await?.ok_or(PostgresInteractionError::ActorForbidden)?;
    let bound = binding(tx, actor, target.dialogue_id, target.entry_id).await?;
    if bound.interaction_id != interaction {
        return Err(PostgresInteractionError::ActorForbidden);
    }
    if bound.control_revision != target.expected_control_revision {
        return Err(PostgresInteractionError::IdempotencyConflict);
    }
    let action = match response {
        None => Action::Cancel,
        Some(InteractionResponseRef::Content(_)) => Action::Answer,
        Some(InteractionResponseRef::Secure(_)) => Action::OpenSecureFlow,
        Some(InteractionResponseRef::Approval(ApprovalDecision::Approve)) => Action::ApproveOnce,
        Some(InteractionResponseRef::Approval(ApprovalDecision::Deny)) => Action::Deny,
        _ => return Err(PostgresInteractionError::ActorForbidden),
    };
    if !bound.actions.contains(&action) {
        return Err(PostgresInteractionError::ActorForbidden);
    }
    Ok(())
}

pub(crate) async fn ordinary(
    tx: &mut PgConnection,
    tenant: musteragent_identity::TenantId,
    conversation: musteragent_identity::ConversationId,
) -> Result<(), PostgresInteractionError> {
    let public:bool=sqlx::query_scalar("SELECT purpose='standard' FROM conversation.conversations WHERE tenant_id=$1 AND conversation_id=$2")
        .bind(tenant.into_uuid()).bind(conversation.into_uuid()).fetch_one(&mut *tx).await?;
    if public {
        Ok(())
    } else {
        Err(PostgresInteractionError::ActorForbidden)
    }
}

impl PostgresInteractionApplicationStore {
    pub(super) async fn dialogue_authority(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        correlation: CorrelationId,
    ) -> Result<DialogueInteractionAuthority, RuntimeError> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Interaction,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| dependency(correlation))?;
        let bound = binding(&mut tx, actor, dialogue, entry)
            .await
            .map_err(|e| map_interaction(e, correlation))?;
        let authority = self
            .load_authority_scoped_on(&mut tx, actor, bound.interaction_id, correlation, true)
            .await?;
        let memory_execution =
            super::owner_memory::execution(&mut tx, actor, &authority.interaction)
                .await
                .map_err(|e| map_interaction(e, correlation))?;
        Ok(DialogueInteractionAuthority {
            authority,
            dialogue_id: dialogue,
            entry_id: entry,
            visible_revision: bound.visible_revision,
            control_revision: bound.control_revision,
            allowed_actions: bound.actions,
            memory_execution,
        })
    }
    pub(super) async fn dialogue_replay(
        &self,
        actor: Actor,
        target: DialogueInteractionTarget,
        query: CommandReplayQuery,
        correlation: CorrelationId,
    ) -> Result<Option<AppendReceipt>, RuntimeError> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Interaction,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| dependency(correlation))?;
        let bound = binding(&mut tx, actor, target.dialogue_id, target.entry_id)
            .await
            .map_err(|e| map_interaction(e, correlation))?;
        if query.tenant_id != actor.tenant_id || query.conversation_id != bound.conversation_id {
            return Err(map_interaction(
                PostgresInteractionError::ActorForbidden,
                correlation,
            ));
        }
        let row=sqlx::query("SELECT command_fingerprint,conversation_id,home_epoch::text AS home_epoch,first_sequence::text AS first_sequence,last_sequence::text AS last_sequence,committed_at_ms FROM event_store.command_idempotency WHERE tenant_id=$1 AND idempotency_key=$2")
            .bind(actor.tenant_id.into_uuid()).bind(query.idempotency_key.as_str()).fetch_optional(&mut *tx).await.map_err(|_|dependency(correlation))?;
        let Some(row) = row else { return Ok(None) };
        if row
            .try_get::<Vec<u8>, _>("command_fingerprint")
            .map_err(|_| corrupt(correlation))?
            != query.command_fingerprint.into_bytes()
            || row
                .try_get::<uuid::Uuid, _>("conversation_id")
                .map_err(|_| corrupt(correlation))?
                != bound.conversation_id.into_uuid()
            || row
                .try_get::<String, _>("home_epoch")
                .map_err(|_| corrupt(correlation))?
                != query.home_epoch.get().to_string()
        {
            return Err(map_interaction(
                PostgresInteractionError::IdempotencyConflict,
                correlation,
            ));
        }
        let seq = |name: &str| -> Result<musteragent_identity::ConversationSeq, RuntimeError> {
            musteragent_identity::ConversationSeq::new(
                parse_u64(row.try_get(name).map_err(|_| corrupt(correlation))?)
                    .map_err(|_| corrupt(correlation))?,
            )
            .map_err(|_| corrupt(correlation))
        };
        Ok(Some(AppendReceipt {
            disposition: AppendDisposition::Duplicate,
            first_sequence: seq("first_sequence")?,
            last_sequence: seq("last_sequence")?,
            committed_at: musteragent_identity::UtcTimestamp::from_unix_millis(
                row.try_get("committed_at_ms")
                    .map_err(|_| corrupt(correlation))?,
            ),
        }))
    }
}

impl PostgresInteractionApplicationStore {
    /// Read the original cloud command, scoped by the public Interaction entry.
    /// Native controls retain their existing inbox and ACK lookup.
    pub(super) async fn dialogue_control_receipt(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        command: musteragent_identity::CommandId,
    ) -> Result<Option<musteragent_runtime_session::DialogueInteractionControlReceipt>, RuntimeError>
    {
        use musteragent_runtime_contract::{CommandAck, CommandDisposition, ResourceRef};
        use musteragent_runtime_session::{
            DialogueInteractionControlDelivery, DialogueInteractionControlReceipt,
        };
        let correlation = CorrelationId::generate();
        let mut conn = self
            .database
            .pool()
            .acquire()
            .await
            .map_err(|_| dependency(correlation))?;
        let cloud: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_publications p JOIN agent.agent_dialogue_entries e USING(entry_id) WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.dialogue_id=$3 AND e.entry_id=$4 AND p.execution_home='cloud')")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(dialogue.into_uuid()).bind(entry.into_uuid()).fetch_one(&mut *conn).await.map_err(|_|dependency(correlation))?;
        if !cloud {
            return Ok(None);
        }
        let bound = binding(&mut conn, actor, dialogue, entry)
            .await
            .map_err(|e| map_interaction(e, correlation))?;
        let row = sqlx::query("SELECT c.first_sequence::text AS first_sequence,c.last_sequence::text AS last_sequence,c.committed_at_ms FROM event_store.command_idempotency c JOIN approval.interaction_responses response ON response.interaction_id=$4 JOIN event_store.ledger_records r ON r.record_id=response.response_ledger_record_id AND r.tenant_id=c.tenant_id AND r.conversation_id=c.conversation_id AND r.conversation_seq BETWEEN c.first_sequence AND c.last_sequence WHERE c.tenant_id=$1 AND c.idempotency_key=$2 AND c.conversation_id=$3")
            .bind(actor.tenant_id.into_uuid()).bind(format!("dialogue-interaction:{}:{}",actor.principal_id,command)).bind(bound.conversation_id.into_uuid()).bind(bound.interaction_id.into_uuid())
            .fetch_optional(&mut *conn).await.map_err(|_|dependency(correlation))?
            .ok_or_else(|| runtime_error(ErrorCode::InteractionNotFound,correlation,false,"Original interaction command has not been committed"))?;
        let seq = |name: &str| -> Result<musteragent_identity::ConversationSeq, RuntimeError> {
            musteragent_identity::ConversationSeq::new(
                parse_u64(row.try_get(name).map_err(|_| corrupt(correlation))?)
                    .map_err(|_| corrupt(correlation))?,
            )
            .map_err(|_| corrupt(correlation))
        };
        Ok(Some(DialogueInteractionControlReceipt {
            command_id: command,
            delivery: DialogueInteractionControlDelivery::Applied(CommandAck {
                command_id: command,
                disposition: CommandDisposition::Duplicate,
                first_conversation_seq: Some(seq("first_sequence")?),
                last_conversation_seq: Some(seq("last_sequence")?),
                committed_at: musteragent_identity::UtcTimestamp::from_unix_millis(
                    row.try_get("committed_at_ms")
                        .map_err(|_| corrupt(correlation))?,
                ),
                resource_refs: vec![ResourceRef::Interaction(bound.interaction_id)],
            }),
        }))
    }
}
