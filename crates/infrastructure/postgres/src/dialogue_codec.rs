//! Canonical protobuf persistence for dialogue receipts; never JSON snapshots.
use musteragent_content_ref::ContentRef;
use musteragent_definition::owned_agent::*;
use musteragent_execution_context::*;
use musteragent_identity::*;
pub(super) use musteragent_persistence_codec::dialogue_codec::intent;
use musteragent_persistence_codec::{encode_content_ref, wire};
use musteragent_policy::PermissionMode;
use musteragent_runtime_session::{
    DialogueStoreError as Error, OwnedAgentActivationDisposition, OwnedAgentActivationReceipt,
};
use prost::Message;

use sqlx::{Row, postgres::PgRow};
use std::num::NonZeroU64;

pub(crate) struct AccountOutboxEvent<'a> {
    pub(crate) topic: &'a str,
    pub(crate) aggregate: uuid::Uuid,
    pub(crate) cursor: DialogueCursor,
    pub(crate) event: &'a str,
    pub(crate) payload: wire::AccountChange,
    pub(crate) at: UtcTimestamp,
}

pub(super) fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}
pub(super) fn transaction(_: crate::PostgresTransactionError) -> Error {
    Error::Infrastructure
}
pub(super) fn required<T>(value: Option<T>) -> Result<T, Error> {
    value.ok_or(Error::Corrupt)
}

pub(super) fn nz(value: u64) -> Result<NonZeroU64, Error> {
    NonZeroU64::new(value).ok_or(Error::Corrupt)
}
pub(super) fn unsigned(value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::Corrupt)
}
pub(super) fn integer(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::RevisionConflict)
}

pub(super) fn content_bytes(value: &ContentRef) -> Vec<u8> {
    encode_content_ref(value).encode_to_vec()
}
pub(super) fn content(value: wire::ContentRef) -> Result<ContentRef, Error> {
    musteragent_persistence_codec::submission_decode::content_ref(
        Some(value),
        "dialogue.content_ref",
    )
    .map_err(|_| Error::Corrupt)
}
pub(super) fn content_from_bytes(value: Vec<u8>) -> Result<ContentRef, Error> {
    content(wire::ContentRef::decode(value.as_slice()).map_err(|_| Error::Corrupt)?)
}

pub(super) fn decode_intent(value: wire::DialogueUserIntent) -> Result<UserIntent, Error> {
    musteragent_persistence_codec::dialogue_codec::decode_intent(value).map_err(|_| Error::Corrupt)
}

pub(super) fn activation(value: &OwnedAgentActivationReceipt) -> wire::OwnedAgentActivationReceipt {
    use musteragent_persistence_codec::dialogue_codec as codec;
    wire::OwnedAgentActivationReceipt {
        space: Some(codec::space(&value.space)),
        agent: Some(codec::agent(&value.agent)),
        configuration: Some(codec::configuration(&value.configuration)),
        disposition: match value.disposition {
            OwnedAgentActivationDisposition::Created => 1,
            OwnedAgentActivationDisposition::ExistingDefault => 2,
            OwnedAgentActivationDisposition::Replayed => 3,
        },
    }
}
pub(super) fn decode_activation(
    value: wire::OwnedAgentActivationReceipt,
) -> Result<OwnedAgentActivationReceipt, Error> {
    use musteragent_persistence_codec::dialogue_codec as codec;
    Ok(OwnedAgentActivationReceipt {
        space: codec::decode_space(required(value.space)?).map_err(|_| Error::Corrupt)?,
        agent: codec::decode_agent(required(value.agent)?).map_err(|_| Error::Corrupt)?,
        configuration: codec::decode_configuration(required(value.configuration)?)
            .map_err(|_| Error::Corrupt)?,
        disposition: match value.disposition {
            1 => OwnedAgentActivationDisposition::Created,
            2 => OwnedAgentActivationDisposition::ExistingDefault,
            3 => OwnedAgentActivationDisposition::Replayed,
            _ => return Err(Error::Corrupt),
        },
    })
}
pub(super) fn space_row(row: &PgRow) -> Result<OwnerSpace, Error> {
    Ok(OwnerSpace {
        space_id: OwnerSpaceId::from_uuid(row.try_get("space_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        owner_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        product: OwnerSpaceProduct::MusterAgent,
        default_agent_id: row
            .try_get::<Option<uuid::Uuid>, _>("default_agent_id")
            .map_err(db)?
            .map(AgentId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        revision: nz(unsigned(row.try_get("revision").map_err(db)?)?)?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(db)?),
    })
}
pub(super) fn agent_row(row: &PgRow) -> Result<OwnedAgent, Error> {
    Ok(OwnedAgent {
        agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        space_id: OwnerSpaceId::from_uuid(row.try_get("space_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        dialogue_id: AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        published_version_id: AgentVersionId::from_uuid(
            row.try_get("published_version_id").map_err(db)?,
        )
        .map_err(|_| Error::Corrupt)?,
        status: match row.try_get::<&str, _>("status").map_err(db)? {
            "active" => OwnedAgentStatus::Active,
            "archived" => OwnedAgentStatus::Archived,
            "deleted" => OwnedAgentStatus::Deleted,
            _ => return Err(Error::Corrupt),
        },
        revision: nz(unsigned(row.try_get("revision").map_err(db)?)?)?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(db)?),
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms").map_err(db)?),
    })
}
pub(super) fn config_row(row: &PgRow) -> Result<OwnedAgentConfiguration, Error> {
    let result = OwnedAgentConfiguration {
        agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        manifest_revision: nz(unsigned(row.try_get("manifest_revision").map_err(db)?)?)?,
        profile: OwnedAgentProfile {
            revision: nz(unsigned(row.try_get("profile_revision").map_err(db)?)?)?,
            display_name: row.try_get("display_name").map_err(db)?,
            description: row.try_get("description").map_err(db)?,
            avatar_ref: row
                .try_get::<Option<Vec<u8>>, _>("avatar_content_ref")
                .map_err(db)?
                .map(content_from_bytes)
                .transpose()?,
        },
        instructions_revision: nz(unsigned(row.try_get("instructions_revision").map_err(db)?)?)?,
        instructions_ref: row
            .try_get::<Option<Vec<u8>>, _>("instructions_content_ref")
            .map_err(db)?
            .map(content_from_bytes)
            .transpose()?,
        permission_revision: nz(unsigned(row.try_get("permission_revision").map_err(db)?)?)?,
        permission_mode: match row.try_get::<&str, _>("permission_mode").map_err(db)? {
            "supervised" => PermissionMode::Supervised,
            "trusted" => PermissionMode::Trusted,
            "full_access" => PermissionMode::FullAccess,
            _ => return Err(Error::Corrupt),
        },
    };
    result.validate().map_err(|_| Error::Corrupt)?;
    Ok(result)
}
pub(super) fn dialogue_row(row: &PgRow) -> Result<AgentDialogue, Error> {
    Ok(AgentDialogue {
        dialogue_id: AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        owner_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        head: DialogueCursor {
            epoch: unsigned(row.try_get("epoch").map_err(db)?)?,
            seq: unsigned(row.try_get("head_seq").map_err(db)?)?,
        },
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(db)?),
    })
}

pub(super) fn require_user(actor: Actor) -> Result<(), Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(Error::Unavailable);
    }
    Ok(())
}
/// The lock serializes only retries of one command, never model execution.
pub(super) async fn lock_dialogue_command(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    command: CommandId,
) -> Result<(), Error> {
    require_user(actor)?;
    let digest = musteragent_content_ref::Sha256::digest(format!(
        "dialogue-command:{}:{}:{}",
        actor.tenant_id, actor.principal_id, command
    ))
    .into_bytes();
    let key = i64::from_be_bytes(digest[..8].try_into().map_err(|_| Error::Corrupt)?);
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    Ok(())
}

pub(super) async fn command_receipt(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    command: CommandId,
    kind: &str,
    hash: musteragent_content_ref::Sha256,
) -> Result<Option<wire::OwnerCommandReceipt>, Error> {
    lock_dialogue_command(tx, actor, command).await?;
    let row = sqlx::query(
        "SELECT command_kind,payload_sha256,receipt
                 FROM agent.owner_command_receipts
                 WHERE tenant_id=$1
                 AND owner_id=$2
                 AND command_id=$3",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(command.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    row.map(|row| {
        if row.try_get::<&str, _>("command_kind").map_err(db)? != kind
            || row.try_get::<Vec<u8>, _>("payload_sha256").map_err(db)? != hash.into_bytes()
        {
            return Err(Error::IdempotencyConflict);
        }
        wire::OwnerCommandReceipt::decode(
            row.try_get::<Vec<u8>, _>("receipt").map_err(db)?.as_slice(),
        )
        .map_err(|_| Error::Corrupt)
    })
    .transpose()
}
pub(super) async fn save_receipt(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    command: CommandId,
    kind: &str,
    hash: musteragent_content_ref::Sha256,
    receipt: wire::OwnerCommandReceipt,
    at: UtcTimestamp,
) -> Result<(), Error> {
    sqlx::query(
            "INSERT INTO agent.owner_command_receipts(tenant_id,owner_id,command_id,command_kind,payload_sha256,receipt,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(actor.tenant_id.into_uuid())
.bind(actor.principal_id.into_uuid())
.bind(command.into_uuid())
.bind(kind)
.bind(hash.into_bytes().as_slice())
.bind(receipt.encode_to_vec())
.bind(at.unix_millis())
.execute(&mut *tx).await.map_err(db)?;
    Ok(())
}
pub(super) async fn outbox(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    AccountOutboxEvent {
        topic,
        aggregate,
        cursor,
        event,
        payload,
        at,
    }: AccountOutboxEvent<'_>,
) -> Result<(), Error> {
    let account = sqlx::query(
        "UPDATE agent.owner_spaces
                 SET head_event_seq=head_event_seq+1
                 WHERE tenant_id=$1
                 AND owner_id=$2
                 AND product='musteragent'
                 RETURNING space_id,sync_epoch,head_event_seq",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    sqlx::query(
            "INSERT INTO agent.owner_outbox(outbox_id,tenant_id,owner_id,topic,aggregate_id,epoch,seq,event_kind,payload,created_at_ms,space_id,account_epoch,account_seq) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        )
        .bind(uuid::Uuid::now_v7())
.bind(actor.tenant_id.into_uuid())
.bind(actor.principal_id.into_uuid())
.bind(topic)
.bind(aggregate)
.bind(integer(cursor.epoch)?)
.bind(integer(cursor.seq)?)
.bind(event)
.bind(payload.encode_to_vec())
.bind(at.unix_millis())
.bind(account.try_get::<uuid::Uuid,_>("space_id").map_err(db)?)
.bind(account.try_get::<i64,_>("sync_epoch").map_err(db)?)
.bind(account.try_get::<i64,_>("head_event_seq").map_err(db)?)
.execute(&mut *tx).await.map_err(db)?;
    Ok(())
}

/// A hint on the existing account channel, delivered only after this transaction
/// commits. It carries no task or authority; reconnects repeat original reads.
pub(crate) async fn wake_native_execution(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
) -> Result<(), Error> {
    sqlx::query("SELECT pg_notify('musteragent_account',space_id::text) FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent'")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .execute(tx).await.map_err(db)?;
    Ok(())
}

/// Pin only ready tenant content while holding the Content Owner row lock.
/// GC takes the same row lock, so it cannot retire bytes between check and pin.
/// Sorting shared content locks avoids opposite avatar/instruction lock orders.
pub(super) async fn retain_content_refs(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent_id: AgentId,
    mut refs: Vec<(&str, uuid::Uuid, &ContentRef)>,
) -> Result<(), Error> {
    refs.sort_by_key(|(_, _, c)| c.content_id);
    for (kind, reference_id, c) in refs {
        lock_ready_content_reference(&mut *tx, actor.tenant_id, c).await?;
        sqlx::query(
            "INSERT INTO agent.dialogue_content_references(tenant_id,owner_id,agent_id,reference_kind,reference_id,content_id) VALUES($1,$2,$3,$4,$5,$6)
                 ON CONFLICT DO NOTHING",
        )
            .bind(actor.tenant_id.into_uuid())
.bind(actor.principal_id.into_uuid())
.bind(agent_id.into_uuid())
.bind(kind)
.bind(reference_id)
.bind(c.content_id.into_uuid())
.execute(&mut *tx).await.map_err(db)?;
    }
    Ok(())
}
pub(super) async fn retain_configuration(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    config: &OwnedAgentConfiguration,
) -> Result<(), Error> {
    sqlx::query(
        "DELETE
                 FROM agent.dialogue_content_references
                 WHERE tenant_id=$1
                 AND owner_id=$2
                 AND agent_id=$3
                 AND reference_kind IN('profile_avatar','instructions')",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(config.agent_id.into_uuid())
    .execute(&mut *tx)
    .await
    .map_err(db)?;
    let mut refs = Vec::new();
    if let Some(c) = &config.profile.avatar_ref {
        refs.push(("profile_avatar", config.agent_id.into_uuid(), c));
    }
    if let Some(c) = &config.instructions_ref {
        refs.push(("instructions", config.agent_id.into_uuid(), c));
    }
    retain_content_refs(tx, actor, config.agent_id, refs).await
}

pub(crate) async fn lock_ready_content_reference(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    c: &ContentRef,
) -> Result<(), Error> {
    require_ready_content(connection, tenant_id, c, true).await
}

/// Validate immutable receipt material against the replay transaction's snapshot.
/// Replaying an already committed Tool neither writes nor takes content-row locks.
pub(crate) async fn read_ready_content_reference(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    c: &ContentRef,
) -> Result<(), Error> {
    require_ready_content(connection, tenant_id, c, false).await
}

async fn require_ready_content(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    c: &ContentRef,
    lock: bool,
) -> Result<(), Error> {
    macro_rules! select_ready {
        ($suffix:literal) => {
            concat!(
                "SELECT content_id
         FROM artifact.content_refs
         WHERE tenant_id=$1
         AND content_id=$2
         AND media_type=$3
         AND byte_length=$4::bigint
         AND content_hash=$5
         AND state=2",
                $suffix
            )
        };
    }
    let exists = sqlx::query(if lock {
        select_ready!(" FOR UPDATE")
    } else {
        select_ready!("")
    })
    .bind(tenant_id.into_uuid())
    .bind(c.content_id.into_uuid())
    .bind(c.media_type.as_str())
    .bind(c.byte_length.to_string())
    .bind(c.sha256.into_bytes().as_slice())
    .fetch_optional(&mut *connection)
    .await
    .map_err(db)?;
    if exists.is_none() {
        return Err(Error::Unavailable);
    }
    Ok(())
}

pub(super) fn intent_row(r: &sqlx::postgres::PgRow) -> Result<UserIntent, Error> {
    macro_rules! id {
        ($t:ty,$key:literal) => {
            <$t>::from_uuid(r.try_get($key).map_err(db)?).map_err(|_| Error::Corrupt)?
        };
    }
    let surface = match r.try_get::<&str, _>("origin_surface").map_err(db)? {
        "web" => DialogueInputSurface::Web,
        "schedule" => DialogueInputSurface::Schedule,
        "capability" => DialogueInputSurface::Capability,
        "desktop" => DialogueInputSurface::Desktop,
        "mobile" => DialogueInputSurface::Mobile,
        _ => return Err(Error::Corrupt),
    };
    Ok(UserIntent {
        intent_id: id!(UserIntentId, "intent_id"),
        command_id: id!(CommandId, "command_id"),
        dialogue_id: id!(AgentDialogueId, "dialogue_id"),
        tenant_id: id!(TenantId, "tenant_id"),
        owner_id: id!(PrincipalId, "owner_id"),
        source_entry_id: id!(DialogueEntryId, "source_entry_id"),
        content_ref: content_from_bytes(r.try_get("content_ref").map_err(db)?)?,
        input_attachments: {
            let inputs =
                musteragent_persistence_codec::dialogue_codec::input_attachments_from_bytes(
                    r.try_get("input_attachments").map_err(db)?,
                )
                .map_err(|_| Error::Corrupt)?;
            if inputs.len()
                != usize::try_from(r.try_get::<i16, _>("input_attachment_count").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?
            {
                return Err(Error::Corrupt);
            }
            inputs
        },
        origin: DialogueInputOrigin {
            surface,
            installation_id: r
                .try_get::<Option<uuid::Uuid>, _>("origin_installation_id")
                .map_err(db)?
                .map(DeviceInstallationId::from_uuid)
                .transpose()
                .map_err(|_| Error::Corrupt)?,
        },
        observed_cursor: DialogueCursor {
            epoch: unsigned(r.try_get("observed_epoch").map_err(db)?)?,
            seq: unsigned(r.try_get("observed_seq").map_err(db)?)?,
        },
        reply_to_intent_id: r
            .try_get::<Option<uuid::Uuid>, _>("reply_to_intent_id")
            .map_err(db)?
            .map(UserIntentId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        accepted_cursor: DialogueCursor {
            epoch: unsigned(r.try_get("accepted_epoch").map_err(db)?)?,
            seq: unsigned(r.try_get("accepted_seq").map_err(db)?)?,
        },
        accepted_at: UtcTimestamp::from_unix_millis(r.try_get("accepted_at_ms").map_err(db)?),
        reply_binding: decode_reply_binding_bytes(
            r.try_get::<Option<Vec<u8>>, _>("reply_binding")
                .map_err(db)?,
        )?,
        task_target: match r.try_get::<Option<Vec<u8>>, _>("task_target") {
            Ok(Some(bytes)) => Some(
                musteragent_persistence_codec::dialogue_codec::decode_task_target(
                    wire::AgentTaskTarget::decode(bytes.as_slice()).map_err(|_| Error::Corrupt)?,
                )
                .map_err(|_| Error::Corrupt)?,
            ),
            Ok(None) => None,
            Err(error) => return Err(db(error)),
        },
    })
}

pub(crate) fn reply_binding_bytes(value: Option<&DialogueReplyBinding>) -> Option<Vec<u8>> {
    value.map(|value| {
        musteragent_persistence_codec::dialogue_codec::reply_binding(value).encode_to_vec()
    })
}
pub(crate) fn decode_reply_binding_bytes(
    value: Option<Vec<u8>>,
) -> Result<Option<DialogueReplyBinding>, Error> {
    value
        .map(|bytes| {
            if bytes.is_empty() || bytes.len() > 32 * 1024 {
                return Err(Error::Corrupt);
            }
            let value =
                wire::DialogueReplyBinding::decode(bytes.as_slice()).map_err(|_| Error::Corrupt)?;
            if value.encode_to_vec() != bytes {
                return Err(Error::Corrupt);
            }
            musteragent_persistence_codec::dialogue_codec::decode_reply_binding(value)
                .map_err(|_| Error::Corrupt)
        })
        .transpose()
}
