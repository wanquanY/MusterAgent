//! Acceptance-owned authorization and immutable input snapshots. Content pins
//! use the existing sorted ready-row locks before any new references are saved.
use crate::dialogue_codec::*;
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_persistence_codec::dialogue_codec as codec;
use musteragent_runtime_content::upload::{content_upload_key, idempotent_content_id};
use musteragent_runtime_contract::AttachmentRef;
use musteragent_runtime_session::{AcceptDialogueIntent, DialogueStoreError as Error};
use prost::Message;
use sqlx::{PgConnection, Row};

pub(super) async fn authorize(
    tx: &mut PgConnection,
    command: &AcceptDialogueIntent,
) -> Result<(), Error> {
    for input in &command.input_attachments {
        match &input.source {
            DialogueInputAttachmentSource::Uploaded { upload_id } => {
                let key = content_upload_key(upload_id, None).map_err(|_| Error::Unavailable)?;
                if idempotent_content_id(command.actor, &key).map_err(|_| Error::Unavailable)?
                    != input.attachment.content_ref.content_id
                {
                    return Err(Error::Unavailable);
                }
            }
            DialogueInputAttachmentSource::AcceptedInput {
                dialogue_id,
                entry_id,
                cursor,
                ordinal,
            } => {
                if *dialogue_id == command.dialogue_id {
                    cursor
                        .require_at_or_before(command.observed_cursor)
                        .map_err(|_| Error::Unavailable)?;
                }
                let original = resolve(
                    tx,
                    command.actor,
                    *dialogue_id,
                    *entry_id,
                    *cursor,
                    *ordinal,
                )
                .await?;
                if original != input.attachment {
                    return Err(Error::Unavailable);
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn resolve(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
    at: DialogueCursor,
    ordinal: u32,
) -> Result<AttachmentRef, Error> {
    require_user(actor)?;
    if at.seq == 0 || ordinal >= 64 {
        return Err(Error::Unavailable);
    }
    at.validate().map_err(|_| Error::Unavailable)?;
    let row = sqlx::query("SELECT e.*, i.input_attachments AS intent_inputs, item.attachment_ref, item.input_ref,
        item.content_id AS item_content_id, item.media_type AS item_media_type,
        item.byte_length AS item_byte_length, item.content_hash AS item_hash, item.display_name AS item_name
      FROM agent.agent_dialogue_entries e
      JOIN agent.dialogue_user_intents i ON i.intent_id=e.source_intent_id AND i.source_entry_id=e.entry_id
       AND i.tenant_id=e.tenant_id AND i.owner_id=e.owner_id AND i.dialogue_id=e.dialogue_id
       AND i.accepted_epoch=e.epoch AND i.accepted_seq=e.seq
      JOIN agent.dialogue_input_attachments item ON item.intent_id=i.intent_id
       AND item.tenant_id=i.tenant_id AND item.owner_id=i.owner_id AND item.ordinal=$7
      JOIN agent.agent_dialogues d ON d.dialogue_id=e.dialogue_id AND d.tenant_id=e.tenant_id AND d.owner_id=e.owner_id
      JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id
      JOIN agent.owner_spaces s ON s.space_id=a.space_id AND s.tenant_id=e.tenant_id AND s.owner_id=e.owner_id
      WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.dialogue_id=$3 AND e.entry_id=$4
       AND e.epoch=$5 AND e.seq=$6 AND e.kind='user_message' AND e.author_agent_id IS NULL
       AND d.epoch=e.epoch AND d.head_seq>=e.seq AND a.status<>'deleted' AND s.product='musteragent'")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(dialogue.into_uuid()).bind(entry.into_uuid()).bind(integer(at.epoch)?).bind(integer(at.seq)?)
        .bind(i32::try_from(ordinal).map_err(|_| Error::Unavailable)?)
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let inputs = codec::input_attachments_from_bytes(
        &row.try_get::<Vec<u8>, _>("intent_inputs").map_err(db)?,
    )
    .map_err(|_| Error::Corrupt)?;
    let public =
        codec::attachments_from_bytes(&row.try_get::<Vec<u8>, _>("input_attachments").map_err(db)?)
            .map_err(|_| Error::Corrupt)?;
    if public.len() != inputs.len()
        || public.len()
            != usize::try_from(
                row.try_get::<i16, _>("input_attachment_count")
                    .map_err(db)?,
            )
            .map_err(|_| Error::Corrupt)?
    {
        return Err(Error::Corrupt);
    }
    let expected = inputs.get(ordinal as usize).ok_or(Error::Corrupt)?;
    if public.get(ordinal as usize) != Some(&expected.attachment)
        || row.try_get::<Vec<u8>, _>("attachment_ref").map_err(db)?
            != codec::attachment(&expected.attachment).encode_to_vec()
        || row.try_get::<Vec<u8>, _>("input_ref").map_err(db)?
            != codec::input_attachment(expected).encode_to_vec()
        || row
            .try_get::<uuid::Uuid, _>("item_content_id")
            .map_err(db)?
            != expected.attachment.content_ref.content_id.into_uuid()
        || row.try_get::<String, _>("item_media_type").map_err(db)?
            != expected.attachment.content_ref.media_type.as_str()
        || row.try_get::<i64, _>("item_byte_length").map_err(db)?
            != integer(expected.attachment.content_ref.byte_length)?
        || row.try_get::<Vec<u8>, _>("item_hash").map_err(db)?
            != expected.attachment.content_ref.sha256.into_bytes()
        || row.try_get::<String, _>("item_name").map_err(db)?
            != expected.attachment.display_name.as_str()
    {
        return Err(Error::Corrupt);
    }
    Ok(expected.attachment.clone())
}

pub(super) async fn save(tx: &mut PgConnection, intent: &UserIntent) -> Result<(), Error> {
    for (ordinal, input) in intent.input_attachments.iter().enumerate() {
        let attachment = &input.attachment;
        let reference = &attachment.content_ref;
        sqlx::query("INSERT INTO agent.dialogue_input_attachments(tenant_id,owner_id,intent_id,ordinal,content_id,media_type,byte_length,content_hash,display_name,attachment_ref,input_ref)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
            .bind(intent.tenant_id.into_uuid()).bind(intent.owner_id.into_uuid()).bind(intent.intent_id.into_uuid())
            .bind(i16::try_from(ordinal).map_err(|_| Error::Corrupt)?).bind(reference.content_id.into_uuid())
            .bind(reference.media_type.as_str()).bind(integer(reference.byte_length)?).bind(reference.sha256.into_bytes().to_vec())
            .bind(attachment.display_name.as_str()).bind(codec::attachment(attachment).encode_to_vec())
            .bind(codec::input_attachment(input).encode_to_vec()).execute(&mut *tx).await.map_err(db)?;
    }
    Ok(())
}

pub(super) async fn verify(tx: &mut PgConnection, intent: &UserIntent) -> Result<(), Error> {
    // The immutable receipt, provenance, normalized rows and public input agree.
    let row = sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3")
        .bind(intent.intent_id.into_uuid()).bind(intent.tenant_id.into_uuid()).bind(intent.owner_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Corrupt)?;
    if intent_row(&row)? != *intent {
        return Err(Error::Corrupt);
    }
    let actor = Actor {
        tenant_id: intent.tenant_id,
        principal_id: intent.owner_id,
        principal_kind: PrincipalKind::User,
    };
    for (ordinal, input) in intent.input_attachments.iter().enumerate() {
        if resolve(
            tx,
            actor,
            intent.dialogue_id,
            intent.source_entry_id,
            intent.accepted_cursor,
            ordinal as u32,
        )
        .await?
            != input.attachment
        {
            return Err(Error::Corrupt);
        }
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM agent.dialogue_input_attachments WHERE tenant_id=$1 AND owner_id=$2 AND intent_id=$3")
        .bind(intent.tenant_id.into_uuid()).bind(intent.owner_id.into_uuid()).bind(intent.intent_id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(db)?;
    if count != intent.input_attachments.len() as i64 {
        return Err(Error::Corrupt);
    }
    Ok(())
}
