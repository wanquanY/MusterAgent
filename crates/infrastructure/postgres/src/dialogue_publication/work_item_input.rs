//! The original public dialogue owns a partner's incoming Agent message. It is
//! committed with task acceptance, never represented as a fresh UserIntent.
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_runtime_session::WorkItemExecutionKey;

pub(crate) async fn append(
    tx: &mut sqlx::PgConnection,
    key: WorkItemExecutionKey,
    target_dialogue: AgentDialogueId,
    reference: &ContentRef,
    at: UtcTimestamp,
) -> Result<DialogueEntryId, DialogueStoreError> {
    if key.report_from_run_id.is_some() || key.executing_agent_id == key.scope.agent_id {
        return Err(DialogueStoreError::Corrupt);
    }
    // The Task Owner already acquired space SHARE, all participating dialogues
    // UPDATE and Agents UPDATE in sorted order. Never upgrade the space lock.
    let row = sqlx::query(
        "SELECT d.* FROM agent.agent_dialogues d
        JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id
        WHERE d.dialogue_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3 AND d.agent_id=$4
        AND a.space_id=$5 AND a.status='active'",
    )
    .bind(target_dialogue.into_uuid())
    .bind(key.actor.tenant_id.into_uuid())
    .bind(key.actor.principal_id.into_uuid())
    .bind(key.executing_agent_id.into_uuid())
    .bind(key.scope.space_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(DialogueStoreError::Unavailable)?;
    let dialogue = dialogue_row(&row)?;
    let cursor = dialogue
        .head
        .next()
        .map_err(|_| DialogueStoreError::RevisionConflict)?;
    let entry_id = DialogueEntryId::from_uuid(uuid::Uuid::new_v5(
        &key.identities().command_id.into_uuid(),
        b"partner-input@1",
    ))
    .map_err(|_| DialogueStoreError::Corrupt)?;
    retain_content_refs(
        tx,
        key.actor,
        key.executing_agent_id,
        vec![("public_entry", entry_id.into_uuid(), reference)],
    )
    .await?;
    let entry = AgentDialogueEntry {
        entry_id,
        dialogue_id: target_dialogue,
        tenant_id: key.actor.tenant_id,
        owner_id: key.actor.principal_id,
        cursor,
        source_intent_id: None,
        author_agent_id: Some(key.scope.agent_id),
        visible_revision: NonZeroU64::MIN,
        content: AgentDialogueContent::AgentMessage {
            content_ref: reference.clone(),
        },
        created_at: at,
        presentation: Default::default(),
    };
    super::insert_entry(tx, &entry)
        .await
        .map_err(|error| match error {
            Error::Store(error) => error,
            _ => DialogueStoreError::Corrupt,
        })?;
    // Internal Agent input does not originate from an external channel and is
    // not sent as an outbound channel message. The original local sync outbox
    // and context source trigger still index exactly the same public entry.
    sqlx::query("UPDATE agent.agent_dialogues SET head_seq=$2 WHERE dialogue_id=$1")
        .bind(target_dialogue.into_uuid())
        .bind(integer(cursor.seq)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    outbox(
        tx,
        key.actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "agent_dialogue",
            aggregate: target_dialogue.into_uuid(),
            cursor,
            event: "public_entry_published",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::PublicEntryPublished(
                    codec::entry(&entry),
                )),
            },
            at,
        },
    )
    .await?;
    Ok(entry_id)
}
