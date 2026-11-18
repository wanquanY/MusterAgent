//! One bounded batch joins read metadata without changing immutable messages.
use super::*;
use musteragent_persistence_codec::dialogue_admission_progress_codec as codec;
pub(super) async fn attach<'a>(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    entries: impl Iterator<Item = &'a mut DialogueHistoryEntry>,
) -> Result<(), Error> {
    let mut entries: Vec<_> = entries
        .filter(|v| matches!(v.entry.content, AgentDialogueContent::UserMessage { .. }))
        .collect();
    let ids: Vec<_> = entries
        .iter()
        .map(|v| v.entry.entry_id.into_uuid())
        .collect();
    if ids.is_empty() {
        return Ok(());
    }
    let rows=sqlx::query("SELECT p.*,i.source_entry_id,i.dialogue_id,d.agent_id FROM agent.dialogue_admission_progress p JOIN agent.dialogue_user_intents i USING(intent_id) JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id WHERE i.source_entry_id=ANY($1) AND i.tenant_id=$2 AND i.owner_id=$3")
        .bind(ids).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_all(tx).await.map_err(db)?;
    for row in rows {
        let entry: uuid::Uuid = row.try_get("source_entry_id").map_err(db)?;
        if let Some(value) = entries
            .iter_mut()
            .find(|v| v.entry.entry_id.into_uuid() == entry)
        {
            value.admission_progress = Some(
                codec::decode(wire::DialogueAdmissionProgress {
                    intent_id: Some(wire::UserIntentId {
                        value: row
                            .try_get::<uuid::Uuid, _>("intent_id")
                            .map_err(db)?
                            .to_string(),
                    }),
                    agent_id: Some(wire::AgentId {
                        value: row
                            .try_get::<uuid::Uuid, _>("agent_id")
                            .map_err(db)?
                            .to_string(),
                    }),
                    dialogue_id: Some(wire::AgentDialogueId {
                        value: row
                            .try_get::<uuid::Uuid, _>("dialogue_id")
                            .map_err(db)?
                            .to_string(),
                    }),
                    entry_id: Some(wire::DialogueEntryId {
                        value: entry.to_string(),
                    }),
                    revision: unsigned(row.try_get("revision").map_err(db)?)?,
                    stage: i32::from(row.try_get::<i16, _>("stage").map_err(db)?),
                    blocker: i32::from(row.try_get::<i16, _>("blocker").map_err(db)?),
                    observed_at_ms: row.try_get("observed_at_ms").map_err(db)?,
                    execution_finished: row.try_get("execution_finished").map_err(db)?,
                })
                .map_err(|_| Error::Corrupt)?,
            );
        }
    }
    Ok(())
}
