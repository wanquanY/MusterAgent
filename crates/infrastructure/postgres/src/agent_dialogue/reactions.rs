//! Bounded metadata join in the same authoritative history snapshot.
use super::*;
pub(super) async fn attach<'a>(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    entries: impl Iterator<Item = &'a mut DialogueHistoryEntry>,
) -> Result<(), Error> {
    let mut entries: Vec<_> = entries
        .filter(|v| {
            matches!(
                v.entry.content,
                AgentDialogueContent::UserMessage { .. }
                    | AgentDialogueContent::AgentMessage { .. }
            )
        })
        .collect();
    let ids: Vec<_> = entries
        .iter()
        .map(|v| v.entry.entry_id.into_uuid())
        .collect();
    if ids.is_empty() {
        return Ok(());
    }
    let relation = crate::public_reactions::RELATION;
    let visible = crate::public_reactions::VISIBLE;
    let rows=sqlx::query(&format!("SELECT e.entry_id,d.agent_id,COALESCE(feedback.revision,0) AS revision,COALESCE(feedback.selected_mask,0) AS selected_mask {relation} LEFT JOIN agent.public_entry_reactions feedback ON feedback.entry_id=e.entry_id WHERE e.entry_id=ANY($1) AND e.tenant_id=$2 AND e.owner_id=$3 AND {visible}"))
      .bind(ids).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_all(tx).await.map_err(db)?;
    for row in rows {
        let id: uuid::Uuid = row.try_get("entry_id").map_err(db)?;
        if let Some(entry) = entries
            .iter_mut()
            .find(|v| v.entry.entry_id.into_uuid() == id)
        {
            let mask: i32 = row.try_get("selected_mask").map_err(db)?;
            entry.reactions = Some(PublicEntryReactions {
                agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                dialogue_id: entry.entry.dialogue_id,
                entry_id: entry.entry.entry_id,
                revision: unsigned(row.try_get("revision").map_err(db)?)?,
                selected: PublicReactionSet::from_mask(
                    u16::try_from(mask).map_err(|_| Error::Corrupt)?,
                )
                .map_err(|_| Error::Corrupt)?,
            });
        }
    }
    Ok(())
}
