//! Refresh the original work directory and message; never create a replacement card.
use musteragent_identity::*;
use musteragent_persistence_codec::{dialogue_codec as codec, wire};
use musteragent_runtime_session::DialogueStoreError as Error;
use sqlx::PgConnection;
pub(crate) async fn changed(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    entry_id: Option<uuid::Uuid>,
    now: UtcTimestamp,
) -> Result<(), Error> {
    crate::agent_work_events::publish(tx, actor, agent, now).await?;
    let Some(entry_id) = entry_id else {
        // Readiness may precede the creating Run's authorized Publication.
        // That owner will attach its original message once, already visible.
        return Ok(());
    };
    let entry = sqlx::query(
        "UPDATE agent.agent_dialogue_entries SET visible_revision=visible_revision+1
        WHERE tenant_id=$1 AND owner_id=$2 AND entry_id=$3 AND kind='agent_message' RETURNING *",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(entry_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    let Some(entry) = entry else {
        return Ok(());
    }; // Never resurrect an erased message.
    let entry = crate::agent_dialogue::entry_row(&entry)?;
    crate::dialogue_codec::outbox(
        tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "dialogue_entry",
            aggregate: entry.entry_id.into_uuid(),
            cursor: musteragent_execution_context::DialogueCursor {
                epoch: 1,
                seq: entry.visible_revision.get(),
            },
            event: "public_entry_changed",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::PublicEntryChanged(
                    codec::entry(&entry),
                )),
            },
            at: now,
        },
    )
    .await
}
fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}
