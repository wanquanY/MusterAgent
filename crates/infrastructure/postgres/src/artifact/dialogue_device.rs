//! Public-entry read uses original Device custody, never a Cloud Run surrogate.
//! FileReference redelivery is an exact retained version, not a new production
//! receipt. Its public-entry content retention is committed by the original
//! locked resolver; reads still require current account/Agent/file authority.
//! Custody is the exact grant acknowledged by Native, which can differ from both
//! the immutable first proof and a newer offered authorization head.
use super::*;
use musteragent_identity::{AgentDialogueId, DialogueEntryId};

pub(super) async fn bind(
    db: &mut sqlx::PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
    allow_archived: bool,
) -> Result<Option<super::dialogue_artifact::Binding>, PostgresArtifactError> {
    let row = sqlx::query(include_str!("device_binding.sql"))
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(dialogue.into_uuid())
        .bind(entry.into_uuid())
        .bind(allow_archived)
        .fetch_optional(db)
        .await?;
    row.map(|row| {
        Ok(super::dialogue_artifact::Binding {
            returned_by: None,
            artifact: decode_artifact(&row)?,
            delivered: id(row.try_get("delivered")?)?,
            agent_id: musteragent_identity::AgentId::from_uuid(row.try_get("agent_id")?)
                .map_err(|_| PostgresArtifactError::Corrupt)?,
        })
    })
    .transpose()
}
