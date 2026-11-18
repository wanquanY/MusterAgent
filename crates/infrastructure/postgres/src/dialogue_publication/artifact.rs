//! A File head change refreshes its existing delivery positions. Delivered
//! versions and publication provenance stay immutable; no status bubble exists.
use super::*;

/// Called in the File Owner's commit while its account/Artifact locks are held.
/// Replaying a terminal receipt or saving identical bytes never calls this.
pub(crate) async fn head_changed(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    artifact_id: ArtifactId,
    dialogue_id: AgentDialogueId,
    entry_id: DialogueEntryId,
) -> Result<(), DialogueStoreError> {
    // All account writers acquire this owner first. In particular they must
    // not upgrade a shared account lock after acquiring Artifact/entry locks.
    let space = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT space_id FROM agent.owner_spaces
         WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent' FOR UPDATE",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    if actor.principal_kind != PrincipalKind::User || space.is_none() {
        return Err(DialogueStoreError::Unavailable);
    }
    let entries = sqlx::query(
        "SELECT e.entry_id FROM agent.agent_dialogue_entries e
         JOIN agent.agent_dialogues d ON d.dialogue_id=e.dialogue_id
          AND d.tenant_id=e.tenant_id AND d.owner_id=e.owner_id
         JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id
         WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.artifact_id=$3
           AND e.kind='artifact' AND a.status='active'
           AND NOT EXISTS(SELECT 1 FROM artifact.work_item_result_asset_returns returned WHERE returned.entry_id=e.entry_id AND returned.requester_id<>returned.producer_id)
         ORDER BY e.dialogue_id,e.entry_id FOR UPDATE OF e",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .fetch_all(&mut *tx)
    .await
    .map_err(db)?;
    let at = crate::ledger::database_now_ms(&mut *tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    let file = sqlx::query("SELECT a.agent_id,v.artifact_version_id,v.version::text AS version
        FROM agent.agent_dialogue_entries e JOIN agent.owned_agents a ON a.dialogue_id=e.dialogue_id
        JOIN artifact.artifacts f ON f.artifact_id=e.artifact_id AND f.tenant_id=e.tenant_id
        JOIN artifact.artifact_versions v ON v.artifact_id=f.artifact_id AND v.version=f.current_version
        WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.dialogue_id=$3 AND e.entry_id=$4 AND e.artifact_id=$5")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(dialogue_id.into_uuid()).bind(entry_id.into_uuid()).bind(artifact_id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(db)?;
    let version = file
        .try_get::<String, _>("version")
        .map_err(db)?
        .parse::<u64>()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    outbox(
        tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "agent_file",
            aggregate: artifact_id.into_uuid(),
            cursor: DialogueCursor {
                epoch: 1,
                seq: version,
            },
            event: "file_head_changed",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::FileHeadChanged(
                    wire::AgentFileHeadChanged {
                        agent_id: Some(wire::AgentId {
                            value: file
                                .try_get::<uuid::Uuid, _>("agent_id")
                                .map_err(db)?
                                .to_string(),
                        }),
                        dialogue_id: Some(wire::AgentDialogueId {
                            value: dialogue_id.to_string(),
                        }),
                        entry_id: Some(wire::DialogueEntryId {
                            value: entry_id.to_string(),
                        }),
                        artifact_id: Some(wire::ArtifactId {
                            value: artifact_id.to_string(),
                        }),
                        current_version_id: Some(wire::ArtifactVersionId {
                            value: file
                                .try_get::<uuid::Uuid, _>("artifact_version_id")
                                .map_err(db)?
                                .to_string(),
                        }),
                        version,
                    },
                )),
            },
            at,
        },
    )
    .await?;
    for row in entries {
        let entry_row = sqlx::query(
            "UPDATE agent.agent_dialogue_entries SET visible_revision=visible_revision+1
             WHERE tenant_id=$1 AND entry_id=$2 AND kind='artifact' RETURNING *",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(row.try_get::<uuid::Uuid, _>("entry_id").map_err(db)?)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let entry = crate::agent_dialogue::entry_row(&entry_row)?;
        outbox(
            tx,
            actor,
            crate::dialogue_codec::AccountOutboxEvent {
                topic: "dialogue_entry",
                aggregate: entry.entry_id.into_uuid(),
                cursor: DialogueCursor {
                    epoch: 1,
                    seq: entry.visible_revision.get(),
                },
                event: "public_entry_changed",
                payload: wire::AccountChange {
                    event: Some(wire::account_change::Event::PublicEntryChanged(
                        codec::entry(&entry),
                    )),
                },
                at,
            },
        )
        .await?;
    }
    Ok(())
}
