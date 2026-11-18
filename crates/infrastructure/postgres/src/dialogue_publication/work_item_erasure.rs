//! Erasure updates the original public position and its existing account stream.
//! Scope is admitted only by the original Task/Privacy owners.
use crate::PostgresPrivacyErasureError as Error;
use crate::privacy_erasure::WorkItemProviderScope;
use musteragent_content_ref::ContentRef;
use musteragent_execution_context::DialogueCursor;
use musteragent_identity::*;
use musteragent_persistence_codec::{dialogue_codec as codec, wire};
use prost::Message;
use sqlx::Row;

pub(crate) struct WorkItemPublicClosure {
    pub(crate) entries: Vec<uuid::Uuid>,
    pub(crate) contents: Vec<ContentRef>,
}

pub(crate) async fn collect(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
) -> Result<WorkItemPublicClosure, Error> {
    let rows = sqlx::query("SELECT e.entry_id,e.content_ref FROM agent.agent_dialogue_entries e
        WHERE e.tenant_id=$1 AND (EXISTS(SELECT 1 FROM agent.dialogue_publications p
          WHERE p.entry_id=e.entry_id AND p.tenant_id=e.tenant_id AND p.source_run_id=ANY($2))
        OR EXISTS(SELECT 1 FROM collaboration.work_item_execution_reservations x
          WHERE x.partner_input_entry_id=e.entry_id AND x.tenant_id=e.tenant_id
            AND x.owner_id=e.owner_id AND x.executing_dialogue_id=e.dialogue_id AND x.run_id=ANY($2))
        OR EXISTS(SELECT 1 FROM approval.interactions i JOIN collaboration.work_item_commands c
          ON c.source_invocation_id=i.requested_by_invocation_id AND c.source_run_id=i.run_id
          WHERE i.interaction_id=e.interaction_id AND i.tenant_id=e.tenant_id
            AND c.tenant_id=e.tenant_id AND c.owner_id=e.owner_id AND c.work_item_id=$3)
        OR EXISTS(SELECT 1 FROM agent.agent_work_creation_publications c
          WHERE c.entry_id=e.entry_id AND c.tenant_id=e.tenant_id AND c.owner_id=e.owner_id
            AND c.work_kind=1 AND c.work_id=$3))
        ORDER BY e.entry_id LIMIT 65537 FOR UPDATE OF e")
        .bind(scope.tenant()).bind(scope.runs()).bind(scope.task()).fetch_all(&mut *tx).await?;
    if rows.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let mut result = WorkItemPublicClosure {
        entries: vec![],
        contents: vec![],
    };
    for row in rows {
        result.entries.push(row.try_get("entry_id")?);
        if let Some(bytes) = row.try_get::<Option<Vec<u8>>, _>("content_ref")? {
            result.contents.push(
                crate::dialogue_codec::content_from_bytes(bytes)
                    .map_err(|_| Error::CorruptState("public content reference"))?,
            );
        }
    }
    Ok(result)
}

pub(crate) async fn erase(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
    closure: &WorkItemPublicClosure,
    job: ErasureJobId,
    now: UtcTimestamp,
) -> Result<(), Error> {
    sqlx::query("DELETE FROM agent.agent_work_creation_publications WHERE tenant_id=$1 AND work_kind=1 AND work_id=$2")
        .bind(scope.tenant()).bind(scope.task()).execute(&mut *tx).await?;
    for id in &closure.entries {
        let row=sqlx::query("UPDATE agent.agent_dialogue_entries e SET kind='erased',
            erasure_job_id=$3,erased_source_payload_sha256=s.payload_sha256,
            visible_revision=e.visible_revision+1,partner_creation_command_id=NULL,
            partner_agent_id=NULL,partner_dialogue_id=NULL,partner_display_name=NULL,
            partner_description=NULL,partner_avatar_ref=NULL,partner_profile_revision=NULL
            FROM agent.dialogue_context_source_entries s
            WHERE e.entry_id=$1 AND e.tenant_id=$2 AND s.entry_id=e.entry_id AND e.kind<>'erased' RETURNING e.*")
            .bind(id).bind(scope.tenant()).bind(job.into_uuid()).fetch_optional(&mut *tx).await?;
        let Some(row) = row else { continue };
        let entry = crate::agent_dialogue::entry_row(&row)
            .map_err(|_| Error::CorruptState("erased public entry"))?;
        // Historical stream copies can include inline partner names. Replace
        // their payload with the same tombstone while preserving account cursors.
        let payload = wire::AccountChange {
            event: Some(wire::account_change::Event::PublicEntryChanged(
                codec::entry(&entry),
            )),
        };
        sqlx::query("UPDATE agent.owner_outbox SET payload=$7,event_kind='public_entry_changed'
            WHERE tenant_id=$1 AND owner_id=$2 AND ((topic='agent_dialogue' AND aggregate_id=$3 AND epoch=$4 AND seq=$5 AND event_kind='public_entry_published') OR (topic='dialogue_entry' AND aggregate_id=$6))")
            .bind(scope.tenant()).bind(entry.owner_id.into_uuid()).bind(entry.dialogue_id.into_uuid())
            .bind(i64::try_from(entry.cursor.epoch).map_err(|_|Error::IntegerOverflow("epoch"))?)
            .bind(i64::try_from(entry.cursor.seq).map_err(|_|Error::IntegerOverflow("seq"))?)
            .bind(entry.entry_id.into_uuid()).bind(payload.encode_to_vec()).execute(&mut *tx).await?;
        crate::dialogue_codec::outbox(
            tx,
            Actor {
                tenant_id: entry.tenant_id,
                principal_id: entry.owner_id,
                principal_kind: PrincipalKind::User,
            },
            crate::dialogue_codec::AccountOutboxEvent {
                topic: "dialogue_entry",
                aggregate: entry.entry_id.into_uuid(),
                cursor: DialogueCursor {
                    epoch: 1,
                    seq: entry.visible_revision.get(),
                },
                event: "public_entry_changed",
                payload,
                at: now,
            },
        )
        .await
        .map_err(|_| Error::CorruptState("erasure account change"))?;
    }
    sqlx::query("DELETE FROM agent.dialogue_content_references WHERE tenant_id=$1 AND reference_kind='public_entry' AND reference_id=ANY($2)")
        .bind(scope.tenant()).bind(&closure.entries).execute(&mut *tx).await?;
    Ok(())
}
