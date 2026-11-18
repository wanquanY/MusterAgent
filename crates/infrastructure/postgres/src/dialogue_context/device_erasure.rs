//! Read observations over the original erasure/source/content facts. This owns
//! no erasure counter, grant or new eligibility state. Hash preservation in the
//! immutable source chain is not permission to reconstruct erased material.
use super::device::accepted_attachments;
use super::*;
use futures_util::TryStreamExt;
use musteragent_runtime_session::DeviceContextState;

pub(super) async fn observe(
    tx: &mut PgConnection,
    state: &DeviceContextState,
) -> Result<Sha256, Error> {
    let cut = state.source.cut();
    // Same original provenance as Privacy's public closure: execution/report
    // publication, partner input and a Task command's Interaction. The Task
    // admission fence is checked before physical tombstones have been written.
    let pending: bool = sqlx::query_scalar("WITH entries AS (
        SELECT e.* FROM agent.dialogue_context_source_entries s
        JOIN agent.agent_dialogue_entries e USING(entry_id,tenant_id,owner_id,dialogue_id,epoch,seq)
        WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$4
          AND s.epoch=$5 AND s.seq<=$6 AND e.kind<>'erased'
    ), executions AS (
        SELECT tenant_id,owner_id,work_item_id,run_id,conversation_id FROM collaboration.work_item_execution_reservations
        UNION ALL SELECT tenant_id,owner_id,work_item_id,report_run_id,report_conversation_id FROM collaboration.work_item_execution_reports
    ), affected AS (
        SELECT x.work_item_id FROM entries e JOIN agent.dialogue_publications p
          ON p.entry_id=e.entry_id AND p.tenant_id=e.tenant_id
        JOIN executions x ON x.tenant_id=e.tenant_id AND x.owner_id=e.owner_id
          AND x.run_id=p.source_run_id AND x.conversation_id=p.source_conversation_id
        UNION SELECT x.work_item_id FROM entries e JOIN collaboration.work_item_execution_reservations x
          ON x.partner_input_entry_id=e.entry_id AND x.tenant_id=e.tenant_id
          AND x.owner_id=e.owner_id AND x.executing_dialogue_id=e.dialogue_id
        UNION SELECT c.work_item_id FROM entries e JOIN approval.interactions i
          ON i.interaction_id=e.interaction_id AND i.tenant_id=e.tenant_id
        JOIN collaboration.work_item_commands c ON c.source_invocation_id=i.requested_by_invocation_id
          AND c.source_run_id=i.run_id AND c.tenant_id=e.tenant_id AND c.owner_id=e.owner_id
    ) SELECT EXISTS(SELECT 1 FROM affected x JOIN collaboration.work_items t USING(work_item_id)
        WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.erasure_job_id IS NOT NULL)")
        .bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid()).bind(cut.agent_id.into_uuid())
        .bind(cut.dialogue_id.into_uuid()).bind(integer(cut.through.epoch)?).bind(integer(cut.through.seq)?)
        .fetch_one(&mut *tx).await.map_err(db)?;
    if pending {
        return Err(Error::Unavailable);
    }
    use prost::Message;
    let mut source_bytes = b"musteragent.native-dialogue-context.source-observation.v1\0".to_vec();
    source_bytes
        .extend(musteragent_persistence_codec::dialogue_context_codec::cut(cut).encode_to_vec());
    source_bytes.extend(state.binding.source_digest.into_bytes());
    let mut fingerprint = Sha256::digest(source_bytes);
    let mut rows = sqlx::query(
        "SELECT e.entry_id,e.seq,e.erasure_job_id,e.erased_source_payload_sha256
        FROM agent.dialogue_context_source_entries s JOIN agent.agent_dialogue_entries e
        USING(entry_id,tenant_id,owner_id,dialogue_id,epoch,seq)
        WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$4
          AND s.epoch=$5 AND s.seq<=$6 AND e.kind='erased' ORDER BY e.seq",
    )
    .bind(cut.tenant_id.into_uuid())
    .bind(cut.owner_id.into_uuid())
    .bind(cut.agent_id.into_uuid())
    .bind(cut.dialogue_id.into_uuid())
    .bind(integer(cut.through.epoch)?)
    .bind(integer(cut.through.seq)?)
    .fetch(&mut *tx);
    while let Some(row) = rows.try_next().await.map_err(db)? {
        let mut bytes = fingerprint.into_bytes().to_vec();
        bytes.extend(
            row.try_get::<uuid::Uuid, _>("entry_id")
                .map_err(db)?
                .as_bytes(),
        );
        bytes.extend(row.try_get::<i64, _>("seq").map_err(db)?.to_be_bytes());
        bytes.extend(
            row.try_get::<uuid::Uuid, _>("erasure_job_id")
                .map_err(db)?
                .as_bytes(),
        );
        let hash = Sha256::from_bytes(
            row.try_get::<Vec<u8>, _>("erased_source_payload_sha256")
                .map_err(db)?,
        )
        .map_err(|_| Error::Corrupt)?;
        bytes.extend(hash.into_bytes());
        fingerprint = Sha256::digest(bytes);
    }
    drop(rows);
    // Existing retention graph binds body/attachment material to the exact
    // public entry or accepted user intent. Tombstoned entries have no readable
    // body and were excluded; preserved references never override Content state.
    let source_intent = match &state.source {
        musteragent_runtime_session::DeviceContextSource::Intent(p) => p.intent.intent_id,
        musteragent_runtime_session::DeviceContextSource::WorkItem(p) => {
            p.preparation().work_item.source_intent_id
        }
    };
    let current = sqlx::query("SELECT EXISTS(
        SELECT 1 FROM agent.dialogue_context_source_entries s JOIN agent.agent_dialogue_entries e
        USING(entry_id,tenant_id,owner_id,dialogue_id,epoch,seq)
        JOIN agent.dialogue_content_references r ON r.tenant_id=e.tenant_id AND r.owner_id=e.owner_id
          AND ((r.reference_kind='public_entry' AND r.reference_id=e.entry_id)
           OR (r.reference_kind='user_intent' AND r.reference_id=e.source_intent_id))
        LEFT JOIN artifact.content_refs c ON c.tenant_id=r.tenant_id AND c.content_id=r.content_id
        WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$4
          AND s.epoch=$5 AND s.seq<=$6 AND e.kind<>'erased' AND (c.state IS NULL OR c.state<>2)) AS unavailable,
        (SELECT i.reply_binding FROM agent.dialogue_user_intents i WHERE i.tenant_id=$1 AND i.owner_id=$2 AND i.intent_id=$7) AS accepted_reply_binding")
        .bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid()).bind(cut.agent_id.into_uuid())
        .bind(cut.dialogue_id.into_uuid()).bind(integer(cut.through.epoch)?).bind(integer(cut.through.seq)?)
        .bind(source_intent.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if current.try_get::<bool, _>("unavailable").map_err(db)? {
        return Err(Error::Unavailable);
    }
    let reply = crate::dialogue_codec::decode_reply_binding_bytes(
        current.try_get("accepted_reply_binding").map_err(db)?,
    )?;
    if let musteragent_runtime_session::DeviceContextSource::Intent(p) = &state.source
        && p.intent.reply_binding != reply
    {
        return Err(Error::Corrupt);
    }
    if let Some(binding) = &reply {
        let actor = Actor {
            tenant_id: cut.tenant_id,
            principal_id: cut.owner_id,
            principal_kind: musteragent_identity::PrincipalKind::User,
        };
        crate::dialogue_publication::reply::observe(tx, actor, binding.agent_id, binding).await?;
        let mut bytes = fingerprint.into_bytes().to_vec();
        bytes.extend(binding.canonical_bytes());
        fingerprint = Sha256::digest(bytes);
    }
    let accepted_body = match &state.source {
        musteragent_runtime_session::DeviceContextSource::Intent(p) => &p.intent.content_ref,
        musteragent_runtime_session::DeviceContextSource::WorkItem(p) => {
            p.preparation().input_content_ref()
        }
    };
    let mut references = accepted_attachments(&state.source);
    references.push(accepted_body);
    if let Some(binding) = &reply {
        references.extend(binding.content_refs());
    }
    references.extend(state.instructions.content_ref.iter());
    if let Some(checkpoint) = &state.checkpoint {
        references.extend([&checkpoint.summary_ref, &checkpoint.source_manifest_ref]);
    }
    // A bounded batch (accepted attachments + body + instructions + two
    // checkpoint refs), not one database round trip per attachment.
    let ids: Vec<_> = references
        .iter()
        .map(|r| r.content_id.into_uuid())
        .collect();
    let media: Vec<_> = references.iter().map(|r| r.media_type.as_str()).collect();
    let lengths: Vec<_> = references
        .iter()
        .map(|r| r.byte_length.to_string())
        .collect();
    let hashes: Vec<_> = references
        .iter()
        .map(|r| r.sha256.into_bytes().to_vec())
        .collect();
    let ready: bool = sqlx::query_scalar("SELECT NOT EXISTS(
        SELECT 1 FROM UNNEST($2::uuid[],$3::text[],$4::text[],$5::bytea[])
          expected(id,media,length,hash)
        LEFT JOIN artifact.content_refs c ON c.tenant_id=$1 AND c.content_id=expected.id
          AND c.media_type=expected.media AND c.byte_length::text=expected.length AND c.content_hash=expected.hash
        WHERE c.content_id IS NULL OR c.state<>2)")
        .bind(cut.tenant_id.into_uuid()).bind(ids).bind(media).bind(lengths).bind(hashes)
        .fetch_one(tx).await.map_err(db)?;
    if !ready {
        return Err(Error::Unavailable);
    }
    Ok(fingerprint)
}
