//! Device-to-canonical version identity lives in the original Artifact owner.
use super::*;
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
};

use crate::artifact::device_export_identity::{
    canonical_artifact, next_versions, version_fingerprint,
};
pub(super) async fn register(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    delivery: &DeviceToolDelivery,
    manifest: &DeviceArtifactExportManifest,
    entry: &DeviceArtifactExport,
    now: UtcTimestamp,
) -> Result<(ArtifactId, ArtifactVersionId, bool), Failure> {
    let artifact_id = canonical_artifact(delivery.binding(), entry)?;
    let version_id = ArtifactVersionId::from_uuid(uuid::Uuid::new_v5(
        &artifact_id.into_uuid(),
        entry.version.artifact_version_id.into_uuid().as_bytes(),
    ))
    .map_err(|_| Failure::Corrupt)?;
    let version_proof = version_fingerprint(entry)?;
    let existing = sqlx::query(
        "SELECT v.artifact_id,v.artifact_version_id,v.version_fingerprint
        FROM artifact.device_versions v WHERE v.tenant_id=$1 AND v.device_id=$2
         AND v.installation_id=$3 AND v.source_artifact_id=$4 AND v.source_version_id=$5",
    )
    .bind(source.actor.tenant_id.into_uuid())
    .bind(delivery.binding().device_id.into_uuid())
    .bind(delivery.binding().installation_id.into_uuid())
    .bind(entry.artifact_id.into_uuid())
    .bind(entry.version.artifact_version_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(database_error)?;
    if let Some(existing) = existing {
        if existing
            .try_get::<uuid::Uuid, _>("artifact_id")
            .map_err(database_error)?
            != artifact_id.into_uuid()
            || existing
                .try_get::<uuid::Uuid, _>("artifact_version_id")
                .map_err(database_error)?
                != version_id.into_uuid()
            || existing
                .try_get::<Vec<u8>, _>("version_fingerprint")
                .map_err(database_error)?
                != version_proof.into_bytes()
        {
            return Err(Failure::Forbidden);
        }
        // A deleted/trash source cannot be revived by resending an old version.
        let live: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM artifact.artifacts a
            WHERE a.artifact_id=$1 AND a.tenant_id=$2 AND a.lifecycle IN(1,2)
             AND resource.agent_artifact_owned(a.tenant_id,$3,a.artifact_id))",
        )
        .bind(artifact_id.into_uuid())
        .bind(source.actor.tenant_id.into_uuid())
        .bind(source.agent_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(database_error)?;
        if !live {
            return Err(Failure::ReferenceUnavailable);
        }
        return Ok((artifact_id, version_id, false));
    }
    let head = sqlx::query("SELECT a.current_version::text AS version,a.lifecycle,a.kind=4 AS image,
         EXISTS(SELECT 1 FROM artifact.device_versions proof
          WHERE proof.artifact_id=a.artifact_id AND proof.tenant_id=$2 AND proof.owner_id=$3 AND proof.agent_id=$4
           AND proof.dialogue_id=$5 AND proof.conversation_id=$6 AND proof.device_id=$7 AND proof.installation_id=$8
           AND proof.source_artifact_id=$9 AND proof.source_activity_id=$10 AND proof.source_created_by_run_id=$11
           AND proof.source_created_at_ms=$12) AS owned,
         (SELECT max(source_version)::text FROM artifact.device_versions WHERE artifact_id=a.artifact_id) AS source_head
        FROM artifact.artifacts a WHERE a.artifact_id=$1 FOR UPDATE")
        .bind(artifact_id.into_uuid()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid())
        .bind(source.agent_id.into_uuid()).bind(source.dialogue_id.into_uuid()).bind(source.conversation_id.into_uuid())
        .bind(delivery.binding().device_id.into_uuid()).bind(delivery.binding().installation_id.into_uuid())
        .bind(entry.artifact_id.into_uuid()).bind(entry.source_activity_id.into_uuid()).bind(entry.source_created_by_run_id.into_uuid())
        .bind(entry.created_at.unix_millis()).fetch_optional(&mut *tx).await.map_err(database_error)?;
    let (previous, next) = if let Some(head) = head {
        if !head.try_get::<bool, _>("owned").map_err(database_error)?
            || head.try_get::<bool, _>("image").map_err(database_error)?
                != (entry.kind == ArtifactKind::Image)
            || ![1, 2].contains(
                &head
                    .try_get::<i16, _>("lifecycle")
                    .map_err(database_error)?,
            )
        {
            return Err(Failure::Forbidden);
        }
        let source_head = head
            .try_get::<Option<String>, _>("source_head")
            .map_err(database_error)?
            .ok_or(Failure::Corrupt)?
            .parse::<u64>()
            .map_err(|_| Failure::Corrupt)?;
        let canonical_head = head
            .try_get::<String, _>("version")
            .map_err(database_error)?
            .parse::<u64>()
            .map_err(|_| Failure::Corrupt)?;
        next_versions(
            Some((canonical_head, source_head)),
            entry.version.version.get(),
        )?
    } else {
        next_versions(None, entry.version.version.get())?
    };
    if previous.is_none() {
        crate::artifact::insert_artifact(
            tx,
            &Artifact {
                artifact_id,
                tenant_id: source.actor.tenant_id,
                source: ArtifactSource::Conversation(source.conversation_id),
                origin: ArtifactOrigin::Runtime {
                    activity_id: entry.source_activity_id,
                    created_by_run_id: entry.source_created_by_run_id,
                },
                kind: entry.kind,
                lifecycle: ArtifactLifecycle::Published,
                current_version: next,
                title: entry.title.clone(),
                created_at: entry.created_at,
                updated_at: entry.version.committed_at,
            },
        )
        .await
        .map_err(|_| Failure::ReferenceUnavailable)?;
    }
    let p = &entry.producer;
    sqlx::query("INSERT INTO artifact.device_versions
        (artifact_version_id,artifact_id,tenant_id,owner_id,agent_id,dialogue_id,intent_id,conversation_id,run_id,
         device_id,installation_id,source_artifact_id,source_version_id,source_version,source_activity_id,
         source_created_by_run_id,source_created_at_ms,source_participant_id,producer_invocation_id,producer_call_id,
         producer_tool_key,producer_result_id,producer_result_hash,producer_result_media_type,producer_result_length,
         terminal_record_id,terminal_record_hash,terminal_run_sequence,terminal_at_ms,content_id,content_hash,
         version_fingerprint,publication_invocation_id,export_commitment,canonical_version)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::numeric,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,
         $25::numeric,$26,$27,$28::numeric,$29,$30,$31,$32,$33,$34,$35::numeric)")
        .bind(version_id.into_uuid()).bind(artifact_id.into_uuid()).bind(source.actor.tenant_id.into_uuid())
        .bind(source.actor.principal_id.into_uuid()).bind(source.agent_id.into_uuid()).bind(source.dialogue_id.into_uuid())
        .bind(source.require_intent().map_err(|_| Failure::Forbidden)?.into_uuid()).bind(source.conversation_id.into_uuid()).bind(delivery.binding().run_id.into_uuid())
        .bind(delivery.binding().device_id.into_uuid()).bind(delivery.binding().installation_id.into_uuid())
        .bind(entry.artifact_id.into_uuid()).bind(entry.version.artifact_version_id.into_uuid()).bind(entry.version.version.get().to_string())
        .bind(entry.source_activity_id.into_uuid()).bind(entry.source_created_by_run_id.into_uuid()).bind(entry.created_at.unix_millis())
        .bind(entry.version.committed_by.participant_id().ok_or(Failure::Forbidden)?.into_uuid())
        .bind(p.invocation_id.into_uuid()).bind(p.tool_call_id.as_str()).bind(p.tool_key.as_str())
        .bind(p.result_ref.content_id.into_uuid()).bind(p.result_ref.sha256.into_bytes().to_vec())
        .bind(p.result_ref.media_type.as_str()).bind(p.result_ref.byte_length.to_string())
        .bind(p.terminal_record_id.into_uuid()).bind(p.terminal_record_hash.into_bytes().to_vec())
        .bind(p.terminal_run_sequence.get().to_string()).bind(p.terminal_at.unix_millis())
        .bind(entry.version.content_ref.content_id.into_uuid()).bind(entry.version.content_hash.into_bytes().to_vec())
        .bind(version_proof.into_bytes().to_vec()).bind(delivery.invocation_id().into_uuid())
        .bind(manifest.commitment(delivery)?.into_bytes().to_vec()).bind(next.get().to_string()).execute(&mut *tx).await.map_err(database_error)?;
    let canonical = ArtifactVersion {
        artifact_version_id: version_id,
        artifact_id,
        version: next,
        content_ref: entry.version.content_ref.clone(),
        dependency_refs: entry.version.dependency_refs.clone(),
        content_hash: entry.version.content_hash,
        source_invocation_id: None, // Actual local producer is in device_versions, never Cloud FK.
        committed_by: entry.version.committed_by,
        committed_at: entry.version.committed_at,
    };
    crate::artifact::insert_version(tx, &canonical)
        .await
        .map_err(|_| Failure::ReferenceUnavailable)?;
    if let Some(previous) = previous {
        // Original head CAS; a local numeric version never overwrites Cloud head.
        crate::artifact::update_artifact(
            tx,
            source.actor.tenant_id,
            source.conversation_id,
            artifact_id,
            previous,
            &canonical,
            None,
        )
        .await
        .map_err(|_| Failure::StaleFence)?;
    } else if entry.kind == ArtifactKind::File {
        // A Resource File node owns actual file bytes. Image roots are typed
        // visual manifests and use the original Artifact custody/lifecycle.
        crate::resource::dialogue_device::place(
            tx,
            source.actor,
            source.agent_id,
            artifact_id,
            &entry.title,
            now,
        )
        .await
        .map_err(|_| Failure::ReferenceUnavailable)?;
    }
    Ok((artifact_id, version_id, previous.is_some()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_export_cannot_claim_missing_history() {
        assert_eq!(next_versions(None, 2), Err(Failure::ReferenceUnavailable));
        assert_eq!(next_versions(None, 0), Err(Failure::ReferenceUnavailable));
        assert_eq!(next_versions(None, 1).unwrap().1.get(), 1);
    }
    #[test]
    fn source_revision_never_overwrites_user_edited_canonical_head() {
        let (previous, next) = next_versions(Some((8, 3)), 4).unwrap();
        assert_eq!(previous.unwrap().get(), 8);
        assert_eq!(next.get(), 9);
    }
    #[test]
    fn stale_and_skipped_source_versions_fail_closed() {
        for source in [1, 3, 5, 9] {
            assert_eq!(
                next_versions(Some((8, 3)), source),
                Err(Failure::StaleFence)
            );
        }
    }
    #[test]
    fn corrupt_and_exhausted_heads_cannot_wrap() {
        assert_eq!(next_versions(Some((2, 3)), 4), Err(Failure::Corrupt));
        assert_eq!(next_versions(Some((8, 0)), 1), Err(Failure::Corrupt));
        assert_eq!(next_versions(Some((u64::MAX, 1)), 2), Err(Failure::Corrupt));
        assert_eq!(
            next_versions(Some((u64::MAX, u64::MAX)), 1),
            Err(Failure::Corrupt)
        );
    }
}
