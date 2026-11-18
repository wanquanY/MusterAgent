//! Stable local-to-canonical identity and immutable producer proof, shared by
//! direct delivery and verified Task delivery. The transport has no effect on either.
use musteragent_artifact::ArtifactKind;
use musteragent_content_ref::Sha256;
use musteragent_identity::{ArtifactId, VersionNumber};
use musteragent_runtime_session::{
    DeviceArtifactExport, DeviceDeliveryBinding, DialogueToolPublicationError as Failure,
};
pub(crate) fn canonical_artifact(
    binding: &DeviceDeliveryBinding,
    entry: &DeviceArtifactExport,
) -> Result<ArtifactId, Failure> {
    let mut identity = b"musteragent.device-artifact@1\0".to_vec();
    identity.extend_from_slice(binding.device_id.into_uuid().as_bytes());
    identity.extend_from_slice(binding.installation_id.into_uuid().as_bytes());
    ArtifactId::from_uuid(uuid::Uuid::new_v5(
        &entry.artifact_id.into_uuid(),
        &identity,
    ))
    .map_err(|_| Failure::Corrupt)
}

/// The exact version proof is independent from the delivery invocation: another
/// legitimate delivery of the same producer version may reuse its mapping.
pub(crate) fn version_fingerprint(entry: &DeviceArtifactExport) -> Result<Sha256, Failure> {
    let p = &entry.producer;
    let r = &entry.version.content_ref;
    let mut value = serde_json::json!({
        "domain":"musteragent.device-artifact-version@1",
        "artifact":entry.artifact_id.to_string(), "version":entry.version.artifact_version_id.to_string(),
        "number":entry.version.version.get().to_string(), "activity":entry.source_activity_id.to_string(),
        "creation_run":entry.source_created_by_run_id.to_string(), "conversation":entry.source_conversation_id.to_string(),
        "created":entry.created_at.unix_millis(), "participant":entry.version.committed_by.participant_id().map(|x|x.to_string()),
        "content":r.content_id.to_string(), "hash":r.sha256.into_bytes().to_vec(),
        "media":r.media_type.as_str(), "length":r.byte_length.to_string(),
        "invocation":p.invocation_id.to_string(), "call":p.tool_call_id.as_str(), "tool":p.tool_key.as_str(),
        "result":p.result_ref.content_id.to_string(), "result_hash":p.result_ref.sha256.into_bytes().to_vec(),
        "result_media":p.result_ref.media_type.as_str(), "result_length":p.result_ref.byte_length.to_string(),
        "record":p.terminal_record_id.to_string(), "record_hash":p.terminal_record_hash.into_bytes().to_vec(),
        "sequence":p.terminal_run_sequence.get().to_string(), "terminal":p.terminal_at.unix_millis()
    });
    match entry.kind {
        // Already committed TXT proofs retain their byte-for-byte v1 identity.
        ArtifactKind::File if entry.version.dependency_refs.is_empty() => {}
        ArtifactKind::Image => {
            value["domain"] = serde_json::json!("musteragent.device-artifact-version@2");
            value["kind"] = serde_json::json!("image");
            value["dependencies"] = serde_json::Value::Array(entry.version.dependency_refs.iter()
                .map(|r| serde_json::json!({
                    "content": r.content_id.to_string(), "hash": r.sha256.into_bytes().to_vec(),
                    "media": r.media_type.as_str(), "length": r.byte_length.to_string(),
                })).collect());
        }
        _ => return Err(Failure::ReferenceUnavailable),
    }
    let bytes = serde_json::to_vec(&value).map_err(|_| Failure::Corrupt)?;
    Ok(Sha256::digest(bytes))
}

/// Source history must be complete and monotonic; canonical numbering is its
/// own authority and includes intervening user edits. No number is copied over.
pub(crate) fn next_versions(
    head: Option<(u64, u64)>,
    source: u64,
) -> Result<(Option<VersionNumber>, VersionNumber), Failure> {
    let Some((canonical, previous_source)) = head else {
        if source != 1 {
            return Err(Failure::ReferenceUnavailable);
        }
        return Ok((None, VersionNumber::new(1).map_err(|_| Failure::Corrupt)?));
    };
    if canonical < previous_source || previous_source == 0 {
        return Err(Failure::Corrupt);
    }
    if source != previous_source.checked_add(1).ok_or(Failure::Corrupt)? {
        return Err(Failure::StaleFence);
    }
    let previous = VersionNumber::new(canonical).map_err(|_| Failure::Corrupt)?;
    let next = VersionNumber::new(canonical.checked_add(1).ok_or(Failure::Corrupt)?)
        .map_err(|_| Failure::Corrupt)?;
    Ok((Some(previous), next))
}
