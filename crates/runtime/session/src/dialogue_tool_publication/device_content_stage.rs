//! Command-bound reclaimable prewrites in the ORIGINAL Content store. Chunks
//! are private transport material; only the Artifact publication makes a root
//! reachable. There is no second upload session, database or success state.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::ContentId;

pub const DEVICE_ARTIFACT_CHUNK_BYTES: usize = 4 * 1024 * 1024;

pub fn device_artifact_chunk_id(
    delivery: &DeviceToolDelivery,
    manifest: &DeviceArtifactExportManifest,
    reference: &ContentRef,
    offset: u64,
) -> Result<ContentId, DialogueToolPublicationError> {
    DeviceArtifactContents::require_staged(delivery, manifest)?;
    if !manifest.content_refs().contains(reference)
        || !offset.is_multiple_of(DEVICE_ARTIFACT_CHUNK_BYTES as u64)
        || offset >= reference.byte_length
    {
        return Err(DialogueToolPublicationError::InvalidArguments);
    }
    let name = format!(
        "native-artifact-chunk:{}:{}:{offset}",
        manifest.commitment(delivery)?,
        reference.content_id
    );
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &reference.content_id.into_uuid(),
        name.as_bytes(),
    ))
    .map_err(|_| DialogueToolPublicationError::InvalidArguments)
}

pub fn device_artifact_chunk_reference(
    delivery: &DeviceToolDelivery,
    manifest: &DeviceArtifactExportManifest,
    reference: &ContentRef,
    offset: u64,
    data: &[u8],
) -> Result<ContentRef, DialogueToolPublicationError> {
    let id = device_artifact_chunk_id(delivery, manifest, reference, offset)?;
    let expected = (reference.byte_length - offset).min(DEVICE_ARTIFACT_CHUNK_BYTES as u64);
    if data.len() as u64 != expected {
        return Err(DialogueToolPublicationError::InvalidArguments);
    }
    Ok(ContentRef {
        content_id: id,
        media_type: MediaType::new("application/octet-stream")
            .map_err(|_| DialogueToolPublicationError::Corrupt)?,
        byte_length: expected,
        sha256: Sha256::digest(data),
    })
}
