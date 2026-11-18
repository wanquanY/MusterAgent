//! One bounded Artifact root crosses Native transport. Images retain their exact
//! canonical dependencies already held by the destination Content owner; media
//! bytes are not uploaded through this publication channel.
use super::{
    DeviceArtifactExportManifest as Manifest, DeviceArtifactExportRole as Role,
    DeviceToolDelivery as Delivery, DialogueToolPublicationError as Error,
};
use musteragent_artifact::ArtifactKind;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_runtime_artifact::{ArtifactFileRepresentation, artifact_file_representation};
use std::collections::BTreeSet;

pub const MAX_DEVICE_ARTIFACT_ROOT_BYTES: usize = 1024 * 1024;
// Preserve the existing public bound for TXT consumers.
pub const MAX_DEVICE_TXT_BYTES: usize = MAX_DEVICE_ARTIFACT_ROOT_BYTES;
const MAX_IMAGE_DEPENDENCIES: usize = 16;
#[derive(Clone, Eq, PartialEq)]
pub struct DeviceArtifactContent {
    pub content_ref: ContentRef,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Eq, PartialEq)]
pub struct DeviceArtifactContents(Vec<DeviceArtifactContent>);
impl DeviceArtifactContents {
    pub fn new(
        delivery: &Delivery,
        manifest: &Manifest,
        entries: Vec<DeviceArtifactContent>,
    ) -> Result<Self, Error> {
        let value = Self(entries);
        value.validate_for(delivery, manifest)?;
        Ok(value)
    }
    /// Evaluate BEFORE reading local bytes or allocating an incoming buffer.
    /// This remains a closed set of original formats, not a generic file upload.
    /// Image dependency bytes must already exist under the destination tenant.
    pub fn require_supported(delivery: &Delivery, manifest: &Manifest) -> Result<(), Error> {
        manifest.validate_for(delivery)?;
        if Self::require_staged(delivery, manifest).is_ok() {
            return Ok(());
        }
        if manifest.entries.len() != 1 || !delivery.publication().evidence.is_empty() {
            return Err(Error::ReferenceUnavailable);
        }
        Self::require_supported_result_entry(&manifest.entries[0])
    }
    pub(crate) fn require_supported_result_entry(
        e: &super::DeviceArtifactExport,
    ) -> Result<(), Error> {
        if e.role != Role::Delivery
            || e.version.content_ref.byte_length > MAX_DEVICE_ARTIFACT_ROOT_BYTES as u64
        {
            return Err(Error::ReferenceUnavailable);
        }
        let supported = match e.kind {
            ArtifactKind::File => {
                e.version.content_ref.media_type.as_str() == "text/plain"
                    && e.version.dependency_refs.is_empty()
            }
            ArtifactKind::Image => {
                e.version.content_ref.media_type.as_str() == "application/json"
                    && (1..=MAX_IMAGE_DEPENDENCIES).contains(&e.version.dependency_refs.len())
                    && e.version.dependency_refs.iter().all(|r| {
                        r.byte_length > 0
                            && matches!(
                                r.media_type.as_str(),
                                "image/png" | "image/jpeg" | "image/webp" | "image/gif"
                            )
                    })
            }
            _ => false,
        };
        if !supported {
            return Err(Error::ReferenceUnavailable);
        }
        Ok(())
    }
    pub fn require_staged(delivery: &Delivery, manifest: &Manifest) -> Result<(), Error> {
        manifest.validate_for(delivery)?;
        if manifest.entries.is_empty()
            || manifest.entries.iter().any(|entry| {
                entry.kind != ArtifactKind::File
                    || !entry.version.dependency_refs.is_empty()
                    || entry.version.content_ref.byte_length
                        > musteragent_runtime_content::MAX_CONTENT_BYTES
            })
        {
            return Err(Error::ReferenceUnavailable);
        }
        Ok(())
    }
    pub fn staged(delivery: &Delivery, manifest: &Manifest) -> Result<Self, Error> {
        Self::require_staged(delivery, manifest)?;
        Ok(Self(vec![]))
    }
    pub fn is_staged(&self) -> bool {
        self.0.is_empty()
    }
    pub fn validate_for(&self, delivery: &Delivery, manifest: &Manifest) -> Result<(), Error> {
        if self.is_staged() {
            return Self::require_staged(delivery, manifest);
        }
        Self::require_supported(delivery, manifest)?;
        if self.0.len() != 1 || self.0[0].content_ref != manifest.entries[0].version.content_ref {
            return Err(Error::InvalidArguments);
        }
        Self::validate_result_root(&manifest.entries[0], &self.0[0].bytes)
    }
    pub(crate) fn validate_result_root(
        entry: &super::DeviceArtifactExport,
        bytes: &[u8],
    ) -> Result<(), Error> {
        Self::require_supported_result_entry(entry)?;
        let expected = &entry.version.content_ref;
        if bytes.len() > MAX_DEVICE_ARTIFACT_ROOT_BYTES
            || bytes.len() as u64 != expected.byte_length
            || Sha256::digest(bytes) != expected.sha256
            || std::str::from_utf8(bytes).is_err()
        {
            return Err(Error::InvalidArguments);
        }
        if entry.kind == ArtifactKind::Image {
            let ArtifactFileRepresentation::Visual(references) =
                artifact_file_representation(entry.kind, &entry.version, bytes)
                    .map_err(|_| Error::InvalidArguments)?
            else {
                return Err(Error::InvalidArguments);
            };
            // The format owner validates each visual reference. Publication
            // additionally requires a closed set: no unused or repeated assets.
            let ids: BTreeSet<_> = references.iter().map(|r| r.content_id).collect();
            if ids.len() != references.len()
                || references.len() != entry.version.dependency_refs.len()
                || references
                    .iter()
                    .any(|r| !entry.version.dependency_refs.contains(r))
            {
                return Err(Error::InvalidArguments);
            }
        }
        Ok(())
    }
    pub fn entries(&self) -> &[DeviceArtifactContent] {
        &self.0
    }
}
