//! Immutable export facts selected by the original SQLite Artifact Owner.
//! This is an exact-version transfer unit, NOT a complete-history assertion or
//! permission to create a PG Run/Tool. Native authenticates its canonical digest.
use super::{DeviceToolDelivery, DialogueToolPublicationError as Error};
use musteragent_artifact::{ArtifactKind, ArtifactVersion, ArtifactVersionAuthor};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_execution_context::{AgentArtifactLocator, MAX_PUBLIC_DELIVERIES};
use musteragent_identity::*;
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_DEVICE_EXPORT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceArtifactExportScope {
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub conversation_id: ConversationId,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeviceArtifactExportRole {
    Delivery,
    Evidence,
}

/// Metadata of an actual successful producer. Its result remains private:
/// this reference proves origin and is not part of the user-download closure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceArtifactProducer {
    pub invocation_id: InvocationId,
    pub tool_call_id: ToolCallId,
    pub tool_key: VersionedKey,
    pub result_ref: ContentRef,
    pub terminal_record_id: LedgerRecordId,
    pub terminal_record_hash: Sha256,
    pub terminal_run_sequence: RunSeq,
    pub terminal_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceArtifactExport {
    pub role: DeviceArtifactExportRole,
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub title: String,
    pub source_activity_id: ActivityId,
    pub source_created_by_run_id: RunId,
    pub source_conversation_id: ConversationId,
    pub created_at: UtcTimestamp,
    /// Original source version ID AND numeric version. A PG head is a separate
    /// authority; import must record a mapping/CAS, never overwrite by number.
    pub version: ArtifactVersion,
    pub producer: DeviceArtifactProducer,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceArtifactExportManifest {
    pub request_fingerprint: Sha256,
    pub scope: DeviceArtifactExportScope,
    /// Request order: delivery selections followed by evidence selections.
    /// Each item contains only the exact version produced by its cited call.
    pub entries: Vec<DeviceArtifactExport>,
}
impl DeviceArtifactExportManifest {
    /// Structural validation only: SQLite proves owner/producer/closure; Native
    /// must independently match scope against its admitted membership custody.
    pub fn validate_for(&self, delivery: &DeviceToolDelivery) -> Result<(), Error> {
        self.validate_selection(
            delivery.fingerprint(),
            delivery.invocation_id(),
            delivery.publication(),
        )
    }
    pub(crate) fn validate_selection(
        &self,
        fingerprint: Sha256,
        invocation: InvocationId,
        publication: &musteragent_execution_context::NativePublication,
    ) -> Result<(), Error> {
        if self.request_fingerprint != fingerprint
            || self.entries.is_empty()
            || self.entries.len() > MAX_PUBLIC_DELIVERIES
        {
            return Err(Error::InvalidArguments);
        }
        let requested = publication
            .artifacts
            .iter()
            .map(|r| (DeviceArtifactExportRole::Delivery, r))
            .chain(
                publication
                    .evidence
                    .iter()
                    .map(|r| (DeviceArtifactExportRole::Evidence, r)),
            )
            .collect::<Vec<_>>();
        if requested.len() != self.entries.len() {
            return Err(Error::InvalidArguments);
        }
        let mut deliveries = BTreeSet::new();
        let mut identities = BTreeMap::new();
        for ((role, requested), entry) in requested.iter().zip(&self.entries) {
            // No SQLite ResourceNode owner is installed. An arbitrary node ID
            // must never be reinterpreted as an Artifact UUID.
            if requested.artifact != AgentArtifactLocator::Artifact(entry.artifact_id)
                || *role != entry.role
                || requested.tool_call_id != entry.producer.tool_call_id
                || (*role == DeviceArtifactExportRole::Delivery
                    && !deliveries.insert(entry.artifact_id))
                || entry.title.trim().is_empty()
                || entry.title.chars().count() > 512
                || entry.source_conversation_id != self.scope.conversation_id
                || entry.version.artifact_id != entry.artifact_id
                || entry.version.source_invocation_id != Some(entry.producer.invocation_id)
                || entry.producer.invocation_id == invocation
                || entry.version.committed_at != entry.producer.terminal_at
                || entry.created_at > entry.version.committed_at
                || !matches!(
                    entry.version.committed_by,
                    ArtifactVersionAuthor::Participant(_)
                )
            {
                return Err(Error::InvalidArguments);
            }
            entry
                .version
                .validate()
                .map_err(|_| Error::InvalidArguments)?;
            for reference in std::iter::once(&entry.version.content_ref)
                .chain(entry.version.dependency_refs.iter())
                .chain(std::iter::once(&entry.producer.result_ref))
            {
                if identities
                    .insert(reference.content_id, reference)
                    .is_some_and(|old| old != reference)
                {
                    return Err(Error::InvalidArguments);
                }
            }
        }
        Ok(())
    }
    pub(crate) fn result_metadata(&self) -> Result<Vec<u8>, Error> {
        super::device_artifact_codec::encode(self)
    }
    pub(crate) fn from_result_metadata(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_DEVICE_EXPORT_BYTES {
            return Err(Error::InvalidArguments);
        }
        let manifest = super::device_artifact_codec::decode(bytes)?;
        if manifest.result_metadata()? != bytes {
            return Err(Error::Corrupt);
        }
        Ok(manifest)
    }
    /// Deterministic immutable metadata, bounded independently of media bytes.
    pub fn encode(&self, delivery: &DeviceToolDelivery) -> Result<Vec<u8>, Error> {
        self.validate_for(delivery)?;
        super::device_artifact_codec::encode(self)
    }
    pub fn decode(bytes: &[u8], delivery: &DeviceToolDelivery) -> Result<Self, Error> {
        if bytes.len() > MAX_DEVICE_EXPORT_BYTES {
            return Err(Error::InvalidArguments);
        }
        let manifest = super::device_artifact_codec::decode(bytes)?;
        manifest.validate_for(delivery)?;
        if manifest.encode(delivery)? != bytes {
            return Err(Error::Corrupt);
        }
        Ok(manifest)
    }
    /// Future Native wire/PG receipt must bind this commitment, not merely
    /// the unchanged v1 request fingerprint. Existing transport fails closed.
    pub fn commitment(&self, delivery: &DeviceToolDelivery) -> Result<Sha256, Error> {
        let mut bytes = b"musteragent.device-artifact-delivery.v1\0".to_vec();
        bytes.extend_from_slice(&self.request_fingerprint.into_bytes());
        bytes.extend_from_slice(&Sha256::digest(self.encode(delivery)?).into_bytes());
        Ok(Sha256::digest(bytes))
    }
    /// Only these immutable roots/dependencies may cross the content channel.
    /// The destination still needs the signed manifest and original custody.
    pub fn content_refs(&self) -> Vec<ContentRef> {
        let mut seen = BTreeSet::new();
        self.entries
            .iter()
            .flat_map(|e| {
                std::iter::once(&e.version.content_ref).chain(e.version.dependency_refs.iter())
            })
            .filter(|r| seen.insert(r.content_id))
            .cloned()
            .collect()
    }
}

/// The cloud Artifact owner acknowledges this exact immutable export, not only
/// the Tool request. A plain text receipt cannot satisfy this typed contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceArtifactDeliveryReceipt {
    pub delivery_receipt: super::DeviceToolDeliveryReceipt,
    pub export_commitment: Sha256,
}
impl DeviceArtifactDeliveryReceipt {
    pub fn validate_for(
        &self,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceArtifactExportManifest,
    ) -> Result<(), Error> {
        self.delivery_receipt.validate_for(delivery)?;
        if self.export_commitment != manifest.commitment(delivery)? {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
}
