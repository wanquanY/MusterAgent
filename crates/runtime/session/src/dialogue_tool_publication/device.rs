//! Device delivery data for the original Dialogue publication owner. These
//! values are not authority: Native authenticates the live account/membership,
//! SQLite proves the original invocation and PostgreSQL proves source custody.
use super::DialogueToolPublicationError as Error;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_execution_context::{MAX_PUBLICATION_TOOL_BYTES, NativePublication};
use musteragent_identity::{
    CommandId, DeviceId, DeviceInstallationId, DialogueEntryId, InvocationId, RunId, UserIntentId,
    WorkItemId,
};
use musteragent_runtime_contract::ApplicationFuture;

/// Immutable facts selected from the original local admission receipt. No
/// destination Dialogue, Agent, principal or tenant can be supplied by a tool.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceDeliveryBinding {
    /// Original message provenance; a WorkItem subject is never admitted as this Intent.
    pub intent_id: UserIntentId,
    pub work_item: Option<WorkItemDevicePublicationSubject>,
    pub run_id: RunId,
    pub device_id: DeviceId,
    pub installation_id: DeviceInstallationId,
    pub source_digest: Sha256,
    pub snapshot_hash: Sha256,
    pub grant_proof_hash: Sha256,
}

/// Exact original WorkItem reservation identity. All revisions are nonzero,
/// bounded SQL integers; the source envelope additionally binds the Agent and owner.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemDevicePublicationSubject {
    pub work_item_id: WorkItemId,
    pub goal_revision: u64,
    pub control_revision: u64,
    pub assignment_epoch: u64,
}
impl WorkItemDevicePublicationSubject {
    pub fn validate(&self) -> Result<(), Error> {
        if [
            self.goal_revision,
            self.control_revision,
            self.assignment_epoch,
        ]
        .iter()
        .any(|v| *v == 0 || *v > i64::MAX as u64)
        {
            return Err(Error::InvalidArguments);
        }
        Ok(())
    }
}
impl DeviceDeliveryBinding {
    pub fn validate_subject(&self) -> Result<(), Error> {
        if let Some(subject) = self.work_item {
            subject.validate()?;
        }
        Ok(())
    }
    /// The signed Native URL/signature namespace uses the execution subject,
    /// while intent_id remains original provenance for the return conversation.
    pub fn execution_subject_id(&self) -> uuid::Uuid {
        self.work_item
            .map(|v| v.work_item_id.into_uuid())
            .unwrap_or_else(|| self.intent_id.into_uuid())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceToolDelivery {
    binding: DeviceDeliveryBinding,
    invocation_id: InvocationId,
    tool_key: String,
    request_ref: ContentRef,
    request_bytes: Vec<u8>,
    publication: NativePublication,
    files: Vec<musteragent_runtime_file_query::FileReference>,
    fingerprint: Sha256,
}

impl DeviceToolDelivery {
    /// Checks bounded immutable data only. The local owner must still read the
    /// actual invocation, sealed snapshot and membership under its writer fence.
    pub fn new(
        binding: DeviceDeliveryBinding,
        invocation_id: InvocationId,
        tool_key: String,
        request_ref: ContentRef,
        request_bytes: Vec<u8>,
    ) -> Result<Self, Error> {
        binding.validate_subject()?;
        if [
            binding.source_digest,
            binding.snapshot_hash,
            binding.grant_proof_hash,
        ]
        .iter()
        .any(|hash| hash.into_bytes() == [0; 32])
            || request_bytes.len() > MAX_PUBLICATION_TOOL_BYTES
            || request_ref.byte_length != request_bytes.len() as u64
            || request_ref.sha256 != Sha256::digest(&request_bytes)
            || request_ref.media_type.as_str() != "application/json"
        {
            return Err(Error::InvalidArguments);
        }
        // This is the existing registered Tool decoder, never assistant text.
        let decoded = super::decode_dialogue_tool_publication(&tool_key, &request_bytes)?;
        let fingerprint = fingerprint(&binding, invocation_id, &tool_key, &request_ref);
        Ok(Self {
            binding,
            invocation_id,
            tool_key,
            request_ref,
            request_bytes,
            publication: decoded.publication,
            files: decoded.files,
            fingerprint,
        })
    }
    pub fn binding(&self) -> &DeviceDeliveryBinding {
        &self.binding
    }
    pub fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
    pub fn tool_key(&self) -> &str {
        &self.tool_key
    }
    pub fn request_ref(&self) -> &ContentRef {
        &self.request_ref
    }
    pub fn request_bytes(&self) -> &[u8] {
        &self.request_bytes
    }
    pub fn publication(&self) -> &NativePublication {
        &self.publication
    }
    pub fn file_references(&self) -> &[musteragent_runtime_file_query::FileReference] {
        &self.files
    }
    pub fn fingerprint(&self) -> Sha256 {
        self.fingerprint
    }
    pub fn entry_count(&self) -> usize {
        self.files.len()
            + self.publication.artifacts.len()
            + usize::from(self.publication.text.is_some())
    }
    /// Same stable logical identity as the original Cloud Tool publication.
    pub fn delivery_id(&self) -> Result<CommandId, Error> {
        CommandId::from_uuid(uuid::Uuid::new_v5(
            &self.invocation_id.into_uuid(),
            b"public-tool-delivery@1",
        ))
        .map_err(|_| Error::Corrupt)
    }
    pub fn result_bytes(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(&serde_json::json!({
            "delivery_id": self.delivery_id()?.to_string(),
            "committed": true, "entry_count": self.entry_count(),
        }))
        .map_err(|_| Error::Corrupt)
    }
}

/// An authenticated remote receipt is still checked against this exact durable
/// delivery. Replaying one invocation for another never terminalizes a tool.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceToolDeliveryReceipt {
    pub invocation_id: InvocationId,
    pub fingerprint: Sha256,
    pub entry_ids: Vec<DialogueEntryId>,
    pub result_ref: ContentRef,
    pub result_bytes: Vec<u8>,
}
impl DeviceToolDeliveryReceipt {
    pub fn validate_for(&self, delivery: &DeviceToolDelivery) -> Result<(), Error> {
        let expected = delivery.result_bytes()?;
        if self.invocation_id != delivery.invocation_id()
            || self.fingerprint != delivery.fingerprint()
            || self.entry_ids.len() != delivery.entry_count()
            || self
                .entry_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.entry_ids.len()
            || self.result_bytes != expected
            || self.result_ref.byte_length != expected.len() as u64
            || self.result_ref.sha256 != Sha256::digest(&expected)
            || self.result_ref.media_type.as_str() != "application/json"
            || self.result_ref.content_id.into_uuid()
                != uuid::Uuid::new_v5(&self.invocation_id.into_uuid(), b"public-tool-receipt@1")
        {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
}

/// Original Native account transport only. It binds the request digest to a
/// freshly checked Native device proof and preserves the same invocation on
/// every retry. Network errors do not turn into success or fresh model work.
pub trait DeviceToolDeliveryTransport: Send + Sync {
    fn deliver_artifacts_with_activity<'a>(
        &'a self,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a super::DeviceArtifactExportManifest,
        _activity: &'a dyn super::DialoguePublicationActivity,
    ) -> ApplicationFuture<'a, Result<super::DeviceArtifactDeliveryReceipt, Error>> {
        self.deliver_artifacts(delivery, manifest)
    }

    /// Explicitly separate from the v1 text wire. Until the Native/PG owners
    /// install manifest authentication and streaming, exports remain rejected.
    fn deliver_artifacts<'a>(
        &'a self,
        _delivery: &'a DeviceToolDelivery,
        _manifest: &'a super::DeviceArtifactExportManifest,
    ) -> ApplicationFuture<'a, Result<super::DeviceArtifactDeliveryReceipt, Error>> {
        Box::pin(async { Err(Error::ReferenceUnavailable) })
    }

    fn deliver<'a>(
        &'a self,
        delivery: &'a DeviceToolDelivery,
    ) -> ApplicationFuture<'a, Result<DeviceToolDeliveryReceipt, Error>>;
}

/// Internal account publication port. The adapter authenticates the actual
/// current Native subject; the store independently binds that subject to the
/// original admitted Device custody and current Owned configuration.
pub trait DeviceToolPublicationStore: Send + Sync {
    /// Observes an already committed exact export under current subject custody.
    /// A miss confers no content access; this operation never stages/imports bytes.
    fn lookup_device_artifact_delivery<'a>(
        &'a self,
        _actor: musteragent_identity::Actor,
        _target: &'a crate::OriginDeviceExecutionTarget,
        _delivery: &'a DeviceToolDelivery,
        _manifest: &'a super::DeviceArtifactExportManifest,
    ) -> ApplicationFuture<'a, Result<Option<super::DeviceArtifactDeliveryReceipt>, Error>> {
        Box::pin(async { Err(Error::ReferenceUnavailable) })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Existing host implementations use this port signature."
    )]
    fn stage_device_artifact_chunk<'a>(
        &'a self,
        _actor: musteragent_identity::Actor,
        _target: &'a crate::OriginDeviceExecutionTarget,
        _delivery: &'a DeviceToolDelivery,
        _manifest: &'a super::DeviceArtifactExportManifest,
        _reference: &'a ContentRef,
        _offset: u64,
        _data: &'a [u8],
    ) -> ApplicationFuture<'a, Result<ContentRef, Error>> {
        Box::pin(async { Err(Error::ReferenceUnavailable) })
    }

    fn publish_device_artifacts<'a>(
        &'a self,
        _actor: musteragent_identity::Actor,
        _target: &'a crate::OriginDeviceExecutionTarget,
        _delivery: &'a DeviceToolDelivery,
        _manifest: &'a super::DeviceArtifactExportManifest,
        _contents: &'a super::DeviceArtifactContents,
    ) -> ApplicationFuture<'a, Result<super::DeviceArtifactDeliveryReceipt, Error>> {
        Box::pin(async { Err(Error::ReferenceUnavailable) })
    }

    fn publish_device<'a>(
        &'a self,
        actor: musteragent_identity::Actor,
        target: &'a crate::OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
    ) -> ApplicationFuture<'a, Result<DeviceToolDeliveryReceipt, Error>>;
}

fn fingerprint(
    b: &DeviceDeliveryBinding,
    invocation: InvocationId,
    tool: &str,
    request: &ContentRef,
) -> Sha256 {
    let mut bytes = if let Some(task) = b.work_item {
        let mut bytes = b"musteragent.work-item-device-tool-delivery.v1\0".to_vec();
        bytes.extend_from_slice(task.work_item_id.into_uuid().as_bytes());
        for revision in [
            task.goal_revision,
            task.control_revision,
            task.assignment_epoch,
        ] {
            bytes.extend_from_slice(&revision.to_be_bytes());
        }
        bytes
    } else {
        b"musteragent.device-tool-delivery.v1\0".to_vec()
    };
    for id in [
        b.intent_id.into_uuid(),
        b.run_id.into_uuid(),
        b.device_id.into_uuid(),
        b.installation_id.into_uuid(),
        invocation.into_uuid(),
        request.content_id.into_uuid(),
    ] {
        bytes.extend_from_slice(id.as_bytes());
    }
    for hash in [
        b.source_digest,
        b.snapshot_hash,
        b.grant_proof_hash,
        request.sha256,
    ] {
        bytes.extend_from_slice(&hash.into_bytes());
    }
    for field in [tool, request.media_type.as_str()] {
        bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
        bytes.extend_from_slice(field.as_bytes());
    }
    bytes.extend_from_slice(&request.byte_length.to_be_bytes());
    Sha256::digest(bytes)
}

#[cfg(test)]
#[path = "device_tests.rs"]
mod tests;
