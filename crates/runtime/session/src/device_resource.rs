//! Exact Device invocation data for the original canonical Resource owner.
//! These values are not authority. Native selects them from the live SQLite
//! journal; the authenticated ingress and PG owner verify current custody.
//! Tool arguments/results remain owned by their original registered decoders.
mod codec;
mod file_transfer;
mod operation;
mod publication;
mod task_result_assets;
pub use file_transfer::*;
pub use operation::DeviceResourceOperation;
pub use publication::*;

use crate::{DeviceDeliveryBinding, OriginDeviceExecutionTarget};
use musteragent_capability::CapabilityKey;
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::*;
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
use musteragent_tool::ToolKey;
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_DEVICE_RESOURCE_TOOL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_DEVICE_RESOURCE_WIRE_BYTES: usize = 40 * 1024 * 1024;
pub const MAX_DEVICE_RESOURCE_REFS: usize = 256;

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum DeviceResourceError {
    #[error("resource invocation does not match the closed contract")]
    InvalidArguments,
    #[error("current resource execution authority is unavailable")]
    Forbidden,
    #[error("current Device execution fence is stale")]
    StaleFence,
    #[error("resource command identity conflicts with a durable command")]
    Conflict,
    /// The owner aborted before accepting a Task. Re-select only the original
    /// explicit Device and requalify its current policy under the same request.
    #[error("device policy changed before Task acceptance; reselect the original Device")]
    TargetPolicyChanged,
    #[error("resource reference is unavailable")]
    ReferenceUnavailable,
    #[error("resource outcome is unknown; reconcile the same immutable command")]
    Infrastructure,
    #[error("resource receipt or canonical encoding failed integrity checks")]
    Corrupt,
}
use DeviceResourceError as Error;

/// Immutable original invocation fields. Current execution ownership is kept
/// exclusively in `DeviceResourceRequest::attempt`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceResourceInvocation {
    pub run_context: RunContext,
    /// Original invocation timing; never reconstructed from a fresh timeout.
    pub timing: InvocationExecutionTiming,
    pub invocation_id: InvocationId,
    pub requested_by_attempt_id: AttemptId,
    pub tool_call_id: ToolCallId,
    pub tool_key: ToolKey,
    pub capability_key: CapabilityKey,
    pub provider_id: CapabilityProviderId,
    pub idempotency_key: IdempotencyKey,
    pub request_ref: ContentRef,
    pub request_schema: SchemaRef,
    pub result_schema: SchemaRef,
    /// Original journal authorization, never references extracted from model JSON.
    pub authorized_content: Vec<ContentRef>,
    /// Original Memory provenance from the Native journal. The receiving owner
    /// verifies account, Agent, origin and approval against admitted custody.
    pub memory: Option<DeviceMemoryContext>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceMemoryContext {
    pub evidence_record_id: LedgerRecordId,
    pub approval_interaction_id: Option<InteractionId>,
    pub user_origin: Option<DeviceMemoryUserOrigin>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceMemoryUserOrigin {
    pub record_id: LedgerRecordId,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceResourceCommand {
    admission: DeviceDeliveryBinding,
    invocation: DeviceResourceInvocation,
    request_bytes: Vec<u8>,
    operation: DeviceResourceOperation,
    result_artifact_manifest: Option<crate::DeviceArtifactExportManifest>,
    result_metadata: Option<Vec<u8>>,
    commitment: Sha256,
}
impl DeviceResourceCommand {
    pub fn new(
        admission: DeviceDeliveryBinding,
        mut invocation: DeviceResourceInvocation,
        request_bytes: Vec<u8>,
    ) -> Result<Self, Error> {
        validate_admission(&admission)?;
        let run = &invocation.run_context;
        let home = RuntimeHome::Device {
            device_id: admission.device_id,
        };
        if run.run_id != admission.run_id
            || run.control_home != home
            || run.execution_home != home
            || !matches!(run.execution_scope,
                ExecutionScope::DevicePrivate { device_id, .. }
                    if device_id == admission.device_id)
            || (run.delegation_id.is_some() && run.parent_run_id.is_none())
        {
            return Err(Error::InvalidArguments);
        }
        match &invocation.timing {
            InvocationExecutionTiming::LegacyFixed { deadline } if deadline.unix_millis() <= 0 => {
                return Err(Error::InvalidArguments);
            }
            InvocationExecutionTiming::ActivityV1(timing) => {
                nonzero(timing.policy.digest)?;
                if timing
                    .business_deadline
                    .as_ref()
                    .is_some_and(|d| d.at.unix_millis() <= 0)
                {
                    return Err(Error::InvalidArguments);
                }
            }
            _ => {}
        }
        let operation = DeviceResourceOperation::from_tool_key(invocation.tool_key.as_str())?;
        if operation.is_memory() != invocation.memory.is_some() {
            return Err(Error::InvalidArguments);
        }
        // Every accepted version currently binds the same exact Capability key.
        // Provider identity and both schemas are additionally checked against
        // the original sealed catalog by Native and by the receiving authority.
        if invocation.capability_key.as_str() != invocation.tool_key.as_str() {
            return Err(Error::InvalidArguments);
        }
        validate_schema(&invocation.request_schema)?;
        validate_schema(&invocation.result_schema)?;
        validate_json_content(&invocation.request_ref, &request_bytes)?;
        validate_refs(&invocation.authorized_content)?;
        let mut refs = vec![invocation.request_ref.clone()];
        refs.extend(invocation.authorized_content.iter().cloned());
        validate_ref_identities(&refs)?;
        // Authorization is a set. Canonicalize its order so recovery does not
        // depend on database row order, while retaining exact original facts.
        invocation
            .authorized_content
            .sort_by_key(|reference| reference.content_id);
        let mut value = Self {
            admission,
            invocation,
            request_bytes,
            operation,
            result_artifact_manifest: None,
            result_metadata: None,
            commitment: Sha256::digest([]),
        };
        value.commitment = digest(
            b"musteragent.device-resource-command.v1\0",
            &value.encode()?,
        );
        Ok(value)
    }
    pub fn admission(&self) -> &DeviceDeliveryBinding {
        &self.admission
    }
    pub fn run_context(&self) -> &RunContext {
        &self.invocation.run_context
    }
    pub fn invocation(&self) -> &DeviceResourceInvocation {
        &self.invocation
    }
    pub fn request_bytes(&self) -> &[u8] {
        &self.request_bytes
    }
    pub const fn operation(&self) -> DeviceResourceOperation {
        self.operation
    }
    pub const fn commitment(&self) -> Sha256 {
        self.commitment
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        codec::encode_command(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let value = codec::decode_command(bytes)?;
        require_canonical(bytes, &value.encode()?)?;
        Ok(value)
    }
}

/// Renewable dispatch/lookup proof. This exact envelope is Native-signed. It
/// must be reconstructed from current execution custody, not caller fields.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceResourceRequest {
    command: DeviceResourceCommand,
    attempt: AttemptContext,
    /// Preparing an approval statement cannot execute or commit the Tool.
    memory_approval_only: bool,
    result_roots: Vec<Vec<u8>>,
}
impl DeviceResourceRequest {
    pub fn new(command: DeviceResourceCommand, attempt: AttemptContext) -> Result<Self, Error> {
        if command.admission.run_id != attempt.run_id || attempt.lease_expires_at.unix_millis() <= 0
        {
            return Err(Error::InvalidArguments);
        }
        Ok(Self {
            command,
            attempt,
            memory_approval_only: false,
            result_roots: vec![],
        })
    }
    pub fn for_memory_approval(mut self) -> Result<Self, Error> {
        if !self.command.operation().has_memory_approval()
            || self
                .command
                .invocation()
                .memory
                .as_ref()
                .is_none_or(|m| m.approval_interaction_id.is_some())
        {
            return Err(Error::InvalidArguments);
        }
        self.memory_approval_only = true;
        Ok(self)
    }
    pub const fn is_memory_approval(&self) -> bool {
        self.memory_approval_only
    }
    pub fn command(&self) -> &DeviceResourceCommand {
        &self.command
    }
    pub fn timing(&self) -> &InvocationExecutionTiming {
        &self.command.invocation.timing
    }
    pub fn attempt(&self) -> &AttemptContext {
        &self.attempt
    }
    pub fn validate_at(&self, now: UtcTimestamp) -> Result<(), Error> {
        if now >= self.attempt.lease_expires_at {
            Err(Error::StaleFence)
        } else {
            Ok(())
        }
    }
    pub fn proof_digest(&self) -> Result<Sha256, Error> {
        Ok(digest(
            b"musteragent.device-resource-current-proof.v1\0",
            &self.encode()?,
        ))
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        codec::encode_request(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let value = codec::decode_request(bytes)?;
        require_canonical(bytes, &value.encode()?)?;
        Ok(value)
    }
}

/// Exact canonical version produced by this command. `version` is the PG
/// version at commit, never an assertion about the mutable current head.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceCanonicalFileVersion {
    pub node_id: ResourceNodeId,
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    pub version: VersionNumber,
    pub content_ref: ContentRef,
    pub dependency_refs: Vec<ContentRef>,
}
impl DeviceCanonicalFileVersion {
    pub fn validate(&self) -> Result<(), Error> {
        validate_refs(&self.dependency_refs)?;
        let mut refs = vec![self.content_ref.clone()];
        refs.extend(self.dependency_refs.iter().cloned());
        validate_refs(&refs)
    }
}

/// A durable logical refusal by the canonical Resource owner. Admission,
/// lease, network, infrastructure and uncertain-effect failures are not logical
/// rejections and must not be converted into one of these codes.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeviceResourceRejectionCode {
    InvalidArguments,
    Conflict,
    ReferenceUnavailable,
    AccessDenied,
}
impl DeviceResourceRejectionCode {
    /// Closed safe text: original parser errors may contain user content.
    pub fn safe_message(self) -> &'static str {
        match self {
            Self::InvalidArguments => "The Resource request is invalid.",
            Self::Conflict => "The Resource request conflicts with its current version.",
            Self::ReferenceUnavailable => "The requested Resource is unavailable.",
            Self::AccessDenied => "The Resource operation is not permitted.",
        }
    }
}

/// Exact original Tool output. No receipt wrapper fields enter the Tool result.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceResourceSuccess {
    pub result_ref: ContentRef,
    pub result_schema: SchemaRef,
    pub result_bytes: Vec<u8>,
    pub authorized_content: Vec<ContentRef>,
    pub produced_versions: Vec<DeviceCanonicalFileVersion>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The public enum exposes these payloads by value to host implementations."
)]
pub enum DeviceResourceOutcome {
    Succeeded(DeviceResourceSuccess),
    /// Terminal refusal: no successful result, content identity or version.
    Rejected(DeviceResourceRejectionCode),
}

/// Immutable terminal owner fact, never permission to replay or republish.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceResourceReceipt {
    pub command_commitment: Sha256,
    pub invocation_id: InvocationId,
    pub operation: DeviceResourceOperation,
    pub outcome: DeviceResourceOutcome,
    /// Exact source reads, in ContentId order. Retention/erasure provenance
    /// only: these references never extend the Run's content authorization.
    pub read_dependencies: Vec<ContentRef>,
    pub committed_at: UtcTimestamp,
}
impl DeviceResourceReceipt {
    pub fn success(&self) -> Option<&DeviceResourceSuccess> {
        match &self.outcome {
            DeviceResourceOutcome::Succeeded(value) => Some(value),
            DeviceResourceOutcome::Rejected(_) => None,
        }
    }
    pub fn rejection(&self) -> Option<DeviceResourceRejectionCode> {
        match self.outcome {
            DeviceResourceOutcome::Rejected(code) => Some(code),
            DeviceResourceOutcome::Succeeded(_) => None,
        }
    }
    pub fn validate_for(&self, command: &DeviceResourceCommand) -> Result<(), Error> {
        if self.command_commitment != command.commitment()
            || self.invocation_id != command.invocation.invocation_id
            || self.operation != command.operation
            || self.committed_at.unix_millis() <= 0
        {
            return Err(Error::Corrupt);
        }
        validate_refs(&self.read_dependencies)?;
        if self
            .read_dependencies
            .windows(2)
            .any(|pair| pair[0].content_id >= pair[1].content_id)
        {
            return Err(Error::Corrupt);
        }
        let mut refs = vec![command.invocation.request_ref.clone()];
        refs.extend(command.invocation.authorized_content.iter().cloned());
        refs.extend(self.read_dependencies.iter().cloned());
        if let Some(success) = self.success() {
            if success.result_schema != command.invocation.result_schema
                || success.produced_versions.len() > 64
                || (!self.operation.can_produce_versions() && !success.produced_versions.is_empty())
                || (matches!(
                    self.operation,
                    DeviceResourceOperation::FileCreate | DeviceResourceOperation::EditFinalize
                ) && success.produced_versions.len() > 1)
            {
                return Err(Error::Corrupt);
            }
            validate_json_content(&success.result_ref, &success.result_bytes)?;
            validate_schema(&success.result_schema)?;
            validate_refs(&success.authorized_content)?;
            let mut nodes = BTreeSet::new();
            let mut artifacts = BTreeSet::new();
            let mut versions = BTreeSet::new();
            refs.push(success.result_ref.clone());
            refs.extend(success.authorized_content.iter().cloned());
            for version in &success.produced_versions {
                version.validate()?;
                if !nodes.insert(version.node_id)
                    || !artifacts.insert(version.artifact_id)
                    || !versions.insert(version.artifact_version_id)
                {
                    return Err(Error::Corrupt);
                }
                refs.push(version.content_ref.clone());
                refs.extend(version.dependency_refs.iter().cloned());
            }
        }
        validate_ref_identities(&refs)
    }
    pub fn encode(&self, command: &DeviceResourceCommand) -> Result<Vec<u8>, Error> {
        self.validate_for(command)?;
        codec::encode_receipt(self)
    }
    pub fn decode(bytes: &[u8], command: &DeviceResourceCommand) -> Result<Self, Error> {
        let value = codec::decode_receipt(bytes)?;
        require_canonical(bytes, &value.encode(command)?)?;
        Ok(value)
    }
    pub fn commitment(&self, command: &DeviceResourceCommand) -> Result<Sha256, Error> {
        Ok(digest(
            b"musteragent.device-resource-receipt.v1\0",
            &self.encode(command)?,
        ))
    }
}

/// Backend-authenticated owner ingress. Implementations recheck current Native
/// target, actual admission, exact capability and cancellation under their
/// original transaction locks. `None` is unknown, never ProvenNotApplied.
pub trait DeviceResourceStore: Send + Sync {
    fn read_file_content<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceResourceRequest,
        offset: u64,
    ) -> ApplicationFuture<'a, Result<DeviceFileChunk, Error>> {
        let _ = (actor, target, request, offset);
        Box::pin(async { Err(Error::ReferenceUnavailable) })
    }

    fn execute_resource_tool<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceResourceRequest,
    ) -> ApplicationFuture<'a, Result<DeviceResourceReceipt, Error>>;
    fn lookup_resource_tool<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceResourceRequest,
    ) -> ApplicationFuture<'a, Result<Option<DeviceResourceReceipt>, Error>>;
}
/// Native transport signs execute and lookup under different operation frames.
/// Lookup does not execute, replay or turn an absent receipt into retry proof.
pub trait DeviceResourceTransport: Send + Sync {
    fn read_file_content<'a>(
        &'a self,
        request: &'a DeviceResourceRequest,
        offset: u64,
    ) -> ApplicationFuture<'a, Result<DeviceFileChunk, Error>> {
        let _ = (request, offset);
        Box::pin(async { Err(Error::ReferenceUnavailable) })
    }

    fn execute<'a>(
        &'a self,
        request: &'a DeviceResourceRequest,
    ) -> ApplicationFuture<'a, Result<DeviceResourceReceipt, Error>>;
    fn lookup<'a>(
        &'a self,
        request: &'a DeviceResourceRequest,
    ) -> ApplicationFuture<'a, Result<Option<DeviceResourceReceipt>, Error>>;
}

fn validate_admission(value: &DeviceDeliveryBinding) -> Result<(), Error> {
    value
        .validate_subject()
        .map_err(|_| Error::InvalidArguments)?;
    for hash in [
        value.source_digest,
        value.snapshot_hash,
        value.grant_proof_hash,
    ] {
        nonzero(hash)?;
    }
    Ok(())
}
fn nonzero(hash: Sha256) -> Result<(), Error> {
    if hash.into_bytes() == [0; 32] {
        Err(Error::InvalidArguments)
    } else {
        Ok(())
    }
}
fn validate_schema(value: &SchemaRef) -> Result<(), Error> {
    nonzero(value.schema_hash)
}
fn validate_json_content(reference: &ContentRef, bytes: &[u8]) -> Result<(), Error> {
    if bytes.len() > MAX_DEVICE_RESOURCE_TOOL_BYTES
        || reference.media_type.as_str() != "application/json"
        || reference.byte_length != bytes.len() as u64
        || reference.sha256 != Sha256::digest(bytes)
        || !serde_json::from_slice::<serde_json::Value>(bytes).is_ok_and(|v| v.is_object())
    {
        return Err(Error::InvalidArguments);
    }
    Ok(())
}
fn validate_refs(refs: &[ContentRef]) -> Result<(), Error> {
    if refs.len() > MAX_DEVICE_RESOURCE_REFS {
        return Err(Error::InvalidArguments);
    }
    let mut ids = BTreeSet::new();
    for reference in refs {
        nonzero(reference.sha256)?;
        if !ids.insert(reference.content_id) {
            return Err(Error::InvalidArguments);
        }
    }
    Ok(())
}
fn validate_ref_identities(refs: &[ContentRef]) -> Result<(), Error> {
    let mut identities = BTreeMap::new();
    for reference in refs {
        nonzero(reference.sha256)?;
        if identities
            .insert(reference.content_id, reference)
            .is_some_and(|old| old != reference)
        {
            return Err(Error::Corrupt);
        }
    }
    Ok(())
}
fn digest(domain: &[u8], bytes: &[u8]) -> Sha256 {
    let mut input = Vec::with_capacity(domain.len() + bytes.len());
    input.extend_from_slice(domain);
    input.extend_from_slice(bytes);
    Sha256::digest(input)
}
fn require_canonical(input: &[u8], canonical: &[u8]) -> Result<(), Error> {
    if input != canonical {
        Err(Error::Corrupt)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
