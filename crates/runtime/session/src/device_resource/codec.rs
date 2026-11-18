//! Canonical facts envelope. Original Tool bytes are preserved byte-for-byte;
//! this module never interprets them as Resource commands or producer proof.
use super::*;
use musteragent_activity::ActivityKind;
use musteragent_content_ref::MediaType;
use musteragent_run::WorkerId;
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, BusinessDeadline, LivenessPolicyRef,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    id: String,
    media_type: String,
    length: u64,
    sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Schema {
    key: String,
    sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    work_item: Option<TaskSubject>,
    intent_id: String,
    run_id: String,
    device_id: String,
    installation_id: String,
    source_digest: String,
    snapshot_hash: String,
    grant_proof_hash: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskSubject {
    work_item_id: String,
    goal_revision: u64,
    control_revision: u64,
    assignment_epoch: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Invocation {
    run_context: Run,
    timing: Timing,
    id: String,
    requested_by_attempt_id: String,
    tool_call_id: String,
    tool_key: String,
    capability_key: String,
    provider_id: String,
    idempotency_key: String,
    request_ref: Reference,
    request_schema: Schema,
    result_schema: Schema,
    authorized_content: Vec<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory: Option<MemoryContext>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryContext {
    evidence_record_id: String,
    approval_interaction_id: Option<String>,
    user_origin: Option<MemoryOrigin>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryOrigin {
    record_id: String,
    content_ref: Reference,
}
impl From<&DeviceMemoryContext> for MemoryContext {
    fn from(m: &DeviceMemoryContext) -> Self {
        Self {
            evidence_record_id: m.evidence_record_id.to_string(),
            approval_interaction_id: m.approval_interaction_id.map(|id| id.to_string()),
            user_origin: m.user_origin.as_ref().map(|o| MemoryOrigin {
                record_id: o.record_id.to_string(),
                content_ref: (&o.content_ref).into(),
            }),
        }
    }
}
impl TryFrom<MemoryContext> for DeviceMemoryContext {
    type Error = Error;
    fn try_from(m: MemoryContext) -> Result<Self, Error> {
        Ok(Self {
            evidence_record_id: id(m.evidence_record_id)?,
            approval_interaction_id: m.approval_interaction_id.map(id).transpose()?,
            user_origin: m
                .user_origin
                .map(|o| {
                    Ok(DeviceMemoryUserOrigin {
                        record_id: id(o.record_id)?,
                        content_ref: o.content_ref.try_into()?,
                    })
                })
                .transpose()?,
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Run {
    tenant_id: String,
    conversation_id: String,
    activity_id: String,
    activity_kind: String,
    run_id: String,
    executor_participant_id: String,
    requested_by_participant_id: String,
    parent_run_id: Option<String>,
    delegation_id: Option<String>,
    execution_scope: DeviceScope,
    control_home: Home,
    execution_home: Home,
    home_epoch: u64,
    runtime_snapshot_id: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Home {
    kind: String,
    device_id: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceScope {
    kind: String,
    device_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workspace_id: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Timing {
    #[serde(rename = "legacy_fixed")]
    Legacy { deadline_ms: i64 },
    #[serde(rename = "activity_v1")]
    Activity {
        policy_key: String,
        policy_digest: String,
        business_deadline: Option<Deadline>,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Deadline {
    at_ms: i64,
    source: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    schema: String,
    admission: Admission,
    invocation: Invocation,
    request_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_artifacts_hex: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    id: String,
    run_id: String,
    number: u32,
    worker_id: String,
    lease_id: String,
    fencing_token: u64,
    lease_expires_at_ms: i64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    schema: String,
    command: Command,
    attempt: Attempt,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    memory_approval_only: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    result_roots_hex: Vec<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Version {
    node_id: String,
    artifact_id: String,
    artifact_version_id: String,
    version: u64,
    content_ref: Reference,
    dependency_refs: Vec<Reference>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema: String,
    command_commitment: String,
    invocation_id: String,
    operation: String,
    outcome: Outcome,
    read_dependencies: Vec<Reference>,
    committed_at_ms: i64,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Outcome {
    Succeeded {
        result_ref: Reference,
        result_schema: Schema,
        result_hex: String,
        authorized_content: Vec<Reference>,
        produced_versions: Vec<Version>,
    },
    Rejected {
        code: RejectionCode,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RejectionCode {
    InvalidArguments,
    Conflict,
    ReferenceUnavailable,
    AccessDenied,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Producer {
    invocation_id: String,
    tool_call_id: String,
    tool_key: String,
    result_ref: Reference,
    terminal_record_id: String,
    terminal_record_hash: String,
    terminal_run_sequence: u64,
    terminal_at_ms: i64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryReference {
    role: String,
    admission: Admission,
    command_commitment: String,
    receipt_commitment: String,
    operation: String,
    version: Version,
    producer: Producer,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryManifest {
    schema: String,
    request_fingerprint: String,
    entries: Vec<DeliveryReference>,
}

fn id<T: std::str::FromStr>(v: String) -> Result<T, Error> {
    v.parse().map_err(|_| Error::Corrupt)
}
fn hash(v: String) -> Result<Sha256, Error> {
    Sha256::from_hex(&v).map_err(|_| Error::Corrupt)
}
fn key(v: String) -> Result<VersionedKey, Error> {
    VersionedKey::new(v).map_err(|_| Error::Corrupt)
}
fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(v).map_err(|_| Error::Corrupt)?;
    if bytes.len() > MAX_DEVICE_RESOURCE_WIRE_BYTES {
        return Err(Error::InvalidArguments);
    }
    Ok(bytes)
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    if bytes.len() > MAX_DEVICE_RESOURCE_WIRE_BYTES {
        return Err(Error::InvalidArguments);
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Corrupt)
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        value.push(DIGITS[(b >> 4) as usize] as char);
        value.push(DIGITS[(b & 15) as usize] as char);
    }
    value
}
fn unhex(value: String) -> Result<Vec<u8>, Error> {
    if value.len() > MAX_DEVICE_RESOURCE_TOOL_BYTES * 2
        || !value.len().is_multiple_of(2)
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Corrupt);
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |b| match b {
                b'0'..=b'9' => b - b'0',
                _ => b - b'a' + 10,
            };
            Ok((digit(pair[0]) << 4) | digit(pair[1]))
        })
        .collect()
}
impl From<&ContentRef> for Reference {
    fn from(v: &ContentRef) -> Self {
        Self {
            id: v.content_id.to_string(),
            media_type: v.media_type.as_str().into(),
            length: v.byte_length,
            sha256: v.sha256.to_string(),
        }
    }
}
impl TryFrom<Reference> for ContentRef {
    type Error = Error;
    fn try_from(v: Reference) -> Result<Self, Error> {
        Ok(Self {
            content_id: id(v.id)?,
            media_type: MediaType::new(v.media_type).map_err(|_| Error::Corrupt)?,
            byte_length: v.length,
            sha256: hash(v.sha256)?,
        })
    }
}
fn references(values: Vec<Reference>) -> Result<Vec<ContentRef>, Error> {
    values.into_iter().map(TryInto::try_into).collect()
}
impl From<&SchemaRef> for Schema {
    fn from(v: &SchemaRef) -> Self {
        Self {
            key: v.schema_key.as_str().into(),
            sha256: v.schema_hash.to_string(),
        }
    }
}
impl TryFrom<Schema> for SchemaRef {
    type Error = Error;
    fn try_from(v: Schema) -> Result<Self, Error> {
        Ok(Self {
            schema_key: key(v.key)?,
            schema_hash: hash(v.sha256)?,
        })
    }
}
impl From<&DeviceDeliveryBinding> for Admission {
    fn from(v: &DeviceDeliveryBinding) -> Self {
        Self {
            work_item: v.work_item.map(|t| TaskSubject {
                work_item_id: t.work_item_id.to_string(),
                goal_revision: t.goal_revision,
                control_revision: t.control_revision,
                assignment_epoch: t.assignment_epoch,
            }),
            intent_id: v.intent_id.to_string(),
            run_id: v.run_id.to_string(),
            device_id: v.device_id.to_string(),
            installation_id: v.installation_id.to_string(),
            source_digest: v.source_digest.to_string(),
            snapshot_hash: v.snapshot_hash.to_string(),
            grant_proof_hash: v.grant_proof_hash.to_string(),
        }
    }
}
impl TryFrom<Admission> for DeviceDeliveryBinding {
    type Error = Error;
    fn try_from(v: Admission) -> Result<Self, Error> {
        Ok(Self {
            work_item: v
                .work_item
                .map(|t| {
                    Ok(crate::WorkItemDevicePublicationSubject {
                        work_item_id: id(t.work_item_id)?,
                        goal_revision: t.goal_revision,
                        control_revision: t.control_revision,
                        assignment_epoch: t.assignment_epoch,
                    })
                })
                .transpose()?,
            intent_id: id(v.intent_id)?,
            run_id: id(v.run_id)?,
            device_id: id(v.device_id)?,
            installation_id: id(v.installation_id)?,
            source_digest: hash(v.source_digest)?,
            snapshot_hash: hash(v.snapshot_hash)?,
            grant_proof_hash: hash(v.grant_proof_hash)?,
        })
    }
}
const COMMAND_V1: &str = "musteragent.device-resource-command@1";
const COMMAND_V2: &str = "musteragent.device-resource-command@2";
const COMMAND_V3: &str = "musteragent.device-resource-command@3";
fn command_schema(run: &RunContext) -> &'static str {
    if matches!(
        run.execution_scope,
        ExecutionScope::DevicePrivate {
            workspace_id: Some(_),
            ..
        }
    ) {
        COMMAND_V2
    } else {
        COMMAND_V1
    }
}

impl From<&DeviceResourceCommand> for Command {
    fn from(v: &DeviceResourceCommand) -> Self {
        let i = v.invocation();
        Self {
            schema: if v.result_metadata.is_some() {
                "musteragent.device-resource-command@4"
            } else if i.memory.is_some() {
                COMMAND_V3
            } else {
                command_schema(&i.run_context)
            }
            .into(),
            admission: v.admission().into(),
            request_hex: hex(v.request_bytes()),
            result_artifacts_hex: v.result_metadata.as_ref().map(|b| hex(b)),
            invocation: Invocation {
                run_context: (&i.run_context).into(),
                timing: (&i.timing).into(),
                id: i.invocation_id.to_string(),
                requested_by_attempt_id: i.requested_by_attempt_id.to_string(),
                tool_call_id: i.tool_call_id.as_str().into(),
                tool_key: i.tool_key.as_str().into(),
                capability_key: i.capability_key.as_str().into(),
                provider_id: i.provider_id.to_string(),
                idempotency_key: i.idempotency_key.as_str().into(),
                request_ref: (&i.request_ref).into(),
                request_schema: (&i.request_schema).into(),
                result_schema: (&i.result_schema).into(),
                authorized_content: i.authorized_content.iter().map(Into::into).collect(),
                memory: i.memory.as_ref().map(Into::into),
            },
        }
    }
}
impl TryFrom<Command> for DeviceResourceCommand {
    type Error = Error;
    fn try_from(v: Command) -> Result<Self, Error> {
        let scoped = v
            .invocation
            .run_context
            .execution_scope
            .workspace_id
            .is_some();
        let expected = if v.result_artifacts_hex.is_some() {
            "musteragent.device-resource-command@4"
        } else if v.invocation.memory.is_some() {
            COMMAND_V3
        } else if scoped {
            COMMAND_V2
        } else {
            COMMAND_V1
        };
        if v.schema != expected {
            return Err(Error::Corrupt);
        }
        let i = v.invocation;
        let command = Self::new(
            v.admission.try_into()?,
            DeviceResourceInvocation {
                run_context: i.run_context.try_into()?,
                timing: i.timing.try_into()?,
                invocation_id: id(i.id)?,
                requested_by_attempt_id: id(i.requested_by_attempt_id)?,
                tool_call_id: ToolCallId::new(i.tool_call_id).map_err(|_| Error::Corrupt)?,
                tool_key: ToolKey::from_versioned_key(key(i.tool_key)?),
                capability_key: CapabilityKey::from_versioned_key(key(i.capability_key)?),
                provider_id: id(i.provider_id)?,
                idempotency_key: IdempotencyKey::new(i.idempotency_key)
                    .map_err(|_| Error::Corrupt)?,
                request_ref: i.request_ref.try_into()?,
                request_schema: i.request_schema.try_into()?,
                result_schema: i.result_schema.try_into()?,
                authorized_content: references(i.authorized_content)?,
                memory: i.memory.map(TryInto::try_into).transpose()?,
            },
            unhex(v.request_hex)?,
        )?;
        if let Some(hex) = v.result_artifacts_hex {
            command.with_result_artifact_manifest(
                crate::DeviceArtifactExportManifest::from_result_metadata(&unhex(hex)?)
                    .map_err(|_| Error::Corrupt)?,
            )
        } else {
            Ok(command)
        }
    }
}
impl From<&AttemptContext> for Attempt {
    fn from(v: &AttemptContext) -> Self {
        Self {
            id: v.attempt_id.to_string(),
            run_id: v.run_id.to_string(),
            number: v.attempt_number.get(),
            worker_id: v.worker_id.as_str().into(),
            lease_id: v.lease_id.to_string(),
            fencing_token: v.fencing_token.get(),
            lease_expires_at_ms: v.lease_expires_at.unix_millis(),
        }
    }
}
impl TryFrom<Attempt> for AttemptContext {
    type Error = Error;
    fn try_from(v: Attempt) -> Result<Self, Error> {
        Ok(Self {
            attempt_id: id(v.id)?,
            run_id: id(v.run_id)?,
            attempt_number: AttemptNumber::new(v.number).map_err(|_| Error::Corrupt)?,
            worker_id: WorkerId::new(v.worker_id).map_err(|_| Error::Corrupt)?,
            lease_id: id(v.lease_id)?,
            fencing_token: FencingToken::new(v.fencing_token).map_err(|_| Error::Corrupt)?,
            lease_expires_at: UtcTimestamp::from_unix_millis(v.lease_expires_at_ms),
        })
    }
}
impl From<&DeviceCanonicalFileVersion> for Version {
    fn from(v: &DeviceCanonicalFileVersion) -> Self {
        Self {
            node_id: v.node_id.to_string(),
            artifact_id: v.artifact_id.to_string(),
            artifact_version_id: v.artifact_version_id.to_string(),
            version: v.version.get(),
            content_ref: (&v.content_ref).into(),
            dependency_refs: v.dependency_refs.iter().map(Into::into).collect(),
        }
    }
}
impl TryFrom<Version> for DeviceCanonicalFileVersion {
    type Error = Error;
    fn try_from(v: Version) -> Result<Self, Error> {
        Ok(Self {
            node_id: id(v.node_id)?,
            artifact_id: id(v.artifact_id)?,
            artifact_version_id: id(v.artifact_version_id)?,
            version: VersionNumber::new(v.version).map_err(|_| Error::Corrupt)?,
            content_ref: v.content_ref.try_into()?,
            dependency_refs: references(v.dependency_refs)?,
        })
    }
}
pub(super) fn encode_command(v: &DeviceResourceCommand) -> Result<Vec<u8>, Error> {
    encode(&Command::from(v))
}
pub(super) fn decode_command(bytes: &[u8]) -> Result<DeviceResourceCommand, Error> {
    decode::<Command>(bytes)?.try_into()
}
pub(super) fn encode_request(v: &DeviceResourceRequest) -> Result<Vec<u8>, Error> {
    encode(&Request {
        schema: if v.command.result_metadata.is_some() {
            "musteragent.device-resource-request@3"
        } else if v.is_memory_approval() {
            "musteragent.device-resource-request@2"
        } else {
            "musteragent.device-resource-request@1"
        }
        .into(),
        command: v.command().into(),
        attempt: v.attempt().into(),
        memory_approval_only: v.is_memory_approval(),
        result_roots_hex: v.result_roots.iter().map(|b| hex(b)).collect(),
    })
}
pub(super) fn decode_request(bytes: &[u8]) -> Result<DeviceResourceRequest, Error> {
    let v: Request = decode(bytes)?;
    if v.schema
        != if v.command.result_artifacts_hex.is_some() {
            "musteragent.device-resource-request@3"
        } else if v.memory_approval_only {
            "musteragent.device-resource-request@2"
        } else {
            "musteragent.device-resource-request@1"
        }
    {
        return Err(Error::Corrupt);
    }
    let request = DeviceResourceRequest::new(v.command.try_into()?, v.attempt.try_into()?)?;
    if v.memory_approval_only {
        request.for_memory_approval()
    } else if !v.result_roots_hex.is_empty() {
        request.with_result_roots(
            v.result_roots_hex
                .into_iter()
                .map(unhex)
                .collect::<Result<Vec<_>, _>>()?,
        )
    } else {
        Ok(request)
    }
}
pub(super) fn encode_receipt(v: &DeviceResourceReceipt) -> Result<Vec<u8>, Error> {
    let outcome = match &v.outcome {
        DeviceResourceOutcome::Succeeded(value) => Outcome::Succeeded {
            result_ref: (&value.result_ref).into(),
            result_schema: (&value.result_schema).into(),
            result_hex: hex(&value.result_bytes),
            authorized_content: value.authorized_content.iter().map(Into::into).collect(),
            produced_versions: value.produced_versions.iter().map(Into::into).collect(),
        },
        DeviceResourceOutcome::Rejected(code) => Outcome::Rejected {
            code: match code {
                DeviceResourceRejectionCode::InvalidArguments => RejectionCode::InvalidArguments,
                DeviceResourceRejectionCode::Conflict => RejectionCode::Conflict,
                DeviceResourceRejectionCode::ReferenceUnavailable => {
                    RejectionCode::ReferenceUnavailable
                }
                DeviceResourceRejectionCode::AccessDenied => RejectionCode::AccessDenied,
            },
        },
    };
    encode(&Receipt {
        schema: "musteragent.device-resource-receipt@1".into(),
        command_commitment: v.command_commitment.to_string(),
        invocation_id: v.invocation_id.to_string(),
        operation: v.operation.tool_key().into(),
        outcome,
        read_dependencies: v.read_dependencies.iter().map(Into::into).collect(),
        committed_at_ms: v.committed_at.unix_millis(),
    })
}
pub(super) fn decode_receipt(bytes: &[u8]) -> Result<DeviceResourceReceipt, Error> {
    let v: Receipt = decode(bytes)?;
    if v.schema != "musteragent.device-resource-receipt@1" {
        return Err(Error::Corrupt);
    }
    let outcome = match v.outcome {
        Outcome::Succeeded {
            result_ref,
            result_schema,
            result_hex,
            authorized_content,
            produced_versions,
        } => DeviceResourceOutcome::Succeeded(DeviceResourceSuccess {
            result_ref: result_ref.try_into()?,
            result_schema: result_schema.try_into()?,
            result_bytes: unhex(result_hex)?,
            authorized_content: references(authorized_content)?,
            produced_versions: produced_versions
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        }),
        Outcome::Rejected { code } => DeviceResourceOutcome::Rejected(match code {
            RejectionCode::InvalidArguments => DeviceResourceRejectionCode::InvalidArguments,
            RejectionCode::Conflict => DeviceResourceRejectionCode::Conflict,
            RejectionCode::ReferenceUnavailable => {
                DeviceResourceRejectionCode::ReferenceUnavailable
            }
            RejectionCode::AccessDenied => DeviceResourceRejectionCode::AccessDenied,
        }),
    };
    Ok(DeviceResourceReceipt {
        command_commitment: hash(v.command_commitment)?,
        invocation_id: id(v.invocation_id)?,
        operation: DeviceResourceOperation::from_tool_key(&v.operation)?,
        outcome,
        read_dependencies: references(v.read_dependencies)?,
        committed_at: UtcTimestamp::from_unix_millis(v.committed_at_ms),
    })
}
impl From<&crate::DeviceArtifactProducer> for Producer {
    fn from(v: &crate::DeviceArtifactProducer) -> Self {
        Self {
            invocation_id: v.invocation_id.to_string(),
            tool_call_id: v.tool_call_id.as_str().into(),
            tool_key: v.tool_key.as_str().into(),
            result_ref: (&v.result_ref).into(),
            terminal_record_id: v.terminal_record_id.to_string(),
            terminal_record_hash: v.terminal_record_hash.to_string(),
            terminal_run_sequence: v.terminal_run_sequence.get(),
            terminal_at_ms: v.terminal_at.unix_millis(),
        }
    }
}
impl TryFrom<Producer> for crate::DeviceArtifactProducer {
    type Error = Error;
    fn try_from(v: Producer) -> Result<Self, Error> {
        Ok(Self {
            invocation_id: id(v.invocation_id)?,
            tool_call_id: ToolCallId::new(v.tool_call_id).map_err(|_| Error::Corrupt)?,
            tool_key: key(v.tool_key)?,
            result_ref: v.result_ref.try_into()?,
            terminal_record_id: id(v.terminal_record_id)?,
            terminal_record_hash: hash(v.terminal_record_hash)?,
            terminal_run_sequence: RunSeq::new(v.terminal_run_sequence)
                .map_err(|_| Error::Corrupt)?,
            terminal_at: UtcTimestamp::from_unix_millis(v.terminal_at_ms),
        })
    }
}
pub(super) fn encode_manifest(v: &DeviceResourceDeliveryManifest) -> Result<Vec<u8>, Error> {
    encode(&DeliveryManifest {
        schema: "musteragent.device-resource-delivery@1".into(),
        request_fingerprint: v.request_fingerprint.to_string(),
        entries: v
            .entries
            .iter()
            .map(|e| DeliveryReference {
                role: match e.role {
                    crate::DeviceArtifactExportRole::Delivery => "delivery",
                    crate::DeviceArtifactExportRole::Evidence => "evidence",
                }
                .into(),
                admission: (&e.admission).into(),
                command_commitment: e.command_commitment.to_string(),
                receipt_commitment: e.receipt_commitment.to_string(),
                operation: e.operation.tool_key().into(),
                version: (&e.version).into(),
                producer: (&e.producer).into(),
            })
            .collect(),
    })
}
pub(super) fn decode_manifest(bytes: &[u8]) -> Result<DeviceResourceDeliveryManifest, Error> {
    let v: DeliveryManifest = decode(bytes)?;
    if v.schema != "musteragent.device-resource-delivery@1" {
        return Err(Error::Corrupt);
    }
    Ok(DeviceResourceDeliveryManifest {
        request_fingerprint: hash(v.request_fingerprint)?,
        entries: v
            .entries
            .into_iter()
            .map(|e| {
                Ok(DeviceResourceDeliveryReference {
                    role: match e.role.as_str() {
                        "delivery" => crate::DeviceArtifactExportRole::Delivery,
                        "evidence" => crate::DeviceArtifactExportRole::Evidence,
                        _ => return Err(Error::Corrupt),
                    },
                    admission: e.admission.try_into()?,
                    command_commitment: hash(e.command_commitment)?,
                    receipt_commitment: hash(e.receipt_commitment)?,
                    operation: DeviceResourceOperation::from_tool_key(&e.operation)?,
                    version: e.version.try_into()?,
                    producer: e.producer.try_into()?,
                })
            })
            .collect::<Result<_, _>>()?,
    })
}

impl From<RuntimeHome> for Home {
    fn from(v: RuntimeHome) -> Self {
        match v {
            RuntimeHome::Cloud => Self {
                kind: "cloud".into(),
                device_id: None,
            },
            RuntimeHome::Device { device_id } => Self {
                kind: "device".into(),
                device_id: Some(device_id.to_string()),
            },
        }
    }
}
impl TryFrom<Home> for RuntimeHome {
    type Error = Error;
    fn try_from(v: Home) -> Result<Self, Error> {
        match (v.kind.as_str(), v.device_id) {
            ("cloud", None) => Ok(Self::Cloud),
            ("device", Some(value)) => Ok(Self::Device {
                device_id: id(value)?,
            }),
            _ => Err(Error::Corrupt),
        }
    }
}
impl From<&RunContext> for Run {
    fn from(v: &RunContext) -> Self {
        let kind = match v.activity_kind {
            ActivityKind::Chat => "chat",
            ActivityKind::Coordination => "coordination",
            ActivityKind::Automation => "automation",
            ActivityKind::Delegation => "delegation",
            ActivityKind::Maintenance => "maintenance",
            ActivityKind::Evaluation => "evaluation",
        };
        // Construction already rejects every non-DevicePrivate scope.
        let scope = match v.execution_scope {
            ExecutionScope::DevicePrivate {
                device_id,
                workspace_id,
            } => DeviceScope {
                kind: "device_private".into(),
                device_id: device_id.to_string(),
                workspace_id: workspace_id.map(|id| id.to_string()),
            },
            _ => DeviceScope {
                kind: "invalid".into(),
                device_id: String::new(),
                workspace_id: None,
            },
        };
        Self {
            tenant_id: v.tenant_id.to_string(),
            conversation_id: v.conversation_id.to_string(),
            activity_id: v.activity_id.to_string(),
            activity_kind: kind.into(),
            run_id: v.run_id.to_string(),
            executor_participant_id: v.executor_participant_id.to_string(),
            requested_by_participant_id: v.requested_by_participant_id.to_string(),
            parent_run_id: v.parent_run_id.map(|id| id.to_string()),
            delegation_id: v.delegation_id.map(|id| id.to_string()),
            execution_scope: scope,
            control_home: v.control_home.into(),
            execution_home: v.execution_home.into(),
            home_epoch: v.home_epoch.get(),
            runtime_snapshot_id: v.runtime_snapshot_id.to_string(),
        }
    }
}
impl TryFrom<Run> for RunContext {
    type Error = Error;
    fn try_from(v: Run) -> Result<Self, Error> {
        if v.execution_scope.kind != "device_private" {
            return Err(Error::Corrupt);
        }
        let activity_kind = match v.activity_kind.as_str() {
            "chat" => ActivityKind::Chat,
            "coordination" => ActivityKind::Coordination,
            "automation" => ActivityKind::Automation,
            "delegation" => ActivityKind::Delegation,
            "maintenance" => ActivityKind::Maintenance,
            "evaluation" => ActivityKind::Evaluation,
            _ => return Err(Error::Corrupt),
        };
        Ok(Self {
            tenant_id: id(v.tenant_id)?,
            conversation_id: id(v.conversation_id)?,
            activity_id: id(v.activity_id)?,
            activity_kind,
            run_id: id(v.run_id)?,
            executor_participant_id: id(v.executor_participant_id)?,
            requested_by_participant_id: id(v.requested_by_participant_id)?,
            parent_run_id: v.parent_run_id.map(id).transpose()?,
            delegation_id: v.delegation_id.map(id).transpose()?,
            execution_scope: ExecutionScope::DevicePrivate {
                device_id: id(v.execution_scope.device_id)?,
                workspace_id: v.execution_scope.workspace_id.map(id).transpose()?,
            },
            control_home: v.control_home.try_into()?,
            execution_home: v.execution_home.try_into()?,
            home_epoch: HomeEpoch::new(v.home_epoch).map_err(|_| Error::Corrupt)?,
            runtime_snapshot_id: id(v.runtime_snapshot_id)?,
        })
    }
}
impl From<&InvocationExecutionTiming> for Timing {
    fn from(v: &InvocationExecutionTiming) -> Self {
        match v {
            InvocationExecutionTiming::LegacyFixed { deadline } => Self::Legacy {
                deadline_ms: deadline.unix_millis(),
            },
            InvocationExecutionTiming::ActivityV1(t) => Self::Activity {
                policy_key: t.policy.key.as_str().into(),
                policy_digest: t.policy.digest.to_string(),
                business_deadline: t.business_deadline.as_ref().map(|d| Deadline {
                    at_ms: d.at.unix_millis(),
                    source: d.source.as_str().into(),
                }),
            },
        }
    }
}
impl TryFrom<Timing> for InvocationExecutionTiming {
    type Error = Error;
    fn try_from(v: Timing) -> Result<Self, Error> {
        Ok(match v {
            Timing::Legacy { deadline_ms } => Self::LegacyFixed {
                deadline: UtcTimestamp::from_unix_millis(deadline_ms),
            },
            Timing::Activity {
                policy_key,
                policy_digest,
                business_deadline,
            } => Self::ActivityV1(ActivityExecutionTiming {
                policy: LivenessPolicyRef {
                    key: key(policy_key)?,
                    digest: hash(policy_digest)?,
                },
                business_deadline: business_deadline
                    .map(|d| -> Result<BusinessDeadline, Error> {
                        Ok(BusinessDeadline {
                            at: UtcTimestamp::from_unix_millis(d.at_ms),
                            source: key(d.source)?,
                        })
                    })
                    .transpose()?,
            }),
        })
    }
}
