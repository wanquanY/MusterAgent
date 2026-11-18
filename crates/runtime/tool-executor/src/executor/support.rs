use super::*;

pub(super) fn port_from_content_read(error: ContentStoreError) -> LoopPortError {
    match error {
        ContentStoreError::Unavailable => port(ErrorCode::ContentUnavailable, true),
        ContentStoreError::Unauthorized => port(ErrorCode::PolicyDenied, false),
        ContentStoreError::Invariant => port(ErrorCode::InternalInvariantViolated, false),
        ContentStoreError::NotFound
        | ContentStoreError::TooLarge
        | ContentStoreError::LengthMismatch
        | ContentStoreError::HashMismatch
        | ContentStoreError::IdentityConflict => port(ErrorCode::ContentIntegrityFailed, false),
    }
}

pub(super) fn media_type() -> Result<MediaType, LoopPortError> {
    MediaType::new(JSON_MEDIA_TYPE).map_err(|_| port(ErrorCode::InternalInvariantViolated, false))
}

/// The model's arguments as the Tool receives them. A progress-narrating run
/// adds a Runtime-owned `progress_note` to every model-facing schema; it stays
/// in the committed call (so history keeps showing it to the model) and is
/// dropped here for any Tool whose published schema does not define it.
pub(super) fn execution_arguments(
    arguments: &JsonValue,
    material: &ToolExecutionMaterial,
) -> serde_json::Value {
    let mut value = json_to_serde(arguments);
    let field = musteragent_execution_context::PROGRESS_NOTE_FIELD;
    if let serde_json::Value::Object(object) = &mut value
        && object.contains_key(field)
        && !published_schema_defines(&material.tool.input_schema_json, field)
    {
        object.remove(field);
    }
    value
}

/// A `dialogue.publish@1` body that the strict decoder reads as a Runtime
/// progress message. Model calls never reach dispatch with this body: tool
/// preparation admits it only for the reserved Runtime call id.
pub(super) fn runtime_progress_body(
    material: &ToolExecutionMaterial,
    arguments: &serde_json::Value,
) -> bool {
    use musteragent_execution_context::{
        DIALOGUE_PUBLISH_TOOL, DialoguePublicationPurpose, decode_native_publication,
    };
    material.tool.manifest.key.as_str() == DIALOGUE_PUBLISH_TOOL
        && serde_json::to_vec(arguments)
            .ok()
            .and_then(|bytes| decode_native_publication(DIALOGUE_PUBLISH_TOOL, &bytes).ok())
            .is_some_and(|publication| publication.purpose == DialoguePublicationPurpose::Progress)
}

fn published_schema_defines(schema: &[u8], field: &str) -> bool {
    serde_json::from_slice::<serde_json::Value>(schema)
        .ok()
        .and_then(|schema| schema.get("properties").cloned())
        .and_then(|properties| properties.get(field).cloned())
        .is_some()
}

pub(super) fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

pub(super) fn port_from_tool(
    error: musteragent_runtime_capability::ToolExecutionPortError,
) -> LoopPortError {
    port(error.code, error.retryable)
}

pub(super) fn port_from_content_write(error: ContentStoreError) -> LoopPortError {
    match error {
        ContentStoreError::Unavailable => port(ErrorCode::DependencyUnavailable, true),
        // Canonical provider output is bounded well below the content-store
        // limit. Reaching this branch before a durable invocation exists is a
        // runtime configuration/invariant failure, not a Tool rejection.
        ContentStoreError::TooLarge => port(ErrorCode::InternalInvariantViolated, false),
        ContentStoreError::LengthMismatch | ContentStoreError::HashMismatch => {
            port(ErrorCode::ContentIntegrityFailed, false)
        }
        ContentStoreError::IdentityConflict => port(ErrorCode::IdempotencyConflict, false),
        ContentStoreError::NotFound
        | ContentStoreError::Unauthorized
        | ContentStoreError::Invariant => port(ErrorCode::InternalInvariantViolated, false),
    }
}

pub fn invocation_id(
    run_id: musteragent_identity::RunId,
    call_id: &ToolCallId,
) -> Result<InvocationId, LoopPortError> {
    let mut seed = b"musteragent.legacy.tool.invocation.v1\0".to_vec();
    seed.extend_from_slice(run_id.into_uuid().as_bytes());
    seed.extend_from_slice(call_id.as_str().as_bytes());
    InvocationId::from_uuid(deterministic_uuid(&seed))
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))
}

pub fn request_content_id(invocation_id: InvocationId) -> Result<ContentId, LoopPortError> {
    let mut seed = b"musteragent.legacy.tool.request-content.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    ContentId::from_uuid(deterministic_uuid(&seed))
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))
}

pub fn interaction_id(
    invocation_id: InvocationId,
    purpose: &[u8],
) -> Result<InteractionId, LoopPortError> {
    let mut seed = b"musteragent.legacy.tool.interaction.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    seed.extend_from_slice(purpose);
    InteractionId::from_uuid(deterministic_uuid(&seed))
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))
}

pub(super) fn deterministic_uuid(seed: &[u8]) -> uuid::Uuid {
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(value)
}

pub fn nested_invocation_id(
    invocation_id: InvocationId,
    attempt_id: musteragent_identity::AttemptId,
    purpose: &[u8],
) -> Result<InvocationId, LoopPortError> {
    let mut seed = b"musteragent.legacy.nested-model.invocation.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    seed.extend_from_slice(attempt_id.into_uuid().as_bytes());
    seed.extend_from_slice(purpose);
    InvocationId::from_uuid(deterministic_uuid(&seed))
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))
}

pub fn metering_outbox_id(invocation_id: InvocationId) -> Result<OutboxMessageId, LoopPortError> {
    let mut seed = b"musteragent.legacy.model-usage.outbox.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    OutboxMessageId::from_uuid(deterministic_uuid(&seed))
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))
}

pub fn invocation_idempotency_key(
    invocation_id: InvocationId,
) -> Result<IdempotencyKey, LoopPortError> {
    IdempotencyKey::new(format!("tool:{invocation_id}"))
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))
}

pub(super) const RUNTIME_OPERATOR_RECONCILIATION_PREFIX: &str = "runtime-operator:";

pub(super) fn operator_reconciliation_ref(
    invocation_id: InvocationId,
) -> Result<OpaqueExternalRef, LoopPortError> {
    OpaqueExternalRef::new(format!(
        "{RUNTIME_OPERATOR_RECONCILIATION_PREFIX}{invocation_id}"
    ))
    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))
}

pub(super) fn authorization_receipt_query(
    material: &ToolExecutionMaterial,
    session_fingerprint: Sha256,
    observed_at: musteragent_identity::UtcTimestamp,
) -> AuthorizationReceiptQuery {
    AuthorizationReceiptQuery {
        tenant_id: material.run_context.tenant_id,
        principal_id: material.actor.principal_id,
        conversation_id: material.run_context.conversation_id,
        workspace_id: workspace_id(&material.run_context.execution_scope),
        capability_key: material.capability_grant.key.clone(),
        capability_provider_id: material.capability_grant.provider_id,
        permission_mode: material.permission_mode,
        scope_fingerprint: authorization_receipt_scope_fingerprint(material, session_fingerprint),
        observed_at,
    }
}

pub(super) fn session_authorization_is_reusable(
    capability_decision: Decision,
    approval_requirement: ApprovalRequirement,
    invocation_disposition: InvocationPolicyDisposition,
) -> bool {
    capability_decision != Decision::RequireApproval
        && approval_requirement != ApprovalRequirement::Always
        && !matches!(
            invocation_disposition,
            InvocationPolicyDisposition::RequireApproval(_) | InvocationPolicyDisposition::Deny(_)
        )
}

pub(super) fn authorization_receipt(
    query: &AuthorizationReceiptQuery,
    source_interaction_id: InteractionId,
    issued_at: musteragent_identity::UtcTimestamp,
) -> Result<AuthorizationReceipt, LoopPortError> {
    let expires_at = issued_at
        .checked_add_millis(SESSION_AUTHORIZATION_LIFETIME_MS)
        .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false))?;
    Ok(AuthorizationReceipt {
        receipt_id: AuthorizationReceiptId::from_uuid(source_interaction_id.into_uuid())
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
        tenant_id: query.tenant_id,
        principal_id: query.principal_id,
        conversation_id: query.conversation_id,
        workspace_id: query.workspace_id,
        source_interaction_id,
        capability_key: query.capability_key.clone(),
        capability_provider_id: query.capability_provider_id,
        permission_mode: query.permission_mode,
        scope_fingerprint: query.scope_fingerprint,
        issued_at,
        expires_at,
    })
}

pub(super) fn receipt_matches_query(
    receipt: &AuthorizationReceipt,
    query: &AuthorizationReceiptQuery,
) -> bool {
    receipt.validate().is_ok()
        && receipt.tenant_id == query.tenant_id
        && receipt.principal_id == query.principal_id
        && receipt.conversation_id == query.conversation_id
        && receipt.workspace_id == query.workspace_id
        && receipt.capability_key == query.capability_key
        && receipt.capability_provider_id == query.capability_provider_id
        && receipt.permission_mode == query.permission_mode
        && receipt.scope_fingerprint == query.scope_fingerprint
        && receipt.expires_at > query.observed_at
}

pub(super) fn authorization_receipt_scope_fingerprint(
    material: &ToolExecutionMaterial,
    session_fingerprint: Sha256,
) -> Sha256 {
    let mut bytes = b"musteragent.authorization-receipt.scope.v1\0".to_vec();
    receipt_append(&mut bytes, &session_fingerprint.into_bytes());
    receipt_append(
        &mut bytes,
        material.run_context.tenant_id.to_string().as_bytes(),
    );
    receipt_append(
        &mut bytes,
        material.actor.principal_id.to_string().as_bytes(),
    );
    receipt_append(
        &mut bytes,
        material.run_context.conversation_id.to_string().as_bytes(),
    );
    receipt_append(
        &mut bytes,
        workspace_id(&material.run_context.execution_scope)
            .map(|value| value.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    receipt_append(
        &mut bytes,
        material.capability_grant.key.as_str().as_bytes(),
    );
    receipt_append(
        &mut bytes,
        material.capability_grant.provider_id.to_string().as_bytes(),
    );
    bytes.push(material.permission_mode.canonical_code());
    bytes.push(match material.capability_grant.location {
        musteragent_capability::CapabilityLocation::Cloud => 1,
        musteragent_capability::CapabilityLocation::Backend => 2,
        musteragent_capability::CapabilityLocation::Device => 3,
        musteragent_capability::CapabilityLocation::RemoteDevice => 4,
        musteragent_capability::CapabilityLocation::Browser => 5,
    });
    bytes.push(match material.capability_grant.effect {
        musteragent_policy::Effect::Read => 1,
        musteragent_policy::Effect::WriteReversible => 2,
        musteragent_policy::Effect::WriteExternal => 3,
        musteragent_policy::Effect::Destructive => 4,
    });
    bytes.push(match material.capability_grant.policy_decision {
        Decision::Allow => 1,
        Decision::Deny => 2,
        Decision::RequireApproval => 3,
    });
    // Workspace is only an invocation index. Shared approvals must bind the
    // complete project/Home identity, including epoch, independently of it.
    if let musteragent_event_envelope::ExecutionScope::HostedProject { scope } =
        material.run_context.execution_scope
    {
        receipt_append(&mut bytes, b"musteragent.hosted-approval-scope.v1");
        receipt_append(&mut bytes, &scope.identity_bytes());
    }
    for scope in &material.capability_grant.allowed_scopes {
        receipt_append(&mut bytes, scope.as_str().as_bytes());
    }
    Sha256::digest(bytes)
}

pub(super) fn receipt_append(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

pub(super) fn workspace_id(
    scope: &musteragent_event_envelope::ExecutionScope,
) -> Option<WorkspaceId> {
    match scope {
        musteragent_event_envelope::ExecutionScope::HostedProject { scope } => {
            Some(scope.workspace_id)
        }
        musteragent_event_envelope::ExecutionScope::Workspace { workspace_id }
        | musteragent_event_envelope::ExecutionScope::DevicePrivate {
            workspace_id: Some(workspace_id),
            ..
        } => Some(*workspace_id),
        musteragent_event_envelope::ExecutionScope::Tenant { .. }
        | musteragent_event_envelope::ExecutionScope::DevicePrivate {
            workspace_id: None, ..
        } => None,
    }
}

pub(super) fn content_reference_json(reference: musteragent_content_ref::ContentRef) -> String {
    content_reference_value(&reference).to_string()
}

pub(super) fn content_reference_value(reference: &ContentRef) -> serde_json::Value {
    serde_json::json!({
        "content_ref": {
            "content_id": reference.content_id.to_string(),
            "media_type": reference.media_type.as_str(),
            "byte_length": reference.byte_length,
            "sha256": reference.sha256.to_string(),
        }
    })
}

pub(super) fn json_to_serde(value: &JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(value) => serde_json::Value::Bool(*value),
        JsonValue::Integer(value) => serde_json::Value::Number((*value).into()),
        JsonValue::Number(value) => serde_json::Number::from_f64(*value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        JsonValue::String(value) => serde_json::Value::String(value.clone()),
        JsonValue::Array(values) => {
            serde_json::Value::Array(values.iter().map(json_to_serde).collect())
        }
        JsonValue::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_to_serde(value)))
                .collect::<serde_json::Map<_, _>>(),
        ),
    }
}

/// Reconstruct the immutable request identity for journal reads. Execution state
/// belongs to the persisted observation, never to the request used to load it.
pub(super) fn observation_request(
    attempt: &AttemptContext,
    invocation: &ToolInvocation,
) -> ToolInvocation {
    let mut proposed = invocation.clone();
    proposed.requested_by_attempt_id = attempt.attempt_id;
    proposed.status = ToolInvocationStatus::Requested;
    proposed.execution_attempt_id = None;
    proposed.result_ref = None;
    proposed.interaction_id = None;
    proposed
}
