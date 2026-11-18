use super::*;
pub(super) fn command_fingerprint_base<T>(
    command: &CommandEnvelope<T>,
    operation: &[u8],
) -> Vec<u8> {
    let mut bytes = command_fingerprint_domain(command.metadata.schema_version.as_str()).to_vec();
    append_fingerprint_field(&mut bytes, operation);
    bytes.extend_from_slice(command.metadata.actor.tenant_id.into_uuid().as_bytes());
    bytes.extend_from_slice(command.metadata.actor.principal_id.into_uuid().as_bytes());
    bytes.push(match command.metadata.actor.principal_kind {
        musteragent_identity::PrincipalKind::User => 1,
        musteragent_identity::PrincipalKind::Service => 2,
        musteragent_identity::PrincipalKind::System => 3,
    });
    match command.metadata.conversation_guard {
        None => bytes.push(0),
        Some(guard) => {
            bytes.push(1);
            bytes.extend_from_slice(guard.conversation_id.into_uuid().as_bytes());
            bytes.extend_from_slice(&guard.expected_home_epoch.get().to_be_bytes());
            match guard.expected_conversation_seq {
                None => bytes.push(0),
                Some(sequence) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&sequence.get().to_be_bytes());
                }
            }
        }
    }
    bytes
}
pub(super) fn command_fingerprint_domain(command_schema: &str) -> &'static [u8] {
    if command_schema == LEGACY_ASSISTANT_COMMAND_SCHEMA_VERSION {
        LEGACY_ASSISTANT_COMMAND_FINGERPRINT_DOMAIN_V1
    } else {
        KERNEL_COMMAND_FINGERPRINT_DOMAIN_V2
    }
}
pub(super) fn respond_command_fingerprint(command: &CommandEnvelope<RespondInteraction>) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"respond-interaction");
    bytes.extend_from_slice(command.payload.interaction_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&response_hash(&command.payload.response).into_bytes());
    bytes.push(command.payload.response_authority.canonical_code());
    Sha256::digest(bytes)
}
pub(super) fn duplicate_ack(
    command_id: musteragent_identity::CommandId,
    receipt: AppendReceipt,
    resource_refs: Vec<ResourceRef>,
) -> CommandAck {
    CommandAck {
        command_id,
        disposition: CommandDisposition::Duplicate,
        committed_at: receipt.committed_at,
        first_conversation_seq: Some(receipt.first_sequence),
        last_conversation_seq: Some(receipt.last_sequence),
        resource_refs,
    }
}
pub(super) fn append_fingerprint_field(bytes: &mut Vec<u8>, value: &[u8]) {
    let length = u32::try_from(value.len()).unwrap_or(u32::MAX);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(value);
}
pub(super) fn interaction_response_error(
    error: InteractionResponseValidationError,
    command: &CommandEnvelope<RespondInteraction>,
) -> RuntimeError {
    let (code, message) = match error {
        InteractionResponseValidationError::InvalidResponse => (
            ErrorCode::InteractionResponseInvalid,
            "Interaction response does not satisfy its frozen schema",
        ),
        InteractionResponseValidationError::ContentUnavailable => (
            ErrorCode::ContentIntegrityFailed,
            "Interaction response content is unavailable",
        ),
        InteractionResponseValidationError::ContractMismatch => (
            ErrorCode::InternalInvariantViolated,
            "Persisted Interaction schema does not match its prompt contract",
        ),
    };
    runtime_error(code, command.metadata.correlation_id, false, message)
}
pub(super) fn invariant<T>(command: &CommandEnvelope<T>) -> RuntimeError {
    runtime_error(
        ErrorCode::InternalInvariantViolated,
        command.metadata.correlation_id,
        false,
        "Runtime application invariant was violated",
    )
}
pub(super) fn build_error<T>(code: ErrorCode, command: &CommandEnvelope<T>) -> RuntimeError {
    runtime_error(
        code,
        command.metadata.correlation_id,
        false,
        "Runtime application could not build a canonical commit",
    )
}
pub(super) fn create_command_record_id<T>(
    command: &CommandEnvelope<T>,
    index: u32,
) -> Option<LedgerRecordId> {
    let mut label = b"command-record:".to_vec();
    label.extend_from_slice(&index.to_be_bytes());
    LedgerRecordId::from_uuid(derive_for_command_schema(
        command.metadata.command_id,
        command.metadata.schema_version.as_str(),
        &label,
    ))
    .ok()
}
pub(super) fn create_command_outbox_id<T>(
    command: &CommandEnvelope<T>,
    index: u32,
) -> Option<OutboxMessageId> {
    let mut label = b"command-outbox:".to_vec();
    label.extend_from_slice(&index.to_be_bytes());
    OutboxMessageId::from_uuid(derive_for_command_schema(
        command.metadata.command_id,
        command.metadata.schema_version.as_str(),
        &label,
    ))
    .ok()
}
pub(super) fn derive_for_command_schema(
    command_id: CommandId,
    command_schema: &str,
    label: &[u8],
) -> uuid::Uuid {
    let domain = if command_schema == LEGACY_ASSISTANT_COMMAND_SCHEMA_VERSION {
        LEGACY_ASSISTANT_RUNTIME_IDENTITY_DOMAIN_V1
    } else {
        KERNEL_RUNTIME_IDENTITY_DOMAIN_V2
    };
    derive_in_domain(domain, command_id, label)
}
pub(super) fn derive_in_domain(domain: &[u8], command_id: CommandId, label: &[u8]) -> uuid::Uuid {
    let mut seed = domain.to_vec();
    seed.extend_from_slice(command_id.into_uuid().as_bytes());
    seed.extend_from_slice(label);
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(value)
}
pub(super) async fn load_guarded_replay<T>(
    store: &dyn RuntimeApplicationStore,
    command: &CommandEnvelope<T>,
    command_fingerprint: Sha256,
) -> Result<Option<musteragent_ledger::AppendReceipt>, RuntimeError> {
    let guard = command
        .metadata
        .conversation_guard
        .ok_or_else(|| invariant(command))?;
    store
        .load_command_replay(
            CommandReplayQuery {
                tenant_id: command.metadata.actor.tenant_id,
                conversation_id: guard.conversation_id,
                home_epoch: guard.expected_home_epoch,
                idempotency_key: command.metadata.idempotency_key.clone(),
                command_fingerprint,
            },
            command.metadata.correlation_id,
        )
        .await
}

const KERNEL_COMMAND_FINGERPRINT_DOMAIN_V2: &[u8] = b"musteragent.command-fingerprint.v2\0";
const LEGACY_ASSISTANT_COMMAND_FINGERPRINT_DOMAIN_V1: &[u8] =
    b"musteragent.legacy.command-fingerprint.v1\0";
const KERNEL_RUNTIME_IDENTITY_DOMAIN_V2: &[u8] = b"musteragent.runtime.application.v2\0";
const LEGACY_ASSISTANT_RUNTIME_IDENTITY_DOMAIN_V1: &[u8] =
    b"musteragent.legacy.runtime.application.v1\0";

const LEGACY_ASSISTANT_COMMAND_SCHEMA_VERSION: &str = "musteragent.legacy.runtime.command.v1";

use musteragent_approval::CancelInteraction;
use musteragent_run::CancelRun;
pub(super) fn cancel_interaction_command_fingerprint(
    command: &CommandEnvelope<CancelInteraction>,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"cancel-interaction");
    bytes.extend_from_slice(command.payload.interaction_id.into_uuid().as_bytes());
    bytes.push(cancel_reason_fingerprint(command.payload.reason));
    Sha256::digest(bytes)
}
pub(super) fn cancel_run_command_fingerprint(command: &CommandEnvelope<CancelRun>) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"cancel-run");
    bytes.extend_from_slice(command.payload.run_id.into_uuid().as_bytes());
    bytes.push(cancel_reason_fingerprint(command.payload.reason));
    Sha256::digest(bytes)
}
pub(super) const fn cancel_reason_fingerprint(reason: CancelReason) -> u8 {
    match reason {
        CancelReason::UserRequested => 1,
        CancelReason::ConversationArchived => 2,
        CancelReason::ParentCancelled => 3,
        CancelReason::PolicyRevoked => 4,
        CancelReason::SystemShutdown => 5,
        CancelReason::CoordinationInterrupted => 6,
    }
}
