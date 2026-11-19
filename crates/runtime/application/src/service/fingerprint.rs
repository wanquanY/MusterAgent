use musteragent_content_ref::Sha256;
use musteragent_conversation::{
    ArchiveConversation, ConversationKind, ConversationTitleOrigin, CreateConversation,
    RetractMessage, SetConversationTitle, SubmitMessage, UnarchiveConversation,
};
use musteragent_event_envelope::RuntimeHome;
use musteragent_ledger::AppendReceipt;
use musteragent_run::{EnqueueRunControl, RetryRun, RunControlKind};
use musteragent_runtime_contract::{
    ActiveRunArchivePolicy, CommandAck, CommandDisposition, CommandEnvelope, InputContent,
    ResourceRef, RuntimeError,
};
use musteragent_scheduler::AdmissionSelection;

use crate::identity::AdmissionIdentities;

use super::error_mapping::invariant;

const KERNEL_COMMAND_FINGERPRINT_DOMAIN_V2: &[u8] = b"musteragent.command-fingerprint.v2\0";
const LEGACY_COMMAND_FINGERPRINT_DOMAIN_V1: &[u8] = b"musteragent.legacy.command-fingerprint.v1\0";

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

fn command_fingerprint_domain(command_schema: &str) -> &'static [u8] {
    if command_schema == "musteragent.legacy.runtime.command.v1" {
        LEGACY_COMMAND_FINGERPRINT_DOMAIN_V1
    } else {
        KERNEL_COMMAND_FINGERPRINT_DOMAIN_V2
    }
}

pub(super) fn create_command_fingerprint(command: &CommandEnvelope<CreateConversation>) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"create-conversation");
    bytes.extend_from_slice(command.payload.conversation_id.into_uuid().as_bytes());
    bytes.push(match command.payload.kind {
        ConversationKind::Direct => 1,
        ConversationKind::Team => 2,
        ConversationKind::Automation => 3,
        ConversationKind::System => 4,
    });
    match &command.payload.title {
        None => bytes.push(0),
        Some(title) => {
            bytes.push(1);
            append_fingerprint_field(&mut bytes, title.as_str().as_bytes());
        }
    }
    append_runtime_home(&mut bytes, command.payload.control_home);
    match command.payload.workspace_id {
        Some(workspace_id) => {
            bytes.push(1);
            bytes.extend_from_slice(workspace_id.into_uuid().as_bytes());
        }
        None => bytes.push(0),
    }
    Sha256::digest(bytes)
}

pub(super) fn retract_message_command_fingerprint(
    command: &CommandEnvelope<RetractMessage>,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"retract-message");
    bytes.extend_from_slice(command.payload.message_id.into_uuid().as_bytes());
    append_fingerprint_field(&mut bytes, command.payload.reason.as_str().as_bytes());
    Sha256::digest(bytes)
}

pub(super) fn submit_command_fingerprint(
    command: &CommandEnvelope<SubmitMessage>,
    selection: &AdmissionSelection,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"submit-message");
    append_submit_payload_fingerprint(&mut bytes, &command.payload, selection);
    Sha256::digest(bytes)
}

pub(super) fn retry_run_command_fingerprint(
    command: &CommandEnvelope<RetryRun>,
    selection: &AdmissionSelection,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"retry-run");
    bytes.extend_from_slice(command.payload.source_run_id.into_uuid().as_bytes());
    bytes.extend_from_slice(command.payload.source_message_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&command.payload.expected_attempt_number.get().to_be_bytes());
    bytes.extend_from_slice(command.payload.participant_id.into_uuid().as_bytes());
    append_fingerprint_field(
        &mut bytes,
        command.payload.requested_model_class.as_str().as_bytes(),
    );
    append_execution_scope(&mut bytes, command.payload.execution_scope);
    bytes.push(selection.surface.canonical_code());
    bytes.extend_from_slice(
        &u32::try_from(selection.requested_skill_refs.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for skill_ref in &selection.requested_skill_refs {
        append_fingerprint_field(&mut bytes, skill_ref.as_str().as_bytes());
    }
    Sha256::digest(bytes)
}

pub(super) fn enqueue_run_control_command_fingerprint(
    command: &CommandEnvelope<EnqueueRunControl>,
) -> Result<Sha256, RuntimeError> {
    let mut bytes = command_fingerprint_base(command, b"enqueue-run-control");
    bytes.extend_from_slice(command.payload.run_id.into_uuid().as_bytes());
    bytes.extend_from_slice(command.payload.message_id.into_uuid().as_bytes());
    bytes.extend_from_slice(command.payload.participant_id.into_uuid().as_bytes());
    bytes.push(match command.payload.kind {
        RunControlKind::Steering => 1,
        RunControlKind::FollowUp => 2,
    });
    match &command.payload.content {
        InputContent::InlineUtf8(value) => {
            bytes.push(1);
            append_fingerprint_field(&mut bytes, value.as_str().as_bytes());
        }
        InputContent::ContentRef(value) => {
            bytes.push(2);
            append_content_ref(&mut bytes, value);
        }
    }
    bytes.extend_from_slice(
        &u32::try_from(command.payload.attachment_refs.len())
            .map_err(|_| invariant(command))?
            .to_be_bytes(),
    );
    for attachment in &command.payload.attachment_refs {
        append_content_ref(&mut bytes, &attachment.content_ref);
        append_fingerprint_field(&mut bytes, attachment.display_name.as_str().as_bytes());
    }
    Ok(Sha256::digest(bytes))
}

pub(super) fn archive_conversation_command_fingerprint(
    command: &CommandEnvelope<ArchiveConversation>,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"archive-conversation");
    bytes.push(match command.payload.active_run_policy {
        ActiveRunArchivePolicy::Reject => 1,
        ActiveRunArchivePolicy::CancelThenArchive => 2,
    });
    Sha256::digest(bytes)
}

/// Unarchive carries no payload; the operation tag, actor and guard identify it.
pub(super) fn unarchive_conversation_command_fingerprint(
    command: &CommandEnvelope<UnarchiveConversation>,
) -> Sha256 {
    Sha256::digest(command_fingerprint_base(command, b"unarchive-conversation"))
}

pub(super) fn set_conversation_title_command_fingerprint(
    command: &CommandEnvelope<SetConversationTitle>,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"set-conversation-title");
    append_fingerprint_field(&mut bytes, command.payload.title.as_str().as_bytes());
    match command.payload.origin {
        ConversationTitleOrigin::User => bytes.push(1),
        ConversationTitleOrigin::FirstUserMessage => bytes.push(3),
        ConversationTitleOrigin::AcceptedProposal { proposal_record_id } => {
            bytes.push(2);
            bytes.extend_from_slice(proposal_record_id.into_uuid().as_bytes());
        }
    }
    Sha256::digest(bytes)
}

pub(super) fn append_submit_payload_fingerprint(
    bytes: &mut Vec<u8>,
    payload: &SubmitMessage,
    selection: &AdmissionSelection,
) {
    bytes.extend_from_slice(payload.message_id.into_uuid().as_bytes());
    bytes.extend_from_slice(payload.participant_id.into_uuid().as_bytes());
    match &payload.content {
        InputContent::InlineUtf8(value) => {
            bytes.push(1);
            append_fingerprint_field(bytes, value.as_str().as_bytes());
        }
        InputContent::ContentRef(value) => {
            bytes.push(2);
            append_content_ref(bytes, value);
        }
    }
    bytes.extend_from_slice(
        &u32::try_from(payload.attachment_refs.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for attachment in &payload.attachment_refs {
        append_content_ref(bytes, &attachment.content_ref);
    }
    append_fingerprint_field(bytes, payload.requested_model_class.as_str().as_bytes());
    bytes.push(u8::from(payload.requested_features.reasoning_summary));
    match payload.requested_features.reasoning_effort {
        Some(effort) => {
            bytes.push(1);
            append_fingerprint_field(bytes, effort.as_str().as_bytes());
        }
        None => bytes.push(0),
    }
    bytes.push(payload.requested_features.permission_mode.canonical_code());
    append_execution_scope(bytes, payload.execution_scope);
    bytes.push(selection.surface.canonical_code());
    bytes.extend_from_slice(
        &u32::try_from(selection.requested_skill_refs.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for skill_ref in &selection.requested_skill_refs {
        append_fingerprint_field(bytes, skill_ref.as_str().as_bytes());
    }
    if let Some(target) = payload.project_task_target {
        append_fingerprint_field(bytes, &target.canonical_bytes());
    }
}

pub(super) fn admission_ack(
    command_id: musteragent_identity::CommandId,
    message_id: musteragent_identity::MessageId,
    ids: AdmissionIdentities,
    receipt: AppendReceipt,
) -> CommandAck {
    CommandAck {
        command_id,
        disposition: CommandDisposition::Duplicate,
        committed_at: receipt.committed_at,
        first_conversation_seq: Some(receipt.first_sequence),
        last_conversation_seq: Some(receipt.last_sequence),
        resource_refs: vec![
            ResourceRef::Message(message_id),
            ResourceRef::Activity(ids.activity_id),
            ResourceRef::Run(ids.run_id),
        ],
    }
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

pub(super) fn append_content_ref(bytes: &mut Vec<u8>, value: &musteragent_content_ref::ContentRef) {
    bytes.extend_from_slice(value.content_id.into_uuid().as_bytes());
    append_fingerprint_field(bytes, value.media_type.as_str().as_bytes());
    bytes.extend_from_slice(&value.byte_length.to_be_bytes());
    bytes.extend_from_slice(&value.sha256.into_bytes());
}

fn append_runtime_home(bytes: &mut Vec<u8>, home: RuntimeHome) {
    match home {
        RuntimeHome::Cloud => bytes.push(1),
        RuntimeHome::Device { device_id } => {
            bytes.push(2);
            bytes.extend_from_slice(device_id.into_uuid().as_bytes());
        }
    }
}

fn append_execution_scope(bytes: &mut Vec<u8>, scope: musteragent_event_envelope::ExecutionScope) {
    scope.append_identity_bytes(bytes);
}

pub(super) fn resume_run_command_fingerprint(
    command: &CommandEnvelope<musteragent_run::ResumeRun>,
) -> Sha256 {
    let mut bytes = command_fingerprint_base(command, b"resume-run");
    bytes.extend_from_slice(command.payload.run_id.into_uuid().as_bytes());
    bytes.extend_from_slice(command.payload.participant_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&command.payload.expected_attempt_number.get().to_be_bytes());
    bytes.push(match command.payload.action {
        musteragent_run::RunRecoveryAction::RetryDependency => 1,
    });
    Sha256::digest(bytes)
}

#[cfg(test)]
mod tests {
    use musteragent_runtime_contract::COMMAND_SCHEMA_VERSION;

    use super::{
        KERNEL_COMMAND_FINGERPRINT_DOMAIN_V2, LEGACY_COMMAND_FINGERPRINT_DOMAIN_V1,
        command_fingerprint_domain,
    };

    #[test]
    fn command_schema_selects_exact_current_or_legacy_fingerprint_domain() {
        assert_eq!(
            command_fingerprint_domain(COMMAND_SCHEMA_VERSION),
            KERNEL_COMMAND_FINGERPRINT_DOMAIN_V2
        );
        assert_eq!(
            command_fingerprint_domain("musteragent.legacy.runtime.command.v1"),
            LEGACY_COMMAND_FINGERPRINT_DOMAIN_V1
        );
    }
}
