use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{AgentId, CommandId, MessageId, PrincipalId, VersionedKey, WorkspaceId};
use musteragent_runtime_contract::{AttachmentRef, SafeDisplayText};

use crate::{
    ConversationKind, ConversationTitleOrigin, MessageRole, ParticipantDisplaySnapshot,
    ParticipantKind, ServiceRef,
};

/// Why a visible assistant message was committed without a normal Run completion.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PartialMessageReason {
    Cancelled,
    Interrupted,
    ProviderFailure,
    ToolFailure,
}

/// Conversation-owned facts. Speaker identity remains in the Ledger envelope.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ConversationEvent {
    Created {
        kind: ConversationKind,
        control_home: RuntimeHome,
        workspace_id: Option<WorkspaceId>,
    },
    /// One-way assignment of an originally unscoped Conversation. Historical
    /// Run scopes and accepted command receipts remain unchanged.
    WorkspaceAssigned {
        workspace_id: WorkspaceId,
        assignment_command_id: CommandId,
    },
    Archived,
    /// An archived Conversation returned to the active catalog.
    Unarchived,
    TitleChanged {
        title: musteragent_runtime_contract::DisplayText,
        origin: ConversationTitleOrigin,
        set_by: PrincipalId,
    },
    ParticipantJoined {
        kind: ParticipantKind,
        principal_ref: Option<PrincipalId>,
        agent_ref: Option<AgentId>,
        service_ref: Option<ServiceRef>,
        display_snapshot: ParticipantDisplaySnapshot,
    },
    ParticipantLeft,
    MessageAccepted {
        message_id: MessageId,
        role: MessageRole,
        content_ref: ContentRef,
        attachment_refs: Vec<AttachmentRef>,
        /// Explicit skills selected by the user for this immutable message turn.
        requested_skill_refs: Vec<VersionedKey>,
        project_task_target: Option<crate::ProjectTaskTarget>,
    },
    MessageCommitted {
        message_id: MessageId,
        role: MessageRole,
        content_ref: ContentRef,
        content_hash: Sha256,
        content_bytes: u64,
    },
    MessagePartiallyCommitted {
        message_id: MessageId,
        content_ref: ContentRef,
        content_hash: Sha256,
        content_bytes: u64,
        reason: PartialMessageReason,
    },
    MessageImported {
        message_id: MessageId,
        role: MessageRole,
        content_ref: ContentRef,
        projection_ref: Option<ContentRef>,
        attachment_refs: Vec<AttachmentRef>,
        legacy_message_id: SafeDisplayText,
        source_sequence: u64,
        source_payload_hash: Sha256,
    },
    MessageRetracted {
        message_id: MessageId,
        reason: SafeDisplayText,
        source_run_id: Option<musteragent_identity::RunId>,
    },
}
