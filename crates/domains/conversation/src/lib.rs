//! Conversation, Participant and visible Message invariants.

#![forbid(unsafe_code)]

mod events;
mod project_task_target;
mod purpose;
pub use project_task_target::ProjectTaskTarget;
pub use purpose::ConversationPurpose;
mod title;

pub use events::{ConversationEvent, PartialMessageReason};
pub use title::{
    FIRST_USER_MESSAGE_TITLE_MAX_CHARACTERS, first_user_message_title,
    first_user_message_title_from_text,
};

use musteragent_content_ref::ContentRef;
use musteragent_event_envelope::RuntimeHome;
use musteragent_event_envelope::{ExecutionScope, LedgerCursor};
use musteragent_identity::{
    ActivityId, Actor, AgentId, ContextBranchId, ConversationId, ConversationSeq, CorrelationId,
    DottedKey, LedgerRecordId, MessageId, ParticipantId, PrincipalId, RunId, TenantId,
    UtcTimestamp, WorkspaceId,
};
use musteragent_runtime_contract::{
    ActiveRunArchivePolicy, ApplicationFuture, ApplicationStream, AttachmentRef, CommandAck,
    CommandEnvelope, CommandPayload, CommandType, DisplayText, InputContent, ModelClass,
    RequestedFeatures, RuntimeError, SafeDisplayText,
};
use thiserror::Error;

const MAX_MESSAGE_ATTACHMENTS: usize = 64;

/// Product semantics for a Conversation, never a Runtime selector.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ConversationKind {
    Direct,
    Team,
    Automation,
    System,
}

/// Lifecycle of the stable visible container.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ConversationLifecycle {
    Active,
    Archived,
}

/// Canonical Conversation aggregate root fields needed by the first slice.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Conversation {
    pub conversation_id: ConversationId,
    pub tenant_id: TenantId,
    /// Stable Product workspace ownership. Execution scope is selected per Run
    /// and must never be used to reconstruct this catalog attribute.
    pub workspace_id: Option<WorkspaceId>,
    pub kind: ConversationKind,
    /// Canonical Participant that receives ordinary Conversation execution.
    ///
    /// Active Agent Participants are not execution-owner authority: delegated
    /// and collaborative Agents may coexist with this Participant. Consumers
    /// must resolve the root executor through this identity instead of
    /// inferring it from Participant cardinality or join order.
    pub execution_owner_participant_id: ParticipantId,
    pub title: Option<DisplayText>,
    pub control_home: RuntimeHome,
    pub home_epoch: musteragent_identity::HomeEpoch,
    pub next_conversation_seq: ConversationSeq,
    pub lifecycle: ConversationLifecycle,
    pub created_by: PrincipalId,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub archived_at: Option<UtcTimestamp>,
}

/// Public create use-case payload. Tenant and actor live in the Command envelope.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateConversation {
    pub conversation_id: ConversationId,
    pub workspace_id: Option<WorkspaceId>,
    pub kind: ConversationKind,
    pub title: Option<DisplayText>,
    pub control_home: RuntimeHome,
}

impl CreateConversation {
    pub fn validate(&self) -> Result<(), ConversationCommandError> {
        if self.kind != ConversationKind::Direct {
            return Err(ConversationCommandError::UnsupportedConversationKind);
        }
        Ok(())
    }

    /// Legacy Cloud-only validation retained for callers that have not yet
    /// moved home ownership into their composition-pinned Application policy.
    pub fn validate_first_cloud_slice(&self) -> Result<(), ConversationCommandError> {
        self.validate()?;
        if self.control_home != RuntimeHome::Cloud {
            return Err(ConversationCommandError::UnsupportedControlHome);
        }
        Ok(())
    }
}

impl CommandPayload for CreateConversation {
    const COMMAND_TYPE: CommandType = CommandType::CreateConversation;
}

/// Public message admission payload. Runtime configuration is intentionally absent.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubmitMessage {
    pub project_task_target: Option<ProjectTaskTarget>,
    pub message_id: MessageId,
    pub participant_id: ParticipantId,
    pub content: InputContent,
    pub attachment_refs: Vec<AttachmentRef>,
    pub requested_model_class: ModelClass,
    pub requested_features: RequestedFeatures,
    pub execution_scope: ExecutionScope,
}

impl SubmitMessage {
    pub fn validate(&self) -> Result<(), ConversationCommandError> {
        if self.attachment_refs.len() > MAX_MESSAGE_ATTACHMENTS {
            return Err(ConversationCommandError::TooManyAttachments);
        }
        Ok(())
    }
}

impl CommandPayload for SubmitMessage {
    const COMMAND_TYPE: CommandType = CommandType::SubmitMessage;
}

/// Retract one user turn and every visible message committed after it.
///
/// This is a semantic rewind: immutable Ledger records remain retained while
/// future context material excludes the retracted suffix.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RetractMessage {
    pub message_id: MessageId,
    pub reason: SafeDisplayText,
}

impl CommandPayload for RetractMessage {
    const COMMAND_TYPE: CommandType = CommandType::RetractMessage;
}

/// Append-only branch creation at one reachable canonical Ledger record.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ForkConversationContext {
    pub branch_id: ContextBranchId,
    pub parent_branch_id: Option<ContextBranchId>,
    pub fork_from_record_id: LedgerRecordId,
    pub branch_summary_ref: Option<ContentRef>,
}

impl CommandPayload for ForkConversationContext {
    const COMMAND_TYPE: CommandType = CommandType::ForkConversationContext;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ActivateConversationContextBranch {
    pub branch_id: ContextBranchId,
}

impl CommandPayload for ActivateConversationContextBranch {
    const COMMAND_TYPE: CommandType = CommandType::ActivateConversationContextBranch;
}

/// Atomically preserves the current leaf and admits a replacement user turn.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EditAndResubmitMessage {
    pub target_message_id: MessageId,
    pub preserved_branch_id: ContextBranchId,
    pub replacement_branch_id: ContextBranchId,
    pub message: SubmitMessage,
}

impl EditAndResubmitMessage {
    pub fn validate(&self) -> Result<(), ConversationCommandError> {
        if self.preserved_branch_id == self.replacement_branch_id
            || self.target_message_id == self.message.message_id
        {
            return Err(ConversationCommandError::InvalidEditAndResubmit);
        }
        self.message.validate()
    }
}

impl CommandPayload for EditAndResubmitMessage {
    const COMMAND_TYPE: CommandType = CommandType::EditAndResubmitMessage;
}

/// Conversation archive policy is explicit; active Runs are never abandoned.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ArchiveConversation {
    pub active_run_policy: ActiveRunArchivePolicy,
}

impl CommandPayload for ArchiveConversation {
    const COMMAND_TYPE: CommandType = CommandType::ArchiveConversation;
}

/// Origin of the canonical title change recorded in the Conversation Ledger.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ConversationTitleOrigin {
    User,
    FirstUserMessage,
    AcceptedProposal { proposal_record_id: LedgerRecordId },
}

/// Replaces the visible Conversation title without mutating prior facts.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetConversationTitle {
    pub title: DisplayText,
    pub origin: ConversationTitleOrigin,
}

impl CommandPayload for SetConversationTitle {
    const COMMAND_TYPE: CommandType = CommandType::SetConversationTitle;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ConversationCommandError {
    #[error("first Cloud slice supports only Direct conversations")]
    UnsupportedConversationKind,
    #[error("first Cloud slice supports only Cloud control home")]
    UnsupportedControlHome,
    #[error("message contains more than 64 attachments")]
    TooManyAttachments,
    #[error("edit-and-resubmit identities must be distinct")]
    InvalidEditAndResubmit,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct GetConversation {
    pub actor: Actor,
    pub conversation_id: ConversationId,
}

/// Stable keyset cursor for the tenant-owned Conversation catalog.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ConversationListCursor {
    pub updated_at: UtcTimestamp,
    pub conversation_id: ConversationId,
}

/// Runtime-owned catalog query. Product adapters may filter by workspace, but
/// they may not maintain a second Conversation membership index.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ListConversations {
    pub actor: Actor,
    pub conversation_id: Option<ConversationId>,
    pub workspace_id: Option<WorkspaceId>,
    pub lifecycle: Option<ConversationLifecycle>,
    pub page_size: u16,
    pub after: Option<ConversationListCursor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationSummary {
    pub conversation_id: ConversationId,
    pub tenant_id: TenantId,
    pub workspace_id: Option<WorkspaceId>,
    pub kind: ConversationKind,
    pub title: Option<DisplayText>,
    pub lifecycle: ConversationLifecycle,
    pub latest_cursor: LedgerCursor,
    pub has_active_run: bool,
    pub operational: ConversationOperationalSummary,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

/// Runtime-owned operational facts for a Conversation catalog row.
///
/// This is deliberately orthogonal to the visible Conversation lifecycle:
/// one Conversation can remain active while its Runs are executing, waiting
/// for a person, waiting for infrastructure, or completely idle. Product
/// clients must not infer those distinct states from `has_active_run`.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ConversationOperationalSummary {
    pub active_run_count: u32,
    pub queued_run_count: u32,
    pub running_run_count: u32,
    pub waiting_run_count: u32,
    pub cancelling_run_count: u32,
    pub wait_reasons: Vec<ConversationRunWaitReason>,
    pub pending_interaction_count: u32,
    pub pending_interaction_kinds: Vec<ConversationPendingInteractionKind>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ConversationRunWaitReason {
    Approval,
    Input,
    Device,
    RetryBackoff,
    Delegation,
    ExternalOperation,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ConversationPendingInteractionKind {
    Approval,
    Clarification,
    Confirmation,
    Selection,
    Credential,
    OAuth,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationPage {
    pub conversations: Vec<ConversationSummary>,
    pub next_cursor: Option<ConversationListCursor>,
}

/// Tenant/owner-authorized subscription to authoritative Conversation catalog
/// changes.
///
/// The Runtime remains the only catalog authority. Subscribers receive the
/// same `ConversationSummary` projection returned by `list_conversations`, so
/// they never need to turn a high-frequency Ledger hint into another catalog
/// query.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct SubscribeConversationCatalog {
    pub actor: Actor,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ConversationCatalogChange {
    /// The authoritative catalog row was created or its catalog-visible state
    /// changed. `latest_cursor` is current at publication time, but cursor-only
    /// Ledger movement is deliberately not a catalog change.
    Upsert(ConversationSummary),
    /// A row previously authorized and published on this subscription is no
    /// longer visible to the subscriber.
    Remove(ConversationId),
    /// The lossy change feed reported a gap. Consumers must perform one
    /// authoritative full-list convergence before applying later deltas.
    ResyncRequired,
}

pub trait ConversationCatalogApplication: Send + Sync {
    fn subscribe_conversation_catalog(
        &self,
        query: SubscribeConversationCatalog,
    ) -> ApplicationFuture<
        '_,
        Result<ApplicationStream<Result<ConversationCatalogChange, RuntimeError>>, RuntimeError>,
    >;
}

/// Canonical reason a Context branch exists. Product adapters must not infer
/// edit semantics from record ordering or identifiers.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ContextBranchOrigin {
    /// A branch event written before origin metadata became part of the
    /// canonical payload. This value is accepted only while replaying legacy
    /// Ledger records; command handlers and query projections must never emit
    /// or persist it for new branches.
    LegacyUnclassified,
    Fork,
    EditPreserved,
    EditReplacement,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextBranchView {
    pub branch_id: ContextBranchId,
    pub parent_branch_id: Option<ContextBranchId>,
    pub fork_from_record_id: LedgerRecordId,
    pub fork_from_sequence: ConversationSeq,
    pub branch_summary_ref: Option<ContentRef>,
    pub origin: ContextBranchOrigin,
    pub edit_group_id: Option<CorrelationId>,
    pub edit_target_message_id: Option<MessageId>,
    pub created_by: PrincipalId,
    pub created_sequence: ConversationSeq,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationView {
    pub conversation: Conversation,
    pub participants: Vec<Participant>,
    pub latest_cursor: LedgerCursor,
    pub active_activity_ids: Vec<ActivityId>,
    pub active_run_ids: Vec<RunId>,
    pub active_context_branch_id: Option<ContextBranchId>,
    pub context_branches: Vec<ContextBranchView>,
}

/// Consumer-owned use-case Port. Adapters implement it without exposing rows or wire DTOs.
pub trait ConversationApplication: Send + Sync {
    fn create_conversation(
        &self,
        command: CommandEnvelope<CreateConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    /// Recovers the immutable receipt of a historical Device creation.
    ///
    /// Unlike `create_conversation`, this use case never appends or compares a
    /// contemporary command fingerprint. Persistence must prove the canonical
    /// Conversation identity and its exact first-append idempotency envelope.
    fn recover_conversation_creation(
        &self,
        command: CommandEnvelope<CreateConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn submit_message(
        &self,
        command: CommandEnvelope<SubmitMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn retract_message(
        &self,
        command: CommandEnvelope<RetractMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn fork_context(
        &self,
        command: CommandEnvelope<ForkConversationContext>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn activate_context_branch(
        &self,
        command: CommandEnvelope<ActivateConversationContextBranch>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn edit_and_resubmit_message(
        &self,
        command: CommandEnvelope<EditAndResubmitMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn archive_conversation(
        &self,
        command: CommandEnvelope<ArchiveConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn set_conversation_title(
        &self,
        command: CommandEnvelope<SetConversationTitle>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn get_conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>>;

    fn list_conversations(
        &self,
        query: ListConversations,
    ) -> ApplicationFuture<'_, Result<ConversationPage, RuntimeError>>;
}

/// A stable visible speaker type.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ParticipantKind {
    User,
    Agent,
    Service,
}

/// Stable service identity used only by service Participants.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ServiceRef(DottedKey);

impl ServiceRef {
    #[must_use]
    pub const fn from_key(value: DottedKey) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Historical display data. It is not an authorization identity.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParticipantDisplaySnapshot {
    pub display_name: DisplayText,
    pub avatar_ref: Option<ContentRef>,
}

/// Exactly one stable identity reference selected by `ParticipantKind`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParticipantIdentity {
    pub principal_ref: Option<PrincipalId>,
    pub agent_ref: Option<AgentId>,
    pub service_ref: Option<ServiceRef>,
}

/// Stable identity referenced by every visible event.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Participant {
    pub participant_id: ParticipantId,
    pub conversation_id: ConversationId,
    pub kind: ParticipantKind,
    pub principal_ref: Option<PrincipalId>,
    pub agent_ref: Option<AgentId>,
    pub service_ref: Option<ServiceRef>,
    pub display_snapshot: ParticipantDisplaySnapshot,
    pub joined_at: UtcTimestamp,
    pub left_at: Option<UtcTimestamp>,
}

/// A Participant reference did not match its kind.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("participant identity reference does not match kind")]
pub struct ParticipantIdentityError;

impl Participant {
    /// Creates and validates a Participant.
    pub fn new(
        participant_id: ParticipantId,
        conversation_id: ConversationId,
        kind: ParticipantKind,
        identity: ParticipantIdentity,
        display_snapshot: ParticipantDisplaySnapshot,
        joined_at: UtcTimestamp,
    ) -> Result<Self, ParticipantIdentityError> {
        let valid = match kind {
            ParticipantKind::User => {
                identity.principal_ref.is_some()
                    && identity.agent_ref.is_none()
                    && identity.service_ref.is_none()
            }
            ParticipantKind::Agent => {
                identity.principal_ref.is_none()
                    && identity.agent_ref.is_some()
                    && identity.service_ref.is_none()
            }
            ParticipantKind::Service => {
                identity.principal_ref.is_none()
                    && identity.agent_ref.is_none()
                    && identity.service_ref.is_some()
            }
        };
        if !valid {
            return Err(ParticipantIdentityError);
        }
        Ok(Self {
            participant_id,
            conversation_id,
            kind,
            principal_ref: identity.principal_ref,
            agent_ref: identity.agent_ref,
            service_ref: identity.service_ref,
            display_snapshot,
            joined_at,
            left_at: None,
        })
    }
}

/// Visible message role; speaker identity remains `participant_id`.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum MessageRole {
    User,
    Assistant,
    SystemNotice,
}

/// Projection status for a visible Message.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum MessageStatus {
    Committed,
    Partial,
    Retracted,
}

/// Projection identity and provenance, independent of content storage.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MessageProjection {
    pub message_id: MessageId,
    pub conversation_id: ConversationId,
    pub participant_id: ParticipantId,
    pub role: MessageRole,
    pub source_run_id: Option<RunId>,
    pub status: MessageStatus,
    pub content: ContentRef,
    pub first_record_seq: ConversationSeq,
    pub committed_at_seq: ConversationSeq,
    pub created_at: UtcTimestamp,
}

/// Fully resolved user input staged for one atomic admission transaction.
///
/// Content bytes must already be durable in the Content Store. The transaction
/// only authorizes the immutable reference together with Message/Activity/Run
/// state. Storage locators never enter a domain object or admission command.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionMessage {
    pub project_task_target: Option<ProjectTaskTarget>,
    pub message_id: MessageId,
    pub participant_id: ParticipantId,
    pub role: MessageRole,
    pub source_run_id: RunId,
    pub content: ContentRef,
}
