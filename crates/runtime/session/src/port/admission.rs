//! Original admission facts and commit/replay payloads. Storage behavior stays
//! on RuntimeApplicationStore; these types do not authorize a second writer.
use musteragent_content_ref::Sha256;
use musteragent_definition::{AgentDefinition, AgentVersion};
use musteragent_discovery::PriorToolExposure;
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{
    Actor, ContextBranchId, ConversationId, CorrelationId, GoalId, GoalPursuitId, HomeEpoch,
    IdempotencyKey, InterSessionMessageId, LedgerOffset, ParticipantId, RunId, TenantId,
    UtcTimestamp,
};
use musteragent_ledger::{
    AdmissionBatch, AppendReceipt, CoordinationAdmissionBatch, StandingGoalAdmissionBatch,
};
use musteragent_runtime_content::AdmissionContentStage;
use musteragent_runtime_contract::{ErrorCode, OpaqueDiagnosticRef, TraceContext};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentAdmissionIdentity {
    pub definition: AgentDefinition,
    pub version: AgentVersion,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionAuthority {
    pub tenant_id: musteragent_identity::TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    /// Canonical title observed under the same optimistic authority read as
    /// `current_offset`. `None` means the Conversation is still untitled and
    /// may atomically adopt its first user-message title during admission.
    pub title: Option<musteragent_runtime_contract::DisplayText>,
    /// Prevents a delayed or imported later turn from being mistaken for the
    /// first user message when an older untitled Conversation is resumed.
    pub has_user_messages: bool,
    /// Branch identity is read with the optimistic admission offset. A
    /// concurrent branch activation advances the stream and invalidates the
    /// eventual admission commit.
    pub active_context_branch_id: Option<ContextBranchId>,
    /// Latest terminal prior Run reachable from the active branch head,
    /// reconstructed from its immutable Snapshot prefix plus durable discovery
    /// exposure facts.
    pub prior_discovery: Option<PriorRunDiscovery>,
    pub requested_by_participant_id: ParticipantId,
    pub executor_participant_id: ParticipantId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionAuthorizationRequest {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub participant_id: ParticipantId,
    /// Activity class being authorized. Persistence uses it to enforce the
    /// Conversation execution-priority contract under the same optimistic
    /// authority read as the Ledger offset.
    pub activity_kind: musteragent_activity::ActivityKind,
    pub expected_home_epoch: HomeEpoch,
    pub expected_control_home: RuntimeHome,
    pub correlation_id: CorrelationId,
    pub audit_context: AdmissionAuditContext,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionAuditContext {
    pub command_id: musteragent_identity::CommandId,
    pub trace_context: musteragent_runtime_contract::TraceContext,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionDenialQuery {
    pub tenant_id: TenantId,
    pub command_id: musteragent_identity::CommandId,
    pub conversation_id: ConversationId,
    pub command_fingerprint: Sha256,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionDenialCommit {
    pub actor: Actor,
    pub command_id: musteragent_identity::CommandId,
    pub conversation_id: ConversationId,
    pub correlation_id: CorrelationId,
    pub trace_context: TraceContext,
    pub command_fingerprint: Sha256,
    pub error_code: ErrorCode,
    pub retryable: bool,
    pub diagnostic_ref: Option<OpaqueDiagnosticRef>,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionDenialReceipt {
    pub error_code: ErrorCode,
    pub retryable: bool,
    pub diagnostic_ref: Option<OpaqueDiagnosticRef>,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PriorRunDiscovery {
    pub source_run_id: RunId,
    pub tool_exposures: Vec<PriorToolExposure>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RunDiscoveryQuery {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub source_run_id: RunId,
    pub lineage_head_branch_id: Option<ContextBranchId>,
    pub through_offset: LedgerOffset,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandReplayQuery {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub idempotency_key: IdempotencyKey,
    pub command_fingerprint: Sha256,
}

/// Compaction-proof proof request for the original Device Conversation
/// creation append. Mutable presentation fields such as title are excluded:
/// ownership is established by canonical aggregate identity plus the three
/// immutable Ledger envelopes at sequences 1..=3.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationCreationRecoveryQuery {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub workspace_id: Option<musteragent_identity::WorkspaceId>,
    pub control_home: musteragent_event_envelope::RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AdmissionReplayReceipt {
    pub ledger: AppendReceipt,
    /// Original Snapshot/stage preparation time recovered from durable state.
    pub prepared_at: UtcTimestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionCommit {
    pub batch: AdmissionBatch,
    pub content_stage: AdmissionContentStage,
}

/// Target-side durable commit for one cross-Conversation queued turn.
#[derive(Debug, Clone, PartialEq)]
pub struct CoordinationAdmissionCommit {
    pub authority: AdmissionAuthority,
    pub command_id: musteragent_identity::CommandId,
    pub mailbox_message_id: InterSessionMessageId,
    pub batch: CoordinationAdmissionBatch,
    pub content_stage: AdmissionContentStage,
}

impl CoordinationAdmissionCommit {
    /// Both homes validate the same prepared authority before entering their
    /// original Ledger/Content transaction. This does not qualify the sender
    /// or grant receiver permissions; those belong to the admission source.
    pub fn validate(&self) -> Result<(), ErrorCode> {
        if self.command_id != self.content_stage.command_id
            || self.mailbox_message_id != self.batch.mailbox_message_id
            || self.authority.tenant_id != self.batch.append.tenant_id
            || self.authority.conversation_id != self.batch.append.conversation_id
            || self.authority.home_epoch != self.batch.append.expected_home_epoch
            || self.authority.current_offset != self.batch.append.expected_offset
            || self.authority.requested_by_participant_id
                != self.batch.activity.requested_by_participant_id
            || self.authority.executor_participant_id
                != self.batch.run.context.executor_participant_id
            || self.batch.append.tenant_id != self.content_stage.tenant_id
            || self.batch.append.command_fingerprint != self.content_stage.command_fingerprint
            || self.batch.payload_ref != self.content_stage.content
            || self.content_stage.slot
                != musteragent_runtime_content::AdmissionContentSlot::CoordinationContext
        {
            return Err(ErrorCode::ContentIntegrityFailed);
        }
        self.batch
            .validate()
            .map_err(|_| ErrorCode::ContentIntegrityFailed)
    }
}

/// Durable commit for one message-free standing Goal continuation Run.
#[derive(Debug, Clone, PartialEq)]
pub struct StandingGoalAdmissionCommit {
    pub authority: AdmissionAuthority,
    pub command_id: musteragent_identity::CommandId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub batch: StandingGoalAdmissionBatch,
}
