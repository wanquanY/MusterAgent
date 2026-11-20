//! Original Conversation mutation authority and commit contracts.
use super::*;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ArchiveRunTarget {
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub last_run_sequence: RunSeq,
    pub last_attempt_id: Option<AttemptId>,
    pub mode: CancelMode,
    pub pending_interaction_id: Option<InteractionId>,
    pub pending_tool_invocation_id: Option<InvocationId>,
    /// Unknown side effects remain available for reconciliation after archive.
    pub unresolved_tool_invocation_id: Option<InvocationId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArchiveConversationAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub lifecycle: ConversationLifecycle,
    pub requested_by_participant_id: ParticipantId,
    pub active_runs: Vec<ArchiveRunTarget>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArchiveConversationCommit {
    pub actor: Actor,
    pub archived_at: UtcTimestamp,
    pub targets: Vec<ArchiveRunTarget>,
    pub append: AppendRequest,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetConversationTitleAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub lifecycle: ConversationLifecycle,
    pub requested_by_participant_id: ParticipantId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetConversationTitleCommit {
    pub actor: Actor,
    pub title: musteragent_runtime_contract::DisplayText,
    pub updated_at: UtcTimestamp,
    pub append: AppendRequest,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RetractMessageTarget {
    pub message_id: MessageId,
    pub source_run_id: Option<RunId>,
    pub persisted_message: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RetractMessageAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub requested_by_participant_id: ParticipantId,
    pub targets: Vec<RetractMessageTarget>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RetractMessageCommit {
    pub managed_resources: Option<crate::file_retraction::ManagedResourceRetractionPlan>,
    pub actor: Actor,
    pub retracted_at: UtcTimestamp,
    pub targets: Vec<RetractMessageTarget>,
    pub append: AppendRequest,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextBranchAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub requested_by_participant_id: ParticipantId,
    pub active_branch_id: Option<ContextBranchId>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ForkContextAuthority {
    pub context: ContextBranchAuthority,
    pub fork_from_sequence: ConversationSeq,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ForkContextCommit {
    pub actor: Actor,
    pub branch_id: ContextBranchId,
    pub parent_branch_id: Option<ContextBranchId>,
    pub fork_from_record_id: LedgerRecordId,
    pub fork_from_sequence: ConversationSeq,
    pub branch_summary_ref: Option<musteragent_content_ref::ContentRef>,
    pub created_at: UtcTimestamp,
    pub append: AppendRequest,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ActivateContextBranchCommit {
    pub actor: Actor,
    pub branch_id: ContextBranchId,
    pub activated_at: UtcTimestamp,
    pub append: AppendRequest,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EditAndResubmitAuthority {
    pub admission: AdmissionAuthority,
    /// Prior discovery reconstructed at the replacement branch fork point,
    /// not at the current active branch head.
    pub replacement_prior_discovery: Option<PriorRunDiscovery>,
    pub active_branch_id: Option<ContextBranchId>,
    pub preserved_fork_record_id: LedgerRecordId,
    pub preserved_fork_sequence: ConversationSeq,
    pub replacement_fork_record_id: LedgerRecordId,
    pub replacement_fork_sequence: ConversationSeq,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EditAndResubmitAuthorizationRequest {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub participant_id: ParticipantId,
    pub target_message_id: MessageId,
    pub expected_home_epoch: HomeEpoch,
    pub expected_control_home: RuntimeHome,
    pub correlation_id: CorrelationId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EditAndResubmitCommit {
    pub actor: Actor,
    pub edit_group_id: CorrelationId,
    pub target_message_id: MessageId,
    pub preserved_branch_id: ContextBranchId,
    pub replacement_branch_id: ContextBranchId,
    pub parent_branch_id: Option<ContextBranchId>,
    pub preserved_fork_record_id: LedgerRecordId,
    pub preserved_fork_sequence: ConversationSeq,
    pub replacement_fork_record_id: LedgerRecordId,
    pub replacement_fork_sequence: ConversationSeq,
    pub created_at: UtcTimestamp,
    pub admission: AdmissionBatch,
    pub append: AppendRequest,
    /// Admission surface of the replacement Run. API admissions always route
    /// as the API surface, exactly as on submit.
    pub surface: musteragent_scheduler::AdmissionSurface,
    /// Trusted host routing context persisted with the replacement Run in the
    /// edit transaction. Validated exactly like a submit context.
    pub sandbox_routing: Option<musteragent_sandbox::SandboxRoutingContext>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EditAndResubmitAdmissionCommit {
    pub edit: EditAndResubmitCommit,
    pub content_stage: AdmissionContentStage,
}
