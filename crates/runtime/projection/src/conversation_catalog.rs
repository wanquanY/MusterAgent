//! Authoritative, storage-neutral Conversation catalog projection semantics.
//!
//! This module owns the cross-domain read model used by both Device and Cloud
//! homes. It deliberately does not own persistence, transport DTOs, UI labels,
//! or client freshness. SQLite and PostgreSQL adapters must apply the same
//! reducer and preserve its authority/home and idempotency invariants.

mod adapters;

pub use adapters::{
    CatalogAdapterError, CatalogEventContext, CatalogRunProjectionContext, adapt_activity_event,
    adapt_artifact_event, adapt_conversation_event, adapt_goal_evaluation, adapt_goal_pursuit,
    adapt_interaction_event, adapt_run_event, adapt_standing_goal,
};

use std::collections::BTreeMap;

use musteragent_conversation::{ConversationKind, ConversationLifecycle};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{
    ArtifactId, ArtifactProposalId, ArtifactVersionId, ConversationId, DelegationId, DottedKey,
    GoalEvaluationId, GoalId, GoalPursuitId, HomeEpoch, InterSessionMessageId, InteractionId,
    LedgerRecordId, MessageId, ParticipantId, PrincipalId, RunId, TenantId, UtcTimestamp,
    WorkspaceId,
};
use thiserror::Error;

/// Authority commit position used for replay and deterministic activity order.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CatalogActivityPosition(u64);

impl CatalogActivityPosition {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

macro_rules! closed_storage_code {
    ($type:ty, {$($variant:path => $code:literal),+ $(,)?}) => {
        impl $type {
            #[doc(hidden)]
            #[must_use]
            pub const fn storage_code(self) -> i16 {
                match self { $($variant => $code),+ }
            }

            #[doc(hidden)]
            #[must_use]
            pub const fn from_storage_code(code: i16) -> Option<Self> {
                match code { $($code => Some($variant)),+, _ => None }
            }
        }
    };
}

/// Conversation-local revision of user-visible attention events.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AttentionRevision(u64);

impl AttentionRevision {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> Result<Self, CatalogProjectionError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(CatalogProjectionError::AttentionRevisionOverflow)
    }
}

/// Closed classification namespace for idempotent source keys.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogSourceKind {
    ConversationCreated,
    UserMessageCommitted,
    ExecutionAdmitted,
    ExecutionStateChanged,
    VisibleProgress,
    VisibleOutput,
    ExecutionOutcome,
    ChildReport,
    GoalEvaluation,
    GoalDecision,
    ActionOpened,
    ActionClosed,
    ActionReplaced,
    InboundMessage,
    ArtifactOutcome,
    SystemNotice,
    LifecycleChanged,
    StandingGoalStateChanged,
    GoalPursuitStateChanged,
    MetadataChanged,
    InternalMaintenance,
}

closed_storage_code!(CatalogSourceKind, {
    CatalogSourceKind::ConversationCreated => 1,
    CatalogSourceKind::UserMessageCommitted => 2,
    CatalogSourceKind::ExecutionAdmitted => 3,
    CatalogSourceKind::ExecutionStateChanged => 4,
    CatalogSourceKind::VisibleProgress => 5,
    CatalogSourceKind::VisibleOutput => 6,
    CatalogSourceKind::ExecutionOutcome => 7,
    CatalogSourceKind::ChildReport => 8,
    CatalogSourceKind::GoalEvaluation => 9,
    CatalogSourceKind::GoalDecision => 10,
    CatalogSourceKind::ActionOpened => 11,
    CatalogSourceKind::ActionClosed => 12,
    CatalogSourceKind::ActionReplaced => 13,
    CatalogSourceKind::InboundMessage => 14,
    CatalogSourceKind::ArtifactOutcome => 15,
    CatalogSourceKind::SystemNotice => 16,
    CatalogSourceKind::LifecycleChanged => 17,
    CatalogSourceKind::StandingGoalStateChanged => 18,
    CatalogSourceKind::GoalPursuitStateChanged => 19,
    CatalogSourceKind::MetadataChanged => 20,
    CatalogSourceKind::InternalMaintenance => 21,
});

/// Stable business identity behind one source key. Ledger record identity is a
/// last resort for notices that have no stronger domain identity.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogSourceIdentity {
    Conversation(ConversationId),
    Message(MessageId),
    Run(RunId),
    Interaction(InteractionId),
    Delegation(DelegationId),
    Goal(GoalId),
    GoalPursuit(GoalPursuitId),
    GoalEvaluation(GoalEvaluationId),
    Artifact(ArtifactId),
    ArtifactProposal(ArtifactProposalId),
    ArtifactVersion(ArtifactVersionId),
    InterSessionMessage(InterSessionMessageId),
    LedgerRecord(LedgerRecordId),
}

impl CatalogSourceIdentity {
    #[doc(hidden)]
    #[must_use]
    pub const fn storage_kind_code(self) -> i16 {
        match self {
            Self::Conversation(_) => 1,
            Self::Message(_) => 2,
            Self::Run(_) => 3,
            Self::Interaction(_) => 4,
            Self::Delegation(_) => 5,
            Self::Goal(_) => 6,
            Self::GoalPursuit(_) => 7,
            Self::GoalEvaluation(_) => 8,
            Self::Artifact(_) => 9,
            Self::ArtifactProposal(_) => 10,
            Self::ArtifactVersion(_) => 11,
            Self::InterSessionMessage(_) => 12,
            Self::LedgerRecord(_) => 14,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn storage_identity(self) -> String {
        match self {
            Self::Conversation(value) => value.to_string(),
            Self::Message(value) => value.to_string(),
            Self::Run(value) => value.to_string(),
            Self::Interaction(value) => value.to_string(),
            Self::Delegation(value) => value.to_string(),
            Self::Goal(value) => value.to_string(),
            Self::GoalPursuit(value) => value.to_string(),
            Self::GoalEvaluation(value) => value.to_string(),
            Self::Artifact(value) => value.to_string(),
            Self::ArtifactProposal(value) => value.to_string(),
            Self::ArtifactVersion(value) => value.to_string(),
            Self::InterSessionMessage(value) => value.to_string(),
            Self::LedgerRecord(value) => value.to_string(),
        }
    }
}

/// Idempotency key for one semantically visible fact revision.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CatalogSourceKey {
    pub kind: CatalogSourceKind,
    pub identity: CatalogSourceIdentity,
    pub semantic_revision: u64,
}

/// Result of claiming a source key in the authority store's unique-key table.
/// The claim and projection write must share one database transaction.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CatalogSourceClaim {
    New,
    Duplicate,
}

impl CatalogSourceKey {
    pub fn new(
        kind: CatalogSourceKind,
        identity: CatalogSourceIdentity,
        semantic_revision: u64,
    ) -> Result<Self, CatalogProjectionError> {
        if semantic_revision == 0 {
            return Err(CatalogProjectionError::InvalidSemanticRevision);
        }
        Ok(Self {
            kind,
            identity,
            semantic_revision,
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogRunWaitReason {
    Approval,
    UserInput,
    Confirmation,
    Selection,
    Credential,
    OAuth,
    Budget,
    Device,
    Resource,
    Dependency,
    ExternalOperation,
    RetryBackoff,
    Delegation,
    DelegationGroup,
    DelegationCompletions,
}

closed_storage_code!(CatalogRunWaitReason, {
    CatalogRunWaitReason::Approval => 1,
    CatalogRunWaitReason::UserInput => 2,
    CatalogRunWaitReason::Confirmation => 3,
    CatalogRunWaitReason::Selection => 4,
    CatalogRunWaitReason::Credential => 5,
    CatalogRunWaitReason::OAuth => 6,
    CatalogRunWaitReason::Budget => 7,
    CatalogRunWaitReason::Device => 8,
    CatalogRunWaitReason::Resource => 9,
    CatalogRunWaitReason::Dependency => 10,
    CatalogRunWaitReason::ExternalOperation => 11,
    CatalogRunWaitReason::RetryBackoff => 12,
    CatalogRunWaitReason::Delegation => 13,
    CatalogRunWaitReason::DelegationGroup => 14,
    CatalogRunWaitReason::DelegationCompletions => 15,
});

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ExecutionOutcomeKind {
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

closed_storage_code!(ExecutionOutcomeKind, {
    ExecutionOutcomeKind::Completed => 1,
    ExecutionOutcomeKind::Failed => 2,
    ExecutionOutcomeKind::Cancelled => 3,
    ExecutionOutcomeKind::Interrupted => 4,
});

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogRecoverability {
    Retryable,
    RequiresUserAction,
    NotRetryable,
    Unknown,
}

closed_storage_code!(CatalogRecoverability, {
    CatalogRecoverability::Retryable => 1,
    CatalogRecoverability::RequiresUserAction => 2,
    CatalogRecoverability::NotRetryable => 3,
    CatalogRecoverability::Unknown => 4,
});

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionOutcome {
    pub kind: ExecutionOutcomeKind,
    /// Bounded, non-secret class such as `provider.rate_limited`; never raw
    /// error text, credential material, or a stack trace.
    pub error_class: Option<DottedKey>,
    pub recoverability: Option<CatalogRecoverability>,
}

impl ExecutionOutcome {
    #[must_use]
    pub fn completed() -> Self {
        Self {
            kind: ExecutionOutcomeKind::Completed,
            error_class: None,
            recoverability: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CatalogExecutionStatus {
    Queued,
    Running,
    Waiting(CatalogRunWaitReason),
    Cancelling,
    Completed,
    Failed {
        error_class: Option<DottedKey>,
        recoverability: CatalogRecoverability,
    },
    Cancelled,
    Interrupted {
        error_class: Option<DottedKey>,
        recoverability: CatalogRecoverability,
    },
}

impl CatalogExecutionStatus {
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Running | Self::Waiting(_) | Self::Cancelling
        )
    }
}

/// Canonical execution identity. Child detection never depends on display name
/// or Conversation kind.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogExecutionIdentity {
    Root {
        run_id: RunId,
    },
    Child {
        run_id: RunId,
        parent_run_id: RunId,
        delegation_id: DelegationId,
    },
    Pursuit {
        pursuit_id: GoalPursuitId,
        run_id: Option<RunId>,
    },
}

impl CatalogExecutionIdentity {
    /// Ownership may change while a Run is executing (for example goal.create).
    /// The Run identity, unlike Root/Child/Pursuit classification, is immutable.
    fn run_id(self) -> Option<RunId> {
        match self {
            Self::Root { run_id } | Self::Child { run_id, .. } => Some(run_id),
            Self::Pursuit { run_id, .. } => run_id,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogActionKind {
    Approval,
    Confirmation,
    UserInput,
    Selection,
    Credential,
    OAuth,
    Budget,
    Resource,
    Dependency,
}

closed_storage_code!(CatalogActionKind, {
    CatalogActionKind::Approval => 1,
    CatalogActionKind::Confirmation => 2,
    CatalogActionKind::UserInput => 3,
    CatalogActionKind::Selection => 4,
    CatalogActionKind::Credential => 5,
    CatalogActionKind::OAuth => 6,
    CatalogActionKind::Budget => 7,
    CatalogActionKind::Resource => 8,
    CatalogActionKind::Dependency => 9,
});

impl CatalogActionKind {
    #[must_use]
    pub const fn display_priority(self) -> u8 {
        match self {
            Self::Credential | Self::OAuth => 0,
            Self::Approval => 1,
            Self::Confirmation => 2,
            Self::UserInput | Self::Selection => 3,
            Self::Budget => 4,
            Self::Resource | Self::Dependency => 5,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogActionIdentity {
    Interaction(InteractionId),
    Run(RunId),
    GoalPursuit(GoalPursuitId),
    Artifact(ArtifactId),
    ArtifactProposal(ArtifactProposalId),
}

impl CatalogActionIdentity {
    #[doc(hidden)]
    #[must_use]
    pub const fn storage_kind_code(self) -> i16 {
        match self {
            Self::Interaction(_) => 1,
            Self::Run(_) => 2,
            Self::GoalPursuit(_) => 3,
            Self::Artifact(_) => 4,
            Self::ArtifactProposal(_) => 5,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn storage_identity(self) -> String {
        match self {
            Self::Interaction(value) => value.to_string(),
            Self::Run(value) => value.to_string(),
            Self::GoalPursuit(value) => value.to_string(),
            Self::Artifact(value) => value.to_string(),
            Self::ArtifactProposal(value) => value.to_string(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogStandingGoalStatus {
    Draft,
    Active,
    Paused,
    Achieved,
    Failed,
    Cancelled,
}

closed_storage_code!(CatalogStandingGoalStatus, {
    CatalogStandingGoalStatus::Draft => 1,
    CatalogStandingGoalStatus::Active => 2,
    CatalogStandingGoalStatus::Paused => 3,
    CatalogStandingGoalStatus::Achieved => 4,
    CatalogStandingGoalStatus::Failed => 5,
    CatalogStandingGoalStatus::Cancelled => 6,
});

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogGoalPursuitStatus {
    Pending,
    Running,
    Waiting(CatalogRunWaitReason),
    Paused,
    Achieved,
    Failed,
    Cancelled,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogAttentionAudience {
    AllParticipants,
    Principal(PrincipalId),
    Participant(ParticipantId),
    AllExceptPrincipal(PrincipalId),
    AllExceptParticipant(ParticipantId),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CatalogAttentionKind {
    ConversationCreated,
    UserMessage,
    Progress,
    Output,
    ExecutionOutcome,
    ChildReport,
    GoalEvaluation,
    GoalDecision,
    ActionRequired,
    InboundMessage,
    ArtifactOutcome,
    SystemNotice,
}

closed_storage_code!(CatalogAttentionKind, {
    CatalogAttentionKind::ConversationCreated => 1,
    CatalogAttentionKind::UserMessage => 2,
    CatalogAttentionKind::Progress => 3,
    CatalogAttentionKind::Output => 4,
    CatalogAttentionKind::ExecutionOutcome => 5,
    CatalogAttentionKind::ChildReport => 6,
    CatalogAttentionKind::GoalEvaluation => 7,
    CatalogAttentionKind::GoalDecision => 8,
    CatalogAttentionKind::ActionRequired => 9,
    CatalogAttentionKind::InboundMessage => 10,
    CatalogAttentionKind::ArtifactOutcome => 11,
    CatalogAttentionKind::SystemNotice => 12,
});

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogAttentionRecord {
    pub revision: AttentionRevision,
    pub position: CatalogActivityPosition,
    pub kind: CatalogAttentionKind,
    pub source_key: CatalogSourceKey,
    pub audience: CatalogAttentionAudience,
    pub outcome: Option<ExecutionOutcome>,
    pub occurred_at: UtcTimestamp,
    pub committed_at: UtcTimestamp,
}

/// Normalized fact kinds accepted by the cross-domain catalog projector.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CatalogFactKind {
    ConversationCreated,
    UserMessageCommitted,
    ExecutionAdmitted {
        identity: CatalogExecutionIdentity,
        initial_status: CatalogExecutionStatus,
    },
    ExecutionStateChanged {
        identity: CatalogExecutionIdentity,
        status: CatalogExecutionStatus,
        visible_outcome: Option<ExecutionOutcome>,
    },
    VisibleProgress,
    VisibleOutput,
    VisibleExecutionOutcome {
        outcome: ExecutionOutcome,
    },
    ChildReport {
        outcome: ExecutionOutcome,
    },
    GoalEvaluation {
        outcome: Option<ExecutionOutcome>,
    },
    GoalDecisionRequired,
    ActionOpened {
        identity: CatalogActionIdentity,
        kind: CatalogActionKind,
    },
    ActionClosed {
        identity: CatalogActionIdentity,
    },
    ActionReplaced {
        replaced: CatalogActionIdentity,
        replacement: CatalogActionIdentity,
        replacement_kind: CatalogActionKind,
    },
    InboundMessage,
    ArtifactOutcome {
        outcome: ExecutionOutcome,
    },
    SystemNotice {
        requires_attention: bool,
    },
    LifecycleChanged(ConversationLifecycle),
    StandingGoalStateChanged(CatalogStandingGoalStatus),
    GoalPursuitStateChanged(Option<CatalogGoalPursuitStatus>),
    WorkspaceAssigned(WorkspaceId),
    MetadataChanged,
    InternalMaintenance,
}

impl CatalogFactKind {
    #[must_use]
    pub const fn source_kind(&self) -> CatalogSourceKind {
        match self {
            Self::ConversationCreated => CatalogSourceKind::ConversationCreated,
            Self::UserMessageCommitted => CatalogSourceKind::UserMessageCommitted,
            Self::ExecutionAdmitted { .. } => CatalogSourceKind::ExecutionAdmitted,
            Self::ExecutionStateChanged { .. } => CatalogSourceKind::ExecutionStateChanged,
            Self::VisibleProgress => CatalogSourceKind::VisibleProgress,
            Self::VisibleOutput => CatalogSourceKind::VisibleOutput,
            Self::VisibleExecutionOutcome { .. } => CatalogSourceKind::ExecutionOutcome,
            Self::ChildReport { .. } => CatalogSourceKind::ChildReport,
            Self::GoalEvaluation { .. } => CatalogSourceKind::GoalEvaluation,
            Self::GoalDecisionRequired => CatalogSourceKind::GoalDecision,
            Self::ActionOpened { .. } => CatalogSourceKind::ActionOpened,
            Self::ActionClosed { .. } => CatalogSourceKind::ActionClosed,
            Self::ActionReplaced { .. } => CatalogSourceKind::ActionReplaced,
            Self::InboundMessage => CatalogSourceKind::InboundMessage,
            Self::ArtifactOutcome { .. } => CatalogSourceKind::ArtifactOutcome,
            Self::SystemNotice { .. } => CatalogSourceKind::SystemNotice,
            Self::LifecycleChanged(_) => CatalogSourceKind::LifecycleChanged,
            Self::StandingGoalStateChanged(_) => CatalogSourceKind::StandingGoalStateChanged,
            Self::GoalPursuitStateChanged(_) => CatalogSourceKind::GoalPursuitStateChanged,
            Self::WorkspaceAssigned(_) | Self::MetadataChanged => {
                CatalogSourceKind::MetadataChanged
            }
            Self::InternalMaintenance => CatalogSourceKind::InternalMaintenance,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogFact {
    pub control_home: RuntimeHome,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub source_key: CatalogSourceKey,
    pub source_claim: CatalogSourceClaim,
    pub audience: CatalogAttentionAudience,
    pub occurred_at: UtcTimestamp,
    pub committed_at: UtcTimestamp,
    pub kind: CatalogFactKind,
}

/// Fact before the authority store atomically claims its source key.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogFactDraft {
    pub control_home: RuntimeHome,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    /// Original Ledger provenance, retained independently of the semantic
    /// deduplication key. Non-Ledger facts never gain shared visibility.
    pub source_record_id: Option<LedgerRecordId>,
    pub source_key: CatalogSourceKey,
    pub audience: CatalogAttentionAudience,
    pub occurred_at: UtcTimestamp,
    pub committed_at: UtcTimestamp,
    pub kind: CatalogFactKind,
}

impl CatalogFactDraft {
    #[must_use]
    pub fn claimed(self, source_claim: CatalogSourceClaim) -> CatalogFact {
        CatalogFact {
            control_home: self.control_home,
            tenant_id: self.tenant_id,
            conversation_id: self.conversation_id,
            home_epoch: self.home_epoch,
            source_key: self.source_key,
            source_claim,
            audience: self.audience,
            occurred_at: self.occurred_at,
            committed_at: self.committed_at,
            kind: self.kind,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub struct CatalogExecutionCounts {
    pub queued: u32,
    pub running: u32,
    pub waiting: u32,
    pub cancelling: u32,
}

impl CatalogExecutionCounts {
    #[must_use]
    pub const fn total(self) -> u32 {
        self.queued + self.running + self.waiting + self.cancelling
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CatalogProjectionApplyDisposition {
    Applied,
    Duplicate,
    Ignored,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum CatalogProjectionError {
    #[error("catalog source semantic revision must be greater than zero")]
    InvalidSemanticRevision,
    #[error("catalog fact belongs to another authority scope")]
    AuthorityScopeMismatch,
    #[error("catalog source kind does not match its normalized fact kind")]
    SourceKindMismatch,
    #[error("catalog commit position did not advance")]
    NonMonotonicCommitPosition,
    #[error("catalog revision overflow")]
    CatalogRevisionOverflow,
    #[error("attention revision overflow")]
    AttentionRevisionOverflow,
    #[error("an archived conversation cannot retain active execution")]
    ArchiveWithActiveExecution,
    #[error("an admitted execution must start in a non-terminal state")]
    TerminalExecutionAdmission,
    #[error("read receipt belongs to another authority scope")]
    ReceiptAuthorityMismatch,
    #[error("read receipt cannot acknowledge beyond the latest visible revision")]
    ReceiptBeyondVisibleRevision,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationReadReceipt {
    pub control_home: RuntimeHome,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub principal_id: PrincipalId,
    pub seen_through: AttentionRevision,
    pub acknowledged_at: UtcTimestamp,
}

impl ConversationReadReceipt {
    pub fn acknowledge(
        &mut self,
        requested: AttentionRevision,
        latest_visible: AttentionRevision,
        acknowledged_at: UtcTimestamp,
    ) -> Result<bool, CatalogProjectionError> {
        if requested > latest_visible {
            return Err(CatalogProjectionError::ReceiptBeyondVisibleRevision);
        }
        if requested <= self.seen_through {
            return Ok(false);
        }
        self.seen_through = requested;
        self.acknowledged_at = acknowledged_at;
        Ok(true)
    }
}

/// Persistent logical state from which the bounded catalog row is derived.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationCatalogProjection {
    pub control_home: RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub workspace_id: Option<WorkspaceId>,
    pub kind: ConversationKind,
    pub lifecycle: ConversationLifecycle,
    pub created_at: UtcTimestamp,
    pub metadata_updated_at: UtcTimestamp,
    pub last_user_visible_activity_at: UtcTimestamp,
    pub last_activity_position: CatalogActivityPosition,
    pub latest_commit_position: CatalogActivityPosition,
    pub catalog_revision: u64,
    pub attention_revision: AttentionRevision,
    pub latest_attention: Option<CatalogAttentionRecord>,
    pub latest_outcome: Option<ExecutionOutcome>,
    pub standing_goal_status: Option<CatalogStandingGoalStatus>,
    pub pursuit_status: Option<CatalogGoalPursuitStatus>,
    active_executions: BTreeMap<CatalogExecutionIdentity, CatalogExecutionStatus>,
    open_actions: BTreeMap<CatalogActionIdentity, CatalogActionKind>,
}

impl ConversationCatalogProjection {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        control_home: RuntimeHome,
        home_epoch: HomeEpoch,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        workspace_id: Option<WorkspaceId>,
        kind: ConversationKind,
        lifecycle: ConversationLifecycle,
        created_at: UtcTimestamp,
    ) -> Self {
        Self {
            control_home,
            home_epoch,
            tenant_id,
            conversation_id,
            workspace_id,
            kind,
            lifecycle,
            created_at,
            metadata_updated_at: created_at,
            last_user_visible_activity_at: created_at,
            last_activity_position: CatalogActivityPosition::ZERO,
            latest_commit_position: CatalogActivityPosition::ZERO,
            catalog_revision: 0,
            attention_revision: AttentionRevision::ZERO,
            latest_attention: None,
            latest_outcome: None,
            standing_goal_status: None,
            pursuit_status: None,
            active_executions: BTreeMap::new(),
            open_actions: BTreeMap::new(),
        }
    }

    pub fn apply(
        &mut self,
        fact: CatalogFact,
        position: CatalogActivityPosition,
    ) -> Result<CatalogProjectionApplyDisposition, CatalogProjectionError> {
        self.validate_scope(&fact)?;
        if fact.source_key.kind != fact.kind.source_kind() {
            return Err(CatalogProjectionError::SourceKindMismatch);
        }
        if fact.source_claim == CatalogSourceClaim::Duplicate {
            return Ok(CatalogProjectionApplyDisposition::Duplicate);
        }
        if position <= self.latest_commit_position {
            return Err(CatalogProjectionError::NonMonotonicCommitPosition);
        }

        // Keep reducer application atomic even when a future bounded counter
        // overflows or an invariant rejects the fact. Persistence adapters
        // provide the corresponding database transaction boundary.
        let mut next = self.clone();
        let disposition = next.apply_new_fact(fact, position)?;
        *self = next;
        Ok(disposition)
    }

    fn apply_new_fact(
        &mut self,
        fact: CatalogFact,
        position: CatalogActivityPosition,
    ) -> Result<CatalogProjectionApplyDisposition, CatalogProjectionError> {
        let classification = classify_fact(&fact.kind);
        let state_changed = self.apply_state_change(&fact.kind, fact.committed_at)?;
        let catalog_visible = state_changed || classification.advance_activity;
        self.latest_commit_position = position;

        if !catalog_visible {
            return Ok(CatalogProjectionApplyDisposition::Ignored);
        }
        self.catalog_revision = self
            .catalog_revision
            .checked_add(1)
            .ok_or(CatalogProjectionError::CatalogRevisionOverflow)?;

        if classification.advance_activity {
            self.last_user_visible_activity_at =
                self.last_user_visible_activity_at.max(fact.committed_at);
            self.last_activity_position = position;
        }
        if let Some((attention_kind, outcome)) = classification.attention {
            let revision = self.attention_revision.next()?;
            self.attention_revision = revision;
            if let Some(value) = outcome.clone() {
                self.latest_outcome = Some(value);
            }
            self.latest_attention = Some(CatalogAttentionRecord {
                revision,
                position,
                kind: attention_kind,
                source_key: fact.source_key,
                audience: fact.audience,
                outcome,
                occurred_at: fact.occurred_at,
                committed_at: fact.committed_at,
            });
        }
        Ok(CatalogProjectionApplyDisposition::Applied)
    }

    #[must_use]
    pub fn root_execution_counts(&self) -> CatalogExecutionCounts {
        self.execution_counts(|identity| matches!(identity, CatalogExecutionIdentity::Root { .. }))
    }

    #[must_use]
    pub fn child_execution_counts(&self) -> CatalogExecutionCounts {
        self.execution_counts(|identity| matches!(identity, CatalogExecutionIdentity::Child { .. }))
    }

    #[must_use]
    pub fn pursuit_execution_counts(&self) -> CatalogExecutionCounts {
        self.execution_counts(|identity| {
            matches!(identity, CatalogExecutionIdentity::Pursuit { .. })
        })
    }

    #[must_use]
    pub fn has_active_execution(&self) -> bool {
        !self.active_executions.is_empty()
    }

    #[must_use]
    pub fn open_action_count(&self) -> u32 {
        u32::try_from(self.open_actions.len()).unwrap_or(u32::MAX)
    }

    #[must_use]
    pub fn highest_priority_action(&self) -> Option<CatalogActionKind> {
        self.open_actions
            .values()
            .copied()
            .min_by_key(|kind| kind.display_priority())
    }

    /// Bounded, currently active execution rows for durable projection stores.
    pub fn active_executions(
        &self,
    ) -> impl ExactSizeIterator<Item = (&CatalogExecutionIdentity, &CatalogExecutionStatus)> {
        self.active_executions.iter()
    }

    /// Bounded unresolved action rows for durable projection stores.
    pub fn open_actions(
        &self,
    ) -> impl ExactSizeIterator<Item = (&CatalogActionIdentity, &CatalogActionKind)> {
        self.open_actions.iter()
    }

    /// Restores one bounded active execution while decoding a trusted catalog
    /// snapshot. Infrastructure must still validate every enum/code field.
    pub fn restore_active_execution(
        &mut self,
        identity: CatalogExecutionIdentity,
        status: CatalogExecutionStatus,
    ) -> Result<(), CatalogProjectionError> {
        if !status.is_active() {
            return Err(CatalogProjectionError::TerminalExecutionAdmission);
        }
        self.active_executions.insert(identity, status);
        Ok(())
    }

    /// Restores one unresolved action while decoding a trusted catalog snapshot.
    pub fn restore_open_action(
        &mut self,
        identity: CatalogActionIdentity,
        kind: CatalogActionKind,
    ) {
        self.open_actions.insert(identity, kind);
    }

    pub fn has_unread_for(
        &self,
        receipt: &ConversationReadReceipt,
        latest_visible_revision: AttentionRevision,
    ) -> Result<bool, CatalogProjectionError> {
        if receipt.control_home != self.control_home
            || receipt.tenant_id != self.tenant_id
            || receipt.conversation_id != self.conversation_id
            || receipt.home_epoch != self.home_epoch
        {
            return Err(CatalogProjectionError::ReceiptAuthorityMismatch);
        }
        if latest_visible_revision > self.attention_revision {
            return Err(CatalogProjectionError::ReceiptBeyondVisibleRevision);
        }
        Ok(receipt.seen_through < latest_visible_revision)
    }

    fn validate_scope(&self, fact: &CatalogFact) -> Result<(), CatalogProjectionError> {
        if fact.control_home != self.control_home
            || fact.tenant_id != self.tenant_id
            || fact.conversation_id != self.conversation_id
            || fact.home_epoch != self.home_epoch
        {
            return Err(CatalogProjectionError::AuthorityScopeMismatch);
        }
        Ok(())
    }

    fn update_execution(
        &mut self,
        identity: CatalogExecutionIdentity,
        status: &CatalogExecutionStatus,
    ) -> bool {
        let previous_len = self.active_executions.len();
        self.active_executions.retain(|existing, _| {
            *existing == identity
                || identity.run_id().is_none()
                || existing.run_id() != identity.run_id()
        });
        let reclassified = self.active_executions.len() != previous_len;
        let changed = if status.is_active() {
            self.active_executions
                .insert(identity, status.clone())
                .as_ref()
                != Some(status)
        } else {
            self.active_executions.remove(&identity).is_some()
        };
        reclassified || changed
    }

    fn apply_state_change(
        &mut self,
        kind: &CatalogFactKind,
        committed_at: UtcTimestamp,
    ) -> Result<bool, CatalogProjectionError> {
        match kind {
            CatalogFactKind::ExecutionAdmitted {
                identity,
                initial_status,
            } => {
                if !initial_status.is_active() {
                    return Err(CatalogProjectionError::TerminalExecutionAdmission);
                }
                Ok(self.update_execution(*identity, initial_status))
            }
            CatalogFactKind::ExecutionStateChanged {
                identity, status, ..
            } => Ok(self.update_execution(*identity, status)),
            CatalogFactKind::ActionOpened { identity, kind } => {
                Ok(self.open_actions.insert(*identity, *kind).as_ref() != Some(kind))
            }
            CatalogFactKind::ActionClosed { identity } => {
                Ok(self.open_actions.remove(identity).is_some())
            }
            CatalogFactKind::ActionReplaced {
                replaced,
                replacement,
                replacement_kind,
            } => {
                let removed = self.open_actions.remove(replaced).is_some();
                let inserted = self
                    .open_actions
                    .insert(*replacement, *replacement_kind)
                    .as_ref()
                    != Some(replacement_kind);
                Ok(removed || inserted)
            }
            CatalogFactKind::LifecycleChanged(lifecycle) => {
                if *lifecycle == ConversationLifecycle::Archived
                    && !self.active_executions.is_empty()
                {
                    return Err(CatalogProjectionError::ArchiveWithActiveExecution);
                }
                let changed = self.lifecycle != *lifecycle;
                self.lifecycle = *lifecycle;
                self.metadata_updated_at = self.metadata_updated_at.max(committed_at);
                Ok(changed)
            }
            CatalogFactKind::StandingGoalStateChanged(status) => {
                let changed = self.standing_goal_status != Some(*status);
                self.standing_goal_status = Some(*status);
                Ok(changed)
            }
            CatalogFactKind::GoalPursuitStateChanged(status) => {
                let changed = self.pursuit_status != *status;
                self.pursuit_status = *status;
                Ok(changed)
            }
            CatalogFactKind::WorkspaceAssigned(workspace_id) => {
                if self
                    .workspace_id
                    .is_some_and(|existing| existing != *workspace_id)
                {
                    return Err(CatalogProjectionError::AuthorityScopeMismatch);
                }
                self.workspace_id = Some(*workspace_id);
                self.metadata_updated_at = self.metadata_updated_at.max(committed_at);
                Ok(true)
            }
            CatalogFactKind::MetadataChanged => {
                self.metadata_updated_at = self.metadata_updated_at.max(committed_at);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn execution_counts(
        &self,
        predicate: impl Fn(&CatalogExecutionIdentity) -> bool,
    ) -> CatalogExecutionCounts {
        let mut counts = CatalogExecutionCounts::default();
        for (identity, status) in &self.active_executions {
            if !predicate(identity) {
                continue;
            }
            match status {
                CatalogExecutionStatus::Queued => counts.queued = counts.queued.saturating_add(1),
                CatalogExecutionStatus::Running => {
                    counts.running = counts.running.saturating_add(1);
                }
                CatalogExecutionStatus::Waiting(_) => {
                    counts.waiting = counts.waiting.saturating_add(1);
                }
                CatalogExecutionStatus::Cancelling => {
                    counts.cancelling = counts.cancelling.saturating_add(1);
                }
                CatalogExecutionStatus::Completed
                | CatalogExecutionStatus::Failed { .. }
                | CatalogExecutionStatus::Cancelled
                | CatalogExecutionStatus::Interrupted { .. } => {}
            }
        }
        counts
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct FactClassification {
    advance_activity: bool,
    attention: Option<(CatalogAttentionKind, Option<ExecutionOutcome>)>,
}

fn classify_fact(kind: &CatalogFactKind) -> FactClassification {
    let attention = match kind {
        CatalogFactKind::ConversationCreated => {
            Some((CatalogAttentionKind::ConversationCreated, None))
        }
        CatalogFactKind::UserMessageCommitted => Some((CatalogAttentionKind::UserMessage, None)),
        CatalogFactKind::VisibleProgress => Some((CatalogAttentionKind::Progress, None)),
        CatalogFactKind::VisibleOutput => Some((CatalogAttentionKind::Output, None)),
        CatalogFactKind::ExecutionStateChanged {
            identity: CatalogExecutionIdentity::Child { .. },
            visible_outcome: Some(outcome),
            ..
        } => Some((CatalogAttentionKind::ChildReport, Some(outcome.clone()))),
        CatalogFactKind::ExecutionStateChanged {
            visible_outcome: Some(outcome),
            ..
        }
        | CatalogFactKind::VisibleExecutionOutcome { outcome } => Some((
            CatalogAttentionKind::ExecutionOutcome,
            Some(outcome.clone()),
        )),
        CatalogFactKind::ChildReport { outcome } => {
            Some((CatalogAttentionKind::ChildReport, Some(outcome.clone())))
        }
        CatalogFactKind::GoalEvaluation { outcome } => {
            Some((CatalogAttentionKind::GoalEvaluation, outcome.clone()))
        }
        CatalogFactKind::GoalDecisionRequired => Some((CatalogAttentionKind::GoalDecision, None)),
        CatalogFactKind::ActionOpened { .. } | CatalogFactKind::ActionReplaced { .. } => {
            Some((CatalogAttentionKind::ActionRequired, None))
        }
        CatalogFactKind::InboundMessage => Some((CatalogAttentionKind::InboundMessage, None)),
        CatalogFactKind::ArtifactOutcome { outcome } => {
            Some((CatalogAttentionKind::ArtifactOutcome, Some(outcome.clone())))
        }
        CatalogFactKind::SystemNotice {
            requires_attention: true,
        } => Some((CatalogAttentionKind::SystemNotice, None)),
        CatalogFactKind::ExecutionAdmitted { .. } => {
            return FactClassification {
                advance_activity: true,
                attention: None,
            };
        }
        CatalogFactKind::ExecutionStateChanged {
            status: CatalogExecutionStatus::Running | CatalogExecutionStatus::Cancelling,
            visible_outcome: None,
            ..
        }
        | CatalogFactKind::ExecutionStateChanged {
            status:
                CatalogExecutionStatus::Waiting(
                    CatalogRunWaitReason::Approval
                    | CatalogRunWaitReason::UserInput
                    | CatalogRunWaitReason::Confirmation
                    | CatalogRunWaitReason::Selection
                    | CatalogRunWaitReason::Credential
                    | CatalogRunWaitReason::OAuth
                    | CatalogRunWaitReason::Budget
                    | CatalogRunWaitReason::Device
                    | CatalogRunWaitReason::Resource
                    | CatalogRunWaitReason::Dependency
                    | CatalogRunWaitReason::ExternalOperation
                    | CatalogRunWaitReason::Delegation
                    | CatalogRunWaitReason::DelegationGroup
                    | CatalogRunWaitReason::DelegationCompletions,
                ),
            visible_outcome: None,
            ..
        } => {
            return FactClassification {
                advance_activity: true,
                attention: None,
            };
        }
        CatalogFactKind::GoalPursuitStateChanged(Some(
            CatalogGoalPursuitStatus::Pending
            | CatalogGoalPursuitStatus::Running
            | CatalogGoalPursuitStatus::Waiting(_),
        )) => {
            return FactClassification {
                advance_activity: true,
                attention: None,
            };
        }
        CatalogFactKind::GoalPursuitStateChanged(Some(CatalogGoalPursuitStatus::Achieved)) => {
            Some((
                CatalogAttentionKind::ExecutionOutcome,
                Some(ExecutionOutcome::completed()),
            ))
        }
        CatalogFactKind::GoalPursuitStateChanged(Some(CatalogGoalPursuitStatus::Failed)) => Some((
            CatalogAttentionKind::ExecutionOutcome,
            Some(ExecutionOutcome {
                kind: ExecutionOutcomeKind::Failed,
                error_class: None,
                recoverability: Some(CatalogRecoverability::Unknown),
            }),
        )),
        CatalogFactKind::GoalPursuitStateChanged(Some(CatalogGoalPursuitStatus::Cancelled)) => {
            Some((
                CatalogAttentionKind::ExecutionOutcome,
                Some(ExecutionOutcome {
                    kind: ExecutionOutcomeKind::Cancelled,
                    error_class: None,
                    recoverability: None,
                }),
            ))
        }
        CatalogFactKind::ExecutionStateChanged {
            visible_outcome: None,
            ..
        }
        | CatalogFactKind::ActionClosed { .. }
        | CatalogFactKind::SystemNotice {
            requires_attention: false,
        }
        | CatalogFactKind::LifecycleChanged(_)
        | CatalogFactKind::StandingGoalStateChanged(_)
        | CatalogFactKind::GoalPursuitStateChanged(None | Some(CatalogGoalPursuitStatus::Paused))
        | CatalogFactKind::WorkspaceAssigned(_)
        | CatalogFactKind::MetadataChanged
        | CatalogFactKind::InternalMaintenance => None,
    };
    FactClassification {
        advance_activity: attention.is_some(),
        attention,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod execution_identity;

    fn timestamp(value: i64) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(value)
    }

    fn source(kind: CatalogSourceKind, identity: CatalogSourceIdentity) -> CatalogSourceKey {
        CatalogSourceKey::new(kind, identity, 1).expect("valid source key")
    }

    fn fixture() -> ConversationCatalogProjection {
        ConversationCatalogProjection::new(
            RuntimeHome::Cloud,
            HomeEpoch::new(1).expect("home epoch"),
            TenantId::generate(),
            ConversationId::generate(),
            Some(WorkspaceId::generate()),
            ConversationKind::Direct,
            ConversationLifecycle::Active,
            timestamp(1_000),
        )
    }

    fn fact(
        projection: &ConversationCatalogProjection,
        source_key: CatalogSourceKey,
        kind: CatalogFactKind,
        committed_at: i64,
    ) -> CatalogFact {
        CatalogFact {
            control_home: projection.control_home,
            tenant_id: projection.tenant_id,
            conversation_id: projection.conversation_id,
            home_epoch: projection.home_epoch,
            source_key,
            source_claim: CatalogSourceClaim::New,
            audience: CatalogAttentionAudience::AllParticipants,
            occurred_at: timestamp(committed_at - 10),
            committed_at: timestamp(committed_at),
            kind,
        }
    }

    #[test]
    fn admitted_execution_advances_sort_once_without_creating_unread_attention() {
        let mut projection = fixture();
        let run_id = RunId::generate();
        let source_key = source(
            CatalogSourceKind::ExecutionAdmitted,
            CatalogSourceIdentity::Run(run_id),
        );
        let admitted = fact(
            &projection,
            source_key,
            CatalogFactKind::ExecutionAdmitted {
                identity: CatalogExecutionIdentity::Root { run_id },
                initial_status: CatalogExecutionStatus::Queued,
            },
            2_000,
        );

        assert_eq!(
            projection.apply(admitted.clone(), CatalogActivityPosition::new(1)),
            Ok(CatalogProjectionApplyDisposition::Applied)
        );
        assert_eq!(projection.last_user_visible_activity_at, timestamp(2_000));
        assert_eq!(projection.attention_revision, AttentionRevision::ZERO);
        assert_eq!(projection.root_execution_counts().queued, 1);
        let mut replay = admitted;
        replay.source_claim = CatalogSourceClaim::Duplicate;
        assert_eq!(
            projection.apply(replay, CatalogActivityPosition::new(2)),
            Ok(CatalogProjectionApplyDisposition::Duplicate)
        );
        assert_eq!(projection.catalog_revision, 1);

        let started = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionStateChanged,
                CatalogSourceIdentity::LedgerRecord(LedgerRecordId::generate()),
            ),
            CatalogFactKind::ExecutionStateChanged {
                identity: CatalogExecutionIdentity::Root { run_id },
                status: CatalogExecutionStatus::Running,
                visible_outcome: None,
            },
            3_000,
        );
        projection
            .apply(started, CatalogActivityPosition::new(2))
            .expect("Run start is visible once");
        assert_eq!(projection.last_user_visible_activity_at, timestamp(3_000));
        assert_eq!(projection.attention_revision, AttentionRevision::ZERO);
    }

    #[test]
    fn child_keeps_conversation_executing_after_root_finishes() {
        let mut projection = fixture();
        let root_run_id = RunId::generate();
        let child_run_id = RunId::generate();
        let delegation_id = DelegationId::generate();
        let root_identity = CatalogExecutionIdentity::Root {
            run_id: root_run_id,
        };
        let child_identity = CatalogExecutionIdentity::Child {
            run_id: child_run_id,
            parent_run_id: root_run_id,
            delegation_id,
        };
        let root_admitted = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionAdmitted,
                CatalogSourceIdentity::Run(root_run_id),
            ),
            CatalogFactKind::ExecutionAdmitted {
                identity: root_identity,
                initial_status: CatalogExecutionStatus::Running,
            },
            2_000,
        );
        projection
            .apply(root_admitted, CatalogActivityPosition::new(1))
            .expect("root admitted");
        let child_admitted = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionAdmitted,
                CatalogSourceIdentity::Run(child_run_id),
            ),
            CatalogFactKind::ExecutionAdmitted {
                identity: child_identity,
                initial_status: CatalogExecutionStatus::Running,
            },
            3_000,
        );
        projection
            .apply(child_admitted, CatalogActivityPosition::new(2))
            .expect("child admitted");
        let root_completed = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionStateChanged,
                CatalogSourceIdentity::Run(root_run_id),
            ),
            CatalogFactKind::ExecutionStateChanged {
                identity: root_identity,
                status: CatalogExecutionStatus::Completed,
                visible_outcome: None,
            },
            4_000,
        );
        projection
            .apply(root_completed, CatalogActivityPosition::new(3))
            .expect("root completed");

        assert_eq!(projection.root_execution_counts().total(), 0);
        assert_eq!(projection.child_execution_counts().running, 1);
        assert!(projection.has_active_execution());

        let child_completed = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionStateChanged,
                CatalogSourceIdentity::LedgerRecord(LedgerRecordId::generate()),
            ),
            CatalogFactKind::ExecutionStateChanged {
                identity: child_identity,
                status: CatalogExecutionStatus::Completed,
                visible_outcome: Some(ExecutionOutcome::completed()),
            },
            5_000,
        );
        projection
            .apply(child_completed, CatalogActivityPosition::new(4))
            .expect("child report");
        assert_eq!(projection.child_execution_counts().total(), 0);
        assert_eq!(
            projection.latest_attention.as_ref().map(|value| value.kind),
            Some(CatalogAttentionKind::ChildReport)
        );
    }

    #[test]
    fn action_running_and_unread_remain_orthogonal() {
        let mut projection = fixture();
        let root_run_id = RunId::generate();
        let child_run_id = RunId::generate();
        let interaction_id = InteractionId::generate();
        let admitted = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionAdmitted,
                CatalogSourceIdentity::Run(child_run_id),
            ),
            CatalogFactKind::ExecutionAdmitted {
                identity: CatalogExecutionIdentity::Child {
                    run_id: child_run_id,
                    parent_run_id: root_run_id,
                    delegation_id: DelegationId::generate(),
                },
                initial_status: CatalogExecutionStatus::Waiting(CatalogRunWaitReason::Approval),
            },
            2_000,
        );
        projection
            .apply(admitted, CatalogActivityPosition::new(1))
            .expect("child admitted");
        let action = fact(
            &projection,
            source(
                CatalogSourceKind::ActionOpened,
                CatalogSourceIdentity::Interaction(interaction_id),
            ),
            CatalogFactKind::ActionOpened {
                identity: CatalogActionIdentity::Interaction(interaction_id),
                kind: CatalogActionKind::Approval,
            },
            3_000,
        );
        projection
            .apply(action, CatalogActivityPosition::new(2))
            .expect("action opened");

        let receipt = ConversationReadReceipt {
            control_home: projection.control_home,
            tenant_id: projection.tenant_id,
            conversation_id: projection.conversation_id,
            home_epoch: projection.home_epoch,
            principal_id: PrincipalId::generate(),
            seen_through: AttentionRevision::ZERO,
            acknowledged_at: timestamp(1_000),
        };
        assert_eq!(projection.child_execution_counts().waiting, 1);
        assert_eq!(projection.open_action_count(), 1);
        assert_eq!(
            projection.highest_priority_action(),
            Some(CatalogActionKind::Approval)
        );
        assert_eq!(
            projection.has_unread_for(&receipt, projection.attention_revision),
            Ok(true)
        );
    }

    #[test]
    fn receipt_is_monotonic_and_does_not_change_activity_order() {
        let mut projection = fixture();
        let message_id = MessageId::generate();
        let visible = fact(
            &projection,
            source(
                CatalogSourceKind::VisibleOutput,
                CatalogSourceIdentity::Message(message_id),
            ),
            CatalogFactKind::VisibleOutput,
            2_000,
        );
        projection
            .apply(visible, CatalogActivityPosition::new(1))
            .expect("visible output");
        let activity_time = projection.last_user_visible_activity_at;
        let activity_position = projection.last_activity_position;
        let mut receipt = ConversationReadReceipt {
            control_home: projection.control_home,
            tenant_id: projection.tenant_id,
            conversation_id: projection.conversation_id,
            home_epoch: projection.home_epoch,
            principal_id: PrincipalId::generate(),
            seen_through: AttentionRevision::ZERO,
            acknowledged_at: timestamp(1_000),
        };

        assert_eq!(
            receipt.acknowledge(
                AttentionRevision::new(1),
                projection.attention_revision,
                timestamp(3_000)
            ),
            Ok(true)
        );
        assert_eq!(
            receipt.acknowledge(
                AttentionRevision::ZERO,
                projection.attention_revision,
                timestamp(4_000)
            ),
            Ok(false)
        );
        assert_eq!(
            projection.has_unread_for(&receipt, projection.attention_revision),
            Ok(false)
        );
        assert_eq!(projection.last_user_visible_activity_at, activity_time);
        assert_eq!(projection.last_activity_position, activity_position);
    }

    #[test]
    fn metadata_and_internal_facts_never_advance_activity() {
        let mut projection = fixture();
        let original_time = projection.last_user_visible_activity_at;
        let metadata = fact(
            &projection,
            source(
                CatalogSourceKind::MetadataChanged,
                CatalogSourceIdentity::Conversation(projection.conversation_id),
            ),
            CatalogFactKind::MetadataChanged,
            9_000,
        );
        assert_eq!(
            projection.apply(metadata, CatalogActivityPosition::new(1)),
            Ok(CatalogProjectionApplyDisposition::Applied)
        );
        assert_eq!(projection.last_user_visible_activity_at, original_time);
        assert_eq!(
            projection.last_activity_position,
            CatalogActivityPosition::ZERO
        );

        let internal = fact(
            &projection,
            source(
                CatalogSourceKind::InternalMaintenance,
                CatalogSourceIdentity::LedgerRecord(LedgerRecordId::generate()),
            ),
            CatalogFactKind::InternalMaintenance,
            10_000,
        );
        assert_eq!(
            projection.apply(internal, CatalogActivityPosition::new(2)),
            Ok(CatalogProjectionApplyDisposition::Ignored)
        );
        assert_eq!(projection.last_user_visible_activity_at, original_time);
    }

    #[test]
    fn action_replacement_is_atomic_and_attention_advancing() {
        let mut projection = fixture();
        let old_id = InteractionId::generate();
        let new_id = InteractionId::generate();
        let opened = fact(
            &projection,
            source(
                CatalogSourceKind::ActionOpened,
                CatalogSourceIdentity::Interaction(old_id),
            ),
            CatalogFactKind::ActionOpened {
                identity: CatalogActionIdentity::Interaction(old_id),
                kind: CatalogActionKind::UserInput,
            },
            2_000,
        );
        projection
            .apply(opened, CatalogActivityPosition::new(1))
            .expect("opened");
        let replacement = fact(
            &projection,
            source(
                CatalogSourceKind::ActionReplaced,
                CatalogSourceIdentity::Interaction(new_id),
            ),
            CatalogFactKind::ActionReplaced {
                replaced: CatalogActionIdentity::Interaction(old_id),
                replacement: CatalogActionIdentity::Interaction(new_id),
                replacement_kind: CatalogActionKind::Credential,
            },
            3_000,
        );
        projection
            .apply(replacement, CatalogActivityPosition::new(2))
            .expect("replaced");

        assert_eq!(projection.open_action_count(), 1);
        assert_eq!(
            projection.highest_priority_action(),
            Some(CatalogActionKind::Credential)
        );
        assert_eq!(projection.attention_revision, AttentionRevision::new(2));
    }

    #[test]
    fn new_sources_must_follow_authority_commit_order() {
        let mut projection = fixture();
        let first = fact(
            &projection,
            source(
                CatalogSourceKind::InternalMaintenance,
                CatalogSourceIdentity::LedgerRecord(LedgerRecordId::generate()),
            ),
            CatalogFactKind::InternalMaintenance,
            2_000,
        );
        projection
            .apply(first, CatalogActivityPosition::new(4))
            .expect("first commit");
        let stale = fact(
            &projection,
            source(
                CatalogSourceKind::InternalMaintenance,
                CatalogSourceIdentity::LedgerRecord(LedgerRecordId::generate()),
            ),
            CatalogFactKind::InternalMaintenance,
            3_000,
        );
        assert_eq!(
            projection.apply(stale, CatalogActivityPosition::new(3)),
            Err(CatalogProjectionError::NonMonotonicCommitPosition)
        );
    }

    #[test]
    fn invalid_fact_is_rejected_without_partially_advancing_projection() {
        let mut projection = fixture();
        let before = projection.clone();
        let invalid = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionAdmitted,
                CatalogSourceIdentity::Run(RunId::generate()),
            ),
            CatalogFactKind::ExecutionAdmitted {
                identity: CatalogExecutionIdentity::Root {
                    run_id: RunId::generate(),
                },
                initial_status: CatalogExecutionStatus::Completed,
            },
            2_000,
        );

        assert_eq!(
            projection.apply(invalid, CatalogActivityPosition::new(1)),
            Err(CatalogProjectionError::TerminalExecutionAdmission)
        );
        assert_eq!(projection, before);
    }

    #[test]
    fn source_kind_must_match_normalized_fact_kind() {
        let mut projection = fixture();
        let invalid = fact(
            &projection,
            source(
                CatalogSourceKind::MetadataChanged,
                CatalogSourceIdentity::Message(MessageId::generate()),
            ),
            CatalogFactKind::VisibleOutput,
            2_000,
        );

        assert_eq!(
            projection.apply(invalid, CatalogActivityPosition::new(1)),
            Err(CatalogProjectionError::SourceKindMismatch)
        );
        assert_eq!(
            projection.latest_commit_position,
            CatalogActivityPosition::ZERO
        );
        assert_eq!(projection.attention_revision, AttentionRevision::ZERO);
    }

    #[test]
    fn closing_action_changes_catalog_but_neither_attention_nor_sort_order() {
        let mut projection = fixture();
        let interaction_id = InteractionId::generate();
        let opened = fact(
            &projection,
            source(
                CatalogSourceKind::ActionOpened,
                CatalogSourceIdentity::Interaction(interaction_id),
            ),
            CatalogFactKind::ActionOpened {
                identity: CatalogActionIdentity::Interaction(interaction_id),
                kind: CatalogActionKind::Confirmation,
            },
            2_000,
        );
        projection
            .apply(opened, CatalogActivityPosition::new(1))
            .expect("opened");
        let activity_time = projection.last_user_visible_activity_at;
        let activity_position = projection.last_activity_position;
        let attention_revision = projection.attention_revision;
        let closed = fact(
            &projection,
            source(
                CatalogSourceKind::ActionClosed,
                CatalogSourceIdentity::Interaction(interaction_id),
            ),
            CatalogFactKind::ActionClosed {
                identity: CatalogActionIdentity::Interaction(interaction_id),
            },
            9_000,
        );

        assert_eq!(
            projection.apply(closed, CatalogActivityPosition::new(2)),
            Ok(CatalogProjectionApplyDisposition::Applied)
        );
        assert_eq!(projection.open_action_count(), 0);
        assert_eq!(projection.attention_revision, attention_revision);
        assert_eq!(projection.last_user_visible_activity_at, activity_time);
        assert_eq!(projection.last_activity_position, activity_position);
    }

    #[test]
    fn active_goal_is_not_running_without_a_real_pursuit_execution() {
        let mut projection = fixture();
        let goal_id = GoalId::generate();
        let goal_state = fact(
            &projection,
            source(
                CatalogSourceKind::StandingGoalStateChanged,
                CatalogSourceIdentity::Goal(goal_id),
            ),
            CatalogFactKind::StandingGoalStateChanged(CatalogStandingGoalStatus::Active),
            2_000,
        );
        projection
            .apply(goal_state, CatalogActivityPosition::new(1))
            .expect("goal state");

        assert_eq!(
            projection.standing_goal_status,
            Some(CatalogStandingGoalStatus::Active)
        );
        assert_eq!(projection.pursuit_execution_counts().total(), 0);
        assert!(!projection.has_active_execution());
        assert_eq!(projection.attention_revision, AttentionRevision::ZERO);
        assert_eq!(
            projection.last_activity_position,
            CatalogActivityPosition::ZERO
        );
    }

    #[test]
    fn archive_is_rejected_while_any_child_execution_is_active() {
        let mut projection = fixture();
        let root_run_id = RunId::generate();
        let child_run_id = RunId::generate();
        let admitted = fact(
            &projection,
            source(
                CatalogSourceKind::ExecutionAdmitted,
                CatalogSourceIdentity::Run(child_run_id),
            ),
            CatalogFactKind::ExecutionAdmitted {
                identity: CatalogExecutionIdentity::Child {
                    run_id: child_run_id,
                    parent_run_id: root_run_id,
                    delegation_id: DelegationId::generate(),
                },
                initial_status: CatalogExecutionStatus::Running,
            },
            2_000,
        );
        projection
            .apply(admitted, CatalogActivityPosition::new(1))
            .expect("child admitted");
        let before = projection.clone();
        let archive = fact(
            &projection,
            source(
                CatalogSourceKind::LifecycleChanged,
                CatalogSourceIdentity::Conversation(projection.conversation_id),
            ),
            CatalogFactKind::LifecycleChanged(ConversationLifecycle::Archived),
            3_000,
        );

        assert_eq!(
            projection.apply(archive, CatalogActivityPosition::new(2)),
            Err(CatalogProjectionError::ArchiveWithActiveExecution)
        );
        assert_eq!(projection, before);
    }

    #[test]
    fn acknowledging_old_revision_does_not_consume_new_attention() {
        let mut projection = fixture();
        let first_message = MessageId::generate();
        let first = fact(
            &projection,
            source(
                CatalogSourceKind::VisibleOutput,
                CatalogSourceIdentity::Message(first_message),
            ),
            CatalogFactKind::VisibleOutput,
            2_000,
        );
        projection
            .apply(first, CatalogActivityPosition::new(1))
            .expect("first output");
        let mut receipt = ConversationReadReceipt {
            control_home: projection.control_home,
            tenant_id: projection.tenant_id,
            conversation_id: projection.conversation_id,
            home_epoch: projection.home_epoch,
            principal_id: PrincipalId::generate(),
            seen_through: AttentionRevision::ZERO,
            acknowledged_at: timestamp(1_000),
        };
        let rendered_revision = projection.attention_revision;
        let second_message = MessageId::generate();
        let second = fact(
            &projection,
            source(
                CatalogSourceKind::VisibleOutput,
                CatalogSourceIdentity::Message(second_message),
            ),
            CatalogFactKind::VisibleOutput,
            3_000,
        );
        projection
            .apply(second, CatalogActivityPosition::new(2))
            .expect("second output");

        assert_eq!(
            receipt.acknowledge(
                rendered_revision,
                projection.attention_revision,
                timestamp(4_000)
            ),
            Ok(true)
        );
        assert_eq!(
            projection.has_unread_for(&receipt, projection.attention_revision),
            Ok(true)
        );
        assert_eq!(receipt.seen_through, AttentionRevision::new(1));
        assert_eq!(projection.attention_revision, AttentionRevision::new(2));
    }
}
