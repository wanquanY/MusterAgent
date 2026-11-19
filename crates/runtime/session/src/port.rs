mod admission;
pub use admission::*;

mod conversation_mutation;
pub use conversation_mutation::*;

mod interaction;
pub use interaction::RuntimeInteractionApplicationStore;

use musteragent_approval::{Interaction, InteractionRead};
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_conversation::{
    Conversation, ConversationLifecycle, ConversationPage, ConversationView, ListConversations,
    Participant,
};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, Actor, AttemptId, AttemptNumber, ContextBranchId, ConversationId, ConversationSeq,
    CorrelationId, DelegationGroupId, DelegationId, HomeEpoch, InteractionId, InvocationId,
    LedgerOffset, LedgerRecordId, MessageId, ParticipantId, RunId, RunSeq, RuntimeSnapshotId,
    TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_ledger::{
    AdmissionBatch, AdmissionReceipt, AppendReceipt, AppendRequest, CancelMode, CancelRunBatch,
    CancelRunReceipt, CascadeCancelTarget, DelegationAdmissionBatch, RetryAdmissionBatch,
    RunControlBatch,
};
use musteragent_run::RunView;
use musteragent_runtime_content::AdmissionContentStage;
use musteragent_runtime_contract::{ApplicationFuture, CommandAck, ErrorCode, RuntimeError};
use musteragent_runtime_interaction::{
    CloseInteractionCommit, InteractionCommitReceipt, RespondInteractionCommit,
};
use musteragent_runtime_snapshot::RuntimeSnapshotContent;

/// Parent-side authority captured before asking Backend to sign a child grant.
/// Persistence rechecks the same identities inside the final transaction.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DelegationKind {
    /// Parent releases its Attempt and resumes only after a durable child report.
    Delegate,
    /// Parent immediately receives the child Activity handle.
    Spawn,
}

/// Frozen result-delivery contract for an asynchronously spawned child.
/// Synchronous `Delegate` always uses `NotifyOnly` because its Tool result is
/// already delivered through the parent wait transition.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DelegationCompletionMode {
    /// Put the durable ChildReport in the parent Run's private completion
    /// inbox and resume that Run at a fenced safe boundary.
    ResumeParent,
    /// Publish child lifecycle/output to the Conversation projection only.
    NotifyOnly,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegationAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub parent_activity_id: ActivityId,
    pub root_activity_id: ActivityId,
    pub parent_run_id: RunId,
    /// Exact immutable product selected by the parent Snapshot. The child
    /// signer may narrow capabilities, but cannot replace this identity.
    pub product_release: Option<musteragent_identity::ProductReleaseRef>,
    pub parent_attempt_number: AttemptNumber,
    pub parent_last_run_sequence: RunSeq,
    pub requested_by_participant_id: ParticipantId,
    pub executor_participant_id: ParticipantId,
    pub execution_scope: ExecutionScope,
    pub control_home: RuntimeHome,
    pub execution_home: RuntimeHome,
    /// Effective public reasoning policy sealed into the parent Runtime
    /// Snapshot. A direct child inherits this user-visible output boundary;
    /// its exact effort remains owned by the child Execution Grant route.
    pub reasoning_summary: bool,
    pub child_depth: u32,
    /// Admission-sealed fan-out policy decoded from the parent Runtime
    /// Snapshot. These limits participate in the stable-authority comparison
    /// across the Backend signing round trip.
    pub max_delegation_depth: u32,
    pub max_active_delegated_children: u32,
    pub max_delegation_group_size: u32,
    pub already_admitted: bool,
}

impl DelegationAuthority {
    /// Compares only facts that authorize the child Run. `current_offset` and
    /// `parent_last_run_sequence` are commit cursors observed before the
    /// Backend grant round trip, not authority facts. In particular, one
    /// parallel Tool result may advance the parent Run sequence while its
    /// already-authorized sibling delegation is still being committed.
    #[must_use]
    pub fn has_same_stable_authority(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
            && self.conversation_id == other.conversation_id
            && self.home_epoch == other.home_epoch
            && self.parent_activity_id == other.parent_activity_id
            && self.root_activity_id == other.root_activity_id
            && self.parent_run_id == other.parent_run_id
            && self.product_release == other.product_release
            && self.parent_attempt_number == other.parent_attempt_number
            && self.requested_by_participant_id == other.requested_by_participant_id
            && self.executor_participant_id == other.executor_participant_id
            && self.execution_scope == other.execution_scope
            && self.control_home == other.control_home
            && self.execution_home == other.execution_home
            && self.reasoning_summary == other.reasoning_summary
            && self.child_depth == other.child_depth
            && self.max_delegation_depth == other.max_delegation_depth
            && self.max_active_delegated_children == other.max_active_delegated_children
            && self.max_delegation_group_size == other.max_delegation_group_size
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DelegationAuthorizationRequest {
    pub actor: Actor,
    pub kind: DelegationKind,
    pub parent_run_id: RunId,
    pub parent_attempt_id: AttemptId,
    pub invocation_id: InvocationId,
    pub child_run_id: RunId,
    pub delegation_id: DelegationId,
    pub correlation_id: CorrelationId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DelegationAdmissionCommit {
    pub authority: DelegationAuthority,
    pub kind: DelegationKind,
    pub completion_mode: DelegationCompletionMode,
    pub child_command_id: musteragent_identity::CommandId,
    pub parent_attempt_id: AttemptId,
    pub invocation_id: InvocationId,
    pub result_schema: SchemaRef,
    /// Terminal delegation whose immutable objective and presentation
    /// identity were used to create this new child execution. Retries never
    /// mutate or reactivate the original Run.
    pub retry_of: Option<DelegationId>,
    pub batch: DelegationAdmissionBatch,
    pub parent_wait_append: Option<AppendRequest>,
    pub content_stage: AdmissionContentStage,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DelegationReportStatus {
    Active,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl DelegationReportStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegationJoinMember {
    pub delegation_id: DelegationId,
    pub child_activity_id: ActivityId,
    pub child_run_id: RunId,
    pub status: DelegationReportStatus,
    pub report: Option<musteragent_content_ref::ContentRef>,
}

/// Read-only, invocation-fenced view of one child owned by the current parent
/// Run. The descriptor fields remain immutable while execution fields are
/// derived from the child Run and ChildReport truth.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegationInspectionMember {
    pub delegation_id: DelegationId,
    pub kind: DelegationKind,
    pub completion_mode: DelegationCompletionMode,
    pub retry_of: Option<DelegationId>,
    pub display_name: musteragent_runtime_contract::DisplayText,
    pub objective: musteragent_content_ref::ContentRef,
    pub child_activity_id: ActivityId,
    pub child_run_id: RunId,
    pub status: DelegationReportStatus,
    pub report: Option<musteragent_content_ref::ContentRef>,
    pub child_run_state: musteragent_run::RunState,
    pub child_wait_reason: Option<musteragent_run::RunWaitReason>,
    pub child_last_run_sequence: RunSeq,
}

/// Stable authorization snapshot for a read-only delegation inspection. The
/// Conversation offset is also the long-poll revision token.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegationInspectionAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub parent_run_id: RunId,
    pub requested_by_participant_id: ParticipantId,
    pub parent_executor_participant_id: ParticipantId,
    pub members: Vec<DelegationInspectionMember>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegationInspectionRequest {
    pub actor: Actor,
    pub parent_run_id: RunId,
    pub parent_attempt_id: AttemptId,
    pub invocation_id: InvocationId,
    /// Empty means list every child directly owned by `parent_run_id`.
    pub delegation_ids: Vec<DelegationId>,
    pub capability_key: musteragent_identity::VersionedKey,
    pub correlation_id: CorrelationId,
}

/// Efficient wait registration used by `delegation.inspect@1` after an
/// authorized snapshot has established the parent Conversation and revision.
/// It carries no data and never changes Run state.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DelegationRevisionWaitRequest {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub parent_run_id: RunId,
    pub after_offset: LedgerOffset,
    pub max_wait: std::time::Duration,
    pub correlation_id: CorrelationId,
}

/// Stable authority and member snapshot for one all-of delegation join.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegationJoinAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub parent_activity_id: ActivityId,
    pub parent_run_id: RunId,
    pub parent_attempt_number: AttemptNumber,
    pub parent_last_run_sequence: RunSeq,
    pub executor_participant_id: ParticipantId,
    pub max_delegation_group_size: u32,
    pub members: Vec<DelegationJoinMember>,
    pub already_joined: bool,
    pub joined_report: Option<musteragent_content_ref::ContentRef>,
}

impl DelegationJoinAuthority {
    #[must_use]
    pub fn has_same_stable_authority(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
            && self.conversation_id == other.conversation_id
            && self.home_epoch == other.home_epoch
            && self.parent_activity_id == other.parent_activity_id
            && self.parent_run_id == other.parent_run_id
            && self.parent_attempt_number == other.parent_attempt_number
            && self.parent_last_run_sequence == other.parent_last_run_sequence
            && self.executor_participant_id == other.executor_participant_id
            && self.max_delegation_group_size == other.max_delegation_group_size
            && self.members == other.members
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegationJoinAuthorizationRequest {
    pub actor: Actor,
    pub parent_run_id: RunId,
    pub parent_attempt_id: AttemptId,
    pub invocation_id: InvocationId,
    pub group_id: DelegationGroupId,
    pub delegation_ids: Vec<DelegationId>,
    pub correlation_id: CorrelationId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DelegationJoinCommit {
    pub authority: DelegationJoinAuthority,
    pub actor: Actor,
    pub parent_attempt_id: AttemptId,
    pub invocation_id: InvocationId,
    pub group_id: DelegationGroupId,
    pub delegation_ids: Vec<DelegationId>,
    pub result_schema: SchemaRef,
    pub report: Option<musteragent_content_ref::ContentRef>,
    pub parent_wait_append: Option<AppendRequest>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegationJoinCommitReceipt {
    pub group_id: DelegationGroupId,
    pub waiting: bool,
    pub report: Option<musteragent_content_ref::ContentRef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunControlCommit {
    pub batch: RunControlBatch,
    pub content_stage: AdmissionContentStage,
    /// Present only for a parent Agent follow-up addressed to one directly
    /// owned delegated child. Persistence revalidates this fence atomically;
    /// ordinary user-authored controls keep it `None`.
    pub delegated_parent: Option<DelegatedRunControlFence>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegatedRunControlFence {
    pub parent_run_id: RunId,
    pub parent_attempt_id: AttemptId,
    pub parent_invocation_id: InvocationId,
    /// Exact immutable capability version whose pending invocation authorizes
    /// this control. Persistence must never guess a current or legacy key.
    pub capability_key: VersionedKey,
    pub delegation_id: DelegationId,
    pub parent_executor_participant_id: ParticipantId,
}

/// Transactional authority for a cancellation initiated by a parent Agent's
/// currently executing `delegate_cancel` invocation. The ordinary cancellation
/// API remains human-authored; this commit shape prevents delegated controls
/// from borrowing that broader authority after the parent invocation ends.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DelegatedCancelRunCommit {
    pub actor: Actor,
    pub batch: CancelRunBatch,
    pub parent: DelegatedRunControlFence,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateConversationCommit {
    pub actor: Actor,
    pub conversation: Conversation,
    pub user_participant: Participant,
    pub assistant_participant: Participant,
    pub append: AppendRequest,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InteractionAuthority {
    pub interaction: Interaction,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub last_run_sequence: RunSeq,
    pub responding_participant_id: ParticipantId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CancelRunAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub last_run_sequence: RunSeq,
    pub requested_by_participant_id: ParticipantId,
    pub last_attempt_id: Option<AttemptId>,
    pub mode: CancelMode,
    pub pending_interaction_id: Option<InteractionId>,
    pub pending_tool_invocation_id: Option<InvocationId>,
    /// Original unknown outcome: retained, never emitted as ToolCancelled.
    pub unresolved_tool_invocation_id: Option<InvocationId>,
    /// Closed transitive set of still-live delegated descendants observed at
    /// the same Conversation offset as the root authority read.
    pub descendants: Vec<CascadeCancelTarget>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RunControlAuthority {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub current_offset: LedgerOffset,
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub last_run_sequence: RunSeq,
    pub requested_by_participant_id: ParticipantId,
}

/// Persistence boundary used by the shared application service.
///
/// Implementations own transactions and authorization queries but cannot
/// invent semantic events or Runtime snapshots.
pub trait RuntimeApplicationStore: Send + Sync {
    /// Resolves a committed command before mutable authorization state is read.
    /// This keeps retries idempotent after the original command changed the
    /// active Run, branch, cursor, or Conversation lifecycle.
    fn load_command_replay(
        &self,
        query: CommandReplayQuery,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<AppendReceipt>, RuntimeError>>;

    /// Recovers an original Conversation creation receipt from canonical state
    /// and immutable Ledger envelopes. Implementations must not consult
    /// compactable payload bytes or retention-bounded idempotency projections.
    fn recover_conversation_creation(
        &self,
        query: ConversationCreationRecoveryQuery,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>>;

    /// Resolves a committed admission and its original preparation time. The
    /// default preserves compatibility for non-product test stores; durable
    /// Cloud and device stores override it with Snapshot-backed recovery.
    fn load_admission_replay(
        &self,
        query: CommandReplayQuery,
        _runtime_snapshot_id: musteragent_identity::RuntimeSnapshotId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<AdmissionReplayReceipt>, RuntimeError>> {
        let future = self.load_command_replay(query, correlation_id);
        Box::pin(async move {
            Ok(future.await?.map(|ledger| AdmissionReplayReceipt {
                prepared_at: ledger.committed_at,
                ledger,
            }))
        })
    }

    fn load_admission_denial(
        &self,
        query: AdmissionDenialQuery,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<AdmissionDenialReceipt>, RuntimeError>>;

    fn record_admission_denial(
        &self,
        commit: AdmissionDenialCommit,
    ) -> ApplicationFuture<'_, Result<AdmissionDenialReceipt, RuntimeError>>;

    /// Admission always selects an explicit host-registered Agent key.
    fn load_assistant_identity(
        &self,
        agent_key: musteragent_identity::AgentKey,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<AgentAdmissionIdentity, RuntimeError>>;

    fn create_conversation(
        &self,
        command_id: musteragent_identity::CommandId,
        correlation_id: musteragent_identity::CorrelationId,
        commit: CreateConversationCommit,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn authorize_admission(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        expected_home_epoch: HomeEpoch,
        correlation_id: musteragent_identity::CorrelationId,
        audit_context: AdmissionAuditContext,
    ) -> ApplicationFuture<'_, Result<AdmissionAuthority, RuntimeError>>;

    /// Home-aware admission authority used by shared Cloud and Device
    /// Applications. Existing Cloud stores retain their proven authorization;
    /// any non-Cloud composition must override this method and validate the
    /// persisted Conversation home in the same authority read.
    fn authorize_admission_for_home(
        &self,
        request: AdmissionAuthorizationRequest,
    ) -> ApplicationFuture<'_, Result<AdmissionAuthority, RuntimeError>> {
        if request.expected_control_home != RuntimeHome::Cloud {
            let correlation_id = request.correlation_id;
            return Box::pin(async move {
                Err(crate::runtime_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Runtime store does not support the configured Conversation home",
                ))
            });
        }
        self.authorize_admission(
            request.actor,
            request.conversation_id,
            request.participant_id,
            request.expected_home_epoch,
            request.correlation_id,
            request.audit_context,
        )
    }

    fn admit(
        &self,
        command_id: musteragent_identity::CommandId,
        correlation_id: musteragent_identity::CorrelationId,
        batch: AdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, RuntimeError>>;

    /// Commit staged input and routing authority atomically for the real host surface.
    /// Implementations must publish the stage; there is no batch-only fallback.
    fn admit_content(
        &self,
        command_id: musteragent_identity::CommandId,
        correlation_id: CorrelationId,
        surface: musteragent_scheduler::AdmissionSurface,
        commit: AdmissionCommit,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, RuntimeError>>;

    /// An unsupported adapter must reject, never admit only the source message.
    fn admit_goal_start(
        &self,
        _command_id: musteragent_identity::CommandId,
        correlation_id: CorrelationId,
        _surface: musteragent_scheduler::AdmissionSurface,
        _commit: crate::GoalStartAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support atomic Goal start",
            ))
        })
    }

    /// Sole-Home receipt/content/Run/outbox transaction. Unsupported stores
    /// reject explicitly; this must never fall back to ordinary admission.
    /// Independent V10 admission. Unsupported stores cannot downgrade it to V9
    /// or private execution; both variants share the same business transaction.
    /// Definite rejection shares the accepted-command writer and identity.
    /// Unsupported stores cannot claim an uncommitted decision as final.
    fn authorize_delegation(
        &self,
        request: DelegationAuthorizationRequest,
    ) -> ApplicationFuture<'_, Result<DelegationAuthority, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                request.correlation_id,
                false,
                "Runtime store does not support delegated child admission",
            ))
        })
    }

    fn admit_delegation_content(
        &self,
        correlation_id: CorrelationId,
        _commit: DelegationAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support delegated child admission",
            ))
        })
    }

    fn authorize_delegation_join(
        &self,
        request: DelegationJoinAuthorizationRequest,
    ) -> ApplicationFuture<'_, Result<DelegationJoinAuthority, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                request.correlation_id,
                false,
                "Runtime store does not support delegation joins",
            ))
        })
    }

    fn commit_delegation_join(
        &self,
        correlation_id: CorrelationId,
        _commit: DelegationJoinCommit,
    ) -> ApplicationFuture<'_, Result<DelegationJoinCommitReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support delegation joins",
            ))
        })
    }

    fn inspect_delegations(
        &self,
        request: DelegationInspectionRequest,
    ) -> ApplicationFuture<'_, Result<DelegationInspectionAuthority, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                request.correlation_id,
                false,
                "Runtime store does not support delegation inspection",
            ))
        })
    }

    fn wait_for_delegation_revision(
        &self,
        _request: DelegationRevisionWaitRequest,
    ) -> ApplicationFuture<'_, Result<bool, RuntimeError>> {
        Box::pin(async { Ok(false) })
    }

    fn admit_coordination_content(
        &self,
        correlation_id: CorrelationId,
        _commit: CoordinationAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support cross-Conversation turn admission",
            ))
        })
    }

    fn admit_standing_goal(
        &self,
        correlation_id: CorrelationId,
        _commit: StandingGoalAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::StandingGoalCommitOutcome, RuntimeError>>
    {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support standing Goal continuation admission",
            ))
        })
    }

    fn admit_retry(
        &self,
        correlation_id: CorrelationId,
        _batch: RetryAdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support terminal Run retry admission",
            ))
        })
    }

    fn get_conversation(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>>;

    fn list_conversations(
        &self,
        query: ListConversations,
    ) -> ApplicationFuture<'_, Result<ConversationPage, RuntimeError>>;

    fn get_run(
        &self,
        actor: Actor,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<RunView, RuntimeError>>;

    /// Loads the exact immutable Runtime Snapshot that admitted a Run.
    ///
    /// Standing Goal continuation uses this as its execution-profile source;
    /// it must never reconstruct requested model features from deployment
    /// defaults or from the current composer state.
    fn load_run_runtime_snapshot(
        &self,
        _tenant_id: TenantId,
        _conversation_id: ConversationId,
        _run_id: RunId,
        _runtime_snapshot_id: RuntimeSnapshotId,
        _expected_content_hash: Option<Sha256>,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotContent, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support immutable Run Snapshot loading",
            ))
        })
    }

    /// Rebuilds the exact Tool exposure state of one terminal source Run.
    /// The supplied reachability point is the same optimistic Conversation
    /// authority later consumed by retry admission, so an unreachable source
    /// or concurrent branch change fails closed.
    fn load_run_discovery(
        &self,
        _query: RunDiscoveryQuery,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<PriorRunDiscovery>, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support exact Run discovery loading",
            ))
        })
    }

    fn authorize_dialogue_cancel_run(
        &self,
        _fence: crate::DialogueRunControlFence,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<CancelRunAuthority, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support Dialogue Run control",
            ))
        })
    }

    fn cancel_dialogue_run(
        &self,
        correlation_id: CorrelationId,
        _commit: crate::DialogueCancelRunCommit,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support Dialogue Run control",
            ))
        })
    }

    fn authorize_work_item_cancel_run(
        &self,
        _fence: crate::WorkItemRunControlFence,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<CancelRunAuthority, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support Task Run control",
            ))
        })
    }
    fn cancel_work_item_run(
        &self,
        correlation_id: CorrelationId,
        _commit: crate::WorkItemCancelRunCommit,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support Task Run control",
            ))
        })
    }

    fn authorize_cancel_run(
        &self,
        actor: Actor,
        run_id: RunId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<CancelRunAuthority, RuntimeError>>;

    /// Replays only under current Home, requester and peer authority. Shared
    /// operations must never use the private command-replay fast path.
    fn cancel_run(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        batch: CancelRunBatch,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, RuntimeError>>;

    fn cancel_delegated_run(
        &self,
        correlation_id: CorrelationId,
        _commit: DelegatedCancelRunCommit,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Runtime store does not support delegated parent cancellation",
            ))
        })
    }

    fn resume_run(
        &self,
        correlation_id: CorrelationId,
        _commit: crate::RunResumeCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            Err(crate::runtime_error(
                ErrorCode::UnsupportedSchemaVersion,
                correlation_id,
                false,
                "Runtime store does not support durable recovery resumption",
            ))
        })
    }

    fn authorize_run_control(
        &self,
        actor: Actor,
        run_id: RunId,
        participant_id: ParticipantId,
        expected_home_epoch: HomeEpoch,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<RunControlAuthority, RuntimeError>>;

    fn enqueue_run_control(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        batch: RunControlBatch,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>>;

    fn enqueue_run_control_content(
        &self,
        correlation_id: CorrelationId,
        commit: RunControlCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>> {
        if commit.delegated_parent.is_some() {
            return Box::pin(async move {
                Err(crate::runtime_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Runtime store does not support delegated parent controls",
                ))
            });
        }
        self.enqueue_run_control(correlation_id, commit.batch)
    }

    fn authorize_retract_message(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        message_id: MessageId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<RetractMessageAuthority, RuntimeError>>;

    fn retract_message(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: RetractMessageCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>>;

    fn authorize_fork_context(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        parent_branch_id: Option<ContextBranchId>,
        fork_from_record_id: LedgerRecordId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<ForkContextAuthority, RuntimeError>>;

    fn fork_context(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: ForkContextCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>>;

    fn authorize_activate_context_branch(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        branch_id: ContextBranchId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<ContextBranchAuthority, RuntimeError>>;

    fn activate_context_branch(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: ActivateContextBranchCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>>;

    fn authorize_edit_and_resubmit(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        target_message_id: MessageId,
        expected_home_epoch: HomeEpoch,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<EditAndResubmitAuthority, RuntimeError>>;

    /// Home-aware edit admission. Device stores must override this method and
    /// bind the persisted Conversation home in the same authority read.
    fn authorize_edit_and_resubmit_for_home(
        &self,
        request: EditAndResubmitAuthorizationRequest,
    ) -> ApplicationFuture<'_, Result<EditAndResubmitAuthority, RuntimeError>> {
        if request.expected_control_home != RuntimeHome::Cloud {
            let correlation_id = request.correlation_id;
            return Box::pin(async move {
                Err(crate::runtime_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Runtime store does not support the configured Conversation home",
                ))
            });
        }
        self.authorize_edit_and_resubmit(
            request.actor,
            request.conversation_id,
            request.participant_id,
            request.target_message_id,
            request.expected_home_epoch,
            request.correlation_id,
        )
    }

    fn edit_and_resubmit(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: EditAndResubmitCommit,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, RuntimeError>>;

    fn edit_and_resubmit_content(
        &self,
        correlation_id: CorrelationId,
        commit: EditAndResubmitAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, RuntimeError>> {
        self.edit_and_resubmit(correlation_id, commit.edit)
    }

    fn authorize_archive_conversation(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<ArchiveConversationAuthority, RuntimeError>>;

    fn archive_conversation(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: ArchiveConversationCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>>;

    fn authorize_unarchive_conversation(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<UnarchiveConversationAuthority, RuntimeError>>;

    fn unarchive_conversation(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: UnarchiveConversationCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>>;

    fn authorize_set_conversation_title(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<SetConversationTitleAuthority, RuntimeError>>;

    fn set_conversation_title(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: SetConversationTitleCommit,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, RuntimeError>>;
}
