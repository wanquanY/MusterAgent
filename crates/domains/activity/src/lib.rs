//! Activity semantics shared by chat, coordination, automation and delegation.

#![forbid(unsafe_code)]

use musteragent_content_ref::ContentRef;
use musteragent_identity::{
    ActivityId, ConversationId, DelegationId, DreamJobId, GoalId, GoalPursuitId,
    InterSessionMessageId, ParticipantId, UtcTimestamp, VersionedKey,
};
use musteragent_runtime_contract::{CancelReason, ErrorInfo, ResourceRef};

/// Work semantics inside a Conversation.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ActivityKind {
    Chat,
    Coordination,
    Automation,
    Delegation,
    Maintenance,
    /// Independent system-Agent evaluation. It reuses the complete Agent Loop
    /// with a private read-only Inspection/Decision capability profile; its
    /// output is a Runtime receipt and never a user-facing Assistant message.
    Evaluation,
}

impl ActivityKind {
    /// Only user- or workflow-owned work may teach the ordinary Agent.
    ///
    /// Maintenance and Evaluation Runs are Runtime-private control-plane
    /// activities. Feeding their outputs back into the ordinary learning
    /// pipeline would recursively train on the Runtime's own maintenance or
    /// judging prompts instead of on user work.
    #[must_use]
    pub const fn is_ordinary_learning_eligible(self) -> bool {
        matches!(
            self,
            Self::Chat | Self::Coordination | Self::Automation | Self::Delegation
        )
    }
}

/// Aggregate lifecycle independent from Run worker state.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ActivityStatus {
    Planned,
    Active,
    Waiting,
    Completed,
    Failed,
    Cancelled,
}

/// Minimal Activity aggregate root contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Activity {
    pub activity_id: ActivityId,
    pub conversation_id: ConversationId,
    pub kind: ActivityKind,
    pub requested_by_participant_id: ParticipantId,
    pub parent_activity_id: Option<ActivityId>,
    pub root_activity_id: ActivityId,
    pub status: ActivityStatus,
    pub policy_snapshot_ref: VersionedKey,
    pub created_at: UtcTimestamp,
    pub completed_at: Option<UtcTimestamp>,
}

/// Activity-owned facts independent from worker or transport state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ActivityEvent {
    Started {
        kind: ActivityKind,
        root_activity_id: ActivityId,
        parent_activity_id: Option<ActivityId>,
        policy_snapshot_ref: VersionedKey,
    },
    /// Immutable, minimal context handed from a parent Run to one delegated
    /// child Activity. This is deliberately not a Conversation message: it
    /// must be available to the child model without polluting the visible
    /// transcript or granting access to the parent's full context.
    DelegationContextProjected {
        delegation_id: DelegationId,
        parent_invocation_id: musteragent_identity::InvocationId,
        objective_ref: ContentRef,
    },
    /// Immutable input delivered from another Conversation's durable mailbox.
    /// It is private model context rather than a Conversation `UserMessage`,
    /// while Product Projection may expose the same payload as a typed
    /// Coordination ingress with exact provenance. It must never impersonate
    /// the target user or copy either transcript.
    CoordinationContextProjected {
        mailbox_message_id: InterSessionMessageId,
        source_conversation_id: ConversationId,
        payload_ref: ContentRef,
    },
    /// Runtime-private frozen input for one autonomous learning maintenance
    /// Run. It is not a Conversation message and cannot impersonate a user.
    MaintenanceContextProjected {
        dream_job_id: DreamJobId,
        input_manifest_ref: ContentRef,
        model_input_ref: ContentRef,
    },
    /// Runtime-private context for one bounded continuation Run of a durable
    /// standing Goal. It carries exact Goal/Pursuit provenance and immutable
    /// content references without synthesizing a Conversation message.
    StandingGoalContextProjected {
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        objective_ref: ContentRef,
        previous_evaluation_summary_ref: Option<ContentRef>,
        input_refs: Vec<ContentRef>,
    },
    /// Runtime-private input for the built-in Evaluation Agent. Evaluation is
    /// a separately admitted, metered Agent Run whose private Inspection and
    /// Decision Tools cannot enter the user-visible execution capability
    /// catalog. Only its accepted Decision Tool receipt is authoritative.
    StandingGoalJudgeContextProjected {
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        source_run_id: musteragent_identity::RunId,
        judge_input_ref: ContentRef,
    },
    Completed {
        result_refs: Vec<ResourceRef>,
    },
    Failed {
        error: ErrorInfo,
    },
    Cancelled {
        reason: CancelReason,
    },
}

#[cfg(test)]
mod tests {
    use super::ActivityKind;

    #[test]
    fn ordinary_learning_excludes_runtime_private_activities() {
        for kind in [
            ActivityKind::Chat,
            ActivityKind::Coordination,
            ActivityKind::Automation,
            ActivityKind::Delegation,
        ] {
            assert!(kind.is_ordinary_learning_eligible());
        }
        for kind in [ActivityKind::Maintenance, ActivityKind::Evaluation] {
            assert!(!kind.is_ordinary_learning_eligible());
        }
    }
}
