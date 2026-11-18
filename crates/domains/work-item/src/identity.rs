//! Business ownership, executing identity and stable return routing are distinct.
//! These types describe facts already authorized by an admission owner. Their
//! validators check consistency, not account membership or signed capabilities.

use musteragent_identity::{
    AgentDialogueId, AgentId, AgentVersionId, AssignmentEpoch, AssignmentId, OwnerSpaceId,
    PrincipalId, RunId, TenantId, WorkItemId,
};

use super::{WorkItemContractError, WorkItemControlRevision, WorkItemGoalRevision};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OwnedAgentWorkItemScope {
    pub space_id: OwnerSpaceId,
    pub owner_id: PrincipalId,
    /// The requesting/business-owning Agent, never rewritten to the executor.
    pub agent_id: AgentId,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum WorkItemScope {
    OwnedAgent(OwnedAgentWorkItemScope),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WorkItemAssignee {
    SelfAgent {
        published_version_id: AgentVersionId,
    },
    DelegatedAgent {
        agent_id: AgentId,
        published_version_id: AgentVersionId,
    },
    EphemeralWorker {
        assignment_id: AssignmentId,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct WorkItemAgentReceiver {
    pub tenant_id: TenantId,
    pub scope: OwnedAgentWorkItemScope,
    /// The authority resolves and validates this Agent's public Dialogue.
    /// A private coordinating Conversation/Run is deliberately absent.
    pub dialogue_id: AgentDialogueId,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WorkItemReturnTarget {
    OwnedAgent(WorkItemAgentReceiver),
}

/// The original binding owner supplies this exact execution identity. The
/// source Run proves which execution produced evidence; it does not determine
/// the WorkItem's lifetime or act as the Agent's result inbox.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct WorkItemExecutionIdentity {
    pub tenant_id: TenantId,
    pub scope: WorkItemScope,
    pub work_item_id: WorkItemId,
    pub goal_revision: WorkItemGoalRevision,
    pub control_revision: WorkItemControlRevision,
    pub assignment_epoch: AssignmentEpoch,
    pub source_run_id: RunId,
    pub assignee: WorkItemAssignee,
    pub return_target: WorkItemReturnTarget,
}

impl WorkItemExecutionIdentity {
    pub(super) fn validate_route(&self) -> Result<(), WorkItemContractError> {
        match (self.scope, self.return_target) {
            (WorkItemScope::OwnedAgent(scope), WorkItemReturnTarget::OwnedAgent(receiver))
                if receiver.tenant_id == self.tenant_id && receiver.scope == scope => {}
            _ => return Err(WorkItemContractError::ReturnTargetMismatch),
        }
        match (self.scope, self.assignee) {
            (_, WorkItemAssignee::EphemeralWorker { .. })
            | (WorkItemScope::OwnedAgent(_), WorkItemAssignee::SelfAgent { .. }) => Ok(()),
            (
                WorkItemScope::OwnedAgent(scope),
                WorkItemAssignee::DelegatedAgent { agent_id, .. },
            ) if agent_id != scope.agent_id => Ok(()),
            _ => Err(WorkItemContractError::AssigneeMismatch),
        }
    }

    #[must_use]
    pub const fn report_fence(self) -> WorkItemReportFence {
        WorkItemReportFence {
            tenant_id: self.tenant_id,
            scope: self.scope,
            work_item_id: self.work_item_id,
            goal_revision: self.goal_revision,
            control_revision: self.control_revision,
            assignment_epoch: self.assignment_epoch,
            source_run_id: self.source_run_id,
        }
    }
}

/// Exact evidence provenance, not a caller-selected delivery destination.
/// Return routing is loaded from the accepted execution binding, never a report.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct WorkItemReportFence {
    pub tenant_id: TenantId,
    pub scope: WorkItemScope,
    pub work_item_id: WorkItemId,
    pub goal_revision: WorkItemGoalRevision,
    pub control_revision: WorkItemControlRevision,
    pub assignment_epoch: AssignmentEpoch,
    pub source_run_id: RunId,
}
