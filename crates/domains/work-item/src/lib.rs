//! Scope-independent WorkItem contracts. These facts do not grant authority,
//! schedule execution, or own Run/Attempt terminal transitions.

use std::collections::BTreeSet;

use musteragent_identity::{AssignmentEpoch, TenantId, WorkItemId};
use thiserror::Error;

mod admission;
mod identity;
pub use admission::{
    SignedWorkItemAdmissionRejection, WorkItemAdmissionRejectionClaims,
    WorkItemAdmissionRejectionReason,
};
mod metadata;
mod revision;
pub use metadata::{WorkItemCoreMetadata, WorkItemSpecMetadata};

pub use identity::{
    OwnedAgentWorkItemScope, WorkItemAgentReceiver, WorkItemAssignee, WorkItemExecutionIdentity,
    WorkItemReportFence, WorkItemReturnTarget, WorkItemScope,
};
pub use revision::{
    WorkItemControlRevision, WorkItemGoalRevision, WorkItemRevisionError, WorkItemRevisions,
    WorkItemStateRevision,
};

const MAX_OBJECTIVE_CHARS: usize = 8_000;
const MAX_CRITERION_CHARS: usize = 2_000;
const MAX_EVIDENCE_BOUNDARY_CHARS: usize = 4_000;
const MAX_SUCCESS_CRITERIA: usize = 100;
const MAX_DEPENDENCIES: usize = 1_000;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum WorkItemPriority {
    Low,
    Normal,
    High,
    Critical,
}

/// The original WorkItem status vocabulary, shared by both scopes. No separate
/// Owner-scoped task state machine is introduced.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum WorkItemStatus {
    Queued,
    Ready,
    Active,
    Blocked,
    Verifying,
    Completed,
    Superseded,
    Cancelled,
}

impl WorkItemStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Superseded | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemSpec {
    pub objective: String,
    pub success_criteria: Vec<String>,
    pub dependencies: Vec<WorkItemId>,
    pub priority: WorkItemPriority,
    pub evidence_boundary: String,
    pub follow_up_of: Option<WorkItemId>,
}

impl WorkItemSpec {
    /// Validates the immutable objective and dependency contract.
    pub fn validate_contract(&self, work_item_id: WorkItemId) -> Result<(), WorkItemSpecError> {
        validate_text(&self.objective, MAX_OBJECTIVE_CHARS)?;
        validate_text(&self.evidence_boundary, MAX_EVIDENCE_BOUNDARY_CHARS)?;
        if self.success_criteria.is_empty()
            || self.success_criteria.len() > MAX_SUCCESS_CRITERIA
            || self
                .success_criteria
                .iter()
                .any(|criterion| validate_text(criterion, MAX_CRITERION_CHARS).is_err())
        {
            return Err(WorkItemSpecError::InvalidSuccessCriteria);
        }
        self.metadata().validate_contract(work_item_id)?;
        Ok(())
    }
}

fn validate_text(value: &str, max_chars: usize) -> Result<(), WorkItemSpecError> {
    if value.trim().is_empty()
        || value.chars().count() > max_chars
        || value.chars().any(|character| character == '\0')
    {
        Err(WorkItemSpecError::InvalidText)
    } else {
        Ok(())
    }
}

/// Durable business facts independent of the executing Run.
///
/// its codecs and revision cannot be replaced with this type implicitly.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemCore {
    pub work_item_id: WorkItemId,
    pub tenant_id: TenantId,
    pub scope: WorkItemScope,
    pub revisions: WorkItemRevisions,
    pub spec: WorkItemSpec,
    pub status: WorkItemStatus,
    pub assignment_epoch: Option<AssignmentEpoch>,
}

impl WorkItemCore {
    pub fn validate(&self) -> Result<(), WorkItemContractError> {
        self.spec.validate_contract(self.work_item_id)?;
        Ok(())
    }

    /// Computes a candidate under exact goal/control fences. The authoritative
    /// store must atomically recheck and persist it with its receipt/outbox.
    /// Changing a goal invalidates the prior execution's goal fence, without
    /// inventing a new task, Run, assignment, or completion state.
    pub fn amended_goal(
        &self,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
        spec: WorkItemSpec,
    ) -> Result<Self, WorkItemContractError> {
        self.validate()?;
        self.metadata()
            .amended_goal(expected_goal, expected_control, spec)
    }

    /// Checks the supplied binding against current authority facts. It neither
    /// validates a signed grant nor establishes that the executor is permitted
    /// to access resources: the admission owner must perform those checks.
    pub fn validate_execution(
        &self,
        execution: &WorkItemExecutionIdentity,
    ) -> Result<(), WorkItemContractError> {
        self.require_open()?;
        if execution.work_item_id != self.work_item_id
            || execution.tenant_id != self.tenant_id
            || execution.scope != self.scope
        {
            return Err(WorkItemContractError::ScopeMismatch);
        }
        if execution.goal_revision != self.revisions.goal {
            return Err(WorkItemRevisionError::GoalConflict.into());
        }
        if execution.control_revision != self.revisions.control {
            return Err(WorkItemRevisionError::ControlConflict.into());
        }
        if self.assignment_epoch != Some(execution.assignment_epoch) {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        execution.validate_route()?;
        Ok(())
    }

    /// Only determines whether evidence belongs to the current execution and
    /// target. Acceptance does not complete the WorkItem or publish a message.
    /// Rejected late evidence may still be archived by the original report owner.
    pub fn validate_report(
        &self,
        execution: &WorkItemExecutionIdentity,
        report: &WorkItemReportFence,
    ) -> Result<(), WorkItemContractError> {
        self.validate_execution(execution)?;
        if report != &execution.report_fence() {
            return Err(WorkItemContractError::ReportMismatch);
        }
        Ok(())
    }

    fn require_open(&self) -> Result<(), WorkItemContractError> {
        self.validate()?;
        if self.status.is_terminal() {
            return Err(WorkItemContractError::TerminalWorkItem);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum WorkItemSpecError {
    #[error("Work Item text is invalid")]
    InvalidText,
    #[error("Work Item success criteria are invalid")]
    InvalidSuccessCriteria,
    #[error("Work Item dependencies are invalid")]
    InvalidDependencies,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum WorkItemContractError {
    #[error(transparent)]
    Spec(#[from] WorkItemSpecError),
    #[error(transparent)]
    Revision(#[from] WorkItemRevisionError),
    #[error("Work Item scope does not match")]
    ScopeMismatch,
    #[error("Work Item assignment is stale or absent")]
    AssignmentConflict,
    #[error("Work Item return target does not match its business owner")]
    ReturnTargetMismatch,
    #[error("Work Item assignee does not match its scope")]
    AssigneeMismatch,
    #[error("Work Item report does not match its exact execution")]
    ReportMismatch,
    #[error("terminal Work Item cannot accept new execution or goal changes")]
    TerminalWorkItem,
}

#[cfg(test)]
mod tests;

mod input_action;
pub use input_action::*;

mod agent_message_route;
pub use agent_message_route::*;
