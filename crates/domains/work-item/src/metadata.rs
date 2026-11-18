//! Structural projection of the original core. Persisted/readable independently
//! of erasable objective text; all goal transition logic is shared with Core.
use super::*;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemSpecMetadata {
    pub dependencies: Vec<WorkItemId>,
    pub priority: WorkItemPriority,
    pub follow_up_of: Option<WorkItemId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemCoreMetadata {
    pub work_item_id: WorkItemId,
    pub tenant_id: TenantId,
    pub scope: WorkItemScope,
    pub revisions: WorkItemRevisions,
    pub spec: WorkItemSpecMetadata,
    pub status: WorkItemStatus,
    pub assignment_epoch: Option<AssignmentEpoch>,
}

impl WorkItemCore {
    #[must_use]
    pub fn metadata(&self) -> WorkItemCoreMetadata {
        WorkItemCoreMetadata {
            work_item_id: self.work_item_id,
            tenant_id: self.tenant_id,
            scope: self.scope,
            revisions: self.revisions,
            spec: self.spec.metadata(),
            status: self.status,
            assignment_epoch: self.assignment_epoch,
        }
    }
}

impl WorkItemCoreMetadata {
    /// Business assessment is separate from Run completion and delivery.
    pub fn assess(
        &self,
        goal: WorkItemGoalRevision,
        control: WorkItemControlRevision,
        assignment: AssignmentEpoch,
        accepted: bool,
        failed: bool,
    ) -> Result<Self, WorkItemContractError> {
        self.revisions.check_command(goal, control)?;
        if self.status != WorkItemStatus::Verifying
            || self.assignment_epoch != Some(assignment)
            || (accepted && failed)
        {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        Ok(Self {
            revisions: self.revisions.advance_state(self.revisions.state)?,
            status: if accepted {
                WorkItemStatus::Completed
            } else if failed {
                WorkItemStatus::Blocked
            } else {
                WorkItemStatus::Verifying
            },
            ..self.clone()
        })
    }

    /// Advance the control fence before asking the original Run owner to stop.
    /// Blocked means no execution may proceed; it is not a terminal receipt.
    pub fn request_cancel(
        &self,
        goal: WorkItemGoalRevision,
        control: WorkItemControlRevision,
    ) -> Result<Self, WorkItemContractError> {
        if self.status.is_terminal() {
            return Err(WorkItemContractError::TerminalWorkItem);
        }
        Ok(Self {
            revisions: self.revisions.advance_control(goal, control)?,
            status: WorkItemStatus::Blocked,
            ..self.clone()
        })
    }
    /// Only the control consumer with complete Run quiescence may use this.
    pub fn cancellation_settled(
        &self,
        goal: WorkItemGoalRevision,
        control: WorkItemControlRevision,
    ) -> Result<Self, WorkItemContractError> {
        self.revisions.check_command(goal, control)?;
        if self.status != WorkItemStatus::Blocked {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        Ok(Self {
            revisions: self.revisions.advance_state(self.revisions.state)?,
            status: WorkItemStatus::Cancelled,
            ..self.clone()
        })
    }
    /// Reserve one assignment under the original business revisions. Runtime
    /// admission, not reservation, moves the task to Active.
    pub fn reserve_execution(
        &self,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
        epoch: AssignmentEpoch,
    ) -> Result<Self, WorkItemContractError> {
        self.reserve_assignment(expected_goal, expected_control, epoch, false)
    }
    pub fn reserve_returned_execution(
        &self,
        goal: WorkItemGoalRevision,
        control: WorkItemControlRevision,
        epoch: AssignmentEpoch,
    ) -> Result<Self, WorkItemContractError> {
        if !matches!(
            self.status,
            WorkItemStatus::Verifying | WorkItemStatus::Blocked
        ) || self.assignment_epoch.is_none()
        {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        self.reserve_assignment(goal, control, epoch, true)
    }
    fn reserve_assignment(
        &self,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
        epoch: AssignmentEpoch,
        returned: bool,
    ) -> Result<Self, WorkItemContractError> {
        if self.status.is_terminal() {
            return Err(WorkItemContractError::TerminalWorkItem);
        }
        self.revisions
            .check_command(expected_goal, expected_control)?;
        if (!returned
            && !matches!(
                self.status,
                WorkItemStatus::Queued | WorkItemStatus::Ready | WorkItemStatus::Blocked
            ))
            || epoch.get()
                != self
                    .assignment_epoch
                    .map_or(1, |v| v.get().saturating_add(1))
        {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        Ok(Self {
            revisions: self.revisions.advance_state(self.revisions.state)?,
            status: WorkItemStatus::Ready,
            assignment_epoch: Some(epoch),
            ..self.clone()
        })
    }
    /// Called only after a real independent Run has been staged atomically.
    pub fn execution_admitted(
        &self,
        identity: WorkItemExecutionIdentity,
    ) -> Result<Self, WorkItemContractError> {
        if self.tenant_id != identity.tenant_id
            || self.scope != identity.scope
            || self.work_item_id != identity.work_item_id
        {
            return Err(WorkItemContractError::ScopeMismatch);
        }
        self.revisions
            .check_command(identity.goal_revision, identity.control_revision)?;
        if self.status != WorkItemStatus::Ready
            || self.assignment_epoch != Some(identity.assignment_epoch)
        {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        Ok(Self {
            revisions: self.revisions.advance_state(self.revisions.state)?,
            status: WorkItemStatus::Active,
            ..self.clone()
        })
    }
    pub fn execution_rejected(
        &self,
        goal: WorkItemGoalRevision,
        control: WorkItemControlRevision,
        epoch: AssignmentEpoch,
    ) -> Result<Self, WorkItemContractError> {
        self.revisions.check_command(goal, control)?;
        if self.status != WorkItemStatus::Ready || self.assignment_epoch != Some(epoch) {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        Ok(Self {
            revisions: self.revisions.advance_state(self.revisions.state)?,
            status: WorkItemStatus::Blocked,
            ..self.clone()
        })
    }
    /// Called only for a sealed user-deliverable result of this active device
    /// assignment. A stale terminal cannot finish a cancelled or amended task.
    pub fn device_result_delivered(
        &self,
        goal: WorkItemGoalRevision,
        control: WorkItemControlRevision,
        epoch: AssignmentEpoch,
    ) -> Result<Self, WorkItemContractError> {
        self.revisions.check_command(goal, control)?;
        if self.status != WorkItemStatus::Active || self.assignment_epoch != Some(epoch) {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        Ok(Self {
            status: WorkItemStatus::Completed,
            revisions: self.revisions.advance_state(self.revisions.state)?,
            ..self.clone()
        })
    }
    /// Runtime terminal evidence reaches the original business verification
    /// phase. Success is not acceptance of the task's success criteria.
    pub fn execution_reported(
        &self,
        goal: WorkItemGoalRevision,
        control: WorkItemControlRevision,
        epoch: AssignmentEpoch,
        completed: bool,
        cancelled: bool,
    ) -> Result<Self, WorkItemContractError> {
        self.revisions.check_command(goal, control)?;
        if self.status.is_terminal()
            || self.assignment_epoch != Some(epoch)
            || !matches!(
                self.status,
                WorkItemStatus::Active | WorkItemStatus::Blocked | WorkItemStatus::Verifying
            )
        {
            return Err(WorkItemContractError::AssignmentConflict);
        }
        Ok(Self {
            revisions: self.revisions.advance_state(self.revisions.state)?,
            status: if cancelled {
                WorkItemStatus::Cancelled
            } else if completed {
                WorkItemStatus::Verifying
            } else {
                WorkItemStatus::Blocked
            },
            ..self.clone()
        })
    }
    pub fn amended_goal(
        &self,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
        spec: WorkItemSpec,
    ) -> Result<WorkItemCore, WorkItemContractError> {
        self.spec.validate_contract(self.work_item_id)?;
        if self.status.is_terminal() {
            return Err(WorkItemContractError::TerminalWorkItem);
        }
        spec.validate_contract(self.work_item_id)?;
        let revisions = self
            .revisions
            .advance_goal(expected_goal, expected_control)?;
        Ok(WorkItemCore {
            work_item_id: self.work_item_id,
            tenant_id: self.tenant_id,
            scope: self.scope,
            revisions,
            spec,
            status: self.status,
            assignment_epoch: self.assignment_epoch,
        })
    }
}

impl WorkItemSpec {
    #[must_use]
    pub fn metadata(&self) -> WorkItemSpecMetadata {
        WorkItemSpecMetadata {
            dependencies: self.dependencies.clone(),
            priority: self.priority,
            follow_up_of: self.follow_up_of,
        }
    }
}
impl WorkItemSpecMetadata {
    pub fn validate_contract(&self, work_item_id: WorkItemId) -> Result<(), WorkItemSpecError> {
        if self.dependencies.len() > MAX_DEPENDENCIES {
            return Err(WorkItemSpecError::InvalidDependencies);
        }
        let unique = self.dependencies.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != self.dependencies.len()
            || unique.contains(&work_item_id)
            || self.follow_up_of == Some(work_item_id)
        {
            return Err(WorkItemSpecError::InvalidDependencies);
        }
        Ok(())
    }
}
