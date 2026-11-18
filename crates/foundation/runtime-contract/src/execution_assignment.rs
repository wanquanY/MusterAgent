//! Shared structural execution identities, extracted from the Hosted owner.
//!
//! These facts grant no authority. Product owners keep their typed scope and
//! frozen inputs; the original journal remains the only assignment/Worker writer.
use musteragent_identity::{
    AttemptId, AttemptNumber, DeviceId, ExecutionAssignmentId, ExecutorInstanceId, FencingToken,
    LeaseId, PrincipalId, RunAttemptEpoch, RunId, VersionNumber,
};

/// Registered process identity. Environment is a declaration, not attestation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorBinding {
    pub device_id: DeviceId,
    pub owner_principal_id: PrincipalId,
    pub instance_id: ExecutorInstanceId,
    pub runtime_generation: FencingToken,
    pub registration_revision: VersionNumber,
    pub environment_spec_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionAttemptIdentity {
    pub assignment_id: ExecutionAssignmentId,
    pub run_id: RunId,
    /// Initial Worker anchor; legitimate continuations bind later Workers.
    pub attempt_id: AttemptId,
    pub run_attempt_epoch: RunAttemptEpoch,
}
impl ExecutionAttemptIdentity {
    pub fn check_successor(self, previous: Self) -> Result<(), ExecutionBindingError> {
        if self.run_id != previous.run_id {
            return Err(ExecutionBindingError::Run);
        }
        if previous.run_attempt_epoch.get().checked_add(1) != Some(self.run_attempt_epoch.get())
            || self.assignment_id == previous.assignment_id
            || self.attempt_id == previous.attempt_id
        {
            return Err(ExecutionBindingError::Generation);
        }
        Ok(())
    }
}

/// Scope is a product-owned type, never an optional/fabricated Project identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionAssignmentAnchor<S> {
    pub scope: S,
    pub attempt: ExecutionAttemptIdentity,
    pub executor: ExecutorBinding,
}
impl<S: PartialEq> ExecutionAssignmentAnchor<S> {
    pub fn check_current(&self, current: &Self) -> Result<(), ExecutionBindingError> {
        if self.scope != current.scope || self.attempt != current.attempt {
            return Err(ExecutionBindingError::StaleAttempt);
        }
        if self.executor != current.executor {
            return Err(ExecutionBindingError::ExecutorChanged);
        }
        Ok(())
    }
}

/// Only immutable identity projection, not an authorization implementation.
/// The digest must come from the product's canonical, scope-separated codec.
pub trait ExecutionAllocationBinding {
    fn assignment_digest(&self) -> [u8; 32];
    fn initial_attempt_id(&self) -> AttemptId;
}

/// Current core Worker, distinct from the immutable device allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionWorkerBinding {
    pub assignment_digest: [u8; 32],
    pub attempt_id: AttemptId,
    pub attempt_number: AttemptNumber,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
}
impl ExecutionWorkerBinding {
    pub fn validate(self) -> Result<(), ExecutionWorkerBindingError> {
        if self.assignment_digest == [0; 32] {
            Err(ExecutionWorkerBindingError::Assignment)
        } else {
            Ok(())
        }
    }
    pub fn check_assignment(
        self,
        allocation: &(impl ExecutionAllocationBinding + ?Sized),
    ) -> Result<(), ExecutionWorkerBindingError> {
        self.validate()?;
        if self.assignment_digest != allocation.assignment_digest() {
            return Err(ExecutionWorkerBindingError::Assignment);
        }
        Ok(())
    }
    /// The sole writer also checks first-claim state and current core counters.
    pub fn check_initial(
        self,
        allocation: &(impl ExecutionAllocationBinding + ?Sized),
    ) -> Result<(), ExecutionWorkerBindingError> {
        self.check_assignment(allocation)?;
        if self.attempt_id != allocation.initial_attempt_id() {
            return Err(ExecutionWorkerBindingError::InitialAttempt);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExecutionWorkerBindingError {
    #[error("Worker belongs to a different execution assignment")]
    Assignment,
    #[error("initial Worker does not use the allocation's Attempt anchor")]
    InitialAttempt,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExecutionBindingError {
    #[error("execution recovery changed the original Run")]
    Run,
    #[error("execution recovery requires the next epoch and fresh assignment and attempt")]
    Generation,
    #[error("execution assignment is no longer current")]
    StaleAttempt,
    #[error("execution device instance or registration changed")]
    ExecutorChanged,
}
