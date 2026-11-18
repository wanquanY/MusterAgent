//! Execution eligibility is not a Dream failure or control-plane authority.
//! Native foreground retirement releases owned workflow claims without
//! spending the bounded retry budget. It never authorizes model redispatch.

pub trait DreamExecutionOwner: Send + Sync {
    fn is_active(&self) -> bool;
}

/// Existing continuously available Cloud/Device deployments. Foreground-only
/// hosts must supply their account-generation owner instead.
pub struct ContinuousDreamExecutionOwner;

impl DreamExecutionOwner for ContinuousDreamExecutionOwner {
    fn is_active(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DreamExecutionOutcome<T> {
    Completed(T),
    OwnerSuspended,
}
