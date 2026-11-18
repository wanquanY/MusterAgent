//! Minimal source authority pins supplied by the host's source owner.
use musteragent_identity::{AgentId, OwnerApplicationBindingId, PrincipalId, TenantId};
use std::num::NonZeroU64;

/// Exact account and Agent to which a source observation may be delivered.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct GoalSourceSubject {
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub agent_id: AgentId,
}
/// Source revision and revocation epoch checked again when admitting an input.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct GoalSourceBindingGuard {
    pub revision: NonZeroU64,
    pub authority_epoch: NonZeroU64,
}
/// Whether this source binding currently authorizes new observations.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GoalSourceBindingStatus {
    Enabled,
    Disabled,
    Removed,
}
/// Host-authenticated source grant; contains no marketplace or install state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalSourceBinding {
    pub binding_id: OwnerApplicationBindingId,
    pub subject: GoalSourceSubject,
    pub guard: GoalSourceBindingGuard,
    pub status: GoalSourceBindingStatus,
}
