//! Retire an idle Agent through the account owner, preserving its history.
use musteragent_content_ref::Sha256;
use musteragent_identity::{Actor, AgentId, CommandId, UtcTimestamp};
use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy)]
pub struct DeleteOwnedAgent {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub command_id: CommandId,
    pub expected_revision: NonZeroU64,
    pub deleted_at: UtcTimestamp,
}
impl DeleteOwnedAgent {
    pub fn fingerprint(&self) -> Sha256 {
        Sha256::digest(format!(
            "musteragent.agent.delete.v1:{}:{}:{}:{}",
            self.actor.tenant_id, self.actor.principal_id, self.agent_id, self.expected_revision
        ))
    }
}
