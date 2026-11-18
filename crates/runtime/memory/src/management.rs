use musteragent_identity::{
    AgentId, DottedKey, IdempotencyKey, MemoryId, MemoryVersionId, PrincipalId, TenantId,
    UtcTimestamp,
};
use musteragent_runtime_contract::ApplicationFuture;

/// A CAS-governed request to stop one Agent-scoped Memory from participating
/// in future retrievals.
///
/// Revocation is deliberately distinct from privacy erasure: it changes the
/// production lifecycle immediately while retaining immutable provenance.
/// Permanent deletion must continue through the privacy-erasure aggregate and
/// produce an ErasureReceipt.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RevokeAgentMemory {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub memory_id: MemoryId,
    pub expected_memory_version_id: MemoryVersionId,
    pub reason_code: DottedKey,
    pub idempotency_key: IdempotencyKey,
    pub revoked_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AgentMemoryLifecycleDisposition {
    Applied,
    Duplicate,
}

/// Immutable acknowledgement of the exact lifecycle mutation that won.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryRevocationReceipt {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
    pub reason_code: DottedKey,
    pub idempotency_key: IdempotencyKey,
    pub revoked_at: UtcTimestamp,
}

impl AgentMemoryRevocationReceipt {
    #[must_use]
    pub fn matches(&self, command: &RevokeAgentMemory) -> bool {
        self.tenant_id == command.tenant_id
            && self.actor_principal_id == command.actor_principal_id
            && self.agent_id == command.agent_id
            && self.memory_id == command.memory_id
            && self.memory_version_id == command.expected_memory_version_id
            && self.reason_code == command.reason_code
            && self.idempotency_key == command.idempotency_key
            && self.revoked_at == command.revoked_at
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryLifecycleMutation {
    pub disposition: AgentMemoryLifecycleDisposition,
    pub receipt: AgentMemoryRevocationReceipt,
}

/// Dedicated lifecycle boundary for the Learning management plane.
///
/// This is intentionally not added to the broad MemoryStore port: read/write
/// execution dependencies should not automatically gain governance authority.
pub trait AgentMemoryLifecycleStore: Send + Sync {
    type Error: Send;

    fn revoke_agent_memory(
        &self,
        command: RevokeAgentMemory,
    ) -> ApplicationFuture<'_, Result<AgentMemoryLifecycleMutation, Self::Error>>;
}
