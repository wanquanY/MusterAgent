//! Account-owned preparation identity. It is not an active Agent or a Run.
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, AgentVersionId, CommandId, OwnerSpaceId, PrincipalKind,
    UtcTimestamp, VersionedKey,
};
use musteragent_runtime_contract::ApplicationFuture;

use crate::DialogueStoreError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReserveDialogueDefault {
    pub actor: Actor,
    pub command_id: CommandId,
    pub template_ref: VersionedKey,
    pub template_hash: Sha256,
    pub requested_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerDefaultReservation {
    pub actor: Actor,
    pub command_id: CommandId,
    pub agent_id: AgentId,
    pub agent_version_id: AgentVersionId,
    pub space_id: OwnerSpaceId,
    pub dialogue_id: AgentDialogueId,
    pub template_ref: VersionedKey,
    pub template_hash: Sha256,
    pub prepared_at: UtcTimestamp,
}

impl OwnerDefaultReservation {
    pub fn validate_for(&self, actor: Actor) -> Result<(), DialogueStoreError> {
        if actor.principal_kind != PrincipalKind::User || self.actor != actor {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
}

/// Atomically reserve `(tenant_id, owner_id, purpose=default)` exactly once.
/// The first writer fixes every field, including template and timestamp.
/// Later calls return that complete record even if the transport command or
/// current template changed. Never regenerate IDs on a retry or reclaim the
/// reservation because preparation failed. Content/Registry writes are
/// immutable and independently recoverable; only activation makes it visible.
pub trait OwnerDefaultReservationStore: Send + Sync {
    fn reserve_default(
        &self,
        request: ReserveDialogueDefault,
    ) -> ApplicationFuture<'_, Result<OwnerDefaultReservation, DialogueStoreError>>;
}
