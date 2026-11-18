//! Bounded, principal-specific activity over the authoritative catalog, not Ledger bodies.
use crate::{CatalogActivityPosition, ConversationCatalogSourceError};
use musteragent_identity::{Actor, ConversationId, PrincipalKind, WorkspaceId};
use musteragent_runtime_contract::ApplicationFuture;

pub const MAX_WORKSPACE_ACTIVITY_BATCH: usize = 100;

#[derive(Debug, Clone)]
pub struct ListWorkspaceCatalogActivity {
    pub actor: Actor,
    pub workspace_ids: Vec<WorkspaceId>,
    /// Presentation scope supplied by the provisioning owner, before aggregation.
    pub excluded_conversation_ids: Vec<ConversationId>,
}
impl ListWorkspaceCatalogActivity {
    pub fn validate(&self) -> Result<(), ConversationCatalogSourceError> {
        let unique: std::collections::HashSet<_> = self.workspace_ids.iter().collect();
        if self.actor.principal_kind != PrincipalKind::User
            || self.workspace_ids.is_empty()
            || self.workspace_ids.len() > MAX_WORKSPACE_ACTIVITY_BATCH
            || unique.len() != self.workspace_ids.len()
            || self.excluded_conversation_ids.len() > 10_000
            || self
                .excluded_conversation_ids
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.excluded_conversation_ids.len()
        {
            return Err(ConversationCatalogSourceError::InvalidQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkspaceCatalogActivity {
    pub workspace_id: WorkspaceId,
    pub active_executions: u64,
    pub action_required: u64,
    pub unread: u64,
    /// Latest terminal outcome still unread by this principal; never lifetime history.
    pub latest_unread_outcome: Option<crate::ExecutionOutcomeKind>,
    /// Missing or bootstrap-fenced projections cannot be represented as zero activity.
    pub pending_projections: u64,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkspaceCatalogActivityBatch {
    pub snapshot_position: CatalogActivityPosition,
    /// Same order as the request. No accessible conversation and an unknown ID
    /// both return zero, so this endpoint is not a workspace existence oracle.
    pub workspaces: Vec<WorkspaceCatalogActivity>,
}
pub trait WorkspaceCatalogActivitySource: Send + Sync {
    fn workspace_activity(
        &self,
        query: ListWorkspaceCatalogActivity,
    ) -> ApplicationFuture<'_, Result<WorkspaceCatalogActivityBatch, ConversationCatalogSourceError>>;
}
