//! Cloud Workspace root registration, independent of Conversation lifetime.
//!
//! Only the authenticated Backend control plane may call this port, after
//! resolving an active Workspace owned by the actor in the actor's tenant.
//! The receipt proves durable root existence, not ongoing access or admission.
//! Callers must still check current Workspace and execution authority.

use musteragent_identity::{
    Actor, PrincipalId, ResourceNodeId, TenantId, UtcTimestamp, WorkspaceId,
};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub struct EnsureWorkspaceResourceRoot {
    pub actor: Actor,
    pub workspace_id: WorkspaceId,
}

/// Immutable receipt. Retrying registration never changes this value, including
/// after project files change. The root is the scope head, not a synthetic node.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkspaceResourceRootReceipt {
    pub tenant_id: TenantId,
    pub workspace_id: WorkspaceId,
    pub owner_principal_id: PrincipalId,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum WorkspaceResourceRootError {
    #[error("Workspace root registration requires an authenticated user owner")]
    InvalidOwner,
    #[error("Workspace resource query is invalid")]
    InvalidQuery,
    #[error("Workspace root identity conflicts with its registered owner")]
    IdentityConflict,
    #[error("Workspace root persistence is unavailable")]
    DependencyUnavailable,
    #[error("Workspace root receipt is invalid")]
    Corrupt,
}

/// Trusted Backend read after current Workspace ownership validation.
#[derive(Debug, Clone, Copy)]
pub struct ListWorkspaceResourceChildren {
    pub actor: Actor,
    pub workspace_id: WorkspaceId,
    pub parent_node_id: Option<ResourceNodeId>,
    pub include_trashed: bool,
    pub offset: usize,
    pub limit: usize,
}

pub trait WorkspaceResourceRootAuthority: Send + Sync {
    fn list_children(
        &self,
        request: ListWorkspaceResourceChildren,
    ) -> ApplicationFuture<'_, Result<crate::ResourceNodePage, WorkspaceResourceRootError>>;

    fn ensure_root(
        &self,
        request: EnsureWorkspaceResourceRoot,
    ) -> ApplicationFuture<'_, Result<WorkspaceResourceRootReceipt, WorkspaceResourceRootError>>;
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use crate::{ResourceCommandContext, ResourceCommandSource};
    use musteragent_identity::{InvocationId, PrincipalKind};

    #[test]
    fn workspace_source_is_explicit_and_cannot_impersonate_a_tool() {
        let tenant_id = TenantId::generate();
        let workspace_id = WorkspaceId::generate();
        let mut context = ResourceCommandContext {
            tenant_id,
            actor: Actor {
                tenant_id,
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            source: ResourceCommandSource::Workspace { workspace_id },
            invocation_id: None,
            tool_key: None,
            now: UtcTimestamp::from_unix_millis(1),
        };
        assert!(context.validate().is_ok());
        assert_eq!(context.conversation_id(), None);
        assert_eq!(
            context.scope(),
            musteragent_resource::ResourceScope::Project(workspace_id)
        );
        context.invocation_id = Some(InvocationId::generate());
        assert!(context.validate().is_err());
        context.invocation_id = None;
        context.actor.principal_kind = PrincipalKind::System;
        assert!(context.validate().is_err());
        context.actor.principal_kind = PrincipalKind::User;
        context.actor.tenant_id = TenantId::generate();
        assert!(context.validate().is_err());
    }
}
