use musteragent_identity::{
    Actor, AgentId, ArtifactId, ConversationId, ExecutionWorkspaceId, InvocationId, ResourceNodeId,
    ResourceRevision, TenantId, UtcTimestamp, VersionedKey, WorkspaceId,
};
use thiserror::Error;

use crate::ResourceName;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ResourceScope {
    Project(WorkspaceId),
    Conversation(ConversationId),
    /// Durable origin_device workspace, independent of per-intent execution contexts.
    OwnedAgent(AgentId),
    /// A private working tree owned by one Conversation and effective branch.
    /// Project membership never turns this into a shared project scope.
    ExecutionWorkspace(ExecutionWorkspaceId),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ResourceNodeKind {
    Folder,
    File,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ResourceNodeLifecycle {
    Active,
    Trashed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceProvenance {
    pub actor: Actor,
    pub conversation_id: Option<ConversationId>,
    pub invocation_id: Option<InvocationId>,
    pub tool_key: Option<VersionedKey>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceNode {
    pub is_result: bool,
    pub node_id: ResourceNodeId,
    pub tenant_id: TenantId,
    pub scope: ResourceScope,
    pub parent_node_id: Option<ResourceNodeId>,
    pub original_parent_node_id: Option<ResourceNodeId>,
    pub name: ResourceName,
    pub kind: ResourceNodeKind,
    pub lifecycle: ResourceNodeLifecycle,
    pub file_artifact_id: Option<ArtifactId>,
    pub sort_key: String,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub revision: ResourceRevision,
    pub provenance: ResourceProvenance,
}

impl ResourceNode {
    pub fn validate(&self) -> Result<(), ResourceTreeError> {
        if self.sort_key.is_empty() || self.sort_key.len() > 512 {
            return Err(ResourceTreeError::InvalidSortKey);
        }
        match (self.kind, self.file_artifact_id) {
            (ResourceNodeKind::Folder, None) | (ResourceNodeKind::File, Some(_)) => {}
            _ => return Err(ResourceTreeError::InvalidFileBinding),
        }
        if self.lifecycle == ResourceNodeLifecycle::Active && self.original_parent_node_id.is_some()
        {
            Err(ResourceTreeError::InvalidTrashState)
        } else {
            // A trashed root child legitimately has no original parent. The
            // lifecycle distinguishes it from an active root child.
            Ok(())
        }
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ResourceTreeError {
    #[error("Folder/File binding is invalid")]
    InvalidFileBinding,
    #[error("Resource trash state is invalid")]
    InvalidTrashState,
    #[error("Resource sort key is invalid")]
    InvalidSortKey,
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{
        Actor, ArtifactId, ConversationId, PrincipalId, PrincipalKind, ResourceNodeId,
        ResourceRevision, TenantId, UtcTimestamp,
    };

    use super::{
        ResourceName, ResourceNode, ResourceNodeKind, ResourceNodeLifecycle, ResourceProvenance,
        ResourceScope, ResourceTreeError,
    };

    fn node(kind: ResourceNodeKind, file_artifact_id: Option<ArtifactId>) -> ResourceNode {
        let tenant_id = TenantId::generate();
        ResourceNode {
            is_result: false,
            node_id: ResourceNodeId::generate(),
            tenant_id,
            scope: ResourceScope::Conversation(ConversationId::generate()),
            parent_node_id: None,
            original_parent_node_id: None,
            name: ResourceName::new("main.py").expect("name"),
            kind,
            lifecycle: ResourceNodeLifecycle::Active,
            file_artifact_id,
            sort_key: "main.py".to_owned(),
            created_at: UtcTimestamp::from_unix_millis(1),
            updated_at: UtcTimestamp::from_unix_millis(1),
            revision: ResourceRevision::new(1),
            provenance: ResourceProvenance {
                actor: Actor {
                    tenant_id,
                    principal_id: PrincipalId::generate(),
                    principal_kind: PrincipalKind::User,
                },
                conversation_id: None,
                invocation_id: None,
                tool_key: None,
            },
        }
    }

    #[test]
    fn folder_and_file_bindings_are_closed() {
        assert_eq!(node(ResourceNodeKind::Folder, None).validate(), Ok(()));
        assert_eq!(
            node(ResourceNodeKind::File, Some(ArtifactId::generate())).validate(),
            Ok(())
        );
        assert_eq!(
            node(ResourceNodeKind::Folder, Some(ArtifactId::generate())).validate(),
            Err(ResourceTreeError::InvalidFileBinding)
        );
        assert_eq!(
            node(ResourceNodeKind::File, None).validate(),
            Err(ResourceTreeError::InvalidFileBinding)
        );
    }
}
