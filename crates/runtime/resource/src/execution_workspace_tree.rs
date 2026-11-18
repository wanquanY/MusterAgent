//! Bounded complete metadata view for materializing a private working tree.
//! This is a read of Resource, not another persistence/snapshot authority.
use crate::{
    ExecutionWorkspaceBinding, ProjectFileCatalogEntry, ResourceCommandContext,
    ResourceCommandSource, ResourceStoreError,
};
use musteragent_identity::{ResourceNodeId, ResourceRevision};
use musteragent_resource::ResourceNodeKind;
use musteragent_runtime_contract::ApplicationFuture;
use std::collections::{BTreeMap, BTreeSet};

/// Metadata admission bound, not a promise of Browser execution capacity.
pub const MAX_EXECUTION_WORKSPACE_TREE_ENTRIES: usize = 10_000;

#[derive(Debug, Clone)]
pub struct ReadExecutionWorkspaceTree {
    pub context: ResourceCommandContext,
    pub maximum_entries: usize,
    pub maximum_total_bytes: u64,
}
impl ReadExecutionWorkspaceTree {
    pub fn binding(&self) -> Result<ExecutionWorkspaceBinding, ResourceStoreError> {
        self.context
            .validate()
            .map_err(|_| ResourceStoreError::NodeUnavailable)?;
        if self.maximum_entries == 0 || self.maximum_entries > MAX_EXECUTION_WORKSPACE_TREE_ENTRIES
        {
            return Err(ResourceStoreError::BoundsExceeded);
        }
        match self.context.source {
            ResourceCommandSource::ExecutionWorkspace(binding) => Ok(binding),
            _ => Err(ResourceStoreError::NodeUnavailable),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionWorkspaceTreeEntry {
    /// Canonical path relative to /workspace/work; includes empty directories.
    pub path: String,
    pub resource: ProjectFileCatalogEntry,
}
#[derive(Debug, Clone)]
pub struct ExecutionWorkspaceTree {
    pub binding: ExecutionWorkspaceBinding,
    /// Structural CAS. Each file additionally carries its exact Artifact version.
    pub revision: ResourceRevision,
    pub total_bytes: u64,
    pub entries: Vec<ExecutionWorkspaceTreeEntry>,
}
impl ExecutionWorkspaceTree {
    pub fn from_catalog(
        request: &ReadExecutionWorkspaceTree,
        revision: ResourceRevision,
        entries: Vec<ProjectFileCatalogEntry>,
    ) -> Result<Self, ResourceStoreError> {
        let binding = request.binding()?;
        let (total_bytes, entries) = catalog_tree(
            &request.context,
            None,
            request.maximum_entries,
            request.maximum_total_bytes,
            entries,
        )?;
        Ok(Self {
            binding,
            revision,
            total_bytes,
            entries,
        })
    }
}

/// Complete metadata traversal shared by private work and selected project subtrees.
pub fn catalog_tree(
    context: &ResourceCommandContext,
    root_node_id: Option<ResourceNodeId>,
    maximum_entries: usize,
    maximum_total_bytes: u64,
    entries: Vec<ProjectFileCatalogEntry>,
) -> Result<(u64, Vec<ExecutionWorkspaceTreeEntry>), ResourceStoreError> {
    if entries.len() > maximum_entries {
        return Err(ResourceStoreError::BoundsExceeded);
    }
    let mut nodes = BTreeMap::new();
    let mut total_bytes: u64 = 0;
    let mut sibling_names = BTreeSet::new();
    for entry in &entries {
        if entry.node.validate().is_err()
            || entry.node.lifecycle != musteragent_resource::ResourceNodeLifecycle::Active
            || entry.node.tenant_id != context.tenant_id
            || entry.node.scope != context.scope()
            || nodes.insert(entry.node.node_id, &entry.node).is_some()
            || !sibling_names.insert((entry.node.parent_node_id, entry.node.name.comparison_key()))
        {
            return Err(ResourceStoreError::Corrupt);
        }
        match (entry.node.kind, &entry.file) {
            (ResourceNodeKind::Folder, None) => {}
            (ResourceNodeKind::File, Some(file))
                if file.node_id == entry.node.node_id
                    && Some(file.artifact_id) == entry.node.file_artifact_id
                    && file.name == entry.node.name.as_str() =>
            {
                total_bytes = total_bytes
                    .checked_add(file.content_ref.byte_length)
                    .filter(|v| *v <= maximum_total_bytes)
                    .ok_or(ResourceStoreError::BoundsExceeded)?;
            }
            _ => return Err(ResourceStoreError::FileUnavailable),
        }
    }
    let mut paths = BTreeMap::<ResourceNodeId, String>::new();
    for entry in &entries {
        let mut current = Some(entry.node.node_id);
        let mut chain = Vec::new();
        let mut visiting = BTreeSet::new();
        while let Some(id) = current {
            if !visiting.insert(id) {
                return Err(ResourceStoreError::Corrupt);
            }
            let node = nodes.get(&id).ok_or(ResourceStoreError::Corrupt)?;
            if id != entry.node.node_id && node.kind != ResourceNodeKind::Folder {
                return Err(ResourceStoreError::Corrupt);
            }
            if paths.contains_key(&id) {
                break;
            }
            chain.push(id);
            current = node.parent_node_id.filter(|id| Some(*id) != root_node_id);
        }
        let mut prefix = current
            .and_then(|id| paths.get(&id))
            .cloned()
            .unwrap_or_default();
        for id in chain.into_iter().rev() {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(nodes[&id].name.as_str());
            // Bound aggregate path material before allocating an adversarial
            // deep tree; selected ranges must be fully representable.
            if prefix.len() > 4096 {
                return Err(ResourceStoreError::BoundsExceeded);
            }
            paths.insert(id, prefix.clone());
        }
    }
    let mut entries = entries
        .into_iter()
        .map(|resource| {
            Ok(ExecutionWorkspaceTreeEntry {
                path: paths
                    .remove(&resource.node.node_id)
                    .ok_or(ResourceStoreError::Corrupt)?,
                resource,
            })
        })
        .collect::<Result<Vec<_>, ResourceStoreError>>()?;
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok((total_bytes, entries))
}

pub trait ExecutionWorkspaceTreeStore: Send + Sync {
    /// One metadata snapshot, complete or an error; never a truncated tree and
    /// never a body read. A durable execution snapshot must pin these versions.
    fn read_tree(
        &self,
        request: ReadExecutionWorkspaceTree,
    ) -> ApplicationFuture<'_, Result<ExecutionWorkspaceTree, ResourceStoreError>>;
}

#[cfg(test)]
#[path = "execution_workspace_tree_tests.rs"]
mod tests;
