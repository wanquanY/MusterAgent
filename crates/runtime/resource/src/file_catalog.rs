//! Metadata-only file views owned by the existing Resource store.
use crate::{ProjectFileVersion, ResourceCommandContext, ResourceStoreError};
use musteragent_content_ref::Sha256;
use musteragent_identity::{ResourceNodeId, ResourceRevision, UtcTimestamp};
use musteragent_resource::{ResourceNode, ResourceScope};
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Clone, Debug)]
pub struct ProjectFileCatalogEntry {
    pub production: Option<crate::AgentFileProduction>,
    pub node: ResourceNode,
    /// Folders have no version. A file may have an unavailable backing output
    /// (for example a delivery projection still processing); keep its node in
    /// the list with explicit unavailable status, never invent file content.
    pub file: Option<ProjectFileVersion>,
    pub file_updated_at: Option<UtcTimestamp>,
}

#[derive(Clone, Debug)]
pub struct ListProjectFiles {
    pub produced_by: Option<crate::FileProductionFilter>,
    /// False lists immediate children; true queries the original subtree.
    pub recursive: bool,
    pub context: ResourceCommandContext,
    pub parent_node_id: Option<ResourceNodeId>,
    pub offset: usize,
    pub limit: usize,
    pub name_query: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ProjectFileCatalogPage {
    pub scope: ResourceScope,
    pub revision: ResourceRevision,
    /// Complete queried directory metadata fingerprint, including backing file
    /// versions. Tree revision alone does not advance for every file edit.
    pub content_revision: Sha256,
    pub entries: Vec<ProjectFileCatalogEntry>,
    pub next_offset: Option<usize>,
}

/// Return node, file version and scope revision in one consistent storage
/// snapshot. Implementations authorize current membership and placement in
/// that snapshot and batch version metadata; no per-file body/DB round trips.
pub trait ProjectFileCatalogStore: Send + Sync {
    /// Indexed lookup of the original Artifact placement within this authorized
    /// scope. A conversation result is a view of that file, not a detached copy.
    fn resolve_artifact_placement<'a>(
        &'a self,
        _context: &'a ResourceCommandContext,
        _artifact_id: musteragent_identity::ArtifactId,
    ) -> ApplicationFuture<'a, Result<Option<ProjectFileCatalogEntry>, ResourceStoreError>> {
        Box::pin(async { Ok(None) })
    }
    /// One bounded complete subtree at the original Resource read snapshot.
    fn snapshot_files(
        &self,
        _query: ListProjectFiles,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>> {
        Box::pin(async { Err(ResourceStoreError::BoundsExceeded) })
    }

    fn list_files(
        &self,
        query: ListProjectFiles,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>>;
    fn resolve_file<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        node_id: ResourceNodeId,
    ) -> ApplicationFuture<'a, Result<ProjectFileCatalogEntry, ResourceStoreError>>;
}
