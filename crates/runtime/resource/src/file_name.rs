//! Resolve a file's stable placement before using the normal fenced rename.
//! A file name is Resource metadata, not a document edit or a new Artifact.
use crate::{ResourceCommandContext, ResourceStoreError};
use musteragent_identity::{ArtifactId, ResourceNodeId, ResourceRevision};
use musteragent_runtime_contract::ApplicationFuture;

pub struct ResourceFileNameTarget {
    pub node_id: ResourceNodeId,
    pub revision: ResourceRevision,
}

pub trait ResourceFileNameTargets: Send + Sync {
    fn resolve<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        artifact_id: ArtifactId,
    ) -> ApplicationFuture<'a, Result<ResourceFileNameTarget, ResourceStoreError>>;
}
