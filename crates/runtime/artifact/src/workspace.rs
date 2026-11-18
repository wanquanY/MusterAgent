//! File reads authorized by their current Workspace resource placement.
use crate::ArtifactView;
use musteragent_identity::{Actor, ArtifactId, WorkspaceId};
use musteragent_runtime_contract::{ApplicationFuture, RuntimeError};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct GetWorkspaceFileArtifact {
    pub actor: Actor,
    pub workspace_id: WorkspaceId,
    pub artifact_id: ArtifactId,
}

/// Trusted Cloud product adapter only: current product Workspace ownership must
/// be checked before dispatch. Implementations additionally check the real root
/// owner and current active File node placement in the same read snapshot.
/// Immutable Artifact source is provenance, not current placement authority.
pub trait WorkspaceFileArtifactApplication: Send + Sync {
    fn get_workspace_file_artifact(
        &self,
        query: GetWorkspaceFileArtifact,
    ) -> ApplicationFuture<'_, Result<ArtifactView, RuntimeError>>;
}
