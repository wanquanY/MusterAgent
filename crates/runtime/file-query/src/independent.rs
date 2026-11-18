//! Independently imported and host-authored files retain their Artifact identity.
use crate::{FileQueryContext, FileQueryError};
use crate::{IndependentConversationFileSource, MAX_INDEPENDENT_FILE_ARTIFACTS};
use musteragent_artifact::ArtifactOrigin;
use musteragent_projection::ProjectedArtifact;

pub(crate) async fn list(
    context: &FileQueryContext,
    catalog: &dyn IndependentConversationFileSource,
) -> Result<Vec<ProjectedArtifact>, FileQueryError> {
    let artifacts = catalog.snapshot(context).await?;
    if artifacts.len() > MAX_INDEPENDENT_FILE_ARTIFACTS {
        return Err(FileQueryError::Unavailable);
    }
    let mut output = Vec::with_capacity(artifacts.len());
    for item in artifacts {
        let artifact = item.artifact;
        let version = item.current_version;
        if artifact.tenant_id != context.actor.tenant_id
            || artifact.source.conversation_id() != context.conversation_id
            || version.artifact_id != artifact.artifact_id
            || version.version != artifact.current_version
        {
            return Err(FileQueryError::Integrity);
        }
        if matches!(artifact.origin, ArtifactOrigin::Runtime { .. }) {
            continue;
        }
        output.push(ProjectedArtifact {
            artifact_id: artifact.artifact_id,
            activity_id: None,
            created_by_run_id: None,
            updated_by_run_id: None,
            presentation_run_id: None,
            kind: artifact.kind,
            lifecycle: artifact.lifecycle,
            current_version: version.version,
            title: artifact.title,
            current_content_ref: version.content_ref,
            current_dependency_refs: version.dependency_refs,
            updated_at: artifact.updated_at,
        });
    }
    Ok(output)
}
