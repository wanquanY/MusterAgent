//! Semantic file representations belong to the Artifact format owner.
//! Exact versions and closed dependency identities are mandatory.
use crate::ArtifactToolError;
use musteragent_artifact::{ArtifactKind, ArtifactVersion};
use musteragent_content_ref::ContentRef;
use musteragent_runtime_content::verify_content;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ArtifactFileRepresentation {
    Document(ContentRef),
    Visual(Vec<ContentRef>),
    Unsupported,
}

pub fn artifact_file_representation(
    kind: ArtifactKind,
    version: &ArtifactVersion,
    bytes: &[u8],
) -> Result<ArtifactFileRepresentation, ArtifactToolError> {
    version
        .validate()
        .map_err(|_| ArtifactToolError::InvalidInput)?;
    verify_content(&version.content_ref, bytes).map_err(|_| ArtifactToolError::InvalidInput)?;
    match kind {
        ArtifactKind::File => Ok(ArtifactFileRepresentation::Document(
            version.content_ref.clone(),
        )),
        ArtifactKind::Image => crate::tools::image_file_representation(version, bytes),
        _ => Ok(ArtifactFileRepresentation::Unsupported),
    }
}
