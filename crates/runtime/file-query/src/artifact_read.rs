use crate::{
    ContentFileReader, FileContentReader, FileQueryContext, FileQueryError, FileReadResult,
    FileReadView, FileRepresentation, FileSource, ReadFile, ResolvedFile,
    content::MAX_FILE_SOURCE_BYTES,
};
use musteragent_content_ref::ContentRef;
use musteragent_runtime_artifact::{
    ArtifactFileRepresentation, GetArtifact, artifact_file_representation,
};
use musteragent_runtime_content::{ContentStoreError, verify_content};

impl ContentFileReader {
    async fn artifact_version(
        &self,
        context: &FileQueryContext,
        file: &ResolvedFile,
    ) -> Result<
        (
            musteragent_artifact::ArtifactKind,
            musteragent_artifact::ArtifactVersion,
        ),
        FileQueryError,
    > {
        let (kind, version) = self
            .artifacts
            .as_ref()
            .ok_or(FileQueryError::Unsupported)?
            .version(context, file)
            .await?;
        let primary = file
            .content
            .as_ref()
            .ok_or(FileQueryError::MissingContent)?;
        let origin = file.artifact.as_ref().ok_or(FileQueryError::Integrity)?;
        if version.artifact_id != origin.artifact_id
            || version.version != origin.version
            || version.content_ref != *primary
            || !file
                .dependencies
                .iter()
                .all(|r| version.dependency_refs.contains(r))
        {
            return Err(FileQueryError::Integrity);
        }
        Ok((kind, version))
    }

    pub(crate) async fn read_artifact(
        &self,
        context: &FileQueryContext,
        file: ResolvedFile,
        query: ReadFile,
    ) -> Result<FileReadResult, FileQueryError> {
        let (kind, version) = self.artifact_version(context, &file).await?;
        let primary = &version.content_ref;
        let bytes = self.verified(context, primary).await?;
        let representation = artifact_file_representation(kind, &version, &bytes)
            .map_err(|_| FileQueryError::Integrity)?;
        let mut result = FileReadResult {
            view: FileReadView {
                representation_revision: None,
                quality: None,
                entry: file.entry.clone(),
                text: None,
                representation: FileRepresentation::Unsupported,
                extraction_status: "unsupported_format".into(),
                parser_revision: None,
                next_offset: None,
                resolved_start: query.offset,
                resolved_end: query.offset,
            },
            authorized_content: vec![],
        };
        match representation {
            ArtifactFileRepresentation::Unsupported => {}
            ArtifactFileRepresentation::Document(reference) => {
                self.require_dependency(context, &file, &reference)?;
                let mut document = file.clone();
                document.content = Some(reference);
                document.entry.semantic_kind = Some("file".into());
                let mut read = ContentFileReader::new(self.content.clone())
                    .read(context, document, query)
                    .await?;
                read.view.entry = file.entry;
                return Ok(read);
            }
            ArtifactFileRepresentation::Visual(references) => {
                if query.offset > references.len() {
                    return Err(FileQueryError::InvalidQuery);
                }
                let end = query.offset.saturating_add(16).min(references.len());
                for reference in &references[query.offset..end] {
                    self.require_dependency(context, &file, reference)?;
                    self.verified(context, reference).await?;
                }
                result.authorized_content = references[query.offset..end].to_vec();
                result.view.representation = FileRepresentation::VisualContent;
                result.view.extraction_status = "visual_content".into();
                result.view.resolved_end = end;
                result.view.next_offset = (end < references.len()).then_some(end);
            }
        }
        Ok(result)
    }

    fn require_dependency(
        &self,
        context: &FileQueryContext,
        file: &ResolvedFile,
        reference: &ContentRef,
    ) -> Result<(), FileQueryError> {
        if !context.permits_content(reference)
            || (file.content.as_ref() != Some(reference) && !file.dependencies.contains(reference))
        {
            return Err(FileQueryError::Denied);
        }
        Ok(())
    }

    async fn verified(
        &self,
        context: &FileQueryContext,
        reference: &ContentRef,
    ) -> Result<Vec<u8>, FileQueryError> {
        if !context.permits_content(reference) {
            return Err(FileQueryError::Denied);
        }
        if reference.byte_length > MAX_FILE_SOURCE_BYTES {
            return Err(FileQueryError::Unsupported);
        }
        let bytes = self
            .content
            .read(context.actor.tenant_id, reference)
            .await
            .map_err(|error| match error {
                ContentStoreError::NotFound => FileQueryError::MissingContent,
                ContentStoreError::Unauthorized => FileQueryError::Denied,
                ContentStoreError::Unavailable => FileQueryError::Unavailable,
                ContentStoreError::TooLarge => FileQueryError::Unsupported,
                _ => FileQueryError::Integrity,
            })?;
        verify_content(reference, &bytes).map_err(|_| FileQueryError::Integrity)?;
        Ok(bytes)
    }
}

/// Private Artifact reads retain their existing participant authority.
pub(crate) struct PrivateArtifactVersions(
    pub std::sync::Arc<dyn musteragent_runtime_artifact::ArtifactApplication>,
);
impl crate::FileArtifactVersionReader for PrivateArtifactVersions {
    fn version<'a>(
        &'a self,
        context: &'a FileQueryContext,
        file: &'a ResolvedFile,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<
            (
                musteragent_artifact::ArtifactKind,
                musteragent_artifact::ArtifactVersion,
            ),
            FileQueryError,
        >,
    > {
        Box::pin(async move {
            let FileSource::Artifact { artifact_id } = &file.entry.file_ref.source else {
                return Err(FileQueryError::Integrity);
            };
            let artifacts = &self.0;
            let view = artifacts
                .get_artifact(GetArtifact {
                    actor: context.actor,
                    artifact_id: artifact_id
                        .parse()
                        .map_err(|_| FileQueryError::InvalidReference)?,
                })
                .await
                .map_err(crate::authority::query_error)?;
            let primary = file
                .content
                .as_ref()
                .ok_or(FileQueryError::MissingContent)?;
            // The catalog head may contain later or rewound versions. Select only
            // the exact version proven by the effective conversation projection.
            let version = view
                .versions
                .iter()
                .find(|version| {
                    version.content_ref == *primary
                        && format!("{}:{}", version.version.get(), primary.sha256)
                            == file.entry.file_ref.revision
                })
                .ok_or(FileQueryError::Changed)?;
            if version.artifact_id != view.artifact.artifact_id
                || !file
                    .dependencies
                    .iter()
                    .all(|r| version.dependency_refs.contains(r))
            {
                return Err(FileQueryError::Integrity);
            }

            Ok((view.artifact.kind, version.clone()))
        })
    }
}
