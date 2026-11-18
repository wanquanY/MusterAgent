use std::sync::Arc;

use musteragent_runtime_content::{ContentStore, ContentStoreError, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ProjectFileContentRepresentation, ProjectFileReadError, read_stored_file_content,
};

use crate::{
    FileContentReader, FileQueryContext, FileQueryError, FileReadResult, FileReadView,
    FileRepresentation, FileSource, ReadFile, ResolvedFile,
};

/// Source bytes and extracted text are bounded before parsing. Larger files
/// require a streaming format adapter, not an unbounded in-process read.
pub const MAX_FILE_SOURCE_BYTES: u64 = 32 * 1024 * 1024;

/// The same document parser serves message attachments and managed files.
/// Structured Artifacts are routed to their format owner by composition; this
/// reader deliberately cannot turn an internal manifest into user content.
pub struct ContentFileReader {
    pub(crate) content: Arc<dyn ContentStore>,
    pub(crate) artifacts: Option<Arc<dyn crate::FileArtifactVersionReader>>,
    uploaded_documents: Option<Arc<dyn crate::UploadedDocumentReader>>,
}

impl ContentFileReader {
    #[must_use]
    pub fn new(content: Arc<dyn ContentStore>) -> Self {
        Self {
            content,
            artifacts: None,
            uploaded_documents: None,
        }
    }
    #[must_use]
    pub fn with_uploaded_documents(
        mut self,
        reader: Arc<dyn crate::UploadedDocumentReader>,
    ) -> Self {
        self.uploaded_documents = Some(reader);
        self
    }
    #[must_use]
    pub fn with_artifacts(
        mut self,
        artifacts: Arc<dyn musteragent_runtime_artifact::ArtifactApplication>,
    ) -> Self {
        self.artifacts = Some(Arc::new(super::artifact_read::PrivateArtifactVersions(
            artifacts,
        )));
        self
    }
    #[must_use]
    pub fn with_artifact_versions(
        mut self,
        versions: Arc<dyn crate::FileArtifactVersionReader>,
    ) -> Self {
        self.artifacts = Some(versions);
        self
    }
}

impl FileContentReader for ContentFileReader {
    fn read<'a>(
        &'a self,
        context: &'a FileQueryContext,
        file: ResolvedFile,
        query: ReadFile,
    ) -> ApplicationFuture<'a, Result<FileReadResult, FileQueryError>> {
        Box::pin(async move {
            let content = file.content.as_ref().ok_or(FileQueryError::Unsupported)?;
            if !context.permits_content(content) {
                return Err(FileQueryError::Denied);
            }
            if matches!(file.entry.file_ref.source, FileSource::Artifact { .. })
                && file.entry.semantic_kind.as_deref() != Some("file")
            {
                return self.read_artifact(context, file, query).await;
            }
            if matches!(file.entry.file_ref.source, FileSource::Attachment { .. })
                && !content.media_type.as_str().starts_with("image/")
                && let Some(reader) = &self.uploaded_documents
            {
                let representation = reader
                    .read(context, content, query.offset, query.limit)
                    .await?;
                representation.validate(content, query.offset, query.limit)?;
                if representation.state != "not_owned" {
                    return Ok(FileReadResult {
                        view: FileReadView {
                            entry: file.entry,
                            text: representation.text.clone(),
                            representation: if matches!(
                                representation.state.as_str(),
                                "ready" | "partially_ready"
                            ) {
                                FileRepresentation::ExtractedText
                            } else {
                                FileRepresentation::Unsupported
                            },
                            extraction_status: representation.state,
                            parser_revision: representation.parser_revision,
                            representation_revision: representation.representation_revision,
                            quality: Some(representation.quality),
                            next_offset: representation.next_offset,
                            resolved_start: if representation.text.is_none() {
                                query.offset
                            } else {
                                representation.resolved_start
                            },
                            resolved_end: if representation.text.is_none() {
                                query.offset
                            } else {
                                representation.resolved_end
                            },
                        },
                        authorized_content: vec![],
                    });
                }
            }
            if content.byte_length > MAX_FILE_SOURCE_BYTES {
                return Ok(FileReadResult {
                    view: FileReadView {
                        entry: file.entry,
                        text: None,
                        representation: FileRepresentation::Unsupported,
                        extraction_status: "source_too_large".to_owned(),
                        parser_revision: None,
                        representation_revision: None,
                        quality: None,
                        next_offset: None,
                        resolved_start: query.offset,
                        resolved_end: query.offset,
                    },
                    authorized_content: vec![],
                });
            }
            let media = content.media_type.as_str();
            let tool_key = context
                .dialogue_source
                .as_ref()
                .map(|source| source.tool_key.as_str());
            // files.read@6 advertises media source_content too. Audio and
            // video have no text representation, so they are returned as
            // original input for the model's native modality; PDF keeps the
            // extracted text there (first-turn PDFs already go natively).
            let source_media =
                matches!(file.entry.file_ref.source, FileSource::DialogueInput { .. })
                    && match tool_key {
                        Some("files.read@5") => {
                            media.starts_with("image/")
                                || media.starts_with("audio/")
                                || media.starts_with("video/")
                                || media == "application/pdf"
                        }
                        Some("files.read@6") => {
                            media.starts_with("audio/") || media.starts_with("video/")
                        }
                        _ => false,
                    };
            if source_media
                || matches!(
                    content.media_type.as_str(),
                    "image/png" | "image/jpeg" | "image/webp" | "image/gif"
                )
            {
                let bytes = self
                    .content
                    .read(context.actor.tenant_id, content)
                    .await
                    .map_err(|error| match error {
                        ContentStoreError::Unavailable => FileQueryError::Unavailable,
                        ContentStoreError::Unauthorized => FileQueryError::Denied,
                        ContentStoreError::NotFound => FileQueryError::MissingContent,
                        ContentStoreError::TooLarge => FileQueryError::Unsupported,
                        ContentStoreError::LengthMismatch
                        | ContentStoreError::HashMismatch
                        | ContentStoreError::IdentityConflict
                        | ContentStoreError::Invariant => FileQueryError::Integrity,
                    })?;
                verify_content(content, &bytes).map_err(|_| FileQueryError::Integrity)?;
                if query.offset > 1 {
                    return Err(FileQueryError::InvalidQuery);
                }
                return Ok(FileReadResult {
                    view: FileReadView {
                        entry: file.entry,
                        text: None,
                        representation: if source_media {
                            FileRepresentation::SourceContent
                        } else {
                            FileRepresentation::VisualContent
                        },
                        extraction_status: if source_media {
                            "source_content"
                        } else {
                            "visual_content"
                        }
                        .to_owned(),
                        parser_revision: None,
                        representation_revision: None,
                        quality: None,
                        next_offset: None,
                        resolved_start: query.offset,
                        resolved_end: 1,
                    },
                    authorized_content: if query.offset == 0 {
                        vec![content.clone()]
                    } else {
                        vec![]
                    },
                });
            }
            let read = read_stored_file_content(
                self.content.as_ref(),
                context.actor.tenant_id,
                content,
                query.offset,
                query.limit,
                false,
            )
            .await
            .map_err(|error| match error {
                ProjectFileReadError::InvalidRange => FileQueryError::InvalidQuery,
                ProjectFileReadError::InvalidEncoding => FileQueryError::Unsupported,
                ProjectFileReadError::ContentIntegrity => FileQueryError::Integrity,
                _ => FileQueryError::Unavailable,
            })?;
            Ok(FileReadResult {
                view: FileReadView {
                    entry: file.entry,
                    text: read.content,
                    representation: match read.content_representation {
                        ProjectFileContentRepresentation::SourceText => {
                            FileRepresentation::SourceText
                        }
                        ProjectFileContentRepresentation::ExtractedText => {
                            FileRepresentation::ExtractedText
                        }
                        ProjectFileContentRepresentation::Binary => FileRepresentation::Unsupported,
                    },
                    extraction_status: read.extraction_status.as_str().to_owned(),
                    parser_revision: read.parser.map(|p| format!("{}@{}", p.key, p.revision)),
                    representation_revision: Some(format!(
                        "{}:resource.file-parsed@4",
                        content.sha256
                    )),
                    quality: Some(read.quality),
                    next_offset: read.continuation_offset,
                    resolved_start: if read.content_representation
                        == ProjectFileContentRepresentation::Binary
                    {
                        query.offset
                    } else {
                        read.resolved_start_byte
                    },
                    resolved_end: if read.content_representation
                        == ProjectFileContentRepresentation::Binary
                    {
                        query.offset
                    } else {
                        read.resolved_end_byte
                    },
                },
                authorized_content: vec![],
            })
        })
    }
}
