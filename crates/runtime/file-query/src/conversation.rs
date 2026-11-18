use crate::{
    FILE_REFERENCE_SCHEMA, FileAvailability, FileCatalog, FileCoverage, FileEntry, FilePage,
    FileQueryContext, FileQueryError, FileReference, FileScope, FileSource, ListFiles,
    ResolvedFile, cursor,
};
use musteragent_artifact::{ArtifactKind, ArtifactLifecycle};
use musteragent_projection::{
    ConversationProjectionApplication, GetConversationProjection, ProjectedMessageStatus,
};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use std::sync::Arc;

/// Reads the effective projection, not historical catalog heads or the last
/// model prompt. Attachments and generated files retain their original identity.
pub struct ConversationFileCatalog {
    projections: Arc<dyn crate::ConversationFileSnapshotSource>,
    artifacts: Option<Arc<dyn crate::IndependentConversationFileSource>>,
    creation_relations: Option<Arc<dyn crate::ConversationFileRelations>>,
    retractions: Option<Arc<dyn crate::FileRetractionStatusSource>>,
    placements: Option<Arc<dyn crate::ConversationFilePlacements>>,
    product_versions: Option<Arc<dyn crate::ConversationFileVersions>>,
}

impl ConversationFileCatalog {
    #[must_use]
    pub fn new(projections: Arc<dyn ConversationProjectionApplication>) -> Self {
        Self {
            projections: Arc::new(PrivateFileSnapshotSource(projections)),
            artifacts: None,
            creation_relations: None,
            retractions: None,
            placements: None,
            product_versions: None,
        }
    }

    #[must_use]
    pub fn from_source(projections: Arc<dyn crate::ConversationFileSnapshotSource>) -> Self {
        Self {
            projections,
            artifacts: None,
            creation_relations: None,
            retractions: None,
            placements: None,
            product_versions: None,
        }
    }

    #[must_use]
    pub fn with_independent_artifacts(
        mut self,
        artifacts: Arc<dyn crate::IndependentConversationFileSource>,
    ) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    #[must_use]
    pub fn with_creation_relations(
        mut self,
        relations: Arc<dyn crate::ConversationFileRelations>,
    ) -> Self {
        self.creation_relations = Some(relations);
        self
    }

    #[must_use]
    pub fn with_retraction_status(
        mut self,
        owner: Arc<dyn crate::FileRetractionStatusSource>,
    ) -> Self {
        self.retractions = Some(owner);
        self
    }

    #[must_use]
    pub fn with_placements(
        mut self,
        placements: Arc<dyn crate::ConversationFilePlacements>,
    ) -> Self {
        self.placements = Some(placements);
        self
    }

    async fn current(
        &self,
        context: &FileQueryContext,
    ) -> Result<Vec<ResolvedFile>, FileQueryError> {
        let mut snapshot = self.projections.snapshot(context).await?;
        if snapshot.tenant_id != context.actor.tenant_id
            || Some(snapshot.conversation_id) != context.conversation_id
        {
            return Err(FileQueryError::Integrity);
        }
        if let Some(catalog) = &self.artifacts {
            let independent = super::independent::list(context, catalog.as_ref()).await?;
            let mut identities = snapshot
                .artifacts
                .iter()
                .map(|entry| entry.artifact_id)
                .collect::<std::collections::BTreeSet<_>>();
            for artifact in independent {
                if identities.insert(artifact.artifact_id) {
                    snapshot.artifacts.push(artifact);
                }
            }
        }
        if let Some(owner) = &self.placements {
            let ids = snapshot
                .artifacts
                .iter()
                .map(|item| item.artifact_id)
                .collect();
            let placements = owner
                .placements(context, ids)
                .await?
                .into_iter()
                .map(|item| (item.artifact_id, item))
                .collect::<std::collections::BTreeMap<_, _>>();
            snapshot.artifacts.retain_mut(|artifact| {
                if let Some(placement) = placements.get(&artifact.artifact_id) {
                    artifact.title.clone_from(&placement.name);
                    placement.available
                } else {
                    true
                }
            });
        }
        if let Some(owner) = &self.product_versions {
            // Restricted readers cannot acquire newer content through a saved
            // file association. Product composition never supplies a Run grant.
            if context.content_allowlist.is_some() {
                return Err(FileQueryError::Denied);
            }
            let ids = snapshot
                .artifacts
                .iter()
                .filter(|item| item.kind == ArtifactKind::File)
                .map(|item| item.artifact_id)
                .collect::<std::collections::BTreeSet<_>>();
            let mut versions = std::collections::BTreeMap::new();
            for version in owner
                .current_versions(context, ids.iter().copied().collect())
                .await?
            {
                if !ids.contains(&version.artifact_id)
                    || versions.insert(version.artifact_id, version).is_some()
                {
                    return Err(FileQueryError::Integrity);
                }
            }
            for artifact in &mut snapshot.artifacts {
                if let Some(version) = versions.remove(&artifact.artifact_id) {
                    artifact.current_version = version.version;
                    artifact.current_content_ref = version.content_ref;
                    artifact.current_dependency_refs.clear();
                    artifact.updated_at = version.updated_at;
                }
            }
        }
        let mut files = if let Some(relations) = &self.creation_relations {
            relations.files(context, &snapshot).await?
        } else {
            Vec::new()
        };
        for message in snapshot.messages {
            if message.status != ProjectedMessageStatus::Committed {
                continue;
            }
            for attachment in message.attachment_refs {
                let content = attachment.content_ref;
                if !context.permits_content(&content) {
                    continue;
                }
                files.push(ResolvedFile {
                    artifact: None,
                    entry: FileEntry {
                        production: None,
                        native_workspace_id: None,
                        file_ref: FileReference {
                            owned_agent_id: None,
                            schema: FILE_REFERENCE_SCHEMA.to_owned(),
                            conversation_id: context.conversation_id.map(|id| id.to_string()),
                            workspace_id: context.reference_workspace_id(),
                            scope: FileScope::Conversation,
                            source: FileSource::Attachment {
                                message_id: message.message_id.to_string(),
                                content_id: content.content_id.to_string(),
                            },
                            revision: content.sha256.to_string(),
                        },
                        name: attachment.display_name.as_str().to_owned(),
                        is_directory: false,
                        media_type: Some(content.media_type.as_str().to_owned()),
                        byte_length: Some(content.byte_length),
                        modified_at_ms: message.committed_at.unix_millis(),
                        associated_at_ms: Some(message.committed_at.unix_millis()),
                        semantic_kind: None,
                        availability: FileAvailability::Available,
                    },
                    content: Some(content),
                    dependencies: Vec::new(),
                });
            }
        }
        for artifact in snapshot.artifacts {
            if !context.permits_content(&artifact.current_content_ref) {
                continue;
            }
            let content = artifact.current_content_ref;
            files.push(ResolvedFile {
                artifact: Some(crate::ResolvedArtifactVersion {
                    artifact_id: artifact.artifact_id,
                    version: artifact.current_version,
                }),
                entry: FileEntry {
                    production: None,
                    native_workspace_id: None,
                    file_ref: FileReference {
                        owned_agent_id: None,
                        schema: FILE_REFERENCE_SCHEMA.to_owned(),
                        conversation_id: context.conversation_id.map(|id| id.to_string()),
                        workspace_id: context.reference_workspace_id(),
                        scope: FileScope::Conversation,
                        source: FileSource::Artifact {
                            artifact_id: artifact.artifact_id.to_string(),
                        },
                        revision: format!("{}:{}", artifact.current_version.get(), content.sha256),
                    },
                    name: artifact.title,
                    is_directory: false,
                    media_type: Some(content.media_type.as_str().to_owned()),
                    byte_length: Some(content.byte_length),
                    modified_at_ms: artifact.updated_at.unix_millis(),
                    associated_at_ms: Some(artifact.updated_at.unix_millis()),
                    semantic_kind: Some(artifact_kind(artifact.kind).to_owned()),
                    availability: match artifact.lifecycle {
                        ArtifactLifecycle::Draft => FileAvailability::Draft,
                        ArtifactLifecycle::Published => FileAvailability::Available,
                        ArtifactLifecycle::Archived => FileAvailability::Archived,
                        ArtifactLifecycle::Failed => FileAvailability::Failed,
                    },
                },
                content: Some(content),
                dependencies: artifact
                    .current_dependency_refs
                    .into_iter()
                    .filter(|r| context.permits_content(r))
                    .collect(),
            });
        }
        files.sort_by(|a, b| {
            b.entry
                .associated_at_ms
                .cmp(&a.entry.associated_at_ms)
                .then_with(|| a.entry.file_ref.source.cmp(&b.entry.file_ref.source))
        });
        // Repeated references in one committed message are one relation. Do
        // not collapse distinct messages, file identities or versions by name.
        files.dedup_by(|a, b| a.entry.file_ref == b.entry.file_ref);
        Ok(files)
    }
}

impl ConversationFileCatalog {
    /// Enable only in Product composition. Association, rewind visibility and
    /// authorization still come from the effective Conversation projection.
    #[must_use]
    pub fn with_product_versions(
        mut self,
        owner: Arc<dyn crate::ConversationFileVersions>,
    ) -> Self {
        self.product_versions = Some(owner);
        self
    }
}

fn artifact_kind(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Note => "note",
        ArtifactKind::LegacyDiagram => "diagram",
        ArtifactKind::Presentation => "presentation",
        ArtifactKind::Image => "image",
        ArtifactKind::Video => "video",
        ArtifactKind::File => "file",
        ArtifactKind::Application => "application",
    }
}

impl FileCatalog for ConversationFileCatalog {
    fn retraction_status<'a>(
        &'a self,
        context: &'a FileQueryContext,
    ) -> ApplicationFuture<'a, Result<Option<crate::FileRetractionStatus>, FileQueryError>> {
        Box::pin(async move {
            if context.content_allowlist.is_some() {
                return Ok(None);
            }
            match &self.retractions {
                Some(owner) => owner.retraction_status(context).await,
                None => Ok(None),
            }
        })
    }

    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        query: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(async move {
            query.validate(context)?;
            if query.scope != FileScope::Conversation {
                return Err(FileQueryError::InvalidQuery);
            }
            let search = query.name_query.as_ref().map(|s| s.trim().to_lowercase());
            let entries: Vec<_> = self
                .current(context)
                .await?
                .into_iter()
                .map(|f| f.entry)
                .filter(|e| {
                    search
                        .as_ref()
                        .is_none_or(|s| e.name.to_lowercase().contains(s))
                })
                .collect();
            let revision = cursor::revision(&entries)?;
            let fingerprint = cursor::fingerprint(context, &query)?;
            let offset = cursor::offset(query.cursor.as_deref(), &fingerprint, &revision)?;
            if offset > entries.len() {
                return Err(FileQueryError::InvalidQuery);
            }
            let end = offset.saturating_add(query.limit).min(entries.len());
            Ok(FilePage {
                retraction_status: None,
                entries: entries[offset..end].to_vec(),
                next_cursor: if end < entries.len() {
                    Some(cursor::next(&fingerprint, &revision, end)?)
                } else {
                    None
                },
                revision,
                coverage: if context.content_allowlist.is_some() {
                    FileCoverage::RestrictedConversation
                } else {
                    FileCoverage::ProjectedConversation
                },
            })
        })
    }

    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a crate::FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            reference.validate(context)?;
            if reference.scope != FileScope::Conversation {
                return Err(FileQueryError::InvalidReference);
            }
            let file = self
                .current(context)
                .await?
                .into_iter()
                .find(|f| f.entry.file_ref.source == reference.source)
                .ok_or(FileQueryError::NotFound)?;
            Ok(file)
        })
    }
}

struct PrivateFileSnapshotSource(Arc<dyn ConversationProjectionApplication>);
impl crate::ConversationFileSnapshotSource for PrivateFileSnapshotSource {
    fn snapshot<'a>(
        &'a self,
        context: &'a FileQueryContext,
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_projection::ConversationProjectionSnapshot, FileQueryError>,
    > {
        Box::pin(async move {
            self.0
                .get_head_projection(GetConversationProjection {
                    actor: context.actor,
                    conversation_id: context.require_conversation()?,
                    preferred_snapshot_id: None,
                })
                .await
                .map_err(|e| match e.code {
                    ErrorCode::PolicyDenied
                    | ErrorCode::Unauthenticated
                    | ErrorCode::ParticipantNotAuthorized => FileQueryError::Denied,
                    ErrorCode::ConversationNotFound => FileQueryError::NotFound,
                    _ => FileQueryError::Unavailable,
                })
        })
    }
}
