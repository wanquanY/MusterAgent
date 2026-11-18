use crate::{
    FILE_REFERENCE_SCHEMA, FileAvailability, FileCatalog, FileCoverage, FileEntry, FilePage,
    FileQueryContext, FileQueryError, FileReference, FileScope, FileSource, ListFiles,
    ResolvedFile, cursor,
};
use musteragent_identity::ResourceNodeId;
use musteragent_resource::{ResourceNodeKind, ResourceNodeLifecycle, ResourceScope};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ListProjectFiles, ProjectFileCatalogEntry, ProjectFileCatalogStore, ResolveResourceContext,
    ResourceCommandContext, ResourceContextAuthority, ResourceContextError, ResourceStoreError,
};
use std::sync::Arc;

/// Managed storage adapter. Native / Hosted composition supplies a catalog
/// from its actual placement owner, never switches on the caller's UI type.
pub struct ManagedResourceFileCatalog {
    pub(crate) authority: Arc<dyn ResourceContextAuthority>,
    pub(crate) store: Arc<dyn ProjectFileCatalogStore>,
    coverage: Option<Arc<dyn crate::ProjectFileSourceCoverageReader>>,
    pub(crate) scope: FileScope,
}
impl ManagedResourceFileCatalog {
    /// Managed project files only. External source coverage must be supplied
    /// with `new` by a host that owns those additional sources.
    #[must_use]
    pub fn managed_project(
        authority: Arc<dyn ResourceContextAuthority>,
        store: Arc<dyn ProjectFileCatalogStore>,
    ) -> Self {
        Self {
            authority,
            store,
            coverage: None,
            scope: FileScope::Project,
        }
    }

    #[must_use]
    pub fn new(
        authority: Arc<dyn ResourceContextAuthority>,
        store: Arc<dyn ProjectFileCatalogStore>,
        coverage: Arc<dyn crate::ProjectFileSourceCoverageReader>,
    ) -> Self {
        Self {
            authority,
            store,
            coverage: Some(coverage),
            scope: FileScope::Project,
        }
    }

    /// Agent-owned managed resources, authorized only by a sealed invocation.
    #[must_use]
    pub fn for_owned_agent(
        authority: Arc<dyn ResourceContextAuthority>,
        store: Arc<dyn ProjectFileCatalogStore>,
    ) -> Self {
        Self {
            authority,
            store,
            coverage: None,
            scope: FileScope::OwnedAgent,
        }
    }

    pub fn execution_workspace(
        authority: Arc<dyn ResourceContextAuthority>,
        store: Arc<dyn ProjectFileCatalogStore>,
    ) -> Self {
        Self {
            authority,
            store,
            coverage: None,
            scope: FileScope::Work,
        }
    }
    pub(crate) async fn source_coverage(
        &self,
        context: &FileQueryContext,
    ) -> Result<Option<crate::ProjectFileSourceCoverage>, FileQueryError> {
        match &self.coverage {
            Some(owner) => owner.coverage(context).await.map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn context(
        &self,
        context: &FileQueryContext,
    ) -> Result<ResourceCommandContext, FileQueryError> {
        if self.scope == FileScope::OwnedAgent {
            let for_owned_agent = context
                .dialogue_source
                .as_ref()
                .ok_or(FileQueryError::Denied)?;
            let resolved = self
                .authority
                .resolve(ResolveResourceContext {
                    actor: context.actor,
                    conversation_id: context.require_conversation()?,
                    invocation_id: Some(for_owned_agent.invocation_id),
                    now: context.now,
                })
                .await
                .map_err(|error| match error {
                    ResourceContextError::ScopeUnavailable => FileQueryError::Denied,
                    ResourceContextError::DependencyUnavailable => FileQueryError::Unavailable,
                    ResourceContextError::Corrupt => FileQueryError::Integrity,
                })?;
            if resolved.actor != context.actor
                || resolved.conversation_id() != context.conversation_id
                || resolved.scope() != ResourceScope::OwnedAgent(for_owned_agent.agent_id)
                || resolved.invocation_id != Some(for_owned_agent.invocation_id)
                || resolved.tool_key.as_ref() != Some(&for_owned_agent.tool_key)
                || resolved.validate().is_err()
            {
                return Err(FileQueryError::Denied);
            }
            return Ok(resolved);
        }
        if context.dialogue_source.is_some() {
            return Err(FileQueryError::Denied);
        }
        if self.scope == FileScope::Work {
            if context.content_allowlist.is_some() {
                return Err(FileQueryError::Denied);
            }
            let resource = self
                .authority
                .resolve_execution(ResolveResourceContext {
                    actor: context.actor,
                    conversation_id: context.require_conversation()?,
                    invocation_id: None,
                    now: context.now,
                })
                .await
                .map_err(|e| match e {
                    ResourceContextError::ScopeUnavailable => FileQueryError::Denied,
                    ResourceContextError::DependencyUnavailable => FileQueryError::Unavailable,
                    ResourceContextError::Corrupt => FileQueryError::Integrity,
                })?;
            if resource.actor != context.actor
                || resource.conversation_id() != context.conversation_id
                || resource.validate().is_err()
                || resource.workspace_id() != context.project_workspace_id
                || !matches!(resource.scope(), ResourceScope::ExecutionWorkspace(_))
            {
                return Err(FileQueryError::Denied);
            }
            return Ok(resource);
        }
        // A content-only delegation does not imply a grant to enumerate project
        // paths. The execution placement adapter must provide its file grant.
        if context.project_workspace_id.is_none() {
            return Err(FileQueryError::Denied);
        }
        if context.conversation_id.is_none() {
            return Ok(ResourceCommandContext {
                tenant_id: context.actor.tenant_id,
                actor: context.actor,
                source: musteragent_runtime_resource::ResourceCommandSource::Workspace {
                    workspace_id: context.project_workspace_id.ok_or(FileQueryError::Denied)?,
                },
                invocation_id: None,
                tool_key: None,
                now: context.now,
            });
        }
        let resolved = self
            .authority
            .resolve(ResolveResourceContext {
                actor: context.actor,
                conversation_id: context.require_conversation()?,
                invocation_id: None,
                now: context.now,
            })
            .await
            .map_err(|e| match e {
                ResourceContextError::ScopeUnavailable => FileQueryError::Denied,
                ResourceContextError::DependencyUnavailable => FileQueryError::Unavailable,
                ResourceContextError::Corrupt => FileQueryError::Integrity,
            })?;
        if resolved.workspace_id() != context.project_workspace_id
            || resolved.actor != context.actor
            || resolved.conversation_id() != context.conversation_id
            || !matches!(resolved.scope(), ResourceScope::Project(_))
            || resolved.validate().is_err()
        {
            return Err(FileQueryError::Denied);
        }
        Ok(resolved)
    }
}

impl ManagedResourceFileCatalog {
    fn list_query<'a>(
        &'a self,
        context: &'a FileQueryContext,
        query: ListFiles,
        recursive: bool,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(async move {
            query.validate(context)?;
            // A producer selector locates that execution's current files in
            // the bounded subtree, including files in nested folders. Folders
            // themselves have no content-version producer and are not matches.
            let recursive = recursive || query.produced_by.is_some();
            if query.scope != self.scope {
                return Err(FileQueryError::InvalidQuery);
            }
            let resource = self.context(context).await?;
            let source = self.source_coverage(context).await?;
            if source.is_some_and(|s| !s.complete) {
                return Err(FileQueryError::Incomplete);
            }
            let parent = if let Some(reference) = &query.parent {
                let entry = self
                    .store
                    .resolve_file(&resource, source_node_id(&reference.source)?)
                    .await
                    .map_err(map_store)?;
                let file = into_file(context, &resource, entry)?;
                if file.entry.file_ref != *reference {
                    return Err(FileQueryError::Changed);
                }
                if !file.entry.is_directory {
                    return Err(FileQueryError::InvalidQuery);
                }
                Some(node_id(reference)?)
            } else {
                None
            };
            let fingerprint = musteragent_content_ref::Sha256::digest(format!(
                "{}:{recursive}",
                cursor::fingerprint(context, &query)?
            ))
            .to_string();
            let (saved_revision, offset) = cursor::position(query.cursor.as_deref(), &fingerprint)?;
            let page = self
                .store
                .list_files(ListProjectFiles {
                    produced_by: query.produced_by.clone(),
                    context: resource.clone(),
                    recursive,
                    parent_node_id: parent,
                    offset,
                    limit: query.limit,
                    name_query: query.name_query,
                })
                .await
                .map_err(map_store)?;
            if page.scope != resource.scope()
                || page.entries.len() > query.limit
                || page.next_offset.is_some_and(|next| next <= offset)
                || query.produced_by.as_ref().is_some_and(|filter| {
                    page.entries
                        .iter()
                        .any(|entry| entry.production.as_ref().is_none_or(|p| !filter.matches(p)))
                })
            {
                return Err(FileQueryError::Integrity);
            }
            // The two owners do not share a transaction. Fence the Resource
            // read with the original source revision; never join mixed pages.
            let current_source = self.source_coverage(context).await?;
            if current_source != source {
                return Err(FileQueryError::Changed);
            }
            let revision = format!(
                "{}:{}:{}:{}",
                scope_id(&resource)?,
                page.revision.get(),
                page.content_revision,
                source
                    .map(|s| s.revision.to_string())
                    .unwrap_or_else(|| match self.scope {
                        FileScope::OwnedAgent => "agent-resources".into(),
                        _ => "0".into(),
                    })
            );
            if saved_revision.is_some_and(|saved| saved != revision) {
                return Err(FileQueryError::Changed);
            }
            let entries = page
                .entries
                .into_iter()
                .map(|e| into_file(context, &resource, e).map(|f| f.entry))
                .collect::<Result<_, _>>()?;
            Ok(FilePage {
                retraction_status: None,
                entries,
                next_cursor: page
                    .next_offset
                    .map(|next| cursor::next(&fingerprint, &revision, next))
                    .transpose()?,
                revision,
                coverage: match (self.scope, recursive) {
                    (FileScope::OwnedAgent, _) => FileCoverage::OwnedAgentResources,
                    (FileScope::Work, true) => FileCoverage::WorkSubtree,
                    (FileScope::Work, false) => FileCoverage::WorkChildren,
                    (_, true) => FileCoverage::ProjectSubtree,
                    (_, false) => FileCoverage::ProjectChildren,
                },
            })
        })
    }
}
impl FileCatalog for ManagedResourceFileCatalog {
    fn artifact_placement<'a>(
        &'a self,
        context: &'a FileQueryContext,
        origin: &'a crate::ResolvedArtifactVersion,
        native_content: &'a musteragent_content_ref::ContentRef,
    ) -> ApplicationFuture<'a, Result<Option<FileReference>, FileQueryError>> {
        Box::pin(async move {
            if context.content_allowlist.is_some() {
                return Err(FileQueryError::Denied);
            }
            let resource = self.context(context).await?;
            let Some(entry) = self
                .store
                .resolve_artifact_placement(&resource, origin.artifact_id)
                .await
                .map_err(map_store)?
            else {
                return Ok(None);
            };
            let file = into_file(context, &resource, entry)?;
            if file.artifact.as_ref() != Some(origin)
                || file.content.as_ref() != Some(native_content)
            {
                return Err(FileQueryError::Changed);
            }
            Ok(Some(file.entry.file_ref))
        })
    }

    fn file_tree<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<Option<crate::FileTreeDescriptor>, FileQueryError>> {
        Box::pin(async move { self.describe_tree(context, reference).await.map(Some) })
    }

    fn resolve<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            let mut file = self.resolve_current(context, reference).await?;
            if file.entry.file_ref != *reference {
                // Older clients bound Resource metadata revisions into file
                // references. Accept those only when the source content version
                // is identical; a rename does not invalidate immutable bytes.
                let mut expected = file.entry.file_ref.clone();
                expected.revision = reference.revision.clone();
                if expected != *reference
                    || file.entry.is_directory
                    || !same_file_content_revision(
                        &reference.revision,
                        &file.entry.file_ref.revision,
                    )
                {
                    return Err(FileQueryError::Changed);
                }
                file.entry.file_ref = reference.clone();
            }
            Ok(file)
        })
    }
    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        query: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        self.list_query(context, query, false)
    }
    fn list_subtree<'a>(
        &'a self,
        context: &'a FileQueryContext,
        query: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        self.list_query(context, query, true)
    }
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a crate::FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            reference.validate(context)?;
            let resource = self.context(context).await?;
            let entry = self
                .store
                .resolve_file(&resource, source_node_id(&reference.source)?)
                .await
                .map_err(map_store)?;
            let file = into_file(context, &resource, entry)?;
            Ok(file)
        })
    }
}

pub(crate) fn node_id(reference: &FileReference) -> Result<ResourceNodeId, FileQueryError> {
    if !matches!(
        reference.scope,
        FileScope::Project | FileScope::Work | FileScope::OwnedAgent
    ) {
        return Err(FileQueryError::InvalidReference);
    }
    source_node_id(&reference.source)
}
fn source_node_id(source: &FileSource) -> Result<ResourceNodeId, FileQueryError> {
    let FileSource::Resource { node_id } = source else {
        return Err(FileQueryError::InvalidReference);
    };
    ResourceNodeId::from_uuid(
        uuid::Uuid::parse_str(node_id).map_err(|_| FileQueryError::InvalidReference)?,
    )
    .map_err(|_| FileQueryError::InvalidReference)
}

pub(crate) fn into_file(
    context: &FileQueryContext,
    resource: &ResourceCommandContext,
    entry: ProjectFileCatalogEntry,
) -> Result<ResolvedFile, FileQueryError> {
    let node = entry.node;
    if node.tenant_id != context.actor.tenant_id
        || node.scope != resource.scope()
        || node.lifecycle != ResourceNodeLifecycle::Active
    {
        return Err(FileQueryError::Integrity);
    }
    let is_directory = node.kind == ResourceNodeKind::Folder;
    if is_directory && entry.file.is_some() {
        return Err(FileQueryError::Integrity);
    }
    if entry.file.as_ref().is_some_and(|file| {
        file.node_id != node.node_id || Some(file.artifact_id) != node.file_artifact_id
    }) {
        return Err(FileQueryError::Integrity);
    }
    let source_version = entry
        .file
        .as_ref()
        .map(|file| format!("{}:{}", file.artifact_version_id, file.content_ref.sha256))
        .unwrap_or_else(|| {
            if is_directory {
                "folder".into()
            } else {
                "unavailable".into()
            }
        });
    let unavailable = !is_directory && entry.file.is_none();
    if entry.production.as_ref().is_some_and(|production| {
        !production.is_valid()
            || is_directory
            || unavailable
            || context
                .dialogue_source
                .as_ref()
                .is_none_or(|p| production.agent_id != p.agent_id.to_string())
    }) {
        return Err(FileQueryError::Integrity);
    }
    let artifact = entry
        .file
        .as_ref()
        .map(|file| crate::ResolvedArtifactVersion {
            artifact_id: file.artifact_id,
            version: file.version,
        });
    let revision_scope = scope_id(resource)?;
    let exact_revision = entry.file.as_ref().map(|file| {
        crate::resource_file_revision(
            &revision_scope,
            file.artifact_version_id,
            file.content_ref.sha256,
        )
    });
    let content = entry.file.map(|file| file.content_ref);
    Ok(ResolvedFile {
        artifact,
        entry: FileEntry {
            production: entry.production,
            native_workspace_id: None,
            file_ref: FileReference {
                owned_agent_id: context
                    .dialogue_source
                    .as_ref()
                    .map(|source| source.agent_id.to_string()),
                schema: if context.dialogue_source.is_some() {
                    crate::AGENT_FILE_REFERENCE_SCHEMA
                } else {
                    FILE_REFERENCE_SCHEMA
                }
                .into(),
                conversation_id: context
                    .dialogue_source
                    .is_none()
                    .then(|| context.conversation_id.map(|id| id.to_string()))
                    .flatten(),
                workspace_id: context.reference_workspace_id(),
                scope: if context.dialogue_source.is_some() {
                    FileScope::OwnedAgent
                } else if matches!(resource.scope(), ResourceScope::ExecutionWorkspace(_)) {
                    FileScope::Work
                } else {
                    FileScope::Project
                },
                source: FileSource::Resource {
                    node_id: node.node_id.to_string(),
                },
                revision: exact_revision.unwrap_or_else(|| {
                    format!(
                        "{}:{}:{}",
                        revision_scope,
                        if is_directory {
                            node.revision.get().to_string()
                        } else {
                            "file".into()
                        },
                        source_version
                    )
                }),
            },
            name: node.name.as_str().to_owned(),
            is_directory,
            media_type: content.as_ref().map(|c| c.media_type.as_str().to_owned()),
            byte_length: content.as_ref().map(|c| c.byte_length),
            modified_at_ms: entry
                .file_updated_at
                .unwrap_or(node.updated_at)
                .unix_millis(),
            associated_at_ms: None,
            semantic_kind: None,
            availability: if unavailable {
                FileAvailability::Unavailable
            } else {
                FileAvailability::Available
            },
        },
        content,
        dependencies: vec![],
    })
}

fn same_file_content_revision(previous: &str, current: &str) -> bool {
    let mut old = previous.splitn(3, ':');
    let mut new = current.splitn(3, ':');
    let (Some(old_workspace), Some(old_metadata), Some(old_content)) =
        (old.next(), old.next(), old.next())
    else {
        return false;
    };
    let (Some(new_workspace), Some("file"), Some(new_content)) =
        (new.next(), new.next(), new.next())
    else {
        return false;
    };
    old_workspace == new_workspace
        && old_content == new_content
        && old_metadata
            .parse::<u64>()
            .is_ok_and(|revision| revision.to_string() == old_metadata)
}

pub(crate) fn map_store(error: ResourceStoreError) -> FileQueryError {
    match error {
        ResourceStoreError::BoundsExceeded => FileQueryError::Unsupported,
        ResourceStoreError::RevisionConflict => FileQueryError::Changed,
        ResourceStoreError::NodeUnavailable
        | ResourceStoreError::ParentUnavailable
        | ResourceStoreError::FileUnavailable => FileQueryError::NotFound,
        ResourceStoreError::DependencyUnavailable => FileQueryError::Unavailable,
        _ => FileQueryError::Integrity,
    }
}

pub(crate) fn scope_id(resource: &ResourceCommandContext) -> Result<String, FileQueryError> {
    match resource.scope() {
        ResourceScope::ExecutionWorkspace(id) => Ok(id.to_string()),
        ResourceScope::Project(id) => Ok(id.to_string()),
        ResourceScope::OwnedAgent(id) => Ok(id.to_string()),
        _ => Err(FileQueryError::Integrity),
    }
}

#[cfg(test)]
mod file_revision_tests {
    use super::same_file_content_revision;
    #[test]
    fn legacy_metadata_revisions_keep_exact_content_identity() {
        assert!(same_file_content_revision(
            "workspace:7:version:hash",
            "workspace:file:version:hash"
        ));
        assert!(!same_file_content_revision(
            "workspace:7:version:hash",
            "workspace:file:other:hash"
        ));
        assert!(!same_file_content_revision(
            "workspace:7:version:hash",
            "other:file:version:hash"
        ));
        assert!(!same_file_content_revision(
            "workspace:invalid:version:hash",
            "workspace:file:version:hash"
        ));
    }
}
