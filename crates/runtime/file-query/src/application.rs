use crate::{
    FileAvailability, FileCatalog, FileContentReader, FileEntry, FilePage, FileQueryContext,
    FileQueryError, FileReadResult, FileReference, FileScope, ListFiles, MAX_FILE_READ_BYTES,
    ReadFile,
};
use std::sync::Arc;

/// Both clients and tools call this application. Storage selection belongs to
/// the project catalog, so no browser/Electron switch or model-supplied root
/// can redirect a query to another provider.
pub struct FileQueryApplication {
    pub(crate) conversation: Arc<dyn FileCatalog>,
    pub(crate) project: Arc<dyn FileCatalog>,
    pub(crate) agent_catalog: Option<Arc<dyn FileCatalog>>,
    pub(crate) work: Option<Arc<dyn FileCatalog>>,
    pub(crate) reader: Arc<dyn FileContentReader>,
    pub(crate) content: Option<Arc<dyn musteragent_runtime_content::ContentStore>>,
}

impl FileQueryApplication {
    #[must_use]
    pub fn new(
        conversation: Arc<dyn FileCatalog>,
        project: Arc<dyn FileCatalog>,
        reader: Arc<dyn FileContentReader>,
    ) -> Self {
        Self {
            conversation,
            project,
            agent_catalog: None,
            work: None,
            reader,
            content: None,
        }
    }

    pub fn with_work_catalog(mut self, work: Arc<dyn FileCatalog>) -> Self {
        self.work = Some(work);
        self
    }
    #[must_use]
    pub fn with_dialogue_catalog(mut self, catalog: Arc<dyn FileCatalog>) -> Self {
        self.agent_catalog = Some(catalog);
        self
    }

    pub(crate) fn catalog(&self, scope: FileScope) -> Result<&dyn FileCatalog, FileQueryError> {
        match scope {
            FileScope::Conversation => Ok(self.conversation.as_ref()),
            FileScope::Project => Ok(self.project.as_ref()),
            FileScope::Work => self
                .work
                .as_deref()
                .ok_or(FileQueryError::UnsupportedScope(scope)),
            FileScope::OwnedAgent => self
                .agent_catalog
                .as_deref()
                .ok_or(FileQueryError::UnsupportedScope(scope)),
        }
    }

    pub async fn list(
        &self,
        context: &FileQueryContext,
        query: ListFiles,
    ) -> Result<FilePage, FileQueryError> {
        self.search_catalog(context, crate::FileSearchTraversal::Children, query)
            .await
    }

    pub(crate) async fn search_catalog(
        &self,
        context: &FileQueryContext,
        traversal: crate::FileSearchTraversal,
        query: ListFiles,
    ) -> Result<FilePage, FileQueryError> {
        query.validate(context)?;
        let scope = query.scope;
        let limit = query.limit;
        let produced_by = query.produced_by.clone();
        let catalog = self.catalog(scope)?;
        let result = match traversal {
            crate::FileSearchTraversal::Children => catalog.list(context, query).await?,
            crate::FileSearchTraversal::Subtree
                if matches!(
                    scope,
                    FileScope::Project | FileScope::Work | FileScope::OwnedAgent
                ) =>
            {
                let page = catalog.list_subtree(context, query).await?;
                if !(if scope == FileScope::OwnedAgent {
                    matches!(
                        page.coverage,
                        crate::FileCoverage::OwnedAgentResources
                            | crate::FileCoverage::OwnedAgentFiles
                    )
                } else if scope == FileScope::Work {
                    page.coverage == crate::FileCoverage::WorkSubtree
                } else {
                    page.coverage == crate::FileCoverage::ProjectSubtree
                }) {
                    return Err(FileQueryError::Integrity);
                }
                page
            }
            _ => return Err(FileQueryError::InvalidQuery),
        };
        if result.entries.len() > limit || result.revision.is_empty() {
            return Err(FileQueryError::Integrity);
        }
        let mut identities = std::collections::BTreeSet::new();
        for entry in &result.entries {
            entry.validate(context)?;
            if entry.file_ref.scope != scope
                || !identities.insert(&entry.file_ref.source)
                || produced_by.as_ref().is_some_and(|filter| {
                    entry
                        .production
                        .as_ref()
                        .is_none_or(|origin| !filter.matches(origin))
                })
            {
                return Err(FileQueryError::Integrity);
            }
        }
        Ok(result)
    }

    pub async fn resolve_identity(
        &self,
        context: &FileQueryContext,
        identity: &crate::FileIdentity,
    ) -> Result<FileEntry, FileQueryError> {
        identity.validate(context)?;
        let file = self
            .catalog(identity.scope)?
            .resolve_identity(context, identity)
            .await?;
        file.entry.validate(context)?;
        if file.entry.file_ref.identity() != *identity {
            return Err(FileQueryError::Integrity);
        }
        Ok(file.entry)
    }

    pub async fn refresh_reference(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
    ) -> Result<FileEntry, FileQueryError> {
        reference.validate(context)?;
        let resolved = self
            .catalog(reference.scope)?
            .resolve_current(context, reference)
            .await?;
        resolved.entry.validate(context)?;
        let mut identity = resolved.entry.file_ref.clone();
        identity.revision.clone_from(&reference.revision);
        if identity != *reference {
            return Err(FileQueryError::Integrity);
        }
        Ok(resolved.entry)
    }

    pub async fn inspect(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
    ) -> Result<FileEntry, FileQueryError> {
        reference.validate(context)?;
        let resolved = self
            .catalog(reference.scope)?
            .resolve(context, reference)
            .await?;
        resolved.entry.validate(context)?;
        if &resolved.entry.file_ref != reference {
            return Err(FileQueryError::Changed);
        }
        Ok(resolved.entry)
    }

    pub async fn prepare_content(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
    ) -> Result<FileEntry, FileQueryError> {
        reference.validate(context)?;
        let catalog = self.catalog(reference.scope)?;
        let original = catalog.resolve(context, reference).await?;
        original.entry.validate(context)?;
        if original.entry.file_ref != *reference {
            return Err(FileQueryError::Changed);
        }
        if original.entry.is_directory {
            return Err(FileQueryError::InvalidQuery);
        }
        let prepared = catalog.prepare_content(context, reference).await?;
        prepared.entry.validate(context)?;
        let mut identity = prepared.entry.file_ref.clone();
        identity.revision.clone_from(&reference.revision);
        let mut metadata = prepared.entry.clone();
        metadata.file_ref.revision.clone_from(&reference.revision);
        metadata.availability = original.entry.availability;
        metadata.byte_length = original.entry.byte_length;
        if identity != *reference
            || metadata != original.entry
            || prepared.entry.is_directory
            || prepared.entry.availability != FileAvailability::Available
            || prepared.content.is_none()
            || prepared.artifact != original.artifact
            || prepared.dependencies != original.dependencies
            || (original.content.is_some() && prepared.content != original.content)
            || (original.entry.byte_length.is_some()
                && prepared.entry.byte_length != original.entry.byte_length)
            || prepared
                .content
                .as_ref()
                .is_some_and(|content| prepared.entry.byte_length != Some(content.byte_length))
        {
            return Err(FileQueryError::Integrity);
        }
        if prepared
            .content
            .iter()
            .chain(&prepared.dependencies)
            .any(|content| !context.permits_content(content))
        {
            return Err(FileQueryError::Denied);
        }
        let current = catalog.resolve(context, &prepared.entry.file_ref).await?;
        if current.entry != prepared.entry
            || current.content != prepared.content
            || current.dependencies != prepared.dependencies
            || current.artifact != prepared.artifact
        {
            return Err(FileQueryError::Changed);
        }
        Ok(prepared.entry)
    }

    pub async fn read(
        &self,
        context: &FileQueryContext,
        query: ReadFile,
    ) -> Result<FileReadResult, FileQueryError> {
        query.file_ref.validate(context)?;
        if query.limit == 0 || query.limit > MAX_FILE_READ_BYTES {
            return Err(FileQueryError::InvalidQuery);
        }
        let catalog = self.catalog(query.file_ref.scope)?;
        let resolved = catalog.resolve(context, &query.file_ref).await?;
        resolved.entry.validate(context)?;
        if resolved.entry.file_ref != query.file_ref {
            return Err(FileQueryError::Changed);
        }
        if resolved.entry.is_directory {
            return Err(FileQueryError::InvalidQuery);
        }
        if resolved.entry.availability == FileAvailability::Unavailable {
            return Err(FileQueryError::Unavailable);
        }
        // Native project bytes are authorized by the selected physical root.
        // Their immutable Content identity is established only after the read;
        // discovery never copies every local file into the Content store.
        let native_project = matches!(
            query.file_ref.source,
            crate::FileSource::Native { .. }
                | crate::FileSource::NativeCreation { .. }
                | crate::FileSource::Hosted { .. }
                | crate::FileSource::HostedCreation { .. }
        ) && context.project_workspace_id.is_some();
        let result = self
            .reader
            .read(context, resolved.clone(), query.clone())
            .await?;
        let view = &result.view;
        if view.entry != resolved.entry
            || view.text.as_ref().is_some_and(|s| s.len() > query.limit)
            || view
                .next_offset
                .is_some_and(|offset| offset <= query.offset || offset != view.resolved_end)
            || view.resolved_start < query.offset
            || view.resolved_end < view.resolved_start
            || view.text.as_ref().is_some_and(|text| {
                text.len() != view.resolved_end.saturating_sub(view.resolved_start)
            })
            || result.authorized_content.iter().any(|r| {
                !native_project
                    && (!context.permits_content(r)
                        || (resolved.content.as_ref() != Some(r)
                            && !resolved.dependencies.contains(r)))
            })
        {
            return Err(FileQueryError::Integrity);
        }
        // Reauthorization after asynchronous IO closes the list/read race. It
        // also rejects a native path replaced while the reader was suspended.
        let current = catalog.resolve(context, &query.file_ref).await?;
        if current.entry.file_ref != query.file_ref {
            return Err(FileQueryError::Changed);
        }
        if result.authorized_content.iter().any(|r| {
            !native_project
                && current.content.as_ref() != Some(r)
                && !current.dependencies.contains(r)
        }) {
            return Err(FileQueryError::Denied);
        }
        self.reader.revalidate(context, &current).await?;
        Ok(result)
    }
}
