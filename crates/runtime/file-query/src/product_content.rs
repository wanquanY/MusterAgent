//! Product previews consume original bytes, while Agent reads consume semantic
//! representations. Both reauthorize the same exact file association/version.
use std::sync::Arc;

use musteragent_content_ref::ContentRef;
use musteragent_runtime_content::{ContentSink, ContentStore, MAX_CONTENT_BYTES, verify_content};

use crate::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileContentDescriptor {
    pub file_tree: Option<FileTreeDescriptor>,
    /// Exact Resource placement of the source's native deliverable. Structured
    /// Artifacts retain their manifest primary; the Resource may contain its
    /// declared export instead. Discovery is not a write grant.
    pub resource_ref: Option<FileReference>,
    pub entry: FileEntry,
    pub artifact_origin: Option<ResolvedArtifactVersion>,
    pub primary: ContentRef,
    pub dependencies: Vec<ContentRef>,
}

/// Original bytes are never serialized into model-visible Tool JSON.
#[derive(Debug)]
pub struct FileContentRead {
    pub descriptor: FileContentDescriptor,
    pub content: ContentRef,
    pub bytes: Vec<u8>,
    // Held through Product streaming. A closed consumer releases both bytes
    // and capacity; slow downloads cannot accumulate unbounded buffers.
    pub(crate) capacity: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl FileQueryApplication {
    #[must_use]
    pub fn with_content_store(mut self, content: Arc<dyn ContentStore>) -> Self {
        self.content = Some(content);
        self
    }

    pub async fn open(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
    ) -> Result<FileContentDescriptor, FileQueryError> {
        reference.validate(context)?;
        let catalog = self.catalog(reference.scope)?;
        let original = catalog.resolve(context, reference).await?;
        if original.entry.file_ref != *reference {
            return Err(FileQueryError::Changed);
        }
        if original.entry.is_directory {
            return Err(FileQueryError::InvalidQuery);
        }
        if original.entry.availability == FileAvailability::Unavailable {
            return Err(FileQueryError::Unavailable);
        }
        let material = self
            .reader
            .materialize(context, original.clone(), MAX_CONTENT_BYTES)
            .await?;
        let primary = material.content.ok_or(FileQueryError::MissingContent)?;
        // Only a source without stored content may establish an original native
        // identity. Providers may not replace Artifact/Attachment/Resource bytes.
        let physical = matches!(
            reference.source,
            FileSource::Native { .. }
                | FileSource::NativeCreation { .. }
                | crate::FileSource::Hosted { .. }
                | crate::FileSource::HostedCreation { .. }
        ) && context.project_workspace_id.is_some()
            && original.content.is_none();
        if material.entry != original.entry
            || material.artifact != original.artifact
            || (!physical
                && (original.content.as_ref() != Some(&primary)
                    || material.dependencies != original.dependencies))
            || (!physical
                && std::iter::once(&primary)
                    .chain(&material.dependencies)
                    .any(|r| !context.permits_content(r)))
        {
            return Err(FileQueryError::Denied);
        }
        let current = catalog.resolve(context, reference).await?;
        if current.entry.file_ref != *reference
            || current.content != original.content
            || current.dependencies != original.dependencies
            || current.artifact != original.artifact
        {
            return Err(FileQueryError::Changed);
        }
        Ok(FileContentDescriptor {
            entry: material.entry,
            artifact_origin: material.artifact,
            primary,
            dependencies: material.dependencies,
            file_tree: None,
            resource_ref: matches!(reference.source, FileSource::Resource { .. })
                .then(|| reference.clone()),
        })
    }

    pub async fn read_content(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
        content: &ContentRef,
    ) -> Result<FileContentRead, FileQueryError> {
        let descriptor = self.content_descriptor(context, reference, content).await?;
        let bytes = self.authorized_bytes(context, content).await?;
        self.recheck_content(context, reference, content).await?;
        Ok(FileContentRead {
            descriptor,
            content: content.clone(),
            bytes,
            capacity: None,
        })
    }

    /// Copies into caller-owned PRIVATE staging, never a live response or
    /// published share file. The caller must discard the sink on any error or
    /// future cancellation, including after all bytes have been written.
    /// There is deliberately no whole-file fallback for unsupported stores.
    pub async fn copy_content_to(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
        content: &ContentRef,
        sink: &mut dyn ContentSink,
    ) -> Result<FileContentDescriptor, FileQueryError> {
        let descriptor = self.content_descriptor(context, reference, content).await?;
        self.content
            .as_ref()
            .ok_or(FileQueryError::Unavailable)?
            .copy_to(context.actor.tenant_id, content, sink)
            .await
            .map_err(content_error)?;
        self.recheck_content(context, reference, content).await?;
        Ok(descriptor)
    }

    async fn content_descriptor(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
        content: &ContentRef,
    ) -> Result<FileContentDescriptor, FileQueryError> {
        let descriptor = self.open(context, reference).await?;
        if content.byte_length > MAX_CONTENT_BYTES {
            return Err(FileQueryError::Unsupported);
        }
        if descriptor.primary != *content && !descriptor.dependencies.contains(content) {
            return Err(FileQueryError::Denied);
        }
        Ok(descriptor)
    }

    async fn authorized_bytes(
        &self,
        context: &FileQueryContext,
        content: &ContentRef,
    ) -> Result<Vec<u8>, FileQueryError> {
        let bytes = self
            .content
            .as_ref()
            .ok_or(FileQueryError::Unavailable)?
            .read(context.actor.tenant_id, content)
            .await
            .map_err(content_error)?;
        verify_content(content, &bytes).map_err(|_| FileQueryError::Integrity)?;
        Ok(bytes)
    }

    async fn recheck_content(
        &self,
        context: &FileQueryContext,
        reference: &FileReference,
        content: &ContentRef,
    ) -> Result<(), FileQueryError> {
        // Fresh source resolution after IO fences rewind/deletion and native
        // version changes, including bytes served by the original Content cache.
        let current = self
            .catalog(reference.scope)?
            .resolve(context, reference)
            .await?;
        if current.entry.file_ref != *reference {
            return Err(FileQueryError::Changed);
        }
        if !matches!(
            reference.source,
            FileSource::Native { .. }
                | FileSource::NativeCreation { .. }
                | crate::FileSource::Hosted { .. }
                | crate::FileSource::HostedCreation { .. }
        ) && current.content.as_ref() != Some(content)
            && !current.dependencies.contains(content)
        {
            return Err(FileQueryError::Denied);
        }
        self.reader.revalidate(context, &current).await?;
        Ok(())
    }
}

fn content_error(error: musteragent_runtime_content::ContentStoreError) -> FileQueryError {
    use musteragent_runtime_content::ContentStoreError as E;
    match error {
        E::Unauthorized => FileQueryError::Denied,
        E::NotFound => FileQueryError::MissingContent,
        E::TooLarge => FileQueryError::Unsupported,
        E::Unavailable => FileQueryError::Unavailable,
        E::LengthMismatch | E::HashMismatch | E::IdentityConflict | E::Invariant => {
            FileQueryError::Integrity
        }
    }
}
