use crate::{
    FileEntry, FilePage, FileQueryAccess, FileQueryApplication, FileQueryAuthority,
    FileQueryContext, FileQueryError, FileReadResult, FileReference, ListFiles, ReadFile,
};
use std::sync::Arc;

/// Public application boundary shared by Product and sealed Tool handlers.
/// Every operation rechecks its permission owner after asynchronous work.
pub struct FileQueryService {
    authority: Arc<dyn FileQueryAuthority>,
    application: Arc<FileQueryApplication>,
    product_bytes: Arc<tokio::sync::Semaphore>,
}
impl FileQueryService {
    #[must_use]
    pub fn new(
        authority: Arc<dyn FileQueryAuthority>,
        application: Arc<FileQueryApplication>,
    ) -> Self {
        Self {
            authority,
            application,
            product_bytes: Arc::new(tokio::sync::Semaphore::new(768 * 1024 * 1024)),
        }
    }
    /// Resident composition shares capacity across ephemeral peer query services.
    #[must_use]
    pub fn with_product_capacity(mut self, capacity: Arc<tokio::sync::Semaphore>) -> Self {
        self.product_bytes = capacity;
        self
    }
    async fn recheck(
        &self,
        access: &FileQueryAccess,
        before: &FileQueryContext,
    ) -> Result<(), FileQueryError> {
        let current = self.authority.resolve(access).await?;
        if current.dialogue_source != before.dialogue_source
            || current.actor != before.actor
            || current.conversation_id != before.conversation_id
            || current.authority_revision != before.authority_revision
            || current.project_workspace_id != before.project_workspace_id
            || current.content_allowlist != before.content_allowlist
        {
            return Err(FileQueryError::Denied);
        }
        Ok(())
    }
    pub async fn list(
        &self,
        access: &FileQueryAccess,
        query: ListFiles,
    ) -> Result<FilePage, FileQueryError> {
        self.list_inner(
            access,
            query,
            false,
            &crate::activity::QueryActivity::new(None),
        )
        .await
    }
    /// Product-only mutation lifecycle observation; frozen Tool outputs retain
    /// their existing query schema and never acquire additional private metadata.
    pub async fn list_product(
        &self,
        access: &FileQueryAccess,
        query: ListFiles,
    ) -> Result<FilePage, FileQueryError> {
        if access.run.is_some() {
            return Err(FileQueryError::Denied);
        }
        self.list_inner(
            access,
            query,
            true,
            &crate::activity::QueryActivity::new(None),
        )
        .await
    }
    pub(crate) async fn list_observed(
        &self,
        access: &FileQueryAccess,
        query: ListFiles,
        activity: &crate::activity::QueryActivity<'_>,
    ) -> Result<FilePage, FileQueryError> {
        self.list_inner(access, query, false, activity).await
    }
    async fn list_inner(
        &self,
        access: &FileQueryAccess,
        query: ListFiles,
        product: bool,
        activity: &crate::activity::QueryActivity<'_>,
    ) -> Result<FilePage, FileQueryError> {
        activity.check_stop()?;
        let context = self.authority.resolve(access).await?;
        activity.completed()?;
        let scope = query.scope;
        let mut page = self.application.list(&context, query).await?;
        page.retraction_status = if product && scope == crate::FileScope::Conversation {
            self.application
                .catalog(scope)?
                .retraction_status(&context)
                .await?
        } else {
            None
        };
        activity.completed()?;
        self.recheck(access, &context).await?;
        activity.completed()?;
        Ok(page)
    }
    pub async fn resolve_identity(
        &self,
        access: &FileQueryAccess,
        identity: &crate::FileIdentity,
    ) -> Result<FileEntry, FileQueryError> {
        let context = self.authority.resolve(access).await?;
        let entry = self
            .application
            .resolve_identity(&context, identity)
            .await?;
        self.recheck(access, &context).await?;
        Ok(entry)
    }

    pub async fn refresh_reference(
        &self,
        access: &FileQueryAccess,
        reference: &FileReference,
    ) -> Result<FileEntry, FileQueryError> {
        let context = self.authority.resolve(access).await?;
        let entry = self
            .application
            .refresh_reference(&context, reference)
            .await?;
        self.recheck(access, &context).await?;
        Ok(entry)
    }

    pub async fn inspect(
        &self,
        access: &FileQueryAccess,
        reference: &FileReference,
    ) -> Result<FileEntry, FileQueryError> {
        self.inspect_observed(
            access,
            reference,
            &crate::activity::QueryActivity::new(None),
        )
        .await
    }
    /// Product-only explicit preparation. Sealed Tool contracts do not acquire
    /// a new operation or silently broaden their accepted source/version set.
    pub async fn prepare_content(
        &self,
        access: &FileQueryAccess,
        reference: &FileReference,
    ) -> Result<FileEntry, FileQueryError> {
        if access.run.is_some() {
            return Err(FileQueryError::Denied);
        }
        let context = self.authority.resolve(access).await?;
        let entry = self
            .application
            .prepare_content(&context, reference)
            .await?;
        self.recheck(access, &context).await?;
        Ok(entry)
    }
    pub(crate) async fn inspect_observed(
        &self,
        access: &FileQueryAccess,
        reference: &FileReference,
        activity: &crate::activity::QueryActivity<'_>,
    ) -> Result<FileEntry, FileQueryError> {
        activity.check_stop()?;
        let context = self.authority.resolve(access).await?;
        activity.completed()?;
        let entry = self.application.inspect(&context, reference).await?;
        activity.completed()?;
        self.recheck(access, &context).await?;
        activity.completed()?;
        Ok(entry)
    }
    pub async fn read(
        &self,
        access: &FileQueryAccess,
        query: ReadFile,
    ) -> Result<FileReadResult, FileQueryError> {
        self.read_observed(access, query, &crate::activity::QueryActivity::new(None))
            .await
    }
    pub(crate) async fn read_observed(
        &self,
        access: &FileQueryAccess,
        query: ReadFile,
        activity: &crate::activity::QueryActivity<'_>,
    ) -> Result<FileReadResult, FileQueryError> {
        activity.check_stop()?;
        let context = self.authority.resolve(access).await?;
        activity.completed()?;
        let mut read = self.application.read(&context, query.clone()).await?;
        // A pending representation is a read dependency, not a failed Tool or
        // evidence that the user's file is broken. Own a finite cooperative
        // wait here; Product inspection returns immediately. No SQL lease is
        // retained, and polling never manufactures execution progress.
        if activity.can_wait() {
            for seconds in [1, 2, 3, 4, 5, 5] {
                if read.view.extraction_status != "preparing" {
                    break;
                }
                activity
                    .wait_dependency(std::time::Duration::from_secs(seconds))
                    .await?;
                self.recheck(access, &context).await?;
                read = self.application.read(&context, query.clone()).await?;
            }
        }
        activity.completed()?;
        self.recheck(access, &context).await?;
        activity.completed()?;
        Ok(read)
    }
    pub async fn search(
        &self,
        access: &FileQueryAccess,
        query: crate::SearchFiles,
    ) -> Result<crate::FileSearchPage, FileQueryError> {
        self.search_observed(
            access,
            query,
            &crate::activity::QueryActivity::new(None),
            crate::source_contract::FileSourceContract::Product,
        )
        .await
    }
    pub(crate) async fn search_observed(
        &self,
        access: &FileQueryAccess,
        query: crate::SearchFiles,
        activity: &crate::activity::QueryActivity<'_>,
        contract: crate::source_contract::FileSourceContract,
    ) -> Result<crate::FileSearchPage, FileQueryError> {
        activity.check_stop()?;
        let context = self.authority.resolve(access).await?;
        activity.completed()?;
        let result = self
            .application
            .search_observed(&context, query, activity, contract)
            .await?;
        activity.completed()?;
        self.recheck(access, &context).await?;
        activity.completed()?;
        Ok(result)
    }

    pub async fn open(
        &self,
        access: &FileQueryAccess,
        reference: &FileReference,
    ) -> Result<crate::FileContentDescriptor, FileQueryError> {
        let context = self.authority.resolve(access).await?;
        let entry = self.application.inspect(&context, reference).await?;
        let _capacity = self.reserve_product_bytes(&entry, 0)?;
        let result = self.application.open(&context, reference).await?;
        self.recheck(access, &context).await?;
        Ok(result)
    }

    pub async fn read_content(
        &self,
        access: &FileQueryAccess,
        reference: &FileReference,
        content: &musteragent_content_ref::ContentRef,
    ) -> Result<crate::FileContentRead, FileQueryError> {
        let context = self.authority.resolve(access).await?;
        let entry = self.application.inspect(&context, reference).await?;
        let capacity = self.reserve_product_bytes(&entry, content.byte_length)?;
        let mut result = self
            .application
            .read_content(&context, reference, content)
            .await?;
        self.recheck(access, &context).await?;
        result.capacity = Some(capacity);
        Ok(result)
    }

    pub async fn open_preview(
        &self,
        access: &FileQueryAccess,
        reference: &FileReference,
    ) -> Result<crate::FileContentDescriptor, FileQueryError> {
        let context = self.authority.resolve(access).await?;
        let entry = self.application.inspect(&context, reference).await?;
        let _capacity = self.reserve_product_bytes(&entry, 0)?;
        let result = self.application.open_preview(&context, reference).await?;
        self.recheck(access, &context).await?;
        Ok(result)
    }

    /// Bounded original-byte transfer for native previews/exports. The sink
    /// remains private until this entire operation succeeds: source association
    /// and current authority are rechecked AFTER the copy. Cancellation/error
    /// requires discard, not publication of a partially filled destination.
    pub async fn copy_content_to(
        &self,
        access: &FileQueryAccess,
        reference: &FileReference,
        content: &musteragent_content_ref::ContentRef,
        sink: &mut dyn musteragent_runtime_content::ContentSink,
    ) -> Result<crate::FileContentDescriptor, FileQueryError> {
        let context = self.authority.resolve(access).await?;
        let entry = self.application.inspect(&context, reference).await?;
        // Stored content uses bounded source/sink chunks regardless of file
        // length. Physical native/hosted materialization still reserves its
        // original full-file budget in the same helper as buffered reads.
        let selected_size = if entry.media_type.as_deref()
            == Some("application/vnd.openxmlformats-officedocument.presentationml.presentation")
            && content.media_type.as_str() == "image/png"
        {
            // The renderer yields a bounded page buffer, unlike original-byte
            // ContentStore streaming. Reserve its entire size before reading.
            content.byte_length
        } else {
            musteragent_runtime_content::CONTENT_TRANSFER_CHUNK_BYTES as u64
        };
        let _capacity = self.reserve_product_bytes(&entry, selected_size)?;
        let result = self
            .application
            .copy_content_to(&context, reference, content, sink)
            .await?;
        self.recheck(access, &context).await?;
        Ok(result)
    }

    fn reserve_product_bytes(
        &self,
        entry: &FileEntry,
        selected_size: u64,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, FileQueryError> {
        let native_size = if matches!(
            entry.file_ref.source,
            crate::FileSource::Native { .. }
                | crate::FileSource::NativeCreation { .. }
                | crate::FileSource::Hosted { .. }
                | crate::FileSource::HostedCreation { .. }
        ) {
            entry.byte_length.ok_or(FileQueryError::Integrity)?
        } else {
            0
        };
        let size = native_size.max(selected_size);
        if size > musteragent_runtime_content::MAX_CONTENT_BYTES {
            return Err(FileQueryError::Unsupported);
        }
        // Covers the original read and immutable materialization copy. Empty
        // files still consume capacity, preventing unlimited zero-byte streams.
        let bytes = u32::try_from(size.max(64 * 1024).saturating_mul(2))
            .map_err(|_| FileQueryError::Unsupported)?;
        self.product_bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| FileQueryError::Unavailable)
    }
}
