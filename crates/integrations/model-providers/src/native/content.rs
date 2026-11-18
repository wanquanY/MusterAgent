use std::sync::Arc;

use musteragent_content_ref::ContentRef;
use musteragent_identity::TenantId;
use musteragent_runtime_content::{ContentStore, ContentStoreError};
use musteragent_runtime_contract::ApplicationFuture;
use secrecy::SecretSlice;

use crate::{ContentBatchFuture, ContentResolver, ContentResolverError, ResolvedContent};

/// Reads through the original Content owner. This adapter never holds a second
/// blob store or accepts a filesystem path instead of the authorized reference.
pub struct StoredModelContentResolver {
    content: Arc<dyn ContentStore>,
}

impl StoredModelContentResolver {
    pub fn new(content: Arc<dyn ContentStore>) -> Self {
        Self { content }
    }
}

fn resolver_error(error: ContentStoreError) -> ContentResolverError {
    match error {
        ContentStoreError::NotFound => ContentResolverError::NotFound,
        ContentStoreError::Unauthorized
        | ContentStoreError::LengthMismatch
        | ContentStoreError::HashMismatch
        | ContentStoreError::IdentityConflict
        | ContentStoreError::Invariant => ContentResolverError::Unauthorized,
        ContentStoreError::TooLarge => ContentResolverError::TooLarge,
        ContentStoreError::Unavailable => ContentResolverError::Unavailable,
    }
}

fn inline(content_ref: &ContentRef, bytes: Vec<u8>) -> ResolvedContent {
    ResolvedContent::Inline {
        media_type: content_ref.media_type.clone(),
        bytes: SecretSlice::new(bytes.into_boxed_slice()),
    }
}

impl ContentResolver for StoredModelContentResolver {
    fn resolve<'a>(
        &'a self,
        tenant_id: TenantId,
        content_ref: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<ResolvedContent, ContentResolverError>> {
        Box::pin(async move {
            let bytes = self
                .content
                .read(tenant_id, content_ref)
                .await
                .map_err(resolver_error)?;
            Ok(inline(content_ref, bytes))
        })
    }

    /// One authorized owner read for all media of a model request instead of
    /// one metadata and blob round trip per reference.
    fn resolve_batch<'a>(
        &'a self,
        tenant_id: TenantId,
        content_refs: &'a [&'a ContentRef],
    ) -> ContentBatchFuture<'a> {
        Box::pin(async move {
            let owned = content_refs
                .iter()
                .map(|content_ref| (*content_ref).clone())
                .collect::<Vec<_>>();
            let payloads = self
                .content
                .read_batch(tenant_id, &owned)
                .await
                .map_err(resolver_error)?;
            if payloads.len() != content_refs.len() {
                return Err(ContentResolverError::Unauthorized);
            }
            Ok(content_refs
                .iter()
                .zip(payloads)
                .map(|(content_ref, bytes)| inline(content_ref, bytes))
                .collect())
        })
    }
}
