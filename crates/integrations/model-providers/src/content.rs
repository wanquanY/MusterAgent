use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{ContentId, TenantId};
use secrecy::SecretSlice;
use thiserror::Error;
use url::Url;

pub type ContentFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ResolvedContent, ContentResolverError>> + Send + 'a>>;

/// Provider-ready content obtained only after tenant authorization.
pub enum ResolvedContent {
    PublicUrl {
        media_type: MediaType,
        url: Url,
    },
    Inline {
        media_type: MediaType,
        bytes: SecretSlice<u8>,
    },
}

pub type ContentBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<ResolvedContent>, ContentResolverError>> + Send + 'a>>;

pub trait ContentResolver: Send + Sync {
    fn resolve<'a>(&'a self, tenant_id: TenantId, content_ref: &'a ContentRef)
    -> ContentFuture<'a>;

    /// Resolves every reference of one model request, in order. Owners that
    /// can read several references in one round trip override this; the
    /// default keeps the per-reference contract and its first failure.
    fn resolve_batch<'a>(
        &'a self,
        tenant_id: TenantId,
        content_refs: &'a [&'a ContentRef],
    ) -> ContentBatchFuture<'a> {
        Box::pin(async move {
            let mut resolved = Vec::with_capacity(content_refs.len());
            for content_ref in content_refs {
                resolved.push(self.resolve(tenant_id, content_ref).await?);
            }
            Ok(resolved)
        })
    }
}

#[derive(Default)]
pub struct ResolvedContents(BTreeMap<ContentId, ResolvedContent>);

impl ResolvedContents {
    pub fn insert(
        &mut self,
        content_ref: &ContentRef,
        resolved: ResolvedContent,
    ) -> Result<(), ContentResolverError> {
        let (media_type, byte_length): (&MediaType, Option<u64>) = match &resolved {
            ResolvedContent::PublicUrl { media_type, .. } => (media_type, None),
            ResolvedContent::Inline { media_type, bytes } => {
                use secrecy::ExposeSecret as _;
                (media_type, Some(bytes.expose_secret().len() as u64))
            }
        };
        if media_type != &content_ref.media_type {
            return Err(ContentResolverError::MediaTypeMismatch);
        }
        if byte_length.is_some_and(|length| length != content_ref.byte_length) {
            return Err(ContentResolverError::LengthMismatch);
        }
        if self.0.insert(content_ref.content_id, resolved).is_some() {
            return Err(ContentResolverError::DuplicateContent);
        }
        Ok(())
    }

    pub fn get(&self, content_ref: &ContentRef) -> Result<&ResolvedContent, ContentResolverError> {
        self.0
            .get(&content_ref.content_id)
            .ok_or(ContentResolverError::NotResolved)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ContentResolverError {
    #[error("content reference was not found")]
    NotFound,
    #[error("content reference is not authorized for this invocation")]
    Unauthorized,
    #[error("content reference exceeds Provider policy")]
    TooLarge,
    #[error("content store is unavailable")]
    Unavailable,
    #[error("resolved content media type differs from its immutable ContentRef")]
    MediaTypeMismatch,
    #[error("resolved inline content length differs from its immutable ContentRef")]
    LengthMismatch,
    #[error("content reference was resolved more than once")]
    DuplicateContent,
    #[error("content reference was not resolved before request mapping")]
    NotResolved,
}
