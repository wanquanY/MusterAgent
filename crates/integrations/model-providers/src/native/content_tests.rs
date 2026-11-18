use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use secrecy::ExposeSecret as _;

use super::StoredModelContentResolver;
use crate::{ContentResolver, ContentResolverError, ResolvedContent};

struct ContentProbe {
    tenant: TenantId,
    reference: ContentRef,
    error: Option<ContentStoreError>,
}

impl ContentStore for ContentProbe {
    fn put(
        &self,
        _request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    fn read<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            assert_eq!(
                reference, &self.reference,
                "immutable reference is passed unchanged"
            );
            if tenant != self.tenant {
                return Err(ContentStoreError::Unauthorized);
            }
            if let Some(error) = self.error {
                return Err(error);
            }
            Ok(b"image-fixture".to_vec())
        })
    }
}

#[tokio::test]
async fn original_content_owner_retains_tenant_reference_and_failure_semantics() {
    let tenant = TenantId::generate();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").expect("media type"),
        byte_length: 13,
        sha256: Sha256::digest(b"image-fixture"),
    };
    for (error, expected) in [
        (ContentStoreError::NotFound, ContentResolverError::NotFound),
        (
            ContentStoreError::Unauthorized,
            ContentResolverError::Unauthorized,
        ),
        (
            ContentStoreError::HashMismatch,
            ContentResolverError::Unauthorized,
        ),
        (
            ContentStoreError::LengthMismatch,
            ContentResolverError::Unauthorized,
        ),
        (
            ContentStoreError::IdentityConflict,
            ContentResolverError::Unauthorized,
        ),
        (
            ContentStoreError::Invariant,
            ContentResolverError::Unauthorized,
        ),
        (ContentStoreError::TooLarge, ContentResolverError::TooLarge),
        (
            ContentStoreError::Unavailable,
            ContentResolverError::Unavailable,
        ),
    ] {
        let resolver = StoredModelContentResolver::new(Arc::new(ContentProbe {
            tenant,
            reference: reference.clone(),
            error: Some(error),
        }));
        assert_eq!(
            resolver.resolve(tenant, &reference).await.err(),
            Some(expected)
        );
    }
    let resolver = StoredModelContentResolver::new(Arc::new(ContentProbe {
        tenant,
        reference: reference.clone(),
        error: None,
    }));
    assert_eq!(
        resolver
            .resolve(TenantId::generate(), &reference)
            .await
            .err(),
        Some(ContentResolverError::Unauthorized)
    );
    let ResolvedContent::Inline { bytes, media_type } = resolver
        .resolve(tenant, &reference)
        .await
        .expect("exact content")
    else {
        panic!("stored material must be inline")
    };
    assert_eq!(bytes.expose_secret(), b"image-fixture");
    assert_eq!(media_type, reference.media_type);
}

struct BatchProbe {
    batches: std::sync::atomic::AtomicUsize,
}

impl ContentStore for BatchProbe {
    fn put(
        &self,
        _request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    fn read<'a>(
        &'a self,
        _tenant: TenantId,
        _reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        panic!("model media must be read as one batch")
    }

    fn read_batch<'a>(
        &'a self,
        _tenant: TenantId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<Vec<u8>>, ContentStoreError>> {
        self.batches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            Ok(references
                .iter()
                .map(|reference| reference.content_id.to_string().into_bytes())
                .collect())
        })
    }
}

#[tokio::test]
async fn request_media_resolves_through_one_ordered_owner_batch() {
    let tenant = TenantId::generate();
    let references = (0..3)
        .map(|_| ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("image/png").expect("media type"),
            byte_length: 36,
            sha256: Sha256::digest(b"unused"),
        })
        .collect::<Vec<_>>();
    let borrowed = references.iter().collect::<Vec<_>>();
    let store = Arc::new(BatchProbe {
        batches: std::sync::atomic::AtomicUsize::new(0),
    });
    let resolver = StoredModelContentResolver::new(store.clone());
    let resolved = resolver
        .resolve_batch(tenant, &borrowed)
        .await
        .expect("batch");
    assert_eq!(store.batches.load(std::sync::atomic::Ordering::SeqCst), 1);
    for (reference, resolved) in references.iter().zip(resolved) {
        let ResolvedContent::Inline { bytes, media_type } = resolved else {
            panic!("stored material must be inline")
        };
        assert_eq!(
            bytes.expose_secret(),
            reference.content_id.to_string().as_bytes()
        );
        assert_eq!(media_type, reference.media_type);
    }
}
