use super::*;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::ContentId;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Debug)]
struct Material;
impl RetainedContentMaterial for Material {
    fn retained_bytes(&self) -> usize {
        1
    }
}

fn batch(
    cache: Arc<VerifiedContentCache>,
    tenant: TenantId,
    reference: &ContentRef,
) -> ContentBatch {
    ContentBatch {
        payloads: HashMap::from([(reference.clone(), b"source".to_vec())]),
        reuse: Some((cache.clone(), cache.begin_read(tenant).unwrap())),
    }
}

fn reference() -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/pdf").unwrap(),
        byte_length: 6,
        sha256: Sha256::digest(b"source"),
    }
}

#[tokio::test]
async fn simultaneous_cold_reads_share_one_decode() {
    let cache = Arc::new(VerifiedContentCache::default());
    let tenant = TenantId::generate();
    let source = reference();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let (batch, source, calls) = (
            batch(cache.clone(), tenant, &source),
            source.clone(),
            calls.clone(),
        );
        tasks.push(tokio::spawn(async move {
            batch
                .decode_async(&source, "test@1", |_| async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    Ok::<_, ContentStoreError>(Material)
                })
                .await
                .unwrap()
        }));
    }
    let first = tasks.remove(0).await.unwrap();
    for task in tasks {
        assert!(Arc::ptr_eq(&first, &task.await.unwrap()));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // A different tenant or parser version cannot consume that material.
    for (tenant, purpose) in [(TenantId::generate(), "test@1"), (tenant, "test@2")] {
        batch(cache.clone(), tenant, &source)
            .decode_async(&source, purpose, |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ContentStoreError>(Material)
            })
            .await
            .unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn cancellation_releases_admission_and_does_not_publish_partial_material() {
    let cache = Arc::new(VerifiedContentCache::default());
    let tenant = TenantId::generate();
    let source = reference();
    let started = Arc::new(Notify::new());
    let task = {
        let (batch, source, started) = (
            batch(cache.clone(), tenant, &source),
            source.clone(),
            started.clone(),
        );
        tokio::spawn(async move {
            batch
                .decode_async(&source, "test@1", |_| async {
                    started.notify_one();
                    std::future::pending::<Result<Material, ContentStoreError>>().await
                })
                .await
        })
    };
    started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        batch(cache, tenant, &source).decode_async(&source, "test@1", |_| async {
            Ok::<_, ContentStoreError>(Material)
        }),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn erasure_during_decode_rejects_publication_and_result() {
    let cache = Arc::new(VerifiedContentCache::default());
    let tenant = TenantId::generate();
    let source = reference();
    let read = batch(cache.clone(), tenant, &source);
    let result = read
        .decode_async(&source, "test@1", |_| async {
            cache.invalidate_tenant(tenant).unwrap();
            Ok::<_, ContentStoreError>(Material)
        })
        .await;
    assert!(matches!(result, Err(ContentStoreError::Unavailable)));
    assert_eq!(cache.stats().unwrap().entries, 0);
}
