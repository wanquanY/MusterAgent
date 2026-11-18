use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::{
    ContentStore, ContentStoreError, PutContent, VerifiedContentCache,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::read_stored_file_content;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct Store {
    bytes: Vec<u8>,
    allowed: AtomicBool,
    authorizations: AtomicUsize,
    cache: Arc<VerifiedContentCache>,
}
impl ContentStore for Store {
    fn material_cache(&self) -> Option<Arc<VerifiedContentCache>> {
        Some(self.cache.clone())
    }
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        panic!("read cannot write")
    }
    fn read<'a>(
        &'a self,
        _: TenantId,
        _: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.authorizations.fetch_add(1, Ordering::SeqCst);
            if self.allowed.load(Ordering::SeqCst) {
                Ok(self.bytes.clone())
            } else {
                Err(ContentStoreError::Unauthorized)
            }
        })
    }
}
#[tokio::test]
async fn parser_material_reuses_exact_content_but_never_replaces_current_authorization()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = b"first second".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain")?,
        sha256: Sha256::digest(&bytes),
        byte_length: bytes.len() as u64,
    };
    let store = Store {
        bytes,
        allowed: AtomicBool::new(true),
        authorizations: AtomicUsize::new(0),
        cache: Arc::new(VerifiedContentCache::default()),
    };
    let tenant = TenantId::generate();
    assert_eq!(
        read_stored_file_content(&store, tenant, &reference, 0, 6, false)
            .await?
            .content
            .as_deref(),
        Some("first ")
    );
    assert_eq!(
        read_stored_file_content(&store, tenant, &reference, 6, 6, false)
            .await?
            .content
            .as_deref(),
        Some("second")
    );
    assert_eq!(store.authorizations.load(Ordering::SeqCst), 2);
    assert_eq!(store.cache.stats()?.material_hits, 1);
    store.allowed.store(false, Ordering::SeqCst);
    assert!(
        read_stored_file_content(&store, tenant, &reference, 0, 6, false)
            .await
            .is_err()
    );
    assert_eq!(
        store.cache.stats()?.material_hits,
        1,
        "denied requests never consult retained parser output"
    );
    store.cache.invalidate_tenant(tenant)?;
    store.allowed.store(true, Ordering::SeqCst);
    read_stored_file_content(&store, tenant, &reference, 0, 6, false).await?;
    assert_eq!(
        store.cache.stats()?.material_hits,
        1,
        "erasure discarded parsed material with raw bytes"
    );
    Ok(())
}
