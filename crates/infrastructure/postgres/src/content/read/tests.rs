use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{DottedKey, TenantId};
use musteragent_runtime_content::{
    ApplicationFuture, BlobIdentity, BlobPutDisposition, BlobStore, BlobStoreError, ContentStore,
    ContentStoreError, PutBlob, PutBlobReceipt, PutContent, StorageLocator,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::{PostgresContentStore, PostgresTieredBlobStore, migrate};

struct Blobs {
    bodies: Mutex<HashMap<BlobIdentity, Vec<u8>>>,
    reads: AtomicUsize,
    pause: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}
impl Default for Blobs {
    fn default() -> Self {
        Self {
            bodies: Mutex::default(),
            reads: AtomicUsize::new(0),
            pause: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}
impl BlobStore for Blobs {
    fn put_stream<'a>(
        &'a self,
        _identity: musteragent_runtime_content::BlobIdentity,
        _length: u64,
        _hash: musteragent_content_ref::Sha256,
        _source: &'a mut dyn musteragent_runtime_content::ContentSource,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<
            musteragent_runtime_content::PutBlobReceipt,
            musteragent_runtime_content::ContentStoreError,
        >,
    > {
        panic!("cache fixture does not exercise streaming writes")
    }
    fn copy_to<'a>(
        &'a self,
        _identity: musteragent_runtime_content::BlobIdentity,
        _locator: &'a musteragent_runtime_content::StorageLocator,
        _length: u64,
        _hash: musteragent_content_ref::Sha256,
        _sink: &'a mut dyn musteragent_runtime_content::ContentSink,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<(), musteragent_runtime_content::ContentStoreError>,
    > {
        panic!("cache fixture does not exercise streaming reads")
    }

    fn put(
        &self,
        request: PutBlob,
    ) -> ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>> {
        Box::pin(async move {
            let locator = self.derive_locator(request.identity)?;
            let mut bodies = self.bodies.lock().expect("read put fixture");
            let disposition = if let Some(previous) = bodies.get(&request.identity) {
                if *previous != request.bytes {
                    return Err(BlobStoreError::IdentityConflict);
                }
                BlobPutDisposition::AlreadyPresent
            } else {
                bodies.insert(request.identity, request.bytes);
                BlobPutDisposition::Created
            };
            Ok(PutBlobReceipt {
                locator,
                disposition,
            })
        })
    }
    fn read<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, BlobStoreError>> {
        Box::pin(async move {
            self.validate_locator(identity, locator)?;
            self.reads.fetch_add(1, Ordering::SeqCst);
            let bytes = self
                .bodies
                .lock()
                .expect("read read fixture")
                .get(&identity)
                .cloned()
                .ok_or(BlobStoreError::Unavailable)?;
            if self.pause.load(Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.release
                    .acquire()
                    .await
                    .expect("read read fixture")
                    .forget();
            }
            Ok(bytes)
        })
    }
    fn delete<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<(), BlobStoreError>> {
        Box::pin(async move {
            self.validate_locator(identity, locator)?;
            self.bodies
                .lock()
                .expect("read delete fixture")
                .remove(&identity);
            Ok(())
        })
    }
    fn derive_locator(&self, identity: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        StorageLocator::new(format!("{}/{}", identity.tenant_id, identity.content_id))
    }
}
struct Fixture {
    _postgres: IsolatedPostgres,
    pool: PgPool,
    store: PostgresContentStore,
    blobs: Arc<Blobs>,
    tenant: TenantId,
}
impl Fixture {
    async fn new() -> Self {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("read new fixture");
        let pool = postgres.pool().await.expect("read new fixture");
        migrate(&pool).await.expect("read new fixture");
        let key_id = DottedKey::new("test.lossless.content").expect("read new fixture");
        let keys = Arc::new(
            OpaqueRefKeyring::new(key_id.clone(), [(key_id, [31; 32])]).expect("read new fixture"),
        );
        let blobs = Arc::new(Blobs::default());
        let tiered = Arc::new(PostgresTieredBlobStore::new(pool.clone(), blobs.clone()));
        Self {
            store: PostgresContentStore::new(pool.clone(), tiered, keys),
            pool,
            blobs,
            tenant: TenantId::generate(),
            _postgres: postgres,
        }
    }
    async fn put(&self, bytes: Vec<u8>) -> ContentRef {
        self.store
            .put(PutContent {
                tenant_id: self.tenant,
                media_type: MediaType::new("application/octet-stream").expect("read put fixture"),
                bytes,
            })
            .await
            .expect("read put fixture")
    }
    async fn tombstone(&self, reference: &ContentRef) {
        sqlx::query("UPDATE artifact.content_refs SET state=3 WHERE content_id=$1")
            .bind(reference.content_id.into_uuid())
            .execute(&self.pool)
            .await
            .expect("tombstone fixture");
    }
}

#[tokio::test]
async fn hot_batch_preserves_144_items_duplicates_and_fresh_full_reference_authorization() {
    let f = Fixture::new().await;
    let watch = f.store.connect_cache_watch().await.expect(
        "hot batch preserves 144 items duplicates and fresh full reference authorization fixture",
    );
    let mut refs = Vec::new();
    let mut expected = Vec::new();
    for n in 0_u32..144 {
        let bytes = n.to_le_bytes().repeat(if n == 2 { 80_000 } else { 1024 });
        refs.push(f.put(bytes.clone()).await);
        expected.push(bytes);
    }
    refs.reverse();
    expected.reverse();
    refs.push(refs[1].clone());
    expected.push(expected[1].clone());
    assert_eq!(f.store.read_batch(f.tenant, &refs).await.expect("hot batch preserves 144 items duplicates and fresh full reference authorization fixture"), expected);
    let reads = f.blobs.reads.load(Ordering::SeqCst);
    assert_eq!(reads, 1);
    assert_eq!(f.store.cache_stats().expect("hot batch preserves 144 items duplicates and fresh full reference authorization fixture").entries, 144);
    let revision: i64 = sqlx::query_scalar("SELECT revision FROM artifact.content_cache_revision")
        .fetch_one(&f.pool)
        .await
        .expect("hot batch preserves 144 items duplicates and fresh full reference authorization fixture");
    f.put(b"new publication does not invalidate history".to_vec())
        .await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM artifact.content_cache_revision")
            .fetch_one(&f.pool)
            .await
            .expect("hot batch preserves 144 items duplicates and fresh full reference authorization fixture"),
        revision
    );
    assert_eq!(f.store.read_batch(f.tenant, &refs).await.expect("hot batch preserves 144 items duplicates and fresh full reference authorization fixture"), expected);
    assert_eq!(f.blobs.reads.load(Ordering::SeqCst), reads);
    assert_eq!(f.store.cache_stats().expect("hot batch preserves 144 items duplicates and fresh full reference authorization fixture").hits, 144);
    assert_eq!(
        f.store.read(TenantId::generate(), &refs[0]).await,
        Err(ContentStoreError::Unauthorized)
    );
    let mut forged = refs[0].clone();
    forged.byte_length += 1;
    assert_eq!(
        f.store.read(f.tenant, &forged).await,
        Err(ContentStoreError::Invariant)
    );
    drop(watch);
    assert_eq!(f.store.cache_stats().expect("hot batch preserves 144 items duplicates and fresh full reference authorization fixture").entries, 0);
}

#[tokio::test]
async fn public_release_batches_preserve_storage_scope_and_fresh_revocation() {
    let f = Fixture::new().await;
    let _watch = f.store.connect_cache_watch().await.expect("cache watch");
    let public_inline_bytes = b"public inline release".to_vec();
    let public_external_bytes = vec![29; 300_000];
    let mut public = Vec::new();
    for bytes in [&public_inline_bytes, &public_external_bytes] {
        public.push(
            f.store
                .put_runtime_release(PutContent {
                    tenant_id: f.tenant,
                    media_type: MediaType::new("application/octet-stream").expect("media type"),
                    bytes: bytes.clone(),
                })
                .await
                .expect("public release"),
        );
    }
    let private_bytes = b"private material".to_vec();
    let private = f.put(private_bytes.clone()).await;
    let refs = vec![
        public[1].clone(),
        private.clone(),
        public[0].clone(),
        public[1].clone(),
    ];
    let expected = vec![
        public_external_bytes.clone(),
        private_bytes,
        public_inline_bytes.clone(),
        public_external_bytes.clone(),
    ];
    for _ in 0..2 {
        assert_eq!(
            f.store
                .read_batch(f.tenant, &refs)
                .await
                .expect("mixed batch"),
            expected
        );
    }
    assert_eq!(f.blobs.reads.load(Ordering::SeqCst), 1);
    assert_eq!(f.store.cache_stats().expect("cache stats").entries, 3);
    let another = TenantId::generate();
    assert_eq!(
        f.store
            .read_batch(another, &public)
            .await
            .expect("public batch"),
        vec![public_inline_bytes, public_external_bytes]
    );
    assert_eq!(
        f.store.read_batch(another, &refs).await,
        Err(ContentStoreError::Unauthorized)
    );
    let mut forged = public[0].clone();
    forged.byte_length += 1;
    assert_eq!(
        f.store.read(another, &forged).await,
        Err(ContentStoreError::Invariant)
    );

    // An unconsumed invalidation must not expose either cached public body.
    sqlx::query("UPDATE artifact.inline_content_blobs SET content_bytes=$1 WHERE content_id=$2")
        .bind(vec![0_u8; public[0].byte_length as usize])
        .bind(public[0].content_id.into_uuid())
        .execute(&f.pool)
        .await
        .expect("alter public body");
    assert!(f.store.read(f.tenant, &public[0]).await.is_err());
    f.tombstone(&public[1]).await;
    assert_eq!(
        f.store.read(another, &public[1]).await,
        Err(ContentStoreError::NotFound)
    );
}

#[tokio::test]
async fn cache_publication_lock_wait_is_bounded_and_does_not_publish_on_timeout() {
    let f = Fixture::new().await;
    let _watch = f.store.connect_cache_watch().await.expect("cache watch");
    let reference = f.put(b"bounded cache publication".to_vec()).await;
    let mut blocker = f.pool.begin().await.expect("isolated revision lock owner");
    sqlx::query("SELECT revision FROM artifact.content_cache_revision WHERE singleton FOR UPDATE")
        .fetch_one(&mut *blocker)
        .await
        .expect("hold cache publication revision lock");

    let result = tokio::time::timeout(Duration::from_secs(8), f.store.read(f.tenant, &reference))
        .await
        .expect("the governed lock timeout must bound cache publication");
    assert_eq!(result, Err(ContentStoreError::Unavailable));
    assert_eq!(f.store.cache_stats().expect("cache stats").entries, 0);

    blocker.rollback().await.expect("release revision lock");
    assert_eq!(
        f.store
            .read(f.tenant, &reference)
            .await
            .expect("read after lock release"),
        b"bounded cache publication"
    );
    assert_eq!(f.store.cache_stats().expect("cache stats").entries, 1);
}

#[tokio::test]
async fn deletion_is_authoritative_even_when_notification_has_not_been_consumed() {
    let f = Fixture::new().await;
    let _watch =
        f.store.connect_cache_watch().await.expect(
            "deletion is authoritative even when notification has not been consumed fixture",
        );
    let reference = f.put(b"original".to_vec()).await;
    f.store
        .read(f.tenant, &reference)
        .await
        .expect("deletion is authoritative even when notification has not been consumed fixture");
    f.tombstone(&reference).await;
    assert_eq!(
        f.store.read(f.tenant, &reference).await,
        Err(ContentStoreError::NotFound)
    );
    assert_eq!(
        f.store
            .cache_stats()
            .expect(
                "deletion is authoritative even when notification has not been consumed fixture"
            )
            .entries,
        0
    );
}

#[tokio::test]
async fn late_external_read_cannot_publish_across_committed_deletion() {
    let f = Fixture::new().await;
    let _watch = f
        .store
        .connect_cache_watch()
        .await
        .expect("late external read cannot publish across committed deletion fixture");
    let reference = f.put(vec![7; 300_000]).await;
    f.blobs.pause.store(true, Ordering::SeqCst);
    let store = f.store.clone();
    let tenant = f.tenant;
    let delayed = reference.clone();
    let reader = tokio::spawn(async move { store.read(tenant, &delayed).await });
    tokio::time::timeout(Duration::from_secs(2), f.blobs.entered.acquire())
        .await
        .expect("late external read cannot publish across committed deletion fixture")
        .expect("late external read cannot publish across committed deletion fixture")
        .forget();
    f.tombstone(&reference).await;
    f.blobs.release.add_permits(1);
    assert_eq!(
        reader
            .await
            .expect("late external read cannot publish across committed deletion fixture"),
        Err(ContentStoreError::NotFound)
    );
    assert_eq!(
        f.store
            .cache_stats()
            .expect("late external read cannot publish across committed deletion fixture")
            .entries,
        0
    );
}

#[tokio::test]
async fn unrelated_tenant_revocations_do_not_restart_or_fail_an_authorized_external_read() {
    let f = Fixture::new().await;
    let _watch = f.store.connect_cache_watch().await.expect("cache watch");
    let bytes = vec![9; 300_000];
    let reference = f.put(bytes.clone()).await;
    let other_tenant = TenantId::generate();
    let unrelated = f
        .store
        .put(PutContent {
            tenant_id: other_tenant,
            media_type: MediaType::new("application/octet-stream").expect("media type"),
            bytes: b"another tenant's disposable content".to_vec(),
        })
        .await
        .expect("unrelated content");
    f.blobs.pause.store(true, Ordering::SeqCst);
    let store = f.store.clone();
    let tenant = f.tenant;
    let reader = tokio::spawn(async move { store.read(tenant, &reference).await });
    tokio::time::timeout(Duration::from_secs(2), f.blobs.entered.acquire())
        .await
        .expect("external read entered")
        .expect("read signal")
        .forget();

    // Both metadata and inline deletion invalidate the global revision, while
    // the exact reference being read remains authorized and byte-identical.
    f.store
        .delete(other_tenant, &unrelated)
        .await
        .expect("unrelated deletion");
    f.blobs.release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .expect("unrelated invalidation must not restart object I/O")
            .expect("reader task"),
        Ok(bytes),
    );
    assert_eq!(f.blobs.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn inline_body_tampering_during_a_batch_cannot_return_previously_verified_bytes() {
    let f = Fixture::new().await;
    let _watch = f.store.connect_cache_watch().await.expect("cache watch");
    let inline = f.put(b"original".to_vec()).await;
    let external = f.put(vec![3; 300_000]).await;
    // Populate L1 first: the batch will initially reuse these verified bytes.
    f.store
        .read(f.tenant, &inline)
        .await
        .expect("warm inline bytes");
    f.blobs.pause.store(true, Ordering::SeqCst);
    let store = f.store.clone();
    let tenant = f.tenant;
    let refs = [inline.clone(), external];
    let reader = tokio::spawn(async move { store.read_batch(tenant, &refs).await });
    tokio::time::timeout(Duration::from_secs(2), f.blobs.entered.acquire())
        .await
        .expect("external read entered")
        .expect("read signal")
        .forget();
    // Leave declared hash/length intact: metadata identity alone cannot detect
    // this change. The inline tuple version must force fresh body verification.
    sqlx::query("UPDATE artifact.inline_content_blobs SET content_bytes=$2 WHERE content_id=$1")
        .bind(inline.content_id.into_uuid())
        .bind(b"tampered".as_slice())
        .execute(&f.pool)
        .await
        .expect("tamper inline body");
    f.blobs.release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .expect("fresh integrity check is bounded")
            .expect("reader task"),
        Err(ContentStoreError::HashMismatch),
    );
    assert_eq!(f.store.cache_stats().expect("cache stats").entries, 0);
}

#[tokio::test]
async fn rollback_keeps_cache_but_committed_body_change_clears_and_reverifies() {
    let f = Fixture::new().await;
    let _watch = f
        .store
        .connect_cache_watch()
        .await
        .expect("rollback keeps cache but committed body change clears and reverifies fixture");
    let reference = f.put(b"original".to_vec()).await;
    f.store
        .read(f.tenant, &reference)
        .await
        .expect("rollback keeps cache but committed body change clears and reverifies fixture");
    let before = f
        .store
        .cache_stats()
        .expect("rollback keeps cache but committed body change clears and reverifies fixture");
    let mut tx = f
        .pool
        .begin()
        .await
        .expect("rollback keeps cache but committed body change clears and reverifies fixture");
    sqlx::query("UPDATE artifact.content_refs SET state=3 WHERE content_id=$1")
        .bind(reference.content_id.into_uuid())
        .execute(&mut *tx)
        .await
        .expect("rollback keeps cache but committed body change clears and reverifies fixture");
    tx.rollback()
        .await
        .expect("rollback keeps cache but committed body change clears and reverifies fixture");
    assert_eq!(
        f.store
            .read(f.tenant, &reference)
            .await
            .expect("rollback keeps cache but committed body change clears and reverifies fixture"),
        b"original"
    );
    assert_eq!(
        f.store
            .cache_stats()
            .expect("rollback keeps cache but committed body change clears and reverifies fixture")
            .invalidations,
        before.invalidations
    );
    sqlx::query("UPDATE artifact.inline_content_blobs SET content_bytes=$2 WHERE content_id=$1")
        .bind(reference.content_id.into_uuid())
        .bind(b"tampered".as_slice())
        .execute(&f.pool)
        .await
        .expect("rollback keeps cache but committed body change clears and reverifies fixture");
    assert_eq!(
        f.store.read(f.tenant, &reference).await,
        Err(ContentStoreError::HashMismatch)
    );
    assert_eq!(
        f.store
            .cache_stats()
            .expect("rollback keeps cache but committed body change clears and reverifies fixture")
            .entries,
        0
    );
}

#[tokio::test]
async fn supervised_listener_erases_idle_memory_and_drop_disables_retention() {
    let f = Fixture::new().await;
    let reference = f.put(b"idle history".to_vec()).await;
    f.store
        .read(f.tenant, &reference)
        .await
        .expect("supervised listener erases idle memory and drop disables retention fixture");
    assert_eq!(
        f.store
            .cache_stats()
            .expect("supervised listener erases idle memory and drop disables retention fixture")
            .entries,
        0,
        "no listener means no retained L1"
    );
    let watch = f
        .store
        .connect_cache_watch()
        .await
        .expect("supervised listener erases idle memory and drop disables retention fixture");
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(watch.run(cancellation.clone()));
    f.store
        .read(f.tenant, &reference)
        .await
        .expect("supervised listener erases idle memory and drop disables retention fixture");
    assert_eq!(
        f.store
            .cache_stats()
            .expect("supervised listener erases idle memory and drop disables retention fixture")
            .entries,
        1
    );
    f.tombstone(&reference).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while f
            .store
            .cache_stats()
            .expect("supervised listener erases idle memory and drop disables retention fixture")
            .entries
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("supervised listener erases idle memory and drop disables retention fixture");
    cancellation.cancel();
    task.await
        .expect("supervised listener erases idle memory and drop disables retention fixture")
        .expect("supervised listener erases idle memory and drop disables retention fixture");
    let other = f
        .put(b"still readable after listener shutdown".to_vec())
        .await;
    assert!(f.store.read(f.tenant, &other).await.is_ok());
    assert_eq!(
        f.store
            .cache_stats()
            .expect("supervised listener erases idle memory and drop disables retention fixture")
            .entries,
        0
    );
}

#[tokio::test]
async fn listener_reconnect_gap_clears_cached_bytes_without_waiting_for_another_notification() {
    let f = Fixture::new().await;
    let reference = f
        .put(b"history at risk of missed deletion notification".to_vec())
        .await;
    let mut watch = f.store.connect_cache_watch().await.expect("listener reconnect gap clears cached bytes without waiting for another notification fixture");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut watch.listener)
        .await
        .expect("listener reconnect gap clears cached bytes without waiting for another notification fixture");
    f.store.read(f.tenant, &reference).await.expect("listener reconnect gap clears cached bytes without waiting for another notification fixture");
    let before = f.store.cache_stats().expect("listener reconnect gap clears cached bytes without waiting for another notification fixture").invalidations;
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .execute(&f.pool)
        .await
        .expect("listener reconnect gap clears cached bytes without waiting for another notification fixture");
    // The listener was not polled at termination. Start its supervisor only
    // now to deterministically exercise the reconnect gap with retained data.
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(watch.run(cancellation.clone()));
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.store.cache_stats().expect("listener reconnect gap clears cached bytes without waiting for another notification fixture").invalidations == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener reconnect gap clears cached bytes without waiting for another notification fixture");
    assert_eq!(f.store.cache_stats().expect("listener reconnect gap clears cached bytes without waiting for another notification fixture").entries, 0);
    cancellation.cancel();
    task.await.expect("listener reconnect gap clears cached bytes without waiting for another notification fixture").expect("listener reconnect gap clears cached bytes without waiting for another notification fixture");
}
