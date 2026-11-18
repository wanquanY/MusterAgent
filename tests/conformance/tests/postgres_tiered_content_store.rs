use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{CommandId, ContentId, DottedKey, TenantId, UtcTimestamp};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{
    ContentGcPolicy, INLINE_CONTENT_MAX_BYTES, PostgresContentStore, PostgresTieredBlobStore,
};
use musteragent_runtime_content::{
    AdmissionContentPlan, AdmissionContentSlot, AdmissionContentSource, ApplicationFuture,
    BlobIdentity, BlobStore, BlobStoreError, ContentStore, ContentStoreError, PutBlob,
    PutBlobReceipt, PutContent, StorageLocator,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;

fn reference_keys() -> Arc<OpaqueRefKeyring> {
    let key_id = DottedKey::new("runtime.content.active").expect("key id");
    Arc::new(OpaqueRefKeyring::new(key_id.clone(), [(key_id, [29_u8; 32])]).expect("keyring"))
}

#[derive(Clone)]
struct FailFirstDeleteBlobStore {
    inner: Arc<ObjectStoreBlobStore>,
    fail_next_delete: Arc<AtomicBool>,
}

#[derive(Clone)]
struct RejectPutBlobStore {
    inner: Arc<ObjectStoreBlobStore>,
}

impl BlobStore for RejectPutBlobStore {
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
        Box::pin(async { Err(musteragent_runtime_content::ContentStoreError::Unavailable) })
    }
    fn copy_to<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        locator: &'a musteragent_runtime_content::StorageLocator,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        sink: &'a mut dyn musteragent_runtime_content::ContentSink,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<(), musteragent_runtime_content::ContentStoreError>,
    > {
        self.inner.copy_to(identity, locator, length, hash, sink)
    }

    fn put(
        &self,
        _request: PutBlob,
    ) -> ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>> {
        Box::pin(async { Err(BlobStoreError::Unavailable) })
    }

    fn read<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, BlobStoreError>> {
        self.inner.read(identity, locator)
    }

    fn delete<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<(), BlobStoreError>> {
        self.inner.delete(identity, locator)
    }

    fn derive_locator(&self, identity: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        self.inner.derive_locator(identity)
    }
}

impl BlobStore for FailFirstDeleteBlobStore {
    fn put_stream<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        source: &'a mut dyn musteragent_runtime_content::ContentSource,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<
            musteragent_runtime_content::PutBlobReceipt,
            musteragent_runtime_content::ContentStoreError,
        >,
    > {
        self.inner.put_stream(identity, length, hash, source)
    }
    fn copy_to<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        locator: &'a musteragent_runtime_content::StorageLocator,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        sink: &'a mut dyn musteragent_runtime_content::ContentSink,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<(), musteragent_runtime_content::ContentStoreError>,
    > {
        self.inner.copy_to(identity, locator, length, hash, sink)
    }

    fn put(
        &self,
        request: PutBlob,
    ) -> ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>> {
        self.inner.put(request)
    }

    fn read<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, BlobStoreError>> {
        self.inner.read(identity, locator)
    }

    fn delete<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<(), BlobStoreError>> {
        Box::pin(async move {
            if self.fail_next_delete.swap(false, Ordering::SeqCst) {
                Err(BlobStoreError::Unavailable)
            } else {
                self.inner.delete(identity, locator).await
            }
        })
    }

    fn derive_locator(&self, identity: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        self.inner.derive_locator(identity)
    }
}

#[tokio::test]
async fn ordinary_content_is_inline_and_large_content_is_delegated() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let remote = Arc::new(ObjectStoreBlobStore::in_memory());
    let blobs: Arc<dyn BlobStore> =
        Arc::new(PostgresTieredBlobStore::new(pool.clone(), remote.clone()));
    let store = PostgresContentStore::new(pool.clone(), blobs, reference_keys());
    let tenant_id = TenantId::generate();

    let inline_bytes = b"ordinary conversation payload".to_vec();
    let inline = store
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/plain").expect("media type"),
            bytes: inline_bytes.clone(),
        })
        .await
        .expect("put inline content");
    assert_eq!(
        store.read(tenant_id, &inline).await.expect("read inline"),
        inline_bytes
    );
    let inline_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact.inline_content_blobs
         WHERE tenant_id = $1 AND content_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(inline.content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count inline row");
    assert_eq!(inline_count, 1);
    let inline_identity = BlobIdentity {
        tenant_id,
        content_id: inline.content_id,
    };
    let inline_locator = remote
        .derive_locator(inline_identity)
        .expect("derive inline locator");
    assert_eq!(
        remote.read(inline_identity, &inline_locator).await,
        Err(BlobStoreError::NotFound)
    );

    let large_bytes = vec![b'x'; INLINE_CONTENT_MAX_BYTES + 1];
    let large = store
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/octet-stream").expect("media type"),
            bytes: large_bytes.clone(),
        })
        .await
        .expect("put large content");
    assert_eq!(
        store.read(tenant_id, &large).await.expect("read large"),
        large_bytes
    );
    let large_inline_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact.inline_content_blobs
         WHERE tenant_id = $1 AND content_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(large.content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count large inline row");
    assert_eq!(large_inline_count, 0);
    let large_identity = BlobIdentity {
        tenant_id,
        content_id: large.content_id,
    };
    let large_locator = remote
        .derive_locator(large_identity)
        .expect("derive large locator");
    assert_eq!(
        remote
            .read(large_identity, &large_locator)
            .await
            .expect("read delegated bytes"),
        large_bytes
    );
    let ordinary_gc_candidates: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact.content_refs
          WHERE content_id IN ($1, $2) AND gc_eligible_at_ms IS NOT NULL",
    )
    .bind(inline.content_id.into_uuid())
    .bind(large.content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("ordinary content must not enter prewrite GC");
    assert_eq!(ordinary_gc_candidates, 0);

    store
        .delete(tenant_id, &inline)
        .await
        .expect("delete inline content");
    store
        .delete(tenant_id, &large)
        .await
        .expect("delete large content");
    assert_eq!(
        remote.read(large_identity, &large_locator).await,
        Err(BlobStoreError::NotFound)
    );

    pool.close().await;
}

#[tokio::test]
async fn admission_content_stages_inline_before_content_ref_publication() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let remote = Arc::new(ObjectStoreBlobStore::in_memory());
    let blobs: Arc<dyn BlobStore> = Arc::new(PostgresTieredBlobStore::new(pool.clone(), remote));
    let store = PostgresContentStore::new(pool.clone(), blobs.clone(), reference_keys());
    let bytes = b"inline admission payload".to_vec();
    let plan = AdmissionContentPlan {
        tenant_id: TenantId::generate(),
        command_id: CommandId::generate(),
        command_fingerprint: Sha256::digest(b"inline-admission-command"),
        slot: AdmissionContentSlot::SubmitMessage,
        proposed_content_id: ContentId::generate(),
        source: AdmissionContentSource::Inline {
            media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
            bytes,
        },
    };

    let staged = store
        .prepare_admission(plan.clone())
        .await
        .expect("stage inline admission content before publication");
    assert_eq!(
        store.read(plan.tenant_id, &staged.content).await,
        Err(ContentStoreError::NotFound),
        "staged bytes must remain invisible until the product command publishes its ContentRef"
    );
    let content_ref_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifact.content_refs WHERE content_id = $1")
            .bind(staged.content.content_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("count unpublished ContentRef");
    assert_eq!(content_ref_count, 0);
    let inline_blob_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact.inline_content_blobs
         WHERE tenant_id = $1 AND content_id = $2",
    )
    .bind(plan.tenant_id.into_uuid())
    .bind(staged.content.content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count staged inline blob");
    assert_eq!(inline_blob_count, 1);
    let protected_locator_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact.admission_content_stages
         WHERE tenant_id = $1 AND command_id = $2 AND slot = $3
           AND storage_ref_protected IS NOT NULL",
    )
    .bind(plan.tenant_id.into_uuid())
    .bind(plan.command_id.into_uuid())
    .bind(i16::from(plan.slot.code()))
    .fetch_one(&pool)
    .await
    .expect("count protected admission locator");
    assert_eq!(protected_locator_count, 1);
    let reconstructed = PostgresContentStore::new(pool.clone(), blobs, reference_keys());
    assert_eq!(
        reconstructed
            .prepare_admission(plan)
            .await
            .expect("reconcile staged content after restart boundary"),
        staged
    );

    pool.close().await;
}

#[tokio::test]
async fn content_gc_reclaims_only_fk_unreachable_bytes_and_persists_a_receipt() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let remote = Arc::new(ObjectStoreBlobStore::in_memory());
    let blobs: Arc<dyn BlobStore> =
        Arc::new(PostgresTieredBlobStore::new(pool.clone(), remote.clone()));
    let store = PostgresContentStore::new(pool.clone(), blobs, reference_keys());
    let tenant_id = TenantId::generate();
    let media_type = MediaType::new("text/markdown;charset=utf-8").expect("media type");
    let orphan = store
        .put_reclaimable_at(
            ContentId::generate(),
            PutContent {
                tenant_id,
                media_type: media_type.clone(),
                bytes: b"failed Note prewrite".to_vec(),
            },
        )
        .await
        .expect("put unreachable candidate");
    let reachable = store
        .put_reclaimable_at(
            ContentId::generate(),
            PutContent {
                tenant_id,
                media_type,
                bytes: b"canonical Note block".to_vec(),
            },
        )
        .await
        .expect("put reachable candidate");
    sqlx::raw_sql(
        "CREATE TABLE artifact.gc_reachability_fixture (
             content_id UUID PRIMARY KEY
                 REFERENCES artifact.content_refs(content_id) ON DELETE RESTRICT
         )",
    )
    .execute(&pool)
    .await
    .expect("create canonical reachability fixture");
    sqlx::query("INSERT INTO artifact.gc_reachability_fixture(content_id) VALUES ($1)")
        .bind(reachable.content_id.into_uuid())
        .execute(&pool)
        .await
        .expect("reference canonical content");
    sqlx::query(
        "UPDATE artifact.content_refs SET gc_eligible_at_ms = 0 WHERE content_id IN ($1, $2)",
    )
    .bind(orphan.content_id.into_uuid())
    .bind(reachable.content_id.into_uuid())
    .execute(&pool)
    .await
    .expect("make candidates eligible");
    let now_ms: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&pool)
            .await
            .expect("database time");

    let report = store
        .sweep_unreferenced_content(
            UtcTimestamp::from_unix_millis(now_ms),
            ContentGcPolicy {
                batch_size: 8,
                referenced_recheck_interval: Duration::from_secs(60),
                failed_delete_retry_interval: Duration::from_secs(1),
            },
        )
        .await
        .expect("sweep unreachable content");

    assert_eq!(report.scanned, 2);
    assert_eq!(report.still_referenced, 1);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.delete_deferred, 0);
    assert_eq!(
        store.read(tenant_id, &orphan).await,
        Err(ContentStoreError::NotFound)
    );
    assert_eq!(
        store
            .read(tenant_id, &reachable)
            .await
            .expect("read canonical content"),
        b"canonical Note block"
    );
    let receipt: (i16, bool, i32) = sqlx::query_as(
        "SELECT status, storage_ref_protected IS NULL, attempt_count
           FROM artifact.content_gc_jobs WHERE content_id = $1",
    )
    .bind(orphan.content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read completed GC receipt");
    assert_eq!(receipt, (2, true, 0));
    let remote_identity = BlobIdentity {
        tenant_id,
        content_id: orphan.content_id,
    };
    let remote_locator = remote
        .derive_locator(remote_identity)
        .expect("derive former locator");
    assert_eq!(
        remote.read(remote_identity, &remote_locator).await,
        Err(BlobStoreError::NotFound)
    );

    pool.close().await;
}

#[tokio::test]
async fn content_gc_reclaims_an_abandoned_staged_prewrite_after_blob_failure() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let remote = Arc::new(ObjectStoreBlobStore::in_memory());
    let rejecting_remote: Arc<dyn BlobStore> = Arc::new(RejectPutBlobStore {
        inner: remote.clone(),
    });
    let blobs: Arc<dyn BlobStore> =
        Arc::new(PostgresTieredBlobStore::new(pool.clone(), rejecting_remote));
    let store = PostgresContentStore::new(pool.clone(), blobs, reference_keys());
    let tenant_id = TenantId::generate();
    let content_id = ContentId::generate();
    let rejected = store
        .put_reclaimable_at(
            content_id,
            PutContent {
                tenant_id,
                media_type: MediaType::new("application/octet-stream").expect("media type"),
                bytes: vec![b's'; INLINE_CONTENT_MAX_BYTES + 1],
            },
        )
        .await;
    assert_eq!(rejected, Err(ContentStoreError::Unavailable));
    let staged: (i16, bool) = sqlx::query_as(
        "SELECT state, gc_eligible_at_ms IS NOT NULL
           FROM artifact.content_refs WHERE content_id = $1",
    )
    .bind(content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read abandoned staged prewrite");
    assert_eq!(staged, (1, true));
    sqlx::query("UPDATE artifact.content_refs SET gc_eligible_at_ms = 0 WHERE content_id = $1")
        .bind(content_id.into_uuid())
        .execute(&pool)
        .await
        .expect("make abandoned stage eligible");
    let now_ms: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&pool)
            .await
            .expect("database time");

    let report = store
        .sweep_unreferenced_content(
            UtcTimestamp::from_unix_millis(now_ms),
            ContentGcPolicy {
                batch_size: 8,
                referenced_recheck_interval: Duration::from_secs(60),
                failed_delete_retry_interval: Duration::from_secs(1),
            },
        )
        .await
        .expect("reclaim abandoned staged prewrite");

    assert_eq!(report.scanned, 1);
    assert_eq!(report.reclaimed, 1);
    let surviving_ref: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifact.content_refs WHERE content_id = $1")
            .bind(content_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("count surviving staged refs");
    assert_eq!(surviving_ref, 0);
    let receipt: (i16, bool) = sqlx::query_as(
        "SELECT status, storage_ref_protected IS NULL
           FROM artifact.content_gc_jobs WHERE content_id = $1",
    )
    .bind(content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read staged GC receipt");
    assert_eq!(receipt, (2, true));

    pool.close().await;
}

#[tokio::test]
async fn content_gc_retries_a_durable_blob_deletion_job_after_failure() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let remote = Arc::new(ObjectStoreBlobStore::in_memory());
    let fail_once = Arc::new(FailFirstDeleteBlobStore {
        inner: remote.clone(),
        fail_next_delete: Arc::new(AtomicBool::new(true)),
    });
    let blobs: Arc<dyn BlobStore> = Arc::new(PostgresTieredBlobStore::new(pool.clone(), fail_once));
    let store = PostgresContentStore::new(pool.clone(), blobs, reference_keys());
    let tenant_id = TenantId::generate();
    let content = store
        .put_reclaimable_at(
            ContentId::generate(),
            PutContent {
                tenant_id,
                media_type: MediaType::new("application/octet-stream").expect("media type"),
                bytes: vec![b'g'; INLINE_CONTENT_MAX_BYTES + 1],
            },
        )
        .await
        .expect("put delegated GC candidate");
    sqlx::query("UPDATE artifact.content_refs SET gc_eligible_at_ms = 0 WHERE content_id = $1")
        .bind(content.content_id.into_uuid())
        .execute(&pool)
        .await
        .expect("make candidate eligible");
    let now_ms: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&pool)
            .await
            .expect("database time");
    let policy = ContentGcPolicy {
        batch_size: 8,
        referenced_recheck_interval: Duration::from_secs(60),
        failed_delete_retry_interval: Duration::from_secs(1),
    };

    let first = store
        .sweep_unreferenced_content(UtcTimestamp::from_unix_millis(now_ms), policy)
        .await
        .expect("persist deletion job despite transient Blob failure");
    assert_eq!(first.scanned, 1);
    assert_eq!(first.reclaimed, 0);
    assert_eq!(first.delete_deferred, 1);
    let pending: (i16, bool) = sqlx::query_as(
        "SELECT status, storage_ref_protected IS NOT NULL
           FROM artifact.content_gc_jobs WHERE content_id = $1",
    )
    .bind(content.content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read pending deletion job");
    assert_eq!(pending, (1, true));

    let second = store
        .sweep_unreferenced_content(UtcTimestamp::from_unix_millis(now_ms + 2_000), policy)
        .await
        .expect("retry durable deletion job");
    assert_eq!(second.pending_retried, 1);
    assert_eq!(second.reclaimed, 1);
    let completed: (i16, bool, i32) = sqlx::query_as(
        "SELECT status, storage_ref_protected IS NULL, attempt_count
           FROM artifact.content_gc_jobs WHERE content_id = $1",
    )
    .bind(content.content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read completed deletion job");
    assert_eq!(completed, (2, true, 2));
    let identity = BlobIdentity {
        tenant_id,
        content_id: content.content_id,
    };
    let locator = remote.derive_locator(identity).expect("derive locator");
    assert_eq!(
        remote.read(identity, &locator).await,
        Err(BlobStoreError::NotFound)
    );

    pool.close().await;
}
