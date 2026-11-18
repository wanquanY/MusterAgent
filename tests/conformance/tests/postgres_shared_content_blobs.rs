#![cfg(test)]

//! Same-tenant physical deduplication keeps every ContentRef's own lifecycle.
use std::sync::Arc;
use std::time::Duration;

use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{ContentId, DottedKey, TenantId, UtcTimestamp};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{
    ContentGcPolicy, INLINE_CONTENT_MAX_BYTES, PostgresContentStore, PostgresTieredBlobStore,
};
use musteragent_runtime_content::{
    ApplicationFuture, BlobIdentity, BlobStore, BlobStoreError, ContentSink, ContentStore,
    ContentStoreError, PutContent,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;

struct Fixture {
    _postgres: IsolatedPostgres,
    pool: sqlx::PgPool,
    remote: Arc<ObjectStoreBlobStore>,
    content: PostgresContentStore,
    tenant: TenantId,
}

impl Fixture {
    async fn new() -> Self {
        let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
        let pool = postgres.pool().await.unwrap();
        musteragent_conformance::migrate(&pool).await.unwrap();
        let remote = Arc::new(ObjectStoreBlobStore::in_memory());
        let blobs: Arc<dyn BlobStore> =
            Arc::new(PostgresTieredBlobStore::new(pool.clone(), remote.clone()));
        let key = DottedKey::new("test.shared").unwrap();
        let keys = Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [31; 32])]).unwrap());
        Self {
            content: PostgresContentStore::new(pool.clone(), blobs, keys),
            _postgres: postgres,
            pool,
            remote,
            tenant: TenantId::generate(),
        }
    }

    async fn put(&self, bytes: &[u8]) -> ContentRef {
        self.content
            .put_reclaimable_at(
                ContentId::generate(),
                PutContent {
                    tenant_id: self.tenant,
                    media_type: MediaType::new("application/octet-stream").unwrap(),
                    bytes: bytes.to_vec(),
                },
            )
            .await
            .unwrap()
    }

    async fn shared_with(&self, reference: &ContentRef) -> Option<uuid::Uuid> {
        sqlx::query_scalar("SELECT blob_content_id FROM artifact.content_refs WHERE content_id=$1")
            .bind(reference.content_id.into_uuid())
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// Bytes physically present for this identity in either tier.
    async fn object_exists(&self, content_id: ContentId) -> bool {
        let inline: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM artifact.inline_content_blobs
              WHERE tenant_id=$1 AND content_id=$2)",
        )
        .bind(self.tenant.into_uuid())
        .bind(content_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .unwrap();
        let identity = BlobIdentity {
            tenant_id: self.tenant,
            content_id,
        };
        let locator = self.remote.derive_locator(identity).unwrap();
        inline
            || match self.remote.read(identity, &locator).await {
                Ok(_) => true,
                Err(BlobStoreError::NotFound) => false,
                Err(error) => panic!("object probe failed: {error:?}"),
            }
    }

    async fn now_ms(&self) -> i64 {
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
}

struct Sink(Vec<u8>);
impl ContentSink for Sink {
    fn write<'a>(
        &'a mut self,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            self.0.extend_from_slice(bytes);
            Ok(())
        })
    }
}

fn policy() -> ContentGcPolicy {
    ContentGcPolicy {
        batch_size: 16,
        referenced_recheck_interval: Duration::from_secs(60),
        failed_delete_retry_interval: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn identical_bytes_share_one_object_and_only_the_last_holder_deletes_it() {
    let f = Fixture::new().await;
    for size in [4_096, INLINE_CONTENT_MAX_BYTES + 1] {
        let bytes = vec![u8::try_from(size % 251).unwrap(); size];
        let first = f.put(&bytes).await;
        let second = f.put(&bytes).await;
        assert_ne!(first.content_id, second.content_id);
        assert_eq!(f.shared_with(&first).await, None);
        assert_eq!(
            f.shared_with(&second).await,
            Some(first.content_id.into_uuid())
        );
        assert!(!f.object_exists(second.content_id).await, "no second copy");

        assert_eq!(f.content.read(f.tenant, &second).await.unwrap(), bytes);
        assert_eq!(
            f.content
                .read_batch(f.tenant, &[first.clone(), second.clone()])
                .await
                .unwrap(),
            vec![bytes.clone(), bytes.clone()]
        );
        let mut sink = Sink(Vec::new());
        f.content
            .copy_to(f.tenant, &second, &mut sink)
            .await
            .unwrap();
        assert_eq!(sink.0, bytes);

        f.content.delete(f.tenant, &first).await.unwrap();
        assert_eq!(
            f.content.read(f.tenant, &first).await,
            Err(ContentStoreError::NotFound)
        );
        assert!(f.object_exists(first.content_id).await, "a holder remains");
        assert_eq!(f.content.read(f.tenant, &second).await.unwrap(), bytes);

        f.content.delete(f.tenant, &second).await.unwrap();
        assert!(
            !f.object_exists(first.content_id).await,
            "last holder erased"
        );
        let locators: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact.content_refs
              WHERE content_id IN ($1,$2) AND storage_ref_protected IS NOT NULL",
        )
        .bind(first.content_id.into_uuid())
        .bind(second.content_id.into_uuid())
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!(locators, 0);
    }

    let foreign = TenantId::generate();
    let bytes = b"tenant boundary".to_vec();
    let own = f.put(&bytes).await;
    let other = f
        .content
        .put_reclaimable_at(
            ContentId::generate(),
            PutContent {
                tenant_id: foreign,
                media_type: MediaType::new("application/octet-stream").unwrap(),
                bytes,
            },
        )
        .await
        .unwrap();
    assert_eq!(f.shared_with(&own).await, None);
    assert_eq!(f.shared_with(&other).await, None, "never across tenants");
}

#[tokio::test]
async fn streams_borrow_only_after_their_declared_digest_is_verified() {
    let f = Fixture::new().await;
    let bytes = vec![3; INLINE_CONTENT_MAX_BYTES + 17];
    let donor = f.put(&bytes).await;

    let forged = ContentRef {
        content_id: ContentId::generate(),
        ..donor.clone()
    };
    assert_eq!(
        f.content
            .put_stream_at(f.tenant, &forged, &mut b"not the donor bytes".as_slice())
            .await,
        Err(ContentStoreError::LengthMismatch)
    );
    let mut wrong = bytes.clone();
    wrong[0] = 4;
    assert_eq!(
        f.content
            .put_stream_at(f.tenant, &forged, &mut wrong.as_slice())
            .await,
        Err(ContentStoreError::HashMismatch)
    );
    assert_eq!(
        f.content.read(f.tenant, &forged).await,
        Err(ContentStoreError::NotFound)
    );

    assert_eq!(
        f.content
            .put_stream_at(f.tenant, &forged, &mut bytes.as_slice())
            .await
            .unwrap(),
        forged
    );
    assert_eq!(
        f.shared_with(&forged).await,
        Some(donor.content_id.into_uuid())
    );
    assert!(!f.object_exists(forged.content_id).await);
    let reader = f.content.open_verified(f.tenant, &forged).await.unwrap();
    let mut tail = [0; 8];
    reader
        .read_exact_at(&mut tail, forged.byte_length - 8)
        .unwrap();
    assert_eq!(tail, [3; 8]);
}

#[tokio::test]
async fn gc_releases_shared_rows_and_deletes_the_object_with_its_last_holder() {
    let f = Fixture::new().await;
    let bytes = vec![9; INLINE_CONTENT_MAX_BYTES + 1];
    let first = f.put(&bytes).await;
    let second = f.put(&bytes).await;
    let kept = f.put(&bytes).await;
    sqlx::query("UPDATE artifact.content_refs SET gc_eligible_at_ms=0 WHERE content_id IN ($1,$2)")
        .bind(first.content_id.into_uuid())
        .bind(second.content_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();

    let now = f.now_ms().await;
    let report = f
        .content
        .sweep_unreferenced_content(UtcTimestamp::from_unix_millis(now), policy())
        .await
        .unwrap();
    assert_eq!((report.scanned, report.reclaimed), (2, 2));
    assert!(
        f.object_exists(first.content_id).await,
        "kept still holds it"
    );
    assert_eq!(f.content.read(f.tenant, &kept).await.unwrap(), bytes);
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact.content_gc_jobs")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(jobs, 0);

    sqlx::query("UPDATE artifact.content_refs SET gc_eligible_at_ms=0 WHERE content_id=$1")
        .bind(kept.content_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    let report = f
        .content
        .sweep_unreferenced_content(UtcTimestamp::from_unix_millis(now), policy())
        .await
        .unwrap();
    assert_eq!(report.reclaimed, 1);
    assert!(!f.object_exists(first.content_id).await);
    let job: (uuid::Uuid, i16) =
        sqlx::query_as("SELECT content_id, status FROM artifact.content_gc_jobs")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(
        job,
        (first.content_id.into_uuid(), 2),
        "keyed by the object"
    );
}

#[tokio::test]
async fn historical_copies_converge_to_the_oldest_owner_after_a_read_grace() {
    let f = Fixture::new().await;
    let bytes = vec![5; INLINE_CONTENT_MAX_BYTES + 3];
    let owner = f.put(&bytes).await;
    // A busy donor is skipped, which is also how pre-deduplication copies look.
    let mut busy = f.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM artifact.content_refs WHERE content_id=$1 FOR UPDATE")
        .bind(owner.content_id.into_uuid())
        .execute(&mut *busy)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let copy = f.put(&bytes).await;
    busy.rollback().await.unwrap();
    assert_eq!(f.shared_with(&copy).await, None);
    assert!(f.object_exists(copy.content_id).await);

    let now = f.now_ms().await;
    let report = f
        .content
        .sweep_unreferenced_content(UtcTimestamp::from_unix_millis(now), policy())
        .await
        .unwrap();
    assert_eq!(report.compacted, 1);
    assert_eq!(
        f.shared_with(&copy).await,
        Some(owner.content_id.into_uuid())
    );
    assert_eq!(f.content.read(f.tenant, &copy).await.unwrap(), bytes);
    assert!(
        f.object_exists(copy.content_id).await,
        "in-flight readers keep the superseded copy"
    );

    let later = now + 2 * 60 * 60 * 1_000;
    let report = f
        .content
        .sweep_unreferenced_content(UtcTimestamp::from_unix_millis(later), policy())
        .await
        .unwrap();
    assert_eq!(report.compacted, 0, "the cursor does not revisit rows");
    assert!(!f.object_exists(copy.content_id).await);
    assert_eq!(f.content.read(f.tenant, &copy).await.unwrap(), bytes);

    f.content.delete(f.tenant, &owner).await.unwrap();
    assert_eq!(f.content.read(f.tenant, &copy).await.unwrap(), bytes);
    assert!(
        sqlx::query("UPDATE artifact.content_refs SET blob_content_id=NULL WHERE content_id=$1")
            .bind(copy.content_id.into_uuid())
            .execute(&f.pool)
            .await
            .is_err(),
        "a borrowed object cannot be silently re-owned"
    );
}
