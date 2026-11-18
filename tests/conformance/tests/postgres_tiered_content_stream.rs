#![cfg(test)]

//! Exercise the cloud storage composition, including the adapter Office uses.
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, DottedKey, TenantId};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{
    INLINE_CONTENT_MAX_BYTES, PostgresContentStore, PostgresTieredBlobStore,
};
use musteragent_runtime_content::{
    ApplicationFuture, BlobIdentity, BlobPutDisposition, BlobStore, BlobStoreError,
    CONTENT_TRANSFER_CHUNK_BYTES, ContentSink, ContentSource, ContentStore,
    ContentStoreError as Error, MAX_CONTENT_BYTES, PutBlob, PutContent,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use std::{sync::Arc, time::Duration};

struct Fixture {
    _postgres: IsolatedPostgres,
    pool: sqlx::PgPool,
    blobs: Arc<PostgresTieredBlobStore>,
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
        let blobs = Arc::new(PostgresTieredBlobStore::new(pool.clone(), remote.clone()));
        let key = DottedKey::new("test.streaming").unwrap();
        let keys = Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [29; 32])]).unwrap());
        Self {
            content: PostgresContentStore::new(pool.clone(), blobs.clone(), keys),
            _postgres: postgres,
            pool,
            blobs,
            remote,
            tenant: TenantId::generate(),
        }
    }
    fn identity(&self) -> BlobIdentity {
        BlobIdentity {
            tenant_id: self.tenant,
            content_id: ContentId::generate(),
        }
    }
    async fn inline_count(&self, identity: BlobIdentity) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM artifact.inline_content_blobs WHERE content_id=$1")
            .bind(identity.content_id.into_uuid())
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
}

struct Sink(Vec<u8>);
impl ContentSink for Sink {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> ApplicationFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            assert!(
                bytes.len() <= CONTENT_TRANSFER_CHUNK_BYTES,
                "bounded read chunks"
            );
            self.0.extend_from_slice(bytes);
            Ok(())
        })
    }
}
struct Fragmented<'a>(&'a [u8]);
impl ContentSource for Fragmented<'_> {
    fn read<'a>(&'a mut self, buffer: &'a mut [u8]) -> ApplicationFuture<'a, Result<usize, Error>> {
        Box::pin(async move {
            assert!(
                buffer.len() <= CONTENT_TRANSFER_CHUNK_BYTES,
                "bounded upload chunks"
            );
            let n = buffer.len().min(self.0.len()).min(7_001);
            buffer[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        })
    }
}

#[tokio::test]
async fn cloud_tiers_stream_roundtrip_and_interoperate_with_buffered_writes() {
    let f = Fixture::new().await;
    for size in [
        0,
        5_127,
        INLINE_CONTENT_MAX_BYTES,
        INLINE_CONTENT_MAX_BYTES + 1,
        (8 << 20) + 17,
    ] {
        let bytes: Vec<u8> = (0..size).map(|n| (n % 251) as u8).collect();
        let hash = Sha256::digest(&bytes);
        let identity = f.identity();
        let receipt = f
            .blobs
            .put_stream(identity, size as u64, hash, &mut Fragmented(&bytes))
            .await
            .unwrap();
        assert_eq!(receipt.disposition, BlobPutDisposition::Created);
        assert_eq!(
            f.inline_count(identity).await,
            i64::from(size <= INLINE_CONTENT_MAX_BYTES)
        );
        if size <= INLINE_CONTENT_MAX_BYTES {
            assert_eq!(
                f.remote.read(identity, &receipt.locator).await,
                Err(BlobStoreError::NotFound)
            );
        }
        assert_eq!(
            f.blobs.read(identity, &receipt.locator).await.unwrap(),
            bytes
        );
        let mut sink = Sink(Vec::new());
        f.blobs
            .copy_to(identity, &receipt.locator, size as u64, hash, &mut sink)
            .await
            .unwrap();
        assert_eq!(sink.0, bytes);
        // Retry a stream and cross API forms without duplicating or replacing it.
        assert_eq!(
            f.blobs
                .put_stream(identity, size as u64, hash, &mut bytes.as_slice())
                .await
                .unwrap()
                .disposition,
            BlobPutDisposition::AlreadyPresent
        );
        assert_eq!(
            f.blobs
                .put(PutBlob {
                    identity,
                    sha256: hash,
                    bytes: bytes.clone()
                })
                .await
                .unwrap()
                .disposition,
            BlobPutDisposition::AlreadyPresent
        );
        let buffered = f.identity();
        let prior = f
            .blobs
            .put(PutBlob {
                identity: buffered,
                sha256: hash,
                bytes: bytes.clone(),
            })
            .await
            .unwrap();
        let mut sink = Sink(Vec::new());
        f.blobs
            .copy_to(buffered, &prior.locator, size as u64, hash, &mut sink)
            .await
            .unwrap();
        assert_eq!(sink.0, bytes);
        assert_eq!(
            f.blobs
                .put_stream(buffered, size as u64, hash, &mut bytes.as_slice())
                .await
                .unwrap()
                .disposition,
            BlobPutDisposition::AlreadyPresent
        );
        // Fresh readers must still reject incorrect metadata and another identity.
        assert_eq!(
            f.blobs
                .copy_to(
                    identity,
                    &receipt.locator,
                    size as u64,
                    Sha256::digest(b"wrong"),
                    &mut Sink(Vec::new())
                )
                .await,
            Err(Error::HashMismatch)
        );
        assert_eq!(
            f.blobs
                .copy_to(
                    identity,
                    &receipt.locator,
                    size as u64 + 1,
                    hash,
                    &mut Sink(Vec::new())
                )
                .await,
            Err(Error::LengthMismatch)
        );
        assert_eq!(
            f.blobs
                .copy_to(
                    f.identity(),
                    &receipt.locator,
                    size as u64,
                    hash,
                    &mut Sink(Vec::new())
                )
                .await,
            Err(Error::Unauthorized)
        );
        if size > 0 {
            let changed = vec![255; size];
            assert!(matches!(
                f.blobs
                    .put_stream(
                        identity,
                        size as u64,
                        Sha256::digest(&changed),
                        &mut changed.as_slice()
                    )
                    .await,
                Err(Error::IdentityConflict | Error::HashMismatch)
            ));
            assert_eq!(
                f.blobs.read(identity, &receipt.locator).await.unwrap(),
                bytes
            );
        }
    }
}

struct BlockedSource;
impl ContentSource for BlockedSource {
    fn read<'a>(&'a mut self, _: &'a mut [u8]) -> ApplicationFuture<'a, Result<usize, Error>> {
        Box::pin(std::future::pending())
    }
}
#[tokio::test]
async fn invalid_or_cancelled_streams_do_not_publish_and_exact_retry_recovers() {
    let f = Fixture::new().await;
    for size in [5_127, INLINE_CONTENT_MAX_BYTES + 1] {
        let bytes = vec![5; size];
        let hash = Sha256::digest(&bytes);
        let identity = f.identity();
        let locator = f.blobs.derive_locator(identity).unwrap();
        for (mut source, expected) in [
            (&bytes[..size - 1], Error::LengthMismatch),
            (bytes.as_slice(), Error::HashMismatch),
        ] {
            let expected_hash = if expected == Error::HashMismatch {
                Sha256::digest(b"wrong")
            } else {
                hash
            };
            assert_eq!(
                f.blobs
                    .put_stream(identity, size as u64, expected_hash, &mut source)
                    .await
                    .unwrap_err(),
                expected
            );
            assert_eq!(f.inline_count(identity).await, 0);
            assert_eq!(
                f.blobs.read(identity, &locator).await,
                Err(BlobStoreError::NotFound)
            );
        }
        let extra = vec![5; size + 1];
        assert_eq!(
            f.blobs
                .put_stream(identity, size as u64, hash, &mut extra.as_slice())
                .await
                .unwrap_err(),
            Error::LengthMismatch
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(30),
                f.blobs
                    .put_stream(identity, size as u64, hash, &mut BlockedSource)
            )
            .await
            .is_err()
        );
        assert_eq!(
            f.blobs.read(identity, &locator).await,
            Err(BlobStoreError::NotFound)
        );
        assert_eq!(
            f.blobs
                .put_stream(identity, size as u64, hash, &mut bytes.as_slice())
                .await
                .unwrap()
                .disposition,
            BlobPutDisposition::Created
        );
    }
    // Bounds are enforced before ever consuming a blocked source.
    assert_eq!(
        f.blobs
            .put_stream(
                f.identity(),
                MAX_CONTENT_BYTES + 1,
                Sha256::digest(b""),
                &mut BlockedSource
            )
            .await
            .unwrap_err(),
        Error::TooLarge
    );
}

#[tokio::test]
async fn content_prewrite_becomes_readable_through_verified_random_access_in_both_tiers() {
    let f = Fixture::new().await;
    for size in [5_127, INLINE_CONTENT_MAX_BYTES + 1] {
        let bytes = vec![7; size];
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/octet-stream").unwrap(),
            byte_length: size as u64,
            sha256: Sha256::digest(&bytes),
        };
        assert_eq!(
            f.content
                .put_stream_at(f.tenant, &reference, &mut bytes.as_slice())
                .await
                .unwrap(),
            reference
        );
        let reader = f.content.open_verified(f.tenant, &reference).await.unwrap();
        let mut part = [0u8; 16];
        reader
            .read_exact_at(&mut part, reference.byte_length - 16)
            .unwrap();
        assert_eq!(part, [7; 16]);
        assert_eq!(f.content.read(f.tenant, &reference).await.unwrap(), bytes);
        let mut sink = Sink(Vec::new());
        f.content
            .copy_to(f.tenant, &reference, &mut sink)
            .await
            .unwrap();
        assert_eq!(sink.0, bytes);
        assert!(matches!(
            f.content
                .open_verified(TenantId::generate(), &reference)
                .await,
            Err(Error::Unauthorized)
        ));
        // Existing ordinary files use the same native reader, not a second path.
        let ordinary = f
            .content
            .put(PutContent {
                tenant_id: f.tenant,
                media_type: reference.media_type.clone(),
                bytes,
            })
            .await
            .unwrap();
        f.content.open_verified(f.tenant, &ordinary).await.unwrap();
        sqlx::query("UPDATE artifact.content_refs SET state=3 WHERE content_id=$1")
            .bind(reference.content_id.into_uuid())
            .execute(&f.pool)
            .await
            .unwrap();
        assert!(matches!(
            f.content.open_verified(f.tenant, &reference).await,
            Err(Error::NotFound)
        ));
    }
}
