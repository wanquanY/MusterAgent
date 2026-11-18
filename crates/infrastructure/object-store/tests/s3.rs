use musteragent_content_ref::Sha256;
use musteragent_identity::{ContentId, TenantId};
use musteragent_object_store::{ObjectStoreBlobStore, S3ObjectStoreConfig};
use musteragent_runtime_content::{
    BlobIdentity, BlobPutDisposition, BlobStore, BlobStoreError, PutBlob,
};
use musteragent_testkit::{IsolatedMinio, MINIO_TEST_ACCESS_KEY, MINIO_TEST_SECRET_KEY};

#[tokio::test]
async fn minio_immutable_roundtrip_replay_conflict_and_tenant_boundary() {
    let minio = IsolatedMinio::start().await.expect("disposable MinIO");
    let store = || {
        ObjectStoreBlobStore::from_s3(S3ObjectStoreConfig {
            bucket: minio.bucket.into(),
            endpoint: minio.endpoint.clone(),
            region: "us-east-1".into(),
            prefix: "kernel".into(),
            access_key_id: MINIO_TEST_ACCESS_KEY.into(),
            secret_access_key: MINIO_TEST_SECRET_KEY.into(),
            allow_http: true,
            virtual_hosted_style: false,
            write_once: Default::default(),
        })
        .expect("S3 client")
    };
    let first = store();
    let identity = BlobIdentity {
        tenant_id: TenantId::generate(),
        content_id: ContentId::generate(),
    };
    let bytes = b"immutable S3 content".to_vec();
    let input = PutBlob {
        identity,
        sha256: Sha256::digest(&bytes),
        bytes: bytes.clone(),
    };
    let receipt = first.put(input.clone()).await.expect("create");
    assert_eq!(receipt.disposition, BlobPutDisposition::Created);
    drop(first);
    let resumed = store();
    assert_eq!(
        resumed
            .read(identity, &receipt.locator)
            .await
            .expect("read after restart"),
        bytes
    );
    assert_eq!(
        resumed.put(input).await.expect("replay").disposition,
        BlobPutDisposition::AlreadyPresent
    );
    assert!(matches!(
        resumed
            .put(PutBlob {
                identity,
                sha256: Sha256::digest(b"changed"),
                bytes: b"changed".to_vec()
            })
            .await,
        Err(BlobStoreError::IdentityConflict)
    ));
    let other = BlobIdentity {
        tenant_id: TenantId::generate(),
        ..identity
    };
    assert!(matches!(
        resumed.read(other, &receipt.locator).await,
        Err(BlobStoreError::LocatorMismatch)
    ));
    resumed
        .delete(identity, &receipt.locator)
        .await
        .expect("delete owned object");
    assert!(matches!(
        resumed.read(identity, &receipt.locator).await,
        Err(BlobStoreError::NotFound)
    ));
    // Exercise real multipart staging and conditional destination completion,
    // including collision: vendor-specific headers ignored by S3 would silently
    // overwrite the first object here.
    for size in [0, 19, (8 << 20) + 337] {
        let identity = BlobIdentity {
            tenant_id: TenantId::generate(),
            content_id: ContentId::generate(),
        };
        let bytes = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let hash = Sha256::digest(&bytes);
        let first = resumed
            .put_stream(identity, size as u64, hash, &mut bytes.as_slice())
            .await
            .expect("S3 staged multipart create");
        assert_eq!(first.disposition, BlobPutDisposition::Created);
        let replay = store()
            .put_stream(identity, size as u64, hash, &mut bytes.as_slice())
            .await
            .expect("S3 multipart replay from new client");
        assert_eq!(replay.disposition, BlobPutDisposition::AlreadyPresent);
        let other = vec![255; size.max(1)];
        assert!(
            store()
                .put_stream(
                    identity,
                    other.len() as u64,
                    Sha256::digest(&other),
                    &mut other.as_slice()
                )
                .await
                .is_err(),
            "different bytes cannot replace an existing immutable identity"
        );
        let mut sink = VerifiedSink(Vec::new());
        resumed
            .copy_to(identity, &first.locator, size as u64, hash, &mut sink)
            .await
            .expect("read original after rejected collision");
        assert_eq!(sink.0, bytes);
        resumed
            .delete(identity, &first.locator)
            .await
            .expect("delete own staged result");
    }
    minio.shutdown().await.expect("remove fixture");
}

struct VerifiedSink(Vec<u8>);
impl musteragent_runtime_content::ContentSink for VerifiedSink {
    fn write<'a>(
        &'a mut self,
        bytes: &'a [u8],
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<(), musteragent_runtime_content::ContentStoreError>,
    > {
        Box::pin(async move {
            assert!(bytes.len() <= musteragent_runtime_content::CONTENT_TRANSFER_CHUNK_BYTES);
            self.0.extend_from_slice(bytes);
            Ok(())
        })
    }
}
