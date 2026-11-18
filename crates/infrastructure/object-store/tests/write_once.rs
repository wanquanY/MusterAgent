#[path = "support/fake_s3.rs"]
mod fake_s3;
use fake_s3::Server;
use musteragent_content_ref::Sha256;
use musteragent_identity::{ContentId, TenantId};
use musteragent_object_store::WriteOnceStrategy;
use musteragent_runtime_content::{
    BlobIdentity, BlobPutDisposition, BlobStore, BlobStoreError, ContentStoreError, PutBlob,
};

const OSS: &str = "x-oss-forbid-overwrite";
const COS: &str = "x-cos-forbid-overwrite";

fn identity() -> BlobIdentity {
    BlobIdentity {
        tenant_id: TenantId::generate(),
        content_id: ContentId::generate(),
    }
}
fn input(identity: BlobIdentity, bytes: &[u8]) -> PutBlob {
    PutBlob {
        identity,
        bytes: bytes.to_vec(),
        sha256: Sha256::digest(bytes),
    }
}
fn strategy(header: &str) -> WriteOnceStrategy {
    WriteOnceStrategy::ForbidOverwriteHeader {
        header: header.into(),
    }
}

#[tokio::test]
async fn standard_rejection_is_visible_for_put_and_multipart() {
    let server = Server::start(OSS).await;
    let store = server.store(WriteOnceStrategy::default());
    let id = identity();
    assert_eq!(
        store.put(input(id, b"body")).await.expect_err("HTTP 400"),
        BlobStoreError::Unavailable
    );
    assert_eq!(
        store
            .put_stream(id, 4, Sha256::digest(b"body"), &mut b"body".as_slice())
            .await
            .expect_err("HTTP 400"),
        ContentStoreError::Unavailable
    );
    let bucket = server.bucket.lock().expect("bucket");
    assert_eq!(bucket.standard_rejections, 2);
    assert!(bucket.objects.keys().all(|key| key.contains("/.staging/")));
}

#[tokio::test]
async fn vendor_put_rereads_replays_and_preserves_original_on_conflict() {
    for header in [OSS, COS] {
        let server = Server::start(header).await;
        let store = server.store(strategy(header));
        let id = identity();
        let receipt = store.put(input(id, b"first")).await.expect("guarded PUT");
        assert_eq!(receipt.disposition, BlobPutDisposition::Created);
        assert_eq!(
            store
                .put(input(id, b"other"))
                .await
                .expect_err("immutable collision"),
            BlobStoreError::IdentityConflict
        );
        assert_eq!(
            store
                .put(input(id, b"first"))
                .await
                .expect("verified replay")
                .disposition,
            BlobPutDisposition::AlreadyPresent
        );
        assert_eq!(
            store.read(id, &receipt.locator).await.expect("original"),
            b"first"
        );
        server.bucket.lock().expect("bucket").collision_code = Some("OperationAborted");
        assert_eq!(
            store
                .put(input(id, b"first"))
                .await
                .expect_err("unrelated 409"),
            BlobStoreError::Unavailable
        );
        let bucket = server.bucket.lock().expect("bucket");
        assert_eq!(bucket.guarded_puts, 6);
        assert_eq!(bucket.guarded_completions, 0);
        assert_eq!(bucket.objects.len(), 1);
    }
}

#[tokio::test]
async fn multipart_copy_atomically_completes_with_the_signed_vendor_header() {
    for header in [OSS, COS] {
        let server = Server::start(header).await;
        let store = server.store(strategy(header));
        let id = identity();
        let bytes = vec![17; (8 << 20) + 13];
        let hash = Sha256::digest(&bytes);
        let length = bytes.len() as u64;
        let receipt = store
            .put_stream(id, length, hash, &mut bytes.as_slice())
            .await
            .expect("multipart create");
        assert_eq!(receipt.disposition, BlobPutDisposition::Created);
        let mut changed = bytes.clone();
        changed[8 << 20] = 3;
        assert_eq!(
            store
                .put_stream(
                    id,
                    length,
                    Sha256::digest(&changed),
                    &mut changed.as_slice()
                )
                .await
                .expect_err("same length conflict"),
            ContentStoreError::HashMismatch
        );
        assert_eq!(
            store
                .put_stream(id, length, hash, &mut bytes.as_slice())
                .await
                .expect("verified replay")
                .disposition,
            BlobPutDisposition::AlreadyPresent
        );
        assert_eq!(
            store
                .read(id, &receipt.locator)
                .await
                .expect("original object"),
            bytes
        );
        server.bucket.lock().expect("bucket").collision_code = Some("UploadConflict");
        assert_eq!(
            store
                .put_stream(id, length, hash, &mut bytes.as_slice())
                .await
                .expect_err("unrelated 409"),
            ContentStoreError::Unavailable
        );
        let bucket = server.bucket.lock().expect("bucket");
        assert_eq!(bucket.guarded_puts, 0);
        assert_eq!(bucket.guarded_completions, 5);
        assert_eq!(bucket.copy_parts, 5);
    }
}

#[tokio::test]
async fn silently_ignored_precondition_never_reaches_an_existing_content_key() {
    for selected in [WriteOnceStrategy::Standard {}, strategy(COS)] {
        let server = Server::start(COS).await;
        let store = server.store(selected);
        let id = identity();
        let locator = store.derive_locator(id).expect("locator");
        {
            let mut bucket = server.bucket.lock().expect("bucket");
            bucket.ignore_standard = true;
            bucket.ignore_put = true;
            bucket.ignore_complete = true;
            bucket.objects.insert(
                format!("test-bucket/{}", locator.as_str()),
                "original".into(),
            );
        }
        assert_eq!(
            store
                .put(input(id, b"other"))
                .await
                .expect_err("ignored PUT precondition"),
            BlobStoreError::Unavailable
        );
        assert_eq!(
            store
                .put_stream(id, 5, Sha256::digest(b"other"), &mut b"other".as_slice())
                .await
                .expect_err("ignored completion precondition"),
            ContentStoreError::Unavailable
        );
        assert_eq!(
            store.read(id, &locator).await.expect("untouched"),
            b"original"
        );
    }
}
