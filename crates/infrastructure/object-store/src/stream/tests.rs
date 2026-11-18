use super::*;
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::BlobStore;

fn identity() -> BlobIdentity {
    BlobIdentity {
        tenant_id: TenantId::generate(),
        content_id: ContentId::generate(),
    }
}
struct BoundedSink(Vec<u8>);
impl ContentSink for BoundedSink {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> ApplicationFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            assert!(bytes.len() <= CONTENT_TRANSFER_CHUNK_BYTES);
            self.0.extend_from_slice(bytes);
            Ok(())
        })
    }
}
#[tokio::test]
async fn multipart_roundtrip_handles_empty_multiple_parts_replay_and_tenant_isolation() {
    let store = ObjectStoreBlobStore::in_memory();
    for size in [0, 19, PART_BYTES + 337] {
        let id = identity();
        let bytes = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let hash = Sha256::digest(&bytes);
        let first = store
            .put_stream(id, size as u64, hash, &mut bytes.as_slice())
            .await
            .expect("valid test fixture");
        assert_eq!(first.disposition, BlobPutDisposition::Created);
        let repeat = store
            .put_stream(id, size as u64, hash, &mut bytes.as_slice())
            .await
            .expect("valid test fixture");
        assert_eq!(repeat.disposition, BlobPutDisposition::AlreadyPresent);
        let mut sink = BoundedSink(Vec::new());
        store
            .copy_to(id, &first.locator, size as u64, hash, &mut sink)
            .await
            .expect("valid test fixture");
        assert_eq!(sink.0, bytes);
        assert_eq!(
            store
                .copy_to(identity(), &first.locator, size as u64, hash, &mut sink)
                .await
                .expect_err("invalid or interrupted stream must fail"),
            Error::Unauthorized
        );
    }
}
#[tokio::test]
async fn malformed_stream_never_publishes_and_cannot_overwrite_existing_identity() {
    let store = ObjectStoreBlobStore::in_memory();
    let id = identity();
    let hash = Sha256::digest(b"exact");
    for bytes in [b"wrong".as_slice(), b"extra!", b"sh"] {
        assert!(
            store
                .put_stream(id, 5, hash, &mut &bytes[..])
                .await
                .is_err()
        );
        assert!(
            store
                .store
                .head(
                    &store
                        .verified_path(id, &store.locator(id).expect("valid test fixture"))
                        .expect("valid test fixture")
                )
                .await
                .is_err()
        );
    }
    let first = store
        .put_stream(id, 5, hash, &mut b"exact".as_slice())
        .await
        .expect("valid test fixture");
    assert!(
        store
            .put_stream(id, 5, Sha256::digest(b"other"), &mut b"other".as_slice())
            .await
            .is_err()
    );
    let mut sink = BoundedSink(Vec::new());
    store
        .copy_to(id, &first.locator, 5, hash, &mut sink)
        .await
        .expect("valid test fixture");
    assert_eq!(sink.0, b"exact");
}
struct Interrupted;
impl ContentSource for Interrupted {
    fn read<'a>(&'a mut self, _: &'a mut [u8]) -> ApplicationFuture<'a, Result<usize, Error>> {
        Box::pin(async { Err(Error::Unavailable) })
    }
}
#[tokio::test]
async fn interrupted_source_leaves_no_public_object() {
    let store = ObjectStoreBlobStore::in_memory();
    let id = identity();
    assert_eq!(
        store
            .put_stream(id, 5, Sha256::digest(b"exact"), &mut Interrupted)
            .await
            .expect_err("invalid or interrupted stream must fail"),
        Error::Unavailable
    );
    assert!(
        store
            .store
            .head(
                &store
                    .verified_path(id, &store.locator(id).expect("valid test fixture"))
                    .expect("valid test fixture")
            )
            .await
            .is_err()
    );
}
#[tokio::test]
async fn content_above_one_atomic_copy_is_stored_as_verified_segments() {
    // 3 segments of 1 MiB, 1 MiB and 37 bytes: the same code path as a
    // 9 GiB object against the production 4 GiB segment size.
    let segment = 1 << 20;
    let store = ObjectStoreBlobStore::in_memory().with_segment_bytes(segment);
    let id = identity();
    let size = 2 * segment as usize + 37;
    let bytes = (0..size).map(|i| (i % 253) as u8).collect::<Vec<_>>();
    let hash = Sha256::digest(&bytes);
    let first = store
        .put_stream(id, size as u64, hash, &mut bytes.as_slice())
        .await
        .expect("valid test fixture");
    assert_eq!(first.disposition, BlobPutDisposition::Created);
    let base = store
        .verified_path(id, &first.locator)
        .expect("valid test fixture");
    assert!(store.store.head(&base).await.is_err());
    for (index, expected) in [(0, segment), (1, segment), (2, 37)] {
        let meta = store
            .store
            .head(&segment_path(&base, index).expect("valid test fixture"))
            .await
            .expect("valid test fixture");
        assert_eq!(meta.size, expected);
    }
    let repeat = store
        .put_stream(id, size as u64, hash, &mut bytes.as_slice())
        .await
        .expect("valid test fixture");
    assert_eq!(repeat.disposition, BlobPutDisposition::AlreadyPresent);
    let mut sink = BoundedSink(Vec::new());
    store
        .copy_to(id, &first.locator, size as u64, hash, &mut sink)
        .await
        .expect("valid test fixture");
    assert_eq!(sink.0, bytes);
    // A wrong declared length selects another layout and can never verify.
    assert!(
        store
            .copy_to(
                id,
                &first.locator,
                segment,
                hash,
                &mut BoundedSink(Vec::new())
            )
            .await
            .is_err()
    );
    // Buffered whole-object reads never assemble a segmented object.
    assert!(store.read(id, &first.locator).await.is_err());
    store
        .delete(id, &first.locator)
        .await
        .expect("valid test fixture");
    for index in 0..3 {
        assert!(
            store
                .store
                .head(&segment_path(&base, index).expect("valid test fixture"))
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn a_wrong_source_occupies_no_segment_of_the_identity() {
    let segment = 1 << 20;
    let store = ObjectStoreBlobStore::in_memory().with_segment_bytes(segment);
    let id = identity();
    let size = segment as usize + 5;
    let bytes = vec![7u8; size];
    let mut wrong = bytes.clone();
    wrong[size - 1] = 8;
    assert_eq!(
        store
            .put_stream(
                id,
                size as u64,
                Sha256::digest(&bytes),
                &mut wrong.as_slice()
            )
            .await
            .expect_err("invalid or interrupted stream must fail"),
        Error::HashMismatch
    );
    let base = store
        .verified_path(id, &store.locator(id).expect("valid test fixture"))
        .expect("valid test fixture");
    for index in 0..2 {
        assert!(
            store
                .store
                .head(&segment_path(&base, index).expect("valid test fixture"))
                .await
                .is_err()
        );
    }
}
