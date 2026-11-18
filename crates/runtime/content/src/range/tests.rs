use super::*;
use musteragent_content_ref::MediaType;
use musteragent_identity::ContentId;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn reference(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/octet-stream").expect("MIME"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

#[test]
fn arbitrary_ranges_match_verified_content_across_block_boundaries() {
    let bytes: Vec<u8> = (0..CONTENT_TRANSFER_CHUNK_BYTES * 3 + 17)
        .map(|i| (i % 251) as u8)
        .collect();
    let expected = reference(&bytes);
    let reader =
        VerifiedContentReader::verify(expected.clone(), Box::new(bytes.clone()), &|| false)
            .expect("verified");
    assert_eq!(reader.reference(), &expected);
    assert_eq!(reader.fingerprints.len(), 4);
    for (offset, count) in [
        (0, 0),
        (0, 100),
        (65530, 130),
        (131070, 65553),
        (bytes.len() - 1, 1),
        (bytes.len(), 0),
    ] {
        let mut actual = vec![0; count];
        reader
            .read_exact_at(&mut actual, offset as u64)
            .expect("range");
        assert_eq!(actual, &bytes[offset..offset + count]);
    }
    let mut untouched = [42; 10];
    assert_eq!(reader.read_at(&mut untouched, u64::MAX).expect("EOF"), 0);
    assert_eq!(
        reader.read_exact_at(&mut untouched, u64::MAX),
        Err(Error::LengthMismatch)
    );
    assert_eq!(untouched, [42; 10]);
}

struct Mutable(Arc<Mutex<Vec<u8>>>);
impl ContentRangeSource for Mutable {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize, Error> {
        self.0
            .lock()
            .map_err(|_| Error::Unavailable)?
            .read_at(buffer, offset)
    }
}
#[test]
fn post_verification_mutation_cannot_return_changed_or_unchecked_bytes() {
    let bytes = vec![17; CONTENT_TRANSFER_CHUNK_BYTES * 2];
    let raw = Arc::new(Mutex::new(bytes.clone()));
    let reader =
        VerifiedContentReader::verify(reference(&bytes), Box::new(Mutable(raw.clone())), &|| false)
            .expect("verified");
    raw.lock().expect("source")[123] = 99;
    let mut buffer = [42; 3];
    assert_eq!(reader.read_at(&mut buffer, 122), Err(Error::HashMismatch));
    assert_eq!(buffer, [42; 3], "no unverified copy");
    assert_eq!(
        reader
            .read_at(&mut buffer, 65540)
            .expect("unmodified block"),
        3
    );
    assert_eq!(buffer, [17; 3]);
    raw.lock().expect("source").truncate(65536);
    assert_eq!(
        reader.read_at(&mut buffer, 65540),
        Err(Error::LengthMismatch)
    );
}

#[test]
fn opening_rejects_wrong_length_digest_and_invalid_source_counts() {
    for (input, error) in [
        (b"hell".to_vec(), Error::LengthMismatch),
        (b"hello!".to_vec(), Error::LengthMismatch),
        (b"HELLO".to_vec(), Error::HashMismatch),
    ] {
        assert_eq!(
            VerifiedContentReader::verify(reference(b"hello"), Box::new(input), &|| false)
                .expect_err("unverified content must be rejected"),
            error
        );
    }
    struct Invalid;
    impl ContentRangeSource for Invalid {
        fn read_at(&self, b: &mut [u8], _: u64) -> Result<usize, Error> {
            Ok(b.len() + 1)
        }
    }
    for bytes in [&b"hello"[..], &b""[..]] {
        assert_eq!(
            VerifiedContentReader::verify(reference(bytes), Box::new(Invalid), &|| false)
                .expect_err("invalid source counts must be rejected"),
            Error::Invariant
        );
    }
    let empty = VerifiedContentReader::verify(reference(b""), Box::new(Vec::new()), &|| false)
        .expect("empty");
    assert!(empty.fingerprints.is_empty());
    assert_eq!(empty.read_at(&mut [0], 0).expect("empty EOF"), 0);
}

#[test]
fn oversize_and_cancellation_reject_before_or_between_bounded_source_reads() {
    struct ShortReads(Arc<AtomicUsize>);
    impl ContentRangeSource for ShortReads {
        fn read_at(&self, b: &mut [u8], _: u64) -> Result<usize, Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let n = b.len().min(7);
            b[..n].fill(0);
            Ok(n)
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let mut expected = reference(&[0; 100]);
    expected.byte_length = MAX_CONTENT_BYTES + 1;
    assert_eq!(
        VerifiedContentReader::verify(expected, Box::new(ShortReads(calls.clone())), &|| false)
            .expect_err("oversize content must be rejected before reading"),
        Error::TooLarge
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        VerifiedContentReader::verify(
            reference(&[0; 100]),
            Box::new(ShortReads(calls.clone())),
            &|| calls.load(Ordering::SeqCst) >= 3
        )
        .expect_err("cancellation must interrupt bounded reads"),
        Error::Unavailable
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[test]
fn multi_megabyte_virtual_source_uses_bounded_buffers_and_compact_fingerprints() {
    struct Repeated {
        length: u64,
        maximum: Arc<AtomicUsize>,
    }
    impl ContentRangeSource for Repeated {
        fn read_at(&self, b: &mut [u8], offset: u64) -> Result<usize, Error> {
            self.maximum.fetch_max(b.len(), Ordering::SeqCst);
            let n = self.length.saturating_sub(offset).min(b.len() as u64) as usize;
            b[..n].fill(0xa7);
            Ok(n)
        }
    }
    let length = 8 * 1024 * 1024 + 17;
    let mut whole = Hasher::new();
    for _ in 0..128 {
        whole.update([0xa7; CONTENT_TRANSFER_CHUNK_BYTES]);
    }
    whole.update([0xa7; 17]);
    let mut expected = reference(b"");
    expected.byte_length = length;
    expected.sha256 = Sha256::from_bytes(whole.finalize()).expect("digest");
    let maximum = Arc::new(AtomicUsize::new(0));
    let reader = VerifiedContentReader::verify(
        expected,
        Box::new(Repeated {
            length,
            maximum: maximum.clone(),
        }),
        &|| false,
    )
    .expect("large source");
    assert_eq!(reader.fingerprints.len(), 129);
    let mut sample = [0; 31];
    reader
        .read_exact_at(&mut sample, 65530)
        .expect("cross block");
    assert_eq!(sample, [0xa7; 31]);
    assert_eq!(maximum.load(Ordering::SeqCst), CONTENT_TRANSFER_CHUNK_BYTES);
}

#[test]
fn unsupported_store_never_materializes_a_whole_file_as_range_fallback() {
    use crate::{ApplicationFuture, ContentStore, PutContent};
    use musteragent_identity::TenantId;
    use std::task::{Context, Poll, Waker};
    struct Unsupported;
    impl ContentStore for Unsupported {
        fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, Error>> {
            panic!("no write fallback")
        }
        fn read<'a>(
            &'a self,
            _: TenantId,
            _: &'a ContentRef,
        ) -> ApplicationFuture<'a, Result<Vec<u8>, Error>> {
            panic!("no whole-content fallback")
        }
    }
    let expected = reference(b"hello");
    let mut future = Unsupported.open_verified(TenantId::generate(), &expected);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => assert_eq!(
            result.expect_err("unsupported range port must reject"),
            Error::Unavailable
        ),
        Poll::Pending => panic!("unsupported port must reject immediately"),
    }
}
