use super::*;
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("in-memory test ports never suspend"),
    }
}
#[derive(Default)]
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
#[test]
fn known_sha_vector_and_exact_eof_are_required() {
    let hash = Sha256::from_hex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        .expect("SHA vector");
    let mut sink = Sink::default();
    assert_eq!(
        ready(transfer_content(&mut &b"abc"[..], &mut sink, 3, hash)),
        Ok(())
    );
    assert_eq!(sink.0, b"abc");
    for bytes in [&b"ab"[..], &b"abcd"[..]] {
        assert_eq!(
            ready(transfer_content(
                &mut &bytes[..],
                &mut Sink::default(),
                3,
                hash
            )),
            Err(ContentStoreError::LengthMismatch)
        );
    }
    assert_eq!(
        ready(transfer_content(
            &mut &b"ABC"[..],
            &mut Sink::default(),
            3,
            hash
        )),
        Err(ContentStoreError::HashMismatch)
    );
    assert_eq!(
        ready(transfer_content(
            &mut &b""[..],
            &mut Sink::default(),
            0,
            Sha256::digest(b"")
        )),
        Ok(())
    );
    assert_eq!(
        ready(transfer_content(
            &mut &b"x"[..],
            &mut Sink::default(),
            0,
            Sha256::digest(b"")
        )),
        Err(ContentStoreError::LengthMismatch)
    );
}

struct InvalidCount;
impl ContentSource for InvalidCount {
    fn read<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> ApplicationFuture<'a, Result<usize, ContentStoreError>> {
        Box::pin(async move { Ok(buffer.len() + 1) })
    }
}
#[test]
fn invalid_reader_lengths_and_oversized_declarations_fail_before_writing() {
    for length in [0, 1, CONTENT_TRANSFER_CHUNK_BYTES as u64 + 1] {
        let mut sink = Sink::default();
        assert_eq!(
            ready(transfer_content(
                &mut InvalidCount,
                &mut sink,
                length,
                Sha256::digest(b"")
            )),
            Err(ContentStoreError::Invariant)
        );
        assert!(sink.0.is_empty());
    }
    assert_eq!(
        ready(transfer_content(
            &mut InvalidCount,
            &mut Sink::default(),
            MAX_CONTENT_BYTES + 1,
            Sha256::digest(b"")
        )),
        Err(ContentStoreError::TooLarge)
    );
}

struct FailedSink;
impl ContentSink for FailedSink {
    fn write<'a>(
        &'a mut self,
        _bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }
}
#[test]
fn sink_failure_does_not_consume_remaining_input() {
    let bytes = vec![0xa5; CONTENT_TRANSFER_CHUNK_BYTES * 3];
    let mut source = bytes.as_slice();
    assert_eq!(
        ready(transfer_content(
            &mut source,
            &mut FailedSink,
            bytes.len() as u64,
            Sha256::digest(&bytes)
        )),
        Err(ContentStoreError::Unavailable)
    );
    assert_eq!(source.len(), CONTENT_TRANSFER_CHUNK_BYTES * 2);
}
