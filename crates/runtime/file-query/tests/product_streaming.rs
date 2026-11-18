mod support;

use musteragent_content_ref::ContentRef;
use musteragent_identity::TenantId;
use musteragent_runtime_content::{
    CONTENT_TRANSFER_CHUNK_BYTES, ContentSink, ContentStore, ContentStoreError, PutContent,
    transfer_content,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use support::*;

struct StreamingStore {
    values: Arc<Bytes>,
    copies: AtomicUsize,
}
impl ContentStore for StreamingStore {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        panic!("product transfer is read only")
    }
    fn read<'a>(
        &'a self,
        _: TenantId,
        _: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        panic!("a bounded product transfer must not fall back to whole-file reads")
    }
    fn copy_to<'a>(
        &'a self,
        _: TenantId,
        reference: &'a ContentRef,
        sink: &'a mut dyn ContentSink,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            self.copies.fetch_add(1, Ordering::SeqCst);
            // In-memory test source only. Production ContentStore owns its
            // encrypted physical stream; this checks invocation/backpressure.
            let bytes = self
                .values
                .values
                .lock()
                .expect("fixture")
                .get(&reference.content_id)
                .cloned()
                .ok_or(ContentStoreError::NotFound)?;
            transfer_content(
                &mut bytes.as_slice(),
                sink,
                reference.byte_length,
                reference.sha256,
            )
            .await
        })
    }
}

struct Authority(Mutex<FileQueryContext>);
impl FileQueryAuthority for Authority {
    fn resolve<'a>(
        &'a self,
        access: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async move {
            let context = self.0.lock().expect("fixture").clone();
            assert_eq!(context.actor, access.actor);
            assert_eq!(context.conversation_id, access.conversation_id);
            Ok(context)
        })
    }
}
fn service(
    f: &Fixture,
) -> (
    FileQueryService,
    FileQueryAccess,
    Arc<StreamingStore>,
    Arc<Authority>,
) {
    let store = Arc::new(StreamingStore {
        values: f.bytes.clone(),
        copies: AtomicUsize::new(0),
    });
    let authority = Arc::new(Authority(Mutex::new(f.context.clone())));
    let application = FileQueryApplication::new(
        f.catalog.clone(),
        Arc::new(NoProject),
        Arc::new(ContentFileReader::new(store.clone())),
    )
    .with_content_store(store.clone());
    let service = FileQueryService::new(authority.clone(), Arc::new(application))
        .with_product_capacity(Arc::new(tokio::sync::Semaphore::new(
            2 * CONTENT_TRANSFER_CHUNK_BYTES,
        )));
    let access = FileQueryAccess {
        actor: f.context.actor,
        conversation_id: f.context.conversation_id,
        workspace_id: None,
        run: None,
    };
    (service, access, store, authority)
}

#[derive(Default)]
struct PrivateSink {
    bytes: Vec<u8>,
    max_chunk: usize,
    hook: Option<Box<dyn Fn() + Send>>,
    reject: bool,
}
impl ContentSink for PrivateSink {
    fn write<'a>(
        &'a mut self,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            assert!(bytes.len() <= CONTENT_TRANSFER_CHUNK_BYTES);
            self.max_chunk = self.max_chunk.max(bytes.len());
            if self.reject {
                return Err(ContentStoreError::Unavailable);
            }
            self.bytes.extend_from_slice(bytes);
            if let Some(hook) = self.hook.take() {
                hook();
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn original_large_attachment_streams_under_one_bounded_transfer_budget() {
    let f = Fixture::new();
    let bytes = vec![b'x'; 2 * 1024 * 1024 + 17];
    let content = f.attachment("large.txt", "text/plain", &bytes);
    let reference = f.first().await;
    let (service, access, store, _) = service(&f);
    assert_eq!(
        service
            .read_content(&access, &reference, &content)
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::Unavailable
    );
    let mut sink = PrivateSink::default();
    let descriptor = service
        .copy_content_to(&access, &reference, &content, &mut sink)
        .await
        .expect("valid fixture and authorized transfer");
    assert_eq!(descriptor.primary, content);
    assert_eq!(descriptor.entry.file_ref, reference);
    assert_eq!(sink.bytes, bytes);
    assert_eq!(sink.max_chunk, CONTENT_TRANSFER_CHUNK_BYTES);
    assert_eq!(store.copies.load(Ordering::SeqCst), 1);
    assert_eq!(
        *f.bytes
            .reads
            .lock()
            .expect("valid fixture and authorized transfer"),
        0
    );
}

#[tokio::test]
async fn unrelated_content_is_rejected_before_the_first_write() {
    let f = Fixture::new();
    f.attachment("original.txt", "text/plain", b"safe");
    let unrelated = f.bytes.add("text/plain", b"other private data");
    let reference = f.first().await;
    let (service, access, store, _) = service(&f);
    let mut sink = PrivateSink::default();
    assert_eq!(
        service
            .copy_content_to(&access, &reference, &unrelated, &mut sink)
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::Denied
    );
    assert!(sink.bytes.is_empty());
    assert_eq!(store.copies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exact_artifact_dependency_is_streamed_without_latest_version_substitution() {
    let f = Fixture::new();
    let primary = f.artifact(
        "deck",
        musteragent_artifact::ArtifactKind::Presentation,
        "application/json",
        b"{}",
    );
    let asset = f.bytes.add("image/png", b"original asset");
    f.projection
        .0
        .lock()
        .expect("valid fixture and authorized transfer")
        .artifacts[0]
        .current_dependency_refs
        .push(asset.clone());
    let reference = f.first().await;
    let (service, access, store, _) = service(&f);
    let mut sink = PrivateSink::default();
    let descriptor = service
        .copy_content_to(&access, &reference, &asset, &mut sink)
        .await
        .expect("valid fixture and authorized transfer");
    assert_eq!(descriptor.primary, primary);
    assert_eq!(sink.bytes, b"original asset");
    f.projection
        .0
        .lock()
        .expect("valid fixture and authorized transfer")
        .artifacts[0]
        .current_content_ref = f.bytes.add("application/json", b"new version");
    let mut denied = PrivateSink::default();
    assert_eq!(
        service
            .copy_content_to(&access, &reference, &asset, &mut denied)
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::Changed
    );
    assert!(denied.bytes.is_empty());
    assert_eq!(store.copies.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn withdrawn_source_after_copy_never_returns_a_publishable_receipt() {
    let f = Fixture::new();
    let content = f.attachment("secret.txt", "text/plain", b"secret");
    let reference = f.first().await;
    let (service, access, _, _) = service(&f);
    let projection = f.projection.clone();
    let mut sink = PrivateSink {
        hook: Some(Box::new(move || {
            projection
                .0
                .lock()
                .expect("valid fixture and authorized transfer")
                .messages
                .clear()
        })),
        ..Default::default()
    };
    assert_eq!(
        service
            .copy_content_to(&access, &reference, &content, &mut sink)
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::NotFound
    );
    // Private staging has bytes but NO successful receipt; caller must discard.
    assert_eq!(sink.bytes, b"secret");
}

#[tokio::test]
async fn authority_change_after_copy_never_returns_a_publishable_receipt() {
    let f = Fixture::new();
    let content = f.attachment("secret.txt", "text/plain", b"secret");
    let reference = f.first().await;
    let (service, access, _, authority) = service(&f);
    let mut sink = PrivateSink {
        hook: Some(Box::new(move || {
            authority
                .0
                .lock()
                .expect("valid fixture and authorized transfer")
                .authority_revision = "revoked".into()
        })),
        ..Default::default()
    };
    assert_eq!(
        service
            .copy_content_to(&access, &reference, &content, &mut sink)
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::Denied
    );
    assert_eq!(sink.bytes, b"secret");
}

#[tokio::test]
async fn unsupported_streaming_does_not_fall_back_to_buffering() {
    let f = Fixture::new();
    let content = f.attachment("a.txt", "text/plain", b"private");
    let reference = f.first().await;
    let mut sink = PrivateSink::default();
    assert_eq!(
        f.application
            .copy_content_to(&f.context, &reference, &content, &mut sink)
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::Unavailable
    );
    assert!(sink.bytes.is_empty());
    assert_eq!(
        *f.bytes
            .reads
            .lock()
            .expect("valid fixture and authorized transfer"),
        0
    );
}

#[tokio::test]
async fn integrity_failure_and_sink_error_release_capacity_without_success() {
    let f = Fixture::new();
    let content = f.attachment("a.txt", "text/plain", b"safe");
    let reference = f.first().await;
    let (service, access, _, _) = service(&f);
    f.bytes
        .values
        .lock()
        .expect("valid fixture and authorized transfer")
        .insert(content.content_id, b"evil".to_vec());
    assert_eq!(
        service
            .copy_content_to(&access, &reference, &content, &mut PrivateSink::default())
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::Integrity
    );
    f.bytes
        .values
        .lock()
        .expect("valid fixture and authorized transfer")
        .insert(content.content_id, b"safe".to_vec());
    let mut failed = PrivateSink {
        reject: true,
        ..Default::default()
    };
    assert_eq!(
        service
            .copy_content_to(&access, &reference, &content, &mut failed)
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::Unavailable
    );
    let mut success = PrivateSink::default();
    service
        .copy_content_to(&access, &reference, &content, &mut success)
        .await
        .expect("valid fixture and authorized transfer");
    assert_eq!(success.bytes, b"safe");
}

struct SuspendedSink;
impl ContentSink for SuspendedSink {
    fn write<'a>(
        &'a mut self,
        _: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(std::future::pending())
    }
}
#[tokio::test]
async fn slow_consumer_owns_capacity_until_completion_or_future_retirement() {
    use std::task::Poll;
    let f = Fixture::new();
    let content = f.attachment("a.txt", "text/plain", b"safe");
    let reference = f.first().await;
    let (service, access, store, _) = service(&f);
    let mut slow = SuspendedSink;
    let mut pending = Box::pin(service.copy_content_to(&access, &reference, &content, &mut slow));
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(store.copies.load(Ordering::SeqCst), 1);
    let mut other = PrivateSink::default();
    assert_eq!(
        service
            .copy_content_to(&access, &reference, &content, &mut other)
            .await
            .expect_err("transfer must fail closed"),
        FileQueryError::Unavailable
    );
    assert!(other.bytes.is_empty());
    drop(pending); // No receipt; platform owner must still discard/join its sink.
    service
        .copy_content_to(&access, &reference, &content, &mut other)
        .await
        .expect("valid fixture and authorized transfer");
    assert_eq!(other.bytes, b"safe");
}

#[tokio::test]
async fn empty_content_still_rechecks_authority_and_uses_exact_empty_digest() {
    let f = Fixture::new();
    let content = f.attachment("empty.txt", "text/plain", b"");
    let reference = f.first().await;
    let (service, access, store, _) = service(&f);
    let mut sink = PrivateSink::default();
    let descriptor = service
        .copy_content_to(&access, &reference, &content, &mut sink)
        .await
        .expect("valid fixture and authorized transfer");
    assert_eq!(descriptor.primary, content);
    assert!(sink.bytes.is_empty());
    assert_eq!(store.copies.load(Ordering::SeqCst), 1);
}
