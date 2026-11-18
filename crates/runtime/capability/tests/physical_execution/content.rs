use super::*;
use futures_util::FutureExt as _;
use musteragent_execution_liveness::{ExecutionOperation, StopReason};
use musteragent_runtime_capability::{CooperativeExecution, OperationContentStore};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Store {
    reads: AtomicUsize,
    reclaimable: AtomicUsize,
}
impl ContentStore for Store {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { panic!("must retain reclaimable write semantics") })
    }
    fn put_reclaimable_at(
        &self,
        id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            self.reclaimable.fetch_add(1, Ordering::SeqCst);
            Ok(ContentRef {
                content_id: id,
                media_type: request.media_type,
                byte_length: request.bytes.len() as u64,
                sha256: Sha256::digest(&request.bytes),
            })
        })
    }
    fn read<'a>(
        &'a self,
        _: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if reference.byte_length == 0 {
                return Err(ContentStoreError::NotFound);
            }
            Ok(b"verified".to_vec())
        })
    }
}

#[test]
fn finite_content_verification_emits_real_progress_and_preserves_prewrite_ownership() {
    let (_, _, binding) = fixture();
    let sink = dependencies::Sink::new(binding);
    let work_sink = sink.clone();
    let store = Arc::new(Store {
        reads: AtomicUsize::new(0),
        reclaimable: AtomicUsize::new(0),
    });
    let original = store.clone();
    let operation = Box::new(CooperativeExecution::new(
        binding,
        || false,
        move |context| {
            Box::pin(async move {
                let observed = OperationContentStore::new(store, context.clone());
                let reference = ContentRef {
                    content_id: ContentId::generate(),
                    media_type: MediaType::new("text/plain").unwrap(),
                    byte_length: 8,
                    sha256: Sha256::digest(b"verified"),
                };
                // A large finite proof advances for each distinct verified object, not
                // merely when the final aggregate (potentially minutes later) returns.
                for _ in 0..337 {
                    let mut r = reference.clone();
                    r.content_id = ContentId::generate();
                    observed.read(binding.tenant_id, &r).await.unwrap();
                }
                assert_eq!(work_sink.progress(), 337);
                observed.read(binding.tenant_id, &reference).await.unwrap();
                observed.read(binding.tenant_id, &reference).await.unwrap();
                observed.read_batch(binding.tenant_id, &[]).await.unwrap();
                assert_eq!(work_sink.progress(), 338);
                let mut corrupt = reference.clone();
                corrupt.sha256 = Sha256::digest(b"corrupt!");
                assert_eq!(
                    observed.read(binding.tenant_id, &corrupt).await,
                    Err(ContentStoreError::HashMismatch)
                );
                corrupt.byte_length = 0;
                assert_eq!(
                    observed.read(binding.tenant_id, &corrupt).await,
                    Err(ContentStoreError::NotFound)
                );
                assert_eq!(work_sink.progress(), 338);
                observed
                    .put_reclaimable_at(
                        ContentId::generate(),
                        PutContent {
                            tenant_id: binding.tenant_id,
                            media_type: reference.media_type.clone(),
                            bytes: b"prewrite".to_vec(),
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(work_sink.progress(), 339);
                context.quiescent();
                true
            })
        },
    ));
    assert_eq!(operation.execute(sink).now_or_never(), Some(true));
    assert_eq!(original.reclaimable.load(Ordering::SeqCst), 1);
}

#[test]
fn cancelled_content_operation_never_starts_a_later_read() {
    let (_, _, binding) = fixture();
    let sink = dependencies::Sink::new(binding);
    let store = Arc::new(Store {
        reads: AtomicUsize::new(0),
        reclaimable: AtomicUsize::new(0),
    });
    let original = store.clone();
    let operation = Box::new(CooperativeExecution::new(
        binding,
        || false,
        move |context| {
            Box::pin(async move {
                let observed = OperationContentStore::new(store, context.clone());
                context.cancelled().await;
                let reference = ContentRef {
                    content_id: ContentId::generate(),
                    media_type: MediaType::new("text/plain").unwrap(),
                    byte_length: 8,
                    sha256: Sha256::digest(b"verified"),
                };
                assert_eq!(
                    observed.read(binding.tenant_id, &reference).await,
                    Err(ContentStoreError::Unavailable)
                );
                context.quiescent();
                true
            })
        },
    ));
    let stop = operation.stop_control();
    let mut execution = operation.execute(sink.clone());
    assert!(execution.as_mut().now_or_never().is_none());
    let mut stopping = stop.stop(StopReason::UserCancelled);
    assert!(stopping.as_mut().now_or_never().is_none());
    assert_eq!(execution.now_or_never(), Some(true));
    assert!(stopping.now_or_never().unwrap().is_ok());
    assert_eq!(original.reads.load(Ordering::SeqCst), 0);
    assert_eq!(sink.progress(), 0);
}
