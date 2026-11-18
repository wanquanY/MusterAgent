use super::*;
use futures_util::FutureExt;
use musteragent_capability::{CapabilityLocation, IdempotencyContract};
use musteragent_execution_liveness::{ExecutionActivitySink, LivenessError, StopReason};
use musteragent_policy::{ApprovalRequirement, DataClassification, Effect, EgressPolicy};
use musteragent_runtime_capability::{BuiltinTimeCapability, CapabilityExecution, RuntimeClock};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::execution_liveness::ActivitySource;
use std::{
    collections::BTreeSet,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Store {
    reads: AtomicUsize,
    puts: AtomicUsize,
    input: Mutex<Option<Vec<u8>>>,
}
impl ContentStore for Store {
    fn read<'a>(
        &'a self,
        _: TenantId,
        _: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            std::future::poll_fn(|_| {
                self.input
                    .lock()
                    .expect("input")
                    .take()
                    .map_or(std::task::Poll::Pending, |bytes| {
                        std::task::Poll::Ready(Ok(bytes))
                    })
            })
            .await
        })
    }
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            self.puts.fetch_add(1, Ordering::SeqCst);
            Ok(ContentRef {
                content_id: ContentId::generate(),
                media_type: request.media_type,
                byte_length: request.bytes.len() as u64,
                sha256: Sha256::digest(&request.bytes),
            })
        })
    }
}
struct Clock;
impl RuntimeClock for Clock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(100)
    }
}
struct Sink {
    binding: ExecutionBinding,
    ended: AtomicUsize,
    acceptance_committed: AtomicBool,
}
impl ExecutionActivitySink for Sink {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn accepted(
        &self,
    ) -> Result<musteragent_execution_liveness::ExecutionAcceptance, LivenessError> {
        Ok(musteragent_execution_liveness::ExecutionAcceptance::Observed)
    }
    fn wait_for_acceptance(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<(), musteragent_execution_liveness::LivenessError>,
    > {
        Box::pin(std::future::poll_fn(|_| {
            if self.acceptance_committed.load(Ordering::SeqCst) {
                std::task::Poll::Ready(Ok(()))
            } else {
                std::task::Poll::Pending
            }
        }))
    }
    fn ended(&self) -> Result<(), LivenessError> {
        self.ended.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn advanced(&self, _: ActivitySource, _: NonZeroU64) -> Result<(), LivenessError> {
        Ok(())
    }
}
fn operation() -> (CapabilityExecution, Arc<Store>, Arc<Sink>) {
    let (mut invocation, owner, binding) = fixture();
    invocation.tool_key =
        ToolKey::from_versioned_key(VersionedKey::new("system.time.current@1").expect("tool"));
    invocation.capability_key = CapabilityKey::from_versioned_key(
        VersionedKey::new("core.time.read@1").expect("capability"),
    );
    let manifest = CapabilityManifest {
        capability_id: CapabilityId::generate(),
        key: invocation.capability_key.clone(),
        effect: Effect::Read,
        approval: ApprovalRequirement::Never,
        location: CapabilityLocation::Cloud,
        provider_id: invocation.capability_provider_id,
        timeout: Duration::from_secs(30),
        max_attempts: std::num::NonZeroU32::MIN,
        idempotency: IdempotencyContract::ReadOnly,
        required_scopes: BTreeSet::from([DottedKey::new("time.read").expect("scope")]),
        required_contexts: BTreeSet::new(),
        data_classification: DataClassification::Internal,
        egress_policy: EgressPolicy::Internet,
    };
    let store = Arc::new(Store::default());
    let provider = Arc::new(
        BuiltinTimeCapability::new(manifest, store.clone(), Arc::new(Clock)).expect("provider"),
    );
    let operation = provider
        .execution(owner, invocation, binding)
        .expect("prepared without I/O");
    (
        operation,
        store,
        Arc::new(Sink {
            binding,
            ended: AtomicUsize::new(0),
            acceptance_committed: AtomicBool::new(true),
        }),
    )
}
pub(super) fn stop_before_poll_permanently_prevents_time_io() {
    let (operation, store, sink) = operation();
    let control = operation.stop_control();
    assert!(
        control
            .stop(StopReason::UserCancelled)
            .now_or_never()
            .expect("never dispatched")
            .is_ok()
    );
    assert!(matches!(
        operation.execute(sink.clone()).now_or_never(),
        Some(CapabilityOutcome::Failed { .. })
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    assert_eq!(store.puts.load(Ordering::SeqCst), 0);
    assert_eq!(
        sink.ended.load(Ordering::SeqCst),
        0,
        "prevented dispatch is not a native terminal event"
    );
}
pub(super) fn dropping_active_time_work_cannot_confirm_physical_stop() {
    let (operation, store, sink) = operation();
    let control = operation.stop_control();
    let mut work = operation.execute(sink.clone());
    assert!(work.as_mut().now_or_never().is_none());
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    let mut stopped = control.stop(StopReason::UserCancelled);
    assert!(stopped.as_mut().now_or_never().is_none());
    drop(work);
    assert!(
        stopped
            .now_or_never()
            .expect("abandoned ownership reported")
            .is_err()
    );
    assert_eq!(sink.ended.load(Ordering::SeqCst), 0);
}
pub(super) fn time_stop_waits_for_the_original_content_operation_to_finish() {
    let (operation, store, sink) = operation();
    let control = operation.stop_control();
    let mut work = operation.execute(sink.clone());
    assert!(work.as_mut().now_or_never().is_none());
    let mut stopped = control.stop(StopReason::IdleExpired);
    assert!(stopped.as_mut().now_or_never().is_none());
    *store.input.lock().expect("input") = Some(b"{}".to_vec());
    assert!(matches!(
        work.now_or_never(),
        Some(CapabilityOutcome::Failed { .. })
    ));
    assert!(
        stopped
            .now_or_never()
            .expect("content drain proven")
            .is_ok()
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.puts.load(Ordering::SeqCst),
        0,
        "stop prevents later stages"
    );
    assert_eq!(sink.ended.load(Ordering::SeqCst), 1);
}

pub(super) fn acceptance_commit_orders_local_io_and_cancellation() {
    for cancel in [false, true] {
        let (operation, store, sink) = operation();
        sink.acceptance_committed.store(false, Ordering::SeqCst);
        *store.input.lock().expect("input") = Some(b"{}".to_vec());
        let control = operation.stop_control();
        let mut work = operation.execute(sink.clone());
        assert!(work.as_mut().now_or_never().is_none());
        assert_eq!(
            store.reads.load(Ordering::SeqCst),
            0,
            "no I/O before commit"
        );
        if cancel {
            let mut stopped = control.stop(StopReason::UserCancelled);
            assert!(stopped.as_mut().now_or_never().is_none());
            assert!(matches!(
                work.now_or_never(),
                Some(CapabilityOutcome::Failed { .. })
            ));
            assert!(stopped.now_or_never().expect("bounded stop").is_ok());
            assert_eq!(store.reads.load(Ordering::SeqCst), 0);
            assert_eq!(store.puts.load(Ordering::SeqCst), 0);
        } else {
            sink.acceptance_committed.store(true, Ordering::SeqCst);
            assert!(matches!(
                work.now_or_never(),
                Some(CapabilityOutcome::Succeeded { .. })
            ));
            assert_eq!(store.reads.load(Ordering::SeqCst), 1);
            assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        }
    }
}
