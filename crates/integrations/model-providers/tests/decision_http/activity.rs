use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_liveness::{ExecutionActivitySink, LivenessError, StopReason};
use musteragent_identity::VersionedKey;
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, ActivitySource, ExecutionBinding, ExecutionScope,
    InvocationExecutionTiming, LivenessPolicyRef,
};
use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Sink {
    binding: ExecutionBinding,
    starts: AtomicUsize,
    ends: AtomicUsize,
    accepted: tokio::sync::Notify,
}
impl ExecutionActivitySink for Sink {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn accepted(
        &self,
    ) -> Result<musteragent_execution_liveness::ExecutionAcceptance, LivenessError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.accepted.notify_one();
        Ok(musteragent_execution_liveness::ExecutionAcceptance::Observed)
    }
    fn wait_for_acceptance(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<(), musteragent_execution_liveness::LivenessError>,
    > {
        Box::pin(async { Ok(()) })
    }
    fn ended(&self) -> Result<(), LivenessError> {
        self.ends.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn advanced(&self, _: ActivitySource, _: NonZeroU64) -> Result<(), LivenessError> {
        panic!("partial unary JSON cannot fabricate canonical progress")
    }
}
fn activity_request() -> (DecisionModelRequest, ExecutionBinding, Arc<Sink>) {
    let mut request = request(ApiFormat::TypeSafeSystemOne);
    request.authority.invocation.timing =
        InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: LivenessPolicyRef {
                key: VersionedKey::new("musteragent.model-liveness@1").expect("key"),
                digest: Sha256::digest(b"sealed Home policy"),
            },
            business_deadline: None,
        });
    let context = &request.authority.invocation;
    let binding = ExecutionBinding {
        tenant_id: context.run_context.tenant_id,
        run_id: context.run_context.run_id,
        scope: ExecutionScope::Invocation(context.invocation_id),
        operation_identity: Sha256::digest(b"original decision operation"),
        owner_attempt_id: context.attempt_context.attempt_id,
        owner_fence: context.attempt_context.fencing_token,
        producer_generation: NonZeroU64::MIN,
    };
    let sink = Arc::new(Sink {
        binding,
        starts: AtomicUsize::new(0),
        ends: AtomicUsize::new(0),
        accepted: tokio::sync::Notify::new(),
    });
    (request, binding, sink)
}
pub(super) async fn no_hidden_lifetime() {
    let (url, capture) = paced_server(
        200,
        fixture()["response"].clone(),
        Duration::from_millis(250),
    )
    .await;
    let provider = support::provider_with_timeouts(
        &url,
        ApiFormat::TypeSafeSystemOne,
        Duration::from_millis(50),
        Duration::from_millis(100),
    );
    let (request, binding, sink) = activity_request();
    let operation = provider.execution(request, binding).expect("owned request");
    let control = operation.stop_control();
    let response = tokio::time::timeout(Duration::from_secs(2), operation.execute(sink.clone()))
        .await
        .expect("test bound")
        .expect("native response");
    assert_eq!(response.usage.input_tokens, 90);
    assert_eq!(sink.starts.load(Ordering::SeqCst), 1);
    assert_eq!(sink.ends.load(Ordering::SeqCst), 1);
    assert_eq!(
        control
            .stop(StopReason::UserCancelled)
            .await
            .expect("actual native end")
            .binding,
        binding
    );
    capture.await.expect("server");
}
pub(super) async fn in_flight_cancel_keeps_unknown() {
    let (url, capture) = paced_server(
        200,
        fixture()["response"].clone(),
        Duration::from_millis(250),
    )
    .await;
    let provider = support::provider(&url, ApiFormat::TypeSafeSystemOne);
    let (request, binding, sink) = activity_request();
    let operation = provider.execution(request, binding).expect("owned request");
    let control = operation.stop_control();
    let (result, stopped) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(operation.execute(sink.clone()), async {
            sink.accepted.notified().await;
            control.stop(StopReason::UserCancelled).await
        })
    })
    .await
    .expect("finite cancellation");
    assert!(result.is_err());
    assert!(stopped.is_err(), "closing HTTP does not prove remote stop");
    assert_eq!(sink.ends.load(Ordering::SeqCst), 0);
    capture.await.expect("server");
}
pub(super) async fn rejected_prepared_request_never_dispatches() {
    let provider = support::provider("http://127.0.0.1:1/decisions", ApiFormat::TypeSafeSystemOne);
    let (request, binding, sink) = activity_request();
    let operation = provider.execution(request, binding).expect("owned request");
    let control = operation.stop_control();
    assert_eq!(
        control
            .stop(StopReason::UserCancelled)
            .await
            .expect("not dispatched")
            .binding,
        binding
    );
    assert!(operation.execute(sink.clone()).await.is_err());
    assert_eq!(sink.starts.load(Ordering::SeqCst), 0);
}
