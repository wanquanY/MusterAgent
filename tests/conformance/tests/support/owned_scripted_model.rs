//! Owned execution for scripted providers. The script is dispatched only after
//! the Home marker; cancellation cannot manufacture remote completion proof.
use musteragent_execution_liveness::{
    ExecutionStopConfirmation, ExecutionStopControl, ExecutionStopError,
    StopReason as ExecutionStopReason,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, execution_liveness::ExecutionBinding,
};
use musteragent_runtime_model::{
    CanonicalModelRequest, ModelEventStream, ModelExecution, ModelProvider, ModelStreamEvent,
    ProviderError,
};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio_stream::Stream;

pub struct OwnedScriptedModel(pub Arc<dyn ModelProvider>);
struct Control {
    binding: ExecutionBinding,
    dispatched: AtomicBool,
    terminal: AtomicBool,
    cancelled: AtomicBool,
}
impl ExecutionStopControl for Control {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop(
        &self,
        _: ExecutionStopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        self.cancelled.store(true, Ordering::SeqCst);
        Box::pin(async move {
            if !self.dispatched.load(Ordering::SeqCst) || self.terminal.load(Ordering::SeqCst) {
                Ok(ExecutionStopConfirmation {
                    binding: self.binding,
                })
            } else {
                Err(ExecutionStopError {
                    code: ErrorCode::DependencyUnavailable,
                    rejection: None,
                })
            }
        })
    }
}
struct Operation<'a> {
    provider: &'a dyn ModelProvider,
    request: CanonicalModelRequest,
    control: Arc<Control>,
}
impl ModelProvider for OwnedScriptedModel {
    fn stream(&self, _: CanonicalModelRequest) -> ModelEventStream<'_> {
        panic!("supervised fixture must use execution ownership")
    }
    fn execution(
        &self,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'_> + '_>, ProviderError> {
        musteragent_runtime_model::validate_model_execution_binding(&request, binding)?;
        Ok(Box::new(Operation {
            provider: self.0.as_ref(),
            request,
            control: Arc::new(Control {
                binding,
                dispatched: AtomicBool::new(false),
                terminal: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
            }),
        }))
    }
}
impl<'a> ModelExecution<'a> for Operation<'a> {
    fn binding(&self) -> ExecutionBinding {
        self.control.binding
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.control.clone()
    }
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>> {
        self.provider.preflight(&self.request)
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        if self.control.cancelled.load(Ordering::SeqCst) {
            return Box::pin(tokio_stream::empty());
        }
        self.control.dispatched.store(true, Ordering::SeqCst);
        Box::pin(ObservedStream {
            inner: self.provider.stream(self.request),
            control: self.control,
        })
    }
}
struct ObservedStream<'a> {
    inner: ModelEventStream<'a>,
    control: Arc<Control>,
}
impl Stream for ObservedStream<'_> {
    type Item = Result<ModelStreamEvent, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.inner.as_mut().poll_next(cx);
        if matches!(
            &next,
            Poll::Ready(Some(Ok(ModelStreamEvent::ResponseCompleted { .. })))
        ) {
            self.control.terminal.store(true, Ordering::SeqCst);
        }
        next
    }
}
