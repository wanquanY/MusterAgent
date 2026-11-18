//! Keep production execution supervision and vary only provider terminal output.
use super::owned;
use musteragent_execution_liveness::ExecutionStopControl;
use musteragent_runtime_contract::{ApplicationFuture, execution_liveness::ExecutionBinding};
use musteragent_runtime_model::{
    CanonicalModelRequest, CanonicalRole, ModelEventStream, ModelExecution, ModelProvider,
    ModelStreamEvent, ModelText, ProviderError, StopReason,
};
use std::sync::Arc;
use tokio_stream::StreamExt;

pub struct ContinuedProvider(pub owned::ScriptedProvider);
struct ContinuedExecution<'a> {
    inner: Box<dyn ModelExecution<'a> + 'a>,
    continuation: bool,
}
impl ModelProvider for ContinuedProvider {
    fn stream(&self, _: CanonicalModelRequest) -> ModelEventStream<'_> {
        panic!("supervised execution required")
    }
    fn execution(
        &self,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'_> + '_>, ProviderError> {
        let continuation = request
            .input
            .iter()
            .any(|v| v.role == CanonicalRole::Assistant);
        Ok(Box::new(ContinuedExecution {
            inner: self.0.execution(request, binding)?,
            continuation,
        }))
    }
}
impl<'a> ModelExecution<'a> for ContinuedExecution<'a> {
    fn binding(&self) -> ExecutionBinding {
        self.inner.binding()
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.inner.stop_control()
    }
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>> {
        self.inner.preflight()
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        let continuation = self.continuation;
        Box::pin(self.inner.into_stream().map(move |event| {
            event.map(|event| match event {
                ModelStreamEvent::ResponseCompleted { .. } if !continuation => {
                    ModelStreamEvent::ResponseCompleted {
                        stop_reason: StopReason::MaxOutputTokens,
                    }
                }
                ModelStreamEvent::TextDelta { output_index, .. } if continuation => {
                    ModelStreamEvent::TextDelta {
                        output_index,
                        delta: ModelText::new("continued checkpoint detail. ").unwrap(),
                    }
                }
                other => other,
            })
        }))
    }
}
