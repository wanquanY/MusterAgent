use super::*;
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, ExecutionStopControl,
};
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;
use musteragent_runtime_model::{DecisionModelExecution, validate_model_invocation_binding};

pub(super) struct Activity {
    pub control: Arc<execution::ModelControl>,
    pub sink: Arc<dyn ExecutionActivitySink>,
}
impl Activity {
    pub fn terminal(&self) -> Result<(), ProviderError> {
        self.control.terminal_observed()?;
        self.sink.ended().map_err(|_| protocol_error())
    }
}
pub(super) fn execution<'a>(
    provider: &'a DirectModelProvider,
    request: DecisionModelRequest,
    binding: ExecutionBinding,
) -> Result<DecisionModelExecution<'a>, ProviderError> {
    validate_model_invocation_binding(&request.authority.invocation, binding)?;
    Ok(Box::new(OwnedDecision {
        provider,
        request,
        binding,
        control: execution::ModelControl::new(binding),
    }))
}
struct OwnedDecision<'a> {
    provider: &'a DirectModelProvider,
    request: DecisionModelRequest,
    binding: ExecutionBinding,
    control: Arc<execution::ModelControl>,
}
impl<'a> ExecutionOperation<'a, Result<DecisionResponse, DecisionModelFailure>>
    for OwnedDecision<'a>
{
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.control.clone()
    }
    fn execute(
        self: Box<Self>,
        sink: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'a, Result<DecisionResponse, DecisionModelFailure>> {
        Box::pin(async move {
            if sink.binding() != self.binding {
                return Err(invalid().into());
            }
            let _work = self.control.begin_work()?;
            let call = self.provider.evaluate_decision(
                self.request,
                Some(Activity {
                    control: self.control.clone(),
                    sink,
                }),
            );
            tokio::select! {
                biased;
                () = self.control.cancelled.cancelled() => Err(provider_error(ProviderErrorKind::Cancelled, ProviderErrorCode::Cancelled, false, None).into()),
                result = call => result,
            }
        })
    }
}
