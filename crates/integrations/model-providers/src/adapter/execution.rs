//! Resource ownership for an activity-supervised Direct Provider operation.
//! HTTP cancellation is local quiescence; only a verified provider terminal
//! response is evidence of remote completion after dispatch.
use super::*;
use musteragent_execution_liveness::{
    ExecutionStopConfirmation, ExecutionStopControl, ExecutionStopError,
    StopReason as ExecutionStopReason,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, execution_liveness::ExecutionBinding,
};
use musteragent_runtime_model::{ModelExecution, validate_model_execution_binding};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub(super) struct DirectModelExecution<'provider> {
    provider: &'provider DirectModelProvider,
    request: CanonicalModelRequest,
    prepared: Option<PreparedDirectRequest>,
    annotations: musteragent_runtime_model::ModelRequestAnnotations,
    preflight_called: bool,
    control: Arc<ModelControl>,
}
impl<'provider> DirectModelExecution<'provider> {
    pub(super) fn new(
        provider: &'provider DirectModelProvider,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Self, ProviderError> {
        validate_model_execution_binding(&request, binding)?;
        Ok(Self::prepared(provider, request, binding))
    }

    pub(super) fn recovered(
        provider: &'provider DirectModelProvider,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Self, ProviderError> {
        musteragent_runtime_model::validate_model_recovery_binding(&request, binding)?;
        Ok(Self::prepared(provider, request, binding))
    }

    fn prepared(
        provider: &'provider DirectModelProvider,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Self {
        Self {
            provider,
            request,
            prepared: None,
            annotations: Default::default(),
            preflight_called: false,
            control: ModelControl::new(binding),
        }
    }
}
impl<'provider> ModelExecution<'provider> for DirectModelExecution<'provider> {
    fn binding(&self) -> ExecutionBinding {
        self.control.binding
    }
    fn annotate(
        &mut self,
        annotations: musteragent_runtime_model::ModelRequestAnnotations,
    ) -> Result<(), ProviderError> {
        if self.preflight_called {
            return Err(protocol_error());
        }
        self.annotations = annotations;
        Ok(())
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.control.clone()
    }
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>> {
        Box::pin(async move {
            if self.preflight_called {
                return Err(protocol_error());
            }
            self.preflight_called = true;
            let _work = self.control.begin_work()?;
            let prepared = tokio::select! {
                biased;
                () = self.control.cancelled.cancelled() => return Err(cancelled()),
                prepared = self.provider.prepare_annotated_request(&self.request, &self.annotations) => prepared?,
            };
            self.prepared = Some(prepared);
            Ok(())
        })
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'provider> {
        Box::pin(stream! {
            let Self { provider, request, prepared, control, .. } = *self;
            let _work = match control.begin_work() {
                Ok(work) => work,
                Err(error) => { yield Err(error); return; }
            };
            let Some(prepared) = prepared else {
                yield Err(protocol_error());
                return;
            };
            let mut inner = provider.stream_prepared(request, prepared, Some(control.clone()));
            loop {
                let next = tokio::select! {
                    biased;
                    () = control.cancelled.cancelled() => {
                        // Explicitly release the transport before publishing local
                        // quiescence. This is never a remote-stop acknowledgement.
                        drop(inner);
                        drop(_work);
                        yield Err(cancelled());
                        return;
                    }
                    next = inner.next() => next,
                };
                let Some(next) = next else { return; };
                yield next;
            }
        })
    }
}

#[derive(Copy, Clone)]
enum Dispatch {
    NotDispatched,
    InFlight,
    TerminalObserved,
}
struct PhysicalState {
    dispatch: Dispatch,
    busy: bool,
}
pub(super) struct ModelControl {
    binding: ExecutionBinding,
    pub(super) cancelled: CancellationToken,
    state: Mutex<PhysicalState>,
    changed: Notify,
}
impl ModelControl {
    pub(super) fn new(binding: ExecutionBinding) -> Arc<Self> {
        Arc::new(Self {
            binding,
            cancelled: CancellationToken::new(),
            state: Mutex::new(PhysicalState {
                dispatch: Dispatch::NotDispatched,
                busy: false,
            }),
            changed: Notify::new(),
        })
    }

    pub(super) fn begin_work(self: &Arc<Self>) -> Result<WorkGuard, ProviderError> {
        let mut state = self.state.lock().map_err(|_| protocol_error())?;
        if self.cancelled.is_cancelled() {
            return Err(cancelled());
        }
        if state.busy {
            return Err(protocol_error());
        }
        state.busy = true;
        Ok(WorkGuard(self.clone()))
    }
    /// Called immediately before the one actual outbound request, never from
    /// preflight or merely when constructing an unpolled stream.
    pub(super) fn will_dispatch(&self) -> Result<(), ProviderError> {
        let mut state = self.state.lock().map_err(|_| protocol_error())?;
        if self.cancelled.is_cancelled() {
            return Err(cancelled());
        }
        if !state.busy || !matches!(state.dispatch, Dispatch::NotDispatched) {
            return Err(protocol_error());
        }
        state.dispatch = Dispatch::InFlight;
        Ok(())
    }
    /// Only the native adapter calls this after validating the actual remote
    /// response. EOF, Future Drop and a transport error never call this hook.
    pub(super) fn terminal_observed(&self) -> Result<(), ProviderError> {
        let mut state = self.state.lock().map_err(|_| protocol_error())?;
        if !state.busy || !matches!(state.dispatch, Dispatch::InFlight) {
            return Err(protocol_error());
        }
        state.dispatch = Dispatch::TerminalObserved;
        Ok(())
    }
}
impl ExecutionStopControl for ModelControl {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop(
        &self,
        _: ExecutionStopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async move {
            self.cancelled.cancel();
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let state = self.state.lock().map_err(|_| unconfirmed())?;
                    if !state.busy {
                        return match state.dispatch {
                            Dispatch::NotDispatched | Dispatch::TerminalObserved => {
                                Ok(ExecutionStopConfirmation {
                                    binding: self.binding,
                                })
                            }
                            Dispatch::InFlight => Err(unconfirmed()),
                        };
                    }
                }
                changed.await;
            }
        })
    }
}
pub(super) struct WorkGuard(Arc<ModelControl>);
impl Drop for WorkGuard {
    fn drop(&mut self) {
        // Dropping local work only records local quiescence. Dispatch evidence
        // is retained, so a dropped in-flight request remains unconfirmed.
        if let Ok(mut state) = self.0.state.lock() {
            state.busy = false;
        }
        self.0.changed.notify_waiters();
    }
}
fn cancelled() -> ProviderError {
    provider_error(
        ProviderErrorKind::Cancelled,
        ProviderErrorCode::Cancelled,
        false,
        None,
    )
}
fn unconfirmed() -> ExecutionStopError {
    ExecutionStopError {
        code: ErrorCode::DependencyUnavailable,
        rejection: None,
    }
}
