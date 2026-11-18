use musteragent_execution_liveness::{
    ExecutionStopConfirmation, ExecutionStopControl, ExecutionStopError,
    StopReason as ExecutionStopReason,
};
use musteragent_model::ModelKey;
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, OpaqueExternalRef};
use musteragent_runtime_model::{
    CanonicalModelRequest, ModelEventStream, ModelExecution, ModelProvider, ModelStreamEvent,
    ModelText, ModelUsage, ProviderError, ProviderRequestRef, StopReason,
};
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_stream::Stream;
pub struct ScriptedProvider {
    pub stall: bool,
    pub preflight_stall: bool,
    pub sends: Arc<AtomicUsize>,
    pub progress_interval: Duration,
}
struct OwnedModel {
    binding: ExecutionBinding,
    request: CanonicalModelRequest,
    control: Arc<Control>,
    stall: bool,
    preflight_stall: bool,
    sends: Arc<AtomicUsize>,
    progress_interval: Duration,
}
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
        Box::pin(async move {
            self.cancelled.store(true, Ordering::SeqCst);
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
impl ModelProvider for ScriptedProvider {
    fn stream(&self, _: CanonicalModelRequest) -> ModelEventStream<'_> {
        panic!("ActivityV1 cannot invoke legacy stream")
    }
    fn execution(
        &self,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'_> + '_>, ProviderError> {
        musteragent_runtime_model::validate_model_execution_binding(&request, binding)?;
        Ok(Box::new(OwnedModel {
            binding,
            request,
            control: Arc::new(Control {
                binding,
                dispatched: AtomicBool::new(false),
                terminal: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
            }),
            stall: self.stall,
            preflight_stall: self.preflight_stall,
            sends: self.sends.clone(),
            progress_interval: self.progress_interval,
        }))
    }
}
impl<'a> ModelExecution<'a> for OwnedModel {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.control.clone()
    }
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>> {
        Box::pin(async {
            if self.preflight_stall {
                std::future::pending().await
            } else {
                Ok(())
            }
        })
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.control.dispatched.store(true, Ordering::SeqCst);
        let mut events = VecDeque::from([ModelStreamEvent::ResponseStarted {
            response_ref: ProviderRequestRef::from_opaque(
                OpaqueExternalRef::new("owned-vision-response").expect("reference"),
            ),
            response_model_key: ModelKey::new(self.request.route.resolved_model_key)
                .expect("model"),
        }]);
        if !self.stall {
            // Eight progress-bearing deltas that together form the structured
            // checkpoint every compaction result must carry (0181a49fd5).
            for delta in CHECKPOINT_DELTAS {
                events.push_back(ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new(delta).expect("text"),
                });
            }
            events.push_back(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 20,
                output_tokens: 30,
                total_tokens: Some(50),
                ..ModelUsage::default()
            }));
            events.push_back(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            });
        }
        Box::pin(ProgressStream {
            events,
            control: self.control,
            due: Box::pin(tokio::time::sleep(Duration::ZERO)),
            progress_interval: self.progress_interval,
        })
    }
}
const CHECKPOINT_DELTAS: [&str; 8] = [
    "## Goal\nPreserve the user task.\n\n",
    "## Constraints & Preferences\n- Keep durable evidence.\n\n",
    "## Progress\n### Done\n- Earlier history captured.\n\n",
    "### In Progress\n- Continue the unfinished work.\n\n",
    "### Blocked\n- (none)\n\n",
    "## Key Decisions\n- The Ledger stays authoritative.\n\n",
    "## Next Steps\n1. Continue the task.\n\n",
    "## Critical Context\n- visible image detail\n",
];
struct ProgressStream {
    events: VecDeque<ModelStreamEvent>,
    control: Arc<Control>,
    due: Pin<Box<tokio::time::Sleep>>,
    progress_interval: Duration,
}
impl Stream for ProgressStream {
    type Item = Result<ModelStreamEvent, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future as _;
        if self.due.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        let Some(event) = self.events.pop_front() else {
            return Poll::Pending;
        };
        if matches!(event, ModelStreamEvent::ResponseCompleted { .. }) {
            self.control.terminal.store(true, Ordering::SeqCst);
        }
        let interval = self.progress_interval;
        self.due
            .as_mut()
            .reset(tokio::time::Instant::now() + interval);
        Poll::Ready(Some(Ok(event)))
    }
}
