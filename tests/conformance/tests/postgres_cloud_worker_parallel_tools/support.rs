use musteragent_cloud_worker::WorkerTraceContextSource;
use musteragent_identity::{ToolCallId, UtcTimestamp};
use musteragent_loop::{AgentLoopClock, LoopPortError};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_contract::{ApplicationFuture, TraceContext, TraceParent};

pub(super) struct FixedClock(pub(super) UtcTimestamp);

impl RuntimeClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

impl AgentLoopClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

pub(super) struct FixedTrace;

impl WorkerTraceContextSource for FixedTrace {
    fn trace_context<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _call_id: &'a ToolCallId,
    ) -> ApplicationFuture<'a, Result<TraceContext, LoopPortError>> {
        Box::pin(async {
            Ok(TraceContext {
                traceparent: TraceParent::new(
                    "00-00000000000000000000000000000001-0000000000000001-00",
                )
                .expect("traceparent"),
                tracestate: None,
            })
        })
    }
}
