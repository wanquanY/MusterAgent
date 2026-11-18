//! Native dependency evidence shared by nested model adapters. This is an
//! observation bridge, not a second timer or a synthetic heartbeat producer.
use crate::OperationContext;
use musteragent_execution_liveness::{
    Closure, ExecutionAcceptance, ExecutionActivitySink, ExecutionStopConfirmation,
    ExecutionStopControl, ExecutionStopError, LivenessError, StopReason,
};
use musteragent_runtime_contract::execution_liveness::{ActivitySource, ExecutionBinding};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub(crate) struct AuxiliaryActivity {
    sink: Arc<dyn ExecutionActivitySink>,
    parent: Option<OperationContext>,
    terminal: AtomicBool,
}
impl AuxiliaryActivity {
    pub fn new(
        sink: Arc<dyn ExecutionActivitySink>,
        parent: Option<OperationContext>,
    ) -> Arc<Self> {
        Arc::new(Self {
            sink,
            parent,
            terminal: AtomicBool::new(false),
        })
    }
    pub fn terminal_observed(&self) -> bool {
        self.terminal.load(Ordering::SeqCst)
    }
    fn parent_progress(&self) -> Result<(), LivenessError> {
        if let Some(parent) = &self.parent {
            parent.completed_stage()?;
        }
        Ok(())
    }
}
impl ExecutionActivitySink for AuxiliaryActivity {
    fn binding(&self) -> ExecutionBinding {
        self.sink.binding()
    }
    fn accepted(&self) -> Result<ExecutionAcceptance, LivenessError> {
        let acceptance = self.sink.accepted()?;
        if acceptance == ExecutionAcceptance::Observed {
            self.parent_progress()?;
        }
        Ok(acceptance)
    }
    fn wait_for_acceptance(&self) -> ApplicationFuture<'_, Result<(), LivenessError>> {
        self.sink.wait_for_acceptance()
    }
    fn advanced(&self, source: ActivitySource, cursor: NonZeroU64) -> Result<(), LivenessError> {
        self.sink.advanced(source, cursor)?;
        self.parent_progress()
    }
    fn ended(&self) -> Result<(), LivenessError> {
        self.terminal.store(true, Ordering::SeqCst);
        self.sink.ended()?;
        self.parent_progress()
    }
}

/// Pure factory rejection proves no physical I/O was started for this binding.
pub(crate) struct NeverDispatched(pub ExecutionBinding);
impl ExecutionStopControl for NeverDispatched {
    fn binding(&self) -> ExecutionBinding {
        self.0
    }
    fn stop(
        &self,
        _: StopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async { Ok(ExecutionStopConfirmation { binding: self.0 }) })
    }
}
pub(crate) fn stopped_code(closure: Closure) -> ErrorCode {
    match closure {
        Closure::StopConfirmed(
            StopReason::StartupExpired
            | StopReason::IdleExpired
            | StopReason::BusinessDeadlineExpired
            | StopReason::PostExitDrainExpired
            | StopReason::RequestTimedOut,
        )
        | Closure::StopUnconfirmed(
            StopReason::StartupExpired
            | StopReason::IdleExpired
            | StopReason::BusinessDeadlineExpired
            | StopReason::PostExitDrainExpired
            | StopReason::RequestTimedOut,
        ) => ErrorCode::DependencyTimeout,
        _ => ErrorCode::DependencyUnavailable,
    }
}
