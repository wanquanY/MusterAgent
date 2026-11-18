//! Common ownership for cooperative adapters. Timing remains in the Runtime
//! reactor. Each adapter must confirm its own physical completion explicitly.
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, ExecutionStopConfirmation, ExecutionStopControl,
    ExecutionStopError, ExecutionStopSignal, LivenessError, StopReason,
};
use musteragent_runtime_contract::execution_liveness::{ActivitySource, ExecutionBinding};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

mod stage_activity;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Prepared,
    Running,
    Ended,
    Abandoned,
}
struct State {
    phase: Phase,
    stopping: bool,
    unconfirmed_dependency: bool,
    progress: u64,
}
struct Control {
    binding: ExecutionBinding,
    state: Mutex<State>,
    changed: Notify,
}
impl Control {
    fn begin(&self) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.phase != Phase::Prepared || state.stopping {
            return false;
        }
        state.phase = Phase::Running;
        true
    }
    fn finish(&self) -> bool {
        let confirmed = if let Ok(mut state) = self.state.lock() {
            let confirmed = !state.unconfirmed_dependency;
            state.phase = if confirmed {
                Phase::Ended
            } else {
                Phase::Abandoned
            };
            confirmed
        } else {
            false
        };
        self.changed.notify_waiters();
        confirmed
    }
}
impl ExecutionStopSignal for Control {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stopping(&self) -> bool {
        self.state.lock().map_or(true, |state| state.stopping)
    }
    fn cancelled(&self) -> ApplicationFuture<'_, ()> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.stopping() {
                    return;
                }
                changed.await;
            }
        })
    }
}
impl ExecutionStopControl for Control {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop(
        &self,
        _: StopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let mut state = self.state.lock().map_err(|_| unconfirmed())?;
                    let newly_stopping = !state.stopping;
                    state.stopping = true;
                    if state.phase == Phase::Prepared {
                        state.phase = Phase::Ended;
                    }
                    if newly_stopping {
                        self.changed.notify_waiters();
                    }
                    match state.phase {
                        Phase::Ended => {
                            return Ok(ExecutionStopConfirmation {
                                binding: self.binding,
                            });
                        }
                        Phase::Abandoned => return Err(unconfirmed()),
                        _ => {}
                    }
                }
                changed.await;
            }
        })
    }
}

/// Evidence and cancellation for this operation only. Call `quiescent` after
/// the actual resource or bounded local I/O has ended, never from Future Drop.
#[derive(Clone)]
pub struct OperationContext {
    activity: Arc<dyn ExecutionActivitySink>,
    control: Arc<Control>,
}
impl OperationContext {
    pub fn activity(&self) -> Arc<dyn ExecutionActivitySink> {
        self.activity.clone()
    }
    /// Observe an owned native stage without giving that stage ownership of
    /// the enclosing operation's terminal evidence. The caller still owns the
    /// stage's process tree, stop proof and uncertain dependencies.
    pub fn stage_activity(&self) -> Arc<dyn ExecutionActivitySink> {
        stage_activity::StageActivity::new(self.clone())
    }
    pub fn stop_signal(&self) -> Arc<dyn ExecutionStopSignal> {
        self.control.clone()
    }
    pub fn stopping(&self) -> bool {
        self.control.stopping()
    }
    pub async fn cancelled(&self) {
        self.control.cancelled().await;
    }
    /// Await an owned, cancellation-safe read and report its actual completion.
    /// Only for reads whose local resources are released when the future is
    /// dropped. Never use for writes, detached jobs or uncertain external effects.
    pub async fn read_stage<T, E>(
        &self,
        read: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<Result<T, E>, ErrorCode> {
        let value = tokio::select! {
            biased;
            () = self.cancelled() => return Err(ErrorCode::RunTransitionInvalid),
            value = read => value,
        };
        if value.is_ok() {
            self.completed_stage()
                .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
        }
        Ok(value)
    }
    pub fn advanced(&self, cursor: NonZeroU64) -> Result<(), LivenessError> {
        self.activity.advanced(ActivitySource::ExecutorWork, cursor)
    }
    /// A completed native stage, including validated work by an owned child.
    /// Callers must not invoke this from polling, retries or timer ticks.
    pub fn completed_stage(&self) -> Result<(), LivenessError> {
        let cursor = {
            let mut state = self
                .control
                .state
                .lock()
                .map_err(|_| LivenessError::ScopeClosed)?;
            state.progress = state
                .progress
                .checked_add(1)
                .ok_or(LivenessError::ScopeClosed)?;
            NonZeroU64::new(state.progress).ok_or(LivenessError::ScopeClosed)?
        };
        self.advanced(cursor)
    }
    /// Retain uncertainty if a nested request is abandoned or cannot prove its
    /// stop. A later child success never clears an earlier unknown dependency.
    pub fn dependency(&self) -> OperationDependency {
        OperationDependency {
            control: self.control.clone(),
            confirmed: false,
        }
    }
    pub fn quiescent(&self) {
        if self.control.finish() {
            // A committed stop may already reject the late native end. The stop
            // proof remains available without resurrecting the stopped operation.
            let _ = self.activity.ended();
        }
    }
}

type Body<'a, T> = Box<dyn FnOnce(OperationContext) -> ApplicationFuture<'a, T> + Send + 'a>;
/// Constructs no I/O and spawns no task. The original caller polls execution
/// during cancellation and owns the common reactor's finite cleanup window.
pub struct CooperativeExecution<'a, T> {
    control: Arc<Control>,
    body: Option<Body<'a, T>>,
    rejected: fn() -> T,
}
impl<'a, T> CooperativeExecution<'a, T> {
    pub fn new(
        binding: ExecutionBinding,
        rejected: fn() -> T,
        body: impl FnOnce(OperationContext) -> ApplicationFuture<'a, T> + Send + 'a,
    ) -> Self {
        Self {
            control: Arc::new(Control {
                binding,
                state: Mutex::new(State {
                    phase: Phase::Prepared,
                    stopping: false,
                    unconfirmed_dependency: false,
                    progress: 0,
                }),
                changed: Notify::new(),
            }),
            body: Some(Box::new(body)),
            rejected,
        }
    }
}
impl<T> Drop for CooperativeExecution<'_, T> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.control.state.lock() {
            state.phase = match state.phase {
                Phase::Prepared => Phase::Ended,
                Phase::Running => Phase::Abandoned,
                other => other,
            };
        }
        self.control.changed.notify_waiters();
    }
}
impl<'a, T: Send + 'a> ExecutionOperation<'a, T> for CooperativeExecution<'a, T> {
    fn binding(&self) -> ExecutionBinding {
        self.control.binding
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.control.clone()
    }
    fn execute(
        mut self: Box<Self>,
        activity: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'a, T> {
        Box::pin(async move {
            if activity.binding() != self.control.binding || !self.control.begin() {
                return (self.rejected)();
            }
            if activity.accepted().is_err() {
                self.control.finish();
                return (self.rejected)();
            }
            // EnterWait and other local commits must see acceptance in the
            // authoritative journal, not merely queued producer evidence.
            let accepted = tokio::select! {
                biased;
                _ = self.control.cancelled() => false,
                result = activity.wait_for_acceptance() => result.is_ok(),
            };
            if !accepted {
                self.control.finish();
                return (self.rejected)();
            }
            let Some(body) = self.body.take() else {
                return (self.rejected)();
            };
            body(OperationContext {
                activity,
                control: self.control.clone(),
            })
            .await
        })
    }
}
fn unconfirmed() -> ExecutionStopError {
    ExecutionStopError {
        code: ErrorCode::DependencyUnavailable,
        rejection: None,
    }
}

/// Resource evidence for an owned nested invocation. Drop records uncertainty;
/// it never manufactures native completion or cancels an independent Agent.
pub struct OperationDependency {
    control: Arc<Control>,
    confirmed: bool,
}
impl OperationDependency {
    pub fn confirm_stopped(&mut self) {
        self.confirmed = true;
    }
}
impl Drop for OperationDependency {
    fn drop(&mut self) {
        if !self.confirmed
            && let Ok(mut state) = self.control.state.lock()
        {
            state.unconfirmed_dependency = true;
        }
    }
}
