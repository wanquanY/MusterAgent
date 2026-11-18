//! A child process ending is completed work, not the end of a composite Tool.
//! Translate fresh child evidence into the original operation's work counter.
use super::*;

struct State {
    cursors: [u64; ActivitySource::COUNT],
    ended: bool,
}
pub(super) struct StageActivity {
    parent: OperationContext,
    state: Mutex<State>,
}
impl StageActivity {
    pub(super) fn new(parent: OperationContext) -> Arc<Self> {
        Arc::new(Self {
            parent,
            state: Mutex::new(State {
                cursors: [0; ActivitySource::COUNT],
                ended: false,
            }),
        })
    }
    fn open(&self) -> Result<std::sync::MutexGuard<'_, State>, LivenessError> {
        let state = self.state.lock().map_err(|_| LivenessError::ScopeClosed)?;
        if state.ended || self.parent.stopping() {
            return Err(LivenessError::ScopeClosed);
        }
        Ok(state)
    }
}
impl ExecutionActivitySink for StageActivity {
    fn binding(&self) -> ExecutionBinding {
        self.parent.activity.binding()
    }
    fn accepted(
        &self,
    ) -> Result<musteragent_execution_liveness::ExecutionAcceptance, LivenessError> {
        let _state = self.open()?;
        self.parent.activity.accepted()
    }
    fn wait_for_acceptance(&self) -> ApplicationFuture<'_, Result<(), LivenessError>> {
        Box::pin(async move {
            {
                let _state = self.open()?;
            }
            self.parent.activity.wait_for_acceptance().await
        })
    }
    fn advanced(&self, source: ActivitySource, cursor: NonZeroU64) -> Result<(), LivenessError> {
        let mut state = self.open()?;
        if cursor.get() <= state.cursors[source.index()] {
            return Err(LivenessError::NonAdvancingEvidence);
        }
        // Serialize both streams through this stage. A later stage may restart
        // its byte offsets while the parent's work counter remains monotonic.
        self.parent.completed_stage()?;
        state.cursors[source.index()] = cursor.get();
        Ok(())
    }
    fn ended(&self) -> Result<(), LivenessError> {
        let mut state = self.open()?;
        self.parent.completed_stage()?;
        state.ended = true;
        Ok(())
    }
}
