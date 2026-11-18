use std::num::NonZeroU64;

use musteragent_runtime_contract::execution_liveness::ActivitySource;

use crate::LocalInstant;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ExecutionAcceptance {
    Observed,
    AlreadyObserved,
}

/// Capability-side observation port, bound once by Home to one admitted
/// operation. Implementations stamp receipt locally; callers supply neither
/// timestamps nor a replacement owner/operation. Only actual new work belongs
/// here. Health checks, poll success, replay and model-authored status do not.
pub trait ExecutionActivitySink: Send + Sync {
    fn binding(&self) -> musteragent_runtime_contract::execution_liveness::ExecutionBinding;
    /// Acceptance is an idempotent fact for this binding. Reattaching after a
    /// durable wait may acknowledge it again, but cannot replace its timestamp,
    /// create new activity or renew any budget. Closed/ended operations reject it.
    fn accepted(&self) -> Result<ExecutionAcceptance, crate::LivenessError>;
    /// Await Home's durable acceptance before local work can commit a wait or
    /// other state transition. This is a commit barrier, never new activity.
    fn wait_for_acceptance(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<(), crate::LivenessError>>;
    /// A validated native terminal or local process exit. EOF, dropping a future,
    /// cancellation intent and transport health do not establish this fact.
    fn ended(&self) -> Result<(), crate::LivenessError>;
    fn advanced(
        &self,
        source: ActivitySource,
        work_cursor: NonZeroU64,
    ) -> Result<(), crate::LivenessError>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ObservationOrigin {
    /// Only an authenticated, task-bound observer with a freshness guarantee
    /// may use this. Reconnected remote streams must finish fresh verification
    /// first. This enum is not itself a proof or a remote trust boundary.
    Live,
    /// Replay can reconstruct history, never renew current execution.
    Replay,
}

/// Input from a trusted adapter. The command carries its full execution binding.
/// The adapter verifies source facts/proofs; the reducer verifies the admitted
/// source, freshness and monotonic cursors. Never deserialize this from an
/// untrusted provider and treat `Live` as self-authenticating.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ActivityObservation {
    pub source: ActivitySource,
    pub sequence: NonZeroU64,
    /// Stream byte offset, canonical-event index, provider progress revision,
    /// committed checkpoint index, or executor work counter for this source.
    /// A new message sequence with an unchanged work cursor is not progress.
    pub work_cursor: NonZeroU64,
    /// The first fresh observation at Home, not the time a queued/replayed
    /// callback is eventually reduced. Queueing cannot manufacture extra time.
    pub observed_at: LocalInstant,
    pub origin: ObservationOrigin,
}
