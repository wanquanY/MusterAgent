//! Shared, deterministic execution-liveness policy. No timers, tasks, I/O,
//! authorization renewal, retries, or public Run state are owned by this crate.
//!
//! The owner validates authenticated evidence, reduces a command, and commits
//! the returned state under the original fence/revision before acting. A
//! rejected commit discards the transition. Persistence/recovery and adapters
//! must be implemented before enabling this policy for new admissions.

#![forbid(unsafe_code)]

mod checkpoint;
mod clock;
mod control;
mod evidence;
mod journal;
mod observations;
mod operation;
mod record;
mod recovery;
mod reducer;
mod state;

pub use checkpoint::{ActivityCursor, CheckpointParts, ExecutionCheckpoint};
pub use clock::{ClockSample, LivenessHomeClock, LocalInstant};
pub use control::{ExecutionStopConfirmation, ExecutionStopControl, ExecutionStopError};
pub use evidence::{
    ActivityObservation, ExecutionAcceptance, ExecutionActivitySink, ObservationOrigin,
};
pub use journal::{
    DueExecutionScope, ExecutionAdmission, ExecutionLivenessDueIndex,
    FencedExecutionLivenessJournal, LivenessJournalError, MAX_DUE_SCOPES,
};
pub use observations::{
    ActivitySpan, ExecutionActivityChannel, ExecutionActivityFactory, ExecutionActivitySource,
    ObservationBatch, SampledObservations,
};
pub use operation::{ExecutionOperation, ExecutionStopSignal};
pub use record::{ExecutionRecord, RecordMutation, RecordTransition};
pub use recovery::{RecoveryActivityProof, RecoveryTicket};
pub use reducer::{Command, ControlStop, Event, LivenessError, Transition};
pub use state::{Action, Closure, ExecutionLiveness, Phase, StopReason};
