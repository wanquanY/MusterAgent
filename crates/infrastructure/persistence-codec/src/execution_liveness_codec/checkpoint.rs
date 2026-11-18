//! Portable liveness records, independent of public Run/Tool result enums.
use musteragent_execution_liveness::{
    ActivityCursor, CheckpointParts, ClockSample, Closure, ExecutionCheckpoint, ExecutionRecord,
    LocalInstant, Phase, RecoveryTicket, StopReason,
};
use musteragent_identity::{FencingToken, RunSeq, VersionedKey};
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, BusinessDeadline, DurableWaitBinding, ExecutionBinding, ExecutionScope,
};
use prost::Message as _;
use std::{num::NonZeroU64, sync::Arc, time::Duration};

use super::{
    LivenessCodecError, activity_source, decode_activity_source, execution_liveness_policy,
    execution_liveness_policy_message,
};
use crate::{
    wire,
    wire_common::{decode_sha256, decode_timestamp, parse_id, sha256, timestamp},
};

pub fn encode_execution_liveness_record(value: &ExecutionRecord) -> Vec<u8> {
    execution_liveness_record_message(value).encode_to_vec()
}

pub fn execution_liveness_record_message(
    value: &ExecutionRecord,
) -> wire::ExecutionLivenessRecordV1 {
    use wire::execution_liveness_record_v1::State;
    let state = match value {
        ExecutionRecord::Checkpoint(value) => {
            State::Checkpoint(Box::new(checkpoint_message(value)))
        }
        ExecutionRecord::Recovering(value) => {
            State::Recovering(Box::new(wire::ExecutionRecoveryTicketV1 {
                original: Some(checkpoint_message(value.checkpoint())),
                rebound: Some(binding_message(value.binding())),
                nonce: Some(sha256(value.nonce())),
                started_at: Some(sample_message(value.started_at())),
            }))
        }
    };
    wire::ExecutionLivenessRecordV1 { state: Some(state) }
}

pub fn decode_execution_liveness_record(
    bytes: &[u8],
) -> Result<ExecutionRecord, LivenessCodecError> {
    if bytes.len() > 32 * 1024 {
        return Err(LivenessCodecError::InvalidContract);
    }
    let record = wire::ExecutionLivenessRecordV1::decode(bytes)
        .map_err(|_| LivenessCodecError::Malformed)?;
    execution_liveness_record(record)
}

pub fn execution_liveness_record(
    record: wire::ExecutionLivenessRecordV1,
) -> Result<ExecutionRecord, LivenessCodecError> {
    use wire::execution_liveness_record_v1::State;
    if record.encoded_len() > 32 * 1024 {
        return Err(LivenessCodecError::InvalidContract);
    }
    match required(record.state)? {
        State::Checkpoint(value) => checkpoint(*value).map(ExecutionRecord::from),
        State::Recovering(value) => Ok(ExecutionRecord::from(RecoveryTicket::begin(
            checkpoint(required(value.original)?)?,
            binding(required(value.rebound)?)?,
            decode_sha256(value.nonce).map_err(invalid)?,
            sample(required(value.started_at)?)?,
        )?)),
    }
}

fn checkpoint_message(value: &ExecutionCheckpoint) -> wire::ExecutionCheckpointV1 {
    use wire::execution_checkpoint_v1::Phase as WirePhase;
    let value = value.parts();
    let phase = match value.phase {
        Phase::Starting { due } => WirePhase::StartingDue(point_message(due)),
        Phase::Running { due } => WirePhase::RunningDue(point_message(due)),
        Phase::Draining { ended_at, due } => WirePhase::Draining(wire::ExecutionDrainingV1 {
            ended_at: Some(point_message(ended_at)),
            due: Some(point_message(due)),
        }),
        Phase::Waiting {
            binding,
            idle_remaining,
        } => WirePhase::Waiting(wire::ExecutionWaitV1 {
            checkpoint_run_sequence: binding.checkpoint_sequence.get(),
            wait_identity: Some(sha256(binding.identity)),
            idle_remaining_seconds: idle_remaining.as_secs(),
            idle_remaining_nanos: idle_remaining.subsec_nanos(),
        }),
        Phase::Stopping {
            reason,
            cleanup_due,
        } => WirePhase::Stopping(wire::ExecutionStoppingV1 {
            reason: stop_reason(reason) as i32,
            cleanup_due: Some(point_message(cleanup_due)),
        }),
        Phase::Closed(value) => WirePhase::Closed(closure_message(value)),
    };
    wire::ExecutionCheckpointV1 {
        binding: Some(binding_message(value.binding)),
        policy: Some(execution_liveness_policy_message(&value.policy)),
        business: value
            .business
            .map(|(contract, due)| wire::ExecutionBusinessDeadlineV1 {
                at: Some(timestamp(contract.at)),
                source_policy_key: contract.source.as_str().to_owned(),
                anchored_due: Some(point_message(due)),
            }),
        revision: value.revision,
        phase: Some(phase),
        dispatched_at: Some(point_message(value.dispatched_at)),
        accepted_at: value.accepted_at.map(point_message),
        active_since: value.active_since.map(point_message),
        last_wait_sequence: value.last_wait_sequence.map(RunSeq::get),
        last_reduced_at: Some(point_message(value.last_reduced_at)),
        last_activity_at: value.last_activity_at.map(point_message),
        cursors: ActivitySource::ALL
            .into_iter()
            .filter_map(|source| {
                let cursor = value.cursors[source.index()];
                (cursor.sequence != 0).then(|| wire::ExecutionActivityCursorV1 {
                    source: activity_source(source) as i32,
                    sequence: cursor.sequence,
                    work_cursor: cursor.work,
                })
            })
            .collect(),
        saved_at: Some(sample_message(value.saved_at)),
    }
}

fn checkpoint(
    value: wire::ExecutionCheckpointV1,
) -> Result<ExecutionCheckpoint, LivenessCodecError> {
    use wire::execution_checkpoint_v1::Phase as WirePhase;
    let phase = match required(value.phase)? {
        WirePhase::StartingDue(value) => Phase::Starting { due: point(value)? },
        WirePhase::RunningDue(value) => Phase::Running { due: point(value)? },
        WirePhase::Draining(value) => Phase::Draining {
            ended_at: point(required(value.ended_at)?)?,
            due: point(required(value.due)?)?,
        },
        WirePhase::Waiting(value) => Phase::Waiting {
            binding: DurableWaitBinding {
                checkpoint_sequence: RunSeq::new(value.checkpoint_run_sequence).map_err(invalid)?,
                identity: decode_sha256(value.wait_identity).map_err(invalid)?,
            },
            idle_remaining: duration(value.idle_remaining_seconds, value.idle_remaining_nanos)?,
        },
        WirePhase::Stopping(value) => Phase::Stopping {
            reason: decode_stop_reason(value.reason)?,
            cleanup_due: point(required(value.cleanup_due)?)?,
        },
        WirePhase::Closed(value) => Phase::Closed(closure(value)?),
    };
    if value.cursors.len() > ActivitySource::COUNT {
        return Err(LivenessCodecError::InvalidContract);
    }
    let mut cursors = [ActivityCursor::default(); ActivitySource::COUNT];
    let mut prior_source = None;
    for cursor in value.cursors {
        let source = decode_activity_source(cursor.source)?;
        if prior_source.is_some_and(|prior| prior >= source.index())
            || cursor.sequence == 0
            || cursor.work_cursor == 0
        {
            return Err(LivenessCodecError::InvalidContract);
        }
        prior_source = Some(source.index());
        cursors[source.index()] = ActivityCursor {
            sequence: cursor.sequence,
            work: cursor.work_cursor,
        };
    }
    let business = value
        .business
        .map(|value| -> Result<_, LivenessCodecError> {
            Ok((
                BusinessDeadline {
                    at: decode_timestamp(value.at).map_err(invalid)?,
                    source: VersionedKey::new(value.source_policy_key).map_err(invalid)?,
                },
                point(required(value.anchored_due)?)?,
            ))
        })
        .transpose()?;
    Ok(ExecutionCheckpoint::from_parts(CheckpointParts {
        binding: binding(required(value.binding)?)?,
        policy: Arc::new(execution_liveness_policy(required(value.policy)?)?),
        business,
        revision: value.revision,
        phase,
        dispatched_at: point(required(value.dispatched_at)?)?,
        accepted_at: value.accepted_at.map(point).transpose()?,
        active_since: value.active_since.map(point).transpose()?,
        last_wait_sequence: value
            .last_wait_sequence
            .map(RunSeq::new)
            .transpose()
            .map_err(invalid)?,
        last_reduced_at: point(required(value.last_reduced_at)?)?,
        last_activity_at: value.last_activity_at.map(point).transpose()?,
        cursors,
        saved_at: sample(required(value.saved_at)?)?,
    })?)
}

pub(crate) fn binding_message(value: ExecutionBinding) -> wire::ExecutionBindingV1 {
    use wire::execution_binding_v1::Scope;
    wire::ExecutionBindingV1 {
        tenant_id: Some(wire::TenantId {
            value: value.tenant_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: value.run_id.to_string(),
        }),
        scope: Some(match value.scope {
            ExecutionScope::Invocation(id) => Scope::InvocationId(wire::InvocationId {
                value: id.to_string(),
            }),
            ExecutionScope::RunHandoff {
                previous_invocation,
            } => Scope::HandoffAfterInvocationId(wire::InvocationId {
                value: previous_invocation.to_string(),
            }),
        }),
        operation_identity: Some(sha256(value.operation_identity)),
        owner_attempt_id: Some(wire::AttemptId {
            value: value.owner_attempt_id.to_string(),
        }),
        owner_fence: value.owner_fence.get(),
        producer_generation: value.producer_generation.get(),
    }
}

pub(crate) fn binding(
    value: wire::ExecutionBindingV1,
) -> Result<ExecutionBinding, LivenessCodecError> {
    use wire::execution_binding_v1::Scope;
    Ok(ExecutionBinding {
        tenant_id: parse_id(value.tenant_id.map(|id| id.value)).map_err(invalid)?,
        run_id: parse_id(value.run_id.map(|id| id.value)).map_err(invalid)?,
        scope: match required(value.scope)? {
            Scope::InvocationId(id) => {
                ExecutionScope::Invocation(parse_id(Some(id.value)).map_err(invalid)?)
            }
            Scope::HandoffAfterInvocationId(id) => ExecutionScope::RunHandoff {
                previous_invocation: parse_id(Some(id.value)).map_err(invalid)?,
            },
        },
        operation_identity: decode_sha256(value.operation_identity).map_err(invalid)?,
        owner_attempt_id: parse_id(value.owner_attempt_id.map(|id| id.value)).map_err(invalid)?,
        owner_fence: FencingToken::new(value.owner_fence).map_err(invalid)?,
        producer_generation: required(NonZeroU64::new(value.producer_generation))?,
    })
}

fn point_message(value: LocalInstant) -> wire::ExecutionClockPointV1 {
    wire::ExecutionClockPointV1 {
        home_clock_epoch: value.epoch.to_string(),
        elapsed_seconds: value.elapsed.as_secs(),
        elapsed_nanos: value.elapsed.subsec_nanos(),
    }
}
fn point(value: wire::ExecutionClockPointV1) -> Result<LocalInstant, LivenessCodecError> {
    Ok(LocalInstant {
        epoch: value.home_clock_epoch.parse().map_err(invalid)?,
        elapsed: duration(value.elapsed_seconds, value.elapsed_nanos)?,
    })
}
pub(crate) fn sample_message(value: ClockSample) -> wire::ExecutionClockSampleV1 {
    wire::ExecutionClockSampleV1 {
        monotonic: Some(point_message(value.monotonic)),
        utc: Some(timestamp(value.utc)),
    }
}
pub(crate) fn sample(
    value: wire::ExecutionClockSampleV1,
) -> Result<ClockSample, LivenessCodecError> {
    Ok(ClockSample {
        monotonic: point(required(value.monotonic)?)?,
        utc: decode_timestamp(value.utc).map_err(invalid)?,
    })
}
fn duration(seconds: u64, nanos: u32) -> Result<Duration, LivenessCodecError> {
    if nanos >= 1_000_000_000 {
        return Err(LivenessCodecError::InvalidContract);
    }
    Ok(Duration::new(seconds, nanos))
}
fn required<T>(value: Option<T>) -> Result<T, LivenessCodecError> {
    value.ok_or(LivenessCodecError::InvalidContract)
}
fn invalid<T>(_: T) -> LivenessCodecError {
    LivenessCodecError::InvalidContract
}

fn closure_message(value: Closure) -> wire::ExecutionClosureV1 {
    use wire::execution_closure_v1::Outcome;
    wire::ExecutionClosureV1 {
        outcome: Some(match value {
            Closure::TerminalObserved => Outcome::TerminalObserved(()),
            Closure::StopConfirmed(reason) => Outcome::StopConfirmed(stop_reason(reason) as i32),
            Closure::StopUnconfirmed(reason) => {
                Outcome::StopUnconfirmed(stop_reason(reason) as i32)
            }
        }),
    }
}
fn closure(value: wire::ExecutionClosureV1) -> Result<Closure, LivenessCodecError> {
    use wire::execution_closure_v1::Outcome;
    Ok(match required(value.outcome)? {
        Outcome::TerminalObserved(()) => Closure::TerminalObserved,
        Outcome::StopConfirmed(reason) => Closure::StopConfirmed(decode_stop_reason(reason)?),
        Outcome::StopUnconfirmed(reason) => Closure::StopUnconfirmed(decode_stop_reason(reason)?),
    })
}
pub(crate) fn stop_reason(value: StopReason) -> wire::ExecutionStopReasonV1 {
    match value {
        StopReason::StartupExpired => wire::ExecutionStopReasonV1::StartupExpired,
        StopReason::IdleExpired => wire::ExecutionStopReasonV1::IdleExpired,
        StopReason::BusinessDeadlineExpired => wire::ExecutionStopReasonV1::BusinessDeadlineExpired,
        StopReason::UserCancelled => wire::ExecutionStopReasonV1::UserCancelled,
        StopReason::ParentCancelled => wire::ExecutionStopReasonV1::ParentCancelled,
        StopReason::OwnershipLost => wire::ExecutionStopReasonV1::OwnershipLost,
        StopReason::AuthorizationLost => wire::ExecutionStopReasonV1::AuthorizationLost,
        StopReason::SystemShutdown => wire::ExecutionStopReasonV1::SystemShutdown,
        StopReason::RecoveryUnverified => wire::ExecutionStopReasonV1::RecoveryUnverified,
        StopReason::ExecutionFailed => wire::ExecutionStopReasonV1::ExecutionFailed,
        StopReason::RequestTimedOut => wire::ExecutionStopReasonV1::RequestTimedOut,
        StopReason::CoordinationRequested => wire::ExecutionStopReasonV1::CoordinationRequested,
        StopReason::PostExitDrainExpired => wire::ExecutionStopReasonV1::PostExitDrainExpired,
    }
}
pub(crate) fn decode_stop_reason(value: i32) -> Result<StopReason, LivenessCodecError> {
    match wire::ExecutionStopReasonV1::try_from(value) {
        Ok(wire::ExecutionStopReasonV1::StartupExpired) => Ok(StopReason::StartupExpired),
        Ok(wire::ExecutionStopReasonV1::IdleExpired) => Ok(StopReason::IdleExpired),
        Ok(wire::ExecutionStopReasonV1::BusinessDeadlineExpired) => {
            Ok(StopReason::BusinessDeadlineExpired)
        }
        Ok(wire::ExecutionStopReasonV1::UserCancelled) => Ok(StopReason::UserCancelled),
        Ok(wire::ExecutionStopReasonV1::ParentCancelled) => Ok(StopReason::ParentCancelled),
        Ok(wire::ExecutionStopReasonV1::OwnershipLost) => Ok(StopReason::OwnershipLost),
        Ok(wire::ExecutionStopReasonV1::AuthorizationLost) => Ok(StopReason::AuthorizationLost),
        Ok(wire::ExecutionStopReasonV1::SystemShutdown) => Ok(StopReason::SystemShutdown),
        Ok(wire::ExecutionStopReasonV1::RecoveryUnverified) => Ok(StopReason::RecoveryUnverified),
        Ok(wire::ExecutionStopReasonV1::ExecutionFailed) => Ok(StopReason::ExecutionFailed),
        Ok(wire::ExecutionStopReasonV1::RequestTimedOut) => Ok(StopReason::RequestTimedOut),
        Ok(wire::ExecutionStopReasonV1::PostExitDrainExpired) => {
            Ok(StopReason::PostExitDrainExpired)
        }
        Ok(wire::ExecutionStopReasonV1::CoordinationRequested) => {
            Ok(StopReason::CoordinationRequested)
        }
        _ => Err(LivenessCodecError::InvalidContract),
    }
}
