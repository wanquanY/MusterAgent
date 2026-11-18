//! Strict, lossless translation of sealed liveness policies. Old timeout fields
//! are never accepted as a substitute for this versioned message.
pub(crate) mod checkpoint;
mod timing;
pub use checkpoint::{
    decode_execution_liveness_record, encode_execution_liveness_record, execution_liveness_record,
    execution_liveness_record_message,
};
pub use timing::{
    EncodedInvocationTiming, activity_execution_timing, activity_execution_timing_message,
    decode_activity_execution_timing, decode_invocation_timing_columns,
    encode_activity_execution_timing, encode_invocation_timing_columns,
    invocation_activity_timing_message, invocation_execution_timing,
    invocation_execution_timing_message, optional_invocation_execution_timing,
};

use std::time::Duration;

use musteragent_identity::VersionedKey;
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, ExecutionLivenessPolicy, ExecutionLivenessWindows, LivenessPolicyError,
};
use prost::Message as _;
use thiserror::Error;

use crate::wire;
use crate::wire_common::{decode_sha256, sha256};

pub fn encode_execution_liveness_policy(value: &ExecutionLivenessPolicy) -> Vec<u8> {
    execution_liveness_policy_message(value).encode_to_vec()
}

pub fn decode_execution_liveness_policy(
    bytes: &[u8],
) -> Result<ExecutionLivenessPolicy, LivenessCodecError> {
    // Policies are tiny, and untrusted lengths must not allocate arbitrarily.
    if bytes.len() > 4096 {
        return Err(LivenessCodecError::InvalidContract);
    }
    execution_liveness_policy(
        wire::ExecutionLivenessPolicyV1::decode(bytes)
            .map_err(|_| LivenessCodecError::Malformed)?,
    )
}

pub fn execution_liveness_policy_message(
    value: &ExecutionLivenessPolicy,
) -> wire::ExecutionLivenessPolicyV1 {
    let [startup, idle, lag, checkpoint, request, cleanup, drain] = value
        .windows()
        .ordered()
        .map(|duration| u64::try_from(duration.as_millis()).expect("validated policy"));
    wire::ExecutionLivenessPolicyV1 {
        policy_key: value.reference().key.as_str().to_owned(),
        policy_digest: Some(sha256(value.reference().digest)),
        startup_window_ms: startup,
        idle_window_ms: idle,
        max_delivery_lag_ms: lag,
        checkpoint_interval_ms: checkpoint,
        observation_request_window_ms: request,
        cleanup_window_ms: cleanup,
        post_exit_drain_window_ms: drain,
        accepted_sources: ActivitySource::ALL
            .into_iter()
            .filter(|source| value.accepts(*source))
            .map(|source| activity_source(source) as i32)
            .collect(),
    }
}

pub fn execution_liveness_policy(
    value: wire::ExecutionLivenessPolicyV1,
) -> Result<ExecutionLivenessPolicy, LivenessCodecError> {
    let supplied_digest =
        decode_sha256(value.policy_digest).map_err(|_| LivenessCodecError::InvalidContract)?;
    if value.accepted_sources.len() > ActivitySource::COUNT
        || value
            .accepted_sources
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(LivenessCodecError::InvalidContract);
    }
    let sources = value
        .accepted_sources
        .into_iter()
        .map(decode_activity_source)
        .collect::<Result<Vec<_>, _>>()?;
    let policy = ExecutionLivenessPolicy::new(
        VersionedKey::new(value.policy_key).map_err(|_| LivenessCodecError::InvalidContract)?,
        ExecutionLivenessWindows {
            startup: Duration::from_millis(value.startup_window_ms),
            idle: Duration::from_millis(value.idle_window_ms),
            max_delivery_lag: Duration::from_millis(value.max_delivery_lag_ms),
            checkpoint_interval: Duration::from_millis(value.checkpoint_interval_ms),
            observation_request: Duration::from_millis(value.observation_request_window_ms),
            cleanup: Duration::from_millis(value.cleanup_window_ms),
            post_exit_drain: Duration::from_millis(value.post_exit_drain_window_ms),
        },
        sources,
    )?;
    if policy.reference().digest != supplied_digest {
        return Err(LivenessCodecError::DigestMismatch);
    }
    Ok(policy)
}

pub(crate) fn activity_source(source: ActivitySource) -> wire::ExecutionActivitySourceV1 {
    match source {
        ActivitySource::Stdout => wire::ExecutionActivitySourceV1::Stdout,
        ActivitySource::Stderr => wire::ExecutionActivitySourceV1::Stderr,
        ActivitySource::CanonicalModelEvent => wire::ExecutionActivitySourceV1::CanonicalModelEvent,
        ActivitySource::StructuredProgress => wire::ExecutionActivitySourceV1::StructuredProgress,
        ActivitySource::CommittedCheckpoint => wire::ExecutionActivitySourceV1::CommittedCheckpoint,
        ActivitySource::ExecutorWork => wire::ExecutionActivitySourceV1::ExecutorWork,
    }
}

pub(crate) fn decode_activity_source(source: i32) -> Result<ActivitySource, LivenessCodecError> {
    match wire::ExecutionActivitySourceV1::try_from(source) {
        Ok(wire::ExecutionActivitySourceV1::Stdout) => Ok(ActivitySource::Stdout),
        Ok(wire::ExecutionActivitySourceV1::Stderr) => Ok(ActivitySource::Stderr),
        Ok(wire::ExecutionActivitySourceV1::CanonicalModelEvent) => {
            Ok(ActivitySource::CanonicalModelEvent)
        }
        Ok(wire::ExecutionActivitySourceV1::StructuredProgress) => {
            Ok(ActivitySource::StructuredProgress)
        }
        Ok(wire::ExecutionActivitySourceV1::CommittedCheckpoint) => {
            Ok(ActivitySource::CommittedCheckpoint)
        }
        Ok(wire::ExecutionActivitySourceV1::ExecutorWork) => Ok(ActivitySource::ExecutorWork),
        _ => Err(LivenessCodecError::InvalidContract),
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum LivenessCodecError {
    #[error(transparent)]
    State(#[from] musteragent_execution_liveness::LivenessError),
    #[error("malformed execution-liveness protobuf")]
    Malformed,
    #[error("execution-liveness violates the closed contract")]
    InvalidContract,
    #[error("execution-liveness policy digest mismatch")]
    DigestMismatch,
    #[error(transparent)]
    Policy(#[from] LivenessPolicyError),
}
