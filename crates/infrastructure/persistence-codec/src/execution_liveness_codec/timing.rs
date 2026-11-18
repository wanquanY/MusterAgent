//! Invocation intent is distinct from an anchored, mutable Home checkpoint.
use super::{LivenessCodecError, wire};
use crate::wire_common::{decode_sha256, decode_timestamp, sha256, timestamp};
use musteragent_identity::VersionedKey;
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, BusinessDeadline, InvocationExecutionTiming, LivenessPolicyRef,
};
use prost::Message as _;

/// Closed storage representation shared by both Home database adapters.
pub struct EncodedInvocationTiming {
    pub legacy_deadline_ms: Option<i64>,
    pub activity: Option<Vec<u8>>,
    pub activity_hash: Option<Vec<u8>>,
}
pub fn encode_invocation_timing_columns(
    value: &InvocationExecutionTiming,
) -> EncodedInvocationTiming {
    match value {
        InvocationExecutionTiming::LegacyFixed { deadline } => EncodedInvocationTiming {
            legacy_deadline_ms: Some(deadline.unix_millis()),
            activity: None,
            activity_hash: None,
        },
        InvocationExecutionTiming::ActivityV1(value) => {
            let bytes = encode_activity_execution_timing(value);
            let hash = musteragent_content_ref::Sha256::digest(&bytes)
                .into_bytes()
                .to_vec();
            EncodedInvocationTiming {
                legacy_deadline_ms: None,
                activity: Some(bytes),
                activity_hash: Some(hash),
            }
        }
    }
}
pub fn decode_invocation_timing_columns(
    value: EncodedInvocationTiming,
) -> Result<InvocationExecutionTiming, LivenessCodecError> {
    match (
        value.legacy_deadline_ms,
        value.activity,
        value.activity_hash,
    ) {
        (Some(deadline), None, None) => Ok(InvocationExecutionTiming::LegacyFixed {
            deadline: musteragent_identity::UtcTimestamp::from_unix_millis(deadline),
        }),
        (None, Some(bytes), Some(hash))
            if musteragent_content_ref::Sha256::digest(&bytes)
                .into_bytes()
                .as_slice()
                == hash.as_slice() =>
        {
            Ok(InvocationExecutionTiming::ActivityV1(
                decode_activity_execution_timing(&bytes)?,
            ))
        }
        _ => Err(LivenessCodecError::InvalidContract),
    }
}

/// Compatibility columns are closed together: neither and both are rejected
/// by required invocation callers. Empty is retained only for historical start
/// facts that predate persisted execution timing.
pub fn optional_invocation_execution_timing(
    legacy: Option<prost_types::Timestamp>,
    activity: Option<wire::ActivityExecutionTimingV1>,
) -> Result<Option<InvocationExecutionTiming>, LivenessCodecError> {
    match (legacy, activity) {
        (None, None) => Ok(None),
        (Some(value), None) => Ok(Some(InvocationExecutionTiming::LegacyFixed {
            deadline: decode_timestamp(Some(value))
                .map_err(|_| LivenessCodecError::InvalidContract)?,
        })),
        (None, Some(value)) => Ok(Some(InvocationExecutionTiming::ActivityV1(
            activity_execution_timing(value)?,
        ))),
        (Some(_), Some(_)) => Err(LivenessCodecError::InvalidContract),
    }
}
pub fn invocation_activity_timing_message(
    value: &InvocationExecutionTiming,
) -> Option<wire::ActivityExecutionTimingV1> {
    match value {
        InvocationExecutionTiming::LegacyFixed { .. } => None,
        InvocationExecutionTiming::ActivityV1(value) => {
            Some(activity_execution_timing_message(value))
        }
    }
}

pub fn activity_execution_timing_message(
    value: &ActivityExecutionTiming,
) -> wire::ActivityExecutionTimingV1 {
    wire::ActivityExecutionTimingV1 {
        policy_key: value.policy.key.as_str().to_owned(),
        policy_digest: Some(sha256(value.policy.digest)),
        business_deadline: value
            .business_deadline
            .as_ref()
            .map(|value| timestamp(value.at)),
        business_deadline_source_policy_key: value
            .business_deadline
            .as_ref()
            .map(|value| value.source.as_str().to_owned()),
    }
}
pub fn activity_execution_timing(
    value: wire::ActivityExecutionTimingV1,
) -> Result<ActivityExecutionTiming, LivenessCodecError> {
    let invalid = |_| LivenessCodecError::InvalidContract;
    let business_deadline = match (
        value.business_deadline,
        value.business_deadline_source_policy_key,
    ) {
        (None, None) => None,
        (Some(at), Some(source)) => Some(BusinessDeadline {
            at: decode_timestamp(Some(at)).map_err(invalid)?,
            source: VersionedKey::new(source).map_err(|_| LivenessCodecError::InvalidContract)?,
        }),
        _ => return Err(LivenessCodecError::InvalidContract),
    };
    Ok(ActivityExecutionTiming {
        policy: LivenessPolicyRef {
            key: VersionedKey::new(value.policy_key)
                .map_err(|_| LivenessCodecError::InvalidContract)?,
            digest: decode_sha256(value.policy_digest).map_err(invalid)?,
        },
        business_deadline,
    })
}
pub fn encode_activity_execution_timing(value: &ActivityExecutionTiming) -> Vec<u8> {
    activity_execution_timing_message(value).encode_to_vec()
}
pub fn decode_activity_execution_timing(
    bytes: &[u8],
) -> Result<ActivityExecutionTiming, LivenessCodecError> {
    if bytes.len() > 4096 {
        return Err(LivenessCodecError::InvalidContract);
    }
    activity_execution_timing(
        wire::ActivityExecutionTimingV1::decode(bytes)
            .map_err(|_| LivenessCodecError::Malformed)?,
    )
}
pub fn invocation_execution_timing_message(
    value: &InvocationExecutionTiming,
) -> wire::InvocationExecutionTimingV1 {
    use wire::invocation_execution_timing_v1::Timing;
    wire::InvocationExecutionTimingV1 {
        timing: Some(match value {
            InvocationExecutionTiming::LegacyFixed { deadline } => {
                Timing::LegacyDeadline(timestamp(*deadline))
            }
            InvocationExecutionTiming::ActivityV1(value) => {
                Timing::Activity(activity_execution_timing_message(value))
            }
        }),
    }
}
pub fn invocation_execution_timing(
    value: wire::InvocationExecutionTimingV1,
) -> Result<InvocationExecutionTiming, LivenessCodecError> {
    use wire::invocation_execution_timing_v1::Timing;
    match value.timing.ok_or(LivenessCodecError::InvalidContract)? {
        Timing::LegacyDeadline(value) => Ok(InvocationExecutionTiming::LegacyFixed {
            deadline: decode_timestamp(Some(value))
                .map_err(|_| LivenessCodecError::InvalidContract)?,
        }),
        Timing::Activity(value) => Ok(InvocationExecutionTiming::ActivityV1(
            activity_execution_timing(value)?,
        )),
    }
}
