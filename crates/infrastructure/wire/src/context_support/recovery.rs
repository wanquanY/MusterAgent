use super::*;
fn positive(value: u32) -> Result<std::num::NonZeroU32> {
    std::num::NonZeroU32::new(value).ok_or(ContextSupportError::Invalid)
}
use musteragent_runtime_snapshot::RecoveryPolicySnapshot as Policy;
use std::time::Duration;

pub(crate) fn encode(
    policy: Policy,
) -> Result<(
    u32,
    Option<wire::PersistentRecoveryPolicyV1>,
    Option<wire::BoundedBackoffPolicyV1>,
)> {
    if !policy.is_valid() {
        return Err(ContextSupportError::Invalid);
    }
    Ok(match policy {
        Policy::Bounded { max_attempts } => (max_attempts.get(), None, None),
        Policy::BoundedBackoff { spec } => (
            u32::from(spec.max_attempts),
            None,
            Some(wire::BoundedBackoffPolicyV1 {
                max_attempts: u32::from(spec.max_attempts),
                base_backoff_millis: spec.base_backoff_millis,
                factor: u32::from(spec.factor),
                max_backoff_millis: spec.max_backoff_millis,
            }),
        ),
        Policy::Persistent {
            fast_attempts,
            min_probe_interval,
            max_probe_interval,
        } => (
            0,
            Some(wire::PersistentRecoveryPolicyV1 {
                fast_attempts: fast_attempts.get(),
                min_probe_interval_ms: min_probe_interval.as_millis() as u64,
                max_probe_interval_ms: max_probe_interval.as_millis() as u64,
            }),
            None,
        ),
    })
}
pub(crate) fn decode(
    max_attempts: u32,
    persistent: Option<wire::PersistentRecoveryPolicyV1>,
    bounded: Option<wire::BoundedBackoffPolicyV1>,
) -> Result<Policy> {
    let policy = if let Some(v) = bounded {
        if persistent.is_some() || v.max_attempts != max_attempts {
            return Err(ContextSupportError::Invalid);
        }
        Policy::BoundedBackoff {
            spec: musteragent_schedule::RetrySpec {
                max_attempts: u16::try_from(v.max_attempts)
                    .map_err(|_| ContextSupportError::Invalid)?,
                base_backoff_millis: v.base_backoff_millis,
                factor: u16::try_from(v.factor).map_err(|_| ContextSupportError::Invalid)?,
                max_backoff_millis: v.max_backoff_millis,
            },
        }
    } else {
        match persistent {
            None => Policy::Bounded {
                max_attempts: positive(max_attempts)?,
            },
            Some(v) if max_attempts == 0 => Policy::Persistent {
                fast_attempts: positive(v.fast_attempts)?,
                min_probe_interval: Duration::from_millis(v.min_probe_interval_ms),
                max_probe_interval: Duration::from_millis(v.max_probe_interval_ms),
            },
            Some(_) => return Err(ContextSupportError::Invalid),
        }
    };
    if !policy.is_valid() {
        return Err(ContextSupportError::Invalid);
    }
    Ok(policy)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persistent_recovery_keeps_timing_and_rejects_ambiguous_policy() {
        let policy = Policy::production_default();
        let (max, wire, bounded) = encode(policy).expect("production policy");
        assert_eq!(decode(max, wire, bounded).expect("decode"), policy);
        assert!(
            decode(1, wire, None).is_err(),
            "bounded and persistent policies cannot coexist"
        );
        assert!(
            decode(0, None, None).is_err(),
            "an omitted policy must not create unlimited recovery"
        );
        assert!(
            decode(
                0,
                Some(wire::PersistentRecoveryPolicyV1 {
                    fast_attempts: 1,
                    min_probe_interval_ms: 0,
                    max_probe_interval_ms: 0
                }),
                None
            )
            .is_err()
        );
    }
}
