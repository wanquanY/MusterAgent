//! Versioned policy wire shape; missing recovery fields retain legacy semantics.
use super::*;

#[allow(deprecated)] // v1-v8 persisted wire slots; V9+ writers emit only defaults.
pub(crate) fn execution_policy(
    _schema: SnapshotWireSchema,
    value: &RunExecutionPolicySnapshot,
) -> wire::RunExecutionPolicySnapshot {
    wire::RunExecutionPolicySnapshot {
        bounded_backoff_v1: match value.recovery_policy {
            RecoveryPolicySnapshot::BoundedBackoff { spec } => Some(wire::BoundedBackoffPolicyV1 {
                max_attempts: u32::from(spec.max_attempts),
                base_backoff_millis: spec.base_backoff_millis,
                factor: u32::from(spec.factor),
                max_backoff_millis: spec.max_backoff_millis,
            }),
            _ => None,
        },
        provider_failover_v1: value.provider_failover_v1,
        private_tool_minimum_window_ms: duration_millis(value.private_tool_minimum_window),
        execution_liveness_v1: value.liveness.map(|policies| {
            Box::new(wire::RunExecutionLivenessV1 {
                model: Some(crate::execution_liveness_policy_message(
                    &policies.model_policy(),
                )),
                handoff: Some(crate::execution_liveness_policy_message(
                    &policies.handoff_policy(),
                )),
            })
        }),
        max_attempts: value
            .recovery_policy
            .terminal_limit()
            .map_or(0, NonZeroU32::get),
        persistent_recovery_v1: match value.recovery_policy {
            RecoveryPolicySnapshot::Bounded { .. }
            | RecoveryPolicySnapshot::BoundedBackoff { .. } => None,
            RecoveryPolicySnapshot::Persistent {
                fast_attempts,
                min_probe_interval,
                max_probe_interval,
            } => Some(wire::PersistentRecoveryPolicyV1 {
                fast_attempts: fast_attempts.get(),
                min_probe_interval_ms: duration_millis(min_probe_interval),
                max_probe_interval_ms: duration_millis(max_probe_interval),
            }),
        },
        max_consecutive_empty_responses: value.max_consecutive_empty_responses.get(),
        // Field 8 keeps its historical wire name for immutable snapshot
        // compatibility; the domain value is a canonical-event idle timeout.
        model_invocation_timeout_ms: duration_millis(value.model_inactivity_timeout),
        max_model_events_per_commit: usize_as_u64(value.max_model_events_per_commit.get()),
        output_frame_flush_interval_ms: duration_millis(value.output_frame_flush_interval),
        max_output_frame_bytes: usize_as_u64(value.max_output_frame_bytes.get()),
        default_tool_execution_mode: match value.default_tool_execution_mode {
            ToolExecutionMode::Parallel => wire::ToolExecutionMode::Parallel,
            ToolExecutionMode::Sequential => wire::ToolExecutionMode::Sequential,
        } as i32,
        model_stream: Some(wire::ModelStreamBudgetSnapshot {
            max_event_bytes: usize_as_u64(value.model_stream.max_event_bytes.get()),
            max_events: value.model_stream.max_events.get(),
            max_total_bytes: value.model_stream.max_total_bytes.get(),
            max_tool_calls: value.model_stream.max_tool_calls.get(),
            max_tool_argument_bytes: usize_as_u64(value.model_stream.max_tool_argument_bytes.get()),
            max_reasoning_replay_bytes: usize_as_u64(
                value.model_stream.max_reasoning_replay_bytes.get(),
            ),
        }),
        max_delegation_depth: value.max_delegation_depth.get(),
        max_active_delegated_children: value.max_active_delegated_children.get(),
        max_delegation_group_size: value.max_delegation_group_size.get(),
        // Deprecated v1-v8 cumulative-budget fields intentionally remain at
        // their protobuf zero defaults for V9 writes.
        ..Default::default()
    }
}

pub fn decode_execution_policy(
    value: wire::RunExecutionPolicySnapshot,
) -> Result<RunExecutionPolicySnapshot, SnapshotCodecError> {
    let stream = value.model_stream.ok_or(SnapshotCodecError::MissingField)?;
    let policy = RunExecutionPolicySnapshot {
        provider_failover_v1: value.provider_failover_v1,
        private_tool_minimum_window: Duration::from_millis(value.private_tool_minimum_window_ms),
        liveness: value
            .execution_liveness_v1
            .map(|policies| {
                let model = crate::execution_liveness_policy(
                    policies.model.ok_or(SnapshotCodecError::MissingField)?,
                )
                .map_err(|_| SnapshotCodecError::InvalidContract)?;
                let handoff = crate::execution_liveness_policy(
                    policies.handoff.ok_or(SnapshotCodecError::MissingField)?,
                )
                .map_err(|_| SnapshotCodecError::InvalidContract)?;
                musteragent_runtime_snapshot::RunLivenessPolicyV1::from_policies(&model, &handoff)
                    .map_err(|_| SnapshotCodecError::InvalidContract)
            })
            .transpose()?,
        recovery_policy: if let Some(policy) = value.bounded_backoff_v1 {
            if value.persistent_recovery_v1.is_some() || policy.max_attempts != value.max_attempts {
                return Err(SnapshotCodecError::InvalidContract);
            }
            RecoveryPolicySnapshot::BoundedBackoff {
                spec: musteragent_schedule::RetrySpec {
                    max_attempts: u16::try_from(policy.max_attempts)
                        .map_err(|_| SnapshotCodecError::InvalidContract)?,
                    base_backoff_millis: policy.base_backoff_millis,
                    factor: u16::try_from(policy.factor)
                        .map_err(|_| SnapshotCodecError::InvalidContract)?,
                    max_backoff_millis: policy.max_backoff_millis,
                },
            }
        } else {
            match value.persistent_recovery_v1 {
                None => RecoveryPolicySnapshot::Bounded {
                    max_attempts: non_zero_u32(value.max_attempts)?,
                },
                Some(policy) if value.max_attempts == 0 => RecoveryPolicySnapshot::Persistent {
                    fast_attempts: non_zero_u32(policy.fast_attempts)?,
                    min_probe_interval: Duration::from_millis(policy.min_probe_interval_ms),
                    max_probe_interval: Duration::from_millis(policy.max_probe_interval_ms),
                },
                Some(_) => return Err(SnapshotCodecError::InvalidContract),
            }
        },
        max_consecutive_empty_responses: non_zero_u32(value.max_consecutive_empty_responses)?,
        model_inactivity_timeout: Duration::from_millis(value.model_invocation_timeout_ms),
        max_model_events_per_commit: non_zero_usize(value.max_model_events_per_commit)?,
        output_frame_flush_interval: Duration::from_millis(value.output_frame_flush_interval_ms),
        max_output_frame_bytes: non_zero_usize(value.max_output_frame_bytes)?,
        default_tool_execution_mode: match wire::ToolExecutionMode::try_from(
            value.default_tool_execution_mode,
        )
        .map_err(|_| SnapshotCodecError::ClosedEnum)?
        {
            wire::ToolExecutionMode::Parallel => ToolExecutionMode::Parallel,
            wire::ToolExecutionMode::Sequential => ToolExecutionMode::Sequential,
            wire::ToolExecutionMode::Unspecified => {
                return Err(SnapshotCodecError::ClosedEnum);
            }
        },
        model_stream: ModelStreamBudgetSnapshot {
            max_event_bytes: non_zero_usize(stream.max_event_bytes)?,
            max_events: non_zero_u32(stream.max_events)?,
            max_total_bytes: non_zero(stream.max_total_bytes)?,
            max_tool_calls: non_zero_u32(stream.max_tool_calls)?,
            max_tool_argument_bytes: non_zero_usize(stream.max_tool_argument_bytes)?,
            max_reasoning_replay_bytes: non_zero_usize(stream.max_reasoning_replay_bytes)?,
        },
        // These fields were added after Snapshot v7 had already been emitted.
        // Proto zero therefore means the historical production policy, not an
        // unbounded limit. Newly encoded snapshots always carry explicit
        // values and are protected by the outer Snapshot content hash.
        max_delegation_depth: NonZeroU32::new(value.max_delegation_depth)
            .unwrap_or_else(|| NonZeroU32::new(3).expect("constant is non-zero")),
        max_active_delegated_children: NonZeroU32::new(value.max_active_delegated_children)
            .unwrap_or_else(|| NonZeroU32::new(3).expect("constant is non-zero")),
        max_delegation_group_size: NonZeroU32::new(value.max_delegation_group_size)
            .unwrap_or_else(|| NonZeroU32::new(16).expect("constant is non-zero")),
    };
    policy
        .validate()
        .map_err(|_| SnapshotCodecError::InvalidContract)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_tool_profiles_round_trip_without_reinterpreting_historical_runs() {
        use musteragent_runtime_snapshot::PrivateToolPolicy;
        let legacy_profile = PrivateToolPolicy::evaluation("runtime.evaluation.decide@1").unwrap();
        let mut historical = RunExecutionPolicySnapshot::production_defaults();
        historical.private_tool_minimum_window = Duration::ZERO;
        let old_wire = execution_policy(SnapshotWireSchema::Current, &historical);
        assert_eq!(old_wire.private_tool_minimum_window_ms, 0);
        let restored = decode_execution_policy(old_wire).unwrap();
        let old = PrivateToolPolicy::evaluation("runtime.evaluation.decide@1")
            .unwrap()
            .with_minimum_window(restored.private_tool_minimum_window);
        assert_eq!(
            old.liveness.reference(),
            legacy_profile.liveness.reference()
        );
        assert_eq!(old.liveness.windows().idle, Duration::from_secs(60));
        let admitted = RunExecutionPolicySnapshot::production_defaults();
        let new_wire = execution_policy(SnapshotWireSchema::Current, &admitted);
        assert_eq!(new_wire.private_tool_minimum_window_ms, 60_000);
        let restored = decode_execution_policy(new_wire).unwrap();
        let current = PrivateToolPolicy::evaluation("runtime.evaluation.decide@1")
            .unwrap()
            .with_minimum_window(restored.private_tool_minimum_window);
        assert_eq!(current.liveness.windows().idle, Duration::from_secs(60));
        assert_eq!(current.liveness.reference(), old.liveness.reference());
        // Lowering the admission default must not shorten a previous 180s Run
        // or a capability-specific longer window when it is recovered.
        for seconds in [120, 180, 600] {
            let mut previous = admitted;
            previous.private_tool_minimum_window = Duration::from_secs(seconds);
            let pinned = PrivateToolPolicy::evaluation("runtime.evaluation.decide@1")
                .unwrap()
                .with_minimum_window(previous.private_tool_minimum_window);
            let restored =
                decode_execution_policy(execution_policy(SnapshotWireSchema::Current, &previous))
                    .unwrap();
            let recovered = PrivateToolPolicy::evaluation("runtime.evaluation.decide@1")
                .unwrap()
                .with_minimum_window(restored.private_tool_minimum_window);
            assert_eq!(recovered.liveness.reference(), pinned.liveness.reference());
            assert_eq!(
                recovered.liveness.windows().idle,
                Duration::from_secs(seconds)
            );
        }
        let mut invalid = execution_policy(SnapshotWireSchema::Current, &admitted);
        invalid.private_tool_minimum_window_ms = 5_000;
        assert!(decode_execution_policy(invalid).is_err());
    }

    #[test]
    fn bounded_backoff_is_lossless_and_cannot_coexist_with_persistent_recovery() {
        let mut policy = RunExecutionPolicySnapshot::production_defaults();
        policy.recovery_policy = RecoveryPolicySnapshot::BoundedBackoff {
            spec: musteragent_schedule::RetrySpec {
                max_attempts: 5,
                base_backoff_millis: 300,
                factor: 3,
                max_backoff_millis: 5000,
            },
        };
        let encoded = execution_policy(SnapshotWireSchema::Current, &policy);
        assert_eq!(decode_execution_policy(encoded.clone()).unwrap(), policy);
        let mut conflicting = encoded.clone();
        conflicting.persistent_recovery_v1 = Some(wire::PersistentRecoveryPolicyV1 {
            fast_attempts: 1,
            min_probe_interval_ms: 60_000,
            max_probe_interval_ms: 60_000,
        });
        assert!(decode_execution_policy(conflicting).is_err());
        let mut missing_budget = encoded;
        missing_budget.max_attempts = 0;
        assert!(decode_execution_policy(missing_budget).is_err());
    }

    #[test]
    fn persistent_and_legacy_wire_policies_have_distinct_unambiguous_contracts() {
        let legacy = RunExecutionPolicySnapshot::legacy_defaults();
        let wire_legacy = execution_policy(SnapshotWireSchema::Current, &legacy);
        assert_eq!(wire_legacy.max_attempts, 8);
        assert!(wire_legacy.persistent_recovery_v1.is_none());
        assert_eq!(
            decode_execution_policy(wire_legacy).expect("legacy policy"),
            legacy
        );
        let persistent = RunExecutionPolicySnapshot::production_defaults();
        let wire_persistent = execution_policy(SnapshotWireSchema::Current, &persistent);
        assert_eq!(wire_persistent.max_attempts, 0);
        assert_eq!(
            decode_execution_policy(wire_persistent.clone()).expect("persistent policy"),
            persistent
        );
        let mut ambiguous = wire_persistent.clone();
        ambiguous.max_attempts = 8;
        assert!(decode_execution_policy(ambiguous).is_err());
        let mut missing = wire_persistent;
        missing.persistent_recovery_v1 = None;
        assert!(decode_execution_policy(missing).is_err());
    }
    #[test]
    fn phase_policy_wire_round_trip_keeps_sealed_identity_and_rejects_missing_half() {
        use musteragent_runtime_contract::execution_liveness::ExecutionLivenessWindows;
        use musteragent_runtime_snapshot::RunLivenessPolicyV1;
        let windows = ExecutionLivenessWindows {
            startup: Duration::from_secs(20),
            idle: Duration::from_secs(300),
            max_delivery_lag: Duration::from_secs(2),
            checkpoint_interval: Duration::from_secs(1),
            observation_request: Duration::from_secs(5),
            cleanup: Duration::from_secs(5),
            post_exit_drain: Duration::from_secs(1),
        };
        let mut policy = RunExecutionPolicySnapshot::production_defaults();
        policy.liveness = Some(RunLivenessPolicyV1::new(windows, windows).expect("policies"));
        let wire = execution_policy(SnapshotWireSchema::Current, &policy);
        assert_eq!(
            decode_execution_policy(wire.clone()).expect("lossless"),
            policy
        );
        let mut half = wire.clone();
        half.execution_liveness_v1.as_mut().expect("v1").handoff = None;
        assert!(decode_execution_policy(half).is_err());
        let mut tampered = wire;
        tampered
            .execution_liveness_v1
            .as_mut()
            .expect("v1")
            .model
            .as_mut()
            .expect("model")
            .idle_window_ms += 1;
        assert!(decode_execution_policy(tampered).is_err());
    }
}
