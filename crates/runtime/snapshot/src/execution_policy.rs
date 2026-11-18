//! Admission-pinned execution and physical stream limits.
use crate::{RecoveryPolicySnapshot, SnapshotContractError};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolExecutionMode {
    Parallel,
    Sequential,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ModelStreamBudgetSnapshot {
    pub max_event_bytes: NonZeroUsize,
    pub max_events: NonZeroU32,
    pub max_total_bytes: NonZeroU64,
    pub max_tool_calls: NonZeroU32,
    pub max_tool_argument_bytes: NonZeroUsize,
    pub max_reasoning_replay_bytes: NonZeroUsize,
}

/// Maximum plaintext admitted to the versioned completion-intent codec. The
/// protected Content Store envelope has one MiB of independent ciphertext and
/// metadata headroom above this bound.
pub const MAX_DURABLE_MODEL_COMPLETION_BYTES: u64 = 255 * 1_048_576;
/// V2 codec bytes that do not scale with Tool calls or retained payload.
pub const MODEL_COMPLETION_INTENT_FIXED_BYTES: u64 = 1_327;
/// V2 codec worst-case per Tool call, excluding the canonical argument bytes
/// already charged to `max_total_bytes`. This includes maximal identifiers,
/// provider-native replay material, length prefixes and closed discriminants.
pub const MODEL_COMPLETION_INTENT_PER_TOOL_BYTES: u64 = 5_212;

impl ModelStreamBudgetSnapshot {
    /// Proves the largest exact V2 completion intent admitted by this stream
    /// budget. A Tool call consumes at least one canonical event in addition
    /// to the ResponseStarted/ResponseCompleted pair. Retained argument and
    /// reasoning bytes are jointly capped by the canonical total-byte budget.
    pub fn completion_intent_worst_case_bytes(self) -> Option<u64> {
        let event_bounded_tools = self.max_events.get().saturating_sub(2);
        let tool_calls = u64::from(self.max_tool_calls.get().min(event_bounded_tools));
        let argument_bytes =
            tool_calls.checked_mul(u64::try_from(self.max_tool_argument_bytes.get()).ok()?)?;
        let retained_payload = u64::try_from(self.max_reasoning_replay_bytes.get())
            .ok()?
            .checked_add(argument_bytes)?
            .min(self.max_total_bytes.get());
        MODEL_COMPLETION_INTENT_FIXED_BYTES
            .checked_add(tool_calls.checked_mul(MODEL_COMPLETION_INTENT_PER_TOOL_BYTES)?)?
            .checked_add(retained_payload)
    }

    pub fn validate(self) -> Result<Self, SnapshotContractError> {
        if self.max_event_bytes.get() > 16 * 1_048_576
            || self.max_events.get() > 1_000_000
            || self.max_total_bytes.get() > MAX_DURABLE_MODEL_COMPLETION_BYTES
            || self.max_tool_calls.get() > 10_000
            || self.max_tool_argument_bytes.get() > 16 * 1_048_576
            || self.max_reasoning_replay_bytes.get() > 64 * 1_048_576
            || u64::try_from(self.max_event_bytes.get())
                .map_or(true, |value| value > self.max_total_bytes.get())
            || u64::try_from(self.max_tool_argument_bytes.get())
                .map_or(true, |value| value > self.max_total_bytes.get())
            || u64::try_from(self.max_reasoning_replay_bytes.get())
                .map_or(true, |value| value > self.max_total_bytes.get())
            || self
                .completion_intent_worst_case_bytes()
                .is_none_or(|value| value > MAX_DURABLE_MODEL_COMPLETION_BYTES)
        {
            return Err(SnapshotContractError::InvalidModelStreamBudget);
        }
        Ok(self)
    }
}

/// Admission-pinned execution semantics for exactly one Run.
///
/// Infrastructure capacity remains deployment configuration. Every field that
/// can change a Run outcome, retry boundary, persisted Frame or replay result
/// belongs here and is therefore covered by the Runtime Snapshot hash.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RunExecutionPolicySnapshot {
    /// New admissions may advance an authorized provider before retrying the original.
    pub provider_failover_v1: bool,
    /// Versioned task recovery semantics, sealed with this Snapshot.
    pub recovery_policy: RecoveryPolicySnapshot,
    /// Present only in v14+, never inferred from a historical idle field.
    pub liveness: Option<crate::RunLivenessPolicyV1>,
    /// Frozen floor for private protocol tools whose profiles are role-owned.
    /// Zero preserves the exact historical 60/120s profiles; never infer this
    /// field from the current binary when recovering an older Run.
    pub private_tool_minimum_window: Duration,
    pub max_consecutive_empty_responses: NonZeroU32,
    /// Maximum time the Runtime may observe no validated canonical model
    /// event while it is ready to consume the Provider stream.
    pub model_inactivity_timeout: Duration,
    pub max_model_events_per_commit: NonZeroUsize,
    pub output_frame_flush_interval: Duration,
    pub max_output_frame_bytes: NonZeroUsize,
    pub default_tool_execution_mode: ToolExecutionMode,
    pub model_stream: ModelStreamBudgetSnapshot,
    /// Frozen depth ceiling (root depth zero). Cloud admission checks the full
    /// ancestor chain; children inherit this ceiling and cannot increase it.
    pub max_delegation_depth: NonZeroU32,
    /// Maximum number of non-terminal delegated children owned by one parent
    /// Run. Parallel admission serializes on the persistence commit lane and
    /// consumes this exact immutable quota.
    pub max_active_delegated_children: NonZeroU32,
    /// Maximum ordered handles accepted by one durable delegation join.
    pub max_delegation_group_size: NonZeroU32,
}

/// Product default for a Provider stream that produces no canonical progress.
pub const DEFAULT_MODEL_INACTIVITY_TIMEOUT_MILLIS: u64 = 300_000;
pub const DEFAULT_MODEL_INACTIVITY_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_MODEL_INACTIVITY_TIMEOUT_MILLIS);

impl RunExecutionPolicySnapshot {
    #[must_use]
    pub fn production_defaults() -> Self {
        Self {
            provider_failover_v1: true,
            recovery_policy: RecoveryPolicySnapshot::production_default(),
            liveness: Some(crate::RunLivenessPolicyV1::production_default()),
            private_tool_minimum_window:
                musteragent_runtime_contract::execution_liveness::MIN_TOOL_EXECUTION_WINDOW,
            ..Self::legacy_defaults()
        }
    }

    /// Internal jobs belong to a bounded Tool/workflow contract. Their failures
    /// return to the owning Agent instead of creating an interactive repair wait.
    #[must_use]
    pub fn bounded_job_defaults(max_attempts: NonZeroU32) -> Self {
        Self {
            recovery_policy: RecoveryPolicySnapshot::Bounded { max_attempts },
            ..Self::production_defaults()
        }
    }

    /// Frozen defaults used for snapshots predating durable recovery. Future
    /// production changes must override these, never redefine historical policy.
    #[must_use]
    pub fn legacy_defaults() -> Self {
        Self {
            provider_failover_v1: false,
            recovery_policy: RecoveryPolicySnapshot::legacy_default(),
            liveness: None,
            private_tool_minimum_window: Duration::ZERO,
            max_consecutive_empty_responses: NonZeroU32::new(3).expect("constant is non-zero"),
            model_inactivity_timeout: DEFAULT_MODEL_INACTIVITY_TIMEOUT,
            max_model_events_per_commit: NonZeroUsize::new(32).expect("constant is non-zero"),
            output_frame_flush_interval: Duration::from_millis(25),
            max_output_frame_bytes: NonZeroUsize::new(8 * 1_024).expect("constant is non-zero"),
            default_tool_execution_mode: ToolExecutionMode::Parallel,
            model_stream: ModelStreamBudgetSnapshot {
                max_event_bytes: NonZeroUsize::new(4 * 1_048_576).expect("constant is non-zero"),
                max_events: NonZeroU32::new(100_000).expect("constant is non-zero"),
                max_total_bytes: NonZeroU64::new(32 * 1_048_576).expect("constant is non-zero"),
                max_tool_calls: NonZeroU32::new(128).expect("constant is non-zero"),
                max_tool_argument_bytes: NonZeroUsize::new(1_048_576)
                    .expect("constant is non-zero"),
                max_reasoning_replay_bytes: NonZeroUsize::new(8 * 1_048_576)
                    .expect("constant is non-zero"),
            },
            max_delegation_depth: NonZeroU32::new(
                musteragent_runtime_contract::DIRECT_SUBAGENT_DEPTH,
            )
            .expect("constant is non-zero"),
            max_active_delegated_children: NonZeroU32::new(8).expect("constant is non-zero"),
            max_delegation_group_size: NonZeroU32::new(16).expect("constant is non-zero"),
        }
    }

    pub fn validate(self) -> Result<Self, SnapshotContractError> {
        if (!self.private_tool_minimum_window.is_zero()
            && (self.private_tool_minimum_window
                < musteragent_runtime_contract::execution_liveness::MIN_TOOL_EXECUTION_WINDOW
                || self.private_tool_minimum_window > Duration::from_secs(24 * 60 * 60)
                || !self
                    .private_tool_minimum_window
                    .subsec_nanos()
                    .is_multiple_of(1_000_000)))
            || !self.recovery_policy.is_valid()
            || self.max_consecutive_empty_responses.get() > 10
            || self.model_inactivity_timeout.is_zero()
            || self.model_inactivity_timeout > Duration::from_secs(60 * 60)
            || self.max_model_events_per_commit.get() > 1_000
            || !(Duration::from_millis(20)..=Duration::from_millis(50))
                .contains(&self.output_frame_flush_interval)
            || !(4 * 1_024..=16 * 1_024).contains(&self.max_output_frame_bytes.get())
            || self.max_delegation_depth.get() > musteragent_runtime_contract::MAX_DELEGATION_DEPTH
            || self.max_active_delegated_children.get() > 64
            || self.max_delegation_group_size.get() > 64
        {
            return Err(SnapshotContractError::InvalidRunExecutionPolicy);
        }
        self.model_stream.validate()?;
        self.validate_model_liveness_window()?;
        Ok(self)
    }
}

#[cfg(test)]
mod model_inactivity_timeout_tests {
    use std::time::Duration;

    use super::{DEFAULT_MODEL_INACTIVITY_TIMEOUT, RunExecutionPolicySnapshot};

    #[test]
    fn admitted_inactivity_timeout_is_not_converted_into_a_wall_clock_ceiling() {
        let mut historical = RunExecutionPolicySnapshot::legacy_defaults();
        historical.model_inactivity_timeout = Duration::from_secs(600);

        assert!(historical.validate().is_ok());
        assert_eq!(
            historical.model_inactivity_timeout,
            Duration::from_secs(600)
        );
        assert_eq!(
            RunExecutionPolicySnapshot::production_defaults().model_inactivity_timeout,
            DEFAULT_MODEL_INACTIVITY_TIMEOUT
        );
    }
}
