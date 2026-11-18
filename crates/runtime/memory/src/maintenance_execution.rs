//! Policy sealed at Dream admission. Job authority expiry is independent of
//! model silence; it must never become an implicit model/Agent lifetime.
use musteragent_runtime_snapshot::{
    ModelStreamBudgetSnapshot, RunExecutionPolicySnapshot, ToolExecutionMode,
};
use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    time::Duration,
};
pub fn dream_execution_policy_defaults() -> RunExecutionPolicySnapshot {
    let mut policy = RunExecutionPolicySnapshot::bounded_job_defaults(
        NonZeroU32::new(8).expect("recovery attempts"),
    );
    policy.max_consecutive_empty_responses = NonZeroU32::new(2).expect("response failures");
    policy.max_model_events_per_commit = NonZeroUsize::new(32).expect("batch");
    policy.output_frame_flush_interval = Duration::from_millis(25);
    policy.max_output_frame_bytes = NonZeroUsize::new(8 * 1024).expect("frame");
    policy.default_tool_execution_mode = ToolExecutionMode::Sequential;
    policy.model_stream = ModelStreamBudgetSnapshot {
        max_event_bytes: NonZeroUsize::new(1_048_576).expect("event"),
        max_events: NonZeroU32::new(100_000).expect("request events"),
        max_total_bytes: NonZeroU64::new(8 * 1_048_576).expect("request bytes"),
        max_tool_calls: NonZeroU32::MIN,
        max_tool_argument_bytes: NonZeroUsize::new(1_048_576).expect("arguments"),
        max_reasoning_replay_bytes: NonZeroUsize::new(4 * 1_048_576).expect("replay"),
    };
    policy
}
