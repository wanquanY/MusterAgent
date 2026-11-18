use musteragent_content_ref::Sha256;
use musteragent_identity::{UtcTimestamp, VersionedKey};
use musteragent_runtime_capability::{ToolExecutionStart, ToolStartMode};
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, InvocationExecutionTiming, LivenessPolicyRef,
};
#[test]
fn a_tool_start_cannot_change_timing_mode_or_reseal_an_activity_policy() {
    let activity = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
        policy: LivenessPolicyRef {
            key: VersionedKey::new("test.execution@1").expect("key"),
            digest: Sha256::digest(b"policy"),
        },
        business_deadline: None,
    });
    let legacy = InvocationExecutionTiming::LegacyFixed {
        deadline: UtcTimestamp::from_unix_millis(50),
    };
    let start = ToolExecutionStart {
        started_at: UtcTimestamp::from_unix_millis(1),
        timing: activity.clone(),
    };
    for mode in [
        ToolStartMode::Initial,
        ToolStartMode::ReplaySafeRecovery,
        ToolStartMode::ReconciliationProvenNotApplied,
    ] {
        assert!(start.preserves_timing(&activity, mode));
        assert!(!start.preserves_timing(&legacy, mode));
        let changed = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: LivenessPolicyRef {
                key: VersionedKey::new("test.execution@2").expect("key"),
                digest: Sha256::digest(b"new policy"),
            },
            business_deadline: None,
        });
        assert!(!start.preserves_timing(&changed, mode));
    }
    let start = ToolExecutionStart {
        started_at: UtcTimestamp::from_unix_millis(10),
        timing: InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(60),
        },
    };
    assert!(start.preserves_timing(&legacy, ToolStartMode::Initial));
    assert!(!start.preserves_timing(&legacy, ToolStartMode::ReplaySafeRecovery));
    assert!(!start.preserves_timing(&activity, ToolStartMode::Initial));
}
