use super::*;
use crate::GoalBudgetUsage;
use musteragent_identity::{GoalId, GoalPursuitId, TenantId};

fn pending() -> GoalPursuit {
    GoalPursuit {
        tenant_id: TenantId::generate(),
        goal_id: GoalId::generate(),
        pursuit_id: GoalPursuitId::generate(),
        revision: 4,
        status: GoalPursuitStatus::Pending,
        active_run_id: None,
        continuation_source_run_id: Some(RunId::generate()),
        wait_barrier: None,
        budget_usage: GoalBudgetUsage {
            runs: 2,
            elapsed_millis: 100,
            model_tokens: 25,
            tool_invocations: 3,
            consecutive_failures: 0,
        },
        continuation_due_at: Some(UtcTimestamp::from_unix_millis(20)),
        started_at: UtcTimestamp::from_unix_millis(0),
        updated_at: UtcTimestamp::from_unix_millis(10),
        completed_at: None,
    }
}

#[test]
fn admission_preserves_lineage_and_counts_one_run_without_mutating_the_input() {
    let original = pending();
    let source = original.clone();
    let run = RunId::generate();
    let admitted = original
        .admitted_continuation(run, UtcTimestamp::from_unix_millis(30))
        .expect("admitted");
    assert_eq!(original, source);
    assert_eq!(admitted.status, GoalPursuitStatus::Running);
    assert_eq!(admitted.active_run_id, Some(run));
    assert_eq!(
        admitted.continuation_source_run_id,
        source.continuation_source_run_id
    );
    assert_eq!(
        admitted.budget_usage,
        GoalBudgetUsage {
            runs: 3,
            elapsed_millis: 120,
            ..source.budget_usage
        }
    );
    assert_eq!(admitted.revision, source.revision + 1);
    assert!(admitted.continuation_due_at.is_none());
    assert!(
        admitted
            .admitted_continuation(run, UtcTimestamp::from_unix_millis(31))
            .is_err()
    );
}

#[test]
fn paused_terminal_waiting_and_already_running_pursuits_cannot_admit() {
    for status in [
        GoalPursuitStatus::Paused,
        GoalPursuitStatus::Cancelled,
        GoalPursuitStatus::Achieved,
        GoalPursuitStatus::Failed,
        GoalPursuitStatus::Waiting,
        GoalPursuitStatus::Running,
    ] {
        let pursuit = GoalPursuit {
            status,
            ..pending()
        };
        assert!(
            pursuit
                .admitted_continuation(RunId::generate(), UtcTimestamp::from_unix_millis(30))
                .is_err()
        );
    }
}

#[test]
fn missing_source_future_due_and_backwards_clock_fail_closed() {
    let mut pursuit = pending();
    pursuit.continuation_source_run_id = None;
    assert!(
        pursuit
            .admitted_continuation(RunId::generate(), UtcTimestamp::from_unix_millis(30))
            .is_err()
    );
    let mut pursuit = pending();
    pursuit.continuation_due_at = None;
    assert!(
        pursuit
            .admitted_continuation(RunId::generate(), UtcTimestamp::from_unix_millis(30))
            .is_err()
    );
    assert!(
        pending()
            .admitted_continuation(RunId::generate(), UtcTimestamp::from_unix_millis(15))
            .is_err()
    );
    let mut pursuit = pending();
    pursuit.continuation_due_at = Some(UtcTimestamp::from_unix_millis(0));
    assert!(
        pursuit
            .admitted_continuation(RunId::generate(), UtcTimestamp::from_unix_millis(5))
            .is_err()
    );
}

#[test]
fn revision_and_usage_overflows_cannot_partially_advance_a_pursuit() {
    for field in 0..3 {
        let mut pursuit = pending();
        match field {
            0 => pursuit.revision = u64::MAX,
            1 => pursuit.budget_usage.runs = u32::MAX,
            _ => pursuit.budget_usage.elapsed_millis = u64::MAX,
        }
        let before = pursuit.clone();
        assert!(
            pursuit
                .admitted_continuation(RunId::generate(), UtcTimestamp::from_unix_millis(30))
                .is_err()
        );
        assert_eq!(pursuit, before);
    }
}
