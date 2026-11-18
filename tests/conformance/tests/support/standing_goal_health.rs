//! Identical authority/feedback contract for both persistence implementations.
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_automation::{
    ClaimedStandingGoalEvaluation, DeferStandingGoalEvaluation, StandingGoalEvaluationFailure,
    StandingGoalProductApplication, StandingGoalProductError, StandingGoalProgressPhase,
};

pub async fn verify_evaluation_retry_feedback(
    application: &StandingGoalProductApplication,
    claim: &ClaimedStandingGoalEvaluation,
) {
    let goal = &claim.inspection.goal;
    let pursuit = claim.inspection.pursuit.as_ref().expect("pursuit");
    let run_id = pursuit.active_run_id.expect("source Run");
    let mut defer = DeferStandingGoalEvaluation {
        tenant_id: goal.tenant_id,
        pursuit_id: pursuit.pursuit_id,
        run_id,
        claim_token: claim.claim_token.clone(),
        retry_at: UtcTimestamp::from_unix_millis(30_000),
        failure: Some(StandingGoalEvaluationFailure {
            stage: "grant_judge_authority".to_owned(),
            retryable: false,
            runtime_code: Some("invalid_command".to_owned()),
            correlation_id: Some("diagnostic-test".to_owned()),
            occurred_at: UtcTimestamp::from_unix_millis(141),
        }),
    };
    application
        .defer_terminal_evaluation(defer.clone())
        .await
        .expect("persist permanent infrastructure failure");
    let first = application
        .inspect(goal.tenant_id, goal.goal_id)
        .await
        .expect("read persisted feedback");
    assert_eq!(
        first.goal, *goal,
        "infrastructure failure must not decide the user's Goal"
    );
    assert_eq!(first.progress.phase, StandingGoalProgressPhase::Evaluating);
    let retry = first
        .progress
        .evaluation_retry
        .expect("visible retry diagnostics");
    assert_eq!(retry.attempts, 1);
    assert_eq!(
        retry.failure.runtime_code.as_deref(),
        Some("invalid_command")
    );
    assert_eq!(
        retry.failure.correlation_id.as_deref(),
        Some("diagnostic-test")
    );
    defer
        .failure
        .as_mut()
        .expect("deferred evaluation contains failure metadata")
        .occurred_at = UtcTimestamp::from_unix_millis(142);
    application
        .defer_terminal_evaluation(defer.clone())
        .await
        .expect("subsequent failure");
    let current = application
        .inspect(goal.tenant_id, goal.goal_id)
        .await
        .expect("inspect repeated evaluation failure");
    let retry = current
        .progress
        .evaluation_retry
        .expect("repeated failure remains visible");
    assert_eq!(retry.attempts, 2);
    assert_eq!(retry.first_failed_at, UtcTimestamp::from_unix_millis(141));
    assert_eq!(
        retry.failure.occurred_at,
        UtcTimestamp::from_unix_millis(142)
    );

    let mut stale = defer.clone();
    stale.claim_token = "previous-worker".to_owned();
    stale.failure = None;
    assert_eq!(
        application.defer_terminal_evaluation(stale).await,
        Err(StandingGoalProductError::Conflict)
    );
    assert_eq!(
        application
            .inspect(goal.tenant_id, goal.goal_id)
            .await
            .expect("inspect feedback after rejecting stale owner")
            .progress
            .evaluation_retry,
        Some(retry)
    );
    defer.failure = None;
    application
        .defer_terminal_evaluation(defer)
        .await
        .expect("Evaluation admission clears repair feedback");
    assert!(
        application
            .inspect(goal.tenant_id, goal.goal_id)
            .await
            .expect("inspect feedback after successful admission")
            .progress
            .evaluation_retry
            .is_none()
    );
}
