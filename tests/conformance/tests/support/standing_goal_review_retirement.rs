//! Same original store/claim contract runs against PostgreSQL and SQLite.
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_goal::{
    GoalEvaluation, GoalPursuitStatus, GoalVerdict, GoalWaitBarrier, GoalWaitKind,
    StandingGoalStatus,
};
use musteragent_identity::{GoalEvaluationId, UtcTimestamp};
use musteragent_runtime_automation::*;
use musteragent_runtime_content::{ContentStore, PutContent};

/// Historical old owner boundary. Persist via the actual evaluation lease/CAS,
/// never direct database inserts or relabelled current snapshots.
pub async fn retain_legacy_wait(
    owner: &dyn StandingGoalProductStore,
    app: &StandingGoalProductApplication,
    content: &dyn ContentStore,
    claim: &ClaimedStandingGoalEvaluation,
    evidence: Vec<ContentRef>,
    now: UtcTimestamp,
) {
    let mut goal = claim.inspection.goal.clone();
    let mut pursuit = claim
        .inspection
        .pursuit
        .clone()
        .expect("retain legacy wait must succeed");
    let expected_goal = goal.revision;
    let expected_pursuit = pursuit.revision;
    let run = pursuit
        .active_run_id
        .expect("retain legacy wait must succeed");
    let reason = content
        .put(PutContent {
            tenant_id: goal.tenant_id,
            media_type: MediaType::new("text/plain").expect("retain legacy wait must succeed"),
            bytes: b"Historical semantic completion awaits owner confirmation".to_vec(),
        })
        .await
        .expect("retain legacy wait must succeed");
    let evaluation = GoalEvaluation {
        tenant_id: goal.tenant_id,
        evaluation_id: GoalEvaluationId::generate(),
        goal_id: goal.goal_id,
        pursuit_id: pursuit.pursuit_id,
        run_id: Some(run),
        verdict: GoalVerdict::Waiting,
        evidence,
        summary_ref: reason.clone(),
        deterministic: true,
        occurred_at: now,
    };
    goal.revision += 1;
    goal.updated_at = now;
    pursuit.revision += 1;
    pursuit.updated_at = now;
    pursuit.status = GoalPursuitStatus::Waiting;
    pursuit.continuation_source_run_id = Some(run);
    pursuit.active_run_id = None;
    pursuit.continuation_due_at = None;
    pursuit.wait_barrier = Some(GoalWaitBarrier {
        kind: GoalWaitKind::CompletionConfirmation,
        correlation_key: format!(
            "standing-goal-completion:{}:{}:{}",
            goal.goal_id, pursuit.pursuit_id, run
        ),
        reason_ref: reason,
        resume_not_before: None,
        expires_at: None,
    });
    assert_eq!(
        app.evaluate_run(EvaluateStandingGoalRun {
            tenant_id: goal.tenant_id,
            goal_id: goal.goal_id,
            pursuit_id: pursuit.pursuit_id,
            run_id: run,
            claim_token: claim.claim_token.clone(),
            evaluation_id: GoalEvaluationId::generate(),
            verdict: GoalVerdict::Waiting,
            evidence: evaluation.evidence.clone(),
            summary: b"Old human completion path must reject".to_vec(),
            deterministic: true,
            completion_confirmation_required: true,
            wait_barrier: None,
            continuation_due_at: None,
            usage_delta: Default::default(),
            occurred_at: now
        })
        .await,
        Err(StandingGoalProductError::InvalidRequest)
    );
    assert_eq!(
        owner
            .commit_evaluation(
                goal,
                pursuit,
                evaluation,
                expected_goal,
                expected_pursuit,
                claim.claim_token.clone(),
                None
            )
            .await
            .expect("retain legacy wait must succeed"),
        StandingGoalMutationOutcome::Applied
    );
}
pub async fn verify_retirement(
    owner: &dyn StandingGoalProductStore,
    app: &StandingGoalProductApplication,
    content: &dyn ContentStore,
    before: StandingGoalInspection,
) {
    let goal = before.goal.clone();
    let pursuit = before
        .pursuit
        .clone()
        .expect("verify retirement must succeed");
    let prior = before
        .latest_evaluation
        .clone()
        .expect("verify retirement must succeed");
    let now = UtcTimestamp::from_unix_millis(prior.occurred_at.unix_millis() + 100);
    let reason = content
        .put(PutContent {
            tenant_id: goal.tenant_id,
            media_type: MediaType::new("text/plain").expect("verify retirement must succeed"),
            bytes: b"Completion evidence is not yet verifiable".to_vec(),
        })
        .await
        .expect("verify retirement must succeed");
    let prepared = StandingGoalReviewRetirement::prepare(
        goal.clone(),
        pursuit.clone(),
        prior.clone(),
        reason,
        now,
    )
    .expect("verify retirement must succeed");
    let mut forged = prepared.clone();
    forged.goal.status = StandingGoalStatus::Achieved;
    assert!(
        owner.retire_completion_review(forged).await.is_err(),
        "retirement cannot certify a goal"
    );
    let mut stale = prepared.clone();
    stale.previous_goal.revision += 1;
    stale.goal.revision += 1;
    assert_eq!(
        owner
            .retire_completion_review(stale)
            .await
            .expect("verify retirement must succeed"),
        StandingGoalMutationOutcome::Conflict
    );
    assert_eq!(
        app.confirm_completion(ConfirmStandingGoalCompletion {
            tenant_id: goal.tenant_id,
            goal_id: goal.goal_id,
            pursuit_id: pursuit.pursuit_id,
            principal_id: goal.owner,
            expected_goal_revision: goal.revision,
            expected_pursuit_revision: pursuit.revision,
            evaluation_id: GoalEvaluationId::generate(),
            occurred_at: now
        })
        .await,
        Err(StandingGoalProductError::InvalidRequest)
    );
    // An unprocessed/invalid first record cannot monopolize a tenant; another
    // tenant's scan must not reset this tenant's committed cursor.
    assert_eq!(
        owner
            .list_completion_reviews(goal.tenant_id, 1)
            .await
            .expect("verify retirement must succeed"),
        vec![(goal.goal_id, pursuit.pursuit_id)]
    );
    assert!(
        owner
            .list_completion_reviews(musteragent_identity::TenantId::generate(), 1)
            .await
            .expect("verify retirement must succeed")
            .is_empty()
    );
    assert!(
        owner
            .list_completion_reviews(goal.tenant_id, 1)
            .await
            .expect("verify retirement must succeed")
            .is_empty()
    );
    let page = app
        .retire_completion_review_page(goal.tenant_id, 16, now)
        .await
        .expect("verify retirement must succeed");
    assert_eq!(page.retired, 1);
    assert!(page.next_cursor.is_none());
    let once = app
        .inspect(goal.tenant_id, goal.goal_id)
        .await
        .expect("verify retirement must succeed");
    let replay = StandingGoalReviewRetirement::prepare(
        goal.clone(),
        pursuit.clone(),
        prior.clone(),
        once.latest_evaluation
            .as_ref()
            .expect("verify retirement must succeed")
            .summary_ref
            .clone(),
        once.latest_evaluation
            .as_ref()
            .expect("verify retirement must succeed")
            .occurred_at,
    )
    .expect("verify retirement must succeed");
    assert_eq!(
        owner
            .retire_completion_review(replay.clone())
            .await
            .expect("verify retirement must succeed"),
        StandingGoalMutationOutcome::Applied,
        "exact replay is idempotent"
    );
    let page = app
        .retire_completion_review_page(goal.tenant_id, 16, now)
        .await
        .expect("verify retirement must succeed");
    assert_eq!(page.retired, 0);
    assert!(page.next_cursor.is_none());
    let after = app
        .inspect(goal.tenant_id, goal.goal_id)
        .await
        .expect("verify retirement must succeed");
    assert_eq!(after.goal.status, goal.status);
    assert_eq!(after.goal.input_progress, goal.input_progress);
    assert_eq!(after.goal.revision, goal.revision + 1);
    let current = after.pursuit.expect("verify retirement must succeed");
    assert_eq!(current.status, GoalPursuitStatus::Waiting);
    assert_eq!(current.budget_usage, pursuit.budget_usage);
    assert_eq!(current.active_run_id, None);
    assert_eq!(current.continuation_due_at, None);
    assert_eq!(
        current.continuation_source_run_id,
        pursuit.continuation_source_run_id
    );
    let barrier = current
        .wait_barrier
        .expect("verify retirement must succeed");
    assert_eq!(barrier.kind, GoalWaitKind::ExternalDependency);
    assert!(
        barrier
            .correlation_key
            .starts_with("completion-evidence-required:")
    );
    let evaluation = after
        .latest_evaluation
        .expect("verify retirement must succeed");
    assert_eq!(evaluation, replay.evaluation);
    assert_eq!(evaluation.verdict, GoalVerdict::Waiting);
    assert_eq!(evaluation.evidence, prior.evidence);
    assert_ne!(evaluation.evaluation_id, prior.evaluation_id);
    // A real action approval is outside this policy transition, even with the
    // same Goal/Run and original persisted evidence.
    let mut action = pursuit.clone();
    let wait = action
        .wait_barrier
        .as_mut()
        .expect("verify retirement must succeed");
    wait.kind = GoalWaitKind::Approval;
    wait.correlation_key = musteragent_identity::InteractionId::generate().to_string();
    assert!(
        StandingGoalReviewRetirement::prepare(
            goal,
            action,
            prior,
            prepared.evaluation.summary_ref,
            now
        )
        .is_err()
    );
}

pub async fn verify_current_evidence_block(
    app: &StandingGoalProductApplication,
    claim: &ClaimedStandingGoalEvaluation,
    evidence: Vec<ContentRef>,
) {
    let before = claim.inspection.clone();
    let old = before
        .pursuit
        .as_ref()
        .expect("verify current evidence block must succeed");
    let observed = app
        .block_completion_evidence(
            claim,
            GoalEvaluationId::generate(),
            evidence.clone(),
            UtcTimestamp::from_unix_millis(150),
        )
        .await
        .expect("verify current evidence block must succeed");
    assert_eq!(observed.goal.status, before.goal.status);
    assert_eq!(observed.goal.input_progress, before.goal.input_progress);
    let pursuit = observed
        .pursuit
        .as_ref()
        .expect("verify current evidence block must succeed");
    assert_eq!(pursuit.status, GoalPursuitStatus::Waiting);
    assert!(pursuit.active_run_id.is_none());
    assert!(pursuit.continuation_due_at.is_none());
    assert_eq!(pursuit.continuation_source_run_id, old.active_run_id);
    assert_eq!(
        pursuit
            .wait_barrier
            .as_ref()
            .expect("verify current evidence block must succeed")
            .kind,
        GoalWaitKind::ExternalDependency
    );
    let evaluation = observed
        .latest_evaluation
        .expect("verify current evidence block must succeed");
    assert_eq!(evaluation.verdict, GoalVerdict::Waiting);
    assert_eq!(evaluation.evidence, evidence);
    let page = app
        .retire_completion_review_page(
            before.goal.tenant_id,
            16,
            UtcTimestamp::from_unix_millis(200),
        )
        .await
        .expect("verify current evidence block must succeed");
    assert_eq!(
        page.retired, 0,
        "real verification dependency is not retired as a human review"
    );
}
