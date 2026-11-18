//! One claim-return contract exercised against both original durable stores.
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_automation::{
    ClaimStandingGoalPursuits, ClaimedStandingGoalPursuit, DeferStandingGoalAdmission,
    StandingGoalInspection, StandingGoalProductApplication, StandingGoalProductError,
};

pub async fn verify(
    app: &StandingGoalProductApplication,
    claim: &ClaimedStandingGoalPursuit,
    now: UtcTimestamp,
) -> StandingGoalInspection {
    let goal = &claim.inspection.goal;
    let pursuit = claim.inspection.pursuit.as_ref().expect("claimed Pursuit");
    let retry_at = now.checked_add_millis(1).expect("verify must succeed");
    let request = DeferStandingGoalAdmission {
        tenant_id: goal.tenant_id,
        goal_id: goal.goal_id,
        pursuit_id: pursuit.pursuit_id,
        claim_token: claim.claim_token.clone(),
        retry_at,
        occurred_at: now,
    };
    let deferred = app
        .defer_admission(request.clone())
        .await
        .expect("return our current claim");
    let after = deferred.pursuit.as_ref().expect("verify must succeed");
    assert_eq!(
        after.budget_usage, pursuit.budget_usage,
        "version races do not consume Goal budget"
    );
    assert_eq!(after.pursuit_id, pursuit.pursuit_id);
    assert_eq!(after.continuation_due_at, Some(retry_at));
    let mut next = ClaimStandingGoalPursuits {
        tenant_id: goal.tenant_id,
        worker_id: "fresh-source-owner".to_owned(),
        now,
        lease_millis: 30_000,
        limit: 10,
    };
    assert!(
        app.claim_due(next.clone())
            .await
            .expect("verify must succeed")
            .is_empty(),
        "respect bounded retry delay"
    );
    next.now = retry_at;
    let claims = app
        .claim_due(next.clone())
        .await
        .expect("reclaim before original 30s lease expires");
    assert_eq!(claims.len(), 1);
    let fresh = &claims[0];
    assert_ne!(fresh.claim_token, claim.claim_token);
    assert_eq!(fresh.inspection.goal.goal_id, goal.goal_id);
    assert_eq!(
        fresh.input_refs, claim.input_refs,
        "retry retains the frozen input batch"
    );
    assert_eq!(fresh.processing_rules, claim.processing_rules);
    assert_eq!(
        fresh
            .inspection
            .pursuit
            .as_ref()
            .expect("verify must succeed")
            .budget_usage,
        pursuit.budget_usage
    );
    assert_eq!(
        app.defer_admission(request).await,
        Err(StandingGoalProductError::Conflict),
        "previous claimant cannot release the new owner's lease"
    );
    assert!(
        app.claim_due(next)
            .await
            .expect("verify must succeed")
            .is_empty(),
        "new owner's lease stays exclusive"
    );
    fresh.inspection.clone()
}
