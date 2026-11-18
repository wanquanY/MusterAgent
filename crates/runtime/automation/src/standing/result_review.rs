//! Policy retirement belongs to the Goal owner. It preserves the original
//! evaluation and does not certify evidence, settle input or schedule effects.
use super::*;
const POLICY: &[u8] = b"standing-goal.result-review-retired@1";
const REASON: &[u8]=b"Completion evidence cannot yet be verified. A verifiable deterministic receipt is required; no human confirmation or automatic action has been assumed.";
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalReviewRetirement {
    pub previous_goal: StandingGoal,
    pub previous_pursuit: GoalPursuit,
    pub prior_evaluation: GoalEvaluation,
    pub goal: StandingGoal,
    pub pursuit: GoalPursuit,
    pub evaluation: GoalEvaluation,
}
impl StandingGoalReviewRetirement {
    pub fn prepare(
        goal: StandingGoal,
        pursuit: GoalPursuit,
        prior: GoalEvaluation,
        reason: ContentRef,
        now: UtcTimestamp,
    ) -> Result<Self, StandingGoalProductError> {
        let run = prior.run_id.ok_or(StandingGoalProductError::Corrupt)?;
        let barrier = pursuit
            .wait_barrier
            .as_ref()
            .ok_or(StandingGoalProductError::Conflict)?;
        if pursuit.tenant_id != goal.tenant_id
            || pursuit.goal_id != goal.goal_id
            || goal.active_pursuit_id != Some(pursuit.pursuit_id)
            || goal.status.terminal()
            || pursuit.status != GoalPursuitStatus::Waiting
            || pursuit.active_run_id.is_some()
            || pursuit.continuation_source_run_id != Some(run)
            || prior.verdict != GoalVerdict::Waiting
            || prior.tenant_id != goal.tenant_id
            || prior.goal_id != goal.goal_id
            || prior.pursuit_id != pursuit.pursuit_id
            || barrier.kind != GoalWaitKind::CompletionConfirmation
            || barrier.correlation_key
                != completion_confirmation_correlation(goal.goal_id, pursuit.pursuit_id, run)
            || now <= prior.occurred_at
            || now < pursuit.updated_at
            || now < goal.updated_at
        {
            return Err(StandingGoalProductError::Conflict);
        }
        let mut next_goal = goal.clone();
        let mut next_pursuit = pursuit.clone();
        next_goal.revision = next_revision(goal.revision)?;
        next_goal.updated_at = now;
        next_pursuit.revision = next_revision(pursuit.revision)?;
        next_pursuit.updated_at = now;
        next_pursuit.wait_barrier = Some(evidence_barrier(run, reason.clone()));
        let evaluation = GoalEvaluation {
            tenant_id: goal.tenant_id,
            evaluation_id: GoalEvaluationId::from_uuid(uuid::Uuid::new_v5(
                &prior.evaluation_id.into_uuid(),
                POLICY,
            ))
            .map_err(|_| StandingGoalProductError::Corrupt)?,
            goal_id: goal.goal_id,
            pursuit_id: pursuit.pursuit_id,
            run_id: Some(run),
            verdict: GoalVerdict::Waiting,
            evidence: prior.evidence.clone(),
            summary_ref: reason,
            deterministic: true,
            occurred_at: now,
        };
        next_goal.validate()?;
        next_pursuit.validate()?;
        Ok(Self {
            previous_goal: goal,
            previous_pursuit: pursuit,
            prior_evaluation: prior,
            goal: next_goal,
            pursuit: next_pursuit,
            evaluation,
        })
    }
    pub fn validate(&self) -> Result<(), StandingGoalProductError> {
        if *self
            != Self::prepare(
                self.previous_goal.clone(),
                self.previous_pursuit.clone(),
                self.prior_evaluation.clone(),
                self.evaluation.summary_ref.clone(),
                self.evaluation.occurred_at,
            )?
        {
            return Err(StandingGoalProductError::Corrupt);
        }
        Ok(())
    }
}
pub struct StandingGoalReviewRetirementPage {
    pub retired: u16,
    pub next_cursor: Option<GoalPursuitId>,
}
fn evidence_barrier(run: RunId, reason: ContentRef) -> GoalWaitBarrier {
    GoalWaitBarrier {
        kind: GoalWaitKind::ExternalDependency,
        correlation_key: format!("completion-evidence-required:{run}"),
        reason_ref: reason,
        resume_not_before: None,
        expires_at: None,
    }
}
impl StandingGoalProductApplication {
    pub async fn block_completion_evidence(
        &self,
        claim: &ClaimedStandingGoalEvaluation,
        evaluation_id: GoalEvaluationId,
        evidence: Vec<ContentRef>,
        now: UtcTimestamp,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let pursuit = claim
            .inspection
            .pursuit
            .as_ref()
            .ok_or(StandingGoalProductError::Corrupt)?;
        let run = pursuit
            .active_run_id
            .ok_or(StandingGoalProductError::Corrupt)?;
        let tenant = claim.inspection.goal.tenant_id;
        let reason = self.put_text(tenant, REASON).await?;
        self.evaluate_run(EvaluateStandingGoalRun {
            tenant_id: tenant,
            goal_id: claim.inspection.goal.goal_id,
            pursuit_id: pursuit.pursuit_id,
            run_id: run,
            claim_token: claim.claim_token.clone(),
            evaluation_id,
            verdict: GoalVerdict::Waiting,
            evidence,
            summary: REASON.to_vec(),
            deterministic: true,
            completion_confirmation_required: false,
            wait_barrier: Some(evidence_barrier(run, reason)),
            continuation_due_at: None,
            usage_delta: claim.run.usage,
            occurred_at: now,
        })
        .await
    }
    pub async fn retire_completion_review_page(
        &self,
        tenant: TenantId,
        limit: u16,
        now: UtcTimestamp,
    ) -> Result<StandingGoalReviewRetirementPage, StandingGoalProductError> {
        if limit == 0 || limit > 64 {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        let candidates = self.store.list_completion_reviews(tenant, limit).await?;
        let next_cursor = (candidates.len() == usize::from(limit))
            .then(|| candidates.last().map(|v| v.1))
            .flatten();
        let mut retired = 0u16;
        for (goal_id, pursuit_id) in candidates {
            let result = self
                .retire_completion_review(tenant, goal_id, pursuit_id, now)
                .await;
            match result {
                Ok(()) => retired += 1,
                Err(StandingGoalProductError::Conflict | StandingGoalProductError::NotFound) => (),
                Err(error) => {
                    tracing::warn!(%goal_id,%error,"Goal result review retirement deferred; original evidence retained")
                }
            }
        }
        Ok(StandingGoalReviewRetirementPage {
            retired,
            next_cursor,
        })
    }
    async fn retire_completion_review(
        &self,
        tenant: TenantId,
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        now: UtcTimestamp,
    ) -> Result<(), StandingGoalProductError> {
        let (goal, pursuit) = self
            .store
            .load(tenant, goal_id)
            .await?
            .ok_or(StandingGoalProductError::NotFound)?;
        let pursuit = pursuit
            .filter(|p| p.pursuit_id == pursuit_id)
            .ok_or(StandingGoalProductError::Conflict)?;
        let prior = self
            .store
            .load_latest_evaluation(tenant, pursuit_id)
            .await?
            .ok_or(StandingGoalProductError::Corrupt)?;
        let at = UtcTimestamp::from_unix_millis(
            now.unix_millis()
                .max(
                    prior
                        .occurred_at
                        .unix_millis()
                        .checked_add(1)
                        .ok_or(StandingGoalProductError::Corrupt)?,
                )
                .max(pursuit.updated_at.unix_millis())
                .max(goal.updated_at.unix_millis()),
        );
        let reason = self.put_text(tenant, REASON).await?;
        let retirement = StandingGoalReviewRetirement::prepare(goal, pursuit, prior, reason, at)?;
        mutation(self.store.retire_completion_review(retirement).await?)
    }
}
