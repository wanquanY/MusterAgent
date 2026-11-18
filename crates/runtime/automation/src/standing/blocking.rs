//! Account and execution barriers are product lifecycle decisions, not model judgments.
use super::*;
use musteragent_runtime_contract::ErrorCode;

impl StandingGoalProductApplication {
    /// Settles an owner-claimed observation without admitting another paid Run.
    /// The existing evaluation lease/revision fence makes this idempotent under
    /// concurrent supervisors. Only explicit recovery may release the barrier.
    pub async fn block_execution(
        &self,
        claim: &ClaimedStandingGoalEvaluation,
        evaluation_id: GoalEvaluationId,
        code: Option<ErrorCode>,
        now: UtcTimestamp,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let pursuit = claim
            .inspection
            .pursuit
            .as_ref()
            .ok_or(StandingGoalProductError::Corrupt)?;
        let run_id = pursuit
            .active_run_id
            .ok_or(StandingGoalProductError::Corrupt)?;
        let tenant_id = claim.inspection.goal.tenant_id;
        let summary = match code {
            Some(ErrorCode::CreditsExhausted) => "Execution stopped because available credits are insufficient. Add credits, then explicitly continue the goal.",
            Some(ErrorCode::ModelNotEntitled) => "Execution stopped because the selected model is not available to this account. Review model access, then explicitly continue the goal.",
            _ => "Execution stopped. Resolve the issue reported in this conversation, then explicitly continue the goal.",
        }.as_bytes().to_vec();
        let reason_ref = self.put_text(tenant_id, &summary).await?;
        self.evaluate_run(EvaluateStandingGoalRun {
            tenant_id,
            goal_id: claim.inspection.goal.goal_id,
            pursuit_id: pursuit.pursuit_id,
            run_id,
            claim_token: claim.claim_token.clone(),
            evaluation_id,
            verdict: GoalVerdict::Waiting,
            evidence: Vec::new(),
            summary,
            deterministic: true,
            completion_confirmation_required: false,
            wait_barrier: Some(GoalWaitBarrier {
                kind: GoalWaitKind::ExternalDependency,
                correlation_key: format!("run-blocked:{run_id}"),
                reason_ref,
                resume_not_before: None,
                expires_at: None,
            }),
            continuation_due_at: None,
            usage_delta: claim.run.usage,
            occurred_at: now,
        })
        .await
    }
}
