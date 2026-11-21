//! Claim-fenced admission settlement. Suspending a host is not a Goal failure.
use super::*;

const DEFAULT_ADMISSION_DENIED_REASON: &[u8] = b"Execution admission was denied. Resolve account, permission or configuration requirements, then explicitly continue.";

enum AdmissionResolution {
    Failure,
    Deferred,
}

impl StandingGoalProductApplication {
    pub async fn record_admission_failure(
        &self,
        request: RecordStandingGoalAdmissionFailure,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        self.settle_admission(request, AdmissionResolution::Failure)
            .await
    }

    /// Release the original claim without consuming the Goal's failure budget.
    /// A fresh owner may re-claim this same Pursuit after its explicit retry time.
    pub async fn defer_admission(
        &self,
        request: DeferStandingGoalAdmission,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        self.settle_admission(
            RecordStandingGoalAdmissionFailure {
                tenant_id: request.tenant_id,
                goal_id: request.goal_id,
                pursuit_id: request.pursuit_id,
                claim_token: request.claim_token,
                permanent: false,
                retry_at: Some(request.retry_at),
                occurred_at: request.occurred_at,
                reason: None,
            },
            AdmissionResolution::Deferred,
        )
        .await
    }

    async fn settle_admission(
        &self,
        request: RecordStandingGoalAdmissionFailure,
        resolution: AdmissionResolution,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        if request.claim_token.is_empty()
            || request.claim_token.len() > 512
            || request.permanent == request.retry_at.is_some()
            || (!request.permanent && request.reason.is_some())
            || request
                .retry_at
                .is_some_and(|value| value < request.occurred_at)
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        let (mut goal, pursuit) = self
            .store
            .load(request.tenant_id, request.goal_id)
            .await?
            .ok_or(StandingGoalProductError::NotFound)?;
        let mut pursuit = pursuit.ok_or(StandingGoalProductError::Corrupt)?;
        if !goal.allows_existing_work()
            || pursuit.pursuit_id != request.pursuit_id
            || pursuit.status != GoalPursuitStatus::Pending
        {
            return Err(StandingGoalProductError::Conflict);
        }
        let expected_goal_revision = goal.revision;
        let expected_pursuit_revision = pursuit.revision;
        if matches!(resolution, AdmissionResolution::Failure) {
            pursuit.budget_usage.consecutive_failures = pursuit
                .budget_usage
                .consecutive_failures
                .checked_add(1)
                .ok_or(StandingGoalProductError::Corrupt)?;
        }
        let blocked = request.permanent;
        pursuit.revision = next_revision(pursuit.revision)?;
        pursuit.updated_at = request.occurred_at;
        pursuit.continuation_due_at = if blocked { None } else { request.retry_at };
        if blocked {
            let reason_bytes = request
                .reason
                .as_ref()
                .map_or(DEFAULT_ADMISSION_DENIED_REASON, |reason| {
                    reason.as_str().as_bytes()
                });
            pursuit.status = GoalPursuitStatus::Waiting;
            pursuit.wait_barrier = Some(GoalWaitBarrier {
                kind: GoalWaitKind::ExternalDependency,
                correlation_key: format!("admission-blocked:{}", pursuit.pursuit_id),
                reason_ref: self.put_text(request.tenant_id, reason_bytes).await?,
                resume_not_before: None,
                expires_at: None,
            });
        }
        goal.revision = next_revision(goal.revision)?;
        goal.updated_at = request.occurred_at;
        goal.validate()?;
        pursuit.validate()?;
        mutation(
            self.store
                .settle_admission_claim(
                    goal.clone(),
                    pursuit.clone(),
                    expected_goal_revision,
                    expected_pursuit_revision,
                    request.claim_token,
                    request.occurred_at,
                )
                .await?,
        )?;
        let objective = self
            .content
            .read(goal.tenant_id, &goal.objective_ref)
            .await?;
        let latest_evaluation = self
            .store
            .load_latest_evaluation(goal.tenant_id, pursuit.pursuit_id)
            .await?;
        let progress =
            standing_goal_progress(&goal, Some(&pursuit), latest_evaluation.as_ref(), None);
        Ok(StandingGoalInspection {
            goal,
            pursuit: Some(pursuit),
            objective,
            latest_evaluation,
            progress,
        })
    }
}
