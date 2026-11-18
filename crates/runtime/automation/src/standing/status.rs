//! User Goal state transitions; Run control remains a separate owner.
use super::*;
use musteragent_goal::validate_goal_transition;

pub(super) enum StatusChangeIntent {
    SetStatus,
    ResumeWait,
}

impl StandingGoalProductApplication {
    pub async fn change_status(
        &self,
        request: ChangeStandingGoalStatus,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let inspection = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        self.change_status_from_inspection(inspection, request, StatusChangeIntent::SetStatus)
            .await
    }

    pub async fn prepare_status(
        &self,
        request: ChangeStandingGoalStatus,
    ) -> Result<PreparedStandingGoalChange, StandingGoalProductError> {
        let inspection = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        self.prepare_status_from_inspection(inspection, request, StatusChangeIntent::SetStatus)
            .await
    }

    pub(super) async fn change_status_from_inspection(
        &self,
        inspection: StandingGoalInspection,
        request: ChangeStandingGoalStatus,
        intent: StatusChangeIntent,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let prepared = self
            .prepare_status_from_inspection(inspection, request, intent)
            .await?;
        self.commit_prepared_change(prepared).await
    }

    async fn prepare_status_from_inspection(
        &self,
        mut inspection: StandingGoalInspection,
        request: ChangeStandingGoalStatus,
        intent: StatusChangeIntent,
    ) -> Result<PreparedStandingGoalChange, StandingGoalProductError> {
        if inspection.goal.revision != request.expected_goal_revision {
            return Err(StandingGoalProductError::Conflict);
        }
        validate_goal_transition(inspection.goal.status, request.status)?;
        // Goal existence and execution existence are separate lifecycle axes.
        // A draft may be cancelled without ever creating a Pursuit or Run.
        // An Active Goal can own a Waiting Pursuit. Only the explicit command
        // that verified its barrier may resume it; a repeated Active request
        // remains idempotent and must not clear that barrier.
        if inspection.goal.status == request.status
            && matches!(intent, StatusChangeIntent::SetStatus)
        {
            if request.new_pursuit_id.is_some()
                || request.initial_run_id.is_some()
                || request.initial_message_id.is_some()
                || request.expected_pursuit_revision
                    != inspection.pursuit.as_ref().map(|p| p.revision)
            {
                return Err(StandingGoalProductError::Conflict);
            }
            return Ok(PreparedStandingGoalChange {
                inspection,
                mutation: StandingGoalChangeMutation::Unchanged {
                    goal_revision: request.expected_goal_revision,
                    pursuit_revision: request.expected_pursuit_revision,
                },
            });
        }
        if inspection.goal.status == StandingGoalStatus::Draft
            && request.status == StandingGoalStatus::Cancelled
        {
            return self.cancel_draft(inspection, request).await;
        }
        let draft_activation = inspection.goal.status == StandingGoalStatus::Draft
            && request.status == StandingGoalStatus::Active;
        let paused_activation = inspection.goal.status == StandingGoalStatus::Paused
            && request.status == StandingGoalStatus::Active;
        let continuous_control = inspection.goal.mode
            == musteragent_goal::StandingGoalMode::Continuous
            && (paused_activation || request.status == StandingGoalStatus::Paused)
            && matches!(intent, StatusChangeIntent::SetStatus);
        let supplied_source = match (request.initial_run_id, request.initial_message_id) {
            (Some(run_id), None) => Some(run_id),
            (None, Some(message_id)) => Some(
                self.store
                    .resolve_message_source_run(
                        request.tenant_id,
                        inspection.goal.conversation_id,
                        message_id,
                    )
                    .await?
                    .ok_or(StandingGoalProductError::Conflict)?,
            ),
            (None, None) => None,
            (Some(_), Some(_)) => return Err(StandingGoalProductError::InvalidRequest),
        };
        if continuous_control && supplied_source.is_some() {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        if paused_activation
            && !continuous_control
            && supplied_source.is_none()
            && self
                .store
                .requires_explicit_activation_source(request.tenant_id, request.goal_id)
                .await?
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        let mut pursuit = if draft_activation {
            if inspection.pursuit.is_some() || request.expected_pursuit_revision.is_some() {
                return Err(StandingGoalProductError::Conflict);
            }
            let independent = supplied_source.is_none()
                && inspection.goal.mode == musteragent_goal::StandingGoalMode::Continuous
                && self
                    .store
                    .has_independent_input_source(inspection.goal.clone())
                    .await?;
            // An independently sourced Goal needs no renderer-created Run or
            // Pursuit identity. The original lifecycle owner assigns the first
            // Pursuit; commit still rechecks source authority under its locks.
            let pursuit_id = match request.new_pursuit_id {
                Some(id) => id,
                None if independent => GoalPursuitId::from_uuid(uuid::Uuid::new_v5(
                    &inspection.goal.goal_id.into_uuid(),
                    b"standing-goal-independent-activation@1",
                ))
                .map_err(|_| StandingGoalProductError::Corrupt)?,
                None => return Err(StandingGoalProductError::InvalidRequest),
            };
            let (active, pursuit) = if let Some(initial_run_id) = supplied_source {
                inspection.goal.admitted_first_run(
                    pursuit_id,
                    initial_run_id,
                    request.occurred_at,
                )?
            } else if independent {
                inspection.goal.activated_waiting_for_input(
                    pursuit_id,
                    inspection.goal.objective_ref.clone(),
                    request.occurred_at,
                )?
            } else {
                return Err(StandingGoalProductError::InvalidRequest);
            };
            inspection.goal = active;
            pursuit
        } else {
            if request.new_pursuit_id.is_some() || (supplied_source.is_some() && !paused_activation)
            {
                return Err(StandingGoalProductError::InvalidRequest);
            }
            let pursuit = inspection
                .pursuit
                .ok_or(StandingGoalProductError::Corrupt)?;
            let expected_pursuit_revision = request
                .expected_pursuit_revision
                .ok_or(StandingGoalProductError::Conflict)?;
            if pursuit.revision != expected_pursuit_revision || pursuit.status.terminal() {
                return Err(StandingGoalProductError::Conflict);
            }
            pursuit
        };
        match request.status {
            StandingGoalStatus::Active => {
                if !draft_activation && !continuous_control {
                    if pursuit.status == GoalPursuitStatus::Running {
                        return Err(StandingGoalProductError::RunStillActive);
                    }
                    if let Some(source_run_id) = supplied_source {
                        pursuit.status = GoalPursuitStatus::Running;
                        pursuit.active_run_id = Some(source_run_id);
                        if pursuit.continuation_source_run_id != Some(source_run_id) {
                            pursuit.budget_usage.runs = pursuit
                                .budget_usage
                                .runs
                                .checked_add(1)
                                .ok_or(StandingGoalProductError::InvalidRequest)?;
                        }
                        pursuit.continuation_source_run_id = None;
                        pursuit.continuation_due_at = None;
                    } else {
                        // Resume from the frozen source. Admission must wait for
                        // that Run to finish naturally; pausing a Goal never
                        // cancels its source Run.
                        pursuit.status = GoalPursuitStatus::Pending;
                        pursuit.continuation_due_at = Some(request.occurred_at);
                    }
                    pursuit.wait_barrier = None;
                }
            }
            StandingGoalStatus::Paused if matches!(intent, StatusChangeIntent::ResumeWait) => {
                pursuit.status = GoalPursuitStatus::Pending;
                pursuit.wait_barrier = None;
                pursuit.continuation_due_at = Some(request.occurred_at);
            }
            StandingGoalStatus::Paused if continuous_control => {
                // Only close new input admission. Keep the exact Running,
                // Pending or Waiting state, including causal wait timestamps.
                // The store permits further Runs only for accepted input.
                if pursuit.status == GoalPursuitStatus::Pending
                    && inspection.goal.input_progress.pending() == 0
                {
                    pursuit.budget_usage.elapsed_millis = pursuit
                        .budget_usage
                        .elapsed_millis
                        .checked_add(active_interval_millis(
                            pursuit.updated_at,
                            request.occurred_at,
                        )?)
                        .ok_or(StandingGoalProductError::Corrupt)?;
                    pursuit.status = GoalPursuitStatus::Waiting;
                    pursuit.wait_barrier = Some(
                        inspection
                            .goal
                            .input_wait(inspection.goal.objective_ref.clone()),
                    );
                    pursuit.continuation_due_at = None;
                    pursuit.updated_at = request.occurred_at;
                }
            }
            StandingGoalStatus::Paused => {
                // Freeze automatic continuation, not the source Run. Preserve
                // its identity for a later resume while allowing the current
                // execution and its reply to finish normally.
                if matches!(
                    pursuit.status,
                    GoalPursuitStatus::Pending | GoalPursuitStatus::Running
                ) {
                    pursuit.budget_usage.elapsed_millis = pursuit
                        .budget_usage
                        .elapsed_millis
                        .checked_add(active_interval_millis(
                            pursuit.updated_at,
                            request.occurred_at,
                        )?)
                        .ok_or(StandingGoalProductError::Corrupt)?;
                    if let Some(active_run_id) = pursuit.active_run_id.take() {
                        pursuit.continuation_source_run_id = Some(active_run_id);
                    }
                }
                pursuit.status = GoalPursuitStatus::Paused;
                pursuit.continuation_due_at = None;
                pursuit.wait_barrier = None;
            }
            StandingGoalStatus::Cancelled => {
                if matches!(
                    pursuit.status,
                    GoalPursuitStatus::Pending | GoalPursuitStatus::Running
                ) {
                    pursuit.budget_usage.elapsed_millis = pursuit
                        .budget_usage
                        .elapsed_millis
                        .checked_add(active_interval_millis(
                            pursuit.updated_at,
                            request.occurred_at,
                        )?)
                        .ok_or(StandingGoalProductError::Corrupt)?;
                }
                // Close Goal scheduling without cancelling the executing Run.
                if let Some(source) = pursuit.active_run_id.take() {
                    pursuit.continuation_source_run_id = Some(source);
                }
                pursuit.status = GoalPursuitStatus::Cancelled;
                pursuit.continuation_due_at = None;
                pursuit.wait_barrier = None;
                pursuit.completed_at = Some(request.occurred_at);
            }
            StandingGoalStatus::Achieved | StandingGoalStatus::Failed => {
                return Err(StandingGoalProductError::EvaluationRequired);
            }
            StandingGoalStatus::Draft => return Err(StandingGoalProductError::InvalidRequest),
        }
        if !draft_activation {
            pursuit.revision = next_revision(pursuit.revision)?;
            inspection.goal.revision = next_revision(inspection.goal.revision)?;
            inspection.goal.status = request.status;
            inspection.goal.active_pursuit_id =
                (request.status == StandingGoalStatus::Active).then_some(pursuit.pursuit_id);
            inspection.goal.updated_at = request.occurred_at;
        }
        if !continuous_control {
            pursuit.updated_at = request.occurred_at;
        }
        inspection.goal.validate()?;
        pursuit.validate()?;
        inspection.progress = self
            .observe_progress(
                &inspection.goal,
                Some(&pursuit),
                inspection.latest_evaluation.as_ref(),
            )
            .await?;
        inspection.pursuit = Some(pursuit);
        Ok(PreparedStandingGoalChange {
            inspection,
            mutation: if draft_activation {
                StandingGoalChangeMutation::Activate {
                    goal_revision: request.expected_goal_revision,
                }
            } else {
                StandingGoalChangeMutation::ReplacePair {
                    goal_revision: request.expected_goal_revision,
                    pursuit_revision: request
                        .expected_pursuit_revision
                        .expect("validated existing Pursuit revision"),
                }
            },
        })
    }

    /// A never-started Goal has no Pursuit to terminate. The same Goal CAS used
    /// by edits also fences concurrent activation, without manufacturing work.
    async fn cancel_draft(
        &self,
        mut inspection: StandingGoalInspection,
        request: ChangeStandingGoalStatus,
    ) -> Result<PreparedStandingGoalChange, StandingGoalProductError> {
        if request.expected_pursuit_revision.is_some()
            || request.new_pursuit_id.is_some()
            || request.initial_run_id.is_some()
            || request.initial_message_id.is_some()
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        if inspection.pursuit.is_some() || inspection.goal.active_pursuit_id.is_some() {
            return Err(StandingGoalProductError::Corrupt);
        }
        inspection.goal.revision = next_revision(inspection.goal.revision)?;
        inspection.goal.status = StandingGoalStatus::Cancelled;
        inspection.goal.updated_at = request.occurred_at;
        inspection.goal.validate()?;
        inspection.progress = standing_goal_progress(
            &inspection.goal,
            None,
            inspection.latest_evaluation.as_ref(),
            None,
        );
        Ok(PreparedStandingGoalChange {
            inspection,
            mutation: StandingGoalChangeMutation::Replace {
                goal_revision: request.expected_goal_revision,
            },
        })
    }
}
