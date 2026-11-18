//! PostgreSQL authority for durable standing Goals, Pursuits and Evaluations.

#[path = "standing_goal/assignment.rs"]
mod assignment;

mod activation;
pub(crate) mod admission;
mod authority;
pub(crate) mod change;
mod codec;
mod decode;
mod error;
mod evaluation_decode;
mod health;
mod history;
pub(crate) mod input;
pub(crate) mod owner_goal;
mod result_review;
pub(crate) mod row_decode;
mod rules;
pub(crate) mod start;
mod wake;

use authority::{assert_source_run, load_execution_grant_proof_tx};
use codec::*;
use decode::*;
use error::*;
use evaluation_decode::content_ref;
pub(crate) use evaluation_decode::decode_evaluation;
use row_decode::*;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_goal::{
    GoalBudget, GoalBudgetUsage, GoalCompletionContract, GoalEvaluation, GoalEvaluationProposal,
    GoalOrigin, GoalPursuit, GoalPursuitStatus, GoalRiskClass, GoalVerdict, GoalWaitBarrier,
    GoalWaitKind, StandingGoal, StandingGoalStatus,
};
use musteragent_identity::{
    Actor, AgentId, ContentId, ConversationId, GoalEvaluationId, GoalId, GoalPursuitId, MessageId,
    PrincipalId, PrincipalKind, RunId, TenantId, UtcTimestamp,
};
use musteragent_runtime_automation::{
    ClaimStandingGoalEvaluations, ClaimStandingGoalPursuits, DeferStandingGoalEvaluation,
    DeleteStandingGoal, GoalRunTerminalObservation, GoalRunTerminalStatus,
    RecordStandingGoalWakeSignal, StandingGoalMutationOutcome, StandingGoalProductStore,
    StandingGoalRunPhaseObservation, StandingGoalStoreError, StandingGoalWakeSignalDisposition,
    StoredStandingGoalEvaluationClaim, StoredStandingGoalPursuitClaim, WakeDueStandingGoalWaits,
    WakeEventStandingGoalWaits,
};
use musteragent_runtime_contract::ApplicationFuture;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row as _};

struct EvaluationRevisions {
    expected_goal_revision: u64,
    expected_pursuit_revision: u64,
}

pub(crate) const GOAL_SELECT: &str = "SELECT goal.goal_id, goal.tenant_id,
            goal.input_accepted_seq, goal.input_consumed_seq,
            goal.revision::text AS revision, goal.owner_principal_id, goal.origin, goal.mode,
            goal.creator_agent_id, goal.target_agent_id, goal.conversation_id, goal.title,
            objective.content_id AS objective_content_id,
            objective.media_type AS objective_media_type,
            objective.byte_length::text AS objective_byte_length,
            objective.content_hash AS objective_hash,
            goal.constraints::text AS constraints_json,
            goal.success_criteria::text AS success_criteria_json,
            goal.evidence_requirements::text AS evidence_requirements_json,
            goal.failure_conditions::text AS failure_conditions_json,
            goal.minimum_evidence_items, goal.completion_risk_class, goal.max_runs,
            goal.max_elapsed_millis::text AS max_elapsed_millis,
            goal.max_model_tokens::text AS max_model_tokens,
            goal.max_tool_invocations::text AS max_tool_invocations,
            goal.max_consecutive_failures, goal.status, goal.active_pursuit_id,
            goal.created_at_ms, goal.updated_at_ms
     FROM (SELECT retained.* FROM runtime.standing_goals retained
       WHERE NOT EXISTS(SELECT 1 FROM runtime.goal_deletions deleted
         WHERE deleted.tenant_id=retained.tenant_id AND deleted.goal_id=retained.goal_id)) AS goal
     JOIN artifact.content_refs AS objective
       ON objective.tenant_id = goal.tenant_id
      AND objective.content_id = goal.objective_content_id";

pub(crate) const PURSUIT_SELECT: &str =
    "SELECT pursuit.pursuit_id, pursuit.tenant_id, pursuit.goal_id,
            pursuit.revision::text AS revision, pursuit.status, pursuit.active_run_id,
            pursuit.continuation_source_run_id,
            pursuit.wait_kind, pursuit.wait_correlation_key,
            wait_reason.content_id AS wait_reason_content_id,
            wait_reason.media_type AS wait_reason_media_type,
            wait_reason.byte_length::text AS wait_reason_byte_length,
            wait_reason.content_hash AS wait_reason_hash,
            pursuit.wait_resume_not_before_ms, pursuit.wait_expires_at_ms, pursuit.used_runs,
            pursuit.used_elapsed_millis::text AS used_elapsed_millis,
            pursuit.used_model_tokens::text AS used_model_tokens,
            pursuit.used_tool_invocations::text AS used_tool_invocations,
            pursuit.consecutive_failures, pursuit.continuation_due_at_ms,
            pursuit.started_at_ms, pursuit.updated_at_ms, pursuit.completed_at_ms
     FROM runtime.goal_pursuits AS pursuit
     LEFT JOIN artifact.content_refs AS wait_reason
       ON wait_reason.tenant_id = pursuit.tenant_id
      AND wait_reason.content_id = pursuit.wait_reason_content_id";

pub(crate) const EVALUATION_SELECT: &str =
    "SELECT evaluation.evaluation_id, evaluation.tenant_id, evaluation.goal_id,
            evaluation.pursuit_id, evaluation.run_id, evaluation.verdict,
            summary.content_id AS summary_content_id,
            summary.media_type AS summary_media_type,
            summary.byte_length::text AS summary_byte_length,
            summary.content_hash AS summary_hash,
            evaluation.deterministic, evaluation.occurred_at_ms
     FROM runtime.goal_evaluations AS evaluation
     JOIN artifact.content_refs AS summary
       ON summary.tenant_id = evaluation.tenant_id
      AND summary.content_id = evaluation.summary_content_id";

#[derive(Clone)]
pub struct PostgresStandingGoalProductStore {
    pool: PgPool,
    pub(super) database: crate::PostgresDatabaseRuntime,
}

impl PostgresStandingGoalProductStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self { pool, database }
    }

    /// Discovers only tenants that currently have actionable Goal work. The
    /// returned owner principal is the authoritative user identity used for
    /// continuation admission; every subsequent claim remains tenant-scoped.
    pub async fn supervisor_actors(
        &self,
        now: UtcTimestamp,
        limit: u16,
    ) -> Result<Vec<Actor>, StandingGoalStoreError> {
        if limit == 0 || limit > 1_024 {
            return Err(StandingGoalStoreError::Corrupt);
        }
        let rows = sqlx::query(
            "SELECT DISTINCT goal.tenant_id, goal.owner_principal_id
             FROM runtime.standing_goals AS goal
             JOIN runtime.goal_pursuits AS pursuit
               ON pursuit.tenant_id = goal.tenant_id AND pursuit.goal_id = goal.goal_id
             LEFT JOIN runtime.runs AS run
               ON run.tenant_id = pursuit.tenant_id AND run.run_id = pursuit.active_run_id
             WHERE (
                 (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND pursuit.status = 1
                 AND (EXISTS (
                 SELECT 1 FROM runtime.runs AS source_run
                 WHERE source_run.tenant_id = pursuit.tenant_id
                   AND source_run.run_id = pursuit.continuation_source_run_id
                   AND source_run.status IN (5, 6, 7, 8)
               ) OR EXISTS (
                 SELECT 1 FROM agent.dialogue_sessions AS dialogue_session
                 WHERE dialogue_session.tenant_id=goal.tenant_id AND dialogue_session.owner_id=goal.owner_principal_id
                   AND dialogue_session.source_goal_id=goal.goal_id AND dialogue_session.conversation_id=goal.conversation_id
                   AND dialogue_session.subject_kind='goal' AND goal.mode=2
                   AND goal.input_accepted_seq>goal.input_consumed_seq
               ))
               AND pursuit.continuation_due_at_ms IS NOT NULL
                 AND pursuit.continuation_due_at_ms <= $1
             ) OR (
                 (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND goal.mode = 2 AND pursuit.status = 3 AND pursuit.wait_kind = 3
                 AND (pursuit.pursuit_id = goal.active_pursuit_id OR goal.status = 3)
                 AND pursuit.wait_correlation_key = 'standing-goal-input:' || goal.goal_id::text
                 AND goal.input_accepted_seq > goal.input_consumed_seq
             ) OR (
                 (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND pursuit.status = 3 AND pursuit.wait_kind = 4
                 AND pursuit.wait_resume_not_before_ms IS NOT NULL
                 AND pursuit.wait_resume_not_before_ms <= $1
             ) OR (
                 (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND pursuit.status = 3 AND pursuit.wait_kind IN (1, 2)
                 AND EXISTS (
                     SELECT 1
                     FROM approval.interactions AS interaction
                     JOIN approval.interaction_responses AS response
                       ON response.interaction_id = interaction.interaction_id
                     WHERE interaction.tenant_id = pursuit.tenant_id
                       AND interaction.interaction_id::text = pursuit.wait_correlation_key
                       AND interaction.status = 2
                       AND interaction.responded_at_ms IS NOT NULL
                       AND interaction.responded_at_ms >= pursuit.updated_at_ms
                       AND ((pursuit.wait_kind = 1 AND interaction.kind <> 1)
                         OR (pursuit.wait_kind = 2 AND interaction.kind = 1
                             AND response.response_kind = 1))
                 )
             ) OR (
                 (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND pursuit.status = 3 AND pursuit.wait_kind = 3
                 AND EXISTS (
                     SELECT 1
                     FROM runtime.goal_wake_signals AS signal
                     WHERE signal.tenant_id = pursuit.tenant_id
                       AND signal.correlation_key = pursuit.wait_correlation_key
                       AND signal.correlation_key NOT LIKE 'standing-goal-input:%'
                       AND signal.occurred_at_ms >= pursuit.updated_at_ms
                       AND NOT EXISTS (
                           SELECT 1
                           FROM runtime.goal_wake_deliveries AS delivery
                           WHERE delivery.tenant_id = signal.tenant_id
                             AND delivery.signal_id = signal.signal_id
                             AND delivery.pursuit_id = pursuit.pursuit_id
                       )
                 )
             ) OR (
                 goal.status IN (2, 3, 6) AND pursuit.status = 2
                 AND run.status IN (5, 6, 7, 8)
             )
             ORDER BY goal.tenant_id, goal.owner_principal_id
             LIMIT $2",
        )
        .bind(now.unix_millis())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(PostgresStandingGoalError::from)?;
        rows.into_iter()
            .map(|row| {
                Ok(Actor {
                    tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
                        .map_err(|_| PostgresStandingGoalError::Corrupt)?,
                    principal_id: PrincipalId::from_uuid(row.try_get("owner_principal_id")?)
                        .map_err(|_| PostgresStandingGoalError::Corrupt)?,
                    principal_kind: PrincipalKind::User,
                })
            })
            .collect::<Result<Vec<_>, PostgresStandingGoalError>>()
            .map_err(Into::into)
    }

    async fn create_inner(
        &self,
        goal: StandingGoal,
        pursuit: Option<GoalPursuit>,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        goal.validate()
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        if let Some(value) = &pursuit {
            value
                .validate()
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            if value.goal_id != goal.goal_id || value.tenant_id != goal.tenant_id {
                return Err(PostgresStandingGoalError::Corrupt);
            }
        }
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        if let Some(run_id) = pursuit.as_ref().and_then(|value| value.active_run_id) {
            assert_source_run(
                &mut transaction,
                goal.tenant_id,
                goal.conversation_id,
                run_id,
            )
            .await?;
        }
        let inserted = insert_goal(&mut transaction, &goal).await?;
        // Read after INSERT has resolved any concurrent DELETE of this identity.
        // A replay must not resurrect a Goal whose deletion receipt just committed.
        let deleted: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.goal_deletions WHERE tenant_id = $1 AND goal_id = $2)")
            .bind(goal.tenant_id.into_uuid()).bind(goal.goal_id.into_uuid()).fetch_one(&mut *transaction).await?;
        if deleted {
            transaction.rollback().await?;
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        if inserted == 1
            && let Some(value) = &pursuit
            && insert_pursuit(&mut transaction, value).await? != 1
        {
            transaction.rollback().await?;
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        transaction.commit().await?;
        Ok(if inserted == 1 {
            StandingGoalMutationOutcome::Applied
        } else {
            StandingGoalMutationOutcome::Conflict
        })
    }

    async fn load_inner(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
    ) -> Result<Option<(StandingGoal, Option<GoalPursuit>)>, PostgresStandingGoalError> {
        let mut transaction = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::StandingGoal,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        let Some(row) = sqlx::query(&format!(
            "{GOAL_SELECT} WHERE goal.tenant_id = $1 AND goal.goal_id = $2"
        ))
        .bind(tenant_id.into_uuid())
        .bind(goal_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        else {
            return Ok(None);
        };
        let goal = decode_goal(&row)?;
        let pursuit = load_latest_pursuit(&mut transaction, tenant_id, goal_id).await?;
        transaction.commit().await?;
        Ok(Some((goal, pursuit)))
    }

    async fn list_inner(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
    ) -> Result<Vec<(StandingGoal, Option<GoalPursuit>)>, PostgresStandingGoalError> {
        let mut transaction = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::StandingGoal,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        let rows = sqlx::query(&format!(
            "{GOAL_SELECT} WHERE goal.tenant_id = $1 AND goal.conversation_id = $2
             ORDER BY goal.updated_at_ms DESC, goal.goal_id DESC"
        ))
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_all(&mut *transaction)
        .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let goal = decode_goal(&row)?;
            let pursuit = load_latest_pursuit(&mut transaction, tenant_id, goal.goal_id).await?;
            result.push((goal, pursuit));
        }
        transaction.commit().await?;
        Ok(result)
    }

    async fn replace_goal_inner(
        &self,
        goal: StandingGoal,
        expected_revision: u64,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let outcome = change::replace(&mut tx, &goal, expected_revision).await?;
        if outcome == StandingGoalMutationOutcome::Applied {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        Ok(outcome)
    }

    async fn replace_pair_inner(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let outcome = change::replace_pair(
            &mut tx,
            &goal,
            &pursuit,
            expected_goal_revision,
            expected_pursuit_revision,
        )
        .await?;
        if outcome == StandingGoalMutationOutcome::Applied {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        Ok(outcome)
    }

    async fn claim_due_inner(
        &self,
        request: ClaimStandingGoalPursuits,
    ) -> Result<Vec<StoredStandingGoalPursuitClaim>, PostgresStandingGoalError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let rows = sqlx::query(
            "SELECT pursuit.pursuit_id, pursuit.goal_id
             FROM runtime.goal_pursuits AS pursuit
             JOIN runtime.standing_goals AS goal
               ON goal.tenant_id = pursuit.tenant_id AND goal.goal_id = pursuit.goal_id
             WHERE pursuit.tenant_id = $1 AND (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND pursuit.status = 1
               AND (EXISTS (
                 SELECT 1 FROM runtime.runs AS source_run
                 WHERE source_run.tenant_id = pursuit.tenant_id
                   AND source_run.run_id = pursuit.continuation_source_run_id
                   AND source_run.status IN (5, 6, 7, 8)
               ) OR EXISTS (
                 SELECT 1 FROM agent.dialogue_sessions AS dialogue_session
                 WHERE dialogue_session.tenant_id=goal.tenant_id AND dialogue_session.owner_id=goal.owner_principal_id
                   AND dialogue_session.source_goal_id=goal.goal_id AND dialogue_session.conversation_id=goal.conversation_id
                   AND dialogue_session.subject_kind='goal' AND goal.mode=2
                   AND goal.input_accepted_seq>goal.input_consumed_seq
               ))
               AND pursuit.continuation_due_at_ms IS NOT NULL
               AND pursuit.continuation_due_at_ms <= $2
               AND (pursuit.claim_token IS NULL OR pursuit.claim_lease_expires_at_ms <= $3)
               AND NOT EXISTS (
                 SELECT 1 FROM runtime.runs AS foreground
                 WHERE foreground.tenant_id = pursuit.tenant_id
                   AND foreground.conversation_id = goal.conversation_id
                   AND foreground.activity_kind = 1
                   AND foreground.status IN (1, 2, 3, 4)
               )
             ORDER BY pursuit.continuation_due_at_ms, pursuit.pursuit_id
             LIMIT $4 FOR UPDATE OF goal SKIP LOCKED",
        )
        .bind(request.tenant_id.into_uuid())
        .bind(request.now.unix_millis())
        .bind(request.now.unix_millis())
        .bind(i64::from(request.limit))
        .fetch_all(&mut *transaction)
        .await?;
        let lease_expires_at = request
            .now
            .checked_add_millis(
                i64::try_from(request.lease_millis)
                    .map_err(|_| PostgresStandingGoalError::Corrupt)?,
            )
            .ok_or(PostgresStandingGoalError::Corrupt)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let pursuit_id = GoalPursuitId::from_uuid(row.try_get("pursuit_id")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            let goal_id = GoalId::from_uuid(row.try_get("goal_id")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            let claim_token = format!("{}:{}", request.worker_id, uuid::Uuid::now_v7());
            let updated = sqlx::query(
                "UPDATE runtime.goal_pursuits AS pursuit
                 SET claim_token = $1, claim_worker_id = $2, claim_lease_expires_at_ms = $3
                 WHERE pursuit.tenant_id = $4 AND pursuit.pursuit_id = $5 AND pursuit.status = 1
                   AND pursuit.continuation_due_at_ms <= $6
                   AND (pursuit.claim_token IS NULL OR pursuit.claim_lease_expires_at_ms <= $7)
                   AND NOT EXISTS (
                     SELECT 1
                     FROM runtime.standing_goals AS goal
                     JOIN runtime.runs AS foreground
                       ON foreground.tenant_id = goal.tenant_id
                      AND foreground.conversation_id = goal.conversation_id
                     WHERE goal.tenant_id = pursuit.tenant_id
                       AND goal.goal_id = pursuit.goal_id
                       AND foreground.activity_kind = 1
                       AND foreground.status IN (1, 2, 3, 4)
                   )",
            )
            .bind(&claim_token)
            .bind(&request.worker_id)
            .bind(lease_expires_at.unix_millis())
            .bind(request.tenant_id.into_uuid())
            .bind(pursuit_id.into_uuid())
            .bind(request.now.unix_millis())
            .bind(request.now.unix_millis())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if updated != 1 {
                continue;
            }
            let goal = load_goal_tx(&mut transaction, request.tenant_id, goal_id)
                .await?
                .ok_or(PostgresStandingGoalError::Corrupt)?;
            let pursuit = load_pursuit_tx(&mut transaction, request.tenant_id, pursuit_id)
                .await?
                .ok_or(PostgresStandingGoalError::Corrupt)?;
            let (input_refs, processing_rules) = input::freeze(&mut transaction, &goal).await?;
            let source = if let Some(binding) =
                owner_goal::load_binding(&mut transaction, &goal).await?
            {
                musteragent_runtime_automation::StandingGoalPursuitSource::OwnerGoal(binding.scope)
            } else {
                let source_run_id = pursuit
                    .continuation_source_run_id
                    .ok_or(PostgresStandingGoalError::Corrupt)?;
                let continuation_authority =
                    load_continuation_authority_tx(&mut transaction, request.tenant_id, pursuit_id)
                        .await?;
                let source_execution_grant_proof = load_execution_grant_proof_tx(
                    &mut transaction,
                    request.tenant_id,
                    source_run_id,
                )
                .await?;
                musteragent_runtime_automation::StandingGoalPursuitSource::TerminalRun {
                    source_run_id,
                    source_execution_grant_proof,
                    continuation_authority,
                }
            };
            claims.push(StoredStandingGoalPursuitClaim {
                processing_rules,
                input_refs,
                goal,
                pursuit,
                claim_token,
                source,
            });
        }
        transaction.commit().await?;
        Ok(claims)
    }

    async fn wake_due_waits_inner(
        &self,
        request: WakeDueStandingGoalWaits,
    ) -> Result<u16, PostgresStandingGoalError> {
        wake::due(self, request).await
    }

    async fn wake_event_waits_inner(
        &self,
        request: WakeEventStandingGoalWaits,
    ) -> Result<u16, PostgresStandingGoalError> {
        wake::event(self, request).await
    }

    async fn record_wake_signal_inner(
        &self,
        request: RecordStandingGoalWakeSignal,
    ) -> Result<StandingGoalWakeSignalDisposition, PostgresStandingGoalError> {
        wake::record_signal(self, request).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn settle_admission_claim_inner(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
        claim_token: String,
        occurred_at: UtcTimestamp,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        goal.validate()
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        pursuit
            .validate()
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        admission::lock_goal(&mut transaction, goal.tenant_id, goal.goal_id).await?;
        let released = sqlx::query(
            "UPDATE runtime.goal_pursuits
             SET claim_token = NULL, claim_worker_id = NULL,
                 claim_lease_expires_at_ms = NULL
             WHERE tenant_id = $1 AND pursuit_id = $2 AND status = 1
               AND revision = $3::numeric AND claim_token = $4
               AND claim_lease_expires_at_ms > $5",
        )
        .bind(goal.tenant_id.into_uuid())
        .bind(pursuit.pursuit_id.into_uuid())
        .bind(expected_pursuit_revision.to_string())
        .bind(claim_token)
        .bind(occurred_at.unix_millis())
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let pursuit_rows = if released == 1 {
            update_pursuit(&mut transaction, &pursuit, expected_pursuit_revision).await?
        } else {
            0
        };
        let goal_rows = if pursuit_rows == 1 {
            update_goal(&mut transaction, &goal, expected_goal_revision).await?
        } else {
            0
        };
        if pursuit_rows == 1 && goal_rows == 1 {
            transaction.commit().await?;
            return Ok(StandingGoalMutationOutcome::Applied);
        }
        let outcome = if goal_exists(&mut transaction, goal.tenant_id, goal.goal_id).await? {
            StandingGoalMutationOutcome::Conflict
        } else {
            StandingGoalMutationOutcome::NotFound
        };
        transaction.rollback().await?;
        Ok(outcome)
    }

    async fn submit_evaluation_proposal_inner(
        &self,
        proposal: GoalEvaluationProposal,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let goal = load_goal_tx(&mut transaction, proposal.tenant_id, proposal.goal_id)
            .await?
            .ok_or(PostgresStandingGoalError::NotFound)?;
        let completion = rules::completion(&mut transaction, &goal, Some(proposal.run_id)).await?;
        proposal
            .validate(&completion)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let pursuit = load_pursuit_tx(&mut transaction, proposal.tenant_id, proposal.pursuit_id)
            .await?
            .ok_or(PostgresStandingGoalError::NotFound)?;
        if !goal.allows_existing_work()
            || pursuit.goal_id != proposal.goal_id
            || pursuit.status != GoalPursuitStatus::Running
            || pursuit.active_run_id != Some(proposal.run_id)
        {
            transaction.rollback().await?;
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        let wait = proposal.wait_barrier.as_ref();
        let inserted = sqlx::query(
            "INSERT INTO runtime.goal_evaluation_proposals (
                 tenant_id, goal_id, pursuit_id, run_id, verdict,
                 summary_content_id,
                 wait_kind, wait_correlation_key, wait_reason_content_id,
                 wait_resume_not_before_ms, wait_expires_at_ms, proposed_at_ms
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (tenant_id, pursuit_id, run_id) DO NOTHING",
        )
        .bind(proposal.tenant_id.into_uuid())
        .bind(proposal.goal_id.into_uuid())
        .bind(proposal.pursuit_id.into_uuid())
        .bind(proposal.run_id.into_uuid())
        .bind(verdict_code(proposal.verdict))
        .bind(proposal.summary_ref.content_id.into_uuid())
        .bind(wait.map(|value| wait_kind_code(value.kind)))
        .bind(wait.map(|value| value.correlation_key.clone()))
        .bind(wait.map(|value| value.reason_ref.content_id.into_uuid()))
        .bind(
            wait.and_then(|value| value.resume_not_before)
                .map(UtcTimestamp::unix_millis),
        )
        .bind(
            wait.and_then(|value| value.expires_at)
                .map(UtcTimestamp::unix_millis),
        )
        .bind(proposal.proposed_at.unix_millis())
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if inserted == 1 {
            insert_proposal_evidence(&mut transaction, &proposal).await?;
            transaction.commit().await?;
            return Ok(StandingGoalMutationOutcome::Applied);
        }
        let existing = load_proposal_tx(
            &mut transaction,
            proposal.tenant_id,
            proposal.pursuit_id,
            proposal.run_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(if existing.as_ref() == Some(&proposal) {
            StandingGoalMutationOutcome::Applied
        } else {
            StandingGoalMutationOutcome::Conflict
        })
    }

    async fn claim_terminal_evaluations_inner(
        &self,
        request: ClaimStandingGoalEvaluations,
    ) -> Result<Vec<StoredStandingGoalEvaluationClaim>, PostgresStandingGoalError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let rows = sqlx::query(
            "SELECT pursuit.pursuit_id, pursuit.goal_id, pursuit.active_run_id,
                    run.status AS run_status, run.started_at_ms, run.finished_at_ms, run.terminal_error
             FROM runtime.goal_pursuits AS pursuit
             JOIN runtime.standing_goals AS goal
               ON goal.tenant_id = pursuit.tenant_id AND goal.goal_id = pursuit.goal_id
             JOIN runtime.runs AS run
               ON run.tenant_id = pursuit.tenant_id AND run.run_id = pursuit.active_run_id
             LEFT JOIN runtime.goal_evaluation_claims AS claim
               ON claim.tenant_id = pursuit.tenant_id
              AND claim.pursuit_id = pursuit.pursuit_id
             WHERE pursuit.tenant_id = $1 AND pursuit.status = 2
               AND (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2)) AND run.status IN (5, 6, 7, 8)
               AND (claim.claim_token IS NULL OR claim.claim_lease_expires_at_ms <= $2)
             ORDER BY run.finished_at_ms, pursuit.pursuit_id LIMIT $3
             FOR UPDATE OF pursuit SKIP LOCKED",
        )
        .bind(request.tenant_id.into_uuid())
        .bind(request.now.unix_millis())
        .bind(i64::from(request.limit))
        .fetch_all(&mut *transaction)
        .await?;
        let lease_expires_at = request
            .now
            .checked_add_millis(
                i64::try_from(request.lease_millis)
                    .map_err(|_| PostgresStandingGoalError::Corrupt)?,
            )
            .ok_or(PostgresStandingGoalError::Corrupt)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let pursuit_id = GoalPursuitId::from_uuid(row.try_get("pursuit_id")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            let goal_id = GoalId::from_uuid(row.try_get("goal_id")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            let run_id = RunId::from_uuid(row.try_get("active_run_id")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            let claim_token = format!("{}:{}", request.worker_id, uuid::Uuid::now_v7());
            let claimed = sqlx::query(
                "INSERT INTO runtime.goal_evaluation_claims AS existing (
                     tenant_id, pursuit_id, run_id, claim_token, claim_worker_id,
                     claim_lease_expires_at_ms, claimed_at_ms
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (tenant_id, pursuit_id) DO UPDATE SET
                     run_id = excluded.run_id, claim_token = excluded.claim_token,
                     claim_worker_id = excluded.claim_worker_id,
                     claim_lease_expires_at_ms = excluded.claim_lease_expires_at_ms,
                     claimed_at_ms = excluded.claimed_at_ms
                 WHERE existing.claim_lease_expires_at_ms <= $8",
            )
            .bind(request.tenant_id.into_uuid())
            .bind(pursuit_id.into_uuid())
            .bind(run_id.into_uuid())
            .bind(&claim_token)
            .bind(&request.worker_id)
            .bind(lease_expires_at.unix_millis())
            .bind(request.now.unix_millis())
            .bind(request.now.unix_millis())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if claimed != 1 {
                continue;
            }
            let goal = load_goal_tx(&mut transaction, request.tenant_id, goal_id)
                .await?
                .ok_or(PostgresStandingGoalError::Corrupt)?;
            let pursuit = load_pursuit_tx(&mut transaction, request.tenant_id, pursuit_id)
                .await?
                .ok_or(PostgresStandingGoalError::Corrupt)?;
            let proposal =
                load_proposal_tx(&mut transaction, request.tenant_id, pursuit_id, run_id).await?;
            let run = terminal_observation(&mut transaction, run_id, &row).await?;
            let source_execution_grant_proof =
                load_execution_grant_proof_tx(&mut transaction, request.tenant_id, run_id).await?;
            let continuation_authority =
                load_continuation_authority_tx(&mut transaction, request.tenant_id, pursuit_id)
                    .await?;
            let processing_rules =
                rules::for_run(&mut transaction, request.tenant_id, goal_id, run_id).await?;
            claims.push(StoredStandingGoalEvaluationClaim {
                owner_goal_scope: owner_goal::load_binding(&mut transaction, &goal)
                    .await?
                    .map(|binding| binding.scope),
                processing_rules,
                goal,
                pursuit,
                claim_token,
                run,
                proposal,
                source_execution_grant_proof,
                continuation_authority,
            });
        }
        transaction.commit().await?;
        Ok(claims)
    }

    async fn commit_evaluation_inner(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        evaluation: GoalEvaluation,
        EvaluationRevisions {
            expected_goal_revision,
            expected_pursuit_revision,
        }: EvaluationRevisions,
        claim_token: String,
        input_settlement: Option<RunId>,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        goal.validate()
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        pursuit
            .validate()
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        admission::lock_goal(&mut transaction, goal.tenant_id, goal.goal_id).await?;
        let completion = rules::completion(&mut transaction, &goal, evaluation.run_id).await?;
        evaluation
            .validate(&completion)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let owns_claim: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime.goal_evaluation_claims
             WHERE tenant_id = $1 AND pursuit_id = $2 AND run_id = $3
               AND claim_token = $4 AND claim_lease_expires_at_ms > $5",
        )
        .bind(goal.tenant_id.into_uuid())
        .bind(pursuit.pursuit_id.into_uuid())
        .bind(evaluation.run_id.map(|value| value.into_uuid()))
        .bind(&claim_token)
        .bind(evaluation.occurred_at.unix_millis())
        .fetch_one(&mut *transaction)
        .await?;
        let pursuit_rows = if owns_claim == 1 {
            update_pursuit(&mut transaction, &pursuit, expected_pursuit_revision).await?
        } else {
            0
        };
        let goal_rows = if pursuit_rows == 1 {
            update_goal(&mut transaction, &goal, expected_goal_revision).await?
        } else {
            0
        };
        if pursuit_rows != 1 || goal_rows != 1 {
            let outcome = if goal_exists(&mut transaction, goal.tenant_id, goal.goal_id).await? {
                StandingGoalMutationOutcome::Conflict
            } else {
                StandingGoalMutationOutcome::NotFound
            };
            transaction.rollback().await?;
            return Ok(outcome);
        }
        if let Some(run) = input_settlement {
            if evaluation.run_id != Some(run) {
                return Err(PostgresStandingGoalError::Corrupt);
            }
            input::settle(&mut transaction, &goal, run).await?;
        }
        insert_evaluation(&mut transaction, &evaluation).await?;
        let deleted = sqlx::query(
            "DELETE FROM runtime.goal_evaluation_claims
             WHERE tenant_id = $1 AND pursuit_id = $2 AND claim_token = $3",
        )
        .bind(goal.tenant_id.into_uuid())
        .bind(pursuit.pursuit_id.into_uuid())
        .bind(claim_token)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if deleted != 1 {
            transaction.rollback().await?;
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        transaction.commit().await?;
        Ok(StandingGoalMutationOutcome::Applied)
    }

    async fn latest_evaluation_inner(
        &self,
        tenant_id: TenantId,
        pursuit_id: GoalPursuitId,
    ) -> Result<Option<GoalEvaluation>, PostgresStandingGoalError> {
        let row = sqlx::query(&format!(
            "{EVALUATION_SELECT} WHERE evaluation.tenant_id = $1
             AND evaluation.pursuit_id = $2
             ORDER BY evaluation.occurred_at_ms DESC, evaluation.evaluation_id DESC LIMIT 1"
        ))
        .bind(tenant_id.into_uuid())
        .bind(pursuit_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => Ok(Some(decode_evaluation(&self.pool, &row).await?)),
            None => Ok(None),
        }
    }
}

impl StandingGoalProductStore for PostgresStandingGoalProductStore {
    fn has_independent_input_source(
        &self,
        goal: StandingGoal,
    ) -> ApplicationFuture<'_, Result<bool, StandingGoalStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::StandingGoal,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| StandingGoalStoreError::Unavailable)?;
            let active = owner_goal::independent_input_source_active(&mut tx, &goal).await?;
            tx.commit()
                .await
                .map_err(|_| StandingGoalStoreError::Unavailable)?;
            Ok(active)
        })
    }
    fn load_processing_rules(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        run_id: RunId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<musteragent_goal::GoalProcessingRules>, StandingGoalStoreError>,
    > {
        Box::pin(async move {
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
                .await
                .map_err(PostgresStandingGoalError::from)?;
            let value = rules::for_run(&mut tx, tenant_id, goal_id, run_id).await?;
            tx.commit().await.map_err(PostgresStandingGoalError::from)?;
            Ok(value)
        })
    }

    fn accept_input(
        &self,
        request: musteragent_runtime_automation::AcceptStandingGoalInput,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_runtime_automation::StandingGoalInputOutcome, StandingGoalStoreError>,
    > {
        Box::pin(async move { self.accept_input_inner(request).await.map_err(Into::into) })
    }

    fn load_deletion(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        principal_id: PrincipalId,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<
        '_,
        Result<
            Option<musteragent_runtime_automation::StandingGoalDeletion>,
            StandingGoalStoreError,
        >,
    > {
        Box::pin(async move {
            self.load_deletion_inner(tenant_id, goal_id, principal_id, conversation_id)
                .await
                .map_err(Into::into)
        })
    }

    fn load_evaluation_retry(
        &self,
        tenant_id: TenantId,
        pursuit_id: GoalPursuitId,
        run_id: RunId,
    ) -> ApplicationFuture<
        '_,
        Result<
            Option<musteragent_runtime_automation::StandingGoalEvaluationRetry>,
            StandingGoalStoreError,
        >,
    > {
        Box::pin(async move {
            self.load_evaluation_retry_inner(tenant_id, pursuit_id, run_id)
                .await
                .map_err(Into::into)
        })
    }

    fn create(
        &self,
        goal: StandingGoal,
        pursuit: Option<GoalPursuit>,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move { self.create_inner(goal, pursuit).await.map_err(Into::into) })
    }

    fn load(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<(StandingGoal, Option<GoalPursuit>)>, StandingGoalStoreError>,
    > {
        Box::pin(async move {
            self.load_inner(tenant_id, goal_id)
                .await
                .map_err(Into::into)
        })
    }

    fn list_for_conversation(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<
        '_,
        Result<Vec<(StandingGoal, Option<GoalPursuit>)>, StandingGoalStoreError>,
    > {
        Box::pin(async move {
            self.list_inner(tenant_id, conversation_id)
                .await
                .map_err(Into::into)
        })
    }

    fn load_latest_evaluation(
        &self,
        tenant_id: TenantId,
        pursuit_id: GoalPursuitId,
    ) -> ApplicationFuture<'_, Result<Option<GoalEvaluation>, StandingGoalStoreError>> {
        Box::pin(async move {
            self.latest_evaluation_inner(tenant_id, pursuit_id)
                .await
                .map_err(Into::into)
        })
    }

    fn load_run_phase(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<StandingGoalRunPhaseObservation>, StandingGoalStoreError>,
    > {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT status,
                        COALESCE(finished_at_ms, started_at_ms, admitted_at_ms) AS changed_at_ms
                 FROM runtime.runs
                 WHERE tenant_id = $1 AND run_id = $2",
            )
            .bind(tenant_id.into_uuid())
            .bind(run_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresStandingGoalError::from)?;
            row.map(|row| {
                let status = row.try_get::<i16, _>("status")?;
                if !(1..=8).contains(&status) {
                    return Err(PostgresStandingGoalError::Corrupt);
                }
                Ok(StandingGoalRunPhaseObservation {
                    terminal: status >= 5,
                    changed_at: UtcTimestamp::from_unix_millis(
                        row.try_get::<i64, _>("changed_at_ms")?,
                    ),
                })
            })
            .transpose()
            .map_err(Into::into)
        })
    }

    fn resolve_message_source_run(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        message_id: MessageId,
    ) -> ApplicationFuture<'_, Result<Option<RunId>, StandingGoalStoreError>> {
        Box::pin(async move {
            let value = sqlx::query_scalar::<_, uuid::Uuid>(
                "SELECT message.run_id
                 FROM conversation.messages AS message
                 JOIN runtime.runs AS run
                   ON run.tenant_id = message.tenant_id
                  AND run.conversation_id = message.conversation_id
                  AND run.run_id = message.run_id
                 WHERE message.tenant_id = $1
                   AND message.conversation_id = $2
                   AND message.message_id = $3
                   AND message.role = 1",
            )
            .bind(tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .bind(message_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresStandingGoalError::from)?;
            value
                .map(|value| id(value, "standing_goal_initial_message_source_run_id"))
                .transpose()
                .map_err(Into::into)
        })
    }

    fn replace_goal(
        &self,
        goal: StandingGoal,
        expected_revision: u64,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.replace_goal_inner(goal, expected_revision)
                .await
                .map_err(Into::into)
        })
    }

    fn replace_goal_and_pursuit(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.replace_pair_inner(
                goal,
                pursuit,
                expected_goal_revision,
                expected_pursuit_revision,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn activate_goal_with_pursuit(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.activate_with_pursuit_inner(goal, pursuit, expected_goal_revision)
                .await
                .map_err(Into::into)
        })
    }

    fn list_completion_reviews(
        &self,
        tenant: TenantId,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<(GoalId, GoalPursuitId)>, StandingGoalStoreError>> {
        Box::pin(async move {
            self.list_completion_reviews_inner(tenant, limit)
                .await
                .map_err(Into::into)
        })
    }
    fn retire_completion_review(
        &self,
        retirement: musteragent_runtime_automation::StandingGoalReviewRetirement,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.retire_completion_review_inner(retirement)
                .await
                .map_err(Into::into)
        })
    }

    fn delete(
        &self,
        request: DeleteStandingGoal,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move { self.delete_inner(request).await.map_err(Into::into) })
    }

    fn claim_due(
        &self,
        request: ClaimStandingGoalPursuits,
    ) -> ApplicationFuture<'_, Result<Vec<StoredStandingGoalPursuitClaim>, StandingGoalStoreError>>
    {
        Box::pin(async move { self.claim_due_inner(request).await.map_err(Into::into) })
    }

    fn wake_due_waits(
        &self,
        request: WakeDueStandingGoalWaits,
    ) -> ApplicationFuture<'_, Result<u16, StandingGoalStoreError>> {
        Box::pin(async move { self.wake_due_waits_inner(request).await.map_err(Into::into) })
    }

    fn wake_event_waits(
        &self,
        request: WakeEventStandingGoalWaits,
    ) -> ApplicationFuture<'_, Result<u16, StandingGoalStoreError>> {
        Box::pin(async move {
            self.wake_event_waits_inner(request)
                .await
                .map_err(Into::into)
        })
    }

    fn record_wake_signal(
        &self,
        request: RecordStandingGoalWakeSignal,
    ) -> ApplicationFuture<'_, Result<StandingGoalWakeSignalDisposition, StandingGoalStoreError>>
    {
        Box::pin(async move {
            self.record_wake_signal_inner(request)
                .await
                .map_err(Into::into)
        })
    }

    fn settle_admission_claim(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
        claim_token: String,
        occurred_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.settle_admission_claim_inner(
                goal,
                pursuit,
                expected_goal_revision,
                expected_pursuit_revision,
                claim_token,
                occurred_at,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn submit_evaluation_proposal(
        &self,
        proposal: GoalEvaluationProposal,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.submit_evaluation_proposal_inner(proposal)
                .await
                .map_err(Into::into)
        })
    }

    fn claim_terminal_evaluations(
        &self,
        request: ClaimStandingGoalEvaluations,
    ) -> ApplicationFuture<'_, Result<Vec<StoredStandingGoalEvaluationClaim>, StandingGoalStoreError>>
    {
        Box::pin(async move {
            self.claim_terminal_evaluations_inner(request)
                .await
                .map_err(Into::into)
        })
    }

    fn defer_terminal_evaluation(
        &self,
        request: DeferStandingGoalEvaluation,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.defer_terminal_evaluation_inner(request)
                .await
                .map_err(Into::into)
        })
    }

    fn commit_evaluation(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        evaluation: GoalEvaluation,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
        claim_token: String,
        input_settlement: Option<RunId>,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.commit_evaluation_inner(
                goal,
                pursuit,
                evaluation,
                EvaluationRevisions {
                    expected_goal_revision,
                    expected_pursuit_revision,
                },
                claim_token,
                input_settlement,
            )
            .await
            .map_err(Into::into)
        })
    }
}

pub(crate) async fn insert_goal(
    transaction: &mut sqlx::PgConnection,
    goal: &StandingGoal,
) -> Result<u64, PostgresStandingGoalError> {
    assignment::guard_creation(transaction, goal).await?;
    // Goal budget/token fields retain the complete unsigned compatibility
    // contract, including the historical `u64::MAX` unbounded sentinel.
    // They are explicit NUMERIC exceptions and were not part of migration 133.
    let result = sqlx::query(
        "INSERT INTO runtime.standing_goals (
             goal_id, tenant_id, revision, owner_principal_id, origin,
             creator_agent_id, target_agent_id, conversation_id, title,
             objective_content_id, constraints, success_criteria,
             evidence_requirements, failure_conditions,
             minimum_evidence_items, completion_risk_class, max_runs, max_elapsed_millis,
             max_model_tokens, max_tool_invocations, max_consecutive_failures,
             status, active_pursuit_id, created_at_ms, updated_at_ms, mode
         ) VALUES (
             $1, $2, $3::numeric, $4, $5, $6, $7, $8, $9, $10,
             $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb,
             $15, $16, $17, $18::numeric, $19::numeric, $20::numeric,
             $21, $22, $23, $24, $25, $26
         ) ON CONFLICT DO NOTHING",
    )
    .bind(goal.goal_id.into_uuid())
    .bind(goal.tenant_id.into_uuid())
    .bind(goal.revision.to_string())
    .bind(goal.owner.into_uuid())
    .bind(origin_code(goal.origin))
    .bind(goal.creator_agent_id.map(|value| value.into_uuid()))
    .bind(goal.target_agent_id.into_uuid())
    .bind(goal.conversation_id.into_uuid())
    .bind(&goal.title)
    .bind(goal.objective_ref.content_id.into_uuid())
    .bind(json(&goal.constraints)?)
    .bind(json(&goal.completion.success_criteria)?)
    .bind(json(&goal.completion.evidence_requirements)?)
    .bind(json(&goal.completion.failure_conditions)?)
    .bind(
        i16::try_from(goal.completion.minimum_evidence_items)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
    )
    .bind(risk_class_code(goal.completion.risk_class))
    .bind(i32::try_from(goal.budget.max_runs).map_err(|_| PostgresStandingGoalError::Corrupt)?)
    .bind(goal.budget.max_elapsed_millis.to_string())
    .bind(goal.budget.max_model_tokens.to_string())
    .bind(goal.budget.max_tool_invocations.to_string())
    .bind(
        i32::try_from(goal.budget.max_consecutive_failures)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
    )
    .bind(goal_status_code(goal.status))
    .bind(goal.active_pursuit_id.map(|value| value.into_uuid()))
    .bind(goal.created_at.unix_millis())
    .bind(goal.updated_at.unix_millis())
    .bind(mode_code(goal.mode))
    .execute(&mut *transaction)
    .await?;
    Ok(result.rows_affected())
}

async fn update_goal(
    transaction: &mut sqlx::PgConnection,
    goal: &StandingGoal,
    expected_revision: u64,
) -> Result<u64, PostgresStandingGoalError> {
    // Keep this statement type-aligned with the still-NUMERIC Goal contract;
    // a BIGINT parameter cast would reject the persisted unbounded sentinel.
    let result = sqlx::query(
        "UPDATE runtime.standing_goals SET
             revision = $1::numeric, target_agent_id = $2, title = $3,
             objective_content_id = $4, constraints = $5::jsonb,
             success_criteria = $6::jsonb, evidence_requirements = $7::jsonb,
             failure_conditions = $8::jsonb, minimum_evidence_items = $9,
             completion_risk_class = $10, max_runs = $11,
             max_elapsed_millis = $12::numeric, max_model_tokens = $13::numeric,
             max_tool_invocations = $14::numeric, max_consecutive_failures = $15,
             status = $16, active_pursuit_id = $17, updated_at_ms = $18
         WHERE tenant_id = $19 AND goal_id = $20 AND revision = $21::numeric AND mode = $22
           AND NOT EXISTS(SELECT 1 FROM runtime.goal_deletions deleted WHERE deleted.tenant_id=$19 AND deleted.goal_id=$20)
           AND NOT EXISTS(SELECT 1 FROM agent.dialogue_sessions binding
             WHERE binding.tenant_id=$19 AND binding.source_goal_id=$20
               AND binding.subject_kind='goal' AND binding.agent_id<>$2)",
    )
    .bind(goal.revision.to_string())
    .bind(goal.target_agent_id.into_uuid())
    .bind(&goal.title)
    .bind(goal.objective_ref.content_id.into_uuid())
    .bind(json(&goal.constraints)?)
    .bind(json(&goal.completion.success_criteria)?)
    .bind(json(&goal.completion.evidence_requirements)?)
    .bind(json(&goal.completion.failure_conditions)?)
    .bind(
        i16::try_from(goal.completion.minimum_evidence_items)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
    )
    .bind(risk_class_code(goal.completion.risk_class))
    .bind(i32::try_from(goal.budget.max_runs).map_err(|_| PostgresStandingGoalError::Corrupt)?)
    .bind(goal.budget.max_elapsed_millis.to_string())
    .bind(goal.budget.max_model_tokens.to_string())
    .bind(goal.budget.max_tool_invocations.to_string())
    .bind(
        i32::try_from(goal.budget.max_consecutive_failures)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
    )
    .bind(goal_status_code(goal.status))
    .bind(goal.active_pursuit_id.map(|value| value.into_uuid()))
    .bind(goal.updated_at.unix_millis())
    .bind(goal.tenant_id.into_uuid())
    .bind(goal.goal_id.into_uuid())
    .bind(expected_revision.to_string())
    .bind(mode_code(goal.mode))
    .execute(&mut *transaction)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn insert_pursuit(
    transaction: &mut sqlx::PgConnection,
    pursuit: &GoalPursuit,
) -> Result<u64, PostgresStandingGoalError> {
    let wait = pursuit.wait_barrier.as_ref();
    let result = sqlx::query(
        "INSERT INTO runtime.goal_pursuits (
             pursuit_id, tenant_id, goal_id, revision, status, active_run_id,
             continuation_source_run_id,
             wait_kind, wait_correlation_key, wait_reason_content_id,
             wait_resume_not_before_ms, wait_expires_at_ms, used_runs,
             used_elapsed_millis, used_model_tokens, used_tool_invocations,
             consecutive_failures, continuation_due_at_ms, started_at_ms,
             updated_at_ms, completed_at_ms
         ) VALUES (
             $1, $2, $3, $4::numeric, $5, $6, $7, $8, $9, $10,
             $11, $12, $13, $14::numeric, $15::numeric, $16::numeric,
             $17, $18, $19, $20, $21
         ) ON CONFLICT (tenant_id, pursuit_id) DO NOTHING",
    )
    .bind(pursuit.pursuit_id.into_uuid())
    .bind(pursuit.tenant_id.into_uuid())
    .bind(pursuit.goal_id.into_uuid())
    .bind(pursuit.revision.to_string())
    .bind(pursuit_status_code(pursuit.status))
    .bind(pursuit.active_run_id.map(|value| value.into_uuid()))
    .bind(
        pursuit
            .continuation_source_run_id
            .map(|value| value.into_uuid()),
    )
    .bind(wait.map(|value| wait_kind_code(value.kind)))
    .bind(wait.map(|value| value.correlation_key.clone()))
    .bind(wait.map(|value| value.reason_ref.content_id.into_uuid()))
    .bind(
        wait.and_then(|value| value.resume_not_before)
            .map(UtcTimestamp::unix_millis),
    )
    .bind(
        wait.and_then(|value| value.expires_at)
            .map(UtcTimestamp::unix_millis),
    )
    .bind(i32::try_from(pursuit.budget_usage.runs).map_err(|_| PostgresStandingGoalError::Corrupt)?)
    .bind(pursuit.budget_usage.elapsed_millis.to_string())
    .bind(pursuit.budget_usage.model_tokens.to_string())
    .bind(pursuit.budget_usage.tool_invocations.to_string())
    .bind(
        i32::try_from(pursuit.budget_usage.consecutive_failures)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
    )
    .bind(pursuit.continuation_due_at.map(UtcTimestamp::unix_millis))
    .bind(pursuit.started_at.unix_millis())
    .bind(pursuit.updated_at.unix_millis())
    .bind(pursuit.completed_at.map(UtcTimestamp::unix_millis))
    .execute(&mut *transaction)
    .await?;
    Ok(result.rows_affected())
}

async fn update_pursuit(
    transaction: &mut sqlx::PgConnection,
    pursuit: &GoalPursuit,
    expected_revision: u64,
) -> Result<u64, PostgresStandingGoalError> {
    let wait = pursuit.wait_barrier.as_ref();
    let result = sqlx::query(
        "UPDATE runtime.goal_pursuits SET revision = $1::numeric, status = $2,
             active_run_id = $3, continuation_source_run_id = $4,
             wait_kind = $5, wait_correlation_key = $6,
             wait_reason_content_id = $7, wait_resume_not_before_ms = $8,
             wait_expires_at_ms = $9, used_runs = $10,
             used_elapsed_millis = $11::numeric, used_model_tokens = $12::numeric,
             used_tool_invocations = $13::numeric, consecutive_failures = $14,
             continuation_due_at_ms = $15, claim_token = NULL,
             claim_worker_id = NULL, claim_lease_expires_at_ms = NULL,
             updated_at_ms = $16, completed_at_ms = $17
         WHERE tenant_id = $18 AND pursuit_id = $19 AND revision = $20::numeric",
    )
    .bind(pursuit.revision.to_string())
    .bind(pursuit_status_code(pursuit.status))
    .bind(pursuit.active_run_id.map(|value| value.into_uuid()))
    .bind(
        pursuit
            .continuation_source_run_id
            .map(|value| value.into_uuid()),
    )
    .bind(wait.map(|value| wait_kind_code(value.kind)))
    .bind(wait.map(|value| value.correlation_key.clone()))
    .bind(wait.map(|value| value.reason_ref.content_id.into_uuid()))
    .bind(
        wait.and_then(|value| value.resume_not_before)
            .map(UtcTimestamp::unix_millis),
    )
    .bind(
        wait.and_then(|value| value.expires_at)
            .map(UtcTimestamp::unix_millis),
    )
    .bind(i32::try_from(pursuit.budget_usage.runs).map_err(|_| PostgresStandingGoalError::Corrupt)?)
    .bind(pursuit.budget_usage.elapsed_millis.to_string())
    .bind(pursuit.budget_usage.model_tokens.to_string())
    .bind(pursuit.budget_usage.tool_invocations.to_string())
    .bind(
        i32::try_from(pursuit.budget_usage.consecutive_failures)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
    )
    .bind(pursuit.continuation_due_at.map(UtcTimestamp::unix_millis))
    .bind(pursuit.updated_at.unix_millis())
    .bind(pursuit.completed_at.map(UtcTimestamp::unix_millis))
    .bind(pursuit.tenant_id.into_uuid())
    .bind(pursuit.pursuit_id.into_uuid())
    .bind(expected_revision.to_string())
    .execute(&mut *transaction)
    .await?;
    Ok(result.rows_affected())
}

async fn insert_evaluation(
    transaction: &mut sqlx::PgConnection,
    evaluation: &GoalEvaluation,
) -> Result<(), PostgresStandingGoalError> {
    sqlx::query(
        "INSERT INTO runtime.goal_evaluations (
             evaluation_id, tenant_id, goal_id, pursuit_id, run_id, verdict,
             summary_content_id, deterministic, occurred_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(evaluation.evaluation_id.into_uuid())
    .bind(evaluation.tenant_id.into_uuid())
    .bind(evaluation.goal_id.into_uuid())
    .bind(evaluation.pursuit_id.into_uuid())
    .bind(evaluation.run_id.map(|value| value.into_uuid()))
    .bind(verdict_code(evaluation.verdict))
    .bind(evaluation.summary_ref.content_id.into_uuid())
    .bind(evaluation.deterministic)
    .bind(evaluation.occurred_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    for (ordinal, content) in evaluation.evidence.iter().enumerate() {
        sqlx::query(
            "INSERT INTO runtime.goal_evaluation_evidence (
                 tenant_id, evaluation_id, ordinal, content_id
             ) VALUES ($1, $2, $3, $4)",
        )
        .bind(evaluation.tenant_id.into_uuid())
        .bind(evaluation.evaluation_id.into_uuid())
        .bind(i16::try_from(ordinal).map_err(|_| PostgresStandingGoalError::Corrupt)?)
        .bind(content.content_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    Ok(())
}

async fn insert_proposal_evidence(
    transaction: &mut sqlx::PgConnection,
    proposal: &GoalEvaluationProposal,
) -> Result<(), PostgresStandingGoalError> {
    for (ordinal, content) in proposal.evidence.iter().enumerate() {
        sqlx::query(
            "INSERT INTO runtime.goal_evaluation_proposal_evidence (
                 tenant_id, pursuit_id, run_id, ordinal, content_id
             ) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(proposal.tenant_id.into_uuid())
        .bind(proposal.pursuit_id.into_uuid())
        .bind(proposal.run_id.into_uuid())
        .bind(i16::try_from(ordinal).map_err(|_| PostgresStandingGoalError::Corrupt)?)
        .bind(content.content_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    Ok(())
}

async fn load_proposal_tx(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    pursuit_id: GoalPursuitId,
    run_id: RunId,
) -> Result<Option<GoalEvaluationProposal>, PostgresStandingGoalError> {
    let row = sqlx::query(
        "SELECT proposal.tenant_id, proposal.goal_id, proposal.pursuit_id,
                proposal.run_id, proposal.verdict,
                summary.content_id AS summary_content_id,
                summary.media_type AS summary_media_type,
                summary.byte_length::text AS summary_byte_length,
                summary.content_hash AS summary_hash,
                proposal.wait_kind, proposal.wait_correlation_key,
                wait_reason.content_id AS wait_reason_content_id,
                wait_reason.media_type AS wait_reason_media_type,
                wait_reason.byte_length::text AS wait_reason_byte_length,
                wait_reason.content_hash AS wait_reason_hash,
                proposal.wait_resume_not_before_ms, proposal.wait_expires_at_ms,
                proposal.proposed_at_ms
         FROM runtime.goal_evaluation_proposals AS proposal
         JOIN artifact.content_refs AS summary
           ON summary.tenant_id = proposal.tenant_id
          AND summary.content_id = proposal.summary_content_id
         LEFT JOIN artifact.content_refs AS wait_reason
           ON wait_reason.tenant_id = proposal.tenant_id
          AND wait_reason.content_id = proposal.wait_reason_content_id
         WHERE proposal.tenant_id = $1 AND proposal.pursuit_id = $2
           AND proposal.run_id = $3",
    )
    .bind(tenant_id.into_uuid())
    .bind(pursuit_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let evidence_rows = sqlx::query(
        "SELECT content.content_id, content.media_type,
                content.byte_length::text AS byte_length,
                content.content_hash
         FROM runtime.goal_evaluation_proposal_evidence AS evidence
         JOIN artifact.content_refs AS content
           ON content.tenant_id = evidence.tenant_id
          AND content.content_id = evidence.content_id
         WHERE evidence.tenant_id = $1 AND evidence.pursuit_id = $2
           AND evidence.run_id = $3 ORDER BY evidence.ordinal",
    )
    .bind(tenant_id.into_uuid())
    .bind(pursuit_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?;
    let verdict = decode_verdict(row.try_get("verdict")?)?;
    let wait_barrier = if verdict == GoalVerdict::Waiting {
        Some(GoalWaitBarrier {
            kind: decode_wait_kind(
                row.try_get::<Option<i16>, _>("wait_kind")?
                    .ok_or(PostgresStandingGoalError::Corrupt)?,
            )?,
            correlation_key: row
                .try_get::<Option<String>, _>("wait_correlation_key")?
                .ok_or(PostgresStandingGoalError::Corrupt)?,
            reason_ref: content_ref(&row, "wait_reason")?,
            resume_not_before: row
                .try_get::<Option<i64>, _>("wait_resume_not_before_ms")?
                .map(UtcTimestamp::from_unix_millis),
            expires_at: row
                .try_get::<Option<i64>, _>("wait_expires_at_ms")?
                .map(UtcTimestamp::from_unix_millis),
        })
    } else {
        None
    };
    Ok(Some(GoalEvaluationProposal {
        tenant_id: id(row.try_get("tenant_id")?, "tenant_id")?,
        goal_id: id(row.try_get("goal_id")?, "goal_id")?,
        pursuit_id: id(row.try_get("pursuit_id")?, "pursuit_id")?,
        run_id: id(row.try_get("run_id")?, "run_id")?,
        verdict,
        evidence: evidence_rows
            .iter()
            .map(|item| content_ref(item, ""))
            .collect::<Result<Vec<_>, _>>()?,
        summary_ref: content_ref(&row, "summary")?,
        wait_barrier,
        proposed_at: UtcTimestamp::from_unix_millis(row.try_get("proposed_at_ms")?),
    }))
}

async fn terminal_observation(
    transaction: &mut sqlx::PgConnection,
    run_id: RunId,
    row: &PgRow,
) -> Result<GoalRunTerminalObservation, PostgresStandingGoalError> {
    let status = match row.try_get::<i16, _>("run_status")? {
        5 => GoalRunTerminalStatus::Completed,
        6 => GoalRunTerminalStatus::Failed,
        7 => GoalRunTerminalStatus::Cancelled,
        8 => GoalRunTerminalStatus::Interrupted,
        _ => return Err(PostgresStandingGoalError::Corrupt),
    };
    let elapsed_millis = status
        .execution_elapsed_millis(
            row.try_get("started_at_ms")?,
            row.try_get("finished_at_ms")?,
        )
        .ok_or(PostgresStandingGoalError::Corrupt)?;
    let model_tokens = parse_u64(
        sqlx::query_scalar::<_, String>(
            "SELECT COALESCE(
                 SUM(COALESCE(total_tokens, input_tokens_uncached + output_tokens)), 0
             )::text
             FROM runtime.model_usage_facts WHERE run_id = $1",
        )
        .bind(run_id.into_uuid())
        .fetch_one(&mut *transaction)
        .await?,
        "model_tokens",
    )?;
    let tool_invocations = u64::try_from(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM tool.tool_invocations WHERE run_id = $1",
        )
        .bind(run_id.into_uuid())
        .fetch_one(&mut *transaction)
        .await?,
    )
    .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    Ok(GoalRunTerminalObservation {
        status,
        failure_code: row
            .try_get::<Option<Vec<u8>>, _>("terminal_error")?
            .as_deref()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(musteragent_runtime_contract::ErrorCode::from_safe_code),
        usage: GoalBudgetUsage {
            runs: 0,
            elapsed_millis,
            model_tokens,
            tool_invocations,
            consecutive_failures: u32::from(status != GoalRunTerminalStatus::Completed),
        },
    })
}

async fn load_latest_pursuit(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    goal_id: GoalId,
) -> Result<Option<GoalPursuit>, PostgresStandingGoalError> {
    sqlx::query(&format!(
        "{PURSUIT_SELECT} WHERE pursuit.tenant_id = $1 AND pursuit.goal_id = $2
         ORDER BY pursuit.started_at_ms DESC, pursuit.pursuit_id DESC LIMIT 1"
    ))
    .bind(tenant_id.into_uuid())
    .bind(goal_id.into_uuid())
    .fetch_optional(connection)
    .await?
    .map(|row| decode_pursuit(&row))
    .transpose()
}

pub(crate) async fn load_goal_tx(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    goal_id: GoalId,
) -> Result<Option<StandingGoal>, PostgresStandingGoalError> {
    sqlx::query(&format!(
        "{GOAL_SELECT} WHERE goal.tenant_id = $1 AND goal.goal_id = $2"
    ))
    .bind(tenant_id.into_uuid())
    .bind(goal_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .map(|row| decode_goal(&row))
    .transpose()
}

async fn load_pursuit_tx(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    pursuit_id: GoalPursuitId,
) -> Result<Option<GoalPursuit>, PostgresStandingGoalError> {
    sqlx::query(&format!(
        "{PURSUIT_SELECT} WHERE pursuit.tenant_id = $1 AND pursuit.pursuit_id = $2"
    ))
    .bind(tenant_id.into_uuid())
    .bind(pursuit_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .map(|row| decode_pursuit(&row))
    .transpose()
}

async fn load_continuation_authority_tx(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    pursuit_id: GoalPursuitId,
) -> Result<Option<Vec<u8>>, PostgresStandingGoalError> {
    let row = sqlx::query(
        "SELECT continuation_authority, continuation_authority_hash
         FROM runtime.goal_pursuits WHERE tenant_id = $1 AND pursuit_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(pursuit_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresStandingGoalError::NotFound)?;
    let authority: Option<Vec<u8>> = row.try_get("continuation_authority")?;
    let authority_hash: Option<Vec<u8>> = row.try_get("continuation_authority_hash")?;
    match (authority, authority_hash) {
        (None, None) => Ok(None),
        (Some(authority), Some(hash))
            if !authority.is_empty()
                && authority.len() <= 512 * 1024
                && Sha256::from_bytes(&hash).ok() == Some(Sha256::digest(&authority)) =>
        {
            Ok(Some(authority))
        }
        _ => Err(PostgresStandingGoalError::Corrupt),
    }
}

async fn goal_exists(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    goal_id: GoalId,
) -> Result<bool, PostgresStandingGoalError> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM runtime.standing_goals
         WHERE tenant_id = $1 AND goal_id = $2
           AND NOT EXISTS(SELECT 1 FROM runtime.goal_deletions deleted WHERE deleted.tenant_id=$1 AND deleted.goal_id=$2)",
    )
    .bind(tenant_id.into_uuid())
    .bind(goal_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?
        == 1)
}

async fn mutation_outcome(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    goal_id: GoalId,
    rows: u64,
) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
    if rows == 1 {
        Ok(StandingGoalMutationOutcome::Applied)
    } else if goal_exists(transaction, tenant_id, goal_id).await? {
        Ok(StandingGoalMutationOutcome::Conflict)
    } else {
        Ok(StandingGoalMutationOutcome::NotFound)
    }
}
