//! Runtime for durable Goals and schedule occurrences.
//!
//! The application owns objective content and state transitions. Persistence
//! adapters own transactional claiming, but cannot invent schedule semantics.

#![forbid(unsafe_code)]

mod admission;
mod goal_identity;
mod goal_start;
pub use admission::*;
pub use goal_identity::*;
pub use goal_start::*;
/// Derive the same Run identity used by ordinary durable admission.
pub fn derive_admission_run_id(
    command_id: musteragent_identity::CommandId,
) -> Result<musteragent_identity::RunId, musteragent_runtime_contract::ErrorCode> {
    musteragent_runtime_admission::derive_admission_run_id(command_id)
        .ok_or(musteragent_runtime_contract::ErrorCode::InternalInvariantViolated)
}

mod evaluation_agent;
mod judge;
mod standing;
mod supervisor;

pub use evaluation_agent::*;
pub use judge::*;
pub use standing::*;
pub use supervisor::*;

use std::collections::BTreeMap;
use std::sync::Arc;

use musteragent_content_ref::MediaType;
use musteragent_goal::{
    ConcurrencyPolicy, ExecutionSpec, Goal, GoalPriority, GoalStatus, LatenessPolicy, RetrySpec,
    ScheduleOccurrence, ScheduleSpec, initial_next_run,
};
use musteragent_identity::{
    AgentId, GoalId, PrincipalId, RunId, ScheduleOccurrenceId, TenantId, UtcTimestamp,
};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalDefinition {
    pub target_agent: AgentId,
    pub name: String,
    pub description: String,
    pub objective: Vec<u8>,
    pub constraints: Vec<String>,
    pub success_metrics: Vec<String>,
    pub priority: GoalPriority,
    pub schedule: ScheduleSpec,
    pub execution: ExecutionSpec,
    pub concurrency_policy: ConcurrencyPolicy,
    pub lateness_policy: LatenessPolicy,
    pub lateness_window_millis: u64,
    pub retry: RetrySpec,
    pub timezone: String,
    pub delete_after_run: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateGoal {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub definition: GoalDefinition,
    pub activate: bool,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplaceGoal {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub expected_revision: u64,
    pub definition: GoalDefinition,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ChangeGoalStatus {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub expected_revision: u64,
    pub status: GoalStatus,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DeleteGoal {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimDueGoals {
    pub tenant_id: TenantId,
    pub worker_id: String,
    pub now: UtcTimestamp,
    pub lease_millis: u64,
    pub limit: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimedGoalOccurrence {
    pub goal: Goal,
    pub occurrence: ScheduleOccurrence,
    pub objective: Vec<u8>,
    /// Existing non-terminal Run that must be cancelled before admitting this
    /// occurrence when the Goal uses `Replace` concurrency.
    pub replace_run: Option<ReplaceRunTarget>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ReplaceRunTarget {
    pub run_id: RunId,
    pub conversation_id: musteragent_identity::ConversationId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StoredGoalClaim {
    pub goal: Goal,
    pub occurrence: ScheduleOccurrence,
    pub replace_run: Option<ReplaceRunTarget>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AcknowledgeOccurrenceAdmission {
    pub tenant_id: TenantId,
    pub occurrence_id: ScheduleOccurrenceId,
    pub claim_token: String,
    pub conversation_id: musteragent_identity::ConversationId,
    pub run_id: RunId,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FailOccurrenceClaim {
    pub tenant_id: TenantId,
    pub occurrence_id: ScheduleOccurrenceId,
    pub claim_token: String,
    pub error: String,
    pub retry_at: Option<UtcTimestamp>,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GoalMutationOutcome {
    Applied,
    Conflict,
    NotFound,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalListPage {
    pub goals: Vec<Goal>,
    pub latest_occurrences: BTreeMap<GoalId, ScheduleOccurrence>,
    pub next_due_at: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalInspection {
    pub goal: Goal,
    pub objective: Vec<u8>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct EnqueueGoalNow {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub occurred_at: UtcTimestamp,
}

pub trait GoalProductStore: Send + Sync {
    fn create(
        &self,
        goal: Goal,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>>;

    fn replace(
        &self,
        goal: Goal,
        expected_revision: u64,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>>;

    fn load(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
    ) -> ApplicationFuture<'_, Result<Option<Goal>, GoalProductStoreError>>;

    fn list(
        &self,
        tenant_id: TenantId,
    ) -> ApplicationFuture<'_, Result<GoalListPage, GoalProductStoreError>>;

    fn delete(
        &self,
        request: DeleteGoal,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>>;

    fn enqueue_now(
        &self,
        request: EnqueueGoalNow,
    ) -> ApplicationFuture<'_, Result<Option<ScheduleOccurrence>, GoalProductStoreError>>;

    /// Materializes due occurrences, advances Goal cursors and takes claims in
    /// one transaction. A crash may expire a lease, but can never duplicate a
    /// trigger identity.
    fn claim_due(
        &self,
        request: ClaimDueGoals,
    ) -> ApplicationFuture<'_, Result<Vec<StoredGoalClaim>, GoalProductStoreError>>;

    fn acknowledge_admission(
        &self,
        request: AcknowledgeOccurrenceAdmission,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>>;

    fn fail_claim(
        &self,
        request: FailOccurrenceClaim,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>>;
}

pub struct GoalProductApplication {
    store: Arc<dyn GoalProductStore>,
    content: Arc<dyn ContentStore>,
}

impl GoalProductApplication {
    #[must_use]
    pub fn new(store: Arc<dyn GoalProductStore>, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn create(&self, request: CreateGoal) -> Result<Goal, GoalProductError> {
        let objective_ref = self
            .put_objective(request.tenant_id, &request.definition)
            .await?;
        let status = if request.activate {
            GoalStatus::Active
        } else {
            GoalStatus::Draft
        };
        let next_run_at = if request.activate {
            initial_next_run(
                &request.definition.schedule,
                &request.definition.timezone,
                request.occurred_at,
            )?
        } else {
            None
        };
        if request.activate && next_run_at.is_none() {
            return Err(GoalProductError::ScheduleElapsed);
        }
        let goal = goal_from_definition(
            request.tenant_id,
            request.goal_id,
            request.principal_id,
            1,
            request.definition,
            objective_ref,
            status,
            next_run_at,
            request.occurred_at,
            request.occurred_at,
            None,
        );
        goal.validate()?;
        match self.store.create(goal.clone()).await? {
            GoalMutationOutcome::Applied => Ok(goal),
            GoalMutationOutcome::Conflict => Err(GoalProductError::Conflict),
            GoalMutationOutcome::NotFound => Err(GoalProductError::Corrupt),
        }
    }

    pub async fn replace(&self, request: ReplaceGoal) -> Result<Goal, GoalProductError> {
        let current = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        if current.revision != request.expected_revision {
            return Err(GoalProductError::Conflict);
        }
        let objective_ref = self
            .put_objective(request.tenant_id, &request.definition)
            .await?;
        let next_run_at = if current.status == GoalStatus::Active {
            initial_next_run(
                &request.definition.schedule,
                &request.definition.timezone,
                request.occurred_at,
            )?
        } else {
            None
        };
        if current.status == GoalStatus::Active && next_run_at.is_none() {
            return Err(GoalProductError::ScheduleElapsed);
        }
        let replacement = goal_from_definition(
            request.tenant_id,
            request.goal_id,
            request.principal_id,
            current
                .revision
                .checked_add(1)
                .ok_or(GoalProductError::Corrupt)?,
            request.definition,
            objective_ref,
            current.status,
            next_run_at,
            current.created_at,
            request.occurred_at,
            current.last_run_at,
        );
        replacement.validate()?;
        mutation(
            self.store
                .replace(replacement.clone(), current.revision)
                .await?,
        )?;
        Ok(replacement)
    }

    pub async fn change_status(&self, request: ChangeGoalStatus) -> Result<Goal, GoalProductError> {
        if matches!(request.status, GoalStatus::Draft) {
            return Err(GoalProductError::InvalidRequest);
        }
        let mut goal = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        if goal.revision != request.expected_revision {
            return Err(GoalProductError::Conflict);
        }
        goal.status = request.status;
        goal.next_run_at = if request.status == GoalStatus::Active {
            initial_next_run(&goal.schedule, &goal.timezone, request.occurred_at)?
        } else {
            None
        };
        if request.status == GoalStatus::Active && goal.next_run_at.is_none() {
            return Err(GoalProductError::ScheduleElapsed);
        }
        goal.revision = goal
            .revision
            .checked_add(1)
            .ok_or(GoalProductError::Corrupt)?;
        goal.updated_at = request.occurred_at;
        goal.validate()?;
        mutation(
            self.store
                .replace(goal.clone(), request.expected_revision)
                .await?,
        )?;
        Ok(goal)
    }

    pub async fn delete(&self, request: DeleteGoal) -> Result<(), GoalProductError> {
        let goal = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        if goal.revision != request.expected_revision {
            return Err(GoalProductError::Conflict);
        }
        mutation(self.store.delete(request).await?)
    }

    pub async fn list(&self, tenant_id: TenantId) -> Result<GoalListPage, GoalProductError> {
        Ok(self.store.list(tenant_id).await?)
    }

    pub async fn inspect(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
    ) -> Result<GoalInspection, GoalProductError> {
        let goal = self
            .store
            .load(tenant_id, goal_id)
            .await?
            .ok_or(GoalProductError::NotFound)?;
        let objective = self.content.read(tenant_id, &goal.objective_ref).await?;
        Ok(GoalInspection { goal, objective })
    }

    pub async fn enqueue_now(
        &self,
        request: EnqueueGoalNow,
    ) -> Result<ScheduleOccurrence, GoalProductError> {
        let goal = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        if matches!(
            goal.status,
            GoalStatus::Achieved | GoalStatus::Failed | GoalStatus::Archived
        ) {
            return Err(GoalProductError::InvalidRequest);
        }
        self.store
            .enqueue_now(request)
            .await?
            .ok_or(GoalProductError::NotFound)
    }

    pub async fn claim_due(
        &self,
        request: ClaimDueGoals,
    ) -> Result<Vec<ClaimedGoalOccurrence>, GoalProductError> {
        if request.worker_id.is_empty()
            || request.worker_id.len() > 255
            || request.lease_millis < 1_000
            || request.lease_millis > 10 * 60_000
            || request.limit == 0
            || request.limit > 100
        {
            return Err(GoalProductError::InvalidRequest);
        }
        let claimed = self.store.claim_due(request).await?;
        let mut result = Vec::with_capacity(claimed.len());
        for claim in claimed {
            let StoredGoalClaim {
                goal,
                occurrence,
                replace_run,
            } = claim;
            let objective = self
                .content
                .read(goal.tenant_id, &goal.objective_ref)
                .await?;
            result.push(ClaimedGoalOccurrence {
                goal,
                occurrence,
                objective,
                replace_run,
            });
        }
        Ok(result)
    }

    pub async fn acknowledge_admission(
        &self,
        request: AcknowledgeOccurrenceAdmission,
    ) -> Result<(), GoalProductError> {
        mutation(self.store.acknowledge_admission(request).await?)
    }

    pub async fn fail_claim(&self, request: FailOccurrenceClaim) -> Result<(), GoalProductError> {
        if request.error.is_empty() || request.error.len() > 4_096 {
            return Err(GoalProductError::InvalidRequest);
        }
        mutation(self.store.fail_claim(request).await?)
    }

    async fn load_owned(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        principal_id: PrincipalId,
    ) -> Result<Goal, GoalProductError> {
        let goal = self
            .store
            .load(tenant_id, goal_id)
            .await?
            .ok_or(GoalProductError::NotFound)?;
        if goal.owner != principal_id {
            return Err(GoalProductError::Unauthorized);
        }
        Ok(goal)
    }

    async fn put_objective(
        &self,
        tenant_id: TenantId,
        definition: &GoalDefinition,
    ) -> Result<musteragent_content_ref::ContentRef, GoalProductError> {
        if definition.objective.is_empty() || definition.objective.len() > 4 * 1024 * 1024 {
            return Err(GoalProductError::InvalidRequest);
        }
        Ok(self
            .content
            .put(PutContent {
                tenant_id,
                media_type: MediaType::new("text/markdown")
                    .map_err(|_| GoalProductError::Corrupt)?,
                bytes: definition.objective.clone(),
            })
            .await?)
    }
}

#[allow(clippy::too_many_arguments)]
fn goal_from_definition(
    tenant_id: TenantId,
    goal_id: GoalId,
    owner: PrincipalId,
    revision: u64,
    definition: GoalDefinition,
    objective_ref: musteragent_content_ref::ContentRef,
    status: GoalStatus,
    next_run_at: Option<UtcTimestamp>,
    created_at: UtcTimestamp,
    updated_at: UtcTimestamp,
    last_run_at: Option<UtcTimestamp>,
) -> Goal {
    Goal {
        tenant_id,
        goal_id,
        revision,
        owner,
        target_agent: definition.target_agent,
        name: definition.name,
        description: definition.description,
        objective_ref,
        constraints: definition.constraints,
        success_metrics: definition.success_metrics,
        priority: definition.priority,
        status,
        schedule: definition.schedule,
        execution: definition.execution,
        concurrency_policy: definition.concurrency_policy,
        lateness_policy: definition.lateness_policy,
        lateness_window_millis: definition.lateness_window_millis,
        retry: definition.retry,
        timezone: definition.timezone,
        delete_after_run: definition.delete_after_run,
        last_run_at,
        next_run_at,
        created_at,
        updated_at,
    }
}

fn mutation(outcome: GoalMutationOutcome) -> Result<(), GoalProductError> {
    match outcome {
        GoalMutationOutcome::Applied => Ok(()),
        GoalMutationOutcome::Conflict => Err(GoalProductError::Conflict),
        GoalMutationOutcome::NotFound => Err(GoalProductError::NotFound),
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum GoalProductStoreError {
    #[error("Goal product store is unavailable")]
    Unavailable,
    #[error("Goal product store returned corrupt state")]
    Corrupt,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum GoalProductError {
    #[error("Goal product request is invalid")]
    InvalidRequest,
    #[error("Goal was not found")]
    NotFound,
    #[error("Goal belongs to another principal")]
    Unauthorized,
    #[error("Goal state changed concurrently")]
    Conflict,
    #[error("Goal schedule has no future occurrence")]
    ScheduleElapsed,
    #[error("Goal product dependency is unavailable")]
    Unavailable,
    #[error("Goal product state is corrupt")]
    Corrupt,
}

impl From<musteragent_goal::GoalContractError> for GoalProductError {
    fn from(_: musteragent_goal::GoalContractError) -> Self {
        Self::InvalidRequest
    }
}

impl From<ContentStoreError> for GoalProductError {
    fn from(error: ContentStoreError) -> Self {
        match error {
            ContentStoreError::NotFound
            | ContentStoreError::LengthMismatch
            | ContentStoreError::HashMismatch
            | ContentStoreError::IdentityConflict
            | ContentStoreError::Invariant => Self::Corrupt,
            ContentStoreError::Unauthorized => Self::Unauthorized,
            ContentStoreError::TooLarge => Self::InvalidRequest,
            ContentStoreError::Unavailable => Self::Unavailable,
        }
    }
}

impl From<GoalProductStoreError> for GoalProductError {
    fn from(error: GoalProductStoreError) -> Self {
        match error {
            GoalProductStoreError::Unavailable => Self::Unavailable,
            GoalProductStoreError::Corrupt => Self::Corrupt,
        }
    }
}

#[cfg(test)]
mod test_error;
#[cfg(test)]
use test_error::runtime_error;

mod owner_goal_tools;
pub use owner_goal_tools::*;

mod owner_schedule_tools;
pub use owner_schedule_tools::*;

pub mod owner_event_sources;

mod goal_binding;

mod scheduled;
pub use scheduled::*;

/// Deterministic schedule policy decisions shared by dispatch adapters.
pub use musteragent_schedule as schedule_policy;
mod delegation;
pub use delegation::*;
