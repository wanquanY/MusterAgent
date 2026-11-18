//! Exercise the scheduled application path without a database or product host.
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_goal::*;
use musteragent_identity::*;
use musteragent_runtime_automation::*;
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Store(Mutex<BTreeMap<(TenantId, GoalId), Goal>>);
impl GoalProductStore for Store {
    fn create(
        &self,
        goal: Goal,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>> {
        Box::pin(async move {
            let mut rows = self.0.lock().expect("fixture mutex is not poisoned");
            match rows.entry((goal.tenant_id, goal.goal_id)) {
                std::collections::btree_map::Entry::Occupied(_) => {
                    Ok(GoalMutationOutcome::Conflict)
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(goal);
                    Ok(GoalMutationOutcome::Applied)
                }
            }
        })
    }
    fn replace(
        &self,
        goal: Goal,
        revision: u64,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>> {
        Box::pin(async move {
            let mut rows = self.0.lock().expect("fixture mutex is not poisoned");
            match rows.get_mut(&(goal.tenant_id, goal.goal_id)) {
                Some(row) if row.revision == revision => {
                    *row = goal;
                    Ok(GoalMutationOutcome::Applied)
                }
                Some(_) => Ok(GoalMutationOutcome::Conflict),
                None => Ok(GoalMutationOutcome::NotFound),
            }
        })
    }
    fn load(
        &self,
        tenant: TenantId,
        id: GoalId,
    ) -> ApplicationFuture<'_, Result<Option<Goal>, GoalProductStoreError>> {
        Box::pin(async move {
            Ok(self
                .0
                .lock()
                .expect("fixture mutex is not poisoned")
                .get(&(tenant, id))
                .cloned())
        })
    }
    fn list(
        &self,
        tenant: TenantId,
    ) -> ApplicationFuture<'_, Result<GoalListPage, GoalProductStoreError>> {
        Box::pin(async move {
            let goals: Vec<_> = self
                .0
                .lock()
                .expect("valid goal fixture")
                .values()
                .filter(|goal| goal.tenant_id == tenant)
                .cloned()
                .collect();
            Ok(GoalListPage {
                next_due_at: goals.iter().filter_map(|goal| goal.next_run_at).min(),
                goals,
                latest_occurrences: BTreeMap::new(),
            })
        })
    }
    fn delete(
        &self,
        request: DeleteGoal,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>> {
        Box::pin(async move {
            let mut rows = self.0.lock().expect("fixture mutex is not poisoned");
            let key = (request.tenant_id, request.goal_id);
            match rows.get(&key) {
                Some(row) if row.revision == request.expected_revision => {
                    rows.remove(&key);
                    Ok(GoalMutationOutcome::Applied)
                }
                Some(_) => Ok(GoalMutationOutcome::Conflict),
                None => Ok(GoalMutationOutcome::NotFound),
            }
        })
    }
    fn enqueue_now(
        &self,
        _: EnqueueGoalNow,
    ) -> ApplicationFuture<'_, Result<Option<ScheduleOccurrence>, GoalProductStoreError>> {
        Box::pin(async { Err(GoalProductStoreError::Unavailable) })
    }
    fn claim_due(
        &self,
        _: ClaimDueGoals,
    ) -> ApplicationFuture<'_, Result<Vec<StoredGoalClaim>, GoalProductStoreError>> {
        Box::pin(async { Err(GoalProductStoreError::Unavailable) })
    }
    fn acknowledge_admission(
        &self,
        _: AcknowledgeOccurrenceAdmission,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>> {
        Box::pin(async { Err(GoalProductStoreError::Unavailable) })
    }
    fn fail_claim(
        &self,
        _: FailOccurrenceClaim,
    ) -> ApplicationFuture<'_, Result<GoalMutationOutcome, GoalProductStoreError>> {
        Box::pin(async { Err(GoalProductStoreError::Unavailable) })
    }
}

#[derive(Default)]
struct Content(Mutex<BTreeMap<ContentId, (TenantId, Vec<u8>)>>);
impl ContentStore for Content {
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            let reference = ContentRef {
                content_id: ContentId::generate(),
                media_type: request.media_type,
                sha256: Sha256::digest(&request.bytes),
                byte_length: request.bytes.len() as u64,
            };
            self.0
                .lock()
                .expect("valid goal fixture")
                .insert(reference.content_id, (request.tenant_id, request.bytes));
            Ok(reference)
        })
    }
    fn read<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            let rows = self.0.lock().expect("fixture mutex is not poisoned");
            let (owner, bytes) = rows
                .get(&reference.content_id)
                .ok_or(ContentStoreError::NotFound)?;
            if *owner != tenant {
                return Err(ContentStoreError::Unauthorized);
            }
            musteragent_runtime_content::verify_content(reference, bytes)?;
            Ok(bytes.clone())
        })
    }
}

fn request(schedule: ScheduleSpec) -> CreateGoal {
    CreateGoal {
        tenant_id: TenantId::generate(),
        goal_id: GoalId::generate(),
        principal_id: PrincipalId::generate(),
        activate: true,
        occurred_at: UtcTimestamp::from_unix_millis(1_000),
        definition: GoalDefinition {
            target_agent: AgentId::generate(),
            name: "Inspect inputs".into(),
            description: String::new(),
            objective: b"Inspect newly accepted data and record the result.".to_vec(),
            constraints: vec![],
            success_metrics: vec![],
            priority: GoalPriority::Normal,
            schedule,
            execution: ExecutionSpec::AgentTurn(AgentTurnExecution {
                requested_model_class: "default".into(),
                workspace_id: None,
                workdir: None,
                conversation_id: None,
                session_target: SessionTarget::Isolated,
                approval_policy: UnattendedApprovalPolicy::Deny,
            }),
            concurrency_policy: ConcurrencyPolicy::Skip,
            lateness_policy: LatenessPolicy::CatchUp,
            lateness_window_millis: 60_000,
            retry: RetrySpec::default(),
            timezone: "UTC".into(),
            delete_after_run: false,
        },
    }
}

#[tokio::test]
async fn interval_goal_keeps_objective_ownership_revision_and_resume_schedule() {
    let app = GoalProductApplication::new(Arc::new(Store::default()), Arc::new(Content::default()));
    let request = request(ScheduleSpec::Interval {
        every_millis: 60_000,
        anchor: UtcTimestamp::from_unix_millis(0),
    });
    let goal = app
        .create(request.clone())
        .await
        .expect("valid goal fixture");
    assert_eq!(
        goal.next_run_at,
        Some(UtcTimestamp::from_unix_millis(60_000))
    );
    assert_eq!(
        app.inspect(goal.tenant_id, goal.goal_id)
            .await
            .expect("valid goal fixture")
            .objective,
        request.definition.objective
    );
    let pause = ChangeGoalStatus {
        tenant_id: goal.tenant_id,
        goal_id: goal.goal_id,
        principal_id: goal.owner,
        expected_revision: 1,
        status: GoalStatus::Paused,
        occurred_at: UtcTimestamp::from_unix_millis(2_000),
    };
    assert_eq!(
        app.change_status(ChangeGoalStatus {
            principal_id: PrincipalId::generate(),
            ..pause
        })
        .await,
        Err(GoalProductError::Unauthorized)
    );
    let paused = app.change_status(pause).await.expect("valid goal fixture");
    assert_eq!(paused.next_run_at, None);
    assert_eq!(paused.revision, 2);
    assert_eq!(
        app.change_status(pause).await,
        Err(GoalProductError::Conflict)
    );
    let resumed = app
        .change_status(ChangeGoalStatus {
            expected_revision: 2,
            status: GoalStatus::Active,
            occurred_at: UtcTimestamp::from_unix_millis(61_000),
            ..pause
        })
        .await
        .expect("valid goal fixture");
    assert_eq!(
        resumed.next_run_at,
        Some(UtcTimestamp::from_unix_millis(120_000))
    );
    assert_eq!(resumed.objective_ref, goal.objective_ref);
}

#[tokio::test]
async fn elapsed_schedule_and_unavailable_dispatch_are_explicit_errors() {
    let store = Arc::new(Store::default());
    let app = GoalProductApplication::new(store.clone(), Arc::new(Content::default()));
    assert_eq!(
        app.create(request(ScheduleSpec::Once {
            at: UtcTimestamp::from_unix_millis(0)
        }))
        .await,
        Err(GoalProductError::ScheduleElapsed)
    );
    assert!(
        store
            .0
            .lock()
            .expect("fixture mutex is not poisoned")
            .is_empty()
    );
    let goal = app
        .create(request(ScheduleSpec::Once {
            at: UtcTimestamp::from_unix_millis(5_000),
        }))
        .await
        .expect("valid goal fixture");
    assert_eq!(
        goal.next_run_at,
        Some(UtcTimestamp::from_unix_millis(5_000))
    );
    assert_eq!(
        app.enqueue_now(EnqueueGoalNow {
            tenant_id: goal.tenant_id,
            goal_id: goal.goal_id,
            principal_id: goal.owner,
            occurred_at: UtcTimestamp::from_unix_millis(2_000)
        })
        .await,
        Err(GoalProductError::Unavailable)
    );
}
