use musteragent_goal::{GoalOrigin, StandingGoalStatus};
use musteragent_identity::{GoalId, GoalPursuitId, PrincipalId, TenantId, UtcTimestamp};
use musteragent_runtime_automation::{
    ChangeStandingGoalStatus, CreateStandingGoal, ReplaceStandingGoal, StandingGoalDefinition,
    StandingGoalProductApplication, StandingGoalProductError,
};

pub async fn draft_cancellation(
    app: &StandingGoalProductApplication,
    tenant_id: TenantId,
    principal_id: PrincipalId,
    definition: StandingGoalDefinition,
) {
    let goal_id = GoalId::generate();
    let create = CreateStandingGoal {
        tenant_id,
        goal_id,
        pursuit_id: GoalPursuitId::generate(),
        principal_id,
        origin: GoalOrigin::Agent,
        creator_agent_id: Some(definition.target_agent_id),
        definition: definition.clone(),
        activate: false,
        initial_run_id: None,
        occurred_at: UtcTimestamp::from_unix_millis(100),
    };
    app.create(create.clone())
        .await
        .expect("create draft without execution");
    app.replace(ReplaceStandingGoal {
        tenant_id,
        goal_id,
        principal_id,
        expected_revision: 1,
        definition,
        occurred_at: UtcTimestamp::from_unix_millis(110),
    })
    .await
    .expect("update draft as in R4");
    let cancel = ChangeStandingGoalStatus {
        tenant_id,
        goal_id,
        principal_id,
        expected_goal_revision: 2,
        expected_pursuit_revision: None,
        new_pursuit_id: None,
        initial_run_id: None,
        initial_message_id: None,
        status: StandingGoalStatus::Cancelled,
        occurred_at: UtcTimestamp::from_unix_millis(120),
    };
    assert!(matches!(
        app.change_status(ChangeStandingGoalStatus {
            expected_pursuit_revision: Some(1),
            ..cancel
        })
        .await,
        Err(StandingGoalProductError::InvalidRequest)
    ));
    let unchanged = app
        .inspect_owned(tenant_id, goal_id, principal_id)
        .await
        .expect("draft cancellation must succeed");
    assert_eq!(unchanged.goal.status, StandingGoalStatus::Draft);
    assert_eq!(unchanged.goal.revision, 2);
    assert!(unchanged.pursuit.is_none());
    let cancelled = app
        .change_status(cancel)
        .await
        .expect("draft cancellation has no pursuit prerequisite");
    assert_eq!(cancelled.goal.status, StandingGoalStatus::Cancelled);
    assert_eq!(cancelled.goal.revision, 3);
    assert!(cancelled.pursuit.is_none());
    assert_eq!(
        app.inspect_owned(tenant_id, goal_id, principal_id)
            .await
            .expect("draft cancellation must succeed"),
        cancelled
    );
    assert!(matches!(
        app.change_status(cancel).await,
        Err(StandingGoalProductError::Conflict)
    ));
    let again = app
        .change_status(ChangeStandingGoalStatus {
            expected_goal_revision: 3,
            ..cancel
        })
        .await
        .expect("same state is a no-op");
    assert_eq!(again, cancelled);
    app.create(CreateStandingGoal {
        goal_id: GoalId::generate(),
        ..create
    })
    .await
    .expect("cancelled draft releases conversation lane");
}
