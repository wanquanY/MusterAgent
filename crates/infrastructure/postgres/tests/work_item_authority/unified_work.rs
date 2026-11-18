//! The account view must discover original roots, not manufacture task state.
use super::*;
use musteragent_goal::{GoalCompletionContract, GoalRiskClass, ScheduleSpec};
use musteragent_postgres::{
    PostgresAccountStore, PostgresAgentWorkQuery, PostgresDialogueActivityProjection,
    PostgresOwnerScheduleStore, PostgresStandingGoalProductStore,
};
use musteragent_runtime_automation::{StandingGoalMutationOutcome, StandingGoalProductStore};

#[tokio::test]
async fn unified_work_roots_keep_owner_scope_category_and_stable_pagination() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, original) = fixture(&pool).await;
    let projection = PostgresDialogueActivityProjection::new(pool.clone());
    projection.refresh(original.scope.agent_id).await.unwrap();
    let (epoch, seq): (i64, i64) = sqlx::query_as(
        "SELECT sync_epoch,head_event_seq FROM agent.owner_spaces WHERE space_id=$1",
    )
    .bind(original.scope.space_id.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let before = AccountCursor {
        space_id: original.scope.space_id,
        epoch: epoch as u64,
        seq: seq as u64,
    };
    authority.create(original.clone()).await.unwrap();
    let mut child = another(&original, "internal-child");
    child.parent_work_item_id = Some(original.work_item_id());
    authority.create(child).await.unwrap();
    let schedule_store = PostgresOwnerScheduleStore::new(pool.clone());
    let schedule = schedule_store
        .change(ChangeOwnerSchedule {
            actor: original.actor,
            agent_id: original.scope.agent_id,
            command_id: CommandId::generate(),
            schedule_id: uuid::Uuid::now_v7(),
            expected_revision: 0,
            action: OwnerScheduleAction::Create,
            definition: Some(OwnerScheduleDefinition {
                title: "Daily review".into(),
                instructions: "Review only new changes".into(),
                timezone: "Asia/Shanghai".into(),
                timing: ScheduleSpec::Cron {
                    expression: "0 9 * * *".into(),
                },
            }),
            now: original.created_at,
        })
        .await
        .unwrap();
    let scope = OwnerGoalScope {
        tenant_id: original.actor.tenant_id,
        owner_id: original.actor.principal_id,
        space_id: original.scope.space_id,
        agent_id: original.scope.agent_id,
        dialogue_id: original.source_dialogue_id,
        goal_id: GoalId::generate(),
    };
    OwnerGoalCreationApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        authority.content.clone(),
    )
    .create(CreateOwnerGoal {
        actor: original.actor,
        command_id: CommandId::generate(),
        binding: OwnerGoalBinding {
            scope,
            revision: 1,
            requesting_agent_id: scope.agent_id,
            source_dialogue_id: scope.dialogue_id,
        },
        title: "Watch new meeting notes".into(),
        objective: b"Create to-dos for my explicit action items".to_vec(),
        constraints: vec![],
        completion: GoalCompletionContract {
            success_criteria: vec!["Changes processed".into()],
            evidence_requirements: vec!["Original receipts".into()],
            failure_conditions: vec![],
            minimum_evidence_items: 1,
            risk_class: GoalRiskClass::Standard,
        },
        occurred_at: original.created_at,
    })
    .await
    .unwrap();
    let query = PostgresAgentWorkQuery::new(pool.clone());
    let request = ReadAgentWork {
        actor: original.actor,
        agent_id: original.scope.agent_id,
        kind: None,
        category: None,
        include_finished: false,
        limit: 1,
        cursor: None,
        reference: None,
    };
    let first = query.read(request.clone()).await.unwrap();
    assert_eq!(
        first.total_count, 3,
        "internal children never become user commissions"
    );
    assert!(
        matches!(&first.records[0], AgentWorkRecord::Goal { value, pursuit: None } if value.goal_id == scope.goal_id)
    );
    let second = query
        .read(ReadAgentWork {
            cursor: first.next,
            ..request.clone()
        })
        .await
        .unwrap();
    assert!(
        matches!(&second.records[0], AgentWorkRecord::Schedule { value, .. } if value.schedule_id == schedule.schedule_id)
    );
    let third = query
        .read(ReadAgentWork {
            cursor: second.next,
            ..request.clone()
        })
        .await
        .unwrap();
    assert!(third.next.is_none());
    assert!(
        matches!(&third.records[0], AgentWorkRecord::WorkItem(value) if value.core.work_item_id == original.work_item_id())
    );
    for category in [
        AgentWorkCategory::Once,
        AgentWorkCategory::Scheduled,
        AgentWorkCategory::Standing,
    ] {
        let page = query
            .read(ReadAgentWork {
                category: Some(category),
                ..request.clone()
            })
            .await
            .unwrap();
        assert_eq!(page.total_count, 1);
    }
    let mut foreign = request.clone();
    foreign.actor.principal_id = PrincipalId::generate();
    assert!(query.read(foreign).await.is_err());
    assert!(
        query
            .read(ReadAgentWork {
                reference: Some(AgentWorkReference {
                    kind: AgentWorkKind::Goal,
                    id: scope.goal_id.into_uuid(),
                    agent_id: AgentId::generate()
                }),
                ..request.clone()
            })
            .await
            .is_err()
    );
    let pause = ChangeOwnerSchedule {
        actor: original.actor,
        agent_id: scope.agent_id,
        command_id: CommandId::generate(),
        schedule_id: schedule.schedule_id,
        expected_revision: schedule.revision,
        action: OwnerScheduleAction::Pause,
        definition: None,
        now: UtcTimestamp::from_unix_millis(400),
    };
    let paused = schedule_store.change(pause.clone()).await.unwrap();
    let read = query
        .read(ReadAgentWork {
            reference: Some(AgentWorkReference {
                kind: AgentWorkKind::Schedule,
                id: paused.schedule_id,
                agent_id: scope.agent_id,
            }),
            ..request
        })
        .await
        .unwrap();
    assert!(
        matches!(&read.records[0], AgentWorkRecord::Schedule { value, .. } if value.revision == paused.revision && value.status == "paused")
    );
    let tail = PostgresAccountStore::new(pool.clone());
    projection.refresh(scope.agent_id).await.unwrap();
    let page = tail
        .read_changes(original.actor, before, 100)
        .await
        .unwrap();
    let changes: Vec<_> = page
        .changes
        .iter()
        .filter_map(|event| match event.change {
            AccountChange::WorkChanged {
                agent_id,
                reference,
            } => Some((agent_id, reference)),
            _ => None,
        })
        .collect();
    assert_eq!(
        changes,
        vec![(scope.agent_id, None)],
        "one coalesced invalidation; original owners remain authoritative"
    );
    let replay = schedule_store.change(pause).await.unwrap();
    assert_eq!(
        (replay.schedule_id, replay.revision, replay.status),
        (paused.schedule_id, paused.revision, paused.status)
    );
    projection.refresh(scope.agent_id).await.unwrap();
    assert!(
        tail.read_changes(original.actor, page.head, 100)
            .await
            .unwrap()
            .changes
            .is_empty(),
        "command replay cannot append another account event"
    );
    // An internal child is absent from the user task view and must not generate
    // work invalidations on its own.
    let mut another_child = another(&original, "another-hidden-child");
    another_child.parent_work_item_id = Some(original.work_item_id());
    authority.create(another_child).await.unwrap();
    projection.refresh(scope.agent_id).await.unwrap();
    assert!(
        !tail
            .read_changes(original.actor, page.head, 100)
            .await
            .unwrap()
            .changes
            .iter()
            .any(|event| matches!(event.change, AccountChange::WorkChanged { .. }))
    );
    // Editing a waiting Goal does not change aggregate Agent activity. Its
    // original owner's mutation must still reach another device's task view.
    let AgentWorkRecord::Goal {
        value: mut changed_goal,
        ..
    } = first.records[0].clone()
    else {
        panic!("goal root")
    };
    let goal_revision = changed_goal.revision;
    changed_goal.revision += 1;
    changed_goal.title = "Watch only my meeting action items".into();
    let goals = PostgresStandingGoalProductStore::new(pool.clone());
    assert_eq!(
        goals
            .replace_goal(changed_goal, goal_revision)
            .await
            .unwrap(),
        StandingGoalMutationOutcome::Applied
    );
    projection.refresh(scope.agent_id).await.unwrap();
    let after_goal = tail
        .read_changes(original.actor, page.head, 100)
        .await
        .unwrap();
    assert_eq!(
        after_goal
            .changes
            .iter()
            .filter(|event| matches!(event.change, AccountChange::WorkChanged { .. }))
            .count(),
        1
    );
    assert!(
        !after_goal
            .changes
            .iter()
            .any(|event| matches!(event.change, AccountChange::ActivityChanged(_))),
        "task facts cannot depend on aggregate activity changing"
    );
    projection.refresh(scope.agent_id).await.unwrap();
    assert!(
        tail.read_changes(original.actor, after_goal.head, 100)
            .await
            .unwrap()
            .changes
            .is_empty(),
        "projection restart must not replay consumed dirty hints"
    );
    let mut foreign_actor = original.actor;
    foreign_actor.principal_id = PrincipalId::generate();
    assert!(tail.read_changes(foreign_actor, before, 100).await.is_err());
}
