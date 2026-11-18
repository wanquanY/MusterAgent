//! PostgreSQL Goal input transaction/claim conformance. Provider and terminal
//! Run observations are fixtures, not external event or model acceptance.
use super::*;
use musteragent_goal::StandingGoalMode;
use musteragent_runtime_automation::{AcceptStandingGoalInput, StandingGoalInputOutcome};

#[tokio::test]
async fn concurrent_input_duplicates_and_pre_wait_arrival_have_one_durable_obligation() {
    input_cycle(false, false).await;
}

#[tokio::test]
async fn paused_continuous_goal_preserves_running_settlement_and_accepted_input_wake() {
    input_cycle(true, false).await;
}

#[tokio::test]
async fn business_rule_edits_split_pending_inputs_and_reclaim_preserves_the_original_version() {
    input_cycle(false, true).await;
}

async fn input_cycle(paused: bool, edited: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("input cycle must succeed");
    let pool = postgres.pool().await.expect("input cycle must succeed");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("input cycle must succeed");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    let application = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        content.clone(),
    );
    let tenant = TenantId::generate();
    let owner = PrincipalId::generate();
    let conversation = ConversationId::generate();
    let agent = AgentId::generate();
    let run = RunId::generate();
    seed_source_run(
        &pool,
        tenant,
        owner,
        conversation,
        ActivityId::generate(),
        run,
        RuntimeSnapshotId::generate(),
        ParticipantId::generate(),
        ParticipantId::generate(),
    )
    .await;
    let goal = GoalId::generate();
    let pursuit = GoalPursuitId::generate();
    let mut definition = definition(agent, conversation, 1);
    definition.mode = StandingGoalMode::Continuous;
    application
        .create(CreateStandingGoal {
            tenant_id: tenant,
            goal_id: goal,
            pursuit_id: pursuit,
            principal_id: owner,
            origin: GoalOrigin::Agent,
            creator_agent_id: Some(agent),
            definition,
            activate: true,
            initial_run_id: Some(run),
            occurred_at: UtcTimestamp::from_unix_millis(100),
        })
        .await
        .expect("input cycle must succeed");
    let evidence = content
        .put(PutContent {
            tenant_id: tenant,
            media_type: MediaType::new("text/plain").expect("input cycle must succeed"),
            bytes: b"source revision 1".to_vec(),
        })
        .await
        .expect("input cycle must succeed");
    let request = AcceptStandingGoalInput {
        tenant_id: tenant,
        goal_id: goal,
        principal_id: owner,
        expected_goal_revision: 1,
        input_key: "source:resource:revision-1".into(),
        content: evidence.clone(),
    };
    let (left, right) = tokio::join!(
        application.accept_authorized_input(request.clone()),
        application.accept_authorized_input(request.clone())
    );
    let replies = [
        left.expect("input cycle must succeed"),
        right.expect("input cycle must succeed"),
    ];
    assert!(replies.contains(&StandingGoalInputOutcome::Accepted { sequence: 1 }));
    assert!(replies.contains(&StandingGoalInputOutcome::Existing { sequence: 1 }));
    let erased = content
        .put(PutContent {
            tenant_id: tenant,
            media_type: MediaType::new("text/plain").expect("input cycle must succeed"),
            bytes: b"withdrawn source".to_vec(),
        })
        .await
        .expect("input cycle must succeed");
    sqlx::query("UPDATE artifact.content_refs SET state=3 WHERE tenant_id=$1 AND content_id=$2")
        .bind(tenant.into_uuid())
        .bind(erased.content_id.into_uuid())
        .execute(&pool)
        .await
        .expect("input cycle must succeed");
    assert_eq!(
        application
            .accept_authorized_input(AcceptStandingGoalInput {
                input_key: "source:withdrawn".into(),
                content: erased,
                ..request.clone()
            })
            .await
            .expect("input cycle must succeed"),
        StandingGoalInputOutcome::Rejected,
        "metadata alone cannot admit withdrawn bytes"
    );
    if paused {
        let stopped = application
            .change_status(ChangeStandingGoalStatus {
                tenant_id: tenant,
                goal_id: goal,
                principal_id: owner,
                expected_goal_revision: 1,
                expected_pursuit_revision: Some(1),
                new_pursuit_id: None,
                initial_run_id: None,
                initial_message_id: None,
                status: StandingGoalStatus::Paused,
                occurred_at: UtcTimestamp::from_unix_millis(115),
            })
            .await
            .expect("input cycle must succeed");
        assert_eq!(
            stopped
                .pursuit
                .as_ref()
                .expect("input cycle must succeed")
                .active_run_id,
            Some(run)
        );
        assert_eq!(
            stopped
                .pursuit
                .as_ref()
                .expect("input cycle must succeed")
                .updated_at,
            UtcTimestamp::from_unix_millis(100)
        );
        assert_eq!(
            application
                .accept_authorized_input(AcceptStandingGoalInput {
                    input_key: "source:late".into(),
                    expected_goal_revision: stopped.goal.revision,
                    ..request.clone()
                })
                .await
                .expect("input cycle must succeed"),
            StandingGoalInputOutcome::Rejected
        );
    }
    terminalize_source_run(&pool, run, 120, 130).await;
    let evaluation = application
        .claim_terminal_evaluations(ClaimStandingGoalEvaluations {
            tenant_id: tenant,
            worker_id: "input-evaluator".into(),
            now: UtcTimestamp::from_unix_millis(140),
            lease_millis: 30_000,
            limit: 10,
        })
        .await
        .expect("input cycle must succeed")
        .remove(0);
    application
        .evaluate_run(EvaluateStandingGoalRun {
            tenant_id: tenant,
            goal_id: goal,
            pursuit_id: pursuit,
            run_id: run,
            claim_token: evaluation.claim_token,
            evaluation_id: GoalEvaluationId::generate(),
            verdict: GoalVerdict::Achieved,
            evidence: vec![evidence.clone()],
            summary: b"setup complete".to_vec(),
            deterministic: true,
            completion_confirmation_required: false,
            wait_barrier: None,
            continuation_due_at: None,
            usage_delta: evaluation.run.usage,
            occurred_at: UtcTimestamp::from_unix_millis(150),
        })
        .await
        .expect("input cycle must succeed");
    drop(application);
    let recovered = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        content.clone(),
    );
    assert_eq!(
        recovered
            .wake_event_waits(WakeEventStandingGoalWaits {
                tenant_id: tenant,
                now: UtcTimestamp::from_unix_millis(160),
                limit: 10,
            })
            .await
            .expect("input cycle must succeed"),
        1
    );
    let claim_request = ClaimStandingGoalPursuits {
        tenant_id: tenant,
        worker_id: "input-worker".into(),
        now: UtcTimestamp::from_unix_millis(170),
        lease_millis: 1000,
        limit: 10,
    };
    if edited {
        let current = recovered
            .inspect_owned(tenant, goal, owner)
            .await
            .expect("input cycle must succeed");
        let mut changed = definition_for_rules(agent, conversation);
        changed.completion.minimum_evidence_items = 2;
        let updated = recovered
            .replace(musteragent_runtime_automation::ReplaceStandingGoal {
                tenant_id: tenant,
                goal_id: goal,
                principal_id: owner,
                expected_revision: current.goal.revision,
                definition: changed,
                occurred_at: UtcTimestamp::from_unix_millis(165),
            })
            .await
            .expect("input cycle must succeed");
        assert_eq!(
            recovered
                .accept_authorized_input(AcceptStandingGoalInput {
                    input_key: "source:resource:changed-rule".into(),
                    expected_goal_revision: updated.goal.revision,
                    ..request.clone()
                })
                .await
                .expect("input cycle must succeed"),
            StandingGoalInputOutcome::Accepted { sequence: 2 }
        );
    }
    let claimed = recovered
        .claim_due(claim_request.clone())
        .await
        .expect("input cycle must succeed")
        .remove(0);
    assert_eq!(claimed.input_refs, vec![evidence]);
    assert_eq!(
        claimed
            .processing_rules
            .as_ref()
            .expect("input cycle must succeed")
            .completion
            .minimum_evidence_items,
        1
    );
    if edited {
        assert_eq!(claimed.inspection.goal.completion.minimum_evidence_items, 2);
        assert_ne!(
            claimed
                .processing_rules
                .as_ref()
                .expect("input cycle must succeed")
                .objective_ref,
            claimed.inspection.goal.objective_ref
        );
        assert_eq!(
            recovered
                .accept_authorized_input(request.clone())
                .await
                .expect("input cycle must succeed"),
            StandingGoalInputOutcome::Existing { sequence: 1 }
        );
    }
    if !edited {
        let second = content
            .put(PutContent {
                tenant_id: tenant,
                media_type: MediaType::new("text/plain").expect("input cycle must succeed"),
                bytes: b"source revision 2".to_vec(),
            })
            .await
            .expect("input cycle must succeed");
        assert_eq!(
            recovered
                .accept_authorized_input(AcceptStandingGoalInput {
                    input_key: "source:resource:revision-2".into(),
                    content: second,
                    expected_goal_revision: claimed.inspection.goal.revision,
                    ..request
                })
                .await
                .expect("input cycle must succeed"),
            if paused {
                StandingGoalInputOutcome::Rejected
            } else {
                StandingGoalInputOutcome::Accepted { sequence: 2 }
            }
        );
    }
    let again = recovered
        .claim_due(ClaimStandingGoalPursuits {
            now: UtcTimestamp::from_unix_millis(1171),
            worker_id: "replacement".into(),
            ..claim_request
        })
        .await
        .expect("input cycle must succeed")
        .remove(0);
    assert_eq!(again.input_refs, claimed.input_refs);
    assert_eq!(again.processing_rules, claimed.processing_rules);
    assert_ne!(again.claim_token, claimed.claim_token);
    let positions: (i64, i64) = sqlx::query_as(
        "SELECT input_accepted_seq,input_consumed_seq FROM runtime.standing_goals WHERE goal_id=$1",
    )
    .bind(goal.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("input cycle must succeed");
    assert_eq!(
        positions,
        (if paused { 1 } else { 2 }, 0),
        "claiming must never acknowledge processing"
    );
}

fn definition_for_rules(agent: AgentId, conversation: ConversationId) -> StandingGoalDefinition {
    let mut changed = definition(agent, conversation, 1);
    changed.mode = StandingGoalMode::Continuous;
    changed.objective = b"Future input rules changed".to_vec();
    changed
}
