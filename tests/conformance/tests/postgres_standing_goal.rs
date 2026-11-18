#[path = "support/standing_goal_admission_retry.rs"]
mod admission_retry_contract;
#[path = "support/standing_goal_health.rs"]
mod health_contract;
#[path = "postgres_standing_goal/inputs.rs"]
mod inputs;
#[path = "support/standing_goal_lifecycle.rs"]
mod lifecycle_contract;
#[path = "support/standing_goal_review_retirement.rs"]
mod review_retirement;
use std::sync::Arc;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_execution_grant::{
    EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1, ExecutionGrantProofHash,
};
use musteragent_goal::{
    GoalBudget, GoalBudgetUsage, GoalCompletionContract, GoalOrigin, GoalPursuitStatus,
    GoalRiskClass, GoalVerdict, GoalWaitKind, StandingGoalStatus,
};
use musteragent_identity::{
    ActivityId, AgentId, ConversationId, DottedKey, GoalEvaluationId, GoalId, GoalPursuitId,
    ParticipantId, PrincipalId, RunId, RuntimeSnapshotId, TenantId, UtcTimestamp,
};
use musteragent_postgres::{PostgresContentStore, PostgresStandingGoalProductStore};
use musteragent_runtime_automation::{
    ChangeStandingGoalStatus, ClaimStandingGoalEvaluations, ClaimStandingGoalPursuits,
    CreateStandingGoal, EvaluateStandingGoalRun, ProposeStandingGoalEvaluation,
    RecordStandingGoalWakeSignal, ReplaceStandingGoal, StandingGoalDefinition,
    StandingGoalProductApplication, StandingGoalProductError, StandingGoalProgressPhase,
    StandingGoalWaitDefinition, StandingGoalWakeSignalDisposition, WakeEventStandingGoalWaits,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;

fn reference_keys() -> Arc<OpaqueRefKeyring> {
    let key_id = DottedKey::new("standing-goal.postgres.test").expect("key id");
    Arc::new(
        OpaqueRefKeyring::new(key_id.clone(), [(key_id, [79_u8; 32])]).expect("reference keyring"),
    )
}

#[tokio::test]
async fn draft_cancellation_does_not_require_a_pursuit() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("draft cancellation does not require a pursuit must succeed");
    let pool = postgres
        .pool()
        .await
        .expect("draft cancellation does not require a pursuit must succeed");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("draft cancellation does not require a pursuit must succeed");
    let content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    let app = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool)),
        content,
    );
    lifecycle_contract::draft_cancellation(
        &app,
        TenantId::generate(),
        PrincipalId::generate(),
        definition(AgentId::generate(), ConversationId::generate(), 1),
    )
    .await;
}

fn definition(
    target_agent_id: AgentId,
    conversation_id: ConversationId,
    revision: u8,
) -> StandingGoalDefinition {
    StandingGoalDefinition {
        mode: Default::default(),
        target_agent_id,
        conversation_id,
        title: format!("Cloud release goal {revision}"),
        objective: format!("Complete Cloud release {revision} safely").into_bytes(),
        constraints: vec!["Never bypass an acceptance gate".to_owned()],
        completion: GoalCompletionContract {
            risk_class: GoalRiskClass::Standard,
            success_criteria: vec!["Every acceptance gate passes".to_owned()],
            evidence_requirements: vec!["Persist test evidence".to_owned()],
            failure_conditions: vec!["Authority is denied".to_owned()],
            minimum_evidence_items: 1,
        },
        budget: GoalBudget {
            max_runs: 37,
            max_elapsed_millis: 9_876_543,
            max_model_tokens: 987_654,
            max_tool_invocations: 4_321,
            max_consecutive_failures: 4,
        },
    }
}

#[tokio::test]
async fn postgres_conversation_rejects_a_second_non_terminal_goal() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply migrations");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    let application = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        content,
    );
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let conversation_id = ConversationId::generate();
    let target_agent_id = AgentId::generate();

    application
        .create(CreateStandingGoal {
            tenant_id,
            goal_id: GoalId::generate(),
            pursuit_id: GoalPursuitId::generate(),
            principal_id,
            origin: GoalOrigin::User,
            creator_agent_id: None,
            definition: definition(target_agent_id, conversation_id, 1),
            activate: false,
            initial_run_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(100),
        })
        .await
        .expect("create first non-terminal Goal");
    let duplicate = application
        .create(CreateStandingGoal {
            tenant_id,
            goal_id: GoalId::generate(),
            pursuit_id: GoalPursuitId::generate(),
            principal_id,
            origin: GoalOrigin::User,
            creator_agent_id: None,
            definition: definition(target_agent_id, conversation_id, 2),
            activate: false,
            initial_run_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(110),
        })
        .await;
    assert_eq!(duplicate, Err(StandingGoalProductError::Conflict));

    application
        .create(CreateStandingGoal {
            tenant_id,
            goal_id: GoalId::generate(),
            pursuit_id: GoalPursuitId::generate(),
            principal_id,
            origin: GoalOrigin::User,
            creator_agent_id: None,
            definition: definition(target_agent_id, ConversationId::generate(), 3),
            activate: false,
            initial_run_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(120),
        })
        .await
        .expect("a different Conversation owns an independent Goal lane");
    pool.close().await;
}

#[tokio::test]
async fn postgres_standing_goal_round_trips_the_same_product_contract() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply migrations");

    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    let application = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        content.clone(),
    );
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let goal_id = GoalId::generate();
    let conversation_id = ConversationId::generate();
    let target_agent_id = AgentId::generate();
    let created_at = UtcTimestamp::from_unix_millis(1_800_000_000_000);

    let draft = application
        .create(CreateStandingGoal {
            tenant_id,
            goal_id,
            pursuit_id: GoalPursuitId::generate(),
            principal_id,
            origin: GoalOrigin::User,
            creator_agent_id: None,
            definition: definition(target_agent_id, conversation_id, 1),
            activate: false,
            initial_run_id: None,
            occurred_at: created_at,
        })
        .await
        .expect("create PostgreSQL Goal");
    assert_eq!(draft.goal.status, StandingGoalStatus::Draft);
    assert_eq!(draft.goal.revision, 1);
    assert_eq!(draft.objective, b"Complete Cloud release 1 safely");

    let listed = application
        .list_owned_for_conversation(tenant_id, conversation_id, principal_id)
        .await
        .expect("list PostgreSQL Goals");
    assert_eq!(listed.goals, vec![draft.clone()]);

    let replaced = application
        .replace(ReplaceStandingGoal {
            tenant_id,
            goal_id,
            principal_id,
            expected_revision: 1,
            definition: definition(target_agent_id, conversation_id, 2),
            occurred_at: UtcTimestamp::from_unix_millis(created_at.unix_millis() + 1),
        })
        .await
        .expect("replace PostgreSQL Goal");
    assert_eq!(replaced.goal.revision, 2);
    assert_eq!(replaced.goal.title, "Cloud release goal 2");
    assert_eq!(replaced.objective, b"Complete Cloud release 2 safely");
    assert_eq!(replaced.goal.budget.max_model_tokens, 987_654);

    let reloaded = application
        .inspect_owned(tenant_id, goal_id, principal_id)
        .await
        .expect("reload PostgreSQL Goal");
    assert_eq!(reloaded, replaced);
    application
        .delete(musteragent_runtime_automation::DeleteStandingGoal {
            tenant_id,
            goal_id,
            principal_id,
            expected_revision: 2,
        })
        .await
        .expect("delete cloud Goal");
    assert!(
        application
            .list_owned_for_conversation(tenant_id, conversation_id, principal_id)
            .await
            .expect("postgres standing goal round trips the same product contract must succeed")
            .goals
            .is_empty()
    );
    assert!(
        application
            .inspect_deletion_owned(tenant_id, goal_id, principal_id, conversation_id)
            .await
            .expect("postgres standing goal round trips the same product contract must succeed")
            .is_some()
    );
    assert!(
        application
            .inspect_deletion_owned(tenant_id, goal_id, PrincipalId::generate(), conversation_id)
            .await
            .expect("postgres standing goal round trips the same product contract must succeed")
            .is_none()
    );

    pool.close().await;
}

#[tokio::test]
async fn postgres_standing_goal_evaluation_continuation_and_external_wake_are_durable() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply migrations");

    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    {
        use musteragent_runtime_automation::EvaluationAgentStateSource;
        let source = musteragent_postgres::PostgresStandingGoalJudgeRunSource::new(
            pool.clone(),
            content.clone(),
        );
        assert!(
            source
                .unjournaled_protocol_receipts(RunId::generate())
                .await
                .expect("legacy protocol receipt query")
                .is_empty()
        );
    }
    let application = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        content.clone(),
    );
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let conversation_id = ConversationId::generate();
    let target_agent_id = AgentId::generate();
    let activity_id = ActivityId::generate();
    let run_id = RunId::generate();
    let snapshot_id = RuntimeSnapshotId::generate();
    let executor_id = ParticipantId::generate();
    let requester_id = ParticipantId::generate();
    seed_source_run(
        &pool,
        tenant_id,
        principal_id,
        conversation_id,
        activity_id,
        run_id,
        snapshot_id,
        executor_id,
        requester_id,
    )
    .await;

    let goal_id = GoalId::generate();
    let pursuit_id = GoalPursuitId::generate();
    application
        .create(CreateStandingGoal {
            tenant_id,
            goal_id,
            pursuit_id,
            principal_id,
            origin: GoalOrigin::Agent,
            creator_agent_id: Some(target_agent_id),
            definition: definition(target_agent_id, conversation_id, 1),
            activate: true,
            initial_run_id: Some(run_id),
            occurred_at: UtcTimestamp::from_unix_millis(100),
        })
        .await
        .expect("activate PostgreSQL Standing Goal");
    let persisted_step_evidence = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/plain;charset=utf-8").expect("evidence media type"),
            bytes: b"first Cloud step persisted".to_vec(),
        })
        .await
        .expect("publish PostgreSQL Goal evidence");
    application
        .propose_evaluation(ProposeStandingGoalEvaluation {
            tenant_id,
            goal_id,
            pursuit_id,
            run_id,
            principal_id,
            verdict: GoalVerdict::Continue,
            evidence: vec![persisted_step_evidence],
            summary: b"Continue from the durable Cloud Goal state".to_vec(),
            wait: None,
            occurred_at: UtcTimestamp::from_unix_millis(110),
        })
        .await
        .expect("persist PostgreSQL Goal proposal");
    terminalize_source_run(&pool, run_id, 120, 130).await;
    let claim = application
        .claim_terminal_evaluations(ClaimStandingGoalEvaluations {
            tenant_id,
            worker_id: "postgres-goal-evaluator".to_owned(),
            now: UtcTimestamp::from_unix_millis(140),
            lease_millis: 30_000,
            limit: 10,
        })
        .await
        .expect("claim PostgreSQL Goal evaluation")
        .into_iter()
        .find(|claim| claim.inspection.goal.goal_id == goal_id)
        .expect("PostgreSQL evaluation claim");
    assert_eq!(
        claim.inspection.progress.phase,
        StandingGoalProgressPhase::Evaluating
    );
    assert_eq!(claim.inspection.progress.run_number, 1);
    health_contract::verify_evaluation_retry_feedback(&application, &claim).await;
    let proposal = claim.proposal.expect("PostgreSQL Goal proposal");
    let continued = application
        .evaluate_run(EvaluateStandingGoalRun {
            tenant_id,
            goal_id,
            pursuit_id,
            run_id,
            claim_token: claim.claim_token,
            evaluation_id: GoalEvaluationId::generate(),
            verdict: proposal.verdict,
            evidence: proposal.evidence,
            summary: claim.proposal_summary.expect("proposal summary"),
            deterministic: false,
            completion_confirmation_required: false,
            wait_barrier: proposal.wait_barrier,
            continuation_due_at: Some(UtcTimestamp::from_unix_millis(150)),
            usage_delta: GoalBudgetUsage {
                model_tokens: 3_400_000,
                ..claim.run.usage
            },
            occurred_at: UtcTimestamp::from_unix_millis(150),
        })
        .await
        .expect("commit PostgreSQL Goal evaluation");
    assert_eq!(continued.goal.status, StandingGoalStatus::Active);
    assert_eq!(
        continued.progress.phase,
        StandingGoalProgressPhase::Continuing
    );
    assert_eq!(continued.progress.run_number, 2);
    assert_eq!(
        continued.pursuit.as_ref().map(|value| value.status),
        Some(GoalPursuitStatus::Pending)
    );
    sqlx::query("UPDATE runtime.runs SET status = 1 WHERE run_id = $1")
        .bind(run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("project queued Cloud foreground Chat");
    let preempted = application
        .claim_due(ClaimStandingGoalPursuits {
            tenant_id,
            worker_id: "postgres-preempted-goal-admission".to_owned(),
            now: UtcTimestamp::from_unix_millis(150),
            lease_millis: 30_000,
            limit: 10,
        })
        .await
        .expect("Cloud foreground Chat preempts Goal continuation");
    assert!(preempted.is_empty());
    // A non-Chat source is subject to the same no-overlap rule after resume.
    sqlx::query("UPDATE runtime.runs SET activity_kind = 5 WHERE run_id = $1")
        .bind(run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("Goal source activity");
    for status in [1_i16, 2, 3, 4] {
        sqlx::query("UPDATE runtime.runs SET status = $1, wait_reason = CASE WHEN $1 = 3 THEN 1 ELSE NULL END WHERE run_id = $2")
            .bind(status)
            .bind(run_id.into_uuid())
            .execute(&pool)
            .await
            .expect("nonterminal source");
        assert!(
            application
                .claim_due(ClaimStandingGoalPursuits {
                    tenant_id,
                    worker_id: "postgres-no-overlap".to_owned(),
                    now: UtcTimestamp::from_unix_millis(150),
                    lease_millis: 30_000,
                    limit: 10,
                })
                .await
                .expect("claim cannot overlap source")
                .is_empty()
        );
    }
    sqlx::query("UPDATE runtime.runs SET activity_kind = 1 WHERE run_id = $1")
        .bind(run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("restore fixture activity");

    sqlx::query("UPDATE runtime.runs SET status = 5 WHERE run_id = $1")
        .bind(run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("finish Cloud foreground Chat");
    let continuations = application
        .claim_due(ClaimStandingGoalPursuits {
            tenant_id,
            worker_id: "postgres-goal-admission".to_owned(),
            now: UtcTimestamp::from_unix_millis(150),
            lease_millis: 30_000,
            limit: 10,
        })
        .await
        .expect("claim PostgreSQL Goal continuation");
    assert_eq!(continuations.len(), 1);
    let musteragent_runtime_automation::StandingGoalPursuitSource::TerminalRun {
        source_run_id,
        source_execution_grant_proof,
        ..
    } = &continuations[0].source
    else {
        panic!("original terminal source");
    };
    assert_eq!(*source_run_id, run_id);
    assert_eq!(
        source_execution_grant_proof.encoded_envelope(),
        b"postgres-standing-goal-source-proof"
    );

    let continued = admission_retry_contract::verify(
        &application,
        &continuations[0],
        UtcTimestamp::from_unix_millis(150),
    )
    .await;

    application
        .change_status(ChangeStandingGoalStatus {
            tenant_id,
            goal_id,
            principal_id,
            expected_goal_revision: continued.goal.revision,
            expected_pursuit_revision: Some(
                continued
                    .pursuit
                    .as_ref()
                    .expect("continued Goal Pursuit")
                    .revision,
            ),
            new_pursuit_id: None,
            initial_run_id: None,
            initial_message_id: None,
            status: StandingGoalStatus::Cancelled,
            occurred_at: UtcTimestamp::from_unix_millis(155),
        })
        .await
        .expect("close the first PostgreSQL Goal before testing another lifecycle");

    let external_goal_id = GoalId::generate();
    let external_pursuit_id = GoalPursuitId::generate();
    let correlation_key = "provider:cloud-release:accepted";
    application
        .create(CreateStandingGoal {
            tenant_id,
            goal_id: external_goal_id,
            pursuit_id: external_pursuit_id,
            principal_id,
            origin: GoalOrigin::Agent,
            creator_agent_id: Some(target_agent_id),
            definition: definition(target_agent_id, conversation_id, 2),
            activate: true,
            initial_run_id: Some(run_id),
            occurred_at: UtcTimestamp::from_unix_millis(160),
        })
        .await
        .expect("activate external-wait PostgreSQL Goal");
    application
        .propose_evaluation(ProposeStandingGoalEvaluation {
            tenant_id,
            goal_id: external_goal_id,
            pursuit_id: external_pursuit_id,
            run_id,
            principal_id,
            verdict: GoalVerdict::Waiting,
            evidence: Vec::new(),
            summary: b"Wait for the durable Cloud provider event".to_vec(),
            wait: Some(StandingGoalWaitDefinition {
                kind: GoalWaitKind::ExternalDependency,
                correlation_key: correlation_key.to_owned(),
                reason: b"Cloud release provider has not accepted the release".to_vec(),
                resume_not_before: None,
                expires_at: None,
            }),
            occurred_at: UtcTimestamp::from_unix_millis(170),
        })
        .await
        .expect("persist PostgreSQL external wait proposal");
    let claim = application
        .claim_terminal_evaluations(ClaimStandingGoalEvaluations {
            tenant_id,
            worker_id: "postgres-external-wait-evaluator".to_owned(),
            now: UtcTimestamp::from_unix_millis(180),
            lease_millis: 30_000,
            limit: 10,
        })
        .await
        .expect("claim PostgreSQL external wait")
        .into_iter()
        .find(|claim| claim.inspection.goal.goal_id == external_goal_id)
        .expect("external wait evaluation claim");
    let proposal = claim.proposal.expect("external wait proposal");
    application
        .evaluate_run(EvaluateStandingGoalRun {
            tenant_id,
            goal_id: external_goal_id,
            pursuit_id: external_pursuit_id,
            run_id,
            claim_token: claim.claim_token,
            evaluation_id: GoalEvaluationId::generate(),
            verdict: proposal.verdict,
            evidence: proposal.evidence,
            summary: claim.proposal_summary.expect("external wait summary"),
            deterministic: true,
            completion_confirmation_required: false,
            wait_barrier: proposal.wait_barrier,
            continuation_due_at: None,
            usage_delta: claim.run.usage,
            occurred_at: UtcTimestamp::from_unix_millis(190),
        })
        .await
        .expect("commit PostgreSQL external wait");
    let signal = RecordStandingGoalWakeSignal {
        tenant_id,
        signal_id: "cloud-provider-event-1".to_owned(),
        correlation_key: correlation_key.to_owned(),
        occurred_at: UtcTimestamp::from_unix_millis(200),
    };
    assert_eq!(
        application
            .record_wake_signal(signal.clone())
            .await
            .expect("record PostgreSQL wake signal"),
        StandingGoalWakeSignalDisposition::Recorded
    );
    assert_eq!(
        application
            .record_wake_signal(signal)
            .await
            .expect("redeliver PostgreSQL wake signal"),
        StandingGoalWakeSignalDisposition::Existing
    );
    assert_eq!(
        application
            .wake_event_waits(WakeEventStandingGoalWaits {
                tenant_id,
                now: UtcTimestamp::from_unix_millis(210),
                limit: 10,
            })
            .await
            .expect("wake PostgreSQL external wait"),
        1
    );
    let resumed = application
        .inspect(tenant_id, external_goal_id)
        .await
        .expect("inspect resumed PostgreSQL Goal")
        .pursuit
        .expect("resumed PostgreSQL Pursuit");
    assert_eq!(resumed.status, GoalPursuitStatus::Pending);
    assert!(resumed.wait_barrier.is_none());
    pool.close().await;
}

#[allow(clippy::too_many_arguments)]
async fn seed_source_run(
    pool: &sqlx::PgPool,
    tenant_id: TenantId,
    principal_id: PrincipalId,
    conversation_id: ConversationId,
    activity_id: ActivityId,
    run_id: RunId,
    snapshot_id: RuntimeSnapshotId,
    executor_id: ParticipantId,
    requester_id: ParticipantId,
) {
    let mut participant_transaction = pool.begin().await.expect("begin participant fixture");
    sqlx::query(
        "INSERT INTO conversation.conversations (
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms,
             execution_owner_participant_id
         ) VALUES ($1, $2, 1, NULL, 1, NULL, 1, 1, $3, 1, 1, NULL, $4)",
    )
    .bind(conversation_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(principal_id.into_uuid())
    .bind(executor_id.into_uuid())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed Goal source Conversation");
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES
             ($1, $3, $4, 1, $5, NULL, NULL, 'Goal Tester', NULL, 1, NULL),
             ($2, $3, $4, 2, NULL,
              (SELECT agent_id FROM agent.agent_definitions WHERE agent_key = 'assistant'),
              NULL, 'Kernel', NULL, 1, NULL)",
    )
    .bind(requester_id.into_uuid())
    .bind(executor_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(principal_id.into_uuid())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed Goal source participants");
    participant_transaction
        .commit()
        .await
        .expect("commit participant fixture");
    sqlx::query(
        "INSERT INTO event_store.ledger_streams (
             tenant_id, conversation_id, home_epoch, last_sequence,
             retained_from_sequence, created_at_ms, updated_at_ms
         ) VALUES ($1, $2, 1, 1, 1, 1, 1)",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed Goal source Ledger authority");
    sqlx::query(
        "INSERT INTO activity.activities (
             activity_id, tenant_id, conversation_id, root_activity_id,
             parent_activity_id, kind, status, policy_snapshot_ref,
             created_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, $1, NULL, 1, 2, 'musteragent.legacy.chat@1', 1, NULL)",
    )
    .bind(activity_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed Goal source Activity");
    let proof = b"postgres-standing-goal-source-proof";
    sqlx::query(
        "INSERT INTO capability.runtime_snapshots (
             runtime_snapshot_id, tenant_id, conversation_id, run_id,
             schema_version, encoded_snapshot, content_hash, created_at_ms,
             execution_grant_proof_schema, execution_grant_proof,
             execution_grant_proof_hash, execution_grant_claims_hash
         ) VALUES ($1, $2, $3, $4, 'musteragent.runtime.snapshot.v17', $5, $6, 1,
                   $7, $8, $9, $10)",
    )
    .bind(snapshot_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(b"standing Goal source snapshot".as_slice())
    .bind(
        Sha256::digest(b"standing Goal source snapshot")
            .into_bytes()
            .to_vec(),
    )
    .bind(EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1)
    .bind(proof.as_slice())
    .bind(ExecutionGrantProofHash::digest(proof).into_bytes().to_vec())
    .bind(
        ExecutionGrantProofHash::digest(b"postgres-standing-goal-source-claims")
            .into_bytes()
            .to_vec(),
    )
    .execute(pool)
    .await
    .expect("seed Goal source Runtime Snapshot");
    sqlx::query(
        "INSERT INTO runtime.runs (
             run_id, tenant_id, conversation_id, activity_id, activity_kind,
             executor_participant_id, requested_by_participant_id, parent_run_id,
             delegation_id, execution_scope_kind, workspace_id, device_id,
             control_home_kind, control_device_id, execution_home_kind,
             execution_device_id, home_epoch, runtime_snapshot_id,
             admitted_sequence, last_run_sequence, next_fencing_token,
             status, wait_reason, wait_interaction_id, wait_invocation_id,
             next_attempt_number, not_before_ms, cancel_requested_at_ms,
             admitted_at_ms, started_at_ms, finished_at_ms, terminal_error, run_version,
             traceparent, tracestate
         ) VALUES (
             $1, $2, $3, $4, 1, $5, $6, NULL, NULL, 1, NULL, NULL,
             1, NULL, 1, NULL, 1, $7, 1, 1, 1,
             2, NULL, NULL, NULL, 1, NULL, NULL, 1, 2, NULL, NULL, 1,
             '00-00000000000000000000000000000001-0000000000000001-00', NULL
         )",
    )
    .bind(run_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(activity_id.into_uuid())
    .bind(executor_id.into_uuid())
    .bind(requester_id.into_uuid())
    .bind(snapshot_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed Goal source Run");
}

async fn terminalize_source_run(
    pool: &sqlx::PgPool,
    run_id: RunId,
    started_at: i64,
    finished_at: i64,
) {
    sqlx::query(
        "UPDATE runtime.runs
         SET status = 5, started_at_ms = $2, finished_at_ms = $3, run_version = 2
         WHERE run_id = $1",
    )
    .bind(run_id.into_uuid())
    .bind(started_at)
    .bind(finished_at)
    .execute(pool)
    .await
    .expect("terminalize Goal source Run");
}

#[tokio::test]
async fn hard_blocked_goal_cannot_schedule_another_paid_run() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply migrations");

    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    {
        use musteragent_runtime_automation::EvaluationAgentStateSource;
        let source = musteragent_postgres::PostgresStandingGoalJudgeRunSource::new(
            pool.clone(),
            content.clone(),
        );
        assert!(
            source
                .unjournaled_protocol_receipts(RunId::generate())
                .await
                .expect("legacy protocol receipt query")
                .is_empty()
        );
    }
    let application = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        content.clone(),
    );
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let conversation_id = ConversationId::generate();
    let target_agent_id = AgentId::generate();
    let activity_id = ActivityId::generate();
    let run_id = RunId::generate();
    let snapshot_id = RuntimeSnapshotId::generate();
    let executor_id = ParticipantId::generate();
    let requester_id = ParticipantId::generate();
    seed_source_run(
        &pool,
        tenant_id,
        principal_id,
        conversation_id,
        activity_id,
        run_id,
        snapshot_id,
        executor_id,
        requester_id,
    )
    .await;

    let goal_id = GoalId::generate();
    let pursuit_id = GoalPursuitId::generate();
    application
        .create(CreateStandingGoal {
            tenant_id,
            goal_id,
            pursuit_id,
            principal_id,
            origin: GoalOrigin::Agent,
            creator_agent_id: Some(target_agent_id),
            definition: definition(target_agent_id, conversation_id, 1),
            activate: true,
            initial_run_id: Some(run_id),
            occurred_at: UtcTimestamp::from_unix_millis(100),
        })
        .await
        .expect("activate PostgreSQL Standing Goal");
    sqlx::query("UPDATE runtime.runs SET status = 6, started_at_ms = 120, finished_at_ms = 130, terminal_error = $2 WHERE run_id = $1")
        .bind(run_id.into_uuid()).bind(b"credits_exhausted".as_slice()).execute(&pool).await.expect("failed source");
    let now = UtcTimestamp::from_unix_millis(140);
    let claim = application
        .claim_terminal_evaluations(ClaimStandingGoalEvaluations {
            tenant_id,
            worker_id: "blocker-test".to_owned(),
            now,
            lease_millis: 30_000,
            limit: 10,
        })
        .await
        .expect("claim")
        .into_iter()
        .next()
        .expect("failed run observation");
    assert_eq!(
        claim.run.failure_code,
        Some(musteragent_runtime_contract::ErrorCode::CreditsExhausted)
    );
    let blocked = application
        .block_execution(
            &claim,
            GoalEvaluationId::generate(),
            claim.run.failure_code,
            now,
        )
        .await
        .expect("block");
    assert_eq!(blocked.goal.status, StandingGoalStatus::Active);
    let pursuit = blocked.pursuit.expect("pursuit");
    assert_eq!(pursuit.status, GoalPursuitStatus::Waiting);
    assert!(pursuit.active_run_id.is_none());
    assert!(pursuit.continuation_due_at.is_none());
    assert_eq!(
        pursuit
            .wait_barrier
            .expect("explicit recovery barrier")
            .correlation_key,
        format!("run-blocked:{run_id}")
    );
    assert!(
        application
            .claim_due(ClaimStandingGoalPursuits {
                tenant_id,
                worker_id: "after-recharge".to_owned(),
                now: UtcTimestamp::from_unix_millis(90_000),
                lease_millis: 30_000,
                limit: 10,
            })
            .await
            .expect("no automatic revival")
            .is_empty()
    );
}

#[tokio::test]
async fn postgres_legacy_result_review_retires_once_with_original_cas() {
    Box::pin(postgres_goal_review_case(true)).await;
}
#[tokio::test]
async fn postgres_high_risk_semantic_completion_stays_unverified_without_human_gate() {
    Box::pin(postgres_goal_review_case(false)).await;
}
async fn postgres_goal_review_case(historical: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("postgres goal review case must succeed");
    let pool = postgres
        .pool()
        .await
        .expect("postgres goal review case must succeed");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("postgres goal review case must succeed");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    let owner = Arc::new(PostgresStandingGoalProductStore::new(pool.clone()));
    let app = StandingGoalProductApplication::new(owner.clone(), content.clone());
    let tenant = TenantId::generate();
    let principal = PrincipalId::generate();
    let conversation = ConversationId::generate();
    let run = RunId::generate();
    let agent = AgentId::generate();
    seed_source_run(
        &pool,
        tenant,
        principal,
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
    let mut spec = definition(agent, conversation, 1);
    spec.completion.risk_class = GoalRiskClass::HighRisk;
    app.create(CreateStandingGoal {
        tenant_id: tenant,
        goal_id: goal,
        pursuit_id: pursuit,
        principal_id: principal,
        origin: GoalOrigin::Agent,
        creator_agent_id: Some(agent),
        definition: spec,
        activate: true,
        initial_run_id: Some(run),
        occurred_at: UtcTimestamp::from_unix_millis(100),
    })
    .await
    .expect("postgres goal review case must succeed");
    let evidence = content
        .put(PutContent {
            tenant_id: tenant,
            media_type: MediaType::new("text/plain")
                .expect("postgres goal review case must succeed"),
            bytes: b"Retained evidence for historical assessment".to_vec(),
        })
        .await
        .expect("postgres goal review case must succeed");
    terminalize_source_run(&pool, run, 110, 120).await;
    let claim = app
        .claim_terminal_evaluations(ClaimStandingGoalEvaluations {
            tenant_id: tenant,
            worker_id: "goal-retirement-fixture".into(),
            now: UtcTimestamp::from_unix_millis(140),
            lease_millis: 30000,
            limit: 10,
        })
        .await
        .expect("postgres goal review case must succeed")
        .remove(0);
    if !historical {
        review_retirement::verify_current_evidence_block(&app, &claim, vec![evidence]).await;
        return;
    }
    review_retirement::retain_legacy_wait(
        owner.as_ref(),
        &app,
        content.as_ref(),
        &claim,
        vec![evidence],
        UtcTimestamp::from_unix_millis(150),
    )
    .await;
    let before = app
        .inspect(tenant, goal)
        .await
        .expect("postgres goal review case must succeed");
    review_retirement::verify_retirement(owner.as_ref(), &app, content.as_ref(), before).await;
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM runtime.goal_result_review_retirements),(SELECT count(*) FROM runtime.goal_evaluations),(SELECT count(*) FROM runtime.runs)").fetch_one(&pool).await.expect("postgres goal review case must succeed");
    assert_eq!(counts, (1, 2, 1));
    let scans: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.goal_review_retirement_scans")
            .fetch_one(&pool)
            .await
            .expect("postgres goal review case must succeed");
    assert_eq!(
        scans, 0,
        "the last retired review removes scan custody; idle reads do not recreate it"
    );
}
