//! Standing-Goal continuation and independent judge admission through the
//! original Runtime application, PostgreSQL Goal claims and Run/Ledger path.
//! The host grant is a fixture; claims, fences and commits are real.
use super::*;
use musteragent_activity::ActivityKind;
use musteragent_goal::{
    GoalBudget, GoalCompletionContract, GoalOrigin, GoalRiskClass, GoalVerdict,
};
use musteragent_identity::{AgentId, GoalEvaluationId, GoalId, GoalPursuitId};
use musteragent_ledger::{AppendDisposition, StandingGoalAdmissionFence};
use musteragent_postgres::PostgresStandingGoalProductStore;
use musteragent_runtime_application::RuntimeStandingGoalAdmission;
use musteragent_runtime_automation::{
    ClaimStandingGoalEvaluations, ClaimStandingGoalPursuits, ClaimedStandingGoalEvaluation,
    CreateStandingGoal, EvaluateStandingGoalRun, ProposeStandingGoalEvaluation,
    StandingGoalAdmission, StandingGoalAdmissionOutcome, StandingGoalDefinition,
    StandingGoalProductApplication, StandingGoalPursuitSource, standing_goal_judge_command_id,
};
use musteragent_wire::ProtobufExecutionGrantProofEncoder;

struct GoalFixture {
    _postgres: IsolatedPostgres,
    pool: sqlx::PgPool,
    content: Arc<dyn ContentStore>,
    application: Arc<RuntimeApplication>,
    goals: StandingGoalProductApplication,
    actor: Actor,
    conversation_id: ConversationId,
    source_run_id: RunId,
    goal_id: GoalId,
    pursuit_id: GoalPursuitId,
    now: UtcTimestamp,
    evaluation: ClaimedStandingGoalEvaluation,
}

fn goal_definition(
    target_agent_id: AgentId,
    conversation_id: ConversationId,
) -> StandingGoalDefinition {
    StandingGoalDefinition {
        mode: Default::default(),
        target_agent_id,
        conversation_id,
        title: "Release the reference build".to_owned(),
        objective: b"Release the reference build after every gate passes".to_vec(),
        constraints: vec!["Never bypass an acceptance gate".to_owned()],
        completion: GoalCompletionContract {
            risk_class: GoalRiskClass::Standard,
            success_criteria: vec!["Every acceptance gate passes".to_owned()],
            evidence_requirements: vec!["Persist test evidence".to_owned()],
            failure_conditions: vec!["Authority is denied".to_owned()],
            minimum_evidence_items: 1,
        },
        budget: GoalBudget {
            max_runs: 8,
            max_elapsed_millis: 3_600_000,
            max_model_tokens: 1_000_000,
            max_tool_invocations: 1_000,
            max_consecutive_failures: 3,
        },
    }
}

fn trace(command_id: CommandId) -> TraceContext {
    let trace = command_id.into_uuid().simple().to_string();
    TraceContext {
        traceparent: TraceParent::new(format!("00-{trace}-{}-01", &trace[..16])).expect("trace"),
        tracestate: None,
    }
}

/// A Cloud Conversation whose first user Run ended, with an active standing
/// Goal on that Run and the terminal evaluation claimed by the supervisor.
async fn goal_fixture() -> GoalFixture {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let key = DottedKey::new("standing-goal.admission.test").expect("key");
    let keys =
        Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [53; 32])]).expect("reference keys"));
    let blobs = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    );
    for entry in builtin_skill_catalog_entries().expect("Skill catalog") {
        let reference = entry.definition.instructions_ref;
        let bytes = musteragent_skill::builtin_skill_instructions(&reference)
            .expect("compiled Skill instruction material");
        blobs
            .retain_runtime_release_at(&reference, bytes.as_bytes().to_vec())
            .await
            .expect("retain Skill material");
    }
    let content: Arc<dyn ContentStore> = Arc::new(blobs.clone());
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let now = UtcTimestamp::from_unix_millis(
        sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&pool)
            .await
            .expect("database clock"),
    );
    publish_host_agent(&pool, actor, content.as_ref()).await;
    let (store, interactions) = production_audit_stores(&pool, keys, blobs);
    let application = Arc::new(
        RuntimeApplication::new(
            musteragent_identity::AgentKey::new("assistant").expect("Agent key"),
            musteragent_runtime_application::RuntimeApplicationPorts {
                store,
                snapshots: Arc::new(TestSnapshotResolver::production(&pool, content.clone())),
                content: content.clone(),
                encoder: Arc::new(ProtobufLedgerPayloadCodec),
                clock: Arc::new(MovingClock::new(now)),
                grant_authenticator: Arc::new(TestGrantAuthenticator),
                interactions,
            },
        )
        .with_execution_grant_proof_encoder(Arc::new(ProtobufExecutionGrantProofEncoder)),
    );
    let conversation_id = ConversationId::generate();
    application
        .create_conversation(
            CommandEnvelope::new(
                metadata(CommandId::generate(), actor, "create-goal", now, None),
                CreateConversation {
                    conversation_id,
                    workspace_id: None,
                    kind: ConversationKind::Direct,
                    title: None,
                    control_home: RuntimeHome::Cloud,
                },
            )
            .expect("create command"),
        )
        .await
        .expect("create Conversation");
    let view = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation view");
    let participant_id = view
        .participants
        .iter()
        .find(|participant| participant.kind == ParticipantKind::User)
        .expect("user participant")
        .participant_id;
    let target_agent_id = view
        .participants
        .iter()
        .find_map(|participant| {
            (participant.participant_id == view.conversation.execution_owner_participant_id)
                .then_some(participant.agent_ref)
                .flatten()
        })
        .expect("assistant Agent");
    let submit_id = CommandId::generate();
    let source_run_id = derive_admission_run_id(submit_id).expect("source Run");
    let current = |view: &musteragent_conversation::ConversationView| {
        Some(ConversationGuard {
            conversation_id,
            expected_home_epoch: view.conversation.home_epoch,
            expected_conversation_seq: Some(
                ConversationSeq::new(view.conversation.next_conversation_seq.get() - 1)
                    .expect("current sequence"),
            ),
        })
    };
    application
        .submit_message_authorized(
            CommandEnvelope::new(
                metadata(submit_id, actor, "submit-goal-source", now, current(&view)),
                SubmitMessage {
                    project_task_target: None,
                    message_id: MessageId::generate(),
                    participant_id,
                    content: InputContent::InlineUtf8(
                        InlineInput::new("Prepare the reference release").expect("message"),
                    ),
                    attachment_refs: Vec::new(),
                    requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
                    requested_features: RequestedFeatures {
                        reasoning_summary: true,
                        ..RequestedFeatures::default()
                    },
                    execution_scope: ExecutionScope::Tenant {
                        tenant_id: actor.tenant_id,
                    },
                },
            )
            .expect("submit command"),
            signed_test_grant(actor, source_run_id, now),
            AdmissionSelection::web(BTreeSet::new()),
        )
        .await
        .expect("admit source Run");

    let goals = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        content.clone(),
    );
    let goal_id = GoalId::generate();
    let pursuit_id = GoalPursuitId::generate();
    let at = |offset: i64| now.checked_add_millis(offset).expect("fixture time");
    goals
        .create(CreateStandingGoal {
            tenant_id: actor.tenant_id,
            goal_id,
            pursuit_id,
            principal_id: actor.principal_id,
            origin: GoalOrigin::User,
            creator_agent_id: None,
            definition: goal_definition(target_agent_id, conversation_id),
            activate: true,
            initial_run_id: Some(source_run_id),
            occurred_at: at(10),
        })
        .await
        .expect("activate standing Goal");
    let evidence = content
        .put(PutContent {
            tenant_id: actor.tenant_id,
            media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
            bytes: b"first release gate passed".to_vec(),
        })
        .await
        .expect("evidence");
    goals
        .propose_evaluation(ProposeStandingGoalEvaluation {
            tenant_id: actor.tenant_id,
            goal_id,
            pursuit_id,
            run_id: source_run_id,
            principal_id: actor.principal_id,
            verdict: GoalVerdict::Continue,
            evidence: vec![evidence],
            summary: b"Continue with the remaining release gates".to_vec(),
            wait: None,
            occurred_at: at(20),
        })
        .await
        .expect("propose evaluation");

    // The source Run ends (a queued cancellation is terminal immediately).
    let after_admission = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation after admission");
    application
        .cancel_run(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "cancel-goal-source",
                    now,
                    current(&after_admission),
                ),
                CancelRun {
                    run_id: source_run_id,
                    reason: CancelReason::UserRequested,
                },
            )
            .expect("cancel command"),
        )
        .await
        .expect("cancel source Run");
    let evaluation = goals
        .claim_terminal_evaluations(ClaimStandingGoalEvaluations {
            tenant_id: actor.tenant_id,
            worker_id: "standing-goal-admission-test".to_owned(),
            now: database_now(&pool).await,
            lease_millis: 60_000,
            limit: 10,
        })
        .await
        .expect("claim terminal evaluation")
        .into_iter()
        .find(|claim| claim.inspection.goal.goal_id == goal_id)
        .expect("terminal evaluation claim");
    GoalFixture {
        _postgres: postgres,
        pool,
        content,
        application,
        goals,
        actor,
        conversation_id,
        source_run_id,
        goal_id,
        pursuit_id,
        now,
        evaluation,
    }
}

/// Goal claims and the admission fence are evaluated against database time.
async fn database_now(pool: &sqlx::PgPool) -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(
        sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(pool)
            .await
            .expect("database clock"),
    )
}

async fn run_lineage(
    application: &RuntimeApplication,
    actor: Actor,
    run_id: RunId,
) -> (ActivityKind, Option<RunId>) {
    let view = application
        .get_run(musteragent_run::GetRun { actor, run_id })
        .await
        .expect("admitted Run");
    (
        view.run.context.activity_kind,
        view.run.context.parent_run_id,
    )
}

#[tokio::test]
async fn standing_goal_continuation_is_fenced_idempotent_and_keeps_the_frozen_profile() {
    let f = goal_fixture().await;
    let evaluated_at = database_now(&f.pool).await;
    let proposal = f.evaluation.proposal.clone().expect("proposal");
    f.goals
        .evaluate_run(EvaluateStandingGoalRun {
            tenant_id: f.actor.tenant_id,
            goal_id: f.goal_id,
            pursuit_id: f.pursuit_id,
            run_id: f.source_run_id,
            claim_token: f.evaluation.claim_token.clone(),
            evaluation_id: GoalEvaluationId::generate(),
            verdict: proposal.verdict,
            evidence: proposal.evidence,
            summary: f.evaluation.proposal_summary.clone().expect("summary"),
            deterministic: false,
            completion_confirmation_required: false,
            wait_barrier: proposal.wait_barrier,
            continuation_due_at: Some(evaluated_at),
            usage_delta: f.evaluation.run.usage,
            occurred_at: evaluated_at,
        })
        .await
        .expect("continue the Goal");
    let claim = f
        .goals
        .claim_due(ClaimStandingGoalPursuits {
            tenant_id: f.actor.tenant_id,
            worker_id: "standing-goal-continuation-test".to_owned(),
            now: database_now(&f.pool).await,
            lease_millis: 60_000,
            limit: 10,
        })
        .await
        .expect("claim continuation")
        .into_iter()
        .find(|claim| claim.inspection.goal.goal_id == f.goal_id)
        .expect("continuation claim");
    let StandingGoalPursuitSource::TerminalRun { source_run_id, .. } = &claim.source else {
        panic!("terminal Run source");
    };
    assert_eq!(*source_run_id, f.source_run_id);
    let goal = &claim.inspection.goal;
    let pursuit = claim.inspection.pursuit.as_ref().expect("Pursuit");
    let fence = StandingGoalAdmissionFence {
        goal_revision: goal.revision,
        pursuit_revision: pursuit.revision,
        claim_token: claim.claim_token.clone(),
    };
    let objective_ref = claim.processing_rules.as_ref().map_or_else(
        || goal.objective_ref.clone(),
        |rules| rules.objective_ref.clone(),
    );
    let summary_ref = claim
        .inspection
        .latest_evaluation
        .as_ref()
        .map(|evaluation| evaluation.summary_ref.clone());
    let admission = RuntimeStandingGoalAdmission::new(Arc::clone(&f.application));
    let deadline = f.now.checked_add_millis(600_000).expect("deadline");
    let prepare = |command_id: CommandId, source: RunId, fence: StandingGoalAdmissionFence| {
        admission.prepare_standing_goal_continuation(
            f.actor,
            f.conversation_id,
            source,
            f.goal_id,
            f.pursuit_id,
            fence,
            objective_ref.clone(),
            summary_ref.clone(),
            claim.input_refs.clone(),
            command_id,
            deadline,
            trace(command_id),
        )
    };

    let command_id = CommandId::generate();
    let prepared = prepare(command_id, f.source_run_id, fence.clone())
        .await
        .expect("prepare continuation");
    assert!(!prepared.already_admitted());
    assert_eq!(prepared.target_command_id(), command_id);
    let target_run_id = prepared.target_run_id();
    assert_eq!(
        target_run_id,
        derive_admission_run_id(command_id).expect("derived Run")
    );
    assert_eq!(
        prepared.execution_scope(),
        ExecutionScope::Tenant {
            tenant_id: f.actor.tenant_id
        }
    );
    let StandingGoalAdmissionOutcome::Admitted(receipt) = prepared
        .admit(signed_test_grant(f.actor, target_run_id, f.now), None)
        .await
        .expect("admit continuation")
    else {
        panic!("the current claim must admit");
    };
    assert_eq!(receipt.run_id, target_run_id);
    assert_eq!(receipt.disposition, AppendDisposition::Committed);
    let (activity_kind, parent) = run_lineage(&f.application, f.actor, target_run_id).await;
    assert_eq!(activity_kind, ActivityKind::Automation);
    assert_eq!(parent, Some(f.source_run_id));

    // The continuation is message-free and keeps the source Run's frozen model
    // configuration (route, model class and requested features).
    let snapshot = |run_id: RunId| {
        let pool = f.pool.clone();
        async move {
            let encoded: Vec<u8> = sqlx::query_scalar(
                "SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE run_id = $1",
            )
            .bind(run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("persisted Snapshot");
            ProtobufSnapshotCodec
                .decode_runtime_snapshot(&encoded)
                .expect("decode Snapshot")
        }
    };
    let source = snapshot(f.source_run_id).await;
    let continued = snapshot(target_run_id).await;
    assert_eq!(continued.model.route, source.model.route);
    assert_eq!(
        continued.model.requested_model_class,
        source.model.requested_model_class
    );
    assert_eq!(
        continued.output_policy.reasoning_summary,
        source.output_policy.reasoning_summary
    );
    let messages: i64 =
        sqlx::query_scalar("SELECT count(*) FROM conversation.messages WHERE conversation_id = $1")
            .bind(f.conversation_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .expect("message count");
    assert_eq!(
        messages, 1,
        "a continuation never fabricates a user message"
    );

    // Replaying the same target is a durable duplicate without a new grant.
    let replay = prepare(command_id, f.source_run_id, fence.clone())
        .await
        .expect("prepare replay");
    assert!(replay.already_admitted());
    assert_eq!(
        replay.replay_receipt().disposition,
        AppendDisposition::Duplicate
    );
    assert_eq!(replay.replay_receipt().run_id, target_run_id);

    // A stale claim cannot admit a second continuation for the same Pursuit.
    let stale_command = CommandId::generate();
    let stale = prepare(stale_command, f.source_run_id, fence)
        .await
        .expect("prepare with a stale claim");
    let stale_run = stale.target_run_id();
    assert_eq!(
        stale
            .admit(signed_test_grant(f.actor, stale_run, f.now), None)
            .await
            .expect("stale claim is a healthy no-op"),
        StandingGoalAdmissionOutcome::Superseded
    );
    let stale_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE run_id = $1")
        .bind(stale_run.into_uuid())
        .fetch_one(&f.pool)
        .await
        .expect("stale Run count");
    assert_eq!(stale_rows, 0, "a superseded admission commits nothing");

    // A non-terminal source cannot be continued.
    let denied = prepare(
        CommandId::generate(),
        target_run_id,
        StandingGoalAdmissionFence {
            goal_revision: goal.revision,
            pursuit_revision: pursuit.revision,
            claim_token: claim.claim_token.clone(),
        },
    )
    .await
    .err()
    .expect("queued source must be refused");
    assert_eq!(denied.code, ErrorCode::PolicyDenied);
}

#[tokio::test]
async fn standing_goal_judge_is_an_evaluation_run_bound_to_the_terminal_source() {
    let f = goal_fixture().await;
    let goal = &f.evaluation.inspection.goal;
    let pursuit = f.evaluation.inspection.pursuit.as_ref().expect("Pursuit");
    let fence = StandingGoalAdmissionFence {
        goal_revision: goal.revision,
        pursuit_revision: pursuit.revision,
        claim_token: f.evaluation.claim_token.clone(),
    };
    let judge_input_ref = f
        .content
        .put(PutContent {
            tenant_id: f.actor.tenant_id,
            media_type: MediaType::new("application/vnd.musteragent.standing-goal-judge.v2+json")
                .expect("judge media type"),
            bytes: br#"{"objective":"release","proposal":"continue"}"#.to_vec(),
        })
        .await
        .expect("judge input");
    let command_id =
        standing_goal_judge_command_id(f.pursuit_id, f.source_run_id).expect("judge command");
    let admission = RuntimeStandingGoalAdmission::new(Arc::clone(&f.application));
    let deadline = f.now.checked_add_millis(600_000).expect("deadline");
    let prepare = || {
        admission.prepare_standing_goal_judge(
            f.actor,
            f.conversation_id,
            f.source_run_id,
            f.goal_id,
            f.pursuit_id,
            fence.clone(),
            judge_input_ref.clone(),
            command_id,
            deadline,
            trace(command_id),
        )
    };
    let prepared = prepare().await.expect("prepare judge");
    assert!(!prepared.already_admitted());
    let judge_run_id = prepared.target_run_id();
    let StandingGoalAdmissionOutcome::Admitted(receipt) = prepared
        .admit(signed_test_grant(f.actor, judge_run_id, f.now), None)
        .await
        .expect("admit judge")
    else {
        panic!("the evaluation claim must admit its judge");
    };
    assert_eq!(receipt.disposition, AppendDisposition::Committed);
    let (activity_kind, parent) = run_lineage(&f.application, f.actor, judge_run_id).await;
    assert_eq!(activity_kind, ActivityKind::Evaluation);
    assert_eq!(parent, Some(f.source_run_id));
    let replay = prepare().await.expect("prepare judge replay");
    assert!(replay.already_admitted());
    assert_eq!(replay.replay_receipt().run_id, judge_run_id);

    // A judge for a source in another Conversation is refused before any grant.
    let foreign = admission
        .prepare_standing_goal_judge(
            f.actor,
            ConversationId::generate(),
            f.source_run_id,
            f.goal_id,
            f.pursuit_id,
            fence.clone(),
            judge_input_ref.clone(),
            CommandId::generate(),
            deadline,
            trace(command_id),
        )
        .await
        .err()
        .expect("foreign Conversation must be refused");
    assert_ne!(foreign.code, ErrorCode::InternalInvariantViolated);
}

/// Host grant fixture: signs exactly the Run the supervisor asks for.
struct FixtureContinuationAuthority {
    actor: Actor,
    now: UtcTimestamp,
    requests: std::sync::Mutex<
        Vec<musteragent_runtime_automation::StandingGoalContinuationAuthorityRequest>,
    >,
}

impl musteragent_runtime_automation::StandingGoalContinuationAuthority
    for FixtureContinuationAuthority
{
    fn grant(
        &self,
        request: musteragent_runtime_automation::StandingGoalContinuationAuthorityRequest,
    ) -> ApplicationFuture<
        '_,
        Result<
            musteragent_runtime_automation::StandingGoalContinuationGrant,
            musteragent_runtime_automation::StandingGoalContinuationAuthorityError,
        >,
    > {
        let signed = signed_test_grant(self.actor, request.target_run_id, self.now);
        self.requests.lock().expect("requests").push(request);
        Box::pin(async move {
            Ok(
                musteragent_runtime_automation::StandingGoalContinuationGrant {
                    signed_execution_grant: signed,
                    durable_continuation_authority: b"fixture-continuation-authority".to_vec(),
                },
            )
        })
    }
}

/// The disposable database runs on this host, so wall time is database time.
struct WallClock;

impl musteragent_runtime_capability::RuntimeClock for WallClock {
    fn now(&self) -> UtcTimestamp {
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("wall clock");
        UtcTimestamp::from_unix_millis(i64::try_from(wall.as_millis()).expect("millis"))
    }
}

#[tokio::test]
async fn supervisor_composed_from_public_ports_admits_the_next_goal_run() {
    let f = goal_fixture().await;
    let evaluated_at = database_now(&f.pool).await;
    let proposal = f.evaluation.proposal.clone().expect("proposal");
    f.goals
        .evaluate_run(EvaluateStandingGoalRun {
            tenant_id: f.actor.tenant_id,
            goal_id: f.goal_id,
            pursuit_id: f.pursuit_id,
            run_id: f.source_run_id,
            claim_token: f.evaluation.claim_token.clone(),
            evaluation_id: GoalEvaluationId::generate(),
            verdict: proposal.verdict,
            evidence: proposal.evidence,
            summary: f.evaluation.proposal_summary.clone().expect("summary"),
            deterministic: false,
            completion_confirmation_required: false,
            wait_barrier: proposal.wait_barrier,
            continuation_due_at: Some(evaluated_at),
            usage_delta: f.evaluation.run.usage,
            occurred_at: evaluated_at,
        })
        .await
        .expect("continue the Goal");
    let authority = Arc::new(FixtureContinuationAuthority {
        actor: f.actor,
        now: f.now,
        requests: std::sync::Mutex::new(Vec::new()),
    });
    let supervisor = musteragent_runtime_automation::StandingGoalSupervisor::new(
        "standing-goal-supervisor-test".to_owned(),
        Arc::new(StandingGoalProductApplication::new(
            Arc::new(PostgresStandingGoalProductStore::new(f.pool.clone())),
            f.content.clone(),
        )),
        Arc::new(RuntimeStandingGoalAdmission::new(Arc::clone(
            &f.application,
        ))),
        authority.clone(),
        Arc::new(WallClock),
    );
    let actors = PostgresStandingGoalProductStore::new(f.pool.clone())
        .supervisor_actors(database_now(&f.pool).await, 16)
        .await
        .expect("supervisor actors");
    assert!(actors.contains(&f.actor), "{actors:?}");
    let progressed = supervisor.run_pass(f.actor).await.expect("supervisor pass");
    assert!(progressed > 0, "the due continuation must be admitted");
    let requests = authority.requests.lock().expect("requests").clone();
    assert_eq!(requests.len(), 1);
    let target_run_id = requests[0].target_run_id;
    let (activity_kind, parent) = run_lineage(&f.application, f.actor, target_run_id).await;
    assert_eq!(activity_kind, ActivityKind::Automation);
    assert_eq!(parent, Some(f.source_run_id));
    let inspection = f
        .goals
        .inspect_owned(f.actor.tenant_id, f.goal_id, f.actor.principal_id)
        .await
        .expect("inspect Goal");
    assert_eq!(
        inspection
            .pursuit
            .as_ref()
            .and_then(|pursuit| pursuit.active_run_id),
        Some(target_run_id)
    );
    // A second pass finds nothing due and admits nothing new.
    supervisor.run_pass(f.actor).await.expect("idle pass");
    assert_eq!(authority.requests.lock().expect("requests").len(), 1);
}
