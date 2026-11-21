//! Unit and application tests for K9:
//! - Standing-Goal continuation refusal carrying a host-supplied `SafeDisplayText`
//!   into the durable wait barrier, halting continuation until explicit resume;
//! - Default fallback reason when no host reason is supplied;
//! - Retryable continuation authority failures backing off without blocking;
//! - `ScheduleView` `pause_reason` serialization and `@2` tool schema projection.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_event_envelope::ExecutionScope;
use musteragent_execution_grant::{
    EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1, EncodedExecutionGrantProof, ExecutionGrantProofHash,
};
use musteragent_goal::{
    GoalBudget, GoalCompletionContract, GoalEvaluation, GoalEvaluationProposal, GoalOrigin,
    GoalProcessingRules, GoalPursuit, GoalPursuitStatus, GoalRiskClass, GoalWaitKind, StandingGoal,
    StandingGoalMode, StandingGoalStatus,
};
use musteragent_identity::{
    Actor, AgentId, CommandId, ConversationId, GoalId, GoalPursuitId, MessageId, PrincipalId,
    PrincipalKind, RunId, TenantId, UtcTimestamp,
};
use musteragent_runtime_automation::{
    AcceptStandingGoalInput, ClaimStandingGoalEvaluations, ClaimStandingGoalPursuits,
    CreateStandingGoal, DeferStandingGoalEvaluation, DeleteStandingGoal,
    RecordStandingGoalAdmissionFailure, RecordStandingGoalWakeSignal, ResumeStandingGoalWait,
    ScheduleAdmissionHookError, ScheduleTriggerOwner, ScheduleView, ScheduledDefinition,
    StandingGoalAdmission, StandingGoalAdmissionFence, StandingGoalAdmissionOutcome,
    StandingGoalAdmissionReceipt, StandingGoalContinuationAuthority,
    StandingGoalContinuationAuthorityError, StandingGoalContinuationAuthorityFailure,
    StandingGoalContinuationAuthorityRequest, StandingGoalContinuationGrant, StandingGoalDeletion,
    StandingGoalEvaluationRetry, StandingGoalInputOutcome, StandingGoalMutationOutcome,
    StandingGoalPreparedAdmission, StandingGoalProductApplication, StandingGoalProductError,
    StandingGoalProductStore, StandingGoalProgressPhase, StandingGoalPursuitSource,
    StandingGoalReviewRetirement, StandingGoalRunPhaseObservation, StandingGoalStoreError,
    StandingGoalSupervisor, StandingGoalWakeSignalDisposition, StandingGoalWithPursuit,
    StoredStandingGoalEvaluationClaim, StoredStandingGoalPursuitClaim, WakeDueStandingGoalWaits,
    WakeEventStandingGoalWaits,
};
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, RuntimeError, SafeDisplayText, TraceContext,
};
use musteragent_schedule::{ConcurrencyPolicy, LatenessPolicy, RetrySpec, ScheduleSpec};

#[derive(Default)]
struct MemoryContent(Mutex<BTreeMap<musteragent_identity::ContentId, (TenantId, Vec<u8>)>>);

impl ContentStore for MemoryContent {
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            let reference = ContentRef {
                content_id: musteragent_identity::ContentId::generate(),
                media_type: request.media_type,
                sha256: Sha256::digest(&request.bytes),
                byte_length: request.bytes.len() as u64,
            };
            self.0
                .lock()
                .expect("content mutex")
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
            let rows = self.0.lock().expect("content mutex");
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

#[derive(Default)]
struct MemoryStandingGoalStore {
    rows: Mutex<BTreeMap<(TenantId, GoalId), StandingGoalWithPursuit>>,
    source_run_id: Mutex<Option<RunId>>,
    claims_issued: Mutex<u32>,
}

impl StandingGoalProductStore for MemoryStandingGoalStore {
    fn load_processing_rules(
        &self,
        _tenant_id: TenantId,
        _goal_id: GoalId,
        _run_id: RunId,
    ) -> ApplicationFuture<'_, Result<Option<GoalProcessingRules>, StandingGoalStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn accept_input(
        &self,
        _request: AcceptStandingGoalInput,
    ) -> ApplicationFuture<'_, Result<StandingGoalInputOutcome, StandingGoalStoreError>> {
        Box::pin(async { Err(StandingGoalStoreError::Unavailable) })
    }

    fn load_deletion(
        &self,
        _tenant_id: TenantId,
        _goal_id: GoalId,
        _principal_id: PrincipalId,
        _conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<Option<StandingGoalDeletion>, StandingGoalStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn load_evaluation_retry(
        &self,
        _tenant_id: TenantId,
        _pursuit_id: GoalPursuitId,
        _run_id: RunId,
    ) -> ApplicationFuture<'_, Result<Option<StandingGoalEvaluationRetry>, StandingGoalStoreError>>
    {
        Box::pin(async { Ok(None) })
    }

    fn create(
        &self,
        goal: StandingGoal,
        pursuit: Option<GoalPursuit>,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            self.rows
                .lock()
                .expect("rows")
                .insert((goal.tenant_id, goal.goal_id), (goal, pursuit));
            Ok(StandingGoalMutationOutcome::Applied)
        })
    }

    fn load(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
    ) -> ApplicationFuture<'_, Result<Option<StandingGoalWithPursuit>, StandingGoalStoreError>>
    {
        Box::pin(async move {
            Ok(self
                .rows
                .lock()
                .expect("rows")
                .get(&(tenant_id, goal_id))
                .cloned())
        })
    }

    fn list_for_conversation(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<Vec<StandingGoalWithPursuit>, StandingGoalStoreError>> {
        Box::pin(async move {
            Ok(self
                .rows
                .lock()
                .expect("rows")
                .iter()
                .filter(|((t, _), (g, _))| *t == tenant_id && g.conversation_id == conversation_id)
                .map(|(_, pair)| pair.clone())
                .collect())
        })
    }

    fn load_latest_evaluation(
        &self,
        _tenant_id: TenantId,
        _pursuit_id: GoalPursuitId,
    ) -> ApplicationFuture<'_, Result<Option<GoalEvaluation>, StandingGoalStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn load_run_phase(
        &self,
        _tenant_id: TenantId,
        _run_id: RunId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<StandingGoalRunPhaseObservation>, StandingGoalStoreError>,
    > {
        Box::pin(async { Ok(None) })
    }

    fn resolve_message_source_run(
        &self,
        _tenant_id: TenantId,
        _conversation_id: ConversationId,
        _message_id: MessageId,
    ) -> ApplicationFuture<'_, Result<Option<RunId>, StandingGoalStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn replace_goal(
        &self,
        goal: StandingGoal,
        expected_revision: u64,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            let mut rows = self.rows.lock().expect("rows");
            match rows.get_mut(&(goal.tenant_id, goal.goal_id)) {
                Some((existing, _)) if existing.revision == expected_revision => {
                    *existing = goal;
                    Ok(StandingGoalMutationOutcome::Applied)
                }
                Some(_) => Ok(StandingGoalMutationOutcome::Conflict),
                None => Ok(StandingGoalMutationOutcome::NotFound),
            }
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
            let mut rows = self.rows.lock().expect("rows");
            match rows.get_mut(&(goal.tenant_id, goal.goal_id)) {
                Some((eg, Some(ep)))
                    if eg.revision == expected_goal_revision
                        && ep.revision == expected_pursuit_revision =>
                {
                    *eg = goal;
                    *ep = pursuit;
                    Ok(StandingGoalMutationOutcome::Applied)
                }
                Some(_) => Ok(StandingGoalMutationOutcome::Conflict),
                None => Ok(StandingGoalMutationOutcome::NotFound),
            }
        })
    }

    fn activate_goal_with_pursuit(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async move {
            let mut rows = self.rows.lock().expect("rows");
            match rows.get_mut(&(goal.tenant_id, goal.goal_id)) {
                Some((eg, ep)) if eg.revision == expected_goal_revision => {
                    *eg = goal;
                    *ep = Some(pursuit);
                    Ok(StandingGoalMutationOutcome::Applied)
                }
                Some(_) => Ok(StandingGoalMutationOutcome::Conflict),
                None => Ok(StandingGoalMutationOutcome::NotFound),
            }
        })
    }

    fn list_completion_reviews(
        &self,
        _tenant: TenantId,
        _limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<(GoalId, GoalPursuitId)>, StandingGoalStoreError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn retire_completion_review(
        &self,
        _retirement: StandingGoalReviewRetirement,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async { Ok(StandingGoalMutationOutcome::NotFound) })
    }

    fn delete(
        &self,
        _request: DeleteStandingGoal,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async { Err(StandingGoalStoreError::Unavailable) })
    }

    fn claim_due(
        &self,
        request: ClaimStandingGoalPursuits,
    ) -> ApplicationFuture<'_, Result<Vec<StoredStandingGoalPursuitClaim>, StandingGoalStoreError>>
    {
        Box::pin(async move {
            let source_run_id = self
                .source_run_id
                .lock()
                .expect("source_run_id")
                .expect("configured source run");
            let envelope = b"opaque-source-proof".to_vec();
            let proof = EncodedExecutionGrantProof::restore(
                musteragent_identity::DottedKey::new(EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1)
                    .expect("proof schema"),
                envelope.clone(),
                ExecutionGrantProofHash::digest(&envelope),
                ExecutionGrantProofHash::digest(b"opaque-claims"),
            )
            .expect("encoded proof");
            let rows = self.rows.lock().expect("rows");
            let mut claims = Vec::new();
            for ((t, _), (goal, pursuit)) in rows.iter() {
                if *t == request.tenant_id
                    && goal.status == StandingGoalStatus::Active
                    && let Some(pursuit) = pursuit
                    && pursuit.status == GoalPursuitStatus::Pending
                    && pursuit
                        .continuation_due_at
                        .is_some_and(|due| due <= request.now)
                {
                    let mut issued = self.claims_issued.lock().expect("claims_issued");
                    *issued += 1;
                    claims.push(StoredStandingGoalPursuitClaim {
                        processing_rules: None,
                        input_refs: Vec::new(),
                        goal: goal.clone(),
                        pursuit: pursuit.clone(),
                        claim_token: format!("claim-{}", *issued),
                        source: StandingGoalPursuitSource::TerminalRun {
                            source_run_id,
                            source_execution_grant_proof: proof.clone(),
                            continuation_authority: Some(b"durable-token".to_vec()),
                        },
                    });
                }
            }
            Ok(claims)
        })
    }

    fn wake_due_waits(
        &self,
        _request: WakeDueStandingGoalWaits,
    ) -> ApplicationFuture<'_, Result<u16, StandingGoalStoreError>> {
        Box::pin(async { Ok(0) })
    }

    fn wake_event_waits(
        &self,
        _request: WakeEventStandingGoalWaits,
    ) -> ApplicationFuture<'_, Result<u16, StandingGoalStoreError>> {
        Box::pin(async { Ok(0) })
    }

    fn record_wake_signal(
        &self,
        _request: RecordStandingGoalWakeSignal,
    ) -> ApplicationFuture<'_, Result<StandingGoalWakeSignalDisposition, StandingGoalStoreError>>
    {
        Box::pin(async { Ok(StandingGoalWakeSignalDisposition::Recorded) })
    }

    fn settle_admission_claim(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
        _claim_token: String,
        _occurred_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        self.replace_goal_and_pursuit(
            goal,
            pursuit,
            expected_goal_revision,
            expected_pursuit_revision,
        )
    }

    fn submit_evaluation_proposal(
        &self,
        _proposal: GoalEvaluationProposal,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async { Err(StandingGoalStoreError::Unavailable) })
    }

    fn claim_terminal_evaluations(
        &self,
        _request: ClaimStandingGoalEvaluations,
    ) -> ApplicationFuture<'_, Result<Vec<StoredStandingGoalEvaluationClaim>, StandingGoalStoreError>>
    {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn defer_terminal_evaluation(
        &self,
        _request: DeferStandingGoalEvaluation,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async { Err(StandingGoalStoreError::Unavailable) })
    }

    fn commit_evaluation(
        &self,
        _goal: StandingGoal,
        _pursuit: GoalPursuit,
        _evaluation: GoalEvaluation,
        _expected_goal_revision: u64,
        _expected_pursuit_revision: u64,
        _claim_token: String,
        _input_settlement: Option<RunId>,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>> {
        Box::pin(async { Err(StandingGoalStoreError::Unavailable) })
    }
}

struct StubPreparedContinuation {
    target_command_id: CommandId,
    target_run_id: RunId,
    execution_scope: ExecutionScope,
}

impl StandingGoalPreparedAdmission for StubPreparedContinuation {
    fn target_command_id(&self) -> CommandId {
        self.target_command_id
    }

    fn target_run_id(&self) -> RunId {
        self.target_run_id
    }

    fn execution_scope(&self) -> ExecutionScope {
        self.execution_scope
    }

    fn already_admitted(&self) -> bool {
        false
    }

    fn replay_receipt(&self) -> StandingGoalAdmissionReceipt {
        panic!("not already admitted")
    }

    fn admit(
        self: Box<Self>,
        _signed_grant: musteragent_execution_grant::SignedExecutionGrant,
        _continuation_authority: Option<Vec<u8>>,
    ) -> ApplicationFuture<'static, Result<StandingGoalAdmissionOutcome, RuntimeError>> {
        Box::pin(async { panic!("admit must not execute when continuation authority refuses") })
    }
}

struct StubRuntimeAdmission;
impl StandingGoalAdmission for StubRuntimeAdmission {
    fn prepare_standing_goal_continuation(
        &self,
        actor: Actor,
        _conversation_id: ConversationId,
        _source_run_id: RunId,
        _goal_id: GoalId,
        _pursuit_id: GoalPursuitId,
        _fence: StandingGoalAdmissionFence,
        _objective_ref: ContentRef,
        _previous_evaluation_summary_ref: Option<ContentRef>,
        _input_refs: Vec<ContentRef>,
        target_command_id: CommandId,
        _deadline: UtcTimestamp,
        _trace_context: TraceContext,
    ) -> ApplicationFuture<'_, Result<Box<dyn StandingGoalPreparedAdmission>, RuntimeError>> {
        Box::pin(async move {
            let derived =
                musteragent_runtime_admission::AdmissionIdentities::derive(target_command_id)
                    .expect("valid command id");
            Ok(Box::new(StubPreparedContinuation {
                target_command_id,
                target_run_id: derived.run_id,
                execution_scope: ExecutionScope::Tenant {
                    tenant_id: actor.tenant_id,
                },
            }) as Box<dyn StandingGoalPreparedAdmission>)
        })
    }

    fn prepare_standing_goal_judge(
        &self,
        _actor: Actor,
        _conversation_id: ConversationId,
        _source_run_id: RunId,
        _goal_id: GoalId,
        _pursuit_id: GoalPursuitId,
        _fence: StandingGoalAdmissionFence,
        _judge_input_ref: ContentRef,
        _target_command_id: CommandId,
        _deadline: UtcTimestamp,
        _trace_context: TraceContext,
    ) -> ApplicationFuture<'_, Result<Box<dyn StandingGoalPreparedAdmission>, RuntimeError>> {
        Box::pin(async { unreachable!("no terminal evaluations due in continuation test") })
    }
}

struct ScriptedHostAuthority(Mutex<StandingGoalContinuationAuthorityError>);

impl StandingGoalContinuationAuthority for ScriptedHostAuthority {
    fn grant(
        &self,
        _request: StandingGoalContinuationAuthorityRequest,
    ) -> ApplicationFuture<
        '_,
        Result<StandingGoalContinuationGrant, StandingGoalContinuationAuthorityError>,
    > {
        let error = self.0.lock().expect("authority mutex").clone();
        Box::pin(async move { Err(error) })
    }
}

struct MutableClock(Mutex<UtcTimestamp>);
impl RuntimeClock for MutableClock {
    fn now(&self) -> UtcTimestamp {
        *self.0.lock().expect("clock mutex")
    }
}

async fn seed_due_goal(
    store: &Arc<MemoryStandingGoalStore>,
    goals: &Arc<StandingGoalProductApplication>,
) -> (Actor, GoalId, GoalPursuitId) {
    let tenant_id = TenantId::generate();
    let goal_id = GoalId::generate();
    let pursuit_id = GoalPursuitId::generate();
    let principal_id = PrincipalId::generate();
    let source_run_id = RunId::generate();
    *store.source_run_id.lock().expect("source_run_id") = Some(source_run_id);

    let t0 = UtcTimestamp::from_unix_millis(1_000);
    let created = goals
        .create(CreateStandingGoal {
            tenant_id,
            goal_id,
            pursuit_id,
            principal_id,
            origin: GoalOrigin::User,
            creator_agent_id: None,
            definition: musteragent_runtime_automation::StandingGoalDefinition {
                mode: StandingGoalMode::Finite,
                target_agent_id: AgentId::generate(),
                conversation_id: ConversationId::generate(),
                title: "Reconcile inventory records".into(),
                objective: b"Continuously reconcile inventory deltas.".to_vec(),
                constraints: Vec::new(),
                completion: GoalCompletionContract {
                    risk_class: GoalRiskClass::Standard,
                    success_criteria: vec!["Reconciliation report stored".into()],
                    evidence_requirements: Vec::new(),
                    failure_conditions: Vec::new(),
                    minimum_evidence_items: 0,
                },
                budget: GoalBudget {
                    max_runs: 10,
                    max_elapsed_millis: 3_600_000,
                    max_model_tokens: 100_000,
                    max_tool_invocations: 100,
                    max_consecutive_failures: 3,
                },
            },
            activate: false,
            initial_run_id: None,
            occurred_at: t0,
        })
        .await
        .expect("create draft standing goal");

    let mut active_goal = created.goal;
    active_goal.status = StandingGoalStatus::Active;
    active_goal.active_pursuit_id = Some(pursuit_id);
    active_goal.revision = 2;
    active_goal.validate().expect("valid active standing goal");
    let due_pursuit = GoalPursuit {
        pursuit_id,
        goal_id,
        tenant_id,
        status: GoalPursuitStatus::Pending,
        active_run_id: None,
        continuation_source_run_id: Some(source_run_id),
        wait_barrier: None,
        budget_usage: Default::default(),
        continuation_due_at: Some(UtcTimestamp::from_unix_millis(2_000)),
        started_at: t0,
        updated_at: t0,
        completed_at: None,
        revision: 1,
    };
    due_pursuit.validate().expect("valid pending pursuit");
    store
        .activate_goal_with_pursuit(active_goal, due_pursuit, 1)
        .await
        .expect("seed active goal + due pursuit");

    (
        Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        },
        goal_id,
        pursuit_id,
    )
}

#[tokio::test]
async fn standing_goal_host_refusal_reason_surfaces_in_wait_barrier_and_halts_until_explicit_resume()
 {
    let store = Arc::new(MemoryStandingGoalStore::default());
    let content = Arc::new(MemoryContent::default());
    let goals = Arc::new(StandingGoalProductApplication::new(
        store.clone(),
        content.clone(),
    ));
    let (actor, goal_id, pursuit_id) = seed_due_goal(&store, &goals).await;

    let host_reason = SafeDisplayText::new(
        "Required external analytics capability was revoked; reconnect integration to continue.",
    )
    .expect("valid host refusal reason");
    let clock = Arc::new(MutableClock(Mutex::new(UtcTimestamp::from_unix_millis(
        5_000,
    ))));
    let supervisor = StandingGoalSupervisor::new(
        "k9-worker-1".into(),
        goals.clone(),
        Arc::new(StubRuntimeAdmission),
        Arc::new(ScriptedHostAuthority(Mutex::new(
            StandingGoalContinuationAuthorityError::denied(host_reason.clone()),
        ))),
        clock.clone(),
    );

    supervisor
        .run_pass(actor)
        .await
        .expect("supervisor settles permanent host refusal");

    let inspected = goals
        .inspect(actor.tenant_id, goal_id)
        .await
        .expect("inspect settled standing goal");
    assert_eq!(inspected.goal.status, StandingGoalStatus::Active);
    assert_eq!(inspected.progress.phase, StandingGoalProgressPhase::Waiting);
    let settled_pursuit = inspected.pursuit.expect("settled pursuit");
    assert_eq!(settled_pursuit.status, GoalPursuitStatus::Waiting);
    assert_eq!(settled_pursuit.continuation_due_at, None);

    let barrier = settled_pursuit
        .wait_barrier
        .expect("external dependency wait barrier");
    assert_eq!(barrier.kind, GoalWaitKind::ExternalDependency);
    assert_eq!(
        barrier.correlation_key,
        format!("admission-blocked:{pursuit_id}")
    );
    let stored_reason = content
        .read(actor.tenant_id, &barrier.reason_ref)
        .await
        .expect("read wait barrier reason");
    assert_eq!(
        std::str::from_utf8(&stored_reason).expect("utf-8 reason"),
        host_reason.as_str()
    );
    assert_eq!(*store.claims_issued.lock().expect("claims_issued"), 1);

    // Subsequent supervisor passes at later timestamps must NOT resume or re-claim.
    *clock.0.lock().expect("clock") = UtcTimestamp::from_unix_millis(600_000);
    supervisor
        .run_pass(actor)
        .await
        .expect("subsequent pass leaves blocked goal halted");
    assert_eq!(*store.claims_issued.lock().expect("claims_issued"), 1);
    let still_waiting = goals
        .inspect(actor.tenant_id, goal_id)
        .await
        .expect("inspect still-waiting goal");
    assert_eq!(
        still_waiting.pursuit.expect("pursuit").status,
        GoalPursuitStatus::Waiting
    );

    // Explicit user resume clears the wait barrier and makes continuation due again.
    let resumed = goals
        .resume_wait(ResumeStandingGoalWait {
            tenant_id: actor.tenant_id,
            goal_id,
            pursuit_id,
            principal_id: actor.principal_id,
            correlation_key: barrier.correlation_key,
            occurred_at: UtcTimestamp::from_unix_millis(601_000),
        })
        .await
        .expect("explicit user resume clears admission barrier");
    let resumed_pursuit = resumed.pursuit.expect("resumed pursuit");
    assert_eq!(resumed_pursuit.status, GoalPursuitStatus::Pending);
    assert!(resumed_pursuit.wait_barrier.is_none());
    assert_eq!(
        resumed_pursuit.continuation_due_at,
        Some(UtcTimestamp::from_unix_millis(601_000))
    );
}

#[tokio::test]
async fn standing_goal_refusal_without_host_reason_preserves_default_message_and_retryable_defers()
{
    let store = Arc::new(MemoryStandingGoalStore::default());
    let content = Arc::new(MemoryContent::default());
    let goals = Arc::new(StandingGoalProductApplication::new(
        store.clone(),
        content.clone(),
    ));
    let (actor, goal_id, pursuit_id) = seed_due_goal(&store, &goals).await;

    // Transient dependency failure must retry (keep Pending + continuation_due_at) and never attach a reason.
    let authority = Arc::new(ScriptedHostAuthority(Mutex::new(
        StandingGoalContinuationAuthorityError {
            runtime_code: Some(ErrorCode::DependencyUnavailable),
            correlation_id: None,
            kind: StandingGoalContinuationAuthorityFailure::DependencyUnavailable,
            retryable: true,
            reason: Some(
                SafeDisplayText::new("Transient upstream timeout").expect("valid display text"),
            ),
        },
    )));
    let clock = Arc::new(MutableClock(Mutex::new(UtcTimestamp::from_unix_millis(
        5_000,
    ))));
    let supervisor = StandingGoalSupervisor::new(
        "k9-worker-2".into(),
        goals.clone(),
        Arc::new(StubRuntimeAdmission),
        authority.clone(),
        clock.clone(),
    );
    supervisor
        .run_pass(actor)
        .await
        .expect("retryable failure defers cleanly");
    let retried = goals
        .inspect(actor.tenant_id, goal_id)
        .await
        .expect("inspect retried goal")
        .pursuit
        .expect("pursuit");
    assert_eq!(retried.status, GoalPursuitStatus::Pending);
    assert!(retried.wait_barrier.is_none());
    assert!(retried.continuation_due_at.is_some());

    // Non-permanent record_admission_failure with a reason is rejected at the product boundary.
    assert_eq!(
        goals
            .record_admission_failure(RecordStandingGoalAdmissionFailure {
                tenant_id: actor.tenant_id,
                goal_id,
                pursuit_id,
                claim_token: "claim-invalid".into(),
                permanent: false,
                retry_at: Some(UtcTimestamp::from_unix_millis(10_000)),
                occurred_at: UtcTimestamp::from_unix_millis(6_000),
                reason: Some(SafeDisplayText::new("Not allowed on transient").unwrap()),
            })
            .await
            .expect_err("transient failure cannot carry wait barrier reason"),
        StandingGoalProductError::InvalidRequest
    );

    // Permanent denial without a host reason uses the default guidance message.
    *authority.0.lock().expect("authority") = StandingGoalContinuationAuthorityError {
        runtime_code: Some(ErrorCode::PolicyDenied),
        correlation_id: None,
        kind: StandingGoalContinuationAuthorityFailure::Denied,
        retryable: false,
        reason: None,
    };
    *clock.0.lock().expect("clock") = UtcTimestamp::from_unix_millis(10_000);
    supervisor
        .run_pass(actor)
        .await
        .expect("permanent denial settles");
    let blocked = goals
        .inspect(actor.tenant_id, goal_id)
        .await
        .expect("inspect blocked goal")
        .pursuit
        .expect("pursuit");
    let barrier = blocked.wait_barrier.expect("barrier");
    let stored_reason = content
        .read(actor.tenant_id, &barrier.reason_ref)
        .await
        .expect("read default reason");
    assert_eq!(
        std::str::from_utf8(&stored_reason).expect("utf-8"),
        "Execution admission was denied. Resolve account, permission or configuration requirements, then explicitly continue."
    );
}

#[test]
fn schedule_view_serializes_pause_reason_for_hosts_and_strips_for_frozen_v2_tools() {
    let definition = ScheduledDefinition {
        title: "Hourly Reconciliation".into(),
        instructions: "Reconcile latest records.".into(),
        timing: ScheduleSpec::Interval {
            every_millis: 3_600_000,
            anchor: UtcTimestamp::from_unix_millis(0),
        },
        timezone: "UTC".into(),
        concurrency: ConcurrencyPolicy::Skip,
        lateness: LatenessPolicy::Skip,
        lateness_window_millis: 60_000,
        retry: RetrySpec::default(),
    };
    definition.validate().expect("valid schedule definition");

    let reason = SafeDisplayText::new("Required external capability was disabled by administrator")
        .expect("valid reason");
    let paused_view = ScheduleView {
        schedule_id: uuid::Uuid::nil(),
        revision: 2,
        definition,
        status: "paused".into(),
        next_run_at_unix_ms: None,
        owner: ScheduleTriggerOwner::Cloud,
        owner_epoch: 1,
        pending_owner: None,
        handover_due_at_unix_ms: None,
        pause_reason: Some(reason.clone()),
    };

    let host_json = serde_json::to_value(&paused_view).expect("serialize host ScheduleView");
    assert_eq!(host_json["status"], "paused");
    assert_eq!(host_json["pause_reason"], reason.as_str());
    let roundtrip: ScheduleView =
        serde_json::from_value(host_json.clone()).expect("roundtrip host ScheduleView");
    assert_eq!(roundtrip, paused_view);

    let v2_tool_json =
        serde_json::to_value(paused_view.without_pause_reason()).expect("serialize v2 tool view");
    assert!(
        v2_tool_json.get("pause_reason").is_none(),
        "without_pause_reason omits pause_reason for additionalProperties: false @2 tools"
    );
    let legacy_roundtrip: ScheduleView =
        serde_json::from_value(v2_tool_json).expect("deserialize legacy receipt without field");
    assert_eq!(legacy_roundtrip.pause_reason, None);

    // Control characters or empty strings in serialized pause_reason are rejected on decode.
    let mut corrupt = host_json;
    corrupt["pause_reason"] = serde_json::json!("bad\0reason");
    assert!(serde_json::from_value::<ScheduleView>(corrupt).is_err());

    let retryable = ScheduleAdmissionHookError::retryable(reason.clone())
        .with_runtime_code(ErrorCode::DependencyUnavailable);
    assert!(retryable.retryable);
    assert_eq!(
        retryable.runtime_code,
        Some(ErrorCode::DependencyUnavailable)
    );
    let permanent = ScheduleAdmissionHookError::permanent(reason);
    assert!(!permanent.retryable);
}
