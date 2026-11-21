use musteragent_conformance::fixtures::agent::{
    FIXTURE_RUNTIME_POLICY_V1, fixture_runtime_policy_content_ref,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[path = "support/live_runtime.rs"]
mod live_runtime;
#[path = "support/owned_scripted_model.rs"]
mod owned_model;
#[path = "support/production_context.rs"]
mod production_context;
#[path = "postgres_interaction_resume/races.rs"]
mod races;
#[path = "support/running_loop.rs"]
mod running_loop;
#[path = "postgres_interaction_resume/support.rs"]
mod support;

use musteragent_approval::{
    ApprovalDecision, InteractionResponseAuthority, InteractionResponseRef, RespondInteraction,
};
use musteragent_capability::{
    CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation, CapabilityManifest,
    IdempotencyContract,
};
use musteragent_cloud_worker::{
    CapabilityDispatcher, CompositeCapabilityDispatcher, DispatchedCapabilityOutcome,
    DispatchedReconciliationOutcome, DurableToolExecutor, InteractionCapabilityHandler,
};
use musteragent_conformance::MemoryBlobs;
use musteragent_conformance::fixtures::agent::{FIXTURE_PERSONA_V1, fixture_version_v1};
use musteragent_content_ref::{MediaType, SchemaRef, Sha256};
use musteragent_identity::{
    Actor, CapabilityId, CapabilityProviderId, CommandId, ConversationSeq, CorrelationId,
    DottedKey, IdempotencyKey, InteractionId, InvocationId, PrincipalKind, ToolCallId,
    UtcTimestamp, VersionedKey,
};
use musteragent_loop::{
    AgentLoopExit, DefaultLoopPolicy, DurableAgentLoop, LoopPortError, ToolExecutor,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_policy::{ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy};
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresContentStore, PostgresDispatchStore,
    PostgresFencedToolInvocationJournal, PostgresInteractionApplicationStore, migrate,
};
use musteragent_run::{AttemptContext, CancelRun, WorkerId};
use musteragent_runtime_capability::{
    ApprovalRequest, CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation,
    CapabilityRegistry, CapabilityResult, DurableToolInvocation, FencedToolInvocationJournal,
    InvocationAuthorization, RuntimeClock, ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, CancelReason, CommandDisposition, CommandEnvelope, CommandMetadata,
    ConversationGuard, ErrorCode, ModelClass, TraceContext, TraceParent,
};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalModelRequest, CanonicalRole, JsonValue, ModelEventStream,
    ModelProvider, ModelStreamEvent, ModelText, ModelUsage, StopReason,
};
use musteragent_runtime_session::InteractionResponseApplication;
use musteragent_runtime_snapshot::{
    AgentVersionSnapshot, ContextPolicySnapshot, ModelRouteSnapshot, OutputPolicySnapshot,
    PolicySnapshot, PromptAssemblySnapshot, PromptSectionSnapshot, ProtocolVersionSnapshot,
    RunExecutionPolicySnapshot, RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION,
    ToolDefinitionSnapshot,
};
use musteragent_scheduler::{
    ClaimOutcome, DispatchStore, DurableScheduler, LocalScheduleSignal, SchedulerPolicy,
};
use musteragent_skill::{SkillAuthorizationRequest, Surface, builtin_skill_catalog};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{
    BuiltinIdempotency, BuiltinToolCatalog, CapabilityExecutionTarget, ConcurrencyClass,
    ToolInvocation, ToolInvocationStatus, ToolKey, ToolManifest, ToolRetryContract, WireName,
};
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder};
use tokio_stream as stream;
use tokio_util::sync::CancellationToken;

use support::{
    EmptyControls, FixedClock, FixedExecutionPolicy, FixedMaterial, FixedTrace,
    assert_approval_audit_binding, legacy_tool_authorization, lock, port, provider_ref,
    response_model_key, runtime_application_store,
};

use production_context::install_production_context;
use running_loop::{Fixture, database_now, execute_loop, keyring, run_context, seed_running_loop};

const TOOL_CALL_ID: &str = "approval-write-call";
const TOOL_WIRE_NAME: &str = "approval_write";

struct ApprovedWriteProvider {
    content: Arc<dyn ContentStore>,
    physical_calls: AtomicUsize,
}

struct MutableRuntimeClock(AtomicI64);

impl MutableRuntimeClock {
    fn new(now: UtcTimestamp) -> Self {
        Self(AtomicI64::new(now.unix_millis()))
    }

    fn set(&self, now: UtcTimestamp) {
        self.0.store(now.unix_millis(), Ordering::SeqCst);
    }
}

impl RuntimeClock for MutableRuntimeClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(self.0.load(Ordering::SeqCst))
    }
}

impl CapabilityDispatcher for ApprovedWriteProvider {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<musteragent_tool_executor::ToolPhysicalExecution<'a>, LoopPortError> {
        Ok(live_runtime::invoke_operation(
            binding,
            self.invoke(attempt, material, durable, invocation),
        ))
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            self.physical_calls.fetch_add(1, Ordering::SeqCst);
            let result_ref = self
                .content
                .put(PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes: br#"{"approved":true,"written":true}"#.to_vec(),
                })
                .await
                .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
            Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(CapabilityResult {
                    result_hash: result_ref.sha256,
                    result_ref,
                    result_schema: invocation.result_schema,
                    provider_request_ref: None,
                }),
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::RunTransitionInvalid, false)) })
    }
}

struct ApprovalThenCompletionModel {
    invocations: AtomicUsize,
    observed_tool_result: AtomicBool,
    attempts: Mutex<Vec<musteragent_identity::AttemptId>>,
    expected_tool_result: &'static str,
}

impl ModelProvider for ApprovalThenCompletionModel {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        lock(&self.attempts).push(request.invocation.attempt_context.attempt_id);
        let ordinal = self.invocations.fetch_add(1, Ordering::SeqCst);
        let events = if ordinal == 0 {
            let call_id = ToolCallId::new(TOOL_CALL_ID).expect("call ID");
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("approval-model-tool"),
                    response_model_key: response_model_key(),
                },
                ModelStreamEvent::ToolCallStarted {
                    output_index: 0,
                    call_id: call_id.clone(),
                    name: WireName::new(TOOL_WIRE_NAME).expect("wire name"),
                    provider_item_ref: None,
                    provider_replay: None,
                },
                ModelStreamEvent::ToolCallCompleted {
                    output_index: 0,
                    call_id,
                    arguments: JsonValue::Object(BTreeMap::new()),
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 12,
                    output_tokens: 4,
                    total_tokens: Some(16),
                    ..ModelUsage::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::ToolCalls,
                },
            ]
        } else {
            self.observed_tool_result.store(
                request.input.iter().any(|message| {
                    message.role == CanonicalRole::Tool
                        && message
                            .tool_call_id
                            .as_ref()
                            .is_some_and(|id| id.as_str() == TOOL_CALL_ID)
                        && message.content.iter().any(|part| {
                            matches!(
                                part,
                                CanonicalContentPart::Text(text)
                                    if text.as_str().contains(self.expected_tool_result)
                            )
                        })
                }),
                Ordering::SeqCst,
            );
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("approval-model-final"),
                    response_model_key: response_model_key(),
                },
                ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new("The approved write completed once.").expect("text"),
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 18,
                    output_tokens: 6,
                    total_tokens: Some(24),
                    ..ModelUsage::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop,
                },
            ]
        };
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
}

struct StructuredInteractionThenCompletionModel {
    invocations: AtomicUsize,
    observed_response: AtomicBool,
    attempts: Mutex<Vec<musteragent_identity::AttemptId>>,
    tool_call_id: &'static str,
    request_arguments: JsonValue,
    expected_response_fragments: Vec<&'static str>,
}

impl ModelProvider for StructuredInteractionThenCompletionModel {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        lock(&self.attempts).push(request.invocation.attempt_context.attempt_id);
        let ordinal = self.invocations.fetch_add(1, Ordering::SeqCst);
        let events = if ordinal == 0 {
            let call_id = ToolCallId::new(self.tool_call_id).expect("call ID");
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("structured-input-model-tool"),
                    response_model_key: response_model_key(),
                },
                ModelStreamEvent::ToolCallStarted {
                    output_index: 0,
                    call_id: call_id.clone(),
                    name: WireName::new("interaction_request").expect("wire name"),
                    provider_item_ref: None,
                    provider_replay: None,
                },
                ModelStreamEvent::ToolCallCompleted {
                    output_index: 0,
                    call_id,
                    arguments: self.request_arguments.clone(),
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 14,
                    output_tokens: 5,
                    total_tokens: Some(19),
                    ..ModelUsage::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::ToolCalls,
                },
            ]
        } else {
            let observed = request.input.iter().any(|message| {
                message.role == CanonicalRole::Tool
                    && message
                        .tool_call_id
                        .as_ref()
                        .is_some_and(|id| id.as_str() == self.tool_call_id)
                    && message.content.iter().any(|part| {
                        matches!(
                            part,
                            CanonicalContentPart::Text(text)
                                if self.expected_response_fragments
                                    .iter()
                                    .all(|fragment| text.as_str().contains(fragment))
                        )
                    })
            });
            self.observed_response.store(observed, Ordering::SeqCst);
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("structured-input-model-final"),
                    response_model_key: response_model_key(),
                },
                ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new("Using the stable release channel.").expect("text"),
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 22,
                    output_tokens: 7,
                    total_tokens: Some(29),
                    ..ModelUsage::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop,
                },
            ]
        };
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
}

#[path = "postgres_interaction_resume/approval_timeout.rs"]
mod approval_timeout;
#[tokio::test]
async fn interaction_wait_requeue_new_attempt() {
    assert_approval_response(ApprovalDecision::Approve).await;
}

#[tokio::test]
async fn approval_denial_requeues_without_external_execution() {
    assert_approval_response(ApprovalDecision::Deny).await;
}

async fn assert_approval_response(decision: ApprovalDecision) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, first_attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let now = database_now(&pool).await;
    let provider_id = CapabilityProviderId::generate();
    let contracts = approval_contracts(fixture, provider_id, content.clone()).await;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        contracts.snapshot,
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let provider = Arc::new(ApprovedWriteProvider {
        content: content.clone(),
        physical_calls: AtomicUsize::new(0),
    });
    let tool_clock = Arc::new(MutableRuntimeClock::new(now));
    let tool_journal: Arc<dyn FencedToolInvocationJournal> = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            codec.clone(),
            codec.clone(),
            keys.clone(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            live_runtime::clock(),
        ),
    );
    let tool_executor: Arc<dyn ToolExecutor> = Arc::new(
        DurableToolExecutor::new(
            Arc::new(FixedMaterial(contracts.material)),
            tool_journal,
            content.clone(),
            provider.clone(),
            tool_clock.clone(),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(live_runtime::supervision(&pool)),
    );
    let model = Arc::new(ApprovalThenCompletionModel {
        invocations: AtomicUsize::new(0),
        observed_tool_result: AtomicBool::new(false),
        attempts: Mutex::new(Vec::new()),
        expected_tool_result: match decision {
            ApprovalDecision::Approve | ApprovalDecision::ApproveSession => "\"approved\":true",
            ApprovalDecision::Deny => "policy_denied",
        },
    });
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixedExecutionPolicy {
            admitted_at: now,
            snapshot: contracts.execution_policy,
        }),
        context,
        tool_executor,
        Arc::new(EmptyControls),
    ));
    let journal = Arc::new(
        PostgresAgentLoopJournal::new(
            pool.clone(),
            content.clone(),
            codec.clone(),
            codec.clone(),
            Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
            keys.clone(),
            running_loop::recovery_keyring(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            live_runtime::clock(),
        ),
    );
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        policy,
        Arc::new(owned_model::OwnedScriptedModel(model.clone())),
        journal,
    )
    .with_execution_supervision(live_runtime::supervision(&pool));

    let interaction_id = match execute_loop(&runtime, fixture, first_attempt.clone()).await {
        AgentLoopExit::Waiting { interaction_id, .. } => interaction_id,
        other => panic!("first Attempt must release at Interaction wait, got {other:?}"),
    };
    let waiting: (i16, i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, attempt.status, tool.status,
                (SELECT count(*) FROM runtime.leases WHERE run_id = run.run_id)
         FROM runtime.runs AS run
         JOIN runtime.attempts AS attempt ON attempt.attempt_id = $2
         JOIN tool.tool_invocations AS tool ON tool.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(first_attempt.attempt_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read waiting state");
    assert_eq!(waiting, (3, 2, 2, 0));
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);

    let expires_at_ms: Option<i64> = sqlx::query_scalar(
        "SELECT expires_at_ms FROM approval.interactions WHERE interaction_id = $1",
    )
    .bind(interaction_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read approval lifetime");
    assert_eq!(
        expires_at_ms,
        Some(
            now.checked_add_millis(24 * 60 * 60 * 1_000)
                .expect("approval expiry")
                .unix_millis()
        ),
        "human approval authority must expire after the frozen 24-hour lifetime"
    );

    let last_sequence = current_sequence(&pool, fixture).await;
    let stale_sequence = last_sequence
        .checked_sub(1)
        .expect("waiting Interaction must follow an earlier Conversation revision");
    // Use the database clock for the user response so its Ledger fact remains
    // causally ordered. The separate SQLite journal test covers a physically
    // stale request deadline; this end-to-end path verifies that the resumed
    // provider execution receives a deadline based on its actual start.
    let response_time = database_now(&pool).await;
    tool_clock.set(response_time);
    let application = InteractionResponseApplication::new(
        runtime_application_store(&pool),
        content,
        codec.clone(),
        Arc::new(FixedClock(response_time)),
        Arc::new(PostgresInteractionApplicationStore::new(
            pool.clone(),
            keys,
            codec.clone(),
        )),
    );
    let web_attempt = CommandEnvelope::new(
        metadata(
            CommandId::generate(),
            fixture,
            stale_sequence,
            response_time,
        ),
        RespondInteraction {
            interaction_id,
            response_authority: InteractionResponseAuthority::WebUserInput,
            response: InteractionResponseRef::Approval(decision),
        },
    )
    .expect("Web response command");
    assert_eq!(
        application
            .respond_interaction(web_attempt)
            .await
            .expect_err("Web user input must not resolve a Desktop approval")
            .code,
        ErrorCode::PolicyDenied,
    );
    let command_id = CommandId::generate();
    let respond = CommandEnvelope::new(
        // The approval was rendered at an earlier projection revision. The
        // application must rebase this safe pending-Interaction response
        // internally instead of requiring a second user click.
        metadata(command_id, fixture, stale_sequence, response_time),
        RespondInteraction {
            interaction_id,
            response_authority: InteractionResponseAuthority::DeviceUserPresence,
            response: InteractionResponseRef::Approval(decision),
        },
    )
    .expect("respond command");
    assert_eq!(
        application
            .respond_interaction(respond.clone())
            .await
            .expect("respond to Interaction")
            .disposition,
        CommandDisposition::Committed
    );
    assert_eq!(
        application
            .respond_interaction(respond)
            .await
            .expect("deduplicate Interaction response")
            .disposition,
        CommandDisposition::Duplicate
    );

    assert_approval_audit_binding(&pool, fixture.tenant_id, interaction_id, decision).await;

    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let receipt = match scheduler
        .claim_one(
            &WorkerId::new("worker-interaction-resume").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim resumed Attempt")
    {
        ClaimOutcome::Claimed(receipt) => receipt,
        ClaimOutcome::Idle { .. } => panic!("responded Interaction must make the Run claimable"),
    };
    let second_attempt = AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id,
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    };
    assert_ne!(second_attempt.attempt_id, first_attempt.attempt_id);
    assert!(second_attempt.fencing_token > first_attempt.fencing_token);

    assert!(matches!(
        execute_loop(&runtime, fixture, second_attempt.clone()).await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(
        provider.physical_calls.load(Ordering::SeqCst),
        usize::from(decision == ApprovalDecision::Approve)
    );
    assert_eq!(model.invocations.load(Ordering::SeqCst), 2);
    assert!(
        model.observed_tool_result.load(Ordering::SeqCst),
        "the new Attempt must rebuild production Context with the approval outcome"
    );
    assert_eq!(
        lock(&model.attempts).as_slice(),
        &[first_attempt.attempt_id, second_attempt.attempt_id]
    );

    if decision == ApprovalDecision::Approve {
        // ActivityV1 Tools carry no absolute deadline: the frozen activity
        // timing is enforced by execution supervision from the physical
        // start, which happens only after approval.
        let (started_at_ms, deadline_ms, has_activity_timing): (Option<i64>, Option<i64>, bool) =
            sqlx::query_as(
                "SELECT started_at_ms, deadline_ms, activity_timing IS NOT NULL
                 FROM tool.tool_invocations
                 WHERE run_id = $1",
            )
            .bind(fixture.run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("read execution timing");
        assert_eq!(started_at_ms, Some(response_time.unix_millis()));
        assert_eq!(
            deadline_ms, None,
            "ActivityV1 Tools have no legacy deadline"
        );
        assert!(
            has_activity_timing,
            "approved Tool must freeze ActivityV1 timing"
        );
    }

    let terminal: (i16, i16, i16, i64, i64, i64) = sqlx::query_as(
        "SELECT run.status, interaction.status, tool.status,
                (SELECT count(*) FROM runtime.attempts WHERE run_id = run.run_id),
                (SELECT count(*) FROM conversation.messages
                 WHERE run_id = run.run_id AND role = 2),
                (SELECT count(*) FROM event_store.outbox
                 WHERE conversation_id = run.conversation_id AND topic = 'run.dispatch')
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         JOIN tool.tool_invocations AS tool ON tool.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read terminal Interaction Run");
    assert_eq!(
        terminal,
        (
            5,
            2,
            if decision == ApprovalDecision::Approve {
                4
            } else {
                5
            },
            2,
            1,
            1
        )
    );
    pool.close().await;
}

#[tokio::test]
async fn approval_expire_never_approves() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, first_attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Perform the requested write only if approval arrives before expiry.",
    )
    .await;
    let now = database_now(&pool).await;
    let provider_id = CapabilityProviderId::generate();
    let contracts = approval_contracts(fixture, provider_id, content.clone()).await;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        contracts.snapshot,
        b"Perform the requested write only if approval arrives before expiry.",
    )
    .await;
    let provider = Arc::new(ApprovedWriteProvider {
        content: content.clone(),
        physical_calls: AtomicUsize::new(0),
    });
    let tool_executor: Arc<dyn ToolExecutor> = Arc::new(
        DurableToolExecutor::new(
            Arc::new(FixedMaterial(contracts.material)),
            Arc::new(
                PostgresFencedToolInvocationJournal::new(
                    pool.clone(),
                    codec.clone(),
                    codec.clone(),
                    keys.clone(),
                )
                .with_execution_liveness(
                    Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                    live_runtime::clock(),
                ),
            ),
            content.clone(),
            provider.clone(),
            Arc::new(FixedClock(now)),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(live_runtime::supervision(&pool)),
    );
    let model = Arc::new(ApprovalThenCompletionModel {
        invocations: AtomicUsize::new(0),
        observed_tool_result: AtomicBool::new(false),
        attempts: Mutex::new(Vec::new()),
        expected_tool_result: "interaction_expired",
    });
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixedExecutionPolicy {
            admitted_at: now,
            snapshot: contracts.execution_policy,
        }),
        context,
        tool_executor,
        Arc::new(EmptyControls),
    ));
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        policy,
        Arc::new(owned_model::OwnedScriptedModel(model.clone())),
        Arc::new(
            PostgresAgentLoopJournal::new(
                pool.clone(),
                content,
                codec.clone(),
                codec.clone(),
                Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
                keys.clone(),
                running_loop::recovery_keyring(),
            )
            .with_execution_liveness(
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                live_runtime::clock(),
            ),
        ),
    )
    .with_execution_supervision(live_runtime::supervision(&pool));

    let interaction_id = match execute_loop(&runtime, fixture, first_attempt.clone()).await {
        AgentLoopExit::Waiting { interaction_id, .. } => interaction_id,
        other => panic!("first Attempt must wait for approval, got {other:?}"),
    };
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);

    let expired_at = database_now(&pool).await.unix_millis() - 1;
    sqlx::query(
        "UPDATE approval.interactions
         SET expires_at_ms = $2
         WHERE interaction_id = $1 AND status = 1",
    )
    .bind(interaction_id.into_uuid())
    .bind(expired_at)
    .execute(&pool)
    .await
    .expect("make Interaction due using the database clock");

    let scheduler_store = Arc::new(PostgresDispatchStore::with_interaction_expiry(
        pool.clone(),
        Arc::new(ProtobufSchedulerEventEncoder),
        codec,
        keys,
    ));
    let (dispatch_tx, mut dispatch_rx) = tokio::sync::mpsc::channel(1);
    let scheduler = DurableScheduler::new(
        SchedulerPolicy {
            claim_batch_size: 1,
            lease_duration: Duration::from_secs(30),
            idle_wakeup_ceiling: Duration::from_secs(30),
        },
        WorkerId::new("worker-interaction-expiry").expect("worker"),
        scheduler_store.clone(),
        Arc::new(LocalScheduleSignal::default()),
        dispatch_tx,
    )
    .expect("build durable Scheduler");
    let scheduler_cancel = CancellationToken::new();
    let scheduler_task = tokio::spawn(scheduler.run(scheduler_cancel.clone()));
    let receipt = tokio::time::timeout(Duration::from_secs(5), dispatch_rx.recv())
        .await
        .expect("Scheduler must not leave an expired Interaction waiting")
        .expect("Scheduler dispatch queue remains open");
    scheduler_cancel.cancel();
    scheduler_task
        .await
        .expect("join Scheduler")
        .expect("stop Scheduler cleanly");
    assert_eq!(
        scheduler_store
            .advance_due_work()
            .await
            .expect("expiry replay is idempotent"),
        0
    );
    let second_attempt = AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id,
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    };
    assert_ne!(second_attempt.attempt_id, first_attempt.attempt_id);
    assert!(second_attempt.fencing_token > first_attempt.fencing_token);
    assert!(matches!(
        execute_loop(&runtime, fixture, second_attempt.clone()).await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);
    assert_eq!(model.invocations.load(Ordering::SeqCst), 2);
    assert!(
        model.observed_tool_result.load(Ordering::SeqCst),
        "the expired approval must enter production Context as a structured Tool failure"
    );
    assert_eq!(
        lock(&model.attempts).as_slice(),
        &[first_attempt.attempt_id, second_attempt.attempt_id]
    );

    let terminal: (i16, i16, i16, i64, i64, i64) = sqlx::query_as(
        "SELECT run.status, interaction.status, tool.status,
                (SELECT count(*) FROM runtime.attempts WHERE run_id = run.run_id),
                (SELECT count(*) FROM conversation.messages
                 WHERE run_id = run.run_id AND role = 2),
                (SELECT count(*) FROM event_store.outbox
                 WHERE conversation_id = run.conversation_id AND topic = 'run.dispatch')
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         JOIN tool.tool_invocations AS tool ON tool.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read expiry-resumed terminal state");
    assert_eq!(terminal, (5, 3, 5, 2, 1, 1));
    pool.close().await;
}

#[tokio::test]
async fn waiting_interaction_cancel_is_terminal_and_cannot_be_responded() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, first_attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let now = database_now(&pool).await;
    let provider_id = CapabilityProviderId::generate();
    let contracts = approval_contracts(fixture, provider_id, content.clone()).await;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        contracts.snapshot.clone(),
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let provider = Arc::new(ApprovedWriteProvider {
        content: content.clone(),
        physical_calls: AtomicUsize::new(0),
    });
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        Arc::new(DefaultLoopPolicy::new(
            Arc::new(FixedExecutionPolicy {
                admitted_at: now,
                snapshot: contracts.execution_policy,
            }),
            context,
            Arc::new(
                DurableToolExecutor::new(
                    Arc::new(FixedMaterial(contracts.material)),
                    Arc::new(
                        PostgresFencedToolInvocationJournal::new(
                            pool.clone(),
                            codec.clone(),
                            codec.clone(),
                            keys.clone(),
                        )
                        .with_execution_liveness(
                            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                            live_runtime::clock(),
                        ),
                    ),
                    content.clone(),
                    provider.clone(),
                    Arc::new(FixedClock(now)),
                    Arc::new(FixedTrace),
                )
                .with_execution_supervision(live_runtime::supervision(&pool)),
            ),
            Arc::new(EmptyControls),
        )),
        Arc::new(owned_model::OwnedScriptedModel(Arc::new(
            ApprovalThenCompletionModel {
                invocations: AtomicUsize::new(0),
                observed_tool_result: AtomicBool::new(false),
                attempts: Mutex::new(Vec::new()),
                expected_tool_result: "unused-after-cancellation",
            },
        ))),
        Arc::new(
            PostgresAgentLoopJournal::new(
                pool.clone(),
                content.clone(),
                codec.clone(),
                codec.clone(),
                Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
                keys.clone(),
                running_loop::recovery_keyring(),
            )
            .with_execution_liveness(
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                live_runtime::clock(),
            ),
        ),
    )
    .with_execution_supervision(live_runtime::supervision(&pool));
    let interaction_id = match execute_loop(&runtime, fixture, first_attempt).await {
        AgentLoopExit::Waiting { interaction_id, .. } => interaction_id,
        other => panic!("Run must wait before cancellation, got {other:?}"),
    };
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);

    let application = InteractionResponseApplication::new(
        runtime_application_store(&pool),
        content,
        codec.clone(),
        Arc::new(FixedClock(now)),
        Arc::new(PostgresInteractionApplicationStore::new(
            pool.clone(),
            keys,
            codec,
        )),
    );
    let sequence = current_sequence(&pool, fixture).await;
    let cancel_id = CommandId::generate();
    let cancel = CommandEnvelope::new(
        metadata(cancel_id, fixture, sequence, now),
        CancelRun {
            run_id: fixture.run_id,
            reason: CancelReason::UserRequested,
        },
    )
    .expect("cancel command");
    assert_eq!(
        application
            .cancel_run(cancel.clone())
            .await
            .expect("cancel waiting Run")
            .disposition,
        CommandDisposition::Committed
    );
    assert_eq!(
        application
            .cancel_run(cancel)
            .await
            .expect("deduplicate waiting cancellation")
            .disposition,
        CommandDisposition::Duplicate
    );

    let response_sequence = current_sequence(&pool, fixture).await;
    let response = CommandEnvelope::new(
        metadata(CommandId::generate(), fixture, response_sequence, now),
        RespondInteraction {
            interaction_id,
            response_authority: InteractionResponseAuthority::DeviceUserPresence,
            response: InteractionResponseRef::Approval(ApprovalDecision::Approve),
        },
    )
    .expect("late response command");
    let late = application
        .respond_interaction(response)
        .await
        .expect_err("cancelled Interaction must reject late response");
    assert_eq!(late.code, ErrorCode::InteractionNotPending);
    assert!(
        matches!(
            PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder))
                .claim_one(
                    &WorkerId::new("worker-after-wait-cancel").expect("worker"),
                    Duration::from_secs(30),
                )
                .await
                .expect("query Scheduler"),
            ClaimOutcome::Idle { .. }
        ),
        "cancelling a waiting Run must not fabricate a resumed Attempt"
    );
    let terminal: (i16, i16, i16, i64, i64) = sqlx::query_as(
        "SELECT run.status, interaction.status, tool.status,
                (SELECT count(*) FROM runtime.attempts WHERE run_id = run.run_id),
                (SELECT count(*) FROM runtime.leases WHERE run_id = run.run_id)
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         JOIN tool.tool_invocations AS tool ON tool.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read cancelled waiting Run");
    assert_eq!(terminal, (7, 4, 7, 1, 0));
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);
    pool.close().await;
}

#[tokio::test]
async fn interaction_multichannel_race_commits_one_response_and_one_resume() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, first_attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let now = database_now(&pool).await;
    let provider_id = CapabilityProviderId::generate();
    let contracts = approval_contracts(fixture, provider_id, content.clone()).await;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        contracts.snapshot.clone(),
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let provider = Arc::new(ApprovedWriteProvider {
        content: content.clone(),
        physical_calls: AtomicUsize::new(0),
    });
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        Arc::new(DefaultLoopPolicy::new(
            Arc::new(FixedExecutionPolicy {
                admitted_at: now,
                snapshot: contracts.execution_policy,
            }),
            context,
            Arc::new(
                DurableToolExecutor::new(
                    Arc::new(FixedMaterial(contracts.material)),
                    Arc::new(
                        PostgresFencedToolInvocationJournal::new(
                            pool.clone(),
                            codec.clone(),
                            codec.clone(),
                            keys.clone(),
                        )
                        .with_execution_liveness(
                            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                            live_runtime::clock(),
                        ),
                    ),
                    content.clone(),
                    provider.clone(),
                    Arc::new(FixedClock(now)),
                    Arc::new(FixedTrace),
                )
                .with_execution_supervision(live_runtime::supervision(&pool)),
            ),
            Arc::new(EmptyControls),
        )),
        Arc::new(owned_model::OwnedScriptedModel(Arc::new(
            ApprovalThenCompletionModel {
                invocations: AtomicUsize::new(0),
                observed_tool_result: AtomicBool::new(false),
                attempts: Mutex::new(Vec::new()),
                expected_tool_result: "unused-before-resume",
            },
        ))),
        Arc::new(
            PostgresAgentLoopJournal::new(
                pool.clone(),
                content.clone(),
                codec.clone(),
                codec.clone(),
                Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
                keys.clone(),
                running_loop::recovery_keyring(),
            )
            .with_execution_liveness(
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                live_runtime::clock(),
            ),
        ),
    )
    .with_execution_supervision(live_runtime::supervision(&pool));
    let interaction_id = match execute_loop(&runtime, fixture, first_attempt).await {
        AgentLoopExit::Waiting { interaction_id, .. } => interaction_id,
        other => panic!("Run must wait before the response race, got {other:?}"),
    };
    let sequence = current_sequence(&pool, fixture).await;
    let application = Arc::new(InteractionResponseApplication::new(
        runtime_application_store(&pool),
        content,
        codec.clone(),
        Arc::new(FixedClock(now)),
        Arc::new(PostgresInteractionApplicationStore::new(
            pool.clone(),
            keys,
            codec,
        )),
    ));
    let response = |decision| {
        CommandEnvelope::new(
            metadata(CommandId::generate(), fixture, sequence, now),
            RespondInteraction {
                interaction_id,
                response_authority: InteractionResponseAuthority::DeviceUserPresence,
                response: InteractionResponseRef::Approval(decision),
            },
        )
        .expect("competing Interaction response")
    };
    let desktop = application.clone();
    let channel = application.clone();
    let (desktop_result, channel_result) = tokio::join!(
        async move {
            desktop
                .respond_interaction(response(ApprovalDecision::Approve))
                .await
        },
        async move {
            channel
                .respond_interaction(response(ApprovalDecision::Deny))
                .await
        },
    );
    let outcomes = [desktop_result, channel_result];
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Ok(ack) if ack.disposition == CommandDisposition::Committed))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(
                result,
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::InteractionNotPending | ErrorCode::ConversationSeqConflict
                    )
            ))
            .count(),
        1
    );
    let committed: (i16, i16, i64, i64) = sqlx::query_as(
        "SELECT run.status, interaction.status,
                (SELECT count(*) FROM approval.interaction_responses
                 WHERE interaction_id = interaction.interaction_id),
                (SELECT count(*) FROM event_store.outbox
                 WHERE conversation_id = run.conversation_id AND topic = 'run.dispatch')
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read response race outcome");
    assert_eq!(committed, (1, 2, 1, 1));
    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    assert!(matches!(
        scheduler
            .claim_one(
                &WorkerId::new("worker-response-race").expect("worker"),
                Duration::from_secs(30),
            )
            .await
            .expect("claim unique resumed Attempt"),
        ClaimOutcome::Claimed(_)
    ));
    assert!(matches!(
        scheduler
            .claim_one(
                &WorkerId::new("worker-response-race-extra").expect("worker"),
                Duration::from_secs(30),
            )
            .await
            .expect("query extra resumed Attempt"),
        ClaimOutcome::Idle { .. }
    ));
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);
    pool.close().await;
}

#[tokio::test]
async fn clarification_response_requeues_and_enters_production_context() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, first_attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Ask me for the release channel, then continue with my answer.",
    )
    .await;
    let now = database_now(&pool).await;
    let provider_id = CapabilityProviderId::generate();
    let contracts = interaction_contracts(fixture, provider_id, content.clone()).await;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        contracts.snapshot,
        b"Ask me for the release channel, then continue with my answer.",
    )
    .await;
    let tool_journal: Arc<dyn FencedToolInvocationJournal> = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            codec.clone(),
            codec.clone(),
            keys.clone(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            live_runtime::clock(),
        ),
    );
    let mut dispatcher = CompositeCapabilityDispatcher::new(Arc::new(CapabilityRegistry::new()));
    dispatcher
        .register_atomic(Arc::new(InteractionCapabilityHandler::new(
            contracts.manifest,
            tool_journal.clone(),
            content.clone(),
            Arc::new(FixedClock(now)),
        )))
        .expect("register Interaction handler");
    let tool_executor: Arc<dyn ToolExecutor> = Arc::new(
        DurableToolExecutor::new(
            Arc::new(FixedMaterial(contracts.material)),
            tool_journal,
            content.clone(),
            Arc::new(dispatcher),
            Arc::new(FixedClock(now)),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(live_runtime::supervision(&pool)),
    );
    let model = Arc::new(StructuredInteractionThenCompletionModel {
        invocations: AtomicUsize::new(0),
        observed_response: AtomicBool::new(false),
        attempts: Mutex::new(Vec::new()),
        tool_call_id: "structured-input-call",
        request_arguments: JsonValue::Object(BTreeMap::from([
            ("kind".into(), JsonValue::String("text_input".into())),
            (
                "question".into(),
                JsonValue::String("Which release channel?".into()),
            ),
            (
                "description".into(),
                JsonValue::String("A real user answer is required.".into()),
            ),
            (
                "placeholder".into(),
                JsonValue::String("stable or beta".into()),
            ),
            ("required".into(), JsonValue::Bool(true)),
        ])),
        expected_response_fragments: vec!["\"text\":\"stable\"", "\"status\":\"submitted\""],
    });
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        Arc::new(DefaultLoopPolicy::new(
            Arc::new(FixedExecutionPolicy {
                admitted_at: now,
                snapshot: contracts.execution_policy,
            }),
            context,
            tool_executor,
            Arc::new(EmptyControls),
        )),
        Arc::new(owned_model::OwnedScriptedModel(model.clone())),
        Arc::new(
            PostgresAgentLoopJournal::new(
                pool.clone(),
                content.clone(),
                codec.clone(),
                codec.clone(),
                Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
                keys.clone(),
                running_loop::recovery_keyring(),
            )
            .with_execution_liveness(
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                live_runtime::clock(),
            ),
        ),
    )
    .with_execution_supervision(live_runtime::supervision(&pool));
    let interaction_id = match execute_loop(&runtime, fixture, first_attempt.clone()).await {
        AgentLoopExit::Waiting { interaction_id, .. } => interaction_id,
        other => {
            type InvocationState = (
                i16,
                Option<uuid::Uuid>,
                String,
                Option<String>,
                Option<String>,
            );
            let state: Vec<InvocationState> = sqlx::query_as(
                "SELECT status, interaction_id, tool_ref, terminal_error::text, terminal_reason::text FROM tool.tool_invocations WHERE run_id = $1",
            )
            .bind(fixture.run_id.into_uuid())
            .fetch_all(&pool)
            .await
            .expect("read failed Interaction Tool state");
            panic!(
                "Clarification must suspend the first Attempt, got {other:?}; Tool state={state:?}"
            )
        }
    };

    let response_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: br#"{"text":"stable"}"#.to_vec(),
        })
        .await
        .expect("store structured response");
    let response_time = UtcTimestamp::from_unix_millis(now.unix_millis() + 1);
    let application = InteractionResponseApplication::new(
        runtime_application_store(&pool),
        content,
        codec.clone(),
        Arc::new(FixedClock(response_time)),
        Arc::new(PostgresInteractionApplicationStore::new(
            pool.clone(),
            keys,
            codec,
        )),
    );
    let respond = CommandEnvelope::new(
        metadata(
            CommandId::generate(),
            fixture,
            current_sequence(&pool, fixture).await,
            response_time,
        ),
        RespondInteraction {
            interaction_id,
            response_authority: InteractionResponseAuthority::DeviceUserPresence,
            response: InteractionResponseRef::Content(response_ref),
        },
    )
    .expect("respond Clarification");
    assert_eq!(
        application
            .respond_interaction(respond)
            .await
            .expect("commit Clarification response")
            .disposition,
        CommandDisposition::Committed
    );

    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let receipt = match scheduler
        .claim_one(
            &WorkerId::new("worker-clarification-resume").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim Clarification-resumed Attempt")
    {
        ClaimOutcome::Claimed(receipt) => receipt,
        ClaimOutcome::Idle { .. } => panic!("Clarification response must requeue the Run"),
    };
    let second_attempt = AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id,
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    };
    assert!(matches!(
        execute_loop(&runtime, fixture, second_attempt.clone()).await,
        AgentLoopExit::Completed { .. }
    ));
    assert!(
        model.observed_response.load(Ordering::SeqCst),
        "the resumed physical model request must contain the validated Interaction Tool Result"
    );
    assert_eq!(
        lock(&model.attempts).as_slice(),
        &[first_attempt.attempt_id, second_attempt.attempt_id]
    );
    let terminal: (i16, i16, i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, interaction.kind, interaction.status, tool.status,
                (SELECT count(*) FROM conversation.messages
                 WHERE run_id = run.run_id AND role = 2)
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         JOIN tool.tool_invocations AS tool ON tool.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read completed Clarification Run");
    assert_eq!(terminal, (5, 2, 2, 4, 1));
    pool.close().await;
}

#[path = "postgres_interaction_resume/selection_response.rs"]
mod selection_response;

#[path = "postgres_interaction_resume/activity_response.rs"]
mod activity_response;

#[tokio::test]
async fn selection_response_is_validated_before_resume_and_enters_production_context() {
    selection_response::run_selection_response_is_validated_before_resume_and_enters_production_context()
        .await;
}

struct InteractionContracts {
    execution_policy: RunExecutionPolicySnapshot,
    snapshot: RuntimeSnapshotContent,
    material: ToolExecutionMaterial,
    manifest: CapabilityManifest,
}

/// Admit the controlled Tool into a current (v14) production snapshot and bind
/// the execution material to exactly what that snapshot sealed.
async fn admit_contract(
    content: Arc<dyn ContentStore>,
    fixture: Fixture,
    controlled: RuntimeSnapshotContent,
    material: &mut ToolExecutionMaterial,
) -> RuntimeSnapshotContent {
    let snapshot = live_runtime::upgrade_snapshot(content, run_context(fixture), controlled).await;
    let tool = snapshot
        .tools
        .iter()
        .find(|tool| tool.manifest.wire_name == material.tool.manifest.wire_name)
        .expect("admitted fixture Tool")
        .clone();
    material.capability_grant = snapshot
        .capabilities
        .iter()
        .find(|grant| grant.key.as_str() == tool.manifest.capability_key.as_str())
        .expect("admitted fixture grant")
        .clone();
    let (authorized_tools, exposure_state) = legacy_tool_authorization(&tool);
    material.tool = tool;
    material.authorized_tools = authorized_tools;
    material.exposure_state = exposure_state;
    material.model_liveness = Some(Arc::new(
        snapshot
            .execution_policy
            .liveness
            .expect("v14 liveness")
            .model_policy(),
    ));
    material.model.route = snapshot.model.route.clone();
    snapshot
}

async fn interaction_contracts(
    fixture: Fixture,
    provider_id: CapabilityProviderId,
    content: Arc<dyn ContentStore>,
) -> InteractionContracts {
    let definition = BuiltinToolCatalog::load_cloud()
        .expect("builtin Tool catalog")
        .by_wire_name("interaction_request")
        .expect("interaction.request Tool")
        .clone();
    let tool_manifest = definition.pin(provider_id);
    let tool = ToolDefinitionSnapshot {
        manifest: tool_manifest.clone(),
        execution_target: definition.target,
        concurrency_class: definition.concurrency,
        idempotency: definition.idempotency,
        file_operation: definition.file_operation.clone(),
        title: definition.title.clone(),
        description: definition.description.clone(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json: definition.input_schema_json.clone(),
        output_schema_json: definition.output_schema_json.clone(),
    };
    let capability_key = CapabilityKey::from_versioned_key(definition.capability_key.clone());
    let grant = CapabilityGrantSnapshot {
        key: capability_key.clone(),
        provider_id,
        location: CapabilityLocation::Cloud,
        effect: definition.effect,
        allowed_scopes: definition.required_scopes.clone(),
        policy_decision: Decision::Allow,
    };
    let route = model_route();
    let host_agent = fixture_version_v1();
    let snapshot = runtime_snapshot(tool.clone(), grant.clone(), route.clone());
    let (authorized_tools, exposure_state) = legacy_tool_authorization(&tool);
    let actor = Actor {
        tenant_id: fixture.tenant_id,
        principal_id: fixture.principal_id,
        principal_kind: PrincipalKind::User,
    };
    let mut material = ToolExecutionMaterial {
        application_capabilities: vec![],
        product_release: None,
        model_liveness: None,
        recovery_policy: std::num::NonZeroU32::new(8)
            .expect("bounded fixture recovery")
            .into(),
        run_context: run_context(fixture),
        actor,
        agent_id: host_agent.agent_id,
        model: route,
        active_model_route_ordinal: 0,
        auxiliary_models: Vec::new(),
        model_stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
        tool,
        authorized_tools,
        discovery: None,
        exposure_state,
        capability_grant: grant,
        authorization: InvocationAuthorization {
            actor_scopes: definition.required_scopes.clone(),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: false,
        },
        permission_mode: musteragent_policy::PermissionMode::Supervised,
        policy_basis: None,
        authorized_content: Vec::new(),
        prior_tool_results: Vec::new(),
        execution_grant_proof: None,
    };
    let manifest = CapabilityManifest {
        capability_id: CapabilityId::generate(),
        key: capability_key,
        effect: definition.effect,
        approval: definition.approval,
        location: CapabilityLocation::Cloud,
        provider_id,
        timeout: definition.timeout,
        max_attempts: definition.max_attempts,
        idempotency: IdempotencyContract::IdempotentByKey,
        required_scopes: definition.required_scopes,
        required_contexts: definition.required_contexts,
        data_classification: definition.data_classification,
        egress_policy: definition.egress_policy,
    };
    let snapshot = admit_contract(content, fixture, snapshot, &mut material).await;
    let mut manifest = manifest;
    manifest.provider_id = material.capability_grant.provider_id;
    manifest.key = material.capability_grant.key.clone();
    InteractionContracts {
        execution_policy: snapshot.execution_policy,
        snapshot,
        material,
        manifest,
    }
}

struct ApprovalContracts {
    execution_policy: RunExecutionPolicySnapshot,
    snapshot: RuntimeSnapshotContent,
    material: ToolExecutionMaterial,
}

async fn approval_contracts(
    fixture: Fixture,
    provider_id: CapabilityProviderId,
    content: Arc<dyn ContentStore>,
) -> ApprovalContracts {
    let input_schema_json = br#"{"type":"object"}"#.to_vec();
    let output_schema_json = br#"{"type":"object"}"#.to_vec();
    let capability_key = VersionedKey::new("fixture.approval-write@1").expect("capability");
    let scope = DottedKey::new("fixture.write").expect("scope");
    let host_agent = fixture_version_v1();
    let route = model_route();
    let tool = ToolDefinitionSnapshot {
        manifest: ToolManifest {
            application: None,
            key: ToolKey::from_versioned_key(
                VersionedKey::new("fixture.approval-write-tool@1").expect("Tool key"),
            ),
            wire_name: WireName::new(TOOL_WIRE_NAME).expect("wire name"),
            input_schema: SchemaRef {
                schema_key: VersionedKey::new("fixture.input@1").expect("input schema"),
                schema_hash: Sha256::digest(&input_schema_json),
            },
            output_schema: SchemaRef {
                schema_key: VersionedKey::new("fixture.output@1").expect("output schema"),
                schema_hash: Sha256::digest(&output_schema_json),
            },
            capability_key: capability_key.clone(),
            capability_provider_id: provider_id,
            effect: Effect::WriteReversible,
            approval: ApprovalRequirement::ByPolicy,
            execution_policy: musteragent_tool::ToolExecutionPolicy::LegacyFixed {
                timeout: Duration::from_secs(30),
            },
            max_attempts: NonZeroU32::new(1).expect("Tool attempt"),
            retry_contract: ToolRetryContract::IdempotentByKey,
            required_scopes: BTreeSet::from([scope.clone()]),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Internal,
            egress_policy: EgressPolicy::None,
        },
        execution_target: CapabilityExecutionTarget::RustRuntime,
        concurrency_class: ConcurrencyClass::SequentialWrite,
        idempotency: BuiltinIdempotency::RuntimeTransaction,
        file_operation: None,
        title: "Approval write".into(),
        description: "Write only after an explicit approval.".into(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json,
        output_schema_json,
    };
    let (authorized_tools, exposure_state) = legacy_tool_authorization(&tool);
    let mut material = ToolExecutionMaterial {
        application_capabilities: vec![],
        product_release: None,
        model_liveness: None,
        recovery_policy: std::num::NonZeroU32::new(8)
            .expect("bounded fixture recovery")
            .into(),
        run_context: run_context(fixture),
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        agent_id: host_agent.agent_id,
        model: route.clone(),
        active_model_route_ordinal: 0,
        auxiliary_models: Vec::new(),
        model_stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
        tool,
        authorized_tools,
        discovery: None,
        exposure_state,
        capability_grant: CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(capability_key),
            provider_id,
            location: CapabilityLocation::Cloud,
            effect: Effect::WriteReversible,
            allowed_scopes: BTreeSet::from([scope.clone()]),
            policy_decision: Decision::Allow,
        },
        authorization: InvocationAuthorization {
            actor_scopes: BTreeSet::from([scope]),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: true,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: false,
        },
        permission_mode: musteragent_policy::PermissionMode::Supervised,
        policy_basis: None,
        authorized_content: Vec::new(),
        prior_tool_results: Vec::new(),
        execution_grant_proof: None,
    };
    let controlled = runtime_snapshot(
        material.tool.clone(),
        material.capability_grant.clone(),
        route,
    );
    let snapshot = admit_contract(content, fixture, controlled, &mut material).await;
    ApprovalContracts {
        execution_policy: snapshot.execution_policy,
        snapshot,
        material,
    }
}

fn runtime_snapshot(
    tool: ToolDefinitionSnapshot,
    grant: CapabilityGrantSnapshot,
    route: ModelRouteSnapshot,
) -> RuntimeSnapshotContent {
    let host_agent = fixture_version_v1();
    let core_time = VersionedKey::new("core.time.read@1").expect("core time Capability");
    let web_fetch = VersionedKey::new("web.fetch@2").expect("web fetch Capability");
    let web_search = VersionedKey::new("web.search@2").expect("web search Capability");
    let authorized_skill_capabilities =
        BTreeSet::from([core_time.clone(), web_fetch.clone(), web_search.clone()]);
    let skills = builtin_skill_catalog()
        .expect("Skill catalog")
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Web,
            agent_contract: VersionedKey::new("musteragent.runtime-contract@1")
                .expect("Agent contract"),
            authorized_capabilities: authorized_skill_capabilities.clone(),
        })
        .expect("resolve Kernel base Skill")
        .into_iter()
        .filter(|skill| {
            skill
                .required_capabilities
                .is_subset(&authorized_skill_capabilities)
        })
        .collect();
    let baseline_provider_id = CapabilityProviderId::generate();
    RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: vec![],
        product_release: None,
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("Snapshot schema"),
        agent: AgentVersionSnapshot::from_resolved(
            &musteragent_conformance::fixtures::agent::system_definition(),
            &host_agent,
        )
        .expect("Kernel Snapshot"),
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety").expect("policy section"),
                    content_ref: fixture_runtime_policy_content_ref().expect("policy content"),
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new("musteragent.legacy.persona")
                        .expect("persona section"),
                    content_ref: host_agent.persona_content_ref.clone(),
                },
            ],
            assembled_hash: Sha256::digest(
                [FIXTURE_RUNTIME_POLICY_V1, FIXTURE_PERSONA_V1].concat(),
            ),
        },
        model: route,
        auxiliary_models: Vec::new(),
        skills,
        tools: vec![tool],
        capabilities: vec![
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(core_time),
                provider_id: baseline_provider_id,
                location: CapabilityLocation::Cloud,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([DottedKey::new("time.read").expect("time scope")]),
                policy_decision: Decision::Allow,
            },
            grant,
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(web_fetch),
                provider_id: baseline_provider_id,
                location: CapabilityLocation::Backend,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([
                    DottedKey::new("network.public").expect("network scope")
                ]),
                policy_decision: Decision::Allow,
            },
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(web_search),
                provider_id: baseline_provider_id,
                location: CapabilityLocation::Backend,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([
                    DottedKey::new("network.search").expect("search scope")
                ]),
                policy_decision: Decision::Allow,
            },
        ],
        policy: PolicySnapshot {
            policy_ref: VersionedKey::new("musteragent.legacy.runtime-policy@1").expect("policy"),
            policy_hash: Sha256::digest(FIXTURE_RUNTIME_POLICY_V1),
            permission_mode: musteragent_policy::PermissionMode::Supervised,
        },
        memory: MemorySelectionSnapshot {
            selected: Vec::new(),
            selection_policy_ref: VersionedKey::new("musteragent.legacy.memory-policy@1")
                .expect("memory policy"),
            selection_policy_hash: Sha256::digest(b"musteragent.legacy.memory-policy@1"),
        },
        instincts: Vec::new(),
        context_policy: ContextPolicySnapshot {
            max_input_tokens: NonZeroU64::new(120_000).expect("input budget"),
            compaction_trigger_tokens: NonZeroU64::new(84_000).expect("70 percent trigger"),
            reserved_output_tokens: NonZeroU64::new(8_192).expect("reserved output"),
            compaction_policy_ref: VersionedKey::new("musteragent.legacy.context-compaction@1")
                .expect("compaction policy"),
            resolved_v2: None,
            source_selection: None,
        },
        output_policy: OutputPolicySnapshot {
            reasoning_summary: true,
        },
        execution_policy: RunExecutionPolicySnapshot::legacy_defaults(),
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new("musteragent.runtime.command.v2")
                .expect("command schema"),
            record_schema: DottedKey::new("musteragent.runtime.record.v1").expect("record schema"),
            snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("Snapshot schema"),
            model_protocol: VersionedKey::new("musteragent.legacy.model.canonical@1")
                .expect("model protocol"),
        },
        discovery: None,
    }
}

fn model_route() -> ModelRouteSnapshot {
    ModelRouteSnapshot {
        requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
        route: ModelRoute {
            route_ref: VersionedKey::new("cloud.chat-primary@1").expect("route"),
            resolved_model_key: "approval-test-model".into(),
            model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::now_v7())
                .expect("model provider"),
            model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::now_v7()).expect("model entry"),
            api_format: ApiFormat::OpenAiResponses,
            adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
            protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
            context_window: NonZeroU64::new(128_000).expect("context window"),
            max_output_tokens: NonZeroU64::new(8_192).expect("output tokens"),
            capabilities: BTreeSet::from([DottedKey::new("tools").expect("capability")]),
            parameters: BTreeMap::new(),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:approval.price@1")
                .expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        credential_grant_ref: CredentialGrantRef::new("grant_approval_resume").expect("grant"),
        fallback_candidates: Vec::new(),
    }
}

fn metadata(
    command_id: CommandId,
    fixture: Fixture,
    expected_sequence: u64,
    issued_at: UtcTimestamp,
) -> CommandMetadata {
    CommandMetadata {
        schema_version: DottedKey::new("musteragent.runtime.command.v2").expect("command schema"),
        command_id,
        idempotency_key: IdempotencyKey::new(format!("interaction-response:{command_id}"))
            .expect("idempotency"),
        issued_at,
        deadline: issued_at.checked_add_millis(30_000).expect("deadline"),
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        correlation_id: CorrelationId::generate(),
        causation_id: None,
        conversation_guard: Some(ConversationGuard {
            conversation_id: fixture.conversation_id,
            expected_home_epoch: musteragent_identity::HomeEpoch::new(1).expect("home epoch"),
            expected_conversation_seq: Some(
                ConversationSeq::new(expected_sequence).expect("conversation sequence"),
            ),
        }),
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("traceparent"),
            tracestate: None,
        },
    }
}

async fn current_sequence(pool: &sqlx::PgPool, fixture: Fixture) -> u64 {
    sqlx::query_scalar::<_, String>(
        "SELECT last_sequence::text FROM event_store.ledger_streams
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("read conversation sequence")
    .parse()
    .expect("parse conversation sequence")
}
