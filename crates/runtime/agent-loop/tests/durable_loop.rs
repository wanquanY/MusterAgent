#[path = "durable_loop/activity_fixture.rs"]
mod activity_fixture;
#[path = "durable_loop/activity_model.rs"]
mod activity_model;
#[path = "durable_loop/approval_batch.rs"]
mod approval_batch;
#[path = "durable_loop/dependency_batch.rs"]
mod dependency_batch;
#[path = "durable_loop/durable_stop.rs"]
mod durable_stop;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::pending;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use futures_util::{StreamExt, future::join_all, stream};
use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ContentId, ConversationId, CorrelationId, FencingToken,
    HomeEpoch, InteractionId, InvocationId, LeaseId, LedgerRecordId, MessageId, OutputId,
    ParticipantId, RunId, RuntimeSnapshotId, TenantId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelKey, ModelProviderId, ModelRoute,
    PricingVersionRef, ProviderStatePolicy,
};
use musteragent_run::{AttemptContext, InvocationContext, RunContext, WorkerId};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, OpaqueExternalRef, TraceContext, TraceParent,
};
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    CanonicalToolDefinition, JsonValue, ModelEventStream, ModelProvider, ModelReplayText,
    ModelStreamEvent, ModelText, ModelUsage, ProviderError, ProviderErrorCode, ProviderErrorKind,
    ProviderReasoningReplayItem, ProviderRequestRef, ReasoningPolicy, StopReason,
};
use musteragent_tool::{ToolKey, WireName};
use tokio::sync::{Barrier, Notify};

use musteragent_loop::{
    AgentLoopExit, AgentLoopFailureStage, AgentLoopJournal, ContextPlanError, ContextPlanInput,
    ContextPlanner, DefaultLoopPolicy, DurableAgentLoop, DurableAttemptOutcome,
    DurableLoopCheckpoint, DurableRunOutcome, DurableToolCompletion, ExecutionPolicySource,
    FallbackDecision, GateVerdict, InvocationSupervisor, LoopControlQueue, LoopPhase, LoopPolicy,
    LoopPortError, ModelActionRejection, ModelActionRejectionCommit, ModelCompletionCommit,
    ModelCompletionKind, ModelEventBatch, ModelFailure, ModelFailureCommit, ModelFailureResolution,
    ModelInvocationObservation, ModelIteration, NextTurnDecision, PinnedRunExecutionPolicy,
    PlannedToolCall, RecoveredLoopState, RunCompletionDisposition, StepContext,
    ToolExecutionOutcome, ToolExecutionPolicyDecision, ToolExecutor, ToolGate, ToolResultCommit,
    TurnSummary,
};
use musteragent_runtime_snapshot::{RunExecutionPolicySnapshot, ToolExecutionMode};
use musteragent_telemetry::AttemptTelemetryContext;

include!("support/durable_loop_correction.rs");

#[path = "durable_loop/delegation.rs"]
mod delegation;
#[path = "durable_loop/max_output_continuation.rs"]
mod max_output_continuation;
#[path = "durable_loop/model_fixtures.rs"]
mod model_fixtures;
#[path = "durable_loop/policy_loading.rs"]
mod policy_loading;

use model_fixtures::{assistant_events, model_request, tool_batch_events};
#[path = "durable_loop/context_overflow_recovery.rs"]
mod context_overflow_recovery;
#[path = "durable_loop/empty_response_recovery.rs"]
mod empty_response_recovery;
#[path = "durable_loop/provider_recovery_policy.rs"]
mod provider_recovery_policy;
#[path = "durable_loop/provider_recovery_runtime.rs"]
mod provider_recovery_runtime;

#[tokio::test]
async fn loop_progress_beyond_legacy_counters() {
    max_output_continuation::run_loop_progress_beyond_legacy_counters().await;
}

#[tokio::test]
async fn loop_model_route_immutable() {
    provider_recovery_policy::loop_model_route_immutable().await;
}

#[path = "durable_loop/argument_decode_correction.rs"]
mod argument_decode_correction;
#[path = "durable_loop/protocol_exhaustion.rs"]
mod protocol_exhaustion;
#[tokio::test]
async fn unoffered_tool_call_is_rejected_without_fallback_or_execution() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("unoffered-tool-call");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("unoffered-tool-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: wire_name("private_admin_delete"),
                provider_item_ref: None,
                provider_replay: None,
            }),
            Ok(ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id,
                arguments: JsonValue::Object(BTreeMap::new()),
            }),
            Ok(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 4,
                output_tokens: 1,
                total_tokens: Some(5),
                ..ModelUsage::default()
            })),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            }),
        ]),
        Script::Events(assistant_events(
            "correction-after-unoffered-tool",
            "Recovered without executing the hallucinated Tool.",
        )),
    ]));
    let tools = Arc::new(CountingTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime_with_route_count(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Duration::from_secs(1),
        non_zero_u32(2),
    );

    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }));
    assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].route.route_ref, requests[1].route.route_ref);
    assert_eq!(journal.snapshot().phase, LoopPhase::Completed);
    assert!(journal.failures().is_empty());
    assert_eq!(journal.action_rejections().len(), 1);
    assert!(
        journal
            .batches()
            .iter()
            .flat_map(|batch| &batch.events)
            .all(|event| !matches!(event, ModelStreamEvent::ToolCallStarted { .. })),
        "an unexposed Tool call must never become public durable output"
    );
}

#[tokio::test]
async fn invalid_tool_arguments_complete_as_failed_tools_without_model_repair() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("invalid-arguments-call");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("invalid-arguments-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: wire_name("external_write"),
                provider_item_ref: None,
                provider_replay: None,
            }),
            Ok(ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id,
                arguments: JsonValue::Object(BTreeMap::new()),
            }),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            }),
        ]),
        Script::Events(assistant_events(
            "correction-after-invalid-arguments",
            "Recovered after correcting the private Tool protocol.",
        )),
    ]));
    let tools = Arc::new(RejectingArgumentsTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Duration::from_secs(1),
    );

    assert!(matches!(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
    assert_eq!(tools.validations.load(Ordering::SeqCst), 1);
    assert!(journal.action_rejections().is_empty());
    assert!(journal.failures().is_empty());
    assert_eq!(
        journal.committed_tool_calls(),
        vec![tool_call_id("invalid-arguments-call")]
    );
    let results = lock(&journal.tool_results);
    assert!(matches!(results[0].message.role, CanonicalRole::Tool));
    let CanonicalContentPart::Text(text) = &results[0].message.content[0] else {
        panic!("text")
    };
    assert!(text.as_str().contains("invalid_command"));
    assert!(
        journal
            .batches()
            .iter()
            .flat_map(|batch| &batch.events)
            .any(|event| matches!(event, ModelStreamEvent::ToolCallCompleted { .. }))
    );
}

#[tokio::test]
async fn unknown_tool_outcome_reaches_the_model_without_interrupting_the_run() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("not-replayable-call");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("unknown-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: wire_name("external_write"),
                provider_item_ref: None,
                provider_replay: None,
            }),
            Ok(ModelStreamEvent::ToolCallArgumentsDelta {
                output_index: 0,
                call_id: call_id.clone(),
                delta: "{}".to_owned(),
            }),
            Ok(ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id: call_id.clone(),
                arguments: JsonValue::Object(BTreeMap::new()),
            }),
            Ok(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 4,
                output_tokens: 2,
                total_tokens: Some(6),
                ..ModelUsage::default()
            })),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            }),
        ]),
        Script::Events(assistant_events(
            "after-unknown",
            "Check the existing operation before choosing the next step.",
        )),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(ReconciliationTools),
        journal.clone(),
        Duration::from_secs(1),
    );

    assert!(matches!(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new()
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert!(journal.failures().is_empty());
    assert_eq!(journal.committed_tool_calls(), vec![call_id]);
    let requests = lock(&provider.requests);
    assert_eq!(requests.len(), 2);
    assert!(lock(&journal.tool_results).iter().flat_map(|result| &result.message.content).any(|part| matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains("tool_outcome_unknown"))));
}

#[tokio::test]
async fn multi_iteration_tool_loop_commits_bounded_batches_and_finishes() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("call-1");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("response-1"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ReasoningDelta {
                output_index: 0,
                delta: model_text("private tool reasoning"),
            }),
            Ok(ModelStreamEvent::ProviderReasoningReplay {
                output_index: 0,
                item: ProviderReasoningReplayItem {
                    source_api_format: ApiFormat::OpenAiResponses,
                    source_model_key: "model-fixture".into(),
                    payload: JsonValue::Object(BTreeMap::from([
                        ("type".into(), JsonValue::String("reasoning".into())),
                        (
                            "encrypted_content".into(),
                            JsonValue::String("opaque-reasoning".into()),
                        ),
                    ])),
                },
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: wire_name("current_time"),
                provider_item_ref: None,
                provider_replay: None,
            }),
            Ok(ModelStreamEvent::ToolCallArgumentsDelta {
                output_index: 0,
                call_id: call_id.clone(),
                delta: "{}".to_owned(),
            }),
            Ok(ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id: call_id.clone(),
                arguments: JsonValue::Object(BTreeMap::new()),
            }),
            Ok(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 10,
                output_tokens: 2,
                total_tokens: Some(12),
                ..ModelUsage::default()
            })),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            }),
        ]),
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("response-2"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("Done."),
            }),
            Ok(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 6,
                output_tokens: 2,
                total_tokens: Some(8),
                ..ModelUsage::default()
            })),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ]),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Duration::from_secs(1),
    );

    assert!(matches!(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new()
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    let state = journal.snapshot();
    assert_eq!(state.phase, LoopPhase::Completed);
    assert_eq!(state.iteration, 2);
    assert_eq!(state.completed_tool_calls, 1);
    assert_eq!(state.total_tokens, 20);
    assert_eq!(
        state
            .continuation_ref
            .as_ref()
            .map(musteragent_runtime_model::ContinuationRef::as_str),
        Some("response-2")
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1]
            .continuation_ref
            .as_ref()
            .map(musteragent_runtime_model::ContinuationRef::as_str),
        Some("response-1"),
        "the next iteration must replay the durable provider continuation"
    );
    let batches = journal.batches();
    assert_eq!(batches.len(), 5);
    assert!(
        batches.iter().all(|batch| batch.events.len() <= 2),
        "provider events must be durably flushed in bounded batches"
    );
    let replay = journal.reasoning_replays();
    assert_eq!(replay.len(), 2);
    let encoded: serde_json::Value =
        serde_json::from_str(replay[0].as_ref().expect("first iteration replay").as_str())
            .expect("provider reasoning replay JSON");
    assert_eq!(
        encoded["schema"],
        "musteragent.legacy.runtime.provider-reasoning-replay.v2"
    );
    assert_eq!(encoded["source_api_format"], "openai.responses");
    assert_eq!(encoded["source_model_key"], "model-fixture");
    assert_eq!(encoded["items"][0]["encrypted_content"], "opaque-reasoning");
    assert!(replay[1].is_none());
}

#[tokio::test]
async fn empty_response_after_tools_is_privately_recovered_and_then_finishes() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("call-search");
    let usage = ModelUsage {
        input_tokens_uncached: 10,
        output_tokens: 2,
        total_tokens: Some(12),
        ..ModelUsage::default()
    };
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("response-tools"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: wire_name("web_search"),
                provider_item_ref: None,
                provider_replay: None,
            }),
            Ok(ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id,
                arguments: JsonValue::Object(BTreeMap::from([(
                    "query".into(),
                    JsonValue::String("Rust".into()),
                )])),
            }),
            Ok(ModelStreamEvent::Usage(usage)),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            }),
        ]),
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("response-empty"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::Usage(usage)),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ]),
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("response-recovered"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("Recovered answer."),
            }),
            Ok(ModelStreamEvent::Usage(usage)),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ]),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
    );

    assert!(matches!(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new()
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    let checkpoint = journal.snapshot();
    assert_eq!(checkpoint.iteration, 3);
    assert_eq!(checkpoint.completed_tool_calls, 1);
    assert_eq!(checkpoint.consecutive_empty_responses, 0);
    assert_eq!(provider.requests().len(), 3);
    assert!(
        provider.requests()[2]
            .instructions
            .iter()
            .any(|value| value.as_str().contains("empty-response recovery")),
        "the retry must receive a private recovery directive"
    );
    assert!(journal.failures().is_empty());
}

#[path = "durable_loop/core_scenarios.rs"]
mod core_scenarios;

fn loop_runtime(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    tools: Arc<dyn ToolExecutor>,
    journal: Arc<FixtureJournal>,
    model_deadline: Duration,
) -> DurableAgentLoop {
    loop_runtime_with_route_count(
        fixture,
        provider,
        tools,
        journal,
        model_deadline,
        NonZeroU32::MIN,
    )
}

fn loop_runtime_with_route_count(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    tools: Arc<dyn ToolExecutor>,
    journal: Arc<FixtureJournal>,
    model_deadline: Duration,
    model_route_count: NonZeroU32,
) -> DurableAgentLoop {
    loop_runtime_with_controls(
        fixture,
        provider,
        tools,
        journal,
        model_deadline,
        Arc::new(EmptyControls),
        model_route_count,
    )
}

fn loop_runtime_with_controls(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    tools: Arc<dyn ToolExecutor>,
    journal: Arc<FixtureJournal>,
    model_deadline: Duration,
    controls: Arc<dyn LoopControlQueue>,
    model_route_count: NonZeroU32,
) -> DurableAgentLoop {
    let mut execution = RunExecutionPolicySnapshot::production_defaults();
    execution.recovery_policy = non_zero_u32(8).into();
    execution.max_consecutive_empty_responses = non_zero_u32(3);
    execution.max_model_events_per_commit = non_zero_usize(2);
    execution.output_frame_flush_interval = Duration::from_millis(25);
    execution.max_output_frame_bytes = non_zero_usize(4 * 1_024);
    execution.model_inactivity_timeout = model_deadline;
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixtureExecutionPolicy {
            execution,
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count,
        }),
        Arc::new(FixtureContext { run: fixture.run }),
        tools,
        controls,
    ));
    activity_fixture::runtime(Arc::new(FixedClock), policy, provider, journal)
}

fn assert_durable_failure(
    exit: AgentLoopExit,
    expected_stage: AgentLoopFailureStage,
    expected_code: ErrorCode,
    expected_retryable: bool,
) {
    let AgentLoopExit::Failed {
        stage,
        code,
        retryable,
        failure_commit_error: None,
        receipt: Some(receipt),
    } = exit
    else {
        panic!("failure must return a committed durable receipt")
    };
    assert_eq!(stage, expected_stage);
    assert_eq!(code, expected_code);
    assert_eq!(retryable, expected_retryable);
    assert_eq!(receipt.attempt_outcome(), DurableAttemptOutcome::Failed);
    assert_eq!(
        receipt.run_outcome(),
        if expected_retryable {
            DurableRunOutcome::RetryBackoff
        } else {
            DurableRunOutcome::Failed
        }
    );
}

struct FixedClock;

impl musteragent_loop::AgentLoopClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1_000)
    }
}

struct FixtureExecutionPolicy {
    execution: RunExecutionPolicySnapshot,
    admitted_at: UtcTimestamp,
    model_route_count: NonZeroU32,
}

struct PendingToolExecutionPolicy {
    execution: RunExecutionPolicySnapshot,
    admitted_at: UtcTimestamp,
    entered: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

impl ExecutionPolicySource for PendingToolExecutionPolicy {
    fn load(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let policy = PinnedRunExecutionPolicy {
            snapshot: activity_fixture::snapshot(self.execution),
            admitted_at: self.admitted_at,
            model_route_count: NonZeroU32::MIN,
        };
        Box::pin(async move { Ok(policy) })
    }

    fn tool_execution_policy(
        &self,
        _attempt: &AttemptContext,
        _wire_name: &WireName,
        _default_mode: ToolExecutionMode,
    ) -> ApplicationFuture<'_, Result<ToolExecutionPolicyDecision, LoopPortError>> {
        let entered = Arc::clone(&self.entered);
        let dropped = Arc::clone(&self.dropped);
        Box::pin(async move {
            struct DropEvidence(Arc<AtomicBool>);
            impl Drop for DropEvidence {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let _evidence = DropEvidence(dropped);
            entered.notify_one();
            pending().await
        })
    }
}

impl ExecutionPolicySource for FixtureExecutionPolicy {
    fn load(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let policy = PinnedRunExecutionPolicy {
            snapshot: activity_fixture::snapshot(self.execution),
            admitted_at: self.admitted_at,
            model_route_count: self.model_route_count,
        };
        Box::pin(async move { Ok(policy) })
    }
}

struct EmptyControls;

impl LoopControlQueue for EmptyControls {
    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }
}

/// Simulates a FollowUp accepted while the first physical model turn is
/// running. It becomes available only at the next turn boundary and must be
/// consumed exactly once before that turn's Context plan.
struct TurnBoundaryFollowUpControls {
    turn_boundaries: AtomicUsize,
    pending: AtomicBool,
}

impl Default for TurnBoundaryFollowUpControls {
    fn default() -> Self {
        Self {
            turn_boundaries: AtomicUsize::new(0),
            pending: AtomicBool::new(true),
        }
    }
}

impl LoopControlQueue for TurnBoundaryFollowUpControls {
    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        self.turn_boundaries.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(0) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        let ready = self.turn_boundaries.load(Ordering::SeqCst) >= 2;
        let consumed = ready && self.pending.swap(false, Ordering::SeqCst);
        Box::pin(async move { Ok(u32::from(consumed)) })
    }
}

#[derive(Clone)]
struct Fixture {
    run: RunContext,
    attempt: AttemptContext,
}

impl Fixture {
    fn new() -> Self {
        let tenant_id = TenantId::generate();
        let run_id = RunId::generate();
        Self {
            run: RunContext {
                tenant_id,
                conversation_id: ConversationId::generate(),
                activity_id: ActivityId::generate(),
                activity_kind: ActivityKind::Chat,
                run_id,
                executor_participant_id: ParticipantId::generate(),
                requested_by_participant_id: ParticipantId::generate(),
                parent_run_id: None,
                delegation_id: None,
                execution_scope: ExecutionScope::Tenant { tenant_id },
                control_home: RuntimeHome::Cloud,
                execution_home: RuntimeHome::Cloud,
                home_epoch: result_or_panic(HomeEpoch::new(1)),
                runtime_snapshot_id: RuntimeSnapshotId::generate(),
            },
            attempt: AttemptContext {
                attempt_id: AttemptId::generate(),
                run_id,
                attempt_number: result_or_panic(AttemptNumber::new(1)),
                worker_id: result_or_panic(WorkerId::new("fixture-worker")),
                lease_id: LeaseId::generate(),
                fencing_token: result_or_panic(FencingToken::new(1)),
                lease_expires_at: UtcTimestamp::from_unix_millis(60_000),
            },
        }
    }

    fn checkpoint(&self) -> DurableLoopCheckpoint {
        DurableLoopCheckpoint {
            run_id: self.run.run_id,
            attempt_id: self.attempt.attempt_id,
            iteration: 0,
            active_model_route_ordinal: 0,
            phase: LoopPhase::ReadyForModel,
            completed_tool_calls: 0,
            consecutive_empty_responses: 0,
            total_tokens: 0,
            total_cost_microusd: 0,
            continuation_ref: None,
            continuation_source_invocation_id: None,
        }
    }

    fn telemetry(&self) -> AttemptTelemetryContext {
        AttemptTelemetryContext {
            tenant_id: self.run.tenant_id,
            conversation_id: self.run.conversation_id,
            activity_id: self.run.activity_id,
            run_id: self.run.run_id,
            attempt_id: self.attempt.attempt_id,
            attempt_number: self.attempt.attempt_number,
            fencing_token: self.attempt.fencing_token,
            correlation_id: CorrelationId::generate(),
            trace_context: TraceContext {
                traceparent: TraceParent::new(
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                )
                .expect("traceparent"),
                tracestate: None,
            },
        }
    }
}

struct FixtureContext {
    run: RunContext,
}

struct PendingContext {
    entered: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

impl ContextPlanner for PendingContext {
    fn prepare_model_iteration(
        &self,
        _input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        let entered = Arc::clone(&self.entered);
        let dropped = Arc::clone(&self.dropped);
        Box::pin(async move {
            struct DropEvidence(Arc<AtomicBool>);
            impl Drop for DropEvidence {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let _evidence = DropEvidence(dropped);
            entered.notify_one();
            pending().await
        })
    }
}

impl ContextPlanner for FixtureContext {
    fn prepare_model_iteration(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        let request = model_request(
            self.run,
            input.attempt,
            input.next_iteration,
            input.active_model_route_ordinal,
            input.continuation_ref,
            input.consecutive_empty_responses,
        );
        Box::pin(async move { Ok(request) })
    }
}

enum Script {
    Events(Vec<Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>>),
    EventsThenPending(Vec<Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>>),
    EventsThenDelayedEof(
        Vec<Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>>,
        Duration,
    ),
    DelayedEvents(
        Vec<(
            Duration,
            Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>,
        )>,
    ),
    Pending,
}

struct ScriptedProvider {
    scripts: Mutex<VecDeque<Script>>,
    requests: Mutex<Vec<CanonicalModelRequest>>,
}

impl ScriptedProvider {
    fn new(scripts: Vec<Script>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<CanonicalModelRequest> {
        lock(&self.requests).clone()
    }
}

impl ModelProvider for ScriptedProvider {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        lock(&self.requests).push(request);
        let script = lock(&self.scripts).pop_front();
        match script {
            Some(Script::Events(events)) => Box::pin(stream::iter(events)),
            Some(Script::EventsThenPending(events)) => {
                Box::pin(stream::iter(events).chain(stream::pending()))
            }
            Some(Script::EventsThenDelayedEof(events, delay)) => Box::pin(
                stream::iter(events).chain(
                    stream::once(async move {
                        tokio::time::sleep(delay).await;
                        None
                    })
                    .filter_map(futures_util::future::ready),
                ),
            ),
            Some(Script::DelayedEvents(events)) => {
                Box::pin(stream::iter(events).then(|(delay, event)| async move {
                    tokio::time::sleep(delay).await;
                    event
                }))
            }
            Some(Script::Pending) => Box::pin(stream::pending()),
            None => panic!("model script exhausted"),
        }
    }
}

struct CompletingTools;

impl ToolExecutor for CompletingTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async move {
            Ok(ToolExecutionOutcome::Completed {
                message: Box::new(CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(model_text("{\"ok\":true}"))],
                    tool_calls: Vec::new(),
                    tool_call_id: Some(call.call_id),
                    tool_name: Some(call.name),
                    reasoning: None,
                }),
                terminal: DurableToolCompletion::Succeeded,
            })
        })
    }
}

struct ReconciliationTools;

impl ToolExecutor for ReconciliationTools {
    fn execute(
        &self,
        attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let completed = CompletingTools.execute(attempt, call);
        Box::pin(async move {
            let mut outcome = completed.await?;
            if let ToolExecutionOutcome::Completed { message, terminal } = &mut outcome {
                message.content = vec![CanonicalContentPart::Text(model_text(
                    r#"{"ok":false,"error":{"code":"tool_outcome_unknown"},"may_have_been_applied":true}"#,
                ))];
                *terminal = DurableToolCompletion::Indeterminate;
            }
            Ok(outcome)
        })
    }
}

/// A parallel batch mixing one ordinary Tool with one asynchronous Tool that
/// goes indeterminate and asks the Run to wait for reconciliation.
struct MixedReconciliationTools {
    wait_call: ToolCallId,
    wait_invocation_id: InvocationId,
    observed_at: UtcTimestamp,
    resume_not_before: UtcTimestamp,
}

impl ToolExecutor for MixedReconciliationTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let wait_call = self.wait_call.clone();
        let wait_invocation_id = self.wait_invocation_id;
        let observed_at = self.observed_at;
        let resume_not_before = self.resume_not_before;
        Box::pin(async move {
            if call.call_id == wait_call {
                return Ok(ToolExecutionOutcome::ReconciliationWaiting {
                    invocation_id: wait_invocation_id,
                    observed_at,
                    resume_not_before,
                });
            }
            Ok(ToolExecutionOutcome::Completed {
                message: Box::new(CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(model_text("{\"ok\":true}"))],
                    tool_calls: Vec::new(),
                    tool_call_id: Some(call.call_id),
                    tool_name: Some(call.name),
                    reasoning: None,
                }),
                terminal: DurableToolCompletion::Succeeded,
            })
        })
    }
}

#[derive(Default)]
struct CountingTools {
    executions: AtomicUsize,
}

#[derive(Default)]
struct RejectingArgumentsTools {
    validations: AtomicUsize,
    executions: AtomicUsize,
}

impl ToolExecutor for RejectingArgumentsTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        if let Some(failure) = call.planning_failure {
            return Box::pin(async move { failure.outcome(&call) });
        }
        self.validations.fetch_add(1, Ordering::SeqCst);
        Box::pin(
            async move { musteragent_loop::ToolCallPlanningFailure::InvalidValue.outcome(&call) },
        )
    }
}

impl ToolExecutor for CountingTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        if let Some(failure) = call.planning_failure {
            return Box::pin(async move { failure.outcome(&call) });
        }
        self.executions.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolExecutionOutcome::Completed {
                message: Box::new(CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(model_text("{\"ok\":true}"))],
                    tool_calls: Vec::new(),
                    tool_call_id: Some(call.call_id),
                    tool_name: Some(call.name),
                    reasoning: None,
                }),
                terminal: DurableToolCompletion::Succeeded,
            })
        })
    }
}

struct BarrierTools {
    barrier: Arc<Barrier>,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl ToolExecutor for BarrierTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst).saturating_add(1);
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.barrier.wait().await;
            tokio::task::yield_now().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(ToolExecutionOutcome::Completed {
                message: Box::new(CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(model_text("{\"ok\":true}"))],
                    tool_calls: Vec::new(),
                    tool_call_id: Some(call.call_id),
                    tool_name: Some(call.name),
                    reasoning: None,
                }),
                terminal: DurableToolCompletion::Succeeded,
            })
        })
    }
}

struct WaitingTools {
    interaction_id: InteractionId,
    journal: Arc<FixtureJournal>,
}

impl ToolExecutor for WaitingTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        _call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let interaction_id = self.interaction_id;
        Box::pin(async move {
            self.journal.mark_waiting(interaction_id);
            Ok(ToolExecutionOutcome::Waiting { interaction_id })
        })
    }
}

struct FixtureJournal {
    native_completion: Mutex<native_publication::CompletionFixture>,
    recovery: Mutex<musteragent_loop::RecoveryBudget>,
    checkpoint: Mutex<DurableLoopCheckpoint>,
    event_batches: Mutex<Vec<ModelEventBatch>>,
    committed_tool_calls: Mutex<Vec<ToolCallId>>,
    tool_results: Mutex<Vec<ToolResultCommit>>,
    model_completions: Mutex<Vec<ModelCompletionCommit>>,
    transitions: Mutex<Vec<FixtureTransition>>,
    reasoning_replays: Mutex<Vec<Option<ModelReplayText>>>,
    completion_kinds: Mutex<Vec<ModelCompletionKind>>,
    action_rejections: Mutex<Vec<ModelActionRejectionCommit>>,
    failures: Mutex<Vec<(ErrorCode, bool)>>,
    model_retry_after_hints: Mutex<Vec<Option<u64>>>,
    cancellation_observations: Mutex<Vec<Option<ModelInvocationObservation>>>,
    failure_error: Mutex<Option<LoopPortError>>,
    completion_control_race: Arc<AtomicUsize>,
    durable_stop_at: Option<durable_stop::Boundary>,
}

/// Durable transition order observed by the fixture Journal. A reconciliation
/// suspension must never precede the commit of a sibling Tool result.
#[derive(Debug, Clone, PartialEq)]
enum FixtureTransition {
    DependencyRecovery {
        retry_after_ms: Option<u64>,
    },
    ToolCommitted(ToolCallId),
    ReconciliationWait {
        invocation_id: InvocationId,
        observed_at: UtcTimestamp,
        resume_not_before: UtcTimestamp,
    },
}

impl FixtureJournal {
    fn new(checkpoint: DurableLoopCheckpoint) -> Self {
        Self::with_batches(checkpoint, Vec::new())
    }

    fn with_batches(
        checkpoint: DurableLoopCheckpoint,
        event_batches: Vec<ModelEventBatch>,
    ) -> Self {
        Self {
            native_completion: Mutex::new(Default::default()),
            recovery: Mutex::new(musteragent_loop::RecoveryBudget::default()),
            checkpoint: Mutex::new(checkpoint),
            event_batches: Mutex::new(event_batches),
            committed_tool_calls: Mutex::new(Vec::new()),
            tool_results: Mutex::new(Vec::new()),
            model_completions: Mutex::new(Vec::new()),
            transitions: Mutex::new(Vec::new()),
            reasoning_replays: Mutex::new(Vec::new()),
            completion_kinds: Mutex::new(Vec::new()),
            action_rejections: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            model_retry_after_hints: Mutex::new(Vec::new()),
            cancellation_observations: Mutex::new(Vec::new()),
            failure_error: Mutex::new(None),
            completion_control_race: Arc::new(AtomicUsize::new(0)),
            durable_stop_at: None,
        }
    }

    fn with_completion_control_race(
        checkpoint: DurableLoopCheckpoint,
        completion_control_race: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            native_completion: Mutex::new(Default::default()),
            recovery: Mutex::new(musteragent_loop::RecoveryBudget::default()),
            checkpoint: Mutex::new(checkpoint),
            event_batches: Mutex::new(Vec::new()),
            committed_tool_calls: Mutex::new(Vec::new()),
            tool_results: Mutex::new(Vec::new()),
            model_completions: Mutex::new(Vec::new()),
            transitions: Mutex::new(Vec::new()),
            reasoning_replays: Mutex::new(Vec::new()),
            completion_kinds: Mutex::new(Vec::new()),
            action_rejections: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            model_retry_after_hints: Mutex::new(Vec::new()),
            cancellation_observations: Mutex::new(Vec::new()),
            failure_error: Mutex::new(None),
            completion_control_race,
            durable_stop_at: None,
        }
    }

    fn with_failure_error(checkpoint: DurableLoopCheckpoint, failure_error: LoopPortError) -> Self {
        Self {
            native_completion: Mutex::new(Default::default()),
            recovery: Mutex::new(musteragent_loop::RecoveryBudget::default()),
            checkpoint: Mutex::new(checkpoint),
            event_batches: Mutex::new(Vec::new()),
            committed_tool_calls: Mutex::new(Vec::new()),
            tool_results: Mutex::new(Vec::new()),
            model_completions: Mutex::new(Vec::new()),
            transitions: Mutex::new(Vec::new()),
            reasoning_replays: Mutex::new(Vec::new()),
            completion_kinds: Mutex::new(Vec::new()),
            action_rejections: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            model_retry_after_hints: Mutex::new(Vec::new()),
            cancellation_observations: Mutex::new(Vec::new()),
            failure_error: Mutex::new(Some(failure_error)),
            completion_control_race: Arc::new(AtomicUsize::new(0)),
            durable_stop_at: None,
        }
    }

    fn snapshot(&self) -> DurableLoopCheckpoint {
        lock(&self.checkpoint).clone()
    }

    fn mark_waiting(&self, interaction_id: InteractionId) {
        lock(&self.checkpoint).phase = LoopPhase::Waiting { interaction_id };
    }

    fn batches(&self) -> Vec<ModelEventBatch> {
        lock(&self.event_batches).clone()
    }

    fn committed_tool_calls(&self) -> Vec<ToolCallId> {
        lock(&self.committed_tool_calls).clone()
    }

    fn transitions(&self) -> Vec<FixtureTransition> {
        lock(&self.transitions).clone()
    }

    fn reasoning_replays(&self) -> Vec<Option<ModelReplayText>> {
        lock(&self.reasoning_replays).clone()
    }

    fn completion_kinds(&self) -> Vec<ModelCompletionKind> {
        lock(&self.completion_kinds).clone()
    }

    fn action_rejections(&self) -> Vec<ModelActionRejectionCommit> {
        lock(&self.action_rejections).clone()
    }

    fn failures(&self) -> Vec<(ErrorCode, bool)> {
        lock(&self.failures).clone()
    }

    fn model_retry_after_hints(&self) -> Vec<Option<u64>> {
        lock(&self.model_retry_after_hints).clone()
    }

    fn cancellation_observations(&self) -> Vec<Option<ModelInvocationObservation>> {
        lock(&self.cancellation_observations).clone()
    }
}

impl AgentLoopJournal for FixtureJournal {
    fn completion_readiness(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::RunCompletionReadiness, LoopPortError>>
    {
        Box::pin(async move { Ok(self.native_readiness()) })
    }

    fn recovery_budget(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::RecoveryBudget, LoopPortError>> {
        Box::pin(async move { Ok(*lock(&self.recovery)) })
    }

    fn load_model_observation(
        &self,
        _attempt: &AttemptContext,
        invocation_id: InvocationId,
    ) -> ApplicationFuture<'_, Result<ModelInvocationObservation, LoopPortError>> {
        Box::pin(async move {
            let mut observation = ModelInvocationObservation {
                invocation_id,
                provider_response_ref: None,
                response_model_key: None,
                usage: None,
                public_output_observed: false,
                replay_barrier_observed: false,
            };
            for batch in self
                .batches()
                .iter()
                .filter(|batch| batch.invocation_id == invocation_id)
            {
                for event in &batch.events {
                    observation.public_output_observed |= event.is_public_output();
                    observation.replay_barrier_observed |= event.blocks_automatic_replay();
                    match event {
                        ModelStreamEvent::ResponseStarted {
                            response_ref,
                            response_model_key,
                        } => {
                            observation.provider_response_ref = Some(response_ref.clone());
                            observation.response_model_key = Some(response_model_key.clone());
                        }
                        ModelStreamEvent::Usage(usage) => observation.usage = Some(*usage),
                        ModelStreamEvent::TextDelta { delta, .. } if !delta.as_str().is_empty() => {
                            observation.public_output_observed = true;
                            observation.replay_barrier_observed = true;
                        }
                        _ => {}
                    }
                }
            }
            Ok(observation)
        })
    }

    fn load_checkpoint(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move { Ok(self.snapshot()) })
    }

    fn recover_checkpoint(
        &self,
        _attempt: &AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        _constraints: musteragent_loop::ModelRecoveryConstraints,
    ) -> ApplicationFuture<'_, Result<RecoveredLoopState, LoopPortError>> {
        Box::pin(async move {
            let mut recovered = checkpoint;
            if matches!(recovered.phase, LoopPhase::ModelInFlight { .. }) {
                // The production Journal terminalizes the abandoned physical
                // invocation in the same fenced transaction before rewinding.
                recovered.phase = LoopPhase::ReadyForModel;
                *lock(&self.checkpoint) = recovered.clone();
            }
            let pending_tool_calls = match &recovered.phase {
                LoopPhase::ToolsPending {
                    remaining_tool_call_ids,
                    ..
                } => recover_calls(&self.batches(), remaining_tool_call_ids)?,
                _ => Vec::new(),
            };
            Ok(RecoveredLoopState {
                pending_model_recovery: None,
                checkpoint: recovered,
                pending_tool_calls,
                recovered_model_completion: None,
                replacement_attempt_scheduled: false,
            })
        })
    }

    fn begin_model(
        &self,
        _attempt: &AttemptContext,
        iteration: &ModelIteration,
        _recovery_capability: musteragent_runtime_model::ProviderRecoveryCapability,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let invocation_id = iteration.invocation_id;
        let iteration_number = iteration.iteration;
        let attempt = _attempt.clone();
        Box::pin(async move {
            if self.durable_stop_at == Some(durable_stop::Boundary::Begin) {
                return self.acknowledge_cancellation(&attempt, None).await;
            }
            let mut checkpoint = lock(&self.checkpoint);
            checkpoint.iteration = iteration_number;
            checkpoint.phase = LoopPhase::ModelInFlight { invocation_id };
            Ok(checkpoint.clone())
        })
    }

    fn mark_model_dispatched(
        &self,
        _attempt: &AttemptContext,
        _invocation_id: InvocationId,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move { Ok(self.snapshot()) })
    }

    fn append_model_events(
        &self,
        _attempt: &AttemptContext,
        batch: ModelEventBatch,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            lock(&self.event_batches).push(batch);
            Ok(self.snapshot())
        })
    }

    fn stage_model_completion(
        &self,
        _attempt: &AttemptContext,
        _completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move { Ok(self.snapshot()) })
    }

    fn finish_model(
        &self,
        _attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            lock(&self.model_completions).push(completion.clone());
            let ModelCompletionCommit {
                invocation_id,
                kind,
                tool_calls,
                reasoning_replay,
                usage,
                continuation_ref,
                ..
            } = completion;
            lock(&self.reasoning_replays).push(reasoning_replay);
            lock(&self.completion_kinds).push(kind);
            *lock(&self.recovery) = musteragent_loop::RecoveryBudget::default();
            let mut checkpoint = lock(&self.checkpoint);
            if let Some(usage) = usage {
                checkpoint.total_tokens =
                    checkpoint
                        .total_tokens
                        .saturating_add(usage.total_tokens.unwrap_or_else(|| {
                            usage
                                .input_tokens_uncached
                                .saturating_add(usage.output_tokens)
                        }));
            }
            checkpoint.continuation_ref = continuation_ref;
            checkpoint.continuation_source_invocation_id =
                checkpoint.continuation_ref.as_ref().map(|_| invocation_id);
            checkpoint.consecutive_empty_responses = if kind == ModelCompletionKind::EmptyResponse {
                checkpoint.consecutive_empty_responses.saturating_add(1)
            } else {
                0
            };
            checkpoint.phase = if kind == ModelCompletionKind::ToolCalls {
                LoopPhase::ToolsPending {
                    invocation_id,
                    remaining_tool_call_ids: tool_calls
                        .into_iter()
                        .map(|call| call.call_id)
                        .collect(),
                }
            } else {
                LoopPhase::ReadyForModel
            };
            Ok(checkpoint.clone())
        })
    }

    fn commit_tool_result(
        &self,
        _attempt: &AttemptContext,
        result: ToolResultCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            lock(&self.tool_results).push(result.clone());
            lock(&self.committed_tool_calls).push(result.call_id.clone());
            lock(&self.transitions).push(FixtureTransition::ToolCommitted(result.call_id.clone()));
            let mut checkpoint = lock(&self.checkpoint);
            let finished = {
                let LoopPhase::ToolsPending {
                    remaining_tool_call_ids,
                    ..
                } = &mut checkpoint.phase
                else {
                    return Err(port_error(ErrorCode::InternalInvariantViolated, false));
                };
                remaining_tool_call_ids.retain(|call_id| call_id != &result.call_id);
                remaining_tool_call_ids.is_empty()
            };
            checkpoint.completed_tool_calls = checkpoint.completed_tool_calls.saturating_add(1);
            if finished {
                checkpoint.phase = LoopPhase::ReadyForModel;
            }
            Ok(checkpoint.clone())
        })
    }

    fn finish_failed_model(
        &self,
        _attempt: &AttemptContext,
        commit: ModelFailureCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = _attempt.clone();
        Box::pin(async move {
            if let Some(error) = lock(&self.failure_error).take() {
                return Err(error);
            }
            if self.durable_stop_at == Some(durable_stop::Boundary::ModelFailure) {
                return self
                    .acknowledge_cancellation(&attempt, Some(commit.observation))
                    .await;
            }
            let ModelFailureCommit {
                recovery_policy,
                observation,
                provider_failure,
                retry_after_ms,
                resolution,
            } = commit;
            lock(&self.model_retry_after_hints).push(retry_after_ms);
            let batches = self.batches();
            let durable_events = batches
                .iter()
                .filter(|batch| batch.invocation_id == observation.invocation_id)
                .flat_map(|batch| &batch.events)
                .collect::<Vec<_>>();
            let durable_public_output = durable_events.iter().any(|event| event.is_public_output());
            let durable_replay_barrier = durable_events
                .iter()
                .any(|event| event.blocks_automatic_replay());
            let durable_identity = durable_events.iter().find_map(|event| match event {
                ModelStreamEvent::ResponseStarted {
                    response_ref,
                    response_model_key,
                } => Some((response_ref, response_model_key)),
                _ => None,
            });
            let durable_usage = durable_events.iter().rev().find_map(|event| match event {
                ModelStreamEvent::Usage(usage) => Some(*usage),
                _ => None,
            });
            if observation.replay_barrier_observed != durable_replay_barrier
                || observation.public_output_observed != durable_public_output
                || observation.provider_response_ref.as_ref()
                    != durable_identity.map(|(response_ref, _)| response_ref)
                || observation.response_model_key.as_ref()
                    != durable_identity.map(|(_, model_key)| model_key)
                || observation.usage != durable_usage
            {
                return Err(port_error(ErrorCode::InternalInvariantViolated, false));
            }
            let mut checkpoint = lock(&self.checkpoint);
            if !matches!(
                checkpoint.phase,
                LoopPhase::ModelInFlight { invocation_id }
                    if invocation_id == observation.invocation_id
            ) {
                return Err(port_error(ErrorCode::InternalInvariantViolated, false));
            }
            match resolution {
                ModelFailureResolution::RetryFallbackRoute {
                    next_route_ordinal: next,
                }
                | ModelFailureResolution::ProviderFallbackV1 {
                    next_route_ordinal: next,
                    ..
                } if resolution.permits_fallback(provider_failure, recovery_policy)
                    && !durable_replay_barrier
                    && next == checkpoint.active_model_route_ordinal.saturating_add(1) =>
                {
                    let mut budget = lock(&self.recovery);
                    budget.prior_transient_distance = if provider_failure.permits_new_attempt()
                        || budget.prior_transient_distance > 0
                    {
                        budget.prior_transient_distance + 1
                    } else {
                        0
                    };
                    budget.provider_failures = 0;
                    checkpoint.active_model_route_ordinal = next;
                    checkpoint.phase = LoopPhase::ReadyForModel;
                }
                ModelFailureResolution::ExhaustedProviderRoundV1 {
                    cause,
                    retry_route_ordinal,
                } if retry_route_ordinal
                    == ModelFailureResolution::retry_ordinal(
                        cause,
                        checkpoint.active_model_route_ordinal,
                        *lock(&self.recovery),
                    ) =>
                {
                    checkpoint.active_model_route_ordinal = retry_route_ordinal.unwrap_or(0);
                    checkpoint.phase = LoopPhase::ReadyForModel;
                }
                ModelFailureResolution::RetryNewAttempt
                    if provider_failure.permits_new_attempt() =>
                {
                    let mut budget = lock(&self.recovery);
                    budget.provider_failures = budget.provider_failures.saturating_add(1);
                    checkpoint.phase = LoopPhase::ReadyForModel;
                }
                ModelFailureResolution::FailRun { failure } if !failure.retryable => {
                    lock(&self.failures).push((failure.code, false));
                    checkpoint.phase = LoopPhase::Failed;
                }
                _ => return Err(port_error(ErrorCode::InternalInvariantViolated, false)),
            }
            checkpoint.continuation_ref = None;
            checkpoint.continuation_source_invocation_id = None;
            Ok(checkpoint.clone())
        })
    }

    fn finish_rejected_model_action(
        &self,
        _attempt: &AttemptContext,
        commit: ModelActionRejectionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.finish_action_rejection(commit)
    }

    fn complete(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RunCompletionDisposition, LoopPortError>> {
        Box::pin(async move {
            if self.native_terminal_race() {
                return Ok(RunCompletionDisposition::RequiredWorkPending);
            }
            if self
                .completion_control_race
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(RunCompletionDisposition::ControlsPending);
            }
            let mut checkpoint = lock(&self.checkpoint);
            checkpoint.phase = LoopPhase::Completed;
            Ok(RunCompletionDisposition::Completed(checkpoint.clone()))
        })
    }

    fn fail(
        &self,
        _attempt: &AttemptContext,
        code: ErrorCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
        _max_attempts: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        let attempt = _attempt.clone();
        Box::pin(async move {
            if let Some(error) = lock(&self.failure_error).take() {
                return Err(error);
            }
            if self.durable_stop_at == Some(durable_stop::Boundary::PortFailure) {
                return self.acknowledge_cancellation(&attempt, None).await;
            }
            lock(&self.failures).push((code, retryable));
            if retryable {
                lock(&self.transitions)
                    .push(FixtureTransition::DependencyRecovery { retry_after_ms });
            }
            let mut checkpoint = lock(&self.checkpoint);
            checkpoint.phase = if retryable {
                if matches!(checkpoint.phase, LoopPhase::ToolsPending { .. }) {
                    checkpoint.phase.clone()
                } else {
                    LoopPhase::ReadyForModel
                }
            } else {
                LoopPhase::Failed
            };
            Ok(checkpoint.clone())
        })
    }

    fn interrupt_external_outcome(
        &self,
        _attempt: &AttemptContext,
        _reconciliation_ref: OpaqueExternalRef,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            let mut checkpoint = lock(&self.checkpoint);
            checkpoint.phase = LoopPhase::Interrupted;
            Ok(checkpoint.clone())
        })
    }

    fn acknowledge_coordination_interrupt(
        &self,
        _attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            lock(&self.cancellation_observations).push(observation);
            let mut checkpoint = lock(&self.checkpoint);
            checkpoint.phase = LoopPhase::Interrupted;
            Ok(checkpoint.clone())
        })
    }

    fn acknowledge_cancellation(
        &self,
        _attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            lock(&self.cancellation_observations).push(observation);
            let mut checkpoint = lock(&self.checkpoint);
            checkpoint.phase = LoopPhase::Cancelled;
            Ok(checkpoint.clone())
        })
    }

    fn wait_for_reconciliation(
        &self,
        _attempt: &AttemptContext,
        invocation_id: InvocationId,
        observed_at: UtcTimestamp,
        resume_not_before: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), LoopPortError>> {
        Box::pin(async move {
            lock(&self.transitions).push(FixtureTransition::ReconciliationWait {
                invocation_id,
                observed_at,
                resume_not_before,
            });
            Ok(())
        })
    }
}

fn recover_calls(
    batches: &[ModelEventBatch],
    remaining: &[ToolCallId],
) -> Result<Vec<PlannedToolCall>, LoopPortError> {
    let mut calls = BTreeMap::<ToolCallId, PlannedToolCall>::new();
    for event in batches.iter().flat_map(|batch| &batch.events) {
        match event {
            ModelStreamEvent::ToolCallStarted {
                call_id,
                name,
                provider_replay,
                ..
            } => {
                calls.insert(
                    call_id.clone(),
                    PlannedToolCall {
                        argument_diagnostic: None,
                        call_id: call_id.clone(),
                        name: name.clone(),
                        arguments: JsonValue::Object(BTreeMap::new()),
                        provider_replay: provider_replay.clone(),
                        planning_failure: None,
                    },
                );
            }
            ModelStreamEvent::ToolCallCompleted {
                call_id, arguments, ..
            } => {
                let Some(call) = calls.get_mut(call_id) else {
                    return Err(port_error(ErrorCode::InternalInvariantViolated, false));
                };
                call.arguments = arguments.clone();
            }
            _ => {}
        }
    }
    remaining
        .iter()
        .map(|call_id| {
            calls
                .remove(call_id)
                .ok_or_else(|| port_error(ErrorCode::InternalInvariantViolated, false))
        })
        .collect()
}

fn port_error(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

fn provider_ref(value: &str) -> ProviderRequestRef {
    ProviderRequestRef::from_opaque(result_or_panic(OpaqueExternalRef::new(value)))
}

fn model_key() -> ModelKey {
    ModelKey::new("model-fixture").expect("model key")
}

fn model_text(value: &str) -> ModelText {
    result_or_panic(ModelText::new(value))
}

fn tool_call_id(value: &str) -> ToolCallId {
    result_or_panic(ToolCallId::new(value))
}

fn wire_name(value: &str) -> WireName {
    result_or_panic(WireName::new(value))
}

fn versioned(value: &str) -> VersionedKey {
    result_or_panic(VersionedKey::new(value))
}

fn non_zero_u32(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => panic!("fixture must be non-zero"),
    }
}

fn non_zero_u64(value: u64) -> NonZeroU64 {
    match NonZeroU64::new(value) {
        Some(value) => value,
        None => panic!("fixture must be non-zero"),
    }
}

fn non_zero_usize(value: usize) -> NonZeroUsize {
    match NonZeroUsize::new(value) {
        Some(value) => value,
        None => panic!("fixture must be non-zero"),
    }
}

fn result_or_panic<T, E: std::fmt::Debug>(value: Result<T, E>) -> T {
    match value {
        Ok(value) => value,
        Err(error) => panic!("fixture construction failed: {error:?}"),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(value) => value,
        Err(error) => panic!("fixture mutex poisoned: {error}"),
    }
}

#[tokio::test(start_paused = true)]
async fn sustained_model_work_is_read_during_original_journal_commits_and_outlives_idle_window() {
    activity_model::sustained_model_work_is_read_during_original_journal_commits_and_outlives_idle_window().await;
}

#[tokio::test(start_paused = true)]
async fn repeated_model_usage_cannot_renew_idle_and_unknown_stop_remains_recorded() {
    activity_model::repeated_model_usage_cannot_renew_idle_and_unknown_stop_remains_recorded()
        .await;
}

#[tokio::test(start_paused = true)]
async fn model_cancellation_drains_owned_stream_before_original_terminal_receipt() {
    activity_model::model_cancellation_drains_owned_stream_before_original_terminal_receipt().await;
}

#[tokio::test(start_paused = true)]
async fn native_model_end_uses_original_drain_budget_instead_of_idle_budget() {
    activity_model::native_model_end_uses_original_drain_budget_instead_of_idle_budget().await;
}

#[tokio::test(start_paused = true)]
async fn native_model_end_can_finish_after_idle_due_within_original_drain_budget() {
    activity_model::native_model_end_can_finish_after_idle_due_within_original_drain_budget().await;
}

#[tokio::test(start_paused = true)]
async fn model_startup_and_transport_failure_need_independent_stop_proof() {
    activity_model::model_startup_and_transport_failure_need_independent_stop_proof().await;
}

#[tokio::test(start_paused = true)]
async fn stalled_model_journal_is_an_infrastructure_interruption_without_false_idle_stop() {
    activity_model::stalled_model_journal_is_an_infrastructure_interruption_without_false_idle_stop().await;
}

#[tokio::test(start_paused = true)]
async fn activity_tool_cancellation_keeps_every_owned_cleanup_future_polled() {
    activity_model::activity_tool_cancellation_keeps_every_owned_cleanup_future_polled().await;
}
#[tokio::test(start_paused = true)]
async fn activity_tool_cannot_fall_through_to_an_unsupported_legacy_executor() {
    activity_model::activity_tool_cannot_fall_through_to_an_unsupported_legacy_executor().await;
}

#[tokio::test]
async fn activity_parallel_batch_commits_siblings_before_cancellation_and_preserves_unknown() {
    activity_model::activity_parallel_batch_commits_siblings_before_cancellation_and_preserves_unknown().await;
}

#[tokio::test]
async fn activity_parallel_commit_failure_preserves_sibling_results_and_unknown() {
    activity_model::activity_parallel_commit_failure_preserves_sibling_results_and_unknown().await;
}

#[tokio::test(start_paused = true)]
async fn tool_failures_timeouts_and_unknown_effects_return_to_model_in_the_same_run() {
    activity_model::tool_feedback::tool_failures_timeouts_and_unknown_effects_return_to_model_in_the_same_run().await;
}

#[tokio::test(start_paused = true)]
async fn repeated_tool_failures_do_not_exhaust_agent_iterations_or_tool_count() {
    activity_model::tool_feedback::repeated_tool_failures_do_not_exhaust_agent_iterations_or_tool_count().await;
}

#[tokio::test(start_paused = true)]
async fn unknown_model_stop_preserves_call_evidence_and_recovers_the_same_agent_run() {
    activity_model::tool_feedback::unknown_model_stop_preserves_call_evidence_and_recovers_the_same_agent_run().await;
}

#[path = "durable_loop/delivery_repair.rs"]
mod delivery_repair;
#[path = "durable_loop/rejection_journal.rs"]
mod rejection_journal;

#[path = "durable_loop/native_publication.rs"]
mod native_publication;
