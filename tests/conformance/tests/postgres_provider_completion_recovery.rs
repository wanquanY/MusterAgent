#![cfg(test)]

use std::collections::BTreeMap;
use std::future::pending;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[path = "postgres_provider_completion_recovery/context_recovery_support.rs"]
mod context_recovery_support;
use musteragent_conformance::fixtures::context_snapshot;
#[path = "postgres_provider_completion_recovery/gateway_ticket.rs"]
mod gateway_ticket;
#[path = "postgres_provider_completion_recovery/generation_compaction.rs"]
mod generation_compaction;
#[path = "postgres_provider_completion_recovery/intent_security_support.rs"]
mod intent_security_support;
#[path = "support/production_context.rs"]
mod production_context;
#[path = "postgres_provider_completion_recovery/read_fault.rs"]
mod read_fault;
#[path = "support/running_loop.rs"]
mod running_loop;
#[path = "postgres_provider_completion_recovery/runtime_fixture.rs"]
mod runtime_fixture;
#[path = "postgres_provider_recovery/support.rs"]
mod support;
#[path = "support/tool_material.rs"]
mod tool_material;

use musteragent_cloud_worker::{
    CapabilityDispatcher, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    DurableToolExecutor, WorkerTraceContextSource,
};
use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_identity::{
    ContentId, DottedKey, InvocationId, TenantId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_loop::{
    AgentLoopClock, AgentLoopExit, AgentLoopJournal, ContextPlanError, ContextPlanInput,
    ContextPlanner, DefaultLoopPolicy, DurableAgentLoop, DurableLoopCheckpoint,
    ExecutionPolicySource, InvocationSupervisor, LoopControlQueue, LoopPortError,
    ModelActionRejectionCommit, ModelCompletionCommit, ModelEventBatch, ModelFailureCommit,
    ModelInvocationObservation, ModelIteration, ModelRecoveryConstraints, PinnedRunExecutionPolicy,
    PlannedToolCall, RecoveredLoopState, RunCompletionDisposition, ToolExecutionOutcome,
    ToolExecutor, ToolResultCommit,
};
use musteragent_model::ModelKey;
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresContentStore, PostgresDispatchStore,
    PostgresFencedToolInvocationJournal, migrate,
};
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    DurableToolInvocation, FencedToolInvocationJournal, RuntimeClock, ToolExecutionMaterial,
};
use musteragent_runtime_content::{BlobStore, ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, OpaqueExternalRef, TraceContext, TraceParent,
};
use musteragent_runtime_model::{
    CanonicalModelRequest, CanonicalRole, CanonicalToolDefinition, JsonValue, ModelEventStream,
    ModelProvider, ModelStreamEvent, ModelText, ModelUsage, ProviderRecoveryCapability,
    ProviderRequestRef, StopReason,
};
use musteragent_runtime_snapshot::RunExecutionPolicySnapshot;
use musteragent_scheduler::{ClaimOutcome, DispatchStore};
use musteragent_storage_protection::ProtectedContentKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{ToolKey, WireName};
use musteragent_wire::ProtobufMeteringPublicationEncoder;
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder};
use tokio::sync::Notify;
use tokio_stream::wrappers::ReceiverStream;

use context_recovery_support::{FixedTrace, SuccessfulCapability};
use intent_security_support::{DeleteGateBlobStore, LargeToolThenFinalProvider};
use running_loop::{Fixture, attempt_telemetry, database_now, keyring, seed_running_loop};
use support::model_iteration;
use tool_material::production_tool_material_source;

#[tokio::test]
async fn unavailable_intent_read_interrupts_without_redispatch_then_recovers_exactly() {
    read_fault::assert_unavailable_intent_read_recovers_without_redispatch().await;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum FaultMode {
    None,
    AppendBeforeCommit,
    AppendAfterCommit,
    StageBeforeCommit,
    StageAfterCommit,
    StageUnavailable,
    FinishBeforeCommit,
    FinishAfterCommit,
    FinishUnavailable,
}

struct FaultJournal {
    inner: Arc<dyn AgentLoopJournal>,
    mode: FaultMode,
    fired: AtomicBool,
    append_attempts: Mutex<Vec<ModelEventBatch>>,
    stage_attempts: Mutex<Vec<ModelCompletionCommit>>,
    finish_attempts: Mutex<Vec<ModelCompletionCommit>>,
    fail_calls: AtomicUsize,
}

impl FaultJournal {
    fn new(inner: Arc<dyn AgentLoopJournal>, mode: FaultMode) -> Self {
        Self {
            inner,
            mode,
            fired: AtomicBool::new(false),
            append_attempts: Mutex::new(Vec::new()),
            stage_attempts: Mutex::new(Vec::new()),
            finish_attempts: Mutex::new(Vec::new()),
            fail_calls: AtomicUsize::new(0),
        }
    }

    fn should_fault(&self, mode: FaultMode) -> bool {
        self.mode == mode && !self.fired.swap(true, Ordering::SeqCst)
    }

    fn unavailable() -> LoopPortError {
        LoopPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        }
    }
}

impl AgentLoopJournal for FaultJournal {
    fn recovery_budget(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::RecoveryBudget, LoopPortError>> {
        self.inner.recovery_budget(attempt)
    }

    fn load_model_observation(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
    ) -> ApplicationFuture<'_, Result<musteragent_loop::ModelInvocationObservation, LoopPortError>>
    {
        self.inner.load_model_observation(attempt, invocation_id)
    }

    fn load_checkpoint(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.load_checkpoint(attempt)
    }

    fn recover_checkpoint(
        &self,
        attempt: &AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        constraints: ModelRecoveryConstraints,
    ) -> ApplicationFuture<'_, Result<RecoveredLoopState, LoopPortError>> {
        self.inner
            .recover_checkpoint(attempt, checkpoint, constraints)
    }

    fn begin_model(
        &self,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        recovery_capability: ProviderRecoveryCapability,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner
            .begin_model(attempt, iteration, recovery_capability)
    }

    fn mark_model_dispatched(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.mark_model_dispatched(attempt, invocation_id)
    }

    fn append_model_events(
        &self,
        attempt: &AttemptContext,
        batch: ModelEventBatch,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.append_attempts
            .lock()
            .expect("append attempts")
            .push(batch.clone());
        let targeted = batch.batch_index == 1;
        let before = targeted && self.should_fault(FaultMode::AppendBeforeCommit);
        let after = targeted && self.should_fault(FaultMode::AppendAfterCommit);
        let inner = self.inner.clone();
        let attempt = attempt.clone();
        Box::pin(async move {
            if before {
                return Err(Self::unavailable());
            }
            let result = inner.append_model_events(&attempt, batch).await;
            if after && result.is_ok() {
                return Err(Self::unavailable());
            }
            result
        })
    }

    fn stage_model_completion(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.stage_attempts
            .lock()
            .expect("stage attempts")
            .push(completion.clone());
        let before = self.should_fault(FaultMode::StageBeforeCommit);
        let after = self.should_fault(FaultMode::StageAfterCommit);
        let unavailable = self.mode == FaultMode::StageUnavailable;
        let inner = self.inner.clone();
        let attempt = attempt.clone();
        Box::pin(async move {
            if before || unavailable {
                return Err(Self::unavailable());
            }
            let result = inner.stage_model_completion(&attempt, completion).await;
            if after && result.is_ok() {
                return Err(Self::unavailable());
            }
            result
        })
    }

    fn finish_model(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.finish_attempts
            .lock()
            .expect("finish attempts")
            .push(completion.clone());
        let before = self.should_fault(FaultMode::FinishBeforeCommit);
        let after = self.should_fault(FaultMode::FinishAfterCommit);
        let unavailable = self.mode == FaultMode::FinishUnavailable;
        let inner = self.inner.clone();
        let attempt = attempt.clone();
        Box::pin(async move {
            if before || unavailable {
                return Err(Self::unavailable());
            }
            let result = inner.finish_model(&attempt, completion).await;
            if after && result.is_ok() {
                return Err(Self::unavailable());
            }
            result
        })
    }

    fn finish_failed_model(
        &self,
        attempt: &AttemptContext,
        commit: ModelFailureCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.finish_failed_model(attempt, commit)
    }

    fn finish_rejected_model_action(
        &self,
        attempt: &AttemptContext,
        commit: ModelActionRejectionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.finish_rejected_model_action(attempt, commit)
    }

    fn commit_tool_result(
        &self,
        attempt: &AttemptContext,
        result: ToolResultCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.commit_tool_result(attempt, result)
    }

    fn wait_for_reconciliation(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        observed_at: UtcTimestamp,
        resume_not_before: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), LoopPortError>> {
        self.inner
            .wait_for_reconciliation(attempt, invocation_id, observed_at, resume_not_before)
    }

    fn complete(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RunCompletionDisposition, LoopPortError>> {
        self.inner.complete(attempt)
    }

    fn fail(
        &self,
        attempt: &AttemptContext,
        code: ErrorCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
        max_attempts: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.fail_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .fail(attempt, code, retryable, retry_after_ms, max_attempts)
    }

    fn interrupt_external_outcome(
        &self,
        attempt: &AttemptContext,
        reconciliation_ref: musteragent_runtime_contract::OpaqueExternalRef,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner
            .interrupt_external_outcome(attempt, reconciliation_ref)
    }

    fn acknowledge_coordination_interrupt(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner
            .acknowledge_coordination_interrupt(attempt, observation)
    }

    fn acknowledge_cancellation(
        &self,
        attempt: &AttemptContext,
        observation: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.inner.acknowledge_cancellation(attempt, observation)
    }
}

struct FixedClock(UtcTimestamp);

impl AgentLoopClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

impl RuntimeClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

struct FixedExecutionPolicy {
    now: UtcTimestamp,
}

impl ExecutionPolicySource for FixedExecutionPolicy {
    fn load(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let mut snapshot = RunExecutionPolicySnapshot::production_defaults();
        snapshot.max_model_events_per_commit = NonZeroUsize::new(2).expect("non-zero");
        let policy = PinnedRunExecutionPolicy {
            snapshot,
            admitted_at: self.now,
            model_route_count: NonZeroU32::MIN,
        };
        Box::pin(async move { Ok(policy) })
    }
}

struct PlanningContext {
    fixture: Fixture,
    content: Arc<dyn ContentStore>,
    route: musteragent_runtime_snapshot::PinnedModelRoute,
    request_ref: musteragent_content_ref::ContentRef,
    exposed_tools: Vec<CanonicalToolDefinition>,
}

impl ContextPlanner for PlanningContext {
    fn prepare_model_iteration(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        let mut iteration = model_iteration(
            self.fixture,
            input.attempt,
            self.request_ref.clone(),
            input.next_iteration,
        );
        iteration.request.continuation_ref = input.continuation_ref;
        iteration.request.invocation.timing = input.invocation_timing;
        iteration.request.route = self.route.route.clone();
        iteration.request.credential_grant_ref = self.route.credential_grant_ref.clone();
        iteration.request.tools = self.exposed_tools.clone();
        Box::pin(async move {
            runtime_fixture::freeze(self.content.as_ref(), &mut iteration).await;
            Ok(iteration)
        })
    }
}

/// Exposes one provider Tool in the canonical request catalog. Without this
/// the Loop correctly rejects the Provider's unexposed call and redispatches
/// for a correction, which is not the flow these fault-injection tests target.
fn exposed_tool_definition(name: &str) -> CanonicalToolDefinition {
    CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(
            VersionedKey::new(format!("fixture.{name}@1")).expect("tool key"),
        ),
        model_name: WireName::new(name).expect("wire name"),
        name: WireName::new(name).expect("wire name"),
        description: ModelText::new("completion recovery fixture Tool").expect("description"),
        input_schema_ref: SchemaRef {
            schema_key: VersionedKey::new(format!("fixture.{name}.input@1")).expect("schema key"),
            schema_hash: Sha256::digest(format!("schema:{name}")),
        },
        input_schema: JsonValue::Object(BTreeMap::from([(
            "type".to_owned(),
            JsonValue::String("object".to_owned()),
        )])),
        strict: true,
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

struct NoTools;

impl ToolExecutor for NoTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        _call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async { pending().await })
    }
}

struct EmptyThenFinalProvider {
    calls: AtomicUsize,
    second_started: Arc<Notify>,
    release_second: Arc<Notify>,
}

impl EmptyThenFinalProvider {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            second_started: Arc::new(Notify::new()),
            release_second: Arc::new(Notify::new()),
        }
    }
}

impl ModelProvider for EmptyThenFinalProvider {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Box::pin(tokio_stream::iter(
                empty_no_usage_events().into_iter().map(Ok),
            )),
            1 => {
                self.second_started.notify_one();
                let (sender, receiver) = tokio::sync::mpsc::channel(4);
                let release = self.release_second.clone();
                tokio::spawn(async move {
                    release.notified().await;
                    for event in final_no_usage_events() {
                        if sender.send(Ok(event)).await.is_err() {
                            break;
                        }
                    }
                });
                Box::pin(ReceiverStream::new(receiver))
            }
            _ => panic!("unexpected Provider redispatch"),
        }
    }
}

struct SingleCompletionProvider {
    calls: AtomicUsize,
    staging_fault: Option<Arc<DeleteGateBlobStore>>,
}

struct ReasoningThenFinalProvider {
    calls: AtomicUsize,
}

impl ReasoningThenFinalProvider {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl ModelProvider for ReasoningThenFinalProvider {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        let events = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => reasoning_only_max_output_events(),
            1 => final_no_usage_events(),
            _ => panic!("unexpected Provider redispatch"),
        };
        Box::pin(tokio_stream::iter(events.into_iter().map(Ok)))
    }
}

impl SingleCompletionProvider {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            staging_fault: None,
        }
    }
}

impl ModelProvider for SingleCompletionProvider {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        if let Some(gate) = &self.staging_fault {
            gate.put_unavailable.store(true, Ordering::SeqCst);
        }
        Box::pin(tokio_stream::iter(
            completion_with_usage_events().into_iter().map(Ok),
        ))
    }
}

struct ToolThenFinalProvider {
    calls: AtomicUsize,
    requests: Mutex<Vec<CanonicalModelRequest>>,
    response_model_key: ModelKey,
}

impl ToolThenFinalProvider {
    fn new() -> Self {
        Self::with_response_model_key(response_model_key())
    }

    fn with_response_model_key(response_model_key: ModelKey) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            response_model_key,
        }
    }

    fn requests(&self) -> Vec<CanonicalModelRequest> {
        self.requests.lock().expect("Provider requests").clone()
    }
}

impl ModelProvider for ToolThenFinalProvider {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        self.requests
            .lock()
            .expect("Provider requests")
            .push(request);
        let events = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => tool_completion_events(&self.response_model_key),
            1 => final_no_usage_events_for(&self.response_model_key),
            _ => panic!("unexpected Provider redispatch"),
        };
        Box::pin(tokio_stream::iter(events.into_iter().map(Ok)))
    }
}

struct RecordingTools {
    calls: Mutex<Vec<PlannedToolCall>>,
    inner: Arc<dyn ToolExecutor>,
}

impl ToolExecutor for RecordingTools {
    fn execute(
        &self,
        attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        self.inner.execute(attempt, call)
    }
    fn execute_activity_with_telemetry<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        telemetry: &'a musteragent_telemetry::AttemptTelemetryContext,
        supervisor: &'a InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, musteragent_loop::InvocationAborted>,
    > {
        self.calls.lock().unwrap().push(call.clone());
        self.inner
            .execute_activity_with_telemetry(attempt, telemetry, supervisor, call)
    }
}

fn empty_no_usage_events() -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("empty-no-usage"),
            response_model_key: response_model_key(),
        },
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        },
    ]
}

fn final_no_usage_events() -> Vec<ModelStreamEvent> {
    final_no_usage_events_for(&response_model_key())
}

fn final_no_usage_events_for(response_model_key: &ModelKey) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("final-no-usage"),
            response_model_key: response_model_key.clone(),
        },
        ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: ModelText::new("completed without usage").expect("text"),
        },
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        },
    ]
}

fn completion_with_usage_events() -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("fault-confirmation"),
            response_model_key: response_model_key(),
        },
        ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 9,
            output_tokens: 3,
            total_tokens: Some(12),
            ..ModelUsage::default()
        }),
        ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: ModelText::new("durable partial and final").expect("text"),
        },
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        },
    ]
}

fn reasoning_only_max_output_events() -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("reasoning-only-max-output"),
            response_model_key: response_model_key(),
        },
        ModelStreamEvent::ReasoningDelta {
            output_index: 0,
            delta: ModelText::new("private reasoning without assistant text").expect("reasoning"),
        },
        ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 10,
            output_tokens: 4,
            reasoning_tokens: 4,
            total_tokens: Some(14),
            ..ModelUsage::default()
        }),
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::MaxOutputTokens,
        },
    ]
}

fn tool_completion_events(response_model_key: &ModelKey) -> Vec<ModelStreamEvent> {
    let call_id = ToolCallId::new("recover-exact-tool").expect("tool call id");
    vec![
        ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("tool-fault-confirmation"),
            response_model_key: response_model_key.clone(),
        },
        ModelStreamEvent::ReasoningDelta {
            output_index: 0,
            delta: ModelText::new("private durable reasoning").expect("reasoning"),
        },
        ModelStreamEvent::ToolCallStarted {
            output_index: 0,
            call_id: call_id.clone(),
            name: WireName::new("exact_recovery_tool").expect("wire name"),
            provider_item_ref: None,
            provider_replay: None,
        },
        ModelStreamEvent::ToolCallArgumentsDelta {
            output_index: 0,
            call_id: call_id.clone(),
            delta: "{\"value\":7}".into(),
        },
        ModelStreamEvent::ToolCallCompleted {
            output_index: 0,
            call_id,
            arguments: JsonValue::Object(BTreeMap::from([("value".into(), JsonValue::Integer(7))])),
        },
        ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 8,
            output_tokens: 2,
            total_tokens: Some(10),
            ..ModelUsage::default()
        }),
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        },
    ]
}

fn provider_ref(value: &str) -> ProviderRequestRef {
    ProviderRequestRef::from_opaque(OpaqueExternalRef::new(value).expect("provider ref"))
}

fn response_model_key() -> ModelKey {
    ModelKey::new("response-model").expect("response model key")
}

async fn build_loop(
    pool: &sqlx::PgPool,
    fixture: Fixture,
    model: Arc<dyn ModelProvider>,
    mode: FaultMode,
) -> (Arc<DurableAgentLoop>, Arc<FaultJournal>) {
    build_loop_with_tools(pool, fixture, model, mode, Arc::new(NoTools)).await
}

async fn build_loop_with_tools(
    pool: &sqlx::PgPool,
    fixture: Fixture,
    model: Arc<dyn ModelProvider>,
    mode: FaultMode,
    tools: Arc<dyn ToolExecutor>,
) -> (Arc<DurableAgentLoop>, Arc<FaultJournal>) {
    build_loop_with_storage(
        pool,
        fixture,
        model,
        mode,
        tools,
        Arc::new(MemoryBlobs::in_memory()),
    )
    .await
}

async fn build_loop_with_storage(
    pool: &sqlx::PgPool,
    fixture: Fixture,
    model: Arc<dyn ModelProvider>,
    mode: FaultMode,
    tools: Arc<dyn ToolExecutor>,
    blobs: Arc<dyn BlobStore>,
) -> (Arc<DurableAgentLoop>, Arc<FaultJournal>) {
    build_loop_with_owners(
        pool,
        fixture,
        model,
        mode,
        tools,
        blobs,
        running_loop::recovery_keyring(),
    )
    .await
}

async fn build_loop_with_owners(
    pool: &sqlx::PgPool,
    fixture: Fixture,
    model: Arc<dyn ModelProvider>,
    mode: FaultMode,
    tools: Arc<dyn ToolExecutor>,
    blobs: Arc<dyn BlobStore>,
    recovery_keys: Arc<ProtectedContentKeyring>,
) -> (Arc<DurableAgentLoop>, Arc<FaultJournal>) {
    build_loop_with_exposed_tools(
        pool,
        fixture,
        model,
        mode,
        tools,
        blobs,
        recovery_keys,
        Vec::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn build_loop_with_exposed_tools(
    pool: &sqlx::PgPool,
    fixture: Fixture,
    model: Arc<dyn ModelProvider>,
    mode: FaultMode,
    tools: Arc<dyn ToolExecutor>,
    blobs: Arc<dyn BlobStore>,
    recovery_keys: Arc<ProtectedContentKeyring>,
    exposed_tools: Vec<CanonicalToolDefinition>,
) -> (Arc<DurableAgentLoop>, Arc<FaultJournal>) {
    let now = database_now(pool).await;
    let keys = keyring();
    let content: Arc<dyn ContentStore> =
        Arc::new(PostgresContentStore::new(pool.clone(), blobs, keys.clone()));
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request")
                .expect("media type"),
            bytes: b"provider completion recovery".to_vec(),
        })
        .await
        .expect("store request");
    let route = support::model_route();
    let snapshot =
        runtime_fixture::install_snapshot(pool, content.clone(), fixture, route, &exposed_tools)
            .await;
    let inner = runtime_fixture::journal(pool, content.clone(), recovery_keys);
    let supervision = runtime_fixture::supervision(&inner);
    let inner: Arc<dyn AgentLoopJournal> = Arc::new(inner);
    let journal = Arc::new(FaultJournal::new(inner, mode));
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixedExecutionPolicy { now }),
        Arc::new(PlanningContext {
            fixture,
            content,
            route: musteragent_runtime_snapshot::PinnedModelRoute {
                route: snapshot.model.route,
                credential_grant_ref: snapshot.model.credential_grant_ref,
            },
            request_ref,
            exposed_tools,
        }),
        tools,
        Arc::new(EmptyControls),
    ));
    (
        Arc::new(
            DurableAgentLoop::new(
                Arc::new(FixedClock(now)),
                policy,
                Arc::new(runtime_fixture::OwnedScriptedModel(model)),
                journal.clone(),
            )
            .with_execution_supervision(supervision),
        ),
        journal,
    )
}

#[tokio::test]
async fn no_usage_empty_response_retries_without_fabricated_usage_before_final_output() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(EmptyThenFinalProvider::new());
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
    let (runtime, journal) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;
    assert!(matches!(
        runtime
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Failed {
            code: ErrorCode::ModelResponseInvalid,
            retryable: true,
            failure_commit_error: None,
            receipt: Some(_),
            ..
        }
    ));

    let midway: (i32, String, i64, i64, i64) = sqlx::query_as(
        "SELECT checkpoint.consecutive_empty_responses,
                checkpoint.total_tokens::text,
                (SELECT count(*) FROM runtime.model_invocations
                  WHERE run_id = checkpoint.run_id AND status = 3),
                (SELECT count(*) FROM runtime.model_usage_facts
                  WHERE run_id = checkpoint.run_id),
                (SELECT count(*) FROM event_store.outbox
                  WHERE topic = 'musteragent.model-usage.v1')
         FROM runtime.loop_checkpoints AS checkpoint
         WHERE checkpoint.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read empty response checkpoint");
    assert_eq!(midway, (0, "0".into(), 1, 0, 0));

    let dispatch =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let worker = WorkerId::new("empty-response-recovery").expect("worker");
    assert!(matches!(
        dispatch
            .claim_one(&worker, Duration::from_secs(120))
            .await
            .expect("claim during backoff"),
        ClaimOutcome::Idle { .. }
    ));
    sqlx::query("UPDATE runtime.runs SET not_before_ms=0 WHERE run_id=$1")
        .bind(fixture.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("advance test clock");
    let ClaimOutcome::Claimed(receipt) = dispatch
        .claim_one(&worker, Duration::from_secs(120))
        .await
        .expect("claim after backoff")
    else {
        panic!("due retry");
    };
    let resumed = AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id,
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    };
    assert_eq!(resumed.run_id, attempt.run_id);
    assert_ne!(resumed.attempt_id, attempt.attempt_id);
    let (runtime, reopened_journal) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        Arc::new(NoTools),
        blobs,
    )
    .await;
    let recovered_checkpoint = reopened_journal
        .inner
        .load_checkpoint(&resumed)
        .await
        .expect("reloaded checkpoint");
    let recovered = reopened_journal
        .inner
        .recover_checkpoint(
            &resumed,
            recovered_checkpoint.clone(),
            ModelRecoveryConstraints {
                recovery_policy: RunExecutionPolicySnapshot::production_defaults().recovery_policy,
            },
        )
        .await
        .expect("recover committed retry");
    assert_eq!(recovered.checkpoint, recovered_checkpoint);

    provider.release_second.notify_one();
    let exit = runtime
        .execute(
            resumed.clone(),
            attempt_telemetry(fixture, &resumed),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(
        matches!(exit, AgentLoopExit::Completed { .. }),
        "{exit:?}, checkpoint={recovered_checkpoint:?}, calls={}",
        provider.calls.load(Ordering::SeqCst)
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(journal.fail_calls.load(Ordering::SeqCst), 0);
    let terminal: (i64, i64, i64, i64, i16, String) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM runtime.attempts WHERE run_id = $1),
             (SELECT count(*) FROM runtime.model_invocations
                WHERE run_id = $1 AND status = 2 AND usage_fact IS NULL
                  AND response_model_key = 'response-model'),
             (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
             (SELECT count(*) FROM conversation.messages
                WHERE run_id = $1 AND role = 2 AND lifecycle = 1),
             run.status, checkpoint.total_tokens::text
         FROM runtime.runs AS run
         JOIN runtime.loop_checkpoints AS checkpoint USING (run_id)
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read no-Usage terminal");
    assert_eq!(terminal, (2, 1, 0, 1, 5, "0".into()));
    pool.close().await;
}

#[tokio::test]
async fn append_and_completion_ambiguity_retry_the_exact_operation_without_redispatch() {
    for mode in [
        FaultMode::AppendBeforeCommit,
        FaultMode::AppendAfterCommit,
        FaultMode::StageBeforeCommit,
        FaultMode::StageAfterCommit,
        FaultMode::FinishBeforeCommit,
        FaultMode::FinishAfterCommit,
    ] {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("start PostgreSQL");
        let pool = postgres.pool().await.expect("connect PostgreSQL");
        migrate(&pool).await.expect("migrate");
        let (fixture, attempt) = seed_running_loop(&pool).await;
        let provider = Arc::new(SingleCompletionProvider::new());
        let (runtime, journal) = build_loop(&pool, fixture, provider.clone(), mode).await;

        assert!(
            matches!(
                runtime
                    .execute(
                        attempt.clone(),
                        attempt_telemetry(fixture, &attempt),
                        InvocationSupervisor::new(),
                    )
                    .await,
                AgentLoopExit::Completed { .. }
            ),
            "{mode:?}"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1, "{mode:?}");
        assert_eq!(journal.fail_calls.load(Ordering::SeqCst), 0, "{mode:?}");
        let durable: (i64, i64, i64, i64, bool, i16, String) = sqlx::query_as(
            "SELECT
                 (SELECT count(*) FROM runtime.model_event_batches
                    WHERE invocation_id = invocation.invocation_id),
                 (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
                 (SELECT count(*) FROM event_store.outbox
                    WHERE topic = 'musteragent.model-usage.v1'),
                 (SELECT count(*) FROM conversation.messages
                    WHERE run_id = $1 AND role = 2 AND lifecycle = 1),
                 invocation.completion_intent_content_id IS NOT NULL,
                 invocation.status, checkpoint.total_tokens::text
             FROM runtime.model_invocations AS invocation
             JOIN runtime.loop_checkpoints AS checkpoint USING (run_id)
             WHERE invocation.run_id = $1",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read durable completion");
        assert_eq!(durable, (2, 1, 1, 1, false, 2, "12".into()), "{mode:?}");
        let erased: (i16, i16, bool, bool) = sqlx::query_as(
            "SELECT intent.lifecycle, content.state,
                    content.storage_ref IS NULL,
                    content.storage_ref_protected IS NULL
             FROM runtime.model_completion_intents AS intent
             JOIN artifact.content_refs AS content USING (content_id)
             WHERE intent.run_id = $1",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read completion-intent erasure lifecycle");
        assert_eq!(erased, (5, 3, true, true), "{mode:?}");

        {
            let appends = journal.append_attempts.lock().expect("append attempts");
            let targeted = appends
                .iter()
                .filter(|batch| batch.batch_index == 1)
                .collect::<Vec<_>>();
            if matches!(
                mode,
                FaultMode::AppendBeforeCommit | FaultMode::AppendAfterCommit
            ) {
                assert_eq!(targeted.len(), 2, "{mode:?}");
                assert_eq!(targeted[0], targeted[1], "{mode:?}");
            }
        }
        {
            let stages = journal.stage_attempts.lock().expect("stage attempts");
            if matches!(
                mode,
                FaultMode::StageBeforeCommit | FaultMode::StageAfterCommit
            ) {
                assert_eq!(stages.len(), 2, "{mode:?}");
                assert_eq!(stages[0], stages[1], "{mode:?}");
            }
        }
        {
            let finishes = journal.finish_attempts.lock().expect("finish attempts");
            if matches!(
                mode,
                FaultMode::FinishBeforeCommit | FaultMode::FinishAfterCommit
            ) {
                assert_eq!(finishes.len(), 2, "{mode:?}");
                assert_eq!(finishes[0], finishes[1], "{mode:?}");
            }
        }
        pool.close().await;
    }
}

#[tokio::test]
async fn replacement_recovers_durable_terminal_without_a_second_provider_dispatch() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(SingleCompletionProvider::new());
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
    let (first_runtime, first_journal) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::FinishUnavailable,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;

    assert_eq!(
        first_runtime
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(first_journal.fail_calls.load(Ordering::SeqCst), 0);
    let interrupted: (i16, Option<i16>, bool, bool, String, i64) = sqlx::query_as(
        "SELECT invocation.status, invocation.terminal_stop_reason,
                invocation.usage_fact IS NOT NULL,
                invocation.completion_intent_content_id IS NOT NULL,
                checkpoint.total_tokens::text,
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1)
         FROM runtime.model_invocations AS invocation
         JOIN runtime.loop_checkpoints AS checkpoint USING (run_id)
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read durable unacknowledged terminal");
    assert_eq!(interrupted, (1, Some(1), true, true, "0".into(), 0));

    let replacement_attempt = claim_replacement_attempt(&pool, &attempt).await;
    assert_eq!(replacement_attempt.attempt_number.get(), 2);
    let producer_attempt_id: uuid::Uuid = sqlx::query_scalar(
        "SELECT attempt_id FROM runtime.model_completion_intents WHERE run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read intent producer Attempt");
    assert_eq!(producer_attempt_id, attempt.attempt_id.into_uuid());
    assert_ne!(
        producer_attempt_id,
        replacement_attempt.attempt_id.into_uuid()
    );

    let (replacement, replacement_journal) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;
    assert!(matches!(
        replacement
            .execute(
                replacement_attempt.clone(),
                attempt_telemetry(fixture, &replacement_attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(replacement_journal.fail_calls.load(Ordering::SeqCst), 0);
    let recovered: (i16, i16, String, i64, i64) = sqlx::query_as(
        "SELECT invocation.status, run.status, checkpoint.total_tokens::text,
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
                (SELECT count(*) FROM event_store.outbox
                  WHERE topic = 'musteragent.model-usage.v1')
         FROM runtime.model_invocations AS invocation
         JOIN runtime.runs AS run USING (run_id)
         JOIN runtime.loop_checkpoints AS checkpoint USING (run_id)
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read recovered terminal");
    assert_eq!(recovered, (2, 5, "12".into(), 1, 1));
    pool.close().await;
}

#[tokio::test]
async fn reasoning_only_max_output_commits_privately_then_continues() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(ReasoningThenFinalProvider::new());
    let (runtime, journal) = build_loop(&pool, fixture, provider.clone(), FaultMode::None).await;

    assert!(matches!(
        runtime
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(journal.fail_calls.load(Ordering::SeqCst), 0);
    let facts: (i64, i64, i64, i16) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM runtime.model_invocations WHERE run_id = $1 AND status = 2),
             (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
             (SELECT count(*) FROM conversation.messages
                WHERE run_id = $1 AND role = 2 AND lifecycle = 1),
             status
         FROM runtime.runs WHERE run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read reasoning continuation result");
    assert_eq!(
        facts,
        (2, 1, 1, 5),
        "private reasoning must persist usage without creating an empty public message",
    );
    pool.close().await;
}

async fn claim_replacement_attempt(
    pool: &sqlx::PgPool,
    stale_attempt: &AttemptContext,
) -> AttemptContext {
    let expired_at = database_now(pool).await.unix_millis().saturating_sub(1);
    sqlx::query("UPDATE runtime.leases SET expires_at_ms = $2 WHERE run_id = $1")
        .bind(stale_attempt.run_id.into_uuid())
        .bind(expired_at)
        .execute(pool)
        .await
        .expect("expire stale Run lease");
    sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms = $2 WHERE attempt_id = $1")
        .bind(stale_attempt.attempt_id.into_uuid())
        .bind(expired_at)
        .execute(pool)
        .await
        .expect("expire stale Attempt lease");

    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let claimed = scheduler
        .claim_one(
            &WorkerId::new("completion-intent-recovery-worker").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim replacement Attempt");
    let ClaimOutcome::Claimed(claimed) = claimed else {
        panic!("expired completion-intent Attempt must be recovered");
    };
    AttemptContext {
        attempt_id: claimed.fence.attempt_id,
        run_id: claimed.fence.run_id,
        attempt_number: claimed.fence.attempt_number,
        worker_id: claimed.fence.worker_id,
        lease_id: claimed.fence.lease_id,
        fencing_token: claimed.fence.fencing_token,
        lease_expires_at: claimed.lease_expires_at,
    }
}

#[tokio::test]
async fn replacement_recovers_exact_tool_completion_intent_before_the_next_turn() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(ToolThenFinalProvider::new());
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
    let (first_runtime, _) = build_loop_with_exposed_tools(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::FinishUnavailable,
        Arc::new(NoTools),
        blobs.clone(),
        running_loop::recovery_keyring(),
        vec![exposed_tool_definition("exact_recovery_tool")],
    )
    .await;

    assert_eq!(
        first_runtime
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let staged: (i16, Option<i16>, bool, i64) = sqlx::query_as(
        "SELECT status, terminal_stop_reason,
                completion_intent_content_id IS NOT NULL,
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1)
         FROM runtime.model_invocations WHERE run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read staged Tool completion");
    assert_eq!(staged, (1, Some(2), true, 0));

    let tools = runtime_fixture::recording_tools(&pool, blobs.clone()).await;
    let (replacement, _) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        tools.clone(),
        blobs,
    )
    .await;
    let exit = replacement
        .execute(
            attempt.clone(),
            attempt_telemetry(fixture, &attempt),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    {
        let recovered_calls = tools.calls.lock().expect("recovered tool calls");
        assert_eq!(recovered_calls.len(), 1);
        assert_eq!(recovered_calls[0].call_id.as_str(), "recover-exact-tool");
        assert_eq!(
            recovered_calls[0].arguments,
            JsonValue::Object(BTreeMap::from([("value".into(), JsonValue::Integer(7),)]))
        );
    }
    let terminal: (i16, i64, i64, String) = sqlx::query_as(
        "SELECT run.status,
                (SELECT count(*) FROM runtime.model_invocations
                  WHERE run_id = $1 AND status = 2),
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
                checkpoint.total_tokens::text
         FROM runtime.runs AS run
         JOIN runtime.loop_checkpoints AS checkpoint USING (run_id)
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read recovered Tool terminal");
    assert_eq!(terminal, (5, 2, 1, "10".into()));
    pool.close().await;
}

#[tokio::test]
async fn recovered_private_reasoning_enters_the_next_production_context_request() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let provider = Arc::new(ToolThenFinalProvider::with_response_model_key(
        ModelKey::new("gpt-context-test").expect("production snapshot model key"),
    ));
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
    let keys = keyring();
    let content: Arc<dyn ContentStore> =
        Arc::new(PostgresContentStore::new(pool.clone(), blobs, keys.clone()));
    let (fixture, attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"recover a private-reasoning Tool turn",
    )
    .await;
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let mut snapshot = context_snapshot::snapshot_content_with_read_tool(
        fixture.run_id,
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()
            .expect("policy content ref"),
        "exact_recovery_tool",
    );
    snapshot.model.route.provider_state_policy = musteragent_model::ProviderStatePolicy::Stateless;
    snapshot = runtime_fixture::upgrade_snapshot(content.clone(), fixture, snapshot).await;
    let planner = production_context::install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        snapshot,
        b"recover a private-reasoning Tool turn",
    )
    .await;
    let now = database_now(&pool).await;
    let journal_inner =
        runtime_fixture::journal(&pool, content.clone(), running_loop::recovery_keyring());
    let supervision = runtime_fixture::supervision(&journal_inner);
    let journal_inner: Arc<dyn AgentLoopJournal> = Arc::new(journal_inner);
    let first_journal = Arc::new(FaultJournal::new(
        journal_inner,
        FaultMode::FinishUnavailable,
    ));
    let first_policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixedExecutionPolicy { now }),
        planner.clone(),
        Arc::new(NoTools),
        Arc::new(EmptyControls),
    ));
    let first = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        first_policy,
        Arc::new(runtime_fixture::OwnedScriptedModel(provider.clone())),
        first_journal,
    )
    .with_execution_supervision(supervision.clone());
    assert_eq!(
        first
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    let tool_journal: Arc<dyn FencedToolInvocationJournal> = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            codec.clone(),
            codec.clone(),
            keys.clone(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            runtime_fixture::clock(),
        ),
    );
    let tools: Arc<dyn ToolExecutor> = Arc::new(
        DurableToolExecutor::new(
            production_tool_material_source(&pool, codec.clone()),
            tool_journal,
            content.clone(),
            Arc::new(SuccessfulCapability {
                content: content.clone(),
            }),
            Arc::new(FixedClock(now)),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(supervision.clone()),
    );
    let replacement_journal: Arc<dyn AgentLoopJournal> = Arc::new(runtime_fixture::journal(
        &pool,
        content.clone(),
        running_loop::recovery_keyring(),
    ));
    let replacement_policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixedExecutionPolicy { now }),
        planner,
        tools,
        Arc::new(EmptyControls),
    ));
    let replacement = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        replacement_policy,
        Arc::new(runtime_fixture::OwnedScriptedModel(provider.clone())),
        replacement_journal,
    )
    .with_execution_supervision(supervision.clone());
    let exit = replacement
        .execute(
            attempt.clone(),
            attempt_telemetry(fixture, &attempt),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let assistant = requests[1]
        .input
        .iter()
        .find(|message| message.role == CanonicalRole::Assistant && !message.tool_calls.is_empty())
        .expect("recovered Tool call in next canonical Provider request");
    assert_eq!(
        assistant.tool_calls[0].call_id.as_str(),
        "recover-exact-tool"
    );
    let reasoning = assistant
        .reasoning
        .as_ref()
        .expect("private reasoning replay in next canonical Provider request");
    let reasoning_bytes = content
        .read(fixture.tenant_id, &reasoning.content_ref)
        .await
        .expect("read projected private reasoning");
    assert_eq!(reasoning_bytes, b"private durable reasoning");
    assert!(requests[1].input.iter().any(|message| {
        message.role == CanonicalRole::Tool
            && message
                .tool_call_id
                .as_ref()
                .is_some_and(|id| id.as_str() == "recover-exact-tool")
    }));
    pool.close().await;
}

#[tokio::test]
async fn large_tool_and_max_reasoning_intent_is_protected_recovered_and_erased_without_usage() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(LargeToolThenFinalProvider::new());
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
    let (first_runtime, _) = build_loop_with_exposed_tools(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::FinishUnavailable,
        Arc::new(NoTools),
        blobs.clone(),
        running_loop::recovery_keyring(),
        vec![exposed_tool_definition("large_recovery_tool")],
    )
    .await;

    assert_eq!(
        first_runtime
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let staged: (
        uuid::Uuid,
        String,
        String,
        Vec<u8>,
        String,
        String,
        i16,
        bool,
    ) = sqlx::query_as(
        "SELECT intent.content_id, intent.media_type,
                    intent.content_bytes::text, intent.content_hash,
                    intent.plaintext_bytes::text, intent.key_version,
                    intent.lifecycle,
                    invocation.completion_intent_protected IS NULL
             FROM runtime.model_completion_intents AS intent
             JOIN runtime.model_invocations AS invocation USING (invocation_id)
             WHERE intent.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read large protected intent");
    let ciphertext_bytes = staged.2.parse::<u64>().expect("ciphertext bytes");
    let plaintext_bytes = staged.4.parse::<u64>().expect("plaintext bytes");
    assert!(plaintext_bytes > 9 * 1_000_000);
    assert!(ciphertext_bytes > plaintext_bytes);
    assert_eq!(staged.5, "runtime.loop.test");
    assert_eq!((staged.6, staged.7), (3, true));
    let reference = ContentRef {
        content_id: ContentId::from_uuid(staged.0).expect("content ID"),
        media_type: MediaType::new(staged.1).expect("media type"),
        byte_length: ciphertext_bytes,
        sha256: Sha256::from_bytes(staged.3).expect("content hash"),
    };
    let keys = keyring();
    let encrypted_store = PostgresContentStore::new(pool.clone(), blobs.clone(), keys);
    let protected_bytes = encrypted_store
        .read(fixture.tenant_id, &reference)
        .await
        .expect("read encrypted intent");
    assert!(
        !protected_bytes
            .windows("large_recovery_tool".len())
            .any(|window| window == b"large_recovery_tool")
    );

    let tools = runtime_fixture::recording_tools(&pool, blobs.clone()).await;
    let (replacement, _) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        tools.clone(),
        blobs,
    )
    .await;
    let exit = replacement
        .execute(
            attempt.clone(),
            attempt_telemetry(fixture, &attempt),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    {
        let calls = tools.calls.lock().expect("recovered large tool call");
        assert_eq!(calls.len(), 1);
        let JsonValue::Object(arguments) = &calls[0].arguments else {
            panic!("recovered arguments must remain an object");
        };
        let Some(JsonValue::String(payload)) = arguments.get("payload") else {
            panic!("recovered payload must remain a string");
        };
        assert_eq!(payload.len(), 1_048_576 - 1_024);
    }
    let durable: (i64, i64, i64, i16, i16, bool, bool) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM runtime.model_invocations
                WHERE run_id = $1 AND status = 2 AND usage_fact IS NULL),
             (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
             (SELECT count(*) FROM event_store.outbox
                WHERE topic = 'musteragent.model-usage.v1'),
             intent.lifecycle, content.state,
             content.storage_ref IS NULL,
             content.storage_ref_protected IS NULL
         FROM runtime.model_completion_intents AS intent
         JOIN artifact.content_refs AS content USING (content_id)
         WHERE intent.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read large intent terminal state");
    assert_eq!(durable, (2, 0, 0, 5, 3, true, true));
    pool.close().await;
}

#[path = "postgres_provider_completion_recovery/crypto_and_staging.rs"]
mod crypto_and_staging;

#[tokio::test]
async fn completion_intent_key_rotation_requires_the_referenced_generation() {
    crypto_and_staging::run_completion_intent_key_rotation_requires_the_referenced_generation()
        .await;
}

#[tokio::test]
async fn intent_tamper_and_tenant_or_invocation_transplant_fail_closed() {
    crypto_and_staging::run_intent_tamper_and_tenant_or_invocation_transplant_fail_closed().await;
}

#[tokio::test]
async fn successful_materialization_unlinks_intent_before_retryable_crypto_erasure() {
    crypto_and_staging::run_successful_materialization_unlinks_intent_before_retryable_crypto_erasure()
        .await;
}

#[tokio::test]
async fn unavailable_blob_staging_is_reconciled_without_orphan_or_redispatch() {
    crypto_and_staging::run_unavailable_blob_staging_is_reconciled_without_orphan_or_redispatch()
        .await;
}

#[tokio::test]
async fn missing_exact_terminal_intent_fails_closed_without_provider_redispatch() {
    crypto_and_staging::run_missing_exact_terminal_intent_fails_closed_without_provider_redispatch(
    )
    .await;
}
