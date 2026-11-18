use musteragent_runtime_contract::SafeDisplayText;
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, InvocationExecutionTiming,
};
use std::sync::Arc;
use std::time::Instant;

use self::rejection::{
    dispatch_rejection_outcome, failed_outcome, safe_text, undispatched_binding_outcome,
};
mod activity;
mod failure_feedback;
mod physical;
mod preparation;
mod recovery;
use preparation::{PreparedTool, invocation_timing};
mod file_input;
mod result_images;
use crate::dispatcher::{
    ApprovalPromptCapabilityDetail, ApprovalPromptEnvelope, ApprovalPromptRequestIdentity,
    ApprovalPromptTaskProvenanceRecord,
};
use crate::invocation_policy::{
    InvocationPolicyDisposition, InvocationPolicyEvaluator, InvocationPolicyInput,
    RuntimeInvocationPolicyEvaluator, combine_dispositions, consequence_fingerprint,
    invocation_policy_identity_for, session_approval_fingerprint,
};
use crate::schema_validation::{ToolSchemaValidatorCache, invalid_arguments_outcome};
use crate::{
    ApprovalAutoReviewDecision, ApprovalAutoReviewInput, CapabilityDispatcher,
    DispatchedCapabilityOutcome, review_approval,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    AuthorizationReceiptId, ContentId, IdempotencyKey, InteractionId, InvocationId,
    OutboxMessageId, ToolCallId, UtcTimestamp, WorkspaceId,
};
use musteragent_loop::{
    DurableToolCompletion, InvocationCompletionObserver, InvocationSpanFactory, LoopPortError,
    PlannedToolCall, ToolArgumentValidation, ToolExecutionOutcome, ToolExecutor,
};
use musteragent_policy::{ApprovalRequirement, Decision, RequiredContext};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    ApprovalRequest, AuthorizationReceipt, AuthorizationReceiptQuery, CapabilityInvocation,
    CapabilityOutcome, CapabilityResult, DurableToolInvocation, DurableToolObservation,
    FencedToolInvocationJournal, InteractionResolution, InvocationAuthorization, RuntimeClock,
    ToolExecutionMaterial, ToolExecutionMaterialSource, ToolExecutionPortError, ToolExecutionStart,
    ToolStartMode, ToolTerminalOutcome, active_pinned_model, authorize_invocation,
    pinned_model_supports_vision,
};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, CancelReason, ErrorCode, OpaqueExternalRef, TraceContext,
};
use musteragent_runtime_interaction::StructuredInteractionRequest;
use musteragent_runtime_interaction::approval_prompt::{
    APPROVAL_RESPONSE_SCHEMA as APPROVAL_SCHEMA, APPROVAL_RESPONSE_SCHEMA_KEY,
};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, JsonValue, ModelText,
};
use musteragent_telemetry::{
    AttemptTelemetryContext, ErrorClass, InvocationOperation, InvocationTelemetryContext,
    RuntimeSpan, TelemetryOutcome,
};
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolRetryContract, WireName};

const JSON_MEDIA_TYPE: &str = "application/json";
const MAX_INLINE_TOOL_RESULT_BYTES: usize = 1_000_000;
const APPROVAL_LIFETIME_MS: i64 = 24 * 60 * 60 * 1_000;
const SESSION_AUTHORIZATION_LIFETIME_MS: i64 = 24 * 60 * 60 * 1_000;

struct ApprovalRequestPolicy {
    policy_reason: Option<&'static str>,
    consequence_fingerprint: Sha256,
    session_reusable: bool,
}

struct ObservedToolTransition<'a> {
    telemetry: Option<&'a AttemptTelemetryContext>,
    call: &'a PlannedToolCall,
    material: &'a ToolExecutionMaterial,
    proposed: ToolInvocation,
    retry_contract: ToolRetryContract,
}

/// Supplies the active distributed trace rather than fabricating trace IDs in
/// domain code.
pub trait WorkerTraceContextSource: Send + Sync {
    fn trace_context<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        call_id: &'a ToolCallId,
    ) -> ApplicationFuture<'a, Result<TraceContext, LoopPortError>>;
}

#[derive(Debug, Default)]
/// Host policy hook that inherits the mandatory kernel invocation policy.
pub struct InheritInvocationPolicyEvaluator;

impl InvocationPolicyEvaluator for InheritInvocationPolicyEvaluator {
    fn evaluate(&self, _input: &InvocationPolicyInput<'_>) -> InvocationPolicyDisposition {
        InvocationPolicyDisposition::Inherit
    }
}

pub struct DurableToolExecutor {
    material: Arc<dyn ToolExecutionMaterialSource>,
    journal: Arc<dyn FencedToolInvocationJournal>,
    content: Arc<dyn ContentStore>,
    dispatcher: Arc<dyn CapabilityDispatcher>,
    application_readiness:
        Option<Arc<dyn musteragent_runtime_interaction::CapabilityRequestResolver>>,
    clock: Arc<dyn RuntimeClock>,
    traces: Arc<dyn WorkerTraceContextSource>,
    invocation_span: Arc<InvocationSpanFactory>,
    invocation_completion: Arc<InvocationCompletionObserver>,
    runtime_invocation_policy: RuntimeInvocationPolicyEvaluator,
    invocation_policy: Arc<dyn InvocationPolicyEvaluator>,
    schemas: ToolSchemaValidatorCache,
    execution_supervision: Option<musteragent_loop::ExecutionSupervision>,
    #[cfg(feature = "release-acceptance")]
    post_execution_barrier: Option<std::path::PathBuf>,
}

impl DurableToolExecutor {
    #[must_use]
    pub fn new(
        material: Arc<dyn ToolExecutionMaterialSource>,
        journal: Arc<dyn FencedToolInvocationJournal>,
        content: Arc<dyn ContentStore>,
        dispatcher: Arc<dyn CapabilityDispatcher>,
        clock: Arc<dyn RuntimeClock>,
        traces: Arc<dyn WorkerTraceContextSource>,
    ) -> Self {
        Self {
            material,
            journal,
            content,
            dispatcher,
            application_readiness: None,
            clock,
            traces,
            invocation_span: Arc::new(|_, _| RuntimeSpan::none()),
            invocation_completion: Arc::new(|_, _, _, _, _, _, _| {}),
            runtime_invocation_policy: RuntimeInvocationPolicyEvaluator,
            invocation_policy: Arc::new(InheritInvocationPolicyEvaluator),
            schemas: ToolSchemaValidatorCache::new(),
            execution_supervision: None,
            #[cfg(feature = "release-acceptance")]
            post_execution_barrier: std::env::var_os(
                "MUSTERAGENT_RELEASE_TOOL_OBSERVATION_BARRIER",
            )
            .map(std::path::PathBuf::from),
        }
    }

    fn bound_dispatcher(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Arc<dyn CapabilityDispatcher>, LoopPortError> {
        Ok(self
            .dispatcher
            .bind(material)?
            .unwrap_or_else(|| self.dispatcher.clone()))
    }

    #[must_use]
    pub fn with_invocation_span_factory(mut self, factory: Arc<InvocationSpanFactory>) -> Self {
        self.invocation_span = factory;
        self
    }

    #[must_use]
    pub fn with_invocation_completion_observer(
        mut self,
        observer: Arc<InvocationCompletionObserver>,
    ) -> Self {
        self.invocation_completion = observer;
        self
    }

    #[must_use]
    pub fn with_invocation_policy_evaluator(
        mut self,
        evaluator: Arc<dyn InvocationPolicyEvaluator>,
    ) -> Self {
        self.invocation_policy = evaluator;
        self
    }

    #[must_use]
    pub fn with_execution_supervision(
        mut self,
        supervision: musteragent_loop::ExecutionSupervision,
    ) -> Self {
        self.execution_supervision = Some(supervision);
        self
    }

    async fn load_requested(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
    ) -> Result<(ToolExecutionMaterial, ToolInvocation, DurableToolInvocation), LoopPortError> {
        // PlannedToolCall is already canonical. A validation failure here is
        // therefore an upstream/runtime invariant breach, not a repairable
        // Tool request rejection (the durable invocation does not exist yet).
        call.arguments
            .validate()
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        let material = tokio::time::timeout(
            musteragent_runtime_contract::execution_liveness::DISCOVERY_REQUEST_WINDOW,
            self.material.load(attempt, &call.name),
        )
        .await
        .map_err(|_| port(ErrorCode::DependencyTimeout, false))?
        .map_err(|error| port(error.code, error.retryable))?;
        if material.run_context.run_id != attempt.run_id {
            return Err(port(ErrorCode::InternalInvariantViolated, false));
        }
        if material.tool.manifest.execution_policy.liveness().is_none() {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        let request_value = execution_arguments(&call.arguments, &material);
        let request_bytes = serde_json::to_vec(&request_value)
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        let invocation_id = invocation_id(material.run_context.run_id, &call.call_id)?;
        let request_ref = self
            .content
            .put_at(
                request_content_id(invocation_id)?,
                PutContent {
                    tenant_id: material.run_context.tenant_id,
                    media_type: media_type()?,
                    bytes: request_bytes,
                },
            )
            .await
            .map_err(port_from_content_write)?;
        let timing = invocation_timing(&material.tool.manifest.execution_policy, self.clock.now())?;
        if !matches!(timing, InvocationExecutionTiming::ActivityV1(_)) {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        let proposed = ToolInvocation {
            invocation_id,
            run_id: material.run_context.run_id,
            requested_by_attempt_id: attempt.attempt_id,
            execution_attempt_id: None,
            tool_call_id: call.call_id.clone(),
            tool_key: material.tool.manifest.key.clone(),
            capability_provider_id: material.tool.manifest.capability_provider_id,
            status: ToolInvocationStatus::Requested,
            effect: material.tool.manifest.effect,
            approval: material.tool.manifest.approval,
            idempotency_key: invocation_idempotency_key(invocation_id)?,
            request_ref,
            result_ref: None,
            interaction_id: None,
            timing,
        };
        let durable = self
            .journal
            .load_or_begin(
                attempt,
                proposed.clone(),
                material.tool.manifest.retry_contract,
            )
            .await
            .map_err(port_from_tool)?;

        Ok((material, proposed, durable))
    }

    async fn validate_model_arguments_inner(
        &self,
        attempt: &AttemptContext,
        name: &WireName,
        arguments: &JsonValue,
    ) -> Result<ToolArgumentValidation, LoopPortError> {
        arguments
            .validate()
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        let material = tokio::time::timeout(
            musteragent_runtime_contract::execution_liveness::DISCOVERY_REQUEST_WINDOW,
            self.material.load(attempt, name),
        )
        .await
        .map_err(|_| port(ErrorCode::DependencyTimeout, false))?
        .map_err(|error| port(error.code, error.retryable))?;
        if material.run_context.run_id != attempt.run_id {
            return Err(port(ErrorCode::InternalInvariantViolated, false));
        }
        let value = json_to_serde(arguments);
        if let Err(validation) = self
            .schemas
            .validate_async(
                material.tool.manifest.input_schema.schema_hash,
                &material.tool.input_schema_json,
                &value,
            )
            .await
        {
            return Ok(ToolArgumentValidation::Invalid {
                // This legacy admission API has a frozen three-item metadata
                // contract. Execution returns the full immutable diagnostic_result.
                issues: validation
                    .argument_issues()?
                    .iter()
                    .take(musteragent_runtime_contract::MAX_TOOL_ARGUMENT_VALIDATION_ISSUES)
                    .cloned()
                    .collect(),
            });
        }

        // `interaction.request@1` shipped with a looser JSON Schema than its
        // semantic parser (for example confirm + options). Historical Runs
        // remain replayable, but their model-authored arguments must still be
        // rejected as a failed Tool invocation. @2 is checked through the
        // same single-source parser as a regression guard against future drift.
        let interaction_valid = interaction_arguments_valid(&material.tool.manifest.key, &value);
        Ok(if interaction_valid {
            ToolArgumentValidation::Valid
        } else {
            ToolArgumentValidation::Invalid { issues: Vec::new() }
        })
    }

    async fn observe_tool_transition<F>(
        &self,
        attempt: &AttemptContext,
        transition: ObservedToolTransition<'_>,
        future: F,
    ) -> Result<ToolExecutionOutcome, LoopPortError>
    where
        F: std::future::Future<Output = Result<ToolExecutionOutcome, LoopPortError>> + Send,
    {
        let ObservedToolTransition {
            telemetry,
            call,
            material,
            proposed,
            retry_contract,
        } = transition;
        let invocation_id = proposed.invocation_id;
        let operation = InvocationOperation::Tool;
        let context = telemetry.map(|telemetry| InvocationTelemetryContext {
            attempt: telemetry.clone(),
            invocation_id,
        });
        let span = context.as_ref().map_or_else(RuntimeSpan::none, |context| {
            (self.invocation_span)(operation, context)
        });
        let started_at = Instant::now();
        let (mut result, durable) = span
            .instrument(async {
                let result = future.await;
                self.wait_at_post_execution_barrier().await;
                let observation = self
                    .journal
                    .load_observation(attempt, proposed, retry_contract);
                let durable =
                    if let Some(policy) = material.tool.manifest.execution_policy.liveness() {
                        tokio::time::timeout(policy.windows().observation_request, observation)
                            .await
                            .unwrap_or(Err(ToolExecutionPortError {
                                code: ErrorCode::DependencyUnavailable,
                                retryable: true,
                            }))
                    } else {
                        Err(ToolExecutionPortError {
                            code: ErrorCode::RuntimeSnapshotInvalid,
                            retryable: false,
                        })
                    };
                (result, durable)
            })
            .await;

        // A QueryBeforeRetry authority may atomically commit the terminal Tool
        // result while the physical/reconciliation future is still returning
        // its pre-commit waiting observation. Normalize to the durable terminal
        // outcome before both telemetry and the Agent Loop observe the result.
        // This keeps one authoritative completion state and ensures a failure
        // to materialize that terminal result is reported as such rather than
        // being hidden behind a successful durable receipt.
        if matches!(
            &result,
            Ok(ToolExecutionOutcome::ReconciliationWaiting { .. })
        ) && let Ok(observation) = &durable
        {
            let durable = observation.durable().clone();
            if let Some(outcome) = durable.recorded_outcome.clone()
                && !matches!(outcome, ToolTerminalOutcome::Indeterminate { .. })
            {
                result = self
                    .message_for_outcome(call, material, &durable, &outcome)
                    .await;
            }
        }
        // A prepared prompt is not a terminal Tool observation. The Loop has
        // not requested an Interaction or suspended this Attempt yet.
        if matches!(&result, Ok(ToolExecutionOutcome::ApprovalRequired { .. })) {
            return result;
        }
        let telemetry_observation = durable_tool_telemetry_observation(&result, durable.as_ref());
        let transition_record_id = durable
            .as_ref()
            .ok()
            .map(|receipt| receipt.transition_record_id());
        if let Some(transition_record_id) = transition_record_id {
            span.record_observation_id(transition_record_id);
        }
        let telemetry_outcome = telemetry_observation.outcome;
        span.record_outcome(telemetry_outcome);
        if let Some(error) = telemetry_observation.error {
            span.record_error(ErrorClass::from_error_code(error));
        }
        if let Some(context) = context.as_ref() {
            span.in_scope(|| {
                (self.invocation_completion)(
                    operation,
                    context,
                    telemetry_outcome,
                    telemetry_observation.error,
                    telemetry_observation.materialization_error,
                    transition_record_id,
                    started_at.elapsed(),
                );
            });
        }
        result
    }

    #[cfg(feature = "release-acceptance")]
    async fn wait_at_post_execution_barrier(&self) {
        let Some(base) = &self.post_execution_barrier else {
            return;
        };
        let mut entered = base.as_os_str().to_owned();
        entered.push(".entered");
        let mut release = base.as_os_str().to_owned();
        release.push(".release");
        tokio::fs::write(
            std::path::PathBuf::from(entered),
            b"durable-transition-committed",
        )
        .await
        .expect("write release Tool observation barrier marker");
        let release = std::path::PathBuf::from(release);
        while tokio::fs::metadata(&release).await.is_err() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[cfg(not(feature = "release-acceptance"))]
    async fn wait_at_post_execution_barrier(&self) {}

    async fn seal_unknown_outcome(
        &self,
        attempt: &AttemptContext,
        durable: DurableToolInvocation,
        reason: SafeDisplayText,
    ) -> Result<DurableToolInvocation, LoopPortError> {
        self.seal_unknown_with_evidence(attempt, durable, reason, None, None)
            .await
    }

    async fn seal_unknown_with_evidence(
        &self,
        attempt: &AttemptContext,
        durable: DurableToolInvocation,
        reason: SafeDisplayText,
        observed_error: Option<musteragent_runtime_contract::ErrorInfo>,
        diagnostic_result: Option<CapabilityResult>,
    ) -> Result<DurableToolInvocation, LoopPortError> {
        let reconciliation_ref = durable
            .prebound_reconciliation_ref
            .clone()
            .map(Ok)
            .unwrap_or_else(|| operator_reconciliation_ref(durable.invocation.invocation_id))?;
        let outcome = ToolTerminalOutcome::Indeterminate {
            reason,
            reconciliation_ref,
            observed_error,
            diagnostic_result,
        };
        self.journal
            .complete(
                attempt,
                durable.invocation.invocation_id,
                outcome,
                self.clock.now(),
            )
            .await
            .map_err(port_from_tool)
    }

    fn execution_start(
        &self,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        mode: ToolStartMode,
    ) -> Result<ToolExecutionStart, LoopPortError> {
        let started_at = self.clock.now();
        let timing = invocation_timing(&material.tool.manifest.execution_policy, started_at)?;
        if timing != durable.invocation.timing || mode != ToolStartMode::Initial {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        Ok(ToolExecutionStart { started_at, timing })
    }

    fn reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        let identity = self
            .bound_dispatcher(material)?
            .reconciliation_identity(material, invocation)?;
        match durable.retry_contract {
            ToolRetryContract::QueryBeforeRetry | ToolRetryContract::NotReplayable => identity
                .map(Some)
                .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false)),
            ToolRetryContract::ReadOnly | ToolRetryContract::IdempotentByKey => Ok(None),
        }
    }

    fn verify_prebound_reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: &CapabilityInvocation,
    ) -> Result<(), LoopPortError> {
        let identity = self
            .bound_dispatcher(material)?
            .reconciliation_identity(material, invocation)?;
        match durable.retry_contract {
            ToolRetryContract::QueryBeforeRetry | ToolRetryContract::NotReplayable
                if identity == durable.prebound_reconciliation_ref =>
            {
                Ok(())
            }
            ToolRetryContract::ReadOnly | ToolRetryContract::IdempotentByKey
                if durable.prebound_reconciliation_ref.is_none() =>
            {
                Ok(())
            }
            _ => Err(port(ErrorCode::RuntimeSnapshotInvalid, false)),
        }
    }

    /// Reloads an outcome written by an atomic capability handler and adopts
    /// the journal's exact terminal variant. The handler return value is only
    /// a consistency proof; it must never be used to reconstruct the outcome,
    /// because the atomic commit may have attached authorized content.
    async fn reload_committed_success(
        &self,
        attempt: &AttemptContext,
        durable: &DurableToolInvocation,
        committed_result: &CapabilityResult,
    ) -> Result<(DurableToolInvocation, ToolTerminalOutcome), LoopPortError> {
        let proposed = observation_request(attempt, &durable.invocation);
        let persisted = self
            .journal
            .load_or_begin(attempt, proposed, durable.retry_contract)
            .await
            .map_err(port_from_tool)?;
        let outcome = persisted
            .recorded_outcome
            .clone()
            .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false))?;
        validate_committed_success(&outcome, committed_result)?;
        Ok((persisted, outcome))
    }

    async fn capability_invocation(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
    ) -> Result<CapabilityInvocation, LoopPortError> {
        Ok(CapabilityInvocation {
            actor: material.actor,
            tenant_id: material.run_context.tenant_id,
            conversation_id: material.run_context.conversation_id,
            run_id: material.run_context.run_id,
            requested_by_attempt_id: durable.invocation.requested_by_attempt_id,
            execution_attempt_id: attempt.attempt_id,
            execution_attempt_number: attempt.attempt_number,
            invocation_id: durable.invocation.invocation_id,
            tool_call_id: call.call_id.clone(),
            tool_key: durable.invocation.tool_key.clone(),
            capability_key: material.capability_grant.key.clone(),
            capability_provider_id: durable.invocation.capability_provider_id,
            idempotency_key: durable.invocation.idempotency_key.clone(),
            request_ref: durable.invocation.request_ref.clone(),
            request_schema: material.tool.manifest.input_schema.clone(),
            result_schema: material.tool.manifest.output_schema.clone(),
            authorized_content: material.authorized_content.clone(),
            workspace_id: workspace_id(&material.run_context.execution_scope),
            timing: durable.invocation.timing.clone(),
            trace_context: self.traces.trace_context(attempt, &call.call_id).await?,
            execution_grant_proof: material.execution_grant_proof.clone(),
        })
    }
}

fn text_content(text: String) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
    let text = escape_unsupported_model_controls(text);
    Ok(vec![CanonicalContentPart::Text(
        ModelText::new(text).map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
    )])
}

fn validate_committed_success(
    outcome: &ToolTerminalOutcome,
    committed_result: &CapabilityResult,
) -> Result<(), LoopPortError> {
    let recorded_result = match outcome {
        ToolTerminalOutcome::Succeeded(result)
        | ToolTerminalOutcome::SucceededWithAuthorizedContent { result, .. } => result,
        ToolTerminalOutcome::Failed {
            diagnostic_result: Some(result),
            ..
        } => result,
        ToolTerminalOutcome::Failed {
            diagnostic_result: None,
            ..
        }
        | ToolTerminalOutcome::Cancelled(_)
        | ToolTerminalOutcome::Indeterminate { .. } => {
            return Err(port(ErrorCode::InternalInvariantViolated, false));
        }
    };
    if recorded_result != committed_result {
        return Err(port(ErrorCode::ContentIntegrityFailed, false));
    }
    Ok(())
}

fn escape_unsupported_model_controls(text: String) -> String {
    if !text
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return text;
    }
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        if character.is_control() && !matches!(character, '\n' | '\r' | '\t') {
            use std::fmt::Write as _;
            write!(&mut escaped, "\\u{:04X}", u32::from(character))
                .expect("writing into a String cannot fail");
        } else {
            escaped.push(character);
        }
    }
    escaped
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct VisionToolOutput {
    #[serde(rename = "analysis")]
    _analysis: String,
    image_refs: Vec<VisionToolContentRef>,
    #[serde(rename = "model_key")]
    _model_key: String,
    mode: String,
    #[serde(rename = "batch")]
    _batch: VisionToolBatch,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct VisionToolBatch {
    #[serde(rename = "logical_image_count")]
    _logical_image_count: usize,
    #[serde(rename = "provider_invocation_count")]
    _provider_invocation_count: usize,
    #[serde(rename = "partitions")]
    _partitions: Vec<VisionToolPartition>,
    #[serde(rename = "degraded_joint_context")]
    _degraded_joint_context: bool,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct VisionToolPartition {
    #[serde(rename = "start")]
    _start: usize,
    #[serde(rename = "count")]
    _count: usize,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct VisionToolContentRef {
    content_id: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}

impl VisionToolContentRef {
    fn decode(self) -> Result<ContentRef, LoopPortError> {
        if self.sha256.len() != Sha256::LENGTH * 2
            || !self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(port(ErrorCode::ContentIntegrityFailed, false));
        }
        let mut hash = [0_u8; Sha256::LENGTH];
        for (index, byte) in hash.iter_mut().enumerate() {
            let offset = index * 2;
            *byte = u8::from_str_radix(&self.sha256[offset..offset + 2], 16)
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        }
        Ok(ContentRef {
            content_id: self
                .content_id
                .parse::<ContentId>()
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?,
            media_type: MediaType::new(self.media_type)
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?,
            byte_length: self.byte_length,
            sha256: Sha256::from_bytes(hash)
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?,
        })
    }
}

fn unavailable_context_reason(
    required: &std::collections::BTreeSet<RequiredContext>,
    available: &std::collections::BTreeSet<RequiredContext>,
) -> Option<&'static str> {
    if required.is_subset(available) {
        return None;
    }
    Some(
        if required.contains(&RequiredContext::Workspace)
            && !available.contains(&RequiredContext::Workspace)
        {
            "workspace_context_required: Tool requires an active authorized workspace"
        } else {
            "required_context_unavailable: Tool requires unavailable conversation context"
        },
    )
}

// The schema is checked first; keep the stricter versioned Interaction parser
// shared with execution instead of maintaining a second payload language here.
fn interaction_arguments_valid(key: &musteragent_tool::ToolKey, value: &serde_json::Value) -> bool {
    if !matches!(
        key.logical_key(),
        "interaction.request" | "interaction.question" | "capability.request"
    ) {
        return true;
    }
    serde_json::to_vec(value).ok().is_some_and(|bytes| {
        if key.as_str() == "capability.request@1" {
            musteragent_runtime_interaction::CapabilityRequest::parse(&bytes).is_ok()
        } else if key.as_str() == "capability.request@2" {
            musteragent_runtime_interaction::ModelCapabilityRequest::parse(&bytes).is_ok()
        } else {
            StructuredInteractionRequest::parse_tool_arguments(key.as_str(), &bytes).is_ok()
        }
    })
}

#[cfg(test)]
mod tests;

impl ToolExecutor for DurableToolExecutor {
    fn execute_activity_with_telemetry<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        telemetry: &'a AttemptTelemetryContext,
        supervisor: &'a musteragent_loop::InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, musteragent_loop::InvocationAborted>,
    > {
        Box::pin(self.execute_activity(attempt, Some(telemetry), supervisor, call))
    }

    fn request_approval(
        &self,
        attempt: &AttemptContext,
        request: ApprovalRequest,
    ) -> ApplicationFuture<'_, Result<InteractionId, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            let interaction_id = request.interaction_id;
            let waiting = self
                .journal
                .request_approval(&attempt, request)
                .await
                .map_err(port_from_tool)?;
            if waiting.invocation.status != ToolInvocationStatus::WaitingInteraction
                || waiting.invocation.interaction_id != Some(interaction_id)
            {
                return Err(port(ErrorCode::InternalInvariantViolated, false));
            }
            Ok(interaction_id)
        })
    }

    fn validate_arguments<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        name: &'a WireName,
        arguments: &'a JsonValue,
    ) -> ApplicationFuture<'a, Result<ToolArgumentValidation, LoopPortError>> {
        Box::pin(async move {
            self.validate_model_arguments_inner(attempt, name, arguments)
                .await
        })
    }

    fn execute(
        &self,
        attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            let supervisor = musteragent_loop::InvocationSupervisor::new();
            self.execute_activity(&attempt, None, &supervisor, call)
                .await
                .map_err(|_| port(ErrorCode::DependencyUnavailable, false))?
        })
    }

    fn execute_with_telemetry(
        &self,
        attempt: &AttemptContext,
        telemetry: &AttemptTelemetryContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let attempt = attempt.clone();
        let telemetry = telemetry.clone();
        Box::pin(async move {
            let supervisor = musteragent_loop::InvocationSupervisor::new();
            self.execute_activity(&attempt, Some(&telemetry), &supervisor, call)
                .await
                .map_err(|_| port(ErrorCode::DependencyUnavailable, false))?
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct ToolTelemetryObservation {
    outcome: TelemetryOutcome,
    error: Option<ErrorCode>,
    materialization_error: Option<ErrorCode>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum DurableToolReceipt {
    Succeeded,
    Failed { code: ErrorCode },
    Cancelled,
    Waiting,
    Indeterminate,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ToolObservationReceipt {
    Durable {
        receipt: DurableToolReceipt,
        materialization_error: Option<ErrorCode>,
    },
    ExecutionError(ErrorCode),
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PhysicalToolError {
    source: LoopPortError,
}

impl From<LoopPortError> for PhysicalToolError {
    fn from(source: LoopPortError) -> Self {
        Self { source }
    }
}

mod approval;
mod observation;
mod readiness;
mod rejection;
mod support;
mod telemetry;
use support::*;
pub use support::{interaction_id, metering_outbox_id, nested_invocation_id};
use telemetry::*;

mod dispatch_authorization;
pub use dispatch_authorization::ToolDispatchAuthorizer;
pub use support::{
    invocation_id as tool_invocation_id,
    invocation_idempotency_key as tool_invocation_idempotency_key,
    request_content_id as tool_request_content_id,
};
