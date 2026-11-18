use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use musteragent_identity::InvocationId;
use musteragent_model::ModelKey;
use musteragent_run::InvocationContext;
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    ModelInvocationRouteRole, ModelProvider, ModelRouteRef, ModelStreamEvent, ModelStreamState,
    ModelText, ModelUsage, ProviderError, ProviderErrorCode, ProviderRequestRef, ReasoningPolicy,
    StopReason,
};

use crate::{ContextCompactionRequest, ContextPlanError};

mod activity;
mod supervision;
pub use activity::CompactionExecutionObservation;

const COMPACTION_SYSTEM_INSTRUCTION: &str = "You are a context summarization assistant. Read the supplied canonical conversation history and produce only a structured checkpoint summary. Separate independent user requests. Preserve the original terms of each one and only outcomes actually established for that request. Do not continue the conversation, answer its questions, invoke tools, or invent completion claims. Omit internal routing and audit metadata (agent IDs, dialogue IDs, ledger IDs, receipt IDs and integrity hashes). Preserve a reference only when the user asked for it or an offered tool needs that exact handle to continue unfinished work.";
const INITIAL_SUMMARY_INSTRUCTION: &str = "Create a structured context checkpoint for another model to continue the work. Preserve exact file paths, function names, user-specified terms, commands, actionable errors, constraints, unfinished work and relevant tool outcomes.";
const ROLLING_SUMMARY_INSTRUCTION: &str = "Update the prior structured checkpoint with the new canonical history. Preserve still-valid prior facts, add new progress and decisions, move completed work to Done, remove resolved blockers, and refresh Next Steps.";
const REQUIRED_SECTIONS: [&str; 9] = [
    "## Goal",
    "## Constraints & Preferences",
    "## Progress",
    "### Done",
    "### In Progress",
    "### Blocked",
    "## Key Decisions",
    "## Next Steps",
    "## Critical Context",
];
pub(crate) const MAX_SUMMARY_BYTES: usize = 512 * 1024;

/// Provider-facing half of Context compaction.
///
/// The planner owns source selection and budgets, while a persistence adapter
/// owns exactly-once dispatch. This type owns the single protocol-neutral
/// request and stream contract shared by Cloud and Device compositions.
#[derive(Clone)]
pub struct ContextCompactionModelInvoker {
    provider: Arc<dyn ModelProvider>,
}

impl ContextCompactionModelInvoker {
    #[must_use]
    pub fn new(provider: Arc<dyn ModelProvider>) -> Self {
        Self { provider }
    }

    pub async fn invoke(
        &self,
        request: &ContextCompactionRequest,
        invocation: InvocationContext,
        inactivity_timeout: Duration,
    ) -> Result<ContextCompactionModelOutput, ContextCompactionModelFailure> {
        validate_request(request).map_err(ContextCompactionModelFailure::without_usage)?;
        if inactivity_timeout.is_zero() {
            return Err(ContextCompactionModelFailure::without_usage(
                ContextPlanError::timeout(),
            ));
        }
        let canonical = canonical_request(request, invocation)
            .map_err(ContextCompactionModelFailure::without_usage)?;
        consume_stream(
            self.provider.as_ref(),
            canonical,
            inactivity_timeout,
            output_policy(request),
        )
        .await
    }

    /// Resolves credentials, immutable content, endpoint and wire mapping
    /// before persistence crosses the Provider dispatch boundary.
    pub async fn preflight(
        &self,
        request: &ContextCompactionRequest,
        invocation: InvocationContext,
    ) -> Result<(), ContextPlanError> {
        let canonical = canonical_request(request, invocation)?;
        self.provider
            .preflight(&canonical)
            .await
            .map_err(|error| provider_failure(&error, false))
    }

    pub fn discard_preflight(&self, invocation_id: InvocationId) {
        self.provider.discard_preflight(invocation_id);
    }

    /// Builds the exact canonical request consumed by [`Self::invoke`].
    ///
    /// Persistence adapters use this before dispatch to content-address and
    /// seal the provider request. Keeping construction here prevents Cloud and
    /// Device from drifting into different prompts or provider semantics.
    pub fn canonical_request(
        &self,
        request: &ContextCompactionRequest,
        invocation: InvocationContext,
    ) -> Result<CanonicalModelRequest, ContextPlanError> {
        canonical_request(request, invocation)
    }

    /// Revalidates a recovered summary using the same contract applied to a
    /// live provider response.
    pub fn validate_summary(value: String) -> Result<ModelText, ContextPlanError> {
        validate_summary(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextCompactionModelOutput {
    pub invocation_id: InvocationId,
    pub model_route_ref: ModelRouteRef,
    pub checkpoint: crate::CompactionCheckpoint,
    pub usage: ModelUsage,
    pub provider_request_ref: ProviderRequestRef,
    pub response_model_key: ModelKey,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextCompactionModelFailure {
    pub error: ContextPlanError,
    pub usage: Option<ModelUsage>,
}

#[derive(Default)]
struct CompactionOutputPolicy {
    continuable: bool,
    prefix: Option<String>,
}
fn output_policy(request: &ContextCompactionRequest) -> CompactionOutputPolicy {
    CompactionOutputPolicy {
        continuable: musteragent_runtime_snapshot::uses_compaction_output_continuation(
            request.slot.compaction_policy_ref.as_str(),
        ),
        prefix: request
            .continuation
            .as_ref()
            .map(|c| c.prefix.as_str().to_owned()),
    }
}

impl ContextCompactionModelFailure {
    fn without_usage(error: ContextPlanError) -> Self {
        Self { error, usage: None }
    }
}

fn validate_request(request: &ContextCompactionRequest) -> Result<(), ContextPlanError> {
    request.validate()
}

pub(crate) fn canonical_request(
    request: &ContextCompactionRequest,
    invocation: InvocationContext,
) -> Result<CanonicalModelRequest, ContextPlanError> {
    let document: serde_json::Value = serde_json::from_slice(&request.serialized_history)
        .map_err(|_| ContextPlanError::invariant())?;
    let mut input = crate::compaction_history::input(&document)?;
    let mut prompt = String::from(if request.previous_summary.is_some() {
        ROLLING_SUMMARY_INSTRUCTION
    } else {
        INITIAL_SUMMARY_INSTRUCTION
    });
    if let Some(previous) = &request.previous_summary {
        let prefix = crate::compaction_history::data_message(
            CanonicalRole::User,
            &format!("Previous historical summary:\n{}", previous.as_str()),
        )?;
        if let Some(first) = input.first_mut().filter(|m| m.role == CanonicalRole::User) {
            first.content.splice(0..0, prefix.content);
        } else {
            input.insert(0, prefix);
        }
    }
    prompt.push_str(
        "\n\nUse exactly these headings in order:\n## Goal\n## Constraints & Preferences\n## Progress\n### Done\n### In Progress\n### Blocked\n## Key Decisions\n## Next Steps\n## Critical Context",
    );
    if musteragent_runtime_snapshot::uses_model_output_capacity(
        request.slot.compaction_policy_ref.as_str(),
    ) {
        prompt.push_str(&format!(
            "\n\nAim for a concise checkpoint of approximately {} tokens or fewer. This is a sizing target, not a truncation boundary. Preserve essential constraints, exact references and unfinished work; finish the checkpoint completely. Do not pad the answer to the model output allowance.",
            request.slot.summary_budget,
        ));
    }
    if request.continuation.is_none() {
        input.push(crate::compaction_history::data_message(
            CanonicalRole::User,
            "End of the historical transcript. Write the cumulative checkpoint now using the required section headings. Preserve useful facts from any earlier checkpoint and the subsequent turns.",
        )?);
    }
    let mut canonical = CanonicalModelRequest {
        invocation,
        route: request.model.route.clone(),
        route_role: ModelInvocationRouteRole::Auxiliary(
            musteragent_model::AuxiliaryModelRole::Compression,
        ),
        credential_grant_ref: request.model.credential_grant_ref.clone(),
        continuation_ref: None,
        runtime_events: Vec::new(),
        instructions: vec![
            ModelText::new(format!("{COMPACTION_SYSTEM_INSTRUCTION}\n{prompt}"))
                .map_err(|_| ContextPlanError::invariant())?,
        ],
        input,
        tools: Vec::new(),
        response_schema: None,
        reasoning_policy: ReasoningPolicy::Disabled,
        cache_policy: CachePolicy::Disabled,
        stream_budget: request.stream_budget,
        max_output_tokens: request.max_output_tokens,
        temperature: None,
    };
    if let Some(continuation) = &request.continuation {
        canonical.invocation.parent_invocation_id = Some(continuation.parent_invocation_id);
        for (role, text) in [
            (CanonicalRole::Assistant, continuation.prefix.as_str()),
            (
                CanonicalRole::User,
                "The checkpoint above was cut off by the single-response output limit. Continue from its exact final character. Output only the remaining text; do not repeat or restart the checkpoint. Complete all remaining required sections. Do not answer the source conversation or invoke tools.",
            ),
        ] {
            canonical.input.push(CanonicalMessage {
                role,
                content: ModelText::chunked(text)
                    .map_err(|_| ContextPlanError::invariant())?
                    .into_iter()
                    .map(CanonicalContentPart::Text)
                    .collect(),
                tool_calls: Vec::new(),
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            });
        }
    }
    canonical
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    Ok(canonical)
}

async fn consume_stream(
    provider: &dyn ModelProvider,
    request: CanonicalModelRequest,
    inactivity_timeout: Duration,
    output: CompactionOutputPolicy,
) -> Result<ContextCompactionModelOutput, ContextCompactionModelFailure> {
    let invocation_id = request.invocation.invocation_id;
    let model_route_ref = ModelRouteRef::from_key(request.route.route_ref.clone());
    consume_events(
        invocation_id,
        model_route_ref,
        provider.stream(request),
        Some(inactivity_timeout),
        None,
        &mut CompactionExecutionObservation::default(),
        output,
    )
    .await
}

async fn consume_events(
    invocation_id: InvocationId,
    model_route_ref: ModelRouteRef,
    mut stream: musteragent_runtime_model::ModelEventStream<'_>,
    inactivity_timeout: Option<Duration>,
    activity: Option<&dyn musteragent_execution_liveness::ExecutionActivitySink>,
    observed: &mut CompactionExecutionObservation,
    output: CompactionOutputPolicy,
) -> Result<ContextCompactionModelOutput, ContextCompactionModelFailure> {
    let mut validator = ModelStreamState::default();
    let mut text = String::new();
    let mut produced_output = false;
    let mut usage = None;
    let mut provider_request_ref = None;
    let mut response_model_key = None;
    let mut stop_reason = None;
    loop {
        let next = match inactivity_timeout {
            None => stream.next().await,
            Some(inactivity_timeout) => tokio::time::timeout(inactivity_timeout, stream.next())
                .await
                .map_err(|_| {
                    // The request crossed the dispatch boundary. An idle stream
                    // cannot prove whether the Provider accepted or completed it,
                    // so persisted recovery must never redispatch this paid call.
                    ContextCompactionModelFailure {
                        error: ContextPlanError::indeterminate(),
                        usage,
                    }
                })?,
        };
        let Some(next) = next else {
            break;
        };
        let event = next.map_err(|error| ContextCompactionModelFailure {
            error: stream_provider_failure(&error, produced_output),
            usage,
        })?;
        produced_output |= event.is_public_output()
            || matches!(
                &event,
                ModelStreamEvent::ReasoningDelta { .. }
                    | ModelStreamEvent::ReasoningSummaryDelta { .. }
                    | ModelStreamEvent::ProviderReasoningReplay { .. }
            );
        let progress =
            validator
                .accept_with_progress(&event)
                .map_err(|_| ContextCompactionModelFailure {
                    error: ContextPlanError::invariant(),
                    usage,
                })?;
        observed
            .record(&event, progress, activity)
            .map_err(|_| ContextCompactionModelFailure {
                error: ContextPlanError::unavailable(),
                usage: observed.usage(),
            })?;
        match event {
            ModelStreamEvent::ResponseStarted {
                response_ref,
                response_model_key: model_key,
            } => {
                provider_request_ref = Some(response_ref);
                response_model_key = Some(model_key);
            }
            ModelStreamEvent::TextDelta { delta, .. } => {
                if text.len().saturating_add(delta.as_str().len()) > MAX_SUMMARY_BYTES {
                    return Err(ContextCompactionModelFailure {
                        error: ContextPlanError::quota(),
                        usage,
                    });
                }
                text.push_str(delta.as_str());
            }
            ModelStreamEvent::Usage(value) => usage = Some(value),
            ModelStreamEvent::ResponseCompleted { stop_reason: value } => {
                stop_reason = Some(value);
            }
            ModelStreamEvent::ResponseError(error) => {
                return Err(ContextCompactionModelFailure {
                    error: stream_provider_failure(&error, produced_output),
                    usage,
                });
            }
            ModelStreamEvent::ReasoningDelta { .. }
            | ModelStreamEvent::ReasoningSummaryDelta { .. }
            | ModelStreamEvent::ProviderReasoningReplay { .. } => {}
            ModelStreamEvent::ToolCallStarted { .. }
            | ModelStreamEvent::ToolCallArgumentsDelta { .. }
            | ModelStreamEvent::ToolCallCompleted { .. }
            | ModelStreamEvent::ToolCallUnexposed { .. }
            | ModelStreamEvent::ToolCallRejected { .. }
            | ModelStreamEvent::ArtifactContentFrame { .. }
            | ModelStreamEvent::NativeCompactionItem { .. } => {
                return Err(ContextCompactionModelFailure {
                    error: ContextPlanError::invariant(),
                    usage,
                });
            }
        }
        if validator.is_terminal() {
            // The Provider's canonical end is authoritative. A connection
            // remaining open cannot delay the original result commit.
            break;
        }
    }
    if !validator.is_terminal() || stop_reason.is_none() {
        return Err(ContextCompactionModelFailure {
            // An interrupted stream after dispatch cannot prove whether the
            // Provider completed the request. Persisted adapters must recover
            // this as indeterminate and must never issue the same paid call a
            // second time.
            error: ContextPlanError::indeterminate(),
            usage,
        });
    }
    let token_limited = stop_reason.is_some_and(StopReason::is_token_limited);
    if stop_reason != Some(StopReason::Stop) && !(output.continuable && token_limited) {
        // Only a complete provider checkpoint can replace its entire source
        // prefix. Headings (including our format-drift wrapper) cannot prove
        // that a token-limited answer retained the unfinished work at its tail.
        // These are observed terminal outcomes, not ambiguous transport loss;
        // retain billable usage and never retry the same truncated generation.
        return Err(ContextCompactionModelFailure {
            error: ContextPlanError {
                early_provider_cause: None,
                code: if stop_reason == Some(StopReason::ContextWindowExceeded) {
                    // The original chunk compactor can reduce this exact input.
                    musteragent_runtime_contract::ErrorCode::ModelContextExceeded
                } else {
                    musteragent_runtime_contract::ErrorCode::ModelResponseInvalid
                },
                retryable: false,
                retry_after_ms: None,
            },
            usage,
        });
    }
    // A continuation must contribute actual text. A second opaque reasoning
    // response or repeated prefix is not progress and cannot authorize a loop.
    if output.continuable
        && (text.trim().is_empty()
            || output
                .prefix
                .as_ref()
                .is_some_and(|prefix| text.starts_with(prefix)))
    {
        return Err(ContextCompactionModelFailure {
            error: ContextPlanError {
                early_provider_cause: None,
                code: musteragent_runtime_contract::ErrorCode::ModelResponseInvalid,
                retryable: false,
                retry_after_ms: None,
            },
            usage,
        });
    }
    let combined = match output.prefix {
        Some(prefix) => prefix + &text,
        None => text,
    };
    let checkpoint = if token_limited {
        crate::compaction_continuation::validate_partial(combined)
            .map(crate::CompactionCheckpoint::Incomplete)
    } else {
        validate_summary(combined).map(crate::CompactionCheckpoint::Complete)
    }
    .map_err(|error| ContextCompactionModelFailure { error, usage })?;
    Ok(ContextCompactionModelOutput {
        invocation_id,
        model_route_ref,
        checkpoint,
        usage: usage.ok_or_else(|| {
            ContextCompactionModelFailure::without_usage(ContextPlanError::unavailable())
        })?,
        provider_request_ref: provider_request_ref.ok_or_else(|| {
            ContextCompactionModelFailure {
                error: ContextPlanError::unavailable(),
                usage,
            }
        })?,
        response_model_key: response_model_key.ok_or_else(|| ContextCompactionModelFailure {
            error: ContextPlanError::unavailable(),
            usage,
        })?,
    })
}

fn validate_summary(value: String) -> Result<ModelText, ContextPlanError> {
    let value = value.trim();
    if value.is_empty() || value.len() > MAX_SUMMARY_BYTES {
        return Err(ContextPlanError::invariant());
    }
    if !has_required_sections(value) {
        return Err(ContextPlanError {
            early_provider_cause: None,
            code: musteragent_runtime_contract::ErrorCode::ModelResponseInvalid,
            retryable: false,
            retry_after_ms: None,
        });
    }
    ModelText::new(value).map_err(|_| ContextPlanError::quota())
}

fn has_required_sections(value: &str) -> bool {
    let mut lines = value.lines().map(str::trim);
    REQUIRED_SECTIONS
        .into_iter()
        .all(|heading| lines.any(|line| line == heading))
}

fn stream_provider_failure(error: &ProviderError, produced_output: bool) -> ContextPlanError {
    let mut failure = provider_failure(error, true);
    if produced_output {
        failure.early_provider_cause = None;
    }
    failure
}

fn provider_failure(error: &ProviderError, dispatched: bool) -> ContextPlanError {
    // Before dispatch, an unavailable credential store or transport cannot
    // have produced a paid call. After dispatch, loss of the transport cannot
    // prove the provider outcome; preserve that physical invocation boundary.
    if dispatched
        && matches!(
            error.safe_code,
            ProviderErrorCode::TransportFailed | ProviderErrorCode::DeadlineExceeded
        )
    {
        return ContextPlanError::indeterminate();
    }
    ContextPlanError {
        early_provider_cause: error
            .safe_code
            .permits_alternate_provider()
            .then_some(error.safe_code),
        retry_after_ms: error.retry_after_ms,
        code: error.safe_code.runtime_code(),
        retryable: error.permits_dependency_recovery(),
    }
}

#[cfg(test)]
#[path = "model_compaction/continuation_tests.rs"]
mod continuation_tests;

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::{StreamExt as _, stream};
    use musteragent_activity::ActivityKind;
    use musteragent_content_ref::Sha256;
    use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
    use musteragent_identity::{
        ActivityId, AttemptId, AttemptNumber, ConversationId, ConversationSeq, CorrelationId,
        DeviceId, FencingToken, HomeEpoch, InvocationId, LeaseId, LedgerRecordId, ParticipantId,
        RunId, RuntimeSnapshotId, TenantId, UtcTimestamp, VersionedKey,
    };
    use musteragent_model::{
        ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute,
        PricingVersionRef, ProviderStatePolicy,
    };
    use musteragent_run::{AttemptContext, InvocationContext, RunContext, WorkerId};
    use musteragent_runtime_contract::{ErrorCode, OpaqueExternalRef};
    use musteragent_runtime_model::{
        CachePolicy, CanonicalContentPart, CanonicalModelRequest, ModelEventStream, ModelProvider,
        ModelStreamEvent, ModelText, ModelUsage, ProviderError, ProviderErrorCode,
        ProviderErrorKind, ProviderRequestRef, ReasoningPolicy, StopReason,
    };
    use musteragent_runtime_snapshot::{ModelStreamBudgetSnapshot, PinnedModelRoute};

    use crate::{ContextCompactionRequest, ContextCompactionSlotV1};

    use super::ContextCompactionModelInvoker;

    pub(super) const SUMMARY: &str = "## Goal\nContinue the exact task.\n\n## Constraints & Preferences\n- Preserve durable evidence.\n\n## Progress\n### Done\n- Earlier history captured.\n\n### In Progress\n- Continue safely.\n\n### Blocked\n- None.\n\n## Key Decisions\n- Use canonical context.\n\n## Next Steps\n1. Resume.\n\n## Critical Context\n- Tool chronology remains authoritative.";

    #[derive(Copy, Clone)]
    enum ProviderMode {
        Success,
        DelayedSuccess,
        ToolCall,
        TransportFailure,
        InterruptedStream,
        Terminal(StopReason, bool),
    }

    struct FixtureProvider(ProviderMode);

    impl ModelProvider for FixtureProvider {
        fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
            assert!(request.tools.is_empty());
            assert_eq!(request.cache_policy, CachePolicy::Disabled);
            assert_eq!(request.reasoning_policy, ReasoningPolicy::Disabled);
            let started = ModelStreamEvent::ResponseStarted {
                response_ref: ProviderRequestRef::from_opaque(
                    OpaqueExternalRef::new("provider:context-test").expect("provider ref"),
                ),
                response_model_key: musteragent_model::ModelKey::new("context-test-model")
                    .expect("model key"),
            };
            if matches!(self.0, ProviderMode::DelayedSuccess) {
                let events = vec![
                    Ok(started),
                    Ok(ModelStreamEvent::TextDelta {
                        output_index: 0,
                        delta: ModelText::new(SUMMARY).expect("summary"),
                    }),
                    Ok(ModelStreamEvent::Usage(ModelUsage {
                        input_tokens_uncached: 100,
                        output_tokens: 50,
                        total_tokens: Some(150),
                        ..ModelUsage::default()
                    })),
                    Ok(ModelStreamEvent::ResponseCompleted {
                        stop_reason: StopReason::Stop,
                    }),
                ];
                return Box::pin(stream::iter(events).then(|event| async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    event
                }));
            }
            let events = match self.0 {
                ProviderMode::Success | ProviderMode::Terminal(_, _) => vec![
                    Ok(started),
                    Ok(ModelStreamEvent::TextDelta {
                        output_index: 0,
                        delta: ModelText::new(if matches!(self.0, ProviderMode::Terminal(_, false)) {
                            "## Goal\nPreserve all files.\n## Constraints & Preferences\n- Never overwrite"
                        } else { SUMMARY }).expect("summary"),
                    }),
                    Ok(ModelStreamEvent::Usage(ModelUsage {
                        input_tokens_uncached: 100,
                        output_tokens: 50,
                        total_tokens: Some(150),
                        ..ModelUsage::default()
                    })),
                    Ok(ModelStreamEvent::ResponseCompleted {
                        stop_reason: match self.0 {
                            ProviderMode::Terminal(reason, _) => reason,
                            _ => StopReason::Stop,
                        },
                    }),
                ],
                ProviderMode::ToolCall => vec![
                    Ok(started),
                    Ok(ModelStreamEvent::ToolCallStarted {
                        output_index: 0,
                        call_id: musteragent_identity::ToolCallId::new("forbidden-call")
                            .expect("call id"),
                        name: musteragent_tool::WireName::new("forbidden_tool")
                            .expect("wire name"),
                        provider_item_ref: None,
                        provider_replay: None,
                    }),
                ],
                ProviderMode::TransportFailure => vec![Err(ProviderError {
                    kind: ProviderErrorKind::Transport,
                    safe_code: ProviderErrorCode::TransportFailed,
                    retryable: true,
                    retry_after_ms: None,
                    diagnostic_ref: None,
                })],
                ProviderMode::InterruptedStream => vec![
                    Ok(started),
                    Ok(ModelStreamEvent::TextDelta {
                        output_index: 0,
                        delta: ModelText::new("## Goal\npartial").expect("partial text"),
                    }),
                ],
                ProviderMode::DelayedSuccess => unreachable!("handled above"),
            };
            Box::pin(stream::iter(events))
        }
    }

    #[tokio::test]
    async fn protocol_neutral_compaction_is_tool_free_and_structured() {
        let request = request();
        let output =
            ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(ProviderMode::Success)))
                .invoke(&request, request.invocation.clone(), Duration::from_secs(1))
                .await
                .expect("structured summary");
        assert_eq!(output.checkpoint.text().as_str(), SUMMARY);
        assert_eq!(output.usage.total_tokens, Some(150));
    }

    #[tokio::test]
    async fn truncated_or_refused_checkpoints_never_replace_complete_source_history() {
        let request = request();
        for reason in [
            StopReason::MaxOutputTokens,
            StopReason::TokenLimit,
            StopReason::ContentFilter,
            StopReason::Cancelled,
        ] {
            for complete_headings in [false, true] {
                let failure = ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(
                    ProviderMode::Terminal(reason, complete_headings),
                )))
                .invoke(&request, request.invocation.clone(), Duration::from_secs(1))
                .await
                .expect_err("an explicit incomplete terminal cannot authorize a checkpoint");
                assert_eq!(failure.error.code, ErrorCode::ModelResponseInvalid);
                assert!(!failure.error.retryable);
                assert!(!failure.error.is_indeterminate());
                assert_eq!(
                    failure.usage.and_then(|usage| usage.total_tokens),
                    Some(150)
                );
            }
        }
    }

    #[tokio::test]
    async fn explicit_compaction_context_exhaustion_enters_existing_chunk_recovery() {
        let request = request();
        let failure = ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(
            ProviderMode::Terminal(StopReason::ContextWindowExceeded, false),
        )))
        .invoke(&request, request.invocation.clone(), Duration::from_secs(1))
        .await
        .expect_err("known exhausted capacity is not an unknown dispatch");
        assert_eq!(failure.error.code, ErrorCode::ModelContextExceeded);
        assert!(!failure.error.retryable);
        assert!(!failure.error.is_indeterminate());
        assert_eq!(
            failure.usage.and_then(|usage| usage.total_tokens),
            Some(150)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn compaction_timeout_tracks_canonical_inactivity_not_total_runtime() {
        let request = request();
        // Virtual time proves idle resets independently of host scheduler load.
        let started = tokio::time::Instant::now();
        let output = ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(
            ProviderMode::DelayedSuccess,
        )))
        .invoke(
            &request,
            request.invocation.clone(),
            Duration::from_millis(30),
        )
        .await
        .expect("active compaction stream");

        assert_eq!(output.checkpoint.text().as_str(), SUMMARY);
        assert!(started.elapsed() > Duration::from_millis(60));
    }

    #[test]
    fn model_capacity_policy_keeps_soft_summary_target_out_of_api_ceiling() {
        let mut request = request();
        request.slot.compaction_policy_ref =
            VersionedKey::new(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V9).unwrap();
        request.model.route.context_window = NonZeroU64::new(1_000_000).unwrap();
        request.model.route.max_output_tokens = NonZeroU64::new(384_000).unwrap();
        request.slot.summary_budget = 12_218;
        request.max_output_tokens = request.model.route.max_output_tokens;
        request.slot.plan_fingerprint = request.expected_plan_fingerprint().unwrap();
        request.validate().unwrap();
        let invoker =
            ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(ProviderMode::Success)));
        let canonical = invoker
            .canonical_request(&request, request.invocation.clone())
            .unwrap();
        assert_eq!(canonical.max_output_tokens.get(), 384_000);
        let prompt = canonical.instructions[0].as_str();
        assert!(prompt.contains("approximately 12218 tokens"));
        assert!(prompt.contains("not a truncation boundary"));
    }

    #[test]
    fn an_answer_to_a_historical_request_cannot_replace_the_checkpoint() {
        let error =
            ContextCompactionModelInvoker::validate_summary("雨燕九羽".to_owned()).unwrap_err();
        assert_eq!(
            error.code,
            musteragent_runtime_contract::ErrorCode::ModelResponseInvalid
        );
    }

    #[test]
    fn oversized_compaction_history_is_one_logical_message_with_bounded_text_parts() {
        let mut request = request();
        let history = format!(
            r#"{{"schema_version":"musteragent.context.compaction-input.v4","messages":[{{"role":"user","text":["{}"]}}]}}"#,
            "large deterministic history ".repeat(48_000)
        );
        assert!(
            history.len() > 1_048_576,
            "fixture must cross one text-part bound"
        );
        request.serialized_history = history.as_bytes().to_vec();
        request.slot.plan_fingerprint = request
            .expected_plan_fingerprint()
            .expect("large history fingerprint");

        let canonical =
            ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(ProviderMode::Success)))
                .canonical_request(&request, request.invocation.clone())
                .expect("large compression history must remain representable");

        assert_eq!(
            canonical.input.len(),
            2,
            "one history message followed by the summarization assignment"
        );
        let message = &canonical.input[0];
        assert!(
            message.content.len() > 1,
            "large history must use bounded parts"
        );
        let reconstructed = message
            .content
            .iter()
            .map(|part| match part {
                CanonicalContentPart::Text(text) => text.as_str(),
                _ => panic!("compression history must remain text-only"),
            })
            .collect::<String>();
        assert_eq!(reconstructed, "large deterministic history ".repeat(48_000));
        assert!(
            message.content.iter().all(|part| match part {
                CanonicalContentPart::Text(text) => text.as_str().len() <= 1_048_576,
                _ => false,
            }),
            "every transport text part must remain bounded"
        );
    }

    #[test]
    fn empty_provider_checkpoint_still_fails_closed() {
        let error = ContextCompactionModelInvoker::validate_summary("  \n".to_owned())
            .expect_err("empty compaction output must remain invalid");
        assert_eq!(error.code, ErrorCode::InternalInvariantViolated);
    }

    #[tokio::test]
    async fn compaction_rejects_tools_and_marks_ambiguous_transport_indeterminate() {
        let request = request();
        let tool_error =
            ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(ProviderMode::ToolCall)))
                .invoke(&request, request.invocation.clone(), Duration::from_secs(1))
                .await
                .expect_err("compression must never call tools");
        assert_eq!(tool_error.error.code, ErrorCode::InternalInvariantViolated);

        let transport_error = ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(
            ProviderMode::TransportFailure,
        )))
        .invoke(&request, request.invocation.clone(), Duration::from_secs(1))
        .await
        .expect_err("ambiguous transport must not be retryable");
        assert!(transport_error.error.is_indeterminate());

        let interrupted = ContextCompactionModelInvoker::new(Arc::new(FixtureProvider(
            ProviderMode::InterruptedStream,
        )))
        .invoke(&request, request.invocation.clone(), Duration::from_secs(1))
        .await
        .expect_err("an unterminated response cannot be safely redispatched");
        assert!(interrupted.error.is_indeterminate());
    }

    #[test]
    fn complete_plan_fingerprint_rejects_history_tampering() {
        let mut request = request();
        request.serialized_history.extend_from_slice(b" ");
        let error = request
            .validate()
            .expect_err("history drift must conflict with sealed identity");
        assert_eq!(error.code, ErrorCode::IdempotencyConflict);
    }

    #[test]
    fn durable_compression_replaces_only_an_old_physical_owner_and_reuses_success() {
        use crate::{CompactionGenerationObservation, CompactionGenerationState};
        use musteragent_runtime_snapshot::RecoveryPolicySnapshot;
        let mut original = request();
        original.recovery_policy = RecoveryPolicySnapshot::production_default();
        let now = UtcTimestamp::from_unix_millis(1_000);
        let observation = CompactionGenerationObservation {
            invocation_id: original.invocation.invocation_id,
            correlation_id: original.invocation.correlation_id,
            generation: original.slot.generation,
            generation_high_watermark: original.slot.generation,
            plan_fingerprint: original.slot.plan_fingerprint,
            logical_plan_fingerprint: Some(
                original.logical_plan_fingerprint().expect("logical plan"),
            ),
            sealed_ledger_cutoff: Some(original.slot.ledger_cutoff),
            sealed_input_tokens_before: Some(original.slot.input_tokens_before),
            owner_attempt_id: AttemptId::generate(),
            owner_fencing_token: original.invocation.attempt_context.fencing_token,
            timing: original.invocation.timing.clone(),
            state: CompactionGenerationState::DispatchedUnknown,
        };
        let replacement = original
            .clone()
            .select_recovery_generation(Some(observation.clone()), now)
            .expect("replacement generation");
        assert_eq!(replacement.slot.generation.get(), 2);
        assert_ne!(
            replacement.invocation.invocation_id,
            original.invocation.invocation_id
        );
        assert_eq!(replacement.serialized_history, original.serialized_history);
        replacement
            .validate()
            .expect("sealed source plan remains valid");
        assert_eq!(
            replacement,
            original
                .clone()
                .select_recovery_generation(Some(observation.clone()), now)
                .expect("deterministic retry")
        );
        let mut older_node = observation.clone();
        older_node.generation_high_watermark =
            NonZeroU32::new(7).expect("valid compaction generation fixture");
        let recovered = original
            .clone()
            .select_recovery_generation(Some(older_node), now)
            .expect("valid compaction generation fixture");
        assert_eq!(
            recovered.slot.generation.get(),
            8,
            "never overwrite a newer chunk generation"
        );
        let mut live = observation.clone();
        live.owner_attempt_id = original.invocation.attempt_context.attempt_id;
        assert_eq!(
            original
                .clone()
                .select_recovery_generation(Some(live), now)
                .expect("same live owner")
                .slot
                .generation
                .get(),
            1
        );
        let mut succeeded = observation.clone();
        succeeded.state = CompactionGenerationState::Succeeded;
        assert_eq!(
            original
                .clone()
                .select_recovery_generation(Some(succeeded), now)
                .expect("reuse success")
                .slot
                .generation
                .get(),
            1
        );
        let mut bookkeeping_only = original.clone();
        bookkeeping_only.slot.ledger_cutoff =
            ConversationSeq::new(original.slot.ledger_cutoff.get() + 5).expect("later bookkeeping");
        bookkeeping_only.slot.plan_fingerprint = bookkeeping_only
            .expected_plan_fingerprint()
            .expect("current source fence");
        let mut cached = observation.clone();
        cached.state = CompactionGenerationState::Succeeded;
        let reused = bookkeeping_only
            .select_recovery_generation(Some(cached), now)
            .expect("reuse semantic success");
        assert_eq!(reused.slot.generation.get(), 1);
        assert_eq!(reused.slot.plan_fingerprint, original.slot.plan_fingerprint);
        let mut cooling = observation.clone();
        cooling.state = CompactionGenerationState::Failed(crate::ContextPlanError {
            early_provider_cause: None,
            code: ErrorCode::CredentialRefreshRequired,
            retryable: true,
            retry_after_ms: Some(120_000),
        });
        assert_eq!(
            original
                .clone()
                .select_recovery_generation(Some(cooling), now)
                .expect_err(
                    "keep original provider cooldown before generating another physical call"
                )
                .retry_after_ms,
            Some(120_000)
        );
        let mut rejected = observation.clone();
        rejected.state = CompactionGenerationState::Failed(crate::ContextPlanError {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::Unauthenticated,
            retryable: false,
        });
        assert_eq!(
            original
                .clone()
                .select_recovery_generation(Some(rejected), now)
                .expect_err("requires authority repair")
                .code,
            ErrorCode::Unauthenticated
        );
        let mut truncated = observation.clone();
        truncated.state = CompactionGenerationState::Failed(crate::ContextPlanError {
            early_provider_cause: None,
            retry_after_ms: None,
            code: ErrorCode::ModelResponseInvalid,
            retryable: false,
        });
        assert_eq!(
            original
                .clone()
                .select_recovery_generation(Some(truncated), now)
                .expect_err("new ownership cannot retry a known truncated checkpoint")
                .code,
            ErrorCode::ModelResponseInvalid,
        );
        original.recovery_policy = RecoveryPolicySnapshot::legacy_default();
        assert_eq!(
            original
                .clone()
                .select_recovery_generation(Some(observation), now)
                .expect("historical contract"),
            original
        );
    }

    pub(crate) fn request() -> ContextCompactionRequest {
        let tenant_id = TenantId::generate();
        let run_id = RunId::generate();
        let record_id = LedgerRecordId::generate();
        let device_id = DeviceId::generate();
        let run_context = RunContext {
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
            control_home: RuntimeHome::Device { device_id },
            execution_home: RuntimeHome::Device { device_id },
            home_epoch: HomeEpoch::new(1).expect("home epoch"),
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
        };
        let invocation = InvocationContext {
            invocation_id: InvocationId::generate(),
            run_context,
            attempt_context: AttemptContext {
                attempt_id: AttemptId::generate(),
                run_id,
                attempt_number: AttemptNumber::new(1).expect("attempt number"),
                worker_id: WorkerId::new("context-test-worker").expect("worker"),
                lease_id: LeaseId::generate(),
                fencing_token: FencingToken::new(1).expect("fence"),
                lease_expires_at: UtcTimestamp::from_unix_millis(10_000),
            },
            parent_invocation_id: None,
            causation_id: record_id,
            correlation_id: CorrelationId::generate(),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(10_000) },
        };
        let model = PinnedModelRoute {
            route: ModelRoute {
                route_ref: VersionedKey::new("device.context-compression@1").expect("route ref"),
                resolved_model_key: "context-test-model".to_owned(),
                model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::from_u128(
                    0x1111_1111_1111_4111_8111_1111_1111_1111,
                ))
                .expect("provider id"),
                model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::from_u128(
                    0x2222_2222_2222_4222_8222_2222_2222_2222,
                ))
                .expect("entry id"),
                api_format: ApiFormat::OpenAiResponses,
                adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
                protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
                context_window: NonZeroU64::new(128_000).expect("window"),
                max_output_tokens: NonZeroU64::new(4_096).expect("max output"),
                capabilities: BTreeSet::new(),
                parameters: BTreeMap::new(),
                fallback_route_refs: Vec::new(),
                pricing_version_ref: PricingVersionRef::new("catalog:test@1").expect("pricing"),
                provider_state_policy: ProviderStatePolicy::Stateless,
            },
            credential_grant_ref: CredentialGrantRef::new("grant_context_test").expect("grant"),
        };
        let mut request = ContextCompactionRequest {
            capture_provider_failure: false,
            continuation: None,
            recovery_policy:
                musteragent_runtime_snapshot::RecoveryPolicySnapshot::legacy_default(),
            slot: ContextCompactionSlotV1 {
                source: Default::default(),
                loop_iteration: 1,
                generation: NonZeroU32::new(1).expect("generation"),
                active_branch_id: None,
                ledger_cutoff: ConversationSeq::new(2).expect("cutoff"),
                source_seq_from: ConversationSeq::new(1).expect("source from"),
                source_seq_to: ConversationSeq::new(1).expect("source to"),
                retained_record_ids: vec![LedgerRecordId::generate()],
                previous_summary_ref: None,
                input_budget: 16_384,
                input_tokens_before: 8_192,
                summary_budget: 1_024,
                compaction_policy_ref: VersionedKey::new("musteragent.legacy.context-compaction@1")
                    .expect("policy"),
                plan_fingerprint: Sha256::digest(b"unsealed"),
            },
            invocation,
            model,
            stream_budget: ModelStreamBudgetSnapshot {
                max_event_bytes: NonZeroUsize::new(1_048_576).expect("event bytes"),
                max_events: NonZeroU32::new(1_000).expect("events"),
                max_total_bytes: NonZeroU64::new(16_777_216).expect("total bytes"),
                max_tool_calls: NonZeroU32::new(1).expect("tool calls"),
                max_tool_argument_bytes: NonZeroUsize::new(1_024).expect("argument bytes"),
                max_reasoning_replay_bytes: NonZeroUsize::new(1_024).expect("reasoning bytes"),
            },
            serialized_history: br#"{"schema_version":"musteragent.context.compaction-input.v4","messages":[{"role":"user","text":["continue"]}]}"#.to_vec(),
            previous_summary: None,
            summarized_record_ids: vec![record_id],
            max_output_tokens: NonZeroU64::new(1_024).expect("output budget"),
        };
        request.slot.plan_fingerprint = request
            .expected_plan_fingerprint()
            .expect("plan fingerprint");
        request
    }
}

#[cfg(test)]
mod failure_classification_tests {
    use super::*;
    use musteragent_runtime_contract::ErrorCode;
    use musteragent_runtime_model::ProviderErrorKind;

    #[test]
    fn preflight_transport_is_recoverable_but_dispatched_transport_is_indeterminate() {
        let error = ProviderError {
            kind: ProviderErrorKind::Transport,
            safe_code: ProviderErrorCode::TransportFailed,
            retryable: true,
            retry_after_ms: None,
            diagnostic_ref: None,
        };
        assert!(provider_failure(&error, false).retryable);
        assert!(provider_failure(&error, true).is_indeterminate());
    }

    #[test]
    fn auxiliary_model_failures_keep_exact_authentication_and_content_causes() {
        for (code, expected) in [
            (
                ProviderErrorCode::AuthenticationFailed,
                ErrorCode::Unauthenticated,
            ),
            (
                ProviderErrorCode::ContentNotFound,
                ErrorCode::ResourceFileUnavailable,
            ),
            (
                ProviderErrorCode::ContentResolutionFailed,
                ErrorCode::ContentIntegrityFailed,
            ),
        ] {
            let error = ProviderError {
                kind: ProviderErrorKind::Transport,
                safe_code: code,
                retryable: true,
                retry_after_ms: None,
                diagnostic_ref: None,
            };
            let mapped = provider_failure(&error, false);
            assert_eq!(mapped.code, expected);
            assert!(!mapped.retryable);
        }
    }
}
