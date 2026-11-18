mod rejection_drain;

use std::collections::BTreeMap;

use musteragent_identity::{InvocationId, OutputId, ToolCallId};
use musteragent_model::ProviderStatePolicy;
use musteragent_runtime_contract::{ErrorCode, OpaqueExternalRef};
use musteragent_runtime_model::{
    ContinuationRef, JsonValue, ModelReplayText, ModelStreamEvent, ModelStreamState, ModelUsage,
    ProviderError, ProviderReasoningReplayItem, ProviderRequestRef, StopReason,
};
use musteragent_runtime_snapshot::ModelStreamBudgetSnapshot;

use crate::output_coalescer::{CoalescerUpdate, ModelEventCoalescer};
use crate::state::{ModelIterationOutcome, PlannedToolCallInternal};
use crate::{LoopPortError, ModelInvocationObservation};

/// Transactional in-memory staging for one canonical Provider stream.
///
/// Every event is applied to a clone. Counters, validator state, Tool
/// accumulation, identity/usage and the public coalescer become authoritative
/// together only after all local checks succeed.
#[derive(Clone)]
pub(crate) struct CanonicalConsumptionState {
    pub(crate) coalescer: ModelEventCoalescer,
    validator: ModelStreamState,
    tools: BTreeMap<u32, PlannedToolCallInternal>,
    tool_outputs: BTreeMap<ToolCallId, u32>,
    tool_argument_fragments: BTreeMap<u32, String>,
    preview_outputs: BTreeMap<ToolCallId, OutputId>,
    stop_reason: Option<StopReason>,
    usage: Option<ModelUsage>,
    provider_response_ref: Option<ProviderRequestRef>,
    response_model_key: Option<musteragent_model::ModelKey>,
    assistant_text_emitted: bool,
    /// Bounded copy of this response's assistant text; `None` once it is too
    /// long to be a progress note.
    assistant_text: Option<String>,
    public_output_observed: bool,
    replay_barrier_observed: bool,
    private_reasoning: String,
    provider_reasoning_replay: Vec<ProviderReasoningReplayItem>,
    canonical_event_count: u32,
    canonical_event_bytes: u64,
    reasoning_replay_bytes: usize,
    rejected_tool_call_count: u32,
}

pub(crate) struct StagedCanonicalEvent {
    pub(crate) state: CanonicalConsumptionState,
    pub(crate) update: CoalescerUpdate,
    pub(crate) provider_failure: Option<ProviderError>,
    pub(crate) progress: musteragent_runtime_model::ModelStreamProgress,
}

impl CanonicalConsumptionState {
    pub(crate) fn new(max_output_bytes: usize, event_capacity: usize) -> Self {
        Self {
            coalescer: ModelEventCoalescer::new(max_output_bytes, event_capacity),
            validator: ModelStreamState::default(),
            tools: BTreeMap::new(),
            tool_outputs: BTreeMap::new(),
            tool_argument_fragments: BTreeMap::new(),
            preview_outputs: BTreeMap::new(),
            stop_reason: None,
            usage: None,
            provider_response_ref: None,
            response_model_key: None,
            assistant_text_emitted: false,
            assistant_text: Some(String::new()),
            public_output_observed: false,
            replay_barrier_observed: false,
            private_reasoning: String::new(),
            provider_reasoning_replay: Vec::new(),
            canonical_event_count: 0,
            canonical_event_bytes: 0,
            reasoning_replay_bytes: 0,
            rejected_tool_call_count: 0,
        }
    }

    pub(crate) fn stage_event(
        &self,
        invocation_id: InvocationId,
        event: ModelStreamEvent,
        budget: ModelStreamBudgetSnapshot,
        previewer: impl Fn(
            &ToolCallId,
            &musteragent_tool::WireName,
            &str,
        ) -> Option<crate::ToolDocumentPreview>,
    ) -> Result<StagedCanonicalEvent, LoopPortError> {
        let mut staged = self.clone();
        staged.account_event(&event, budget)?;
        let progress = staged
            .validator
            .accept_with_progress(&event)
            .map_err(|_| invariant())?;

        let mut preview_candidate = None;
        let mut completed_preview_call = None;
        match &event {
            ModelStreamEvent::ToolCallUnexposed { .. } => return Err(invariant()),
            ModelStreamEvent::ToolCallRejected {
                output_index,
                call_id,
                kind,
                diagnostic,
            } => {
                if staged.tool_outputs.get(call_id) != Some(output_index) {
                    return Err(invariant());
                }
                // The placeholder is replay material only. Persist the failure
                // with the planned call so it cannot execute after recovery.
                staged
                    .tools
                    .get_mut(output_index)
                    .ok_or_else(invariant)?
                    .planning_failure =
                    Some(crate::ToolCallPlanningFailure::from_argument_issue(*kind));
                if let Some(value) = diagnostic {
                    value.validate().map_err(|_| invariant())?;
                }
                staged
                    .tools
                    .get_mut(output_index)
                    .ok_or_else(invariant)?
                    .argument_diagnostic = diagnostic.clone();
                completed_preview_call = Some(call_id.clone());
            }
            ModelStreamEvent::ToolCallStarted {
                output_index,
                call_id,
                name,
                provider_replay,
                ..
            } => {
                if staged.tools.len()
                    >= usize::try_from(budget.max_tool_calls.get()).map_err(|_| quota())?
                    || staged.tools.contains_key(output_index)
                    || staged.tool_outputs.contains_key(call_id)
                {
                    return Err(
                        if staged.tools.len()
                            >= usize::try_from(budget.max_tool_calls.get()).map_err(|_| quota())?
                        {
                            quota()
                        } else {
                            invariant()
                        },
                    );
                }
                staged.tools.insert(
                    *output_index,
                    PlannedToolCallInternal {
                        argument_diagnostic: None,
                        planning_failure: None,
                        call_id: call_id.clone(),
                        name: name.clone(),
                        arguments: JsonValue::Object(BTreeMap::new()),
                        provider_replay: provider_replay.clone(),
                    },
                );
                staged.tool_outputs.insert(call_id.clone(), *output_index);
                staged
                    .tool_argument_fragments
                    .insert(*output_index, String::new());
            }
            ModelStreamEvent::ResponseStarted {
                response_ref,
                response_model_key,
            } => {
                staged.provider_response_ref = Some(response_ref.clone());
                staged.response_model_key = Some(response_model_key.clone());
            }
            ModelStreamEvent::ReasoningDelta { delta, .. } => {
                if staged
                    .private_reasoning
                    .len()
                    .saturating_add(delta.as_str().len())
                    > budget.max_reasoning_replay_bytes.get()
                {
                    return Err(quota());
                }
                staged.private_reasoning.push_str(delta.as_str());
            }
            ModelStreamEvent::TextDelta { delta, .. } => {
                staged.assistant_text_emitted |= !delta.as_str().trim().is_empty();
                if let Some(text) = &mut staged.assistant_text {
                    if text.len() + delta.as_str().len()
                        > musteragent_execution_context::MAX_PROGRESS_MESSAGE_BYTES
                    {
                        staged.assistant_text = None;
                    } else {
                        text.push_str(delta.as_str());
                    }
                }
            }
            ModelStreamEvent::ProviderReasoningReplay { item, .. } => {
                let item_bytes = serde_json::to_vec(&canonical_json(&item.payload))
                    .map_err(|_| invariant())?
                    .len();
                staged.reasoning_replay_bytes = staged
                    .reasoning_replay_bytes
                    .checked_add(item_bytes)
                    .filter(|value| *value <= budget.max_reasoning_replay_bytes.get())
                    .ok_or_else(quota)?;
                staged.provider_reasoning_replay.push(item.clone());
            }
            ModelStreamEvent::ToolCallCompleted {
                output_index,
                call_id,
                arguments,
            } => {
                let argument_bytes = serde_json::to_vec(&canonical_json(arguments))
                    .map_err(|_| invariant())?
                    .len();
                if argument_bytes > budget.max_tool_argument_bytes.get() {
                    return Err(quota());
                }
                if staged.tool_outputs.get(call_id) != Some(output_index) {
                    return Err(invariant());
                }
                staged
                    .tools
                    .get_mut(output_index)
                    .ok_or_else(invariant)?
                    .arguments = arguments.clone();
                let encoded =
                    serde_json::to_string(&canonical_json(arguments)).map_err(|_| invariant())?;
                staged
                    .tool_argument_fragments
                    .insert(*output_index, encoded.clone());
                let name = staged
                    .tools
                    .get(output_index)
                    .ok_or_else(invariant)?
                    .name
                    .clone();
                preview_candidate = Some((call_id.clone(), name, encoded));
                completed_preview_call = Some(call_id.clone());
            }
            ModelStreamEvent::ToolCallArgumentsDelta {
                output_index,
                call_id,
                delta,
            } => {
                if staged.tool_outputs.get(call_id) != Some(output_index) {
                    return Err(invariant());
                }
                let fragments = staged
                    .tool_argument_fragments
                    .get_mut(output_index)
                    .ok_or_else(invariant)?;
                fragments.push_str(delta);
                if fragments.len() > budget.max_tool_argument_bytes.get() {
                    return Err(quota());
                }
                let name = staged
                    .tools
                    .get(output_index)
                    .ok_or_else(invariant)?
                    .name
                    .clone();
                preview_candidate = Some((call_id.clone(), name, fragments.clone()));
            }
            ModelStreamEvent::Usage(usage) => staged.usage = Some(*usage),
            ModelStreamEvent::ResponseCompleted { stop_reason } => {
                staged.stop_reason = Some(*stop_reason);
            }
            ModelStreamEvent::ReasoningSummaryDelta { .. }
            | ModelStreamEvent::ArtifactContentFrame { .. }
            | ModelStreamEvent::NativeCompactionItem { .. }
            | ModelStreamEvent::ResponseError(_) => {}
        }
        let provider_failure = match &event {
            ModelStreamEvent::ResponseError(error) => Some(error.clone()),
            _ => None,
        };
        // Tool start/completion events stay invocation-private until the
        // terminal response proves the whole batch complete. Argument failures
        // are ordinary Tool results after that boundary, not model failures.
        staged.public_output_observed |= !matches!(
            &event,
            ModelStreamEvent::ToolCallStarted { .. }
                | ModelStreamEvent::ToolCallArgumentsDelta { .. }
                | ModelStreamEvent::ToolCallCompleted { .. }
        ) && event.is_public_output();
        staged.replay_barrier_observed |= !matches!(
            &event,
            ModelStreamEvent::ToolCallStarted { .. }
                | ModelStreamEvent::ToolCallArgumentsDelta { .. }
                | ModelStreamEvent::ToolCallCompleted { .. }
        ) && event.blocks_automatic_replay();
        // The coalescer releases a complete Tool lifecycle only together with
        // the validated terminal response. Mirror that exact delivery boundary
        // in the in-memory observation used for the atomic close decision.
        if matches!(&event, ModelStreamEvent::ResponseCompleted { .. }) && !staged.tools.is_empty()
        {
            staged.public_output_observed = true;
            staged.replay_barrier_observed = true;
        }
        let mut update = staged.coalescer.accept(event).map_err(|_| invariant())?;
        if let Some((call_id, name, fragments)) = preview_candidate
            && let Some(preview) = previewer(&call_id, &name, &fragments)
        {
            // Every accepted Artifact preview emits a public metadata header,
            // including a preview whose current document body is empty.
            staged.public_output_observed = true;
            staged.replay_barrier_observed = true;
            let output_id = OutputId::from_uuid(preview.tool_invocation_id.into_uuid())
                .map_err(|_| invariant())?;
            let preview_update = staged
                .coalescer
                .accept_artifact_preview(invocation_id, preview)
                .map_err(|_| invariant())?;
            staged.preview_outputs.insert(call_id, output_id);
            update.should_commit |= preview_update.should_commit;
            update.has_pending_output = preview_update.has_pending_output;
        }
        if let Some(call_id) = completed_preview_call
            && let Some(output_id) = staged.preview_outputs.get(&call_id).copied()
        {
            let preview_update = staged
                .coalescer
                .finish_artifact_preview(output_id)
                .map_err(|_| invariant())?;
            update.should_commit |= preview_update.should_commit;
            update.has_pending_output = preview_update.has_pending_output;
        }
        Ok(StagedCanonicalEvent {
            state: staged,
            update,
            provider_failure,
            progress,
        })
    }

    fn account_event(
        &mut self,
        event: &ModelStreamEvent,
        budget: ModelStreamBudgetSnapshot,
    ) -> Result<(), LoopPortError> {
        self.canonical_event_count = self
            .canonical_event_count
            .checked_add(1)
            .filter(|value| *value <= budget.max_events.get())
            .ok_or_else(quota)?;
        let event_bytes = model_stream_event_size(event)?;
        if event_bytes > budget.max_event_bytes.get() {
            return Err(quota());
        }
        self.canonical_event_bytes = self
            .canonical_event_bytes
            .checked_add(u64::try_from(event_bytes).map_err(|_| quota())?)
            .filter(|value| *value <= budget.max_total_bytes.get())
            .ok_or_else(quota)?;
        Ok(())
    }

    pub(crate) fn observation(&self, invocation_id: InvocationId) -> ModelInvocationObservation {
        ModelInvocationObservation {
            invocation_id,
            provider_response_ref: self.provider_response_ref.clone(),
            response_model_key: self.response_model_key.clone(),
            usage: self.usage,
            public_output_observed: self.public_output_observed,
            replay_barrier_observed: self.replay_barrier_observed,
        }
    }

    pub(crate) fn into_outcome(
        self,
        provider_state_policy: ProviderStatePolicy,
        budget: ModelStreamBudgetSnapshot,
    ) -> Result<ModelIterationOutcome, LoopPortError> {
        if !self.validator.is_terminal() {
            return Err(dependency());
        }
        let stop_reason = self.stop_reason.ok_or_else(dependency)?;
        let tool_calls = self.tools.into_values().collect::<Vec<_>>();
        if (stop_reason == StopReason::ToolCalls && tool_calls.is_empty())
            || (!matches!(
                stop_reason,
                StopReason::ToolCalls
                    | StopReason::MaxOutputTokens
                    | StopReason::TokenLimit
                    | StopReason::ContextWindowExceeded
            ) && !tool_calls.is_empty())
        {
            return Err(invariant());
        }
        let provider_response_ref = self.provider_response_ref.ok_or_else(dependency)?;
        let continuation_ref = if provider_state_policy == ProviderStatePolicy::OpaqueContinuation {
            Some(ContinuationRef::from_opaque(
                OpaqueExternalRef::new(provider_response_ref.as_str()).map_err(|_| invariant())?,
            ))
        } else {
            None
        };
        let reasoning_replay = if !self.provider_reasoning_replay.is_empty() {
            let encoded = encode_provider_reasoning_replay(&self.provider_reasoning_replay)?;
            if encoded.len() > budget.max_reasoning_replay_bytes.get() {
                return Err(quota());
            }
            Some(ModelReplayText::new(encoded).map_err(|_| quota())?)
        } else if self.private_reasoning.is_empty() {
            None
        } else {
            Some(ModelReplayText::new(self.private_reasoning).map_err(|_| quota())?)
        };
        Ok(ModelIterationOutcome {
            tool_calls,
            reasoning_replay,
            stop_reason,
            usage: self.usage,
            provider_response_ref,
            response_model_key: self.response_model_key.ok_or_else(dependency)?,
            continuation_ref,
            assistant_text_emitted: self.assistant_text_emitted,
            assistant_text: self
                .assistant_text
                .map(|text| text.trim().to_owned())
                .filter(|text| !text.is_empty()),
            public_output_observed: self.public_output_observed,
            replay_barrier_observed: self.replay_barrier_observed,
        })
    }
}

fn encode_provider_reasoning_replay(
    items: &[ProviderReasoningReplayItem],
) -> Result<String, LoopPortError> {
    let first = items.first().ok_or_else(invariant)?;
    if items.iter().any(|item| {
        item.source_api_format != first.source_api_format
            || item.source_model_key != first.source_model_key
    }) {
        return Err(invariant());
    }
    let payloads = items
        .iter()
        .map(|item| canonical_json(&item.payload))
        .collect::<Vec<_>>();
    serde_json::to_string(&serde_json::json!({
        "schema": "musteragent.legacy.runtime.provider-reasoning-replay.v2",
        "source_api_format": first.source_api_format.as_str(),
        "source_model_key": first.source_model_key,
        "items": payloads,
    }))
    .map_err(|_| invariant())
}

fn canonical_json(value: &JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(value) => serde_json::Value::Bool(*value),
        JsonValue::Integer(value) => serde_json::json!(value),
        JsonValue::Number(value) => serde_json::json!(value),
        JsonValue::String(value) => serde_json::Value::String(value.clone()),
        JsonValue::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        JsonValue::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect(),
        ),
    }
}

fn model_stream_event_size(event: &ModelStreamEvent) -> Result<usize, LoopPortError> {
    let payload = match event {
        ModelStreamEvent::ToolCallUnexposed { name } => name.as_str().len(),
        ModelStreamEvent::ToolCallRejected {
            call_id,
            diagnostic,
            ..
        } => call_id.as_str().len().saturating_add(
            diagnostic
                .as_ref()
                .map(|value| {
                    value
                        .encode()
                        .map(|bytes| bytes.len())
                        .map_err(|_| invariant())
                })
                .transpose()?
                .unwrap_or(0),
        ),
        ModelStreamEvent::TextDelta { delta, .. }
        | ModelStreamEvent::ReasoningDelta { delta, .. }
        | ModelStreamEvent::ReasoningSummaryDelta { delta, .. } => delta.as_str().len(),
        ModelStreamEvent::ToolCallArgumentsDelta { delta, .. } => delta.len(),
        ModelStreamEvent::ArtifactContentFrame { content, .. } => content.len(),
        ModelStreamEvent::ToolCallCompleted { arguments, .. } => {
            serde_json::to_vec(&canonical_json(arguments))
                .map_err(|_| invariant())?
                .len()
        }
        ModelStreamEvent::ProviderReasoningReplay { item, .. } => {
            serde_json::to_vec(&canonical_json(&item.payload))
                .map_err(|_| invariant())?
                .len()
        }
        ModelStreamEvent::ResponseStarted {
            response_ref,
            response_model_key,
        } => response_ref
            .as_str()
            .len()
            .saturating_add(response_model_key.as_str().len()),
        ModelStreamEvent::ToolCallStarted {
            call_id,
            name,
            provider_item_ref,
            ..
        } => call_id
            .as_str()
            .len()
            .saturating_add(name.as_str().len())
            .saturating_add(
                provider_item_ref
                    .as_ref()
                    .map_or(0, |reference| reference.as_str().len()),
            ),
        ModelStreamEvent::NativeCompactionItem { content_ref } => {
            usize::try_from(content_ref.byte_length).unwrap_or(usize::MAX)
        }
        ModelStreamEvent::Usage(_)
        | ModelStreamEvent::ResponseCompleted { .. }
        | ModelStreamEvent::ResponseError(_) => 128,
    };
    payload.checked_add(128).ok_or_else(quota)
}

const fn invariant() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}

const fn quota() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::QuotaExceeded,
        retryable: false,
    }
}

const fn dependency() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::DependencyUnavailable,
        retryable: true,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use musteragent_identity::{ArtifactId, InvocationId, ToolCallId};
    use musteragent_model::{ModelKey, ProviderStatePolicy};
    use musteragent_runtime_contract::{ErrorCode, OpaqueExternalRef};
    use musteragent_runtime_model::{
        JsonValue, ModelStreamEvent, ModelText, ProviderRequestRef, StopReason,
    };
    use musteragent_runtime_snapshot::RunExecutionPolicySnapshot;
    use musteragent_tool::WireName;

    use super::CanonicalConsumptionState;
    use crate::{ToolDocumentPreview, ToolDocumentPreviewOperation};

    fn outcome(events: Vec<ModelStreamEvent>) -> crate::state::ModelIterationOutcome {
        let policy = RunExecutionPolicySnapshot::production_defaults();
        let mut state = CanonicalConsumptionState::new(
            policy.max_output_frame_bytes.get(),
            policy.max_model_events_per_commit.get(),
        );
        let invocation_id = InvocationId::generate();
        for event in events {
            state = state
                .stage_event(invocation_id, event, policy.model_stream, |_, _, _| None)
                .expect("canonical event")
                .state;
        }
        state
            .into_outcome(ProviderStatePolicy::Stateless, policy.model_stream)
            .expect("successful outcome without Usage")
    }

    fn started() -> ModelStreamEvent {
        ModelStreamEvent::ResponseStarted {
            response_ref: ProviderRequestRef::from_opaque(
                OpaqueExternalRef::new("response-no-usage").expect("response ref"),
            ),
            response_model_key: ModelKey::new("response-model").expect("model key"),
        }
    }

    #[test]
    fn successful_assistant_response_does_not_require_usage() {
        let outcome = outcome(vec![
            started(),
            ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: ModelText::new("answer").expect("text"),
            },
            ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            },
        ]);

        assert!(outcome.usage.is_none());
        assert!(outcome.assistant_text_emitted);
        assert!(outcome.tool_calls.is_empty());
    }

    #[test]
    fn successful_tool_calls_do_not_require_usage() {
        let call_id = ToolCallId::new("call-no-usage").expect("call id");
        let outcome = outcome(vec![
            started(),
            ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: WireName::new("lookup").expect("wire name"),
                provider_item_ref: None,
                provider_replay: None,
            },
            ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id,
                arguments: JsonValue::Object(BTreeMap::new()),
            },
            ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            },
        ]);

        assert!(outcome.usage.is_none());
        assert_eq!(outcome.tool_calls.len(), 1);
        assert!(outcome.public_output_observed);
        assert!(outcome.replay_barrier_observed);
    }

    #[test]
    fn reasoning_summary_is_visible_without_blocking_a_sealed_replacement() {
        let outcome = outcome(vec![
            started(),
            ModelStreamEvent::ReasoningSummaryDelta {
                output_index: 0,
                delta: ModelText::new("working").expect("summary"),
            },
            ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            },
        ]);

        assert!(outcome.public_output_observed);
        assert!(!outcome.replay_barrier_observed);
    }

    #[test]
    fn whitelisted_tool_argument_content_becomes_a_safe_artifact_frame() {
        let policy = RunExecutionPolicySnapshot::production_defaults();
        let model_invocation_id = InvocationId::generate();
        let tool_invocation_id = InvocationId::generate();
        let artifact_id =
            ArtifactId::from_uuid(tool_invocation_id.into_uuid()).expect("artifact identity");
        let call_id = ToolCallId::new("note-preview-call").expect("call id");
        let mut state = CanonicalConsumptionState::new(
            policy.max_output_frame_bytes.get(),
            policy.max_model_events_per_commit.get(),
        );
        for event in [
            started(),
            ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: WireName::new("artifact_note_create").expect("wire name"),
                provider_item_ref: None,
                provider_replay: None,
            },
            ModelStreamEvent::ToolCallArgumentsDelta {
                output_index: 0,
                call_id,
                delta: r##"{"title":"文档","initial_content":"# 文档"##.to_owned(),
            },
        ] {
            state = state
                .stage_event(
                    model_invocation_id,
                    event,
                    policy.model_stream,
                    |_, name, fragments| {
                        (name.as_str() == "artifact_note_create"
                            && fragments.contains("initial_content"))
                        .then(|| ToolDocumentPreview {
                            artifact_id,
                            tool_invocation_id,
                            operation: ToolDocumentPreviewOperation::Create,
                            title: Some("文档".to_owned()),
                            content: "# 文档".to_owned(),
                        })
                    },
                )
                .expect("canonical event")
                .state;
        }
        state.coalescer.flush_pending().expect("flush preview");
        let frames = state.coalescer.ready_batch(8);
        assert_eq!(frames.len(), 2, "response start and preview are durable");
        assert!(
            frames
                .iter()
                .all(|event| !matches!(event, ModelStreamEvent::ToolCallArgumentsDelta { .. }))
        );
        let preview = frames
            .iter()
            .find_map(|event| match event {
                ModelStreamEvent::ArtifactContentFrame { content, .. } => Some(content.as_str()),
                _ => None,
            })
            .expect("safe artifact preview frame");
        let (header, markdown) = preview.split_once('\n').expect("preview envelope");
        assert!(header.contains("musteragent.note-preview.v3"));
        assert!(header.contains(tool_invocation_id.to_string().as_str()));
        assert_eq!(markdown, "# 文档");
        assert!(!preview.contains("initial_content"));
    }

    #[test]
    fn empty_artifact_body_still_records_the_public_preview_header() {
        let policy = RunExecutionPolicySnapshot::production_defaults();
        let model_invocation_id = InvocationId::generate();
        let tool_invocation_id = InvocationId::generate();
        let artifact_id =
            ArtifactId::from_uuid(tool_invocation_id.into_uuid()).expect("artifact identity");
        let call_id = ToolCallId::new("empty-preview-call").expect("call id");
        let mut state = CanonicalConsumptionState::new(
            policy.max_output_frame_bytes.get(),
            policy.max_model_events_per_commit.get(),
        );
        for event in [
            started(),
            ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: WireName::new("artifact_note_create").expect("wire name"),
                provider_item_ref: None,
                provider_replay: None,
            },
            ModelStreamEvent::ToolCallArgumentsDelta {
                output_index: 0,
                call_id,
                delta: r##"{"title":"Empty","initial_content":"""##.to_owned(),
            },
        ] {
            state = state
                .stage_event(
                    model_invocation_id,
                    event,
                    policy.model_stream,
                    |_, _, _| {
                        Some(ToolDocumentPreview {
                            artifact_id,
                            tool_invocation_id,
                            operation: ToolDocumentPreviewOperation::Create,
                            title: Some("Empty".to_owned()),
                            content: String::new(),
                        })
                    },
                )
                .expect("canonical event")
                .state;
        }
        assert!(state.public_output_observed);
        assert!(state.replay_barrier_observed);
    }

    #[test]
    fn successful_empty_response_does_not_require_usage() {
        let outcome = outcome(vec![
            started(),
            ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            },
        ]);

        assert!(outcome.usage.is_none());
        assert!(!outcome.assistant_text_emitted);
        assert!(outcome.tool_calls.is_empty());
    }

    #[test]
    fn private_reasoning_uses_the_eight_mebibyte_replay_contract() {
        let mut events = vec![started()];
        let chunk = ModelText::new("r".repeat(1_048_576)).expect("one MiB event");
        for output_index in 0..8 {
            events.push(ModelStreamEvent::ReasoningDelta {
                output_index,
                delta: chunk.clone(),
            });
        }
        events.push(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        });

        let outcome = outcome(events);
        assert_eq!(
            outcome
                .reasoning_replay
                .as_ref()
                .expect("private replay")
                .as_str()
                .len(),
            8 * 1_048_576
        );
    }

    #[test]
    fn provider_replay_final_wrapper_is_charged_at_the_exact_pinned_boundary() {
        use musteragent_model::ApiFormat;
        use musteragent_runtime_model::ProviderReasoningReplayItem;

        fn item(payload_bytes: usize, output_index: u32) -> ModelStreamEvent {
            ModelStreamEvent::ProviderReasoningReplay {
                output_index,
                item: ProviderReasoningReplayItem {
                    source_api_format: ApiFormat::GoogleGenerateContent,
                    source_model_key: "provider-model".to_owned(),
                    payload: JsonValue::Object(BTreeMap::from([(
                        "data".to_owned(),
                        JsonValue::String("x".repeat(payload_bytes)),
                    )])),
                },
            }
        }

        let policy = RunExecutionPolicySnapshot::production_defaults();
        let limit = policy.model_stream.max_reasoning_replay_bytes.get();
        let item_count = 9_usize;
        let mut probe = CanonicalConsumptionState::new(
            policy.max_output_frame_bytes.get(),
            policy.max_model_events_per_commit.get(),
        );
        let probe_invocation_id = InvocationId::generate();
        for event in [started()]
            .into_iter()
            .chain(
                (0..item_count).map(|index| item(0, u32::try_from(index).expect("output index"))),
            )
            .chain([ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }])
        {
            probe = probe
                .stage_event(
                    probe_invocation_id,
                    event,
                    policy.model_stream,
                    |_, _, _| None,
                )
                .expect("probe event")
                .state;
        }
        let wrapper_bytes = probe
            .clone()
            .into_outcome(ProviderStatePolicy::Stateless, policy.model_stream)
            .expect("probe outcome")
            .reasoning_replay
            .expect("probe replay")
            .as_str()
            .len();
        let payload_bytes = limit - wrapper_bytes;
        let base = payload_bytes / item_count;
        let remainder = payload_bytes % item_count;

        let consume = |extra: usize| {
            let mut state = CanonicalConsumptionState::new(
                policy.max_output_frame_bytes.get(),
                policy.max_model_events_per_commit.get(),
            );
            let events = [started()]
                .into_iter()
                .chain((0..item_count).map(|index| {
                    let tail = usize::from(index == item_count - 1);
                    item(
                        base + tail * (remainder + extra),
                        u32::try_from(index).expect("output index"),
                    )
                }))
                .chain([ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop,
                }]);
            let invocation_id = InvocationId::generate();
            for event in events {
                state = state
                    .stage_event(invocation_id, event, policy.model_stream, |_, _, _| None)
                    .expect("payload remains inside event and payload budgets")
                    .state;
            }
            state.into_outcome(ProviderStatePolicy::Stateless, policy.model_stream)
        };

        let exact = consume(0).expect("exact final replay bound");
        assert_eq!(
            exact.reasoning_replay.expect("exact replay").as_str().len(),
            limit
        );
        assert!(matches!(
            consume(1),
            Err(crate::LoopPortError {
                code: ErrorCode::QuotaExceeded,
                retryable: false,
            })
        ));
    }
}
