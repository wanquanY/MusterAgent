use std::collections::BTreeMap;

use musteragent_identity::ToolCallId;
use musteragent_model::{ApiFormat, ModelKey};
use musteragent_runtime_model::{
    JsonValue, ModelRouteRef, ModelStreamEvent, ModelUsage, ProviderError, ProviderErrorCode,
    ProviderErrorKind, ProviderReasoningReplayItem, ProviderRequestRef, ProviderToolCallReplay,
    StopReason,
};
use musteragent_runtime_snapshot::{ModelStreamBudgetSnapshot, RunExecutionPolicySnapshot};
use musteragent_tool::WireName;
use serde_json::Value;

mod arguments;
mod error;
mod google;
mod identity;
pub(crate) use google::GoogleContinuation;
pub mod overflow;
mod support;
pub use error::NormalizeError;
use support::*;

#[derive(Debug)]
struct ToolState {
    provider_call_id: String,
    call_id: ToolCallId,
    name: WireName,
    arguments: String,
    argument_source: musteragent_runtime_model::ArgumentSource,
    argument_snapshot_seen: bool,
    argument_stream_started: bool,
    arguments_from_step: bool,
    response_item_done: bool,
    provider_item_ref: Option<ProviderRequestRef>,
    provider_replay: Option<ProviderToolCallReplay>,
    started: bool,
    completed: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum InteractionStepKind {
    Thought,
    ModelOutput,
    FunctionCall,
    Input,
}

#[derive(Debug)]
enum ReasoningReplayState {
    OpenAiResponses { item: Value },
    Google { item: Value },
    AnthropicThinking { thinking: String, signature: String },
    AnthropicRedacted { data: String },
    Bedrock { text: String, signature: String },
}

fn append_bounded(
    target: &mut String,
    fragment: &str,
    maximum_bytes: usize,
) -> Result<(), NormalizeError> {
    target
        .len()
        .checked_add(fragment.len())
        .filter(|total| *total <= maximum_bytes)
        .ok_or(NormalizeError::StreamBudgetExceeded)?;
    target.push_str(fragment);
    Ok(())
}

/// Stateful, fail-closed normalizer for one physical Provider invocation.
#[derive(Debug)]
pub struct ProviderNormalizer {
    format: ApiFormat,
    source_route_ref: ModelRouteRef,
    source_model_key: ModelKey,
    response_model_key: ModelKey,
    response_model_reported: bool,
    publish_disclosed_reasoning: bool,
    gateway: bool,
    fallback_response_ref: ProviderRequestRef,
    tool_call_scope: Option<String>,
    response_ref: Option<ProviderRequestRef>,
    started: bool,
    terminal: bool,
    tools: BTreeMap<u32, ToolState>,
    tool_names: Option<BTreeMap<WireName, WireName>>,
    reasoning_replay: BTreeMap<u32, ReasoningReplayState>,
    interaction_steps: BTreeMap<u32, InteractionStepKind>,
    google: google::GoogleState,
    pending_stop: Option<StopReason>,
    latest_usage: ModelUsage,
    interaction_usage: Option<ModelUsage>,
    stream_budget: ModelStreamBudgetSnapshot,
}

impl ProviderNormalizer {
    pub fn new(
        format: ApiFormat,
        source_route_ref: ModelRouteRef,
        source_model_key: impl Into<String>,
        invocation_ref: impl Into<String>,
    ) -> Result<Self, NormalizeError> {
        let source_model_key = source_model_key.into();
        let source_model_key =
            ModelKey::new(source_model_key).map_err(|_| NormalizeError::InvalidModelIdentity)?;
        let invocation_ref = invocation_ref.into();
        let tool_call_scope =
            matches!(format, ApiFormat::OpenAiChatCompletions).then(|| invocation_ref.clone());
        Ok(Self {
            format,
            source_route_ref,
            response_model_key: source_model_key.clone(),
            response_model_reported: false,
            source_model_key,
            publish_disclosed_reasoning: false,
            gateway: false,
            fallback_response_ref: provider_ref(invocation_ref)?,
            tool_call_scope,
            response_ref: None,
            started: false,
            terminal: false,
            tools: BTreeMap::new(),
            tool_names: None,
            reasoning_replay: BTreeMap::new(),
            interaction_steps: BTreeMap::new(),
            google: google::GoogleState::default(),
            pending_stop: None,
            latest_usage: ModelUsage::default(),
            interaction_usage: None,
            stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
        })
    }

    pub(crate) fn with_gateway(mut self, gateway: bool) -> Self {
        self.gateway = gateway;
        self
    }

    /// Authorize provider-disclosed reasoning to enter the public reasoning
    /// output lane in addition to the private replay lane.
    ///
    /// This must only be enabled from an admission-pinned route capability and
    /// a product request. Provider field names alone never grant disclosure.
    #[must_use]
    pub fn with_disclosed_reasoning_output(mut self, enabled: bool) -> Self {
        self.publish_disclosed_reasoning = enabled;
        self
    }

    /// Applies the exact admission-pinned budget before any Provider event is
    /// normalized. This is independent from transport and Loop enforcement.
    pub fn with_stream_budget(
        mut self,
        budget: ModelStreamBudgetSnapshot,
    ) -> Result<Self, NormalizeError> {
        self.stream_budget = budget
            .validate()
            .map_err(|_| NormalizeError::StreamBudgetExceeded)?;
        Ok(self)
    }

    /// Bind this physical response to the exact declared model interface.
    /// Provider/private replay bytes retain provider names; public events use
    /// canonical names. Raw canonical identities cannot bypass the interface.
    pub fn with_tool_definitions(
        mut self,
        tools: &[musteragent_runtime_model::CanonicalToolDefinition],
    ) -> Result<Self, NormalizeError> {
        let mut names = BTreeMap::new();
        let mut canonical = std::collections::BTreeSet::new();
        for tool in tools {
            tool.validate()
                .map_err(|_| NormalizeError::InvalidToolIdentity)?;
            if names
                .insert(tool.model_name.clone(), tool.name.clone())
                .is_some()
                || !canonical.insert(tool.name.clone())
            {
                return Err(NormalizeError::InvalidToolIdentity);
            }
        }
        self.tool_names = Some(names);
        Ok(self)
    }

    pub fn observe(
        &mut self,
        event_name: Option<&str>,
        value: Value,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        if self.terminal {
            return Err(NormalizeError::EventAfterTerminal);
        }
        let events = match self.format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                Err(NormalizeError::UnknownEventType)
            }
            ApiFormat::OpenAiChatCompletions => self.openai_chat(value),
            ApiFormat::OpenAiResponses => self.openai_responses(event_name, value),
            ApiFormat::AnthropicMessages => self.anthropic(event_name, value),
            ApiFormat::GoogleGenerateContent => self.google_generate(value),
            ApiFormat::GoogleInteractions => self.google_interactions(event_name, value),
            ApiFormat::BedrockConverse => self.bedrock(event_name, value),
            ApiFormat::BedrockInvokeModel => self.anthropic(event_name, value),
        }?;
        self.validate_accumulators()?;
        Ok(events)
    }

    pub fn finish(&mut self) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        if self.terminal {
            return Ok(Vec::new());
        }
        if !self.started {
            return Err(NormalizeError::ClosedBeforeStart);
        }
        // Only protocols whose final data frame carries a stop reason may use
        // transport EOF/[DONE] as the terminal delimiter. Protocols with an
        // explicit completion event must observe that event; otherwise a
        // half-closed response could be mistaken for success.
        if !matches!(
            self.format,
            ApiFormat::OpenAiChatCompletions | ApiFormat::GoogleGenerateContent
        ) || self.pending_stop.is_none()
        {
            return Err(NormalizeError::ClosedBeforeTerminal);
        }
        self.complete_terminal()
    }

    fn validate_accumulators(&self) -> Result<(), NormalizeError> {
        if self.tools.len()
            > usize::try_from(self.stream_budget.max_tool_calls.get())
                .map_err(|_| NormalizeError::StreamBudgetExceeded)?
            || self
                .tools
                .values()
                .any(|tool| tool.arguments.len() > self.stream_budget.max_tool_argument_bytes.get())
        {
            return Err(NormalizeError::StreamBudgetExceeded);
        }
        let reasoning_bytes = self.reasoning_replay_bytes()?;
        if reasoning_bytes > self.stream_budget.max_reasoning_replay_bytes.get() {
            return Err(NormalizeError::StreamBudgetExceeded);
        }
        Ok(())
    }

    fn reasoning_replay_bytes(&self) -> Result<usize, NormalizeError> {
        self.reasoning_replay
            .iter()
            .try_fold(0_usize, |total, (_index, state)| {
                let bytes = match state {
                    ReasoningReplayState::Google { .. } => {
                        self.google.replay_sizes.get(_index).copied().unwrap_or(0)
                    }
                    ReasoningReplayState::OpenAiResponses { item } => serde_json::to_vec(item)
                        .map_err(|_| NormalizeError::InvalidJson)?
                        .len(),
                    ReasoningReplayState::AnthropicThinking {
                        thinking,
                        signature,
                    }
                    | ReasoningReplayState::Bedrock {
                        text: thinking,
                        signature,
                    } => thinking.len().saturating_add(signature.len()),
                    ReasoningReplayState::AnthropicRedacted { data } => data.len(),
                };
                total
                    .checked_add(bytes)
                    .ok_or(NormalizeError::StreamBudgetExceeded)
            })
    }

    fn ensure_reasoning_capacity(&self, additional: usize) -> Result<(), NormalizeError> {
        self.reasoning_replay_bytes()?
            .checked_add(additional)
            .filter(|total| *total <= self.stream_budget.max_reasoning_replay_bytes.get())
            .map(|_| ())
            .ok_or(NormalizeError::StreamBudgetExceeded)
    }

    fn complete_terminal(&mut self) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        if self.terminal {
            return Ok(Vec::new());
        }
        if !self.started {
            return Err(NormalizeError::ClosedBeforeStart);
        }
        let stop_reason = self.pending_stop.unwrap_or(StopReason::Stop);
        let mut events = self.complete_reasoning_replay()?;
        if stop_reason.is_token_limited() {
            events.extend(self.complete_truncated_tools()?);
        } else {
            events.extend(self.complete_tools()?);
        }
        events.push(ModelStreamEvent::ResponseCompleted { stop_reason });
        self.terminal = true;
        Ok(events)
    }

    fn complete_interaction_terminal(&mut self) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let mut events = self.complete_terminal()?;
        if let Some(usage) = self.interaction_usage {
            let terminal_index = events
                .len()
                .checked_sub(1)
                .ok_or(NormalizeError::ClosedBeforeStart)?;
            events.insert(
                terminal_index,
                ModelStreamEvent::Usage(google::google_usage_sum(
                    self.google.completed_usage,
                    usage,
                )?),
            );
        }
        Ok(events)
    }

    fn openai_chat(&mut self, value: Value) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let object = json_object(&value)?;
        if let Some(error) = object.get("error") {
            return self.provider_error(error, ProviderErrorKind::Server);
        }
        self.capture_response_ref(object.get("id"))?;
        self.capture_response_model_key(object.get("model"))?;
        let choices = array_required(object, "choices")?;
        let mut events = if choices.is_empty() && object.get("usage").is_none_or(Value::is_null) {
            Vec::new()
        } else {
            self.ensure_started()
        };
        for choice in choices {
            let choice = json_object(choice)?;
            let index = u32_field_default(choice, "index", 0)?;
            let delta = object_required(choice, "delta")?;
            if let Some(value) = non_empty_text(delta.get("reasoning_summary"))? {
                events.push(reasoning_summary(index, value)?);
            }
            if let Some(value) = first_non_empty_text(
                delta,
                &[
                    "reasoning_content",
                    "reasoning",
                    "thinking_content",
                    "thinking",
                ],
            )? {
                events.extend(self.disclosed_reasoning(index, value)?);
            }
            if let Some(content) = delta.get("content") {
                events.extend(text_content(index, content)?);
            }
            if let Some(tool_deltas) = delta.get("tool_calls") {
                for tool_delta in array(tool_deltas)? {
                    events.extend(self.openai_tool_delta(tool_delta)?);
                }
            }
            if let Some(reason) = optional_text(choice.get("finish_reason"))? {
                self.pending_stop = Some(stop_reason(reason, self.format)?);
            }
        }
        if let Some(usage) = object.get("usage").filter(|value| !value.is_null()) {
            events.push(ModelStreamEvent::Usage(openai_usage(usage)?));
        }
        Ok(events)
    }

    fn openai_tool_delta(
        &mut self,
        value: &Value,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let object = json_object(value)?;
        let index = u32_field_default(object, "index", 0)?;
        let function = object.get("function").map(json_object).transpose()?;
        let call_id = optional_text(object.get("id"))?;
        let name = function
            .and_then(|value| value.get("name"))
            .map(required_text)
            .transpose()?;
        let arguments = function
            .and_then(|value| value.get("arguments"))
            .map(string_value)
            .transpose()?
            .unwrap_or_default();
        let mut events = Vec::new();
        match self.tools.entry(index) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(tool_state(
                    call_id,
                    name,
                    index,
                    None,
                    None,
                    self.tool_call_scope.as_deref(),
                )?);
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if call_id.is_some() || name.is_some() =>
            {
                let state = entry.get_mut();
                if call_id.is_some_and(|value| value != state.provider_call_id)
                    || name.is_some_and(|value| value != state.name.as_str())
                {
                    return Err(NormalizeError::ToolIdentityChanged);
                }
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
        events.extend(self.start_tool(index)?);
        if !arguments.is_empty() {
            let state = self
                .tools
                .get_mut(&index)
                .ok_or(NormalizeError::UnknownTool)?;
            append_bounded(
                &mut state.arguments,
                arguments,
                self.stream_budget.max_tool_argument_bytes.get(),
            )?;
            events.push(ModelStreamEvent::ToolCallArgumentsDelta {
                output_index: index,
                call_id: state.call_id.clone(),
                delta: arguments.to_owned(),
            });
        }
        Ok(events)
    }

    fn openai_responses(
        &mut self,
        event_name: Option<&str>,
        value: Value,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let object = json_object(&value)?;
        let event_type = event_name
            .filter(|name| !name.is_empty() && *name != "message")
            .or_else(|| object.get("type").and_then(Value::as_str))
            .ok_or(NormalizeError::MissingEventType)?;
        if let Some(response) = object.get("response").and_then(Value::as_object) {
            self.capture_response_ref(response.get("id"))?;
            self.capture_response_model_key(response.get("model"))?;
        }
        self.capture_response_ref(object.get("response_id"))?;
        let mut events = Vec::new();
        match event_type {
            "response.created" | "response.in_progress" | "response.queued" => {
                events.extend(self.ensure_started());
            }
            "response.output_text.delta" | "response.refusal.delta" => {
                events.extend(self.ensure_started());
                events.push(text_delta(
                    u32_field_default(object, "output_index", 0)?,
                    required_text_field(object, "delta")?,
                )?);
            }
            "response.reasoning_summary_text.delta" | "response.reasoning_summary.delta" => {
                events.extend(self.ensure_started());
                events.push(reasoning_summary(
                    u32_field_default(object, "output_index", 0)?,
                    required_text_field(object, "delta")?,
                )?);
            }
            "response.reasoning_text.delta" | "response.reasoning.delta" => {
                events.extend(self.ensure_started());
                events.extend(self.disclosed_reasoning(
                    u32_field_default(object, "output_index", 0)?,
                    required_text_field(object, "delta")?,
                )?);
            }
            "response.output_item.added" => {
                let item = object_required(object, "item")?;
                if matches!(
                    required_text_field(item, "type")?,
                    "function_call" | "tool_call"
                ) {
                    let index = u32_field_default(object, "output_index", 0)?;
                    if self.tools.contains_key(&index) {
                        return Err(NormalizeError::ToolIdentityChanged);
                    }
                    let call_id = optional_text(item.get("call_id").or(item.get("id")))?;
                    let name = optional_text(item.get("name"))?;
                    let item_ref = optional_text(item.get("id"))?
                        .map(provider_ref)
                        .transpose()?;
                    self.tools.insert(
                        index,
                        tool_state(
                            call_id,
                            name,
                            index,
                            item_ref,
                            None,
                            self.tool_call_scope.as_deref(),
                        )?,
                    );
                    events.extend(self.ensure_started());
                    events.extend(self.start_tool(index)?);
                    if let Some(arguments) = item
                        .get("arguments")
                        .map(string_value)
                        .transpose()?
                        .filter(|value| !value.is_empty())
                    {
                        self.observe_argument_snapshot(
                            index,
                            arguments,
                            musteragent_runtime_model::ArgumentSource::InitialSnapshot,
                        )?;
                    }
                } else if required_text_field(item, "type")? == "compaction" {
                    return Err(NormalizeError::NativeCompactionStoreRequired);
                }
            }
            "response.function_call_arguments.delta" => {
                let index = u32_field_default(object, "output_index", 0)?;
                let delta = required_text_field(object, "delta")?;
                let state = self
                    .tools
                    .get_mut(&index)
                    .ok_or(NormalizeError::UnknownTool)?;
                if state.response_item_done {
                    return Err(NormalizeError::DuplicateToolCompletion);
                }
                if state.argument_snapshot_seen {
                    return Err(NormalizeError::ConflictingToolArguments);
                }
                state.argument_stream_started |= !delta.is_empty();
                append_bounded(
                    &mut state.arguments,
                    delta,
                    self.stream_budget.max_tool_argument_bytes.get(),
                )?;
                events.push(ModelStreamEvent::ToolCallArgumentsDelta {
                    output_index: index,
                    call_id: state.call_id.clone(),
                    delta: delta.into(),
                });
            }
            "response.function_call_arguments.done" => {
                let index = u32_field_default(object, "output_index", 0)?;
                if let Some(arguments) = object.get("arguments").map(string_value).transpose()? {
                    self.observe_argument_snapshot(
                        index,
                        arguments,
                        musteragent_runtime_model::ArgumentSource::ArgumentsDone,
                    )?;
                }
            }
            "response.output_item.done" => {
                let index = u32_field_default(object, "output_index", 0)?;
                if let Some(item) = object.get("item").and_then(Value::as_object)
                    && self.tools.contains_key(&index)
                {
                    self.validate_response_tool_snapshot(index, item)?;
                }
                if let Some(item) = object.get("item").and_then(Value::as_object)
                    && let Some(arguments) = item.get("arguments").map(string_value).transpose()?
                {
                    self.observe_argument_snapshot(
                        index,
                        arguments,
                        musteragent_runtime_model::ArgumentSource::OutputItemDone,
                    )?;
                }
                if let Some(tool) = self.tools.get_mut(&index) {
                    if tool.response_item_done {
                        return Err(NormalizeError::DuplicateToolCompletion);
                    }
                    tool.response_item_done = true;
                }
                // Item completion precedes response.incomplete on output-token
                // exhaustion. Parse only at the response terminal boundary,
                // where truncation is known; no partial Tool may be executed.
                if let Some(item) = object.get("item").and_then(Value::as_object)
                    && item.get("type").and_then(Value::as_str) == Some("reasoning")
                {
                    self.reasoning_replay.insert(
                        index,
                        ReasoningReplayState::OpenAiResponses {
                            item: Value::Object(item.clone()),
                        },
                    );
                }
            }
            "response.completed" | "response.incomplete" => {
                events.extend(self.ensure_started());
                if let Some(response) = object.get("response").and_then(Value::as_object) {
                    self.observe_response_argument_snapshots(response)?;
                    self.capture_openai_response_reasoning(response)?;
                    if let Some(usage) = response.get("usage") {
                        events.push(ModelStreamEvent::Usage(openai_responses_usage(usage)?));
                    }
                    self.pending_stop = if event_type == "response.incomplete" {
                        Some(
                            response
                                .get("incomplete_details")
                                .and_then(Value::as_object)
                                .and_then(|details| details.get("reason"))
                                .and_then(Value::as_str)
                                .map(|reason| stop_reason(reason, self.format))
                                .transpose()?
                                .ok_or(NormalizeError::MissingField("incomplete_details.reason"))?,
                        )
                    } else {
                        response
                            .get("incomplete_details")
                            .and_then(Value::as_object)
                            .and_then(|details| details.get("reason"))
                            .and_then(Value::as_str)
                            .map(|reason| stop_reason(reason, self.format))
                            .transpose()?
                            .or_else(|| (!self.tools.is_empty()).then_some(StopReason::ToolCalls))
                            .or(Some(StopReason::Stop))
                    };
                } else {
                    self.pending_stop = Some(if event_type == "response.incomplete" {
                        return Err(NormalizeError::MissingField("incomplete_details.reason"));
                    } else if self.tools.is_empty() {
                        StopReason::Stop
                    } else {
                        StopReason::ToolCalls
                    });
                }
                events.extend(self.complete_terminal()?);
            }
            "response.failed" | "response.error" | "error" => {
                return self.provider_error(&value, ProviderErrorKind::Server);
            }
            // Lifecycle events with no canonical payload are accepted
            // explicitly; every other event fails closed below.
            "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.done"
            | "response.refusal.done" => {}
            other if other.contains("compaction") => {
                return Err(NormalizeError::NativeCompactionStoreRequired);
            }
            _ => return Err(NormalizeError::UnknownEventType),
        }
        Ok(events)
    }

    fn anthropic(
        &mut self,
        event_name: Option<&str>,
        value: Value,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let object = json_object(&value)?;
        let event_type = object
            .get("type")
            .and_then(Value::as_str)
            .or(event_name)
            .ok_or(NormalizeError::MissingEventType)?;
        let mut events = Vec::new();
        match event_type {
            "message_start" => {
                let message = object_required(object, "message")?;
                self.capture_response_ref(message.get("id"))?;
                self.capture_response_model_key(message.get("model"))?;
                events.extend(self.ensure_started());
                if let Some(usage) = message.get("usage") {
                    let usage = anthropic_usage(usage, Some(self.latest_usage))?;
                    self.latest_usage = usage;
                    events.push(ModelStreamEvent::Usage(usage));
                }
            }
            "content_block_start" => {
                let index = u32_field_default(object, "index", 0)?;
                let block = object_required(object, "content_block")?;
                match required_text_field(block, "type")? {
                    "tool_use" => {
                        let item_ref = optional_text(block.get("id"))?
                            .map(provider_ref)
                            .transpose()?;
                        let state = tool_state(
                            optional_text(block.get("id"))?,
                            optional_text(block.get("name"))?,
                            index,
                            item_ref,
                            None,
                            self.tool_call_scope.as_deref(),
                        )?;
                        self.tools.insert(index, state);
                        events.extend(self.ensure_started());
                        events.extend(self.start_tool(index)?);
                        if let Some(input) = block.get("input")
                            && input.as_object().is_none_or(|object| !object.is_empty())
                        {
                            let arguments = input.to_string();
                            let state = self
                                .tools
                                .get_mut(&index)
                                .ok_or(NormalizeError::UnknownTool)?;
                            state.arguments = arguments.clone();
                            state.arguments_from_step = true;
                            state.argument_source =
                                musteragent_runtime_model::ArgumentSource::InitialSnapshot;
                            events.push(ModelStreamEvent::ToolCallArgumentsDelta {
                                output_index: index,
                                call_id: state.call_id.clone(),
                                delta: arguments,
                            });
                        }
                    }
                    "text" => {
                        if let Some(text) = non_empty_text(block.get("text"))? {
                            events.extend(self.ensure_started());
                            events.push(text_delta(index, text)?);
                        }
                    }
                    "thinking" => {
                        let thinking = optional_text(block.get("thinking"))?
                            .unwrap_or_default()
                            .to_owned();
                        if self
                            .reasoning_replay
                            .insert(
                                index,
                                ReasoningReplayState::AnthropicThinking {
                                    thinking: thinking.clone(),
                                    signature: String::new(),
                                },
                            )
                            .is_some()
                        {
                            return Err(NormalizeError::DuplicateReasoningBlock);
                        }
                        if !thinking.is_empty() {
                            events.extend(self.ensure_started());
                            events.extend(self.disclosed_reasoning(index, &thinking)?);
                        }
                    }
                    "redacted_thinking" => {
                        let data = required_text_field(block, "data")?.to_owned();
                        if self
                            .reasoning_replay
                            .insert(index, ReasoningReplayState::AnthropicRedacted { data })
                            .is_some()
                        {
                            return Err(NormalizeError::DuplicateReasoningBlock);
                        }
                        events.extend(self.ensure_started());
                    }
                    _ => return Err(NormalizeError::UnknownContentBlock),
                }
            }
            "content_block_delta" => {
                let index = u32_field_default(object, "index", 0)?;
                let delta = object_required(object, "delta")?;
                match required_text_field(delta, "type")? {
                    "text_delta" => {
                        let fragment = string_field(delta, "text")?;
                        if !fragment.is_empty() {
                            events.extend(self.ensure_started());
                            events.push(text_delta(index, fragment)?);
                        }
                    }
                    "thinking_delta" => {
                        let thinking = string_field(delta, "thinking")?;
                        self.ensure_reasoning_capacity(thinking.len())?;
                        let ReasoningReplayState::AnthropicThinking {
                            thinking: replay, ..
                        } = self
                            .reasoning_replay
                            .get_mut(&index)
                            .ok_or(NormalizeError::UnknownReasoningBlock)?
                        else {
                            return Err(NormalizeError::UnknownReasoningBlock);
                        };
                        replay.push_str(thinking);
                        if !thinking.is_empty() {
                            events.extend(self.ensure_started());
                            events.extend(self.disclosed_reasoning(index, thinking)?);
                        }
                    }
                    "reasoning_summary_delta" | "summary_text_delta" => {
                        events.extend(self.ensure_started());
                        events.push(reasoning_summary(
                            index,
                            required_text_field(delta, "text")?,
                        )?);
                    }
                    "input_json_delta" => {
                        let fragment = delta
                            .get("partial_json")
                            .ok_or(NormalizeError::MissingField("partial_json"))
                            .and_then(string_value)?;
                        // Anthropic-compatible gateways may emit a syntactically
                        // valid empty JSON delta before the first argument bytes.
                        // It advances no canonical state and must not create an
                        // invalid empty ToolCallArgumentsDelta.
                        if fragment.is_empty() {
                            return Ok(events);
                        }
                        let state = self
                            .tools
                            .get_mut(&index)
                            .ok_or(NormalizeError::UnknownTool)?;
                        if state.arguments_from_step {
                            return Err(NormalizeError::ConflictingToolArguments);
                        }
                        append_bounded(
                            &mut state.arguments,
                            fragment,
                            self.stream_budget.max_tool_argument_bytes.get(),
                        )?;
                        events.push(ModelStreamEvent::ToolCallArgumentsDelta {
                            output_index: index,
                            call_id: state.call_id.clone(),
                            delta: fragment.into(),
                        });
                    }
                    "signature_delta" => {
                        let signature = required_text_field(delta, "signature")?;
                        self.ensure_reasoning_capacity(signature.len())?;
                        let ReasoningReplayState::AnthropicThinking {
                            signature: replay, ..
                        } = self
                            .reasoning_replay
                            .get_mut(&index)
                            .ok_or(NormalizeError::UnknownReasoningBlock)?
                        else {
                            return Err(NormalizeError::UnknownReasoningBlock);
                        };
                        replay.push_str(signature);
                    }
                    _ => return Err(NormalizeError::UnknownDeltaType),
                }
            }
            "content_block_stop" => {
                let index = u32_field_default(object, "index", 0)?;
                if self.reasoning_replay.contains_key(&index)
                    && let Some(event) = self.complete_reasoning_replay_item(index)?
                {
                    events.push(event);
                }
                if self.tools.contains_key(&index) {
                    events.push(self.complete_tool(index)?);
                }
            }
            "message_delta" => {
                if let Some(delta) = object.get("delta").and_then(Value::as_object)
                    && let Some(reason) = optional_text(delta.get("stop_reason"))?
                {
                    self.pending_stop = Some(stop_reason(reason, self.format)?);
                }
                if let Some(usage) = object.get("usage") {
                    let usage = anthropic_usage(usage, Some(self.latest_usage))?;
                    self.latest_usage = usage;
                    events.push(ModelStreamEvent::Usage(usage));
                }
            }
            "message_stop" => {
                if self.pending_stop.is_none() && !self.tools.is_empty() {
                    self.pending_stop = Some(StopReason::ToolCalls);
                }
                events.extend(self.complete_terminal()?);
            }
            "ping" => {}
            "error" => return self.provider_error(&value, ProviderErrorKind::Server),
            _ => return Err(NormalizeError::UnknownEventType),
        }
        Ok(events)
    }

    fn bedrock(
        &mut self,
        event_name: Option<&str>,
        value: Value,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let object = json_object(&value)?;
        let event_type = object
            .get("type")
            .or(object.get("event"))
            .and_then(Value::as_str)
            .or(event_name)
            .ok_or(NormalizeError::MissingEventType)?;
        let mut events = Vec::new();
        match event_type {
            "messageStart" | "message_start" => events.extend(self.ensure_started()),
            "contentBlockStart" | "content_block_start" => {
                let index = u32_field_default_any(object, &["contentBlockIndex", "index"], 0)?;
                let start = object
                    .get("start")
                    .or(object.get("content_block"))
                    .map(json_object)
                    .transpose()?
                    .unwrap_or(object);
                if let Some(tool) = start
                    .get("toolUse")
                    .or(start.get("tool_use"))
                    .and_then(Value::as_object)
                {
                    self.tools.insert(
                        index,
                        tool_state(
                            optional_text(tool.get("toolUseId").or(tool.get("id")))?,
                            optional_text(tool.get("name"))?,
                            index,
                            None,
                            None,
                            self.tool_call_scope.as_deref(),
                        )?,
                    );
                    events.extend(self.ensure_started());
                    events.extend(self.start_tool(index)?);
                }
                if let Some(reasoning) = start
                    .get("reasoningContent")
                    .or(start.get("reasoning_content"))
                    .and_then(Value::as_object)
                {
                    let text = optional_text(reasoning.get("text"))?
                        .unwrap_or_default()
                        .to_owned();
                    let signature = optional_text(reasoning.get("signature"))?
                        .unwrap_or_default()
                        .to_owned();
                    self.reasoning_replay
                        .insert(index, ReasoningReplayState::Bedrock { text, signature });
                    events.extend(self.ensure_started());
                }
            }
            "contentBlockDelta" | "content_block_delta" => {
                let block = object
                    .get("contentBlockDelta")
                    .map(json_object)
                    .transpose()?
                    .unwrap_or(object);
                let index = u32_field_default_any(
                    block,
                    &["contentBlockIndex", "index"],
                    u32_field_default(object, "index", 0)?,
                )?;
                let delta = object_required(block, "delta")?;
                if let Some(text) = non_empty_text(delta.get("text"))? {
                    events.extend(self.ensure_started());
                    events.push(text_delta(index, text)?);
                }
                if let Some(reasoning_text) = bedrock_reasoning_text(delta)? {
                    self.ensure_reasoning_capacity(reasoning_text.len())?;
                    let signature_fragment = delta
                        .get("reasoningContent")
                        .or(delta.get("reasoning_content"))
                        .and_then(Value::as_object)
                        .map(|reasoning| optional_text(reasoning.get("signature")))
                        .transpose()?
                        .flatten();
                    if let Some(fragment) = signature_fragment {
                        self.ensure_reasoning_capacity(fragment.len())?;
                    }
                    let state = self.reasoning_replay.entry(index).or_insert_with(|| {
                        ReasoningReplayState::Bedrock {
                            text: String::new(),
                            signature: String::new(),
                        }
                    });
                    let ReasoningReplayState::Bedrock { text, signature } = state else {
                        return Err(NormalizeError::UnknownReasoningBlock);
                    };
                    text.push_str(reasoning_text);
                    if let Some(fragment) = signature_fragment {
                        signature.push_str(fragment);
                    }
                    events.extend(self.ensure_started());
                    events.extend(self.disclosed_reasoning(index, reasoning_text)?);
                }
                if let Some(tool) = delta
                    .get("toolUse")
                    .or(delta.get("tool_use"))
                    .and_then(Value::as_object)
                {
                    let fragment = required_text_field(tool, "input")?;
                    let state = self
                        .tools
                        .get_mut(&index)
                        .ok_or(NormalizeError::UnknownTool)?;
                    append_bounded(
                        &mut state.arguments,
                        fragment,
                        self.stream_budget.max_tool_argument_bytes.get(),
                    )?;
                    events.push(ModelStreamEvent::ToolCallArgumentsDelta {
                        output_index: index,
                        call_id: state.call_id.clone(),
                        delta: fragment.into(),
                    });
                }
            }
            "contentBlockStop" | "content_block_stop" => {
                let index = u32_field_default_any(object, &["contentBlockIndex", "index"], 0)?;
                if self.reasoning_replay.contains_key(&index)
                    && let Some(event) = self.complete_reasoning_replay_item(index)?
                {
                    events.push(event);
                }
                if self.tools.contains_key(&index) {
                    events.push(self.complete_tool(index)?);
                }
            }
            "metadata" => {
                if let Some(usage) = object.get("usage") {
                    events.push(ModelStreamEvent::Usage(bedrock_usage(usage)?));
                }
            }
            "messageStop" | "message_stop" => {
                self.pending_stop = object
                    .get("stopReason")
                    .or(object.get("stop_reason"))
                    .map(required_text)
                    .transpose()?
                    .map(|reason| stop_reason(reason, self.format))
                    .transpose()?
                    .or(Some(StopReason::Stop));
                events.extend(self.complete_terminal()?);
            }
            "internalServerException"
            | "modelStreamErrorException"
            | "modelTimeoutException"
            | "throttlingException"
            | "validationException"
            | "serviceUnavailableException"
            | "error" => return self.provider_error(&value, bedrock_error_kind(event_type)),
            _ => return Err(NormalizeError::UnknownEventType),
        }
        Ok(events)
    }

    fn disclosed_reasoning(
        &self,
        index: u32,
        value: &str,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let mut events = vec![reasoning(index, value)?];
        if self.publish_disclosed_reasoning {
            events.push(reasoning_summary(index, value)?);
        }
        Ok(events)
    }

    fn start_tool(&mut self, index: u32) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let state = self
            .tools
            .get_mut(&index)
            .ok_or(NormalizeError::UnknownTool)?;
        if state.started {
            return Ok(Vec::new());
        }
        state.started = true;
        let name = match &self.tool_names {
            Some(names) => match names.get(&state.name) {
                Some(canonical) => canonical.clone(),
                None if names.values().any(|canonical| canonical == &state.name) => {
                    return Ok(vec![ModelStreamEvent::ToolCallUnexposed {
                        name: state.name.clone(),
                    }]);
                }
                // Preserve genuinely unknown model names for the Loop's bounded
                // tool_not_exposed correction; no such name can select a tool.
                None => state.name.clone(),
            },
            None => state.name.clone(),
        };
        Ok(vec![ModelStreamEvent::ToolCallStarted {
            output_index: index,
            call_id: state.call_id.clone(),
            name,
            provider_item_ref: state.provider_item_ref.clone(),
            provider_replay: state.provider_replay.clone(),
        }])
    }

    fn complete_reasoning_replay(&mut self) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let indexes = self.reasoning_replay.keys().copied().collect::<Vec<_>>();
        let mut events = Vec::with_capacity(indexes.len());
        for index in indexes {
            if let Some(event) = self.complete_reasoning_replay_item(index)? {
                events.push(event);
            }
        }
        Ok(events)
    }

    fn complete_reasoning_replay_item(
        &mut self,
        index: u32,
    ) -> Result<Option<ModelStreamEvent>, NormalizeError> {
        let state = self
            .reasoning_replay
            .remove(&index)
            .ok_or(NormalizeError::UnknownReasoningBlock)?;
        let payload = match state {
            ReasoningReplayState::OpenAiResponses { item }
            | ReasoningReplayState::Google { item } => item,
            ReasoningReplayState::AnthropicThinking {
                thinking,
                signature,
            } => {
                // Some Anthropic-compatible endpoints and interrupted streams
                // expose thinking text without the provider signature. The
                // public reasoning deltas remain valid conversation truth, but
                // an unsigned block is not valid route-private replay state.
                if thinking.is_empty() || signature.is_empty() {
                    return Ok(None);
                }
                serde_json::json!({
                    "type": "thinking",
                    "thinking": thinking,
                    "signature": signature,
                })
            }
            ReasoningReplayState::AnthropicRedacted { data } => {
                serde_json::json!({"type": "redacted_thinking", "data": data})
            }
            ReasoningReplayState::Bedrock { text, signature } => {
                if text.is_empty() {
                    return Err(NormalizeError::IncompleteReasoningReplay);
                }
                let mut reasoning_text = serde_json::json!({"text": text});
                if !signature.is_empty() {
                    reasoning_text["signature"] = Value::String(signature);
                }
                serde_json::json!({"reasoningContent": {"reasoningText": reasoning_text}})
            }
        };
        let payload = if matches!(
            self.format,
            ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions
        ) {
            google::bounded_private_payload(payload)?
        } else {
            payload
        };
        self.reasoning_replay_event(index, serde_to_canonical(&payload)?)
            .map(Some)
    }

    fn capture_openai_response_reasoning(
        &mut self,
        response: &serde_json::Map<String, Value>,
    ) -> Result<(), NormalizeError> {
        let Some(output) = response.get("output").and_then(Value::as_array) else {
            return Ok(());
        };
        for (index, item) in output.iter().enumerate() {
            let Some(item) = item.as_object() else {
                continue;
            };
            if item.get("type").and_then(Value::as_str) != Some("reasoning") {
                continue;
            }
            let index = u32::try_from(index).map_err(|_| NormalizeError::IndexOverflow)?;
            // The terminal response is authoritative. Azure OpenAI may omit
            // encrypted_content from output_item.done and provide it only here.
            self.reasoning_replay.insert(
                index,
                ReasoningReplayState::OpenAiResponses {
                    item: Value::Object(item.clone()),
                },
            );
        }
        Ok(())
    }

    fn reasoning_replay_event(
        &self,
        output_index: u32,
        payload: JsonValue,
    ) -> Result<ModelStreamEvent, NormalizeError> {
        let item = ProviderReasoningReplayItem {
            source_api_format: self.format,
            source_model_key: self.source_model_key.as_str().to_owned(),
            payload,
        };
        item.validate()
            .map_err(|_| NormalizeError::InvalidReasoningReplay)?;
        Ok(ModelStreamEvent::ProviderReasoningReplay { output_index, item })
    }

    fn provider_error(
        &mut self,
        value: &Value,
        default_kind: ProviderErrorKind,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        // An explicit context-capacity error inside an HTTP 200 stream is a
        // distinct deterministic classification, not a generic server or
        // invalid-request failure.
        if let Some(overflow) = overflow::classify_stream_error_event(self.format, value) {
            tracing::warn!(
                provider.failure = "stream_context_overflow",
                provider.code = overflow.provider_code,
                "Model Provider explicitly rejected the request as exceeding the context window"
            );
            self.terminal = true;
            return Ok(vec![ModelStreamEvent::ResponseError(ProviderError {
                kind: ProviderErrorKind::InvalidRequest,
                safe_code: ProviderErrorCode::ContextWindowExceeded,
                retryable: false,
                retry_after_ms: None,
                diagnostic_ref: self.response_ref.clone(),
            })]);
        }
        let metadata = crate::failure_metadata::FailureMetadata::from_body(value);
        if let Some((kind, safe_code, retryable)) = metadata.classify(self.gateway, None) {
            self.terminal = true;
            return Ok(vec![ModelStreamEvent::ResponseError(ProviderError {
                kind,
                safe_code,
                retryable,
                retry_after_ms: metadata.retry_after_ms,
                diagnostic_ref: self.response_ref.clone(),
            })]);
        }
        let kind = provider_error_kind(value).unwrap_or(default_kind);
        self.terminal = true;
        Ok(vec![ModelStreamEvent::ResponseError(ProviderError {
            kind,
            safe_code: match kind {
                ProviderErrorKind::Authentication => ProviderErrorCode::AuthenticationFailed,
                ProviderErrorKind::Authorization => ProviderErrorCode::AuthorizationFailed,
                ProviderErrorKind::RateLimited => ProviderErrorCode::RateLimited,
                ProviderErrorKind::Quota => ProviderErrorCode::QuotaExceeded,
                ProviderErrorKind::InvalidRequest => ProviderErrorCode::InvalidRequest,
                ProviderErrorKind::Server => ProviderErrorCode::ProviderServer,
                ProviderErrorKind::Protocol => ProviderErrorCode::ProtocolViolation,
                ProviderErrorKind::Timeout => ProviderErrorCode::DeadlineExceeded,
                ProviderErrorKind::Transport => ProviderErrorCode::TransportFailed,
                ProviderErrorKind::Cancelled => ProviderErrorCode::Cancelled,
            },
            retryable: matches!(
                kind,
                ProviderErrorKind::RateLimited
                    | ProviderErrorKind::Server
                    | ProviderErrorKind::Timeout
                    | ProviderErrorKind::Transport
            ),
            retry_after_ms: None,
            diagnostic_ref: self.response_ref.clone(),
        })])
    }
}

/// Classifies only stable, non-sensitive error metadata emitted by Provider
/// protocols or by Musteragent's shared LLM gateway. Error messages deliberately do
/// not cross the canonical boundary.
///
/// A streamed HTTP response has already returned `200 OK` before an upstream
/// failure can occur, so transport status is carried inside the terminal SSE
/// payload. Ignoring that nested status turns deterministic 4xx failures into
/// retryable server failures and makes every Agent Loop repeat an invalid
/// request. This classifier is shared by ordinary conversations, maintenance
/// agents, and every other model consumer.
fn provider_error_kind(value: &Value) -> Option<ProviderErrorKind> {
    provider_error_status(value, 0)
        .and_then(provider_error_kind_from_status)
        .or_else(|| provider_error_code_kind(value, 0))
}

fn provider_error_status(value: &Value, depth: u8) -> Option<u16> {
    if depth > 4 {
        return None;
    }
    let object = value.as_object()?;
    for key in ["upstream_status", "status_code", "http_status"] {
        if let Some(status) = object.get(key).and_then(provider_status_number) {
            return Some(status);
        }
    }
    // Google and a few OpenAI-compatible gateways encode the HTTP status as a
    // numeric `code`. String codes remain semantic codes and are handled below.
    if let Some(status) = object.get("code").and_then(Value::as_u64)
        && let Ok(status) = u16::try_from(status)
    {
        return Some(status);
    }
    for key in ["error", "response", "details", "cause"] {
        if let Some(status) = object
            .get(key)
            .and_then(|nested| provider_error_status(nested, depth.saturating_add(1)))
        {
            return Some(status);
        }
    }
    None
}

fn provider_status_number(value: &Value) -> Option<u16> {
    value
        .as_u64()
        .and_then(|status| u16::try_from(status).ok())
        .or_else(|| value.as_str()?.parse::<u16>().ok())
}

const fn provider_error_kind_from_status(status: u16) -> Option<ProviderErrorKind> {
    match status {
        400 | 404 | 405 | 409 | 410 | 413 | 415 | 422 => Some(ProviderErrorKind::InvalidRequest),
        401 => Some(ProviderErrorKind::Authentication),
        402 => Some(ProviderErrorKind::Quota),
        403 => Some(ProviderErrorKind::Authorization),
        408 => Some(ProviderErrorKind::Timeout),
        429 => Some(ProviderErrorKind::RateLimited),
        500..=599 => Some(ProviderErrorKind::Server),
        _ => None,
    }
}

fn provider_error_code_kind(value: &Value, depth: u8) -> Option<ProviderErrorKind> {
    if depth > 4 {
        return None;
    }
    let object = value.as_object()?;
    for key in ["code", "type"] {
        if let Some(kind) = object
            .get(key)
            .and_then(Value::as_str)
            .and_then(provider_error_kind_from_code)
        {
            return Some(kind);
        }
    }
    for key in ["error", "response", "details", "cause"] {
        if let Some(kind) = object
            .get(key)
            .and_then(|nested| provider_error_code_kind(nested, depth.saturating_add(1)))
        {
            return Some(kind);
        }
    }
    None
}

fn provider_error_kind_from_code(code: &str) -> Option<ProviderErrorKind> {
    match code.trim().to_ascii_lowercase().as_str() {
        "upstream_request_failed"
        | "invalid_request_error"
        | "invalid_argument"
        | "bad_request"
        | "validation_error"
        | "validationexception" => Some(ProviderErrorKind::InvalidRequest),
        "upstream_auth_failed" | "authentication_error" | "unauthenticated" | "unauthorized" => {
            Some(ProviderErrorKind::Authentication)
        }
        "authorization_error" | "permission_error" | "permission_denied" | "forbidden" => {
            Some(ProviderErrorKind::Authorization)
        }
        "upstream_rate_limited" | "rate_limit_error" | "rate_limited" | "throttlingexception" => {
            Some(ProviderErrorKind::RateLimited)
        }
        "quota_exceeded" | "insufficient_quota" => Some(ProviderErrorKind::Quota),
        "upstream_stream_idle_timeout" | "timeout" | "modeltimeoutexception" => {
            Some(ProviderErrorKind::Timeout)
        }
        "upstream_unavailable"
        | "overloaded_error"
        | "server_error"
        | "serviceunavailableexception"
        | "internalserverexception" => Some(ProviderErrorKind::Server),
        _ => None,
    }
}
