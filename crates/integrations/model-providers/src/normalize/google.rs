//! Native Google stream codecs and private replay.
use super::*;

impl ProviderNormalizer {
    pub(super) fn google_generate(
        &mut self,
        value: Value,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let object = json_object(&value)?;
        if let Some(error) = object.get("error") {
            return self.provider_error(error, ProviderErrorKind::Server);
        }
        let starts_hop = object
            .get("candidates")
            .and_then(Value::as_array)
            .is_some_and(|candidates| {
                candidates.iter().any(|candidate| {
                    candidate.get("content").is_some()
                        || candidate
                            .get("finishReason")
                            .and_then(Value::as_str)
                            .is_some_and(|reason| reason != "CONTINUATION")
                })
            });
        self.google_response_ref(object.get("responseId"), starts_hop)?;
        self.capture_response_model_key(
            object.get("modelVersion").or(object.get("model_version")),
        )?;
        let mut events = self.ensure_started();
        let mut invalid_generation = false;
        if let Some(candidates) = object.get("candidates") {
            for (candidate_index, candidate) in array(candidates)?.iter().enumerate() {
                let candidate = json_object(candidate)?;
                if let Some(content) = candidate.get("content").and_then(Value::as_object)
                    && let Some(parts) = content.get("parts")
                {
                    for (part_index, part) in array(parts)?.iter().enumerate() {
                        let index =
                            self.google_index(google_output_index(candidate_index, part_index)?)?;
                        self.capture_google_part(part)?;
                        let part = json_object(part)?;
                        if part.get("thought").and_then(Value::as_bool) == Some(true) {
                            if let Some(text) = non_empty_text(part.get("text"))? {
                                // GenerateContent exposes provider-authored thought summaries,
                                // not a public chain-of-thought channel.
                                events.push(reasoning_summary(index, text)?);
                            }
                        } else if let Some(text) = non_empty_text(part.get("text"))? {
                            events.push(text_delta(index, text)?);
                        } else if let Some(call) = part
                            .get("functionCall")
                            .or(part.get("function_call"))
                            .and_then(Value::as_object)
                        {
                            let signature = optional_text(
                                part.get("thoughtSignature")
                                    .or(part.get("thought_signature")),
                            )?;
                            events.extend(self.google_tool(index, call, signature)?);
                        } else if part.contains_key("thoughtSignature")
                            || part.contains_key("thought_signature")
                        {
                            // Gemini may emit a metadata-only final Part whose sole purpose is
                            // carrying the thought signature. It has no public output delta.
                        } else if part.get("text").and_then(Value::as_str) == Some("") {
                            // Google emits an empty final text Part after tool calls.
                        } else {
                            return Err(NormalizeError::UnknownContentBlock);
                        }
                    }
                }
                if let Some(reason) = optional_text(
                    candidate
                        .get("finishReason")
                        .or(candidate.get("finish_reason")),
                )? {
                    if reason == "CONTINUATION" {
                        self.google_continue(
                            candidate
                                .get("continuationToken")
                                .or(candidate.get("continuation_token")),
                            None,
                        )?;
                        continue;
                    }
                    if matches!(
                        reason,
                        "MALFORMED_RESPONSE" | "MALFORMED_FUNCTION_CALL" | "UNEXPECTED_TOOL_CALL"
                    ) {
                        invalid_generation = true;
                        continue;
                    }
                    let reason = stop_reason(reason, self.format)?;
                    self.pending_stop =
                        Some(if reason == StopReason::Stop && !self.tools.is_empty() {
                            StopReason::ToolCalls
                        } else {
                            reason
                        });
                }
            }
        }
        if let Some(usage) = object.get("usageMetadata") {
            self.google.current_usage = Some(google_usage(usage)?);
        }
        // Cache attribution may arrive only in the final frame. An intermediate
        // snapshot can overstate uncached input, so publish settled usage per hop.
        if (invalid_generation || self.pending_stop.is_some() || self.google.continuation.is_some())
            && let Some(usage) = self.google.current_usage.take()
        {
            self.google.completed_usage = google_usage_sum(self.google.completed_usage, usage)?;
            events.push(ModelStreamEvent::Usage(self.google.completed_usage));
        }
        if invalid_generation {
            self.terminal = true;
            events.push(ModelStreamEvent::ResponseError(ProviderError {
                kind: ProviderErrorKind::Protocol,
                safe_code: ProviderErrorCode::ProtocolViolation,
                retryable: false,
                retry_after_ms: None,
                diagnostic_ref: self.response_ref.clone(),
            }));
            return Ok(events);
        }
        if self.pending_stop.is_some() {
            self.seal_google_content()?;
            events.extend(self.complete_terminal()?);
        }
        Ok(events)
    }

    fn google_tool(
        &mut self,
        index: u32,
        call: &serde_json::Map<String, Value>,
        thought_signature: Option<&str>,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let call_id = optional_text(call.get("id"))?
            .map(str::to_owned)
            .unwrap_or_else(|| format!("google-{}-{index}", self.fallback_response_ref.as_str()));
        let name = required_text_field(call, "name")?;
        let arguments = call
            .get("args")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let arguments_text =
            serde_json::to_string(&arguments).map_err(|_| NormalizeError::InvalidJson)?;
        if let Some(existing) = self.tools.get(&index) {
            if existing.call_id.as_str() == call_id
                && existing.name.as_str() == name
                && existing.completed
            {
                return Ok(Vec::new());
            }
            return Err(NormalizeError::ToolIdentityChanged);
        }
        let provider_replay = thought_signature
            .map(|signature| {
                ProviderToolCallReplay::google_thought_signature(
                    self.source_route_ref.clone(),
                    self.format,
                    self.source_model_key.as_str().to_owned(),
                    signature,
                )
                .map_err(|_| NormalizeError::InvalidProviderReplay)
            })
            .transpose()?;
        self.tools.insert(
            index,
            tool_state(
                Some(&call_id),
                Some(name),
                index,
                None,
                provider_replay,
                self.tool_call_scope.as_deref(),
            )?,
        );
        let mut events = self.start_tool(index)?;
        let state = self
            .tools
            .get_mut(&index)
            .ok_or(NormalizeError::UnknownTool)?;
        state.arguments = arguments_text.clone();
        state.argument_source = musteragent_runtime_model::ArgumentSource::InitialSnapshot;
        events.push(ModelStreamEvent::ToolCallArgumentsDelta {
            output_index: index,
            call_id: state.call_id.clone(),
            delta: arguments_text,
        });
        events.push(self.complete_tool(index)?);
        Ok(events)
    }

    pub(super) fn google_interactions(
        &mut self,
        event_name: Option<&str>,
        value: Value,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let object = json_object(&value)?;
        let event_type = object
            .get("event_type")
            .or(object.get("type"))
            .and_then(Value::as_str)
            .or(event_name)
            .ok_or(NormalizeError::MissingEventType)?;
        if let Some(interaction) = object.get("interaction").and_then(Value::as_object) {
            self.google_response_ref(interaction.get("id"), event_type == "interaction.created")?;
            self.capture_response_model_key(
                interaction
                    .get("model")
                    .or(interaction.get("modelVersion"))
                    .or(interaction.get("model_version")),
            )?;
        }
        let observed_usage = object
            .get("metadata")
            .and_then(Value::as_object)
            .and_then(|metadata| metadata.get("total_usage"))
            .or_else(|| object.get("usage"))
            .or_else(|| {
                object
                    .get("interaction")
                    .and_then(Value::as_object)
                    .and_then(|interaction| interaction.get("usage"))
            });
        if let Some(usage) = observed_usage {
            self.interaction_usage = Some(interaction_usage(usage)?);
        }
        let mut events = Vec::new();
        match event_type {
            "interaction.created" => {
                events.extend(self.ensure_started());
            }
            "step.start" => {
                let index = self.google_index(u32_field_default(object, "index", 0)?)?;
                let step = object_required(object, "step")?;
                self.capture_google_step(index, Value::Object(step.clone()))?;
                match required_text_field(step, "type")? {
                    "function_call" => {
                        let call_id = required_text_field(step, "id")?;
                        let empty_arguments = serde_json::json!({});
                        let arguments = step.get("arguments").unwrap_or(&empty_arguments);
                        self.tools.insert(
                            index,
                            tool_state(
                                Some(call_id),
                                Some(required_text_field(step, "name")?),
                                index,
                                Some(provider_ref(call_id)?),
                                None,
                                self.tool_call_scope.as_deref(),
                            )?,
                        );
                        events.extend(self.ensure_started());
                        events.extend(self.start_tool(index)?);
                        if arguments
                            .as_object()
                            .is_none_or(|object| !object.is_empty())
                        {
                            let arguments = arguments.to_string();
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
                        self.interaction_steps
                            .insert(index, InteractionStepKind::FunctionCall);
                    }
                    "compaction" | "context_compaction" => {
                        return Err(NormalizeError::NativeCompactionStoreRequired);
                    }
                    "thought" => {
                        self.interaction_steps
                            .insert(index, InteractionStepKind::Thought);
                        events.extend(self.ensure_started());
                        if let Some(summary) = step.get("summary") {
                            events.extend(interaction_thought_summary(index, summary)?);
                        }
                    }
                    "model_output" => {
                        events.extend(self.ensure_started());
                        if let Some(content) = step.get("content").and_then(Value::as_array) {
                            for part in content {
                                if part.get("type").and_then(Value::as_str) == Some("text")
                                    && let Some(text) = part.get("text").and_then(Value::as_str)
                                    && !text.is_empty()
                                {
                                    events.push(text_delta(index, text)?);
                                }
                            }
                        }
                        self.interaction_steps
                            .insert(index, InteractionStepKind::ModelOutput);
                        events.extend(self.ensure_started());
                    }
                    "user_input" | "function_result" => {
                        self.interaction_steps
                            .insert(index, InteractionStepKind::Input);
                        events.extend(self.ensure_started());
                    }
                    _ => return Err(NormalizeError::UnknownContentBlock),
                }
            }
            "step.delta" => {
                let index = self.google_index(u32_field_default(object, "index", 0)?)?;
                let delta = object_required(object, "delta")?;
                self.capture_google_step_delta(index, delta)?;
                match required_text_field(delta, "type")? {
                    "text" => {
                        let text = required_text_field(delta, "text")?;
                        events.push(
                            if self.interaction_steps.get(&index)
                                == Some(&InteractionStepKind::Thought)
                            {
                                reasoning_summary(index, text)?
                            } else {
                                text_delta(index, text)?
                            },
                        );
                    }
                    "thought_summary" => {
                        if let Some(content) = delta.get("content") {
                            events.extend(interaction_thought_summary_content(index, content)?);
                        }
                    }
                    "thought_signature" => {
                        // The signature is opaque Provider state. Stateful
                        // continuation keeps it server-side; it must never
                        // become public reasoning output.
                        optional_text(delta.get("signature"))?;
                    }
                    "arguments_delta" => {
                        let Some(fragment) = optional_text(delta.get("arguments"))? else {
                            return Ok(events);
                        };
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
                    _ => return Err(NormalizeError::UnknownDeltaType),
                }
            }
            "step.stop" => {
                let index = self.google_index(u32_field_default(object, "index", 0)?)?;
                if let Some(step) = object.get("step") {
                    events.extend(self.reconcile_google_step_snapshot(index, step)?);
                    self.capture_google_step(index, step.clone())?;
                }
                self.seal_google_step(index)?;
                if self.tools.contains_key(&index) {
                    let state = self
                        .tools
                        .get_mut(&index)
                        .ok_or(NormalizeError::UnknownTool)?;
                    if state.arguments.is_empty() {
                        state.arguments = "{}".to_owned();
                    }
                    events.push(self.complete_tool(index)?);
                }
            }
            "interaction.completed" => {
                let status = object
                    .get("interaction")
                    .and_then(Value::as_object)
                    .and_then(|interaction| interaction.get("status"))
                    .and_then(Value::as_str)
                    .ok_or(NormalizeError::MissingField("status"))?;
                if let Some(steps) = object
                    .get("interaction")
                    .and_then(|v| v.get("steps"))
                    .and_then(Value::as_array)
                {
                    for (index, step) in steps.iter().enumerate() {
                        let index = self.google_index(
                            u32::try_from(index).map_err(|_| NormalizeError::IndexOverflow)?,
                        )?;
                        self.capture_google_step(index, step.clone())?;
                    }
                }
                match status {
                    "requires_continuation" => {
                        let interaction = object_required(object, "interaction")?;
                        self.google_continue(
                            interaction.get("continuation_token"),
                            interaction.get("id").and_then(Value::as_str),
                        )?;
                        if let Some(usage) = self.interaction_usage.take() {
                            self.google.completed_usage =
                                google_usage_sum(self.google.completed_usage, usage)?;
                            events.push(ModelStreamEvent::Usage(self.google.completed_usage));
                        }
                    }
                    "completed" => {
                        self.pending_stop = Some(if self.tools.is_empty() {
                            StopReason::Stop
                        } else {
                            StopReason::ToolCalls
                        });
                        events.extend(self.complete_interaction_terminal()?);
                    }
                    "requires_action" => {
                        self.pending_stop = Some(StopReason::ToolCalls);
                        events.extend(self.complete_interaction_terminal()?);
                    }
                    "incomplete" | "budget_exceeded" => {
                        self.pending_stop = Some(StopReason::MaxOutputTokens);
                        events.extend(self.complete_interaction_terminal()?);
                    }
                    "cancelled" => {
                        self.pending_stop = Some(StopReason::Cancelled);
                        events.extend(self.complete_interaction_terminal()?);
                    }
                    "failed" => {
                        return self.provider_error(&value, ProviderErrorKind::Server);
                    }
                    _ => return Err(NormalizeError::UnknownStopReason),
                }
            }
            "interaction.status_update" => {
                match required_text_field(object, "status")? {
                    "in_progress"
                    | "queued"
                    | "requires_action"
                    | "completed"
                    | "failed"
                    | "cancelled"
                    | "incomplete"
                    | "budget_exceeded"
                    | "requires_continuation" => {}
                    _ => return Err(NormalizeError::UnknownStopReason),
                }
                events.extend(self.ensure_started());
            }
            "error" => {
                return self.provider_error(&value, ProviderErrorKind::Server);
            }
            _ => return Err(NormalizeError::UnknownEventType),
        }
        Ok(events)
    }
}

/// Private continuation proof. Debug deliberately omits opaque provider state.
#[derive(Clone)]
pub(crate) struct GoogleContinuation {
    pub(crate) token: String,
    pub(crate) interaction_id: Option<String>,
}
impl std::fmt::Debug for GoogleContinuation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GoogleContinuation([private])")
    }
}

#[derive(Debug, Default)]
pub(super) struct GoogleState {
    parts: Vec<Value>,
    parts_bytes: usize,
    pub(super) replay_sizes: BTreeMap<u32, usize>,
    continuation: Option<GoogleContinuation>,
    awaiting_hop: bool,
    index_offset: u32,
    next_index: u32,
    hops: u32,
    pub(super) completed_usage: ModelUsage,
    current_usage: Option<ModelUsage>,
}

impl ProviderNormalizer {
    pub(crate) fn take_google_continuation(&mut self) -> Option<GoogleContinuation> {
        let proof = self.google.continuation.take()?;
        self.google.awaiting_hop = true;
        Some(proof)
    }

    fn google_response_ref(
        &mut self,
        value: Option<&Value>,
        starts_hop: bool,
    ) -> Result<(), NormalizeError> {
        // A new identity is legal only after an explicit continuation terminal.
        if self.google.continuation.is_some() || self.google.awaiting_hop {
            let changed = value.and_then(Value::as_str).is_some_and(|id| {
                self.response_ref
                    .as_ref()
                    .is_none_or(|previous| previous.as_str() != id)
            });
            if changed || self.google.awaiting_hop || starts_hop {
                self.google.continuation = None;
                self.google.awaiting_hop = false;
                self.google.index_offset = self.google.next_index;
                self.response_ref = None;
                self.pending_stop = None;
                self.interaction_usage = None;
            }
        }
        self.capture_response_ref(value)
    }

    fn google_index(&mut self, local: u32) -> Result<u32, NormalizeError> {
        let index = self
            .google
            .index_offset
            .checked_add(local)
            .ok_or(NormalizeError::IndexOverflow)?;
        self.google.next_index = self
            .google
            .next_index
            .max(index.checked_add(1).ok_or(NormalizeError::IndexOverflow)?);
        Ok(index)
    }

    fn google_continue(
        &mut self,
        token: Option<&Value>,
        id: Option<&str>,
    ) -> Result<(), NormalizeError> {
        let token = optional_text(token)?
            .filter(|token| !token.is_empty())
            .ok_or(NormalizeError::MissingField("continuation_token"))?;
        if token.len() > self.stream_budget.max_reasoning_replay_bytes.get()
            || !self.tools.is_empty()
        {
            return Err(NormalizeError::InvalidReasoningReplay);
        }
        self.google.hops = self
            .google
            .hops
            .checked_add(1)
            .filter(|hops| *hops <= 64)
            .ok_or(NormalizeError::StreamBudgetExceeded)?;
        self.google.continuation = Some(GoogleContinuation {
            token: token.to_owned(),
            interaction_id: id.map(str::to_owned),
        });
        Ok(())
    }

    fn capture_google_part(&mut self, part: &Value) -> Result<(), NormalizeError> {
        let part = part.as_object().ok_or(NormalizeError::InvalidJson)?;
        // Adjacent streaming text fragments are one native Part; signatures
        // remain attached to precisely the Part on which Google supplied them.
        let additional = serde_json::to_vec(part)
            .map_err(|_| NormalizeError::InvalidJson)?
            .len();
        let bytes = self
            .google
            .parts_bytes
            .checked_add(additional)
            .ok_or(NormalizeError::StreamBudgetExceeded)?;
        self.ensure_reasoning_capacity(bytes)?;
        self.google.parts_bytes = bytes;
        let mut merged = false;
        if let Some(text) = part.get("text").and_then(Value::as_str)
            && let Some(previous) = self.google.parts.last_mut().and_then(Value::as_object_mut)
            && previous.get("thought") == part.get("thought")
            && !previous.contains_key("thoughtSignature")
            && !previous.contains_key("thought_signature")
            && let Some(Value::String(prior)) = previous.get_mut("text")
        {
            prior.push_str(text);
            for (key, value) in part {
                if key != "text" {
                    previous.insert(key.clone(), value.clone());
                }
            }
            merged = true;
        }
        if !merged {
            self.google.parts.push(Value::Object(part.clone()));
        }
        Ok(())
    }

    fn seal_google_content(&mut self) -> Result<(), NormalizeError> {
        if self.google.parts.is_empty() {
            return Ok(());
        }
        let parts = std::mem::take(&mut self.google.parts);
        self.google.replay_sizes.insert(0, self.google.parts_bytes);
        self.reasoning_replay.insert(
            0,
            ReasoningReplayState::Google {
                item: serde_json::json!({"type":"google_content", "parts":parts}),
            },
        );
        Ok(())
    }

    fn capture_google_step(&mut self, index: u32, step: Value) -> Result<(), NormalizeError> {
        if matches!(
            step.get("type").and_then(Value::as_str),
            Some("thought" | "model_output" | "function_call")
        ) {
            let bytes = serde_json::to_vec(&step)
                .map_err(|_| NormalizeError::InvalidJson)?
                .len();
            self.google.replay_sizes.insert(index, bytes);
            self.reasoning_replay
                .insert(index, ReasoningReplayState::Google { item: step });
        }
        Ok(())
    }

    fn capture_google_step_delta(
        &mut self,
        index: u32,
        delta: &serde_json::Map<String, Value>,
    ) -> Result<(), NormalizeError> {
        let additional = serde_json::to_vec(delta)
            .map_err(|_| NormalizeError::InvalidJson)?
            .len();
        self.ensure_reasoning_capacity(additional)?;
        if let Some(size) = self.google.replay_sizes.get_mut(&index) {
            *size = size
                .checked_add(additional)
                .ok_or(NormalizeError::StreamBudgetExceeded)?;
        }
        let Some(ReasoningReplayState::Google { item }) = self.reasoning_replay.get_mut(&index)
        else {
            return Ok(());
        };
        match delta.get("type").and_then(Value::as_str) {
            Some("text") => {
                let text = required_text_field(delta, "text")?;
                let content = item
                    .as_object_mut()
                    .ok_or(NormalizeError::InvalidJson)?
                    .entry("content")
                    .or_insert_with(|| serde_json::json!([]));
                let content = content.as_array_mut().ok_or(NormalizeError::InvalidJson)?;
                if let Some(Value::String(prior)) =
                    content.last_mut().and_then(|part| part.get_mut("text"))
                {
                    prior.push_str(text);
                } else {
                    content.push(serde_json::json!({"type":"text", "text":text}));
                }
            }
            Some("thought_signature") => {
                item["signature"] = delta
                    .get("signature")
                    .cloned()
                    .ok_or(NormalizeError::MissingField("signature"))?;
            }
            Some("thought_summary") => {
                if let Some(content) = delta.get("content") {
                    let summary = item
                        .as_object_mut()
                        .ok_or(NormalizeError::InvalidJson)?
                        .entry("summary")
                        .or_insert_with(|| serde_json::json!([]));
                    summary
                        .as_array_mut()
                        .ok_or(NormalizeError::InvalidJson)?
                        .push(content.clone());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn reconcile_google_step_snapshot(
        &mut self,
        index: u32,
        step: &Value,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let mut events = Vec::new();
        if let Some(tool) = self.tools.get_mut(&index) {
            if let Some(arguments) = step.get("arguments") {
                if !arguments.is_object() {
                    return Err(NormalizeError::ToolArgumentsNotObject);
                }
                if tool.arguments.is_empty() {
                    let text = serde_json::to_string(arguments)
                        .map_err(|_| NormalizeError::InvalidJson)?;
                    if text.len() > self.stream_budget.max_tool_argument_bytes.get() {
                        return Err(NormalizeError::StreamBudgetExceeded);
                    }
                    tool.arguments = text.clone();
                    tool.argument_source =
                        musteragent_runtime_model::ArgumentSource::InitialSnapshot;
                    events.push(ModelStreamEvent::ToolCallArgumentsDelta {
                        output_index: index,
                        call_id: tool.call_id.clone(),
                        delta: text,
                    });
                } else if serde_json::from_str::<Value>(&tool.arguments)
                    .map_err(|_| NormalizeError::MalformedToolJson)?
                    != *arguments
                {
                    return Err(NormalizeError::ConflictingToolArguments);
                }
            }
        } else if step.get("type").and_then(Value::as_str) == Some("model_output") {
            fn text(value: &Value) -> String {
                value
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
                    .filter_map(|part| part.get("text").and_then(Value::as_str))
                    .collect()
            }
            let prior = match self.reasoning_replay.get(&index) {
                Some(ReasoningReplayState::Google { item }) => text(item),
                _ => String::new(),
            };
            let complete = text(step);
            let suffix = complete
                .strip_prefix(&prior)
                .ok_or(NormalizeError::InvalidProviderReplay)?;
            for chunk in musteragent_runtime_model::ModelText::chunked(suffix.to_owned())
                .map_err(|_| NormalizeError::InvalidText)?
            {
                if !chunk.as_str().is_empty() {
                    events.push(ModelStreamEvent::TextDelta {
                        output_index: index,
                        delta: chunk,
                    });
                }
            }
        }
        Ok(events)
    }

    fn seal_google_step(&mut self, index: u32) -> Result<(), NormalizeError> {
        if let Some(tool) = self.tools.get(&index)
            && let Some(ReasoningReplayState::Google { item }) =
                self.reasoning_replay.get_mut(&index)
        {
            item["arguments"] = if tool.arguments.is_empty() {
                serde_json::json!({})
            } else {
                serde_json::from_str(&tool.arguments).map_err(|_| NormalizeError::InvalidJson)?
            };
        }
        Ok(())
    }
}

pub(super) fn google_usage_sum(
    left: ModelUsage,
    right: ModelUsage,
) -> Result<ModelUsage, NormalizeError> {
    fn add(a: u64, b: u64) -> Result<u64, NormalizeError> {
        a.checked_add(b).ok_or(NormalizeError::StreamBudgetExceeded)
    }
    Ok(ModelUsage {
        input_tokens_uncached: add(left.input_tokens_uncached, right.input_tokens_uncached)?,
        output_tokens: add(left.output_tokens, right.output_tokens)?,
        reasoning_tokens: add(left.reasoning_tokens, right.reasoning_tokens)?,
        provider_cache_read_tokens: add(
            left.provider_cache_read_tokens,
            right.provider_cache_read_tokens,
        )?,
        provider_cache_write_tokens: add(
            left.provider_cache_write_tokens,
            right.provider_cache_write_tokens,
        )?,
        provider_cache_write_1h_tokens: add(
            left.provider_cache_write_1h_tokens,
            right.provider_cache_write_1h_tokens,
        )?,
        total_tokens: match (left.total_tokens, right.total_tokens) {
            (Some(a), Some(b)) => Some(add(a, b)?),
            (None, value) | (value, None) => value,
        },
    })
}

/// Native Parts must keep their exact boundaries and signatures. Large native
/// strings are encoded as bounded private JSON chunks, then decoded only by the
/// same-route mapper; public ModelText and canonical Tool limits stay unchanged.
pub(super) fn bounded_private_payload(payload: Value) -> Result<Value, NormalizeError> {
    let encoded = serde_json::to_string(&payload).map_err(|_| NormalizeError::InvalidJson)?;
    if encoded.len() <= 1_048_576 {
        return Ok(payload);
    }
    let chunks = musteragent_runtime_model::ModelText::chunked(encoded)
        .map_err(|_| NormalizeError::InvalidReasoningReplay)?;
    Ok(
        serde_json::json!({"type":"google_native_chunks","wire_chunks":chunks.into_iter().map(|chunk|chunk.as_str().to_owned()).collect::<Vec<_>>()}),
    )
}
