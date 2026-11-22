use super::*;
use musteragent_runtime_model::{
    ArgumentDiagnostic, ArgumentSource, JsonParseCategory, JsonParseIssue,
};

impl ProviderNormalizer {
    pub(super) fn observe_response_argument_snapshots(
        &mut self,
        response: &serde_json::Map<String, Value>,
    ) -> Result<(), NormalizeError> {
        let Some(output) = response.get("output") else {
            return Ok(());
        };
        for (index, item) in array(output)?.iter().enumerate() {
            if !matches!(
                item.get("type").and_then(Value::as_str),
                Some("function_call" | "tool_call")
            ) {
                continue;
            }
            let index = u32::try_from(index).map_err(|_| NormalizeError::StreamBudgetExceeded)?;
            self.validate_response_tool_snapshot(
                index,
                item.as_object().ok_or(NormalizeError::InvalidJson)?,
            )?;
            if let Some(arguments) = item.get("arguments").map(string_value).transpose()? {
                self.observe_argument_snapshot(index, arguments, ArgumentSource::TerminalSnapshot)?;
            }
        }
        Ok(())
    }

    pub(super) fn validate_response_tool_snapshot(
        &self,
        index: u32,
        item: &serde_json::Map<String, Value>,
    ) -> Result<(), NormalizeError> {
        let state = self.tools.get(&index).ok_or(NormalizeError::UnknownTool)?;
        let call_id = item.get("call_id").map(string_value).transpose()?;
        let name = item.get("name").map(string_value).transpose()?;
        let id = item.get("id").map(string_value).transpose()?;
        let kind = item.get("type").map(string_value).transpose()?;
        if call_id.is_some_and(|id| id != state.provider_call_id)
            || name.is_some_and(|name| name != state.name.as_str())
            || id.is_some_and(|id| {
                state
                    .provider_item_ref
                    .as_ref()
                    .is_some_and(|reference| reference.as_str() != id)
            })
            || kind.is_some_and(|kind| !matches!(kind, "function_call" | "tool_call"))
        {
            return Err(NormalizeError::InvalidToolIdentity);
        }
        Ok(())
    }

    pub(super) fn observe_argument_snapshot(
        &mut self,
        index: u32,
        arguments: &str,
        source: ArgumentSource,
    ) -> Result<(), NormalizeError> {
        if arguments.len() > self.stream_budget.max_tool_argument_bytes.get() {
            return Err(NormalizeError::StreamBudgetExceeded);
        }
        let state = self
            .tools
            .get_mut(&index)
            .ok_or(NormalizeError::UnknownTool)?;
        // Relays re-serialize the final snapshot (whitespace, key order); the
        // same JSON value is not a conflict. Different values are: which call
        // to execute is ambiguous, so the attempt fails retryably.
        if (state.argument_stream_started || state.argument_snapshot_seen)
            && state.arguments != arguments
            && serde_json::from_str::<serde_json::Value>(&state.arguments).ok()
                != serde_json::from_str::<serde_json::Value>(arguments).ok()
        {
            return Err(NormalizeError::ConflictingToolArguments);
        }
        state.arguments = arguments.to_owned();
        state.argument_source = source;
        state.argument_snapshot_seen = true;
        Ok(())
    }

    pub(super) fn complete_tool(&mut self, index: u32) -> Result<ModelStreamEvent, NormalizeError> {
        let state = self
            .tools
            .get_mut(&index)
            .ok_or(NormalizeError::UnknownTool)?;
        if state.completed {
            return Err(NormalizeError::DuplicateToolCompletion);
        }
        state.completed = true;
        use musteragent_runtime_contract::ToolArgumentValidationIssueKind as Issue;
        let rejected = |kind, parse| ModelStreamEvent::ToolCallRejected {
            output_index: index,
            call_id: state.call_id.clone(),
            kind,
            diagnostic: Some(ArgumentDiagnostic::observed(
                &state.arguments,
                state.argument_source,
                parse,
            )),
        };
        let arguments: Value = if state.arguments.is_empty() {
            serde_json::json!({})
        } else {
            match serde_json::from_str(&state.arguments) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(rejected(
                        Issue::MalformedJson,
                        Some(JsonParseIssue {
                            category: match error.classify() {
                                serde_json::error::Category::Eof => JsonParseCategory::Eof,
                                serde_json::error::Category::Syntax => JsonParseCategory::Syntax,
                                serde_json::error::Category::Data => JsonParseCategory::Data,
                                serde_json::error::Category::Io => JsonParseCategory::Io,
                            },
                            line: error.line() as u64,
                            column: error.column() as u64,
                        }),
                    ));
                }
            }
        };
        if !arguments.is_object() {
            return Ok(rejected(Issue::ObjectRequired, None));
        }
        let arguments = match serde_to_canonical(&arguments) {
            Ok(value) => value,
            Err(NormalizeError::InvalidJson) => return Ok(rejected(Issue::InvalidValue, None)),
            Err(error) => return Err(error),
        };
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: index,
            call_id: state.call_id.clone(),
            arguments,
        })
    }

    pub(super) fn complete_tools(&mut self) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let indexes = self
            .tools
            .iter()
            .filter_map(|(index, state)| (!state.completed).then_some(*index))
            .collect::<Vec<_>>();
        indexes
            .into_iter()
            .map(|index| self.complete_tool(index))
            .collect()
    }

    pub(super) fn complete_truncated_tools(
        &mut self,
    ) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
        let indexes = self
            .tools
            .iter()
            .filter_map(|(index, state)| (!state.completed).then_some(*index))
            .collect::<Vec<_>>();
        indexes
            .into_iter()
            .map(|index| {
                let state = self
                    .tools
                    .get_mut(&index)
                    .ok_or(NormalizeError::UnknownTool)?;
                state.completed = true;
                Ok(ModelStreamEvent::ToolCallRejected {
                    output_index: index,
                    call_id: state.call_id.clone(),
                    kind: musteragent_runtime_contract::ToolArgumentValidationIssueKind::GenerationTokenLimit,
                    diagnostic: Some(ArgumentDiagnostic::observed(&state.arguments, state.argument_source, None)),
                })
            })
            .collect()
    }
}
