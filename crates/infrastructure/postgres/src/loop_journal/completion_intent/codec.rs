use super::*;

const MAGIC: [u8; 4] = *b"DCI2";
const VERSION: u16 = 2;
const MAX_TOOL_CALLS: usize = 10_000;
const MAX_TOOL_CALL_ID_BYTES: usize = 256;
const MAX_WIRE_NAME_BYTES: usize = 128;
const MAX_ARGUMENT_BYTES: usize = 16 * 1_048_576;
const MAX_REASONING_BYTES: usize = 64 * 1_048_576;
const MAX_EXTERNAL_REF_BYTES: usize = 512;
const MAX_MODEL_KEY_BYTES: usize = 200;
const MAX_ROUTE_REF_BYTES: usize = 192;
const MAX_REPLAY_MODEL_BYTES: usize = 512;
const MAX_REPLAY_SIGNATURE_BYTES: usize = 4_096;

pub(super) fn encode(
    completion: &ModelCompletionCommit,
) -> Result<Vec<u8>, PostgresAgentLoopJournalError> {
    let mut writer = Writer::default();
    writer.raw(&MAGIC)?;
    let version = if completion
        .tool_calls
        .iter()
        .any(|call| call.argument_diagnostic.is_some())
    {
        3
    } else {
        VERSION
    };
    writer.u16(version)?;
    writer.raw(completion.invocation_id.into_uuid().as_bytes())?;
    writer.u8(match completion.kind {
        ModelCompletionKind::AssistantResponse => 1,
        ModelCompletionKind::ToolCalls => 2,
        ModelCompletionKind::EmptyResponse => 3,
        ModelCompletionKind::AssistantContinuation => 4,
        ModelCompletionKind::ReasoningContinuation => 5,
    })?;
    writer.count(completion.tool_calls.len(), MAX_TOOL_CALLS)?;
    for call in &completion.tool_calls {
        call.validate_argument_diagnostic()
            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
        writer.string(call.call_id.as_str(), MAX_TOOL_CALL_ID_BYTES)?;
        writer.string(call.name.as_str(), MAX_WIRE_NAME_BYTES)?;
        let arguments = serde_json::to_vec(&json_to_serde(&call.arguments))
            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
        writer.bytes(&arguments, MAX_ARGUMENT_BYTES)?;
        match &call.provider_replay {
            None => writer.u8(0)?,
            Some(replay) => {
                replay
                    .validate()
                    .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
                writer.u8(1)?;
                writer.u8(1)?;
                writer.string(replay.source_route_ref().as_str(), MAX_ROUTE_REF_BYTES)?;
                writer.u8(api_format_code(replay.source_api_format()))?;
                writer.string(replay.source_model_key(), MAX_REPLAY_MODEL_BYTES)?;
                writer.string(
                    replay
                        .google_signature()
                        .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)?,
                    MAX_REPLAY_SIGNATURE_BYTES,
                )?;
            }
        }
        writer.u8(call
            .planning_failure
            .map_or(0, ToolCallPlanningFailure::storage_code))?;
        if version == 3 {
            match &call.argument_diagnostic {
                None => writer.u8(0)?,
                Some(value) => {
                    writer.u8(1)?;
                    writer.bytes(
                        &value
                            .encode()
                            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?,
                        musteragent_runtime_model::ArgumentDiagnostic::MAX_ENCODED_BYTES,
                    )?;
                }
            }
        }
    }
    match &completion.reasoning_replay {
        None => writer.u8(0)?,
        Some(reasoning) => {
            writer.u8(1)?;
            writer.string(reasoning.as_str(), MAX_REASONING_BYTES)?;
        }
    }
    match completion.usage {
        None => writer.u8(0)?,
        Some(usage) => {
            usage
                .validate()
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            writer.u8(1)?;
            writer.u64(usage.input_tokens_uncached)?;
            writer.u64(usage.output_tokens)?;
            writer.u64(usage.reasoning_tokens)?;
            writer.u64(usage.provider_cache_read_tokens)?;
            writer.u64(usage.provider_cache_write_tokens)?;
            writer.u64(usage.provider_cache_write_1h_tokens)?;
            match usage.total_tokens {
                None => writer.u8(0)?,
                Some(total) => {
                    writer.u8(1)?;
                    writer.u64(total)?;
                }
            }
        }
    }
    writer.string(
        completion.provider_response_ref.as_str(),
        MAX_EXTERNAL_REF_BYTES,
    )?;
    writer.string(completion.response_model_key.as_str(), MAX_MODEL_KEY_BYTES)?;
    match &completion.continuation_ref {
        None => writer.u8(0)?,
        Some(reference) => {
            writer.u8(1)?;
            writer.string(reference.as_str(), MAX_EXTERNAL_REF_BYTES)?;
        }
    }
    writer.finish()
}

pub(super) fn decode(bytes: &[u8]) -> Result<ModelCompletionCommit, PostgresAgentLoopJournalError> {
    if u64::try_from(bytes.len())? > MAX_INTENT_PLAINTEXT_BYTES {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let mut reader = Reader::new(bytes);
    if reader.array::<4>()? != MAGIC {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let version = reader.u16()?;
    if !matches!(version, 2 | 3) {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let invocation_id = InvocationId::from_uuid(uuid::Uuid::from_bytes(reader.array::<16>()?))
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    let kind = match reader.u8()? {
        1 => ModelCompletionKind::AssistantResponse,
        2 => ModelCompletionKind::ToolCalls,
        3 => ModelCompletionKind::EmptyResponse,
        4 => ModelCompletionKind::AssistantContinuation,
        5 => ModelCompletionKind::ReasoningContinuation,
        _ => return Err(PostgresAgentLoopJournalError::CorruptState),
    };
    let tool_count = reader.count(MAX_TOOL_CALLS)?;
    let mut tool_calls = Vec::with_capacity(tool_count);
    for _ in 0..tool_count {
        let call_id = ToolCallId::new(reader.string(MAX_TOOL_CALL_ID_BYTES)?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let name = WireName::new(reader.string(MAX_WIRE_NAME_BYTES)?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let arguments =
            serde_json::from_slice::<serde_json::Value>(reader.bytes(MAX_ARGUMENT_BYTES)?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
                .and_then(serde_to_json)?;
        let provider_replay = match reader.closed_flag()? {
            false => None,
            true => {
                if reader.u8()? != 1 {
                    return Err(PostgresAgentLoopJournalError::CorruptState);
                }
                let route_ref = VersionedKey::new(reader.string(MAX_ROUTE_REF_BYTES)?)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                let api_format = api_format(reader.u8()?)?;
                let source_model = reader.string(MAX_REPLAY_MODEL_BYTES)?;
                let signature = reader.string(MAX_REPLAY_SIGNATURE_BYTES)?;
                Some(
                    ProviderToolCallReplay::google_thought_signature(
                        ModelRouteRef::from_key(route_ref),
                        api_format,
                        source_model,
                        signature,
                    )
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                )
            }
        };
        let planning_failure = match reader.u8()? {
            0 => None,
            code => Some(
                ToolCallPlanningFailure::from_storage_code(code)
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
            ),
        };
        let argument_diagnostic = if version == 3 && reader.closed_flag()? {
            Some(
                musteragent_runtime_model::ArgumentDiagnostic::decode(
                    reader
                        .bytes(musteragent_runtime_model::ArgumentDiagnostic::MAX_ENCODED_BYTES)?,
                )
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            )
        } else {
            None
        };
        tool_calls.push(PlannedToolCall {
            argument_diagnostic,
            call_id,
            name,
            arguments,
            provider_replay,
            planning_failure,
        });
    }
    for call in &tool_calls {
        call.validate_argument_diagnostic()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    }
    if (version == 3)
        != tool_calls
            .iter()
            .any(|call| call.argument_diagnostic.is_some())
    {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let reasoning_replay = if reader.closed_flag()? {
        Some(
            ModelReplayText::new(reader.string(MAX_REASONING_BYTES)?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        )
    } else {
        None
    };
    let usage = if reader.closed_flag()? {
        let usage = ModelUsage {
            input_tokens_uncached: reader.u64()?,
            output_tokens: reader.u64()?,
            reasoning_tokens: reader.u64()?,
            provider_cache_read_tokens: reader.u64()?,
            provider_cache_write_tokens: reader.u64()?,
            provider_cache_write_1h_tokens: reader.u64()?,
            total_tokens: if reader.closed_flag()? {
                Some(reader.u64()?)
            } else {
                None
            },
        };
        usage
            .validate()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        Some(usage)
    } else {
        None
    };
    let provider_response_ref = ProviderRequestRef::from_opaque(
        OpaqueExternalRef::new(reader.string(MAX_EXTERNAL_REF_BYTES)?)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
    );
    let response_model_key = musteragent_model::ModelKey::new(reader.string(MAX_MODEL_KEY_BYTES)?)
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    let continuation_ref = if reader.closed_flag()? {
        Some(musteragent_runtime_model::ContinuationRef::from_opaque(
            OpaqueExternalRef::new(reader.string(MAX_EXTERNAL_REF_BYTES)?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        ))
    } else {
        None
    };
    reader.finish()?;
    let completion = ModelCompletionCommit {
        invocation_id,
        kind,
        tool_calls,
        reasoning_replay,
        usage,
        provider_response_ref,
        response_model_key,
        continuation_ref,
    };
    validate_completion_shape(&completion)?;
    Ok(completion)
}

fn api_format_code(value: ApiFormat) -> u8 {
    match value {
        ApiFormat::OpenAiResponses => 1,
        ApiFormat::OpenAiChatCompletions => 2,
        ApiFormat::AnthropicMessages => 3,
        ApiFormat::GoogleGenerateContent => 4,
        ApiFormat::GoogleInteractions => 5,
        ApiFormat::BedrockConverse => 6,
        ApiFormat::BedrockInvokeModel => 7,
        ApiFormat::TypeSafeSystemOne => 8,
        ApiFormat::OpenRouterDecisions => 9,
    }
}

fn api_format(value: u8) -> Result<ApiFormat, PostgresAgentLoopJournalError> {
    match value {
        1 => Ok(ApiFormat::OpenAiResponses),
        2 => Ok(ApiFormat::OpenAiChatCompletions),
        3 => Ok(ApiFormat::AnthropicMessages),
        4 => Ok(ApiFormat::GoogleGenerateContent),
        5 => Ok(ApiFormat::GoogleInteractions),
        6 => Ok(ApiFormat::BedrockConverse),
        7 => Ok(ApiFormat::BedrockInvokeModel),
        8 => Ok(ApiFormat::TypeSafeSystemOne),
        9 => Ok(ApiFormat::OpenRouterDecisions),
        _ => Err(PostgresAgentLoopJournalError::CorruptState),
    }
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn raw(&mut self, value: &[u8]) -> Result<(), PostgresAgentLoopJournalError> {
        let next = self
            .bytes
            .len()
            .checked_add(value.len())
            .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)?;
        if u64::try_from(next)? > MAX_INTENT_PLAINTEXT_BYTES {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn u8(&mut self, value: u8) -> Result<(), PostgresAgentLoopJournalError> {
        self.raw(&[value])
    }

    fn u16(&mut self, value: u16) -> Result<(), PostgresAgentLoopJournalError> {
        self.raw(&value.to_be_bytes())
    }

    fn u32(&mut self, value: u32) -> Result<(), PostgresAgentLoopJournalError> {
        self.raw(&value.to_be_bytes())
    }

    fn u64(&mut self, value: u64) -> Result<(), PostgresAgentLoopJournalError> {
        self.raw(&value.to_be_bytes())
    }

    fn count(&mut self, value: usize, maximum: usize) -> Result<(), PostgresAgentLoopJournalError> {
        if value > maximum {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        self.u32(u32::try_from(value)?)
    }

    fn bytes(&mut self, value: &[u8], maximum: usize) -> Result<(), PostgresAgentLoopJournalError> {
        if value.is_empty() || value.len() > maximum {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        self.u32(u32::try_from(value.len())?)?;
        self.raw(value)
    }

    fn string(&mut self, value: &str, maximum: usize) -> Result<(), PostgresAgentLoopJournalError> {
        self.bytes(value.as_bytes(), maximum)
    }

    fn finish(self) -> Result<Vec<u8>, PostgresAgentLoopJournalError> {
        if self.bytes.is_empty() {
            Err(PostgresAgentLoopJournalError::InvalidModelBatch)
        } else {
            Ok(self.bytes)
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PostgresAgentLoopJournalError> {
        let end = self
            .cursor
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let value = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], PostgresAgentLoopJournalError> {
        self.take(N)?
            .try_into()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
    }

    fn u8(&mut self) -> Result<u8, PostgresAgentLoopJournalError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, PostgresAgentLoopJournalError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, PostgresAgentLoopJournalError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, PostgresAgentLoopJournalError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn count(&mut self, maximum: usize) -> Result<usize, PostgresAgentLoopJournalError> {
        let value = usize::try_from(self.u32()?)?;
        if value > maximum {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        Ok(value)
    }

    fn bytes(&mut self, maximum: usize) -> Result<&'a [u8], PostgresAgentLoopJournalError> {
        let length = self.count(maximum)?;
        if length == 0 {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        self.take(length)
    }

    fn string(&mut self, maximum: usize) -> Result<String, PostgresAgentLoopJournalError> {
        std::str::from_utf8(self.bytes(maximum)?)
            .map(str::to_owned)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)
    }

    fn closed_flag(&mut self) -> Result<bool, PostgresAgentLoopJournalError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PostgresAgentLoopJournalError::CorruptState),
        }
    }

    fn finish(self) -> Result<(), PostgresAgentLoopJournalError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(PostgresAgentLoopJournalError::CorruptState)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn maximal_metadata_tool(index: usize) -> PlannedToolCall {
        let route_body = "a".repeat(MAX_ROUTE_REF_BYTES - 2);
        let call_prefix = format!("{index:05}");
        let wire_prefix = format!("a{index:05}");
        PlannedToolCall {
            argument_diagnostic: None,
            call_id: ToolCallId::new(format!(
                "{call_prefix}{}",
                "x".repeat(MAX_TOOL_CALL_ID_BYTES - call_prefix.len())
            ))
            .expect("maximal call ID"),
            name: WireName::new(format!(
                "{wire_prefix}{}",
                "x".repeat(MAX_WIRE_NAME_BYTES - wire_prefix.len())
            ))
            .expect("maximal wire name"),
            arguments: JsonValue::Object(BTreeMap::new()),
            provider_replay: Some(
                ProviderToolCallReplay::google_thought_signature(
                    ModelRouteRef::from_key(
                        VersionedKey::new(format!("{route_body}@1")).expect("maximal route ref"),
                    ),
                    ApiFormat::GoogleGenerateContent,
                    "m".repeat(MAX_REPLAY_MODEL_BYTES),
                    "s".repeat(MAX_REPLAY_SIGNATURE_BYTES),
                )
                .expect("maximal provider replay"),
            ),
            planning_failure: Some(ToolCallPlanningFailure::Truncated),
        }
    }

    fn completion() -> ModelCompletionCommit {
        ModelCompletionCommit {
            invocation_id: InvocationId::generate(),
            kind: ModelCompletionKind::ToolCalls,
            tool_calls: vec![PlannedToolCall {
                argument_diagnostic: None,
                call_id: ToolCallId::new("codec-control").expect("call"),
                name: WireName::new("codec_control").expect("name"),
                arguments: JsonValue::Object(BTreeMap::from([(
                    "control".to_owned(),
                    JsonValue::String("\0\n\t\\\"".repeat(1_024)),
                )])),
                provider_replay: None,
                planning_failure: None,
            }],
            reasoning_replay: Some(ModelReplayText::new("\n\t".repeat(1_024)).expect("reasoning")),
            usage: None,
            provider_response_ref: ProviderRequestRef::from_opaque(
                OpaqueExternalRef::new("response-codec-control").expect("response"),
            ),
            response_model_key: musteragent_model::ModelKey::new("model-codec-control")
                .expect("model"),
            continuation_ref: None,
        }
    }

    #[test]
    fn v3_diagnostics_roundtrip_without_downgrading_legacy_intents() {
        use musteragent_runtime_model::{ArgumentDiagnostic, ArgumentSource, StopReason};
        let mut value = completion();
        let legacy = encode(&value).expect("legacy");
        let mut diagnostic = ArgumentDiagnostic::observed("{", ArgumentSource::Stream, None);
        diagnostic.finish(StopReason::ToolCalls, 16384, 1048576, None);
        value.tool_calls[0].planning_failure = Some(ToolCallPlanningFailure::MalformedJson);
        value.tool_calls[0].argument_diagnostic = Some(diagnostic);
        let mut bytes = encode(&value).expect("diagnostic intent");
        assert_eq!(decode(&bytes).expect("decode"), value);
        assert_ne!(bytes, legacy);
        bytes[4..6].copy_from_slice(&VERSION.to_be_bytes());
        assert!(decode(&bytes).is_err(), "downgrade must not drop evidence");
        value.tool_calls[0].argument_diagnostic = None;
        value.tool_calls[0].planning_failure = None;
        assert_eq!(encode(&value).expect("legacy unchanged"), legacy);
    }

    #[test]
    fn v2_preserves_every_non_executable_tool_failure() {
        for failure in [
            None,
            Some(ToolCallPlanningFailure::Truncated),
            Some(ToolCallPlanningFailure::MalformedJson),
            Some(ToolCallPlanningFailure::ObjectRequired),
            Some(ToolCallPlanningFailure::InvalidValue),
        ] {
            let mut expected = completion();
            expected.tool_calls[0].planning_failure = failure;
            assert_eq!(
                decode(&encode(&expected).expect("valid codec fixture"))
                    .expect("valid codec fixture"),
                expected
            );
        }
    }

    #[test]
    fn v2_round_trip_does_not_double_escape_argument_or_reasoning_bytes() {
        let completion = completion();
        let encoded = encode(&completion).expect("encode");
        assert_eq!(decode(&encoded).expect("decode"), completion);
        let argument_json = serde_json::to_vec(&json_to_serde(&completion.tool_calls[0].arguments))
            .expect("argument json");
        assert!(
            encoded
                .windows(argument_json.len())
                .any(|window| window == argument_json)
        );
        assert!(
            encoded
                .windows(
                    completion
                        .reasoning_replay
                        .as_ref()
                        .expect("reasoning")
                        .as_str()
                        .len()
                )
                .any(|window| window
                    == completion
                        .reasoning_replay
                        .as_ref()
                        .expect("reasoning")
                        .as_str()
                        .as_bytes())
        );
    }

    #[test]
    fn v2_rejects_truncation_trailing_bytes_and_unknown_version() {
        let encoded = encode(&completion()).expect("encode");
        assert!(matches!(
            decode(&encoded[..encoded.len() - 1]),
            Err(PostgresAgentLoopJournalError::CorruptState)
        ));
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            decode(&trailing),
            Err(PostgresAgentLoopJournalError::CorruptState)
        ));
        let mut unknown = encoded;
        unknown[5] = 3;
        assert!(matches!(
            decode(&unknown),
            Err(PostgresAgentLoopJournalError::CorruptState)
        ));
    }

    #[test]
    fn frozen_admission_overhead_dominates_multitool_control_payload() {
        let tool_count = 32_usize;
        let reasoning = "推理\n\t\\\"".repeat(8_192);
        let mut completion = completion();
        completion.tool_calls = (0..tool_count).map(maximal_metadata_tool).collect();
        completion.reasoning_replay =
            Some(ModelReplayText::new(reasoning.clone()).expect("reasoning"));
        completion.usage = Some(ModelUsage {
            input_tokens_uncached: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            provider_cache_read_tokens: 0,
            provider_cache_write_tokens: 0,
            provider_cache_write_1h_tokens: 0,
            total_tokens: Some(0),
        });
        completion.provider_response_ref = ProviderRequestRef::from_opaque(
            OpaqueExternalRef::new("r".repeat(MAX_EXTERNAL_REF_BYTES)).expect("response"),
        );
        completion.response_model_key =
            musteragent_model::ModelKey::new("m".repeat(MAX_MODEL_KEY_BYTES)).expect("model");
        completion.continuation_ref =
            Some(musteragent_runtime_model::ContinuationRef::from_opaque(
                OpaqueExternalRef::new("c".repeat(MAX_EXTERNAL_REF_BYTES)).expect("continuation"),
            ));

        let encoded = encode(&completion).expect("encode maximal metadata");
        let arguments_bytes: usize = completion
            .tool_calls
            .iter()
            .map(|call| {
                serde_json::to_vec(&json_to_serde(&call.arguments))
                    .expect("argument JSON")
                    .len()
            })
            .sum();
        let dynamic_bytes = arguments_bytes + reasoning.len();
        let admitted_overhead = usize::try_from(
            musteragent_runtime_snapshot::MODEL_COMPLETION_INTENT_FIXED_BYTES
                + u64::try_from(tool_count).expect("tool count")
                    * musteragent_runtime_snapshot::MODEL_COMPLETION_INTENT_PER_TOOL_BYTES,
        )
        .expect("admission overhead");
        assert_eq!(encoded.len() - dynamic_bytes, admitted_overhead);
        assert_eq!(decode(&encoded).expect("decode"), completion);
    }

    #[test]
    fn legacy_json_remains_readable_but_unknown_shape_fails_closed() {
        let invocation_id = InvocationId::generate();
        let encoded = serde_json::json!({
            "schema": "musteragent.legacy.runtime.model-completion-intent.v1",
            "invocation_id": invocation_id.to_string(),
            "kind": "empty_response",
            "tool_calls": [],
            "reasoning_replay": null,
            "usage": null,
            "provider_response_ref": "legacy-response",
            "response_model_key": "legacy-model",
            "continuation_ref": null,
        })
        .to_string();
        let decoded = super::super::decode_legacy_model_completion_intent(&encoded)
            .expect("decode legacy V1 JSON");
        assert_eq!(decoded.invocation_id, invocation_id);
        assert_eq!(decoded.kind, ModelCompletionKind::EmptyResponse);
        assert!(decoded.usage.is_none());

        let invalid = encoded.replace("empty_response", "tool_calls");
        assert!(matches!(
            super::super::decode_legacy_model_completion_intent(&invalid),
            Err(PostgresAgentLoopJournalError::CorruptState)
                | Err(PostgresAgentLoopJournalError::InvalidModelBatch)
        ));
    }
}
