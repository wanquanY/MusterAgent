pub mod metadata;
mod model_decode;
pub use musteragent_context::compaction_history::{json_from_serde, json_to_serde};
mod timing;

use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_context::{
    CONTEXT_SUMMARY_SCHEMA, ContextCodec, ContextEntry, ContextFileOperations, ContextPlanError,
    ContextSummaryDocument, ContextSummaryItem, ContextSummaryRole,
    ISOLATED_CONTEXT_SUMMARY_SCHEMA, LEGACY_CONTEXT_SUMMARY_SCHEMA, NATIVE_CONTEXT_SUMMARY_SCHEMA,
};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_model::{ModelParameter, ProviderStatePolicy};
use musteragent_run::{AttemptContext, InvocationContext, RunContext};
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    CanonicalRuntimeEvent, CanonicalToolCall, CanonicalToolDefinition, JsonValue, ReasoningPolicy,
    ReasoningReplay,
};
use serde_json::{Map, Value, json};

/// Deterministic, provider-neutral JSON used for content-addressed Context
/// summaries and exact canonical model requests.
#[derive(Debug, Copy, Clone, Default)]
pub struct CanonicalJsonContextCodec;

impl ContextCodec for CanonicalJsonContextCodec {
    fn encode_dialogue_page(
        &self,
        page: &musteragent_execution_context::DialogueContextSourcePage,
    ) -> Result<Vec<u8>, ContextPlanError> {
        crate::dialogue_context_codec::page_bytes(page).map_err(|_| ContextPlanError::invariant())
    }
    fn encode_dialogue_cut(
        &self,
        cut: &musteragent_execution_context::DialogueContextCut,
    ) -> Result<Vec<u8>, ContextPlanError> {
        cut.validate().map_err(|_| ContextPlanError::invariant())?;
        Ok(prost::Message::encode_to_vec(
            &crate::dialogue_context_codec::cut(cut),
        ))
    }

    fn material_cache_identity(&self) -> Option<&'static str> {
        Some("musteragent.canonical-json-context.v2")
    }
    fn decode_json(&self, bytes: &[u8]) -> Result<JsonValue, ContextPlanError> {
        let value: Value =
            serde_json::from_slice(bytes).map_err(|_| ContextPlanError::invariant())?;
        json_from_serde(value)
    }

    fn encode_summary(
        &self,
        summary: &ContextSummaryDocument,
    ) -> Result<Vec<u8>, ContextPlanError> {
        let value = json!({
            "schema_version": summary.schema_version,
            "compacted": summary.compacted,
            "source_seq_from": summary.source_seq_from.get(),
            "source_seq_to": summary.source_seq_to.get(),
            "summary": summary.summary,
            "generator_invocation_id": summary.generator_invocation_id.map(|value| value.to_string()),
            "previous_summary_ref": summary.previous_summary_ref.as_ref().map(content_ref),
            "summarized_record_ids": summary.summarized_record_ids.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "file_operations": {
                "read": summary.file_operations.read,
                "written": summary.file_operations.written,
                "edited": summary.file_operations.edited,
            },
            "items": summary.items.iter().map(|item| json!({
                "source_sequence": item.source_sequence.get(),
                "source_record_ids": item.source_record_ids.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                "role": summary_role(item.role),
                "excerpt": item.excerpt,
            })).collect::<Vec<_>>(),
            "omitted_item_count": summary.omitted_item_count,
        });
        serde_json::to_vec(&value).map_err(|_| ContextPlanError::invariant())
    }

    fn decode_summary(&self, bytes: &[u8]) -> Result<ContextSummaryDocument, ContextPlanError> {
        let value: Value =
            serde_json::from_slice(bytes).map_err(|_| ContextPlanError::invariant())?;
        decode_summary_document(&value)
    }

    fn encode_compaction_history(
        &self,
        entries: &[ContextEntry],
    ) -> Result<Vec<u8>, ContextPlanError> {
        musteragent_context::compaction_history::encode(entries)
    }

    fn encode_model_request(
        &self,
        request: &CanonicalModelRequest,
    ) -> Result<Vec<u8>, ContextPlanError> {
        request
            .validate()
            .map_err(|_| ContextPlanError::invariant())?;
        serde_json::to_vec(&model_request(request)).map_err(|_| ContextPlanError::invariant())
    }
}

fn decode_summary_document(value: &Value) -> Result<ContextSummaryDocument, ContextPlanError> {
    let object = value.as_object().ok_or_else(ContextPlanError::invariant)?;
    let schema = required_str(object, "schema_version")?;
    if !matches!(
        schema,
        CONTEXT_SUMMARY_SCHEMA
            | LEGACY_CONTEXT_SUMMARY_SCHEMA
            | ISOLATED_CONTEXT_SUMMARY_SCHEMA
            | NATIVE_CONTEXT_SUMMARY_SCHEMA
    ) {
        return Err(ContextPlanError::invariant());
    }
    let source_seq_from =
        musteragent_identity::ConversationSeq::new(required_u64(object, "source_seq_from")?)
            .map_err(|_| ContextPlanError::invariant())?;
    let source_seq_to =
        musteragent_identity::ConversationSeq::new(required_u64(object, "source_seq_to")?)
            .map_err(|_| ContextPlanError::invariant())?;
    let items = object
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(ContextPlanError::invariant)?
        .iter()
        .map(decode_summary_item)
        .collect::<Result<Vec<_>, _>>()?;
    if !matches!(
        schema,
        ISOLATED_CONTEXT_SUMMARY_SCHEMA | NATIVE_CONTEXT_SUMMARY_SCHEMA
    ) && items
        .iter()
        .any(|item| item.role == ContextSummaryRole::Runtime)
    {
        return Err(ContextPlanError::invariant());
    }
    let summarized_record_ids = optional_string_array(object, "summarized_record_ids")?
        .into_iter()
        .map(|value| value.parse().map_err(|_| ContextPlanError::invariant()))
        .collect::<Result<Vec<_>, _>>()?;
    let file_operations = object
        .get("file_operations")
        .and_then(Value::as_object)
        .map_or_else(
            || Ok::<_, ContextPlanError>(ContextFileOperations::default()),
            |files| {
                Ok(ContextFileOperations {
                    read: optional_string_array(files, "read")?,
                    written: optional_string_array(files, "written")?,
                    edited: optional_string_array(files, "edited")?,
                })
            },
        )?;
    Ok(ContextSummaryDocument {
        schema_version: if schema == NATIVE_CONTEXT_SUMMARY_SCHEMA {
            NATIVE_CONTEXT_SUMMARY_SCHEMA
        } else if schema == ISOLATED_CONTEXT_SUMMARY_SCHEMA {
            ISOLATED_CONTEXT_SUMMARY_SCHEMA
        } else if schema == CONTEXT_SUMMARY_SCHEMA {
            CONTEXT_SUMMARY_SCHEMA
        } else {
            LEGACY_CONTEXT_SUMMARY_SCHEMA
        },
        compacted: object
            .get("compacted")
            .and_then(Value::as_bool)
            .ok_or_else(ContextPlanError::invariant)?,
        source_seq_from,
        source_seq_to,
        summary: optional_str(object, "summary")?.map(ToOwned::to_owned),
        generator_invocation_id: optional_str(object, "generator_invocation_id")?
            .map(str::parse)
            .transpose()
            .map_err(|_| ContextPlanError::invariant())?,
        previous_summary_ref: object
            .get("previous_summary_ref")
            .filter(|value| !value.is_null())
            .map(decode_content_ref)
            .transpose()?,
        summarized_record_ids,
        file_operations,
        items,
        omitted_item_count: required_u64(object, "omitted_item_count")?,
    })
}

fn decode_summary_item(value: &Value) -> Result<ContextSummaryItem, ContextPlanError> {
    let object = value.as_object().ok_or_else(ContextPlanError::invariant)?;
    let role = match required_str(object, "role")? {
        "user" => ContextSummaryRole::User,
        "assistant" => ContextSummaryRole::Assistant,
        "tool" => ContextSummaryRole::Tool,
        "runtime" => ContextSummaryRole::Runtime,
        _ => return Err(ContextPlanError::invariant()),
    };
    Ok(ContextSummaryItem {
        source_sequence: musteragent_identity::ConversationSeq::new(required_u64(
            object,
            "source_sequence",
        )?)
        .map_err(|_| ContextPlanError::invariant())?,
        source_record_ids: optional_string_array(object, "source_record_ids")?
            .into_iter()
            .map(|value| value.parse().map_err(|_| ContextPlanError::invariant()))
            .collect::<Result<Vec<_>, _>>()?,
        role,
        excerpt: required_str(object, "excerpt")?.to_owned(),
    })
}

fn decode_content_ref(value: &Value) -> Result<ContentRef, ContextPlanError> {
    let object = value.as_object().ok_or_else(ContextPlanError::invariant)?;
    let digest = decode_hex_32(required_str(object, "sha256")?)?;
    Ok(ContentRef {
        content_id: required_str(object, "content_id")?
            .parse()
            .map_err(|_| ContextPlanError::invariant())?,
        media_type: MediaType::new(required_str(object, "media_type")?)
            .map_err(|_| ContextPlanError::invariant())?,
        byte_length: required_u64(object, "byte_length")?,
        sha256: Sha256::from_bytes(digest).map_err(|_| ContextPlanError::invariant())?,
    })
}

fn decode_hex_32(value: &str) -> Result<[u8; 32], ContextPlanError> {
    if value.len() != 64 || !value.is_ascii() {
        return Err(ContextPlanError::invariant());
    }
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
            .map_err(|_| ContextPlanError::invariant())?;
    }
    Ok(output)
}

fn required_str<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ContextPlanError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(ContextPlanError::invariant)
}

fn optional_str<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, ContextPlanError> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .map(Some)
            .ok_or_else(ContextPlanError::invariant),
    }
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Result<u64, ContextPlanError> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(ContextPlanError::invariant)
}

fn optional_string_array(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Vec<String>, ContextPlanError> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(value) => value
            .as_array()
            .ok_or_else(ContextPlanError::invariant)?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(ToOwned::to_owned)
                    .ok_or_else(ContextPlanError::invariant)
            })
            .collect(),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum ModelRequestSchema {
    LegacyV2,
    V3,
    ActivityV4,
    DeliveryRepairV5,
    DialogueInputV6,
    ApprovalObservationV7,
    NativePublicationV8,
}
impl ModelRequestSchema {
    fn as_str(self) -> &'static str {
        match self {
            Self::LegacyV2 => "musteragent.canonical-model-request.v2",
            Self::V3 => "musteragent.canonical-model-request.v3",
            Self::ActivityV4 => "musteragent.canonical-model-request.v4",
            Self::DeliveryRepairV5 => "musteragent.canonical-model-request.v5",
            Self::DialogueInputV6 => "musteragent.canonical-model-request.v6",
            Self::ApprovalObservationV7 => "musteragent.canonical-model-request.v7",
            Self::NativePublicationV8 => "musteragent.canonical-model-request.v8",
        }
    }
}
pub(crate) fn has_delivery_repair_event(request: &CanonicalModelRequest) -> bool {
    request
        .runtime_events
        .iter()
        .any(|event| matches!(event, CanonicalRuntimeEvent::PublicDeliveryRepair { .. }))
}
pub(crate) fn has_dialogue_input_event(request: &CanonicalModelRequest) -> bool {
    request
        .runtime_events
        .iter()
        .any(|event| !event.dialogue_inputs().is_empty())
}
pub(crate) fn has_approval_observation(request: &CanonicalModelRequest) -> bool {
    request
        .runtime_events
        .iter()
        .any(|event| matches!(event, CanonicalRuntimeEvent::ToolApprovalResolved(_)))
}
pub(crate) fn has_native_publication_event(request: &CanonicalModelRequest) -> bool {
    request.runtime_events.iter().any(|event| {
        matches!(
            event,
            CanonicalRuntimeEvent::NativePublicationRequired { .. }
        )
    })
}
fn model_request(request: &CanonicalModelRequest) -> Value {
    let schema = if has_native_publication_event(request) {
        ModelRequestSchema::NativePublicationV8
    } else if has_approval_observation(request) {
        ModelRequestSchema::ApprovalObservationV7
    } else if has_dialogue_input_event(request) {
        ModelRequestSchema::DialogueInputV6
    } else if has_delivery_repair_event(request) {
        ModelRequestSchema::DeliveryRepairV5
    } else if request.invocation.timing.legacy_deadline().is_some() {
        ModelRequestSchema::V3
    } else {
        ModelRequestSchema::ActivityV4
    };
    model_request_for_schema(request, schema)
}
fn model_request_for_schema(request: &CanonicalModelRequest, schema: ModelRequestSchema) -> Value {
    json!({
        "schema_version": schema.as_str(),
        "invocation": invocation(&request.invocation),
        "route": route(&request.route),
        "route_role": match request.route_role {
            musteragent_runtime_model::ModelInvocationRouteRole::Chat => "chat",
            musteragent_runtime_model::ModelInvocationRouteRole::Auxiliary(role) => role.as_str(),
        },
        "credential_grant_ref": request.credential_grant_ref.as_str(),
        "continuation_ref": request.continuation_ref.as_ref().map(|value| value.as_str()),
        "instructions": request.instructions.iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>(),
        "input": request.input.iter().map(message).collect::<Vec<_>>(),
        "runtime_events": request.runtime_events.iter().map(runtime_event).collect::<Vec<_>>(),
        "tools": request.tools.iter().map(|value| tool(value, schema)).collect::<Vec<_>>(),
        "response_schema": request.response_schema.as_ref().map(|(reference, schema)| json!({
            "schema_ref": schema_ref(reference),
            "schema": json_to_serde(schema),
        })),
        "reasoning_policy": reasoning_policy(request.reasoning_policy),
        "cache_policy": cache_policy(request.cache_policy),
        "stream_budget": {
            "max_event_bytes": request.stream_budget.max_event_bytes.get(),
            "max_events": request.stream_budget.max_events.get(),
            "max_total_bytes": request.stream_budget.max_total_bytes.get(),
            "max_tool_calls": request.stream_budget.max_tool_calls.get(),
            "max_tool_argument_bytes": request.stream_budget.max_tool_argument_bytes.get(),
            "max_reasoning_replay_bytes": request.stream_budget.max_reasoning_replay_bytes.get(),
        },
        "max_output_tokens": request.max_output_tokens.get(),
        "temperature": request.temperature,
    })
}

fn runtime_event(value: &CanonicalRuntimeEvent) -> Value {
    match value {
        CanonicalRuntimeEvent::ToolApprovalResolved(v) => json!({
            "kind": "tool_approval_resolved", "invocation_id": v.invocation_id.to_string(),
            "tool_call_id": v.tool_call_id.as_str(), "tool_ref": v.tool_key.as_str(),
            "interaction_id": v.interaction_id.to_string(), "request_record_id": v.request_record_id.to_string(),
            "resolution_record_id": v.resolution_record_id.to_string(), "resolution": v.resolution.as_str(),
        }),
        CanonicalRuntimeEvent::PublicDeliveryRepair {
            source_invocation_id,
            binding,
        } => {
            use base64::Engine;
            let bytes = prost::Message::encode_to_vec(
                &crate::public_delivery_repair_codec::public_delivery_repair_binding_fields(
                    binding,
                ),
            );
            json!({"kind":"public_delivery_repair", "source_invocation_id":source_invocation_id.to_string(),
                "binding_protobuf_base64":base64::engine::general_purpose::STANDARD.encode(bytes)})
        }
        CanonicalRuntimeEvent::AgentDialogueHistory {
            frozen_cut,
            from_seq,
            through_seq,
            source_page_ref,
            materialized_context,
            input_attachments,
        } => {
            let mut value = json!({
                "kind":"agent_dialogue_history", "tenant_id":frozen_cut.tenant_id.to_string(),"owner_id":frozen_cut.owner_id.to_string(),
                "agent_id":frozen_cut.agent_id.to_string(),"dialogue_id":frozen_cut.dialogue_id.to_string(),"epoch":frozen_cut.through.epoch,
                "frozen_through_seq":frozen_cut.through.seq,"entry_count":frozen_cut.entry_count,"source_chain_sha256":frozen_cut.source_chain_sha256.to_string(),
                "source_contract_ref":musteragent_execution_context::DIALOGUE_CONTEXT_SOURCE_CONTRACT,
                "from_seq":from_seq,"through_seq":through_seq,"source_page_ref":content_ref(source_page_ref),"materialized_context":materialized_context.as_str(),
            });
            if !input_attachments.is_empty() {
                use base64::Engine;
                value["input_attachments"] = json!(
                    input_attachments
                        .iter()
                        .map(|input| {
                            base64::engine::general_purpose::STANDARD.encode(
                                prost::Message::encode_to_vec(
                                    &crate::dialogue_context_codec::input_observation(input),
                                ),
                            )
                        })
                        .collect::<Vec<_>>()
                );
            }
            value
        }
        CanonicalRuntimeEvent::NativePublicationRequired {
            source_invocation_id,
        } => json!({
            "kind":"native_publication_required", "source_invocation_id":source_invocation_id.to_string(),
        }),
        CanonicalRuntimeEvent::ModelRecovery {
            source_invocation_id,
        } => json!({
            "kind": "model_recovery", "source_invocation_id": source_invocation_id.to_string(),
        }),
        CanonicalRuntimeEvent::OutputLimitContinuation {
            source_invocation_id,
        } => json!({
            "kind": "output_limit_continuation",
            "source_invocation_id": source_invocation_id.to_string(),
        }),
        CanonicalRuntimeEvent::DelegationCompletion {
            delivery_id,
            delegation_id,
            child_activity_id,
            child_run_id,
            report,
        } => json!({
            "kind": "delegation_completion",
            "delivery_id": delivery_id.to_string(),
            "delegation_id": delegation_id.to_string(),
            "child_activity_id": child_activity_id.to_string(),
            "child_run_id": child_run_id.to_string(),
            "report": report.as_str(),
        }),
    }
}

fn invocation(value: &InvocationContext) -> Value {
    let mut fields = serde_json::Map::from_iter([
        (
            "invocation_id".to_owned(),
            json!(value.invocation_id.to_string()),
        ),
        (
            "run_context".to_owned(),
            json!(run_context(&value.run_context)),
        ),
        (
            "attempt_context".to_owned(),
            json!(attempt_context(&value.attempt_context)),
        ),
        (
            "parent_invocation_id".to_owned(),
            json!(value.parent_invocation_id.map(|id| id.to_string())),
        ),
        (
            "causation_id".to_owned(),
            json!(value.causation_id.to_string()),
        ),
        (
            "correlation_id".to_owned(),
            json!(value.correlation_id.to_string()),
        ),
    ]);
    timing::encode(&mut fields, &value.timing);
    Value::Object(fields)
}

fn run_context(value: &RunContext) -> Value {
    json!({
        "tenant_id": value.tenant_id.to_string(),
        "conversation_id": value.conversation_id.to_string(),
        "activity_id": value.activity_id.to_string(),
        "activity_kind": activity_kind(value.activity_kind),
        "run_id": value.run_id.to_string(),
        "executor_participant_id": value.executor_participant_id.to_string(),
        "requested_by_participant_id": value.requested_by_participant_id.to_string(),
        "parent_run_id": value.parent_run_id.map(|id| id.to_string()),
        "delegation_id": value.delegation_id.map(|id| id.to_string()),
        "execution_scope": execution_scope(value.execution_scope),
        "control_home": runtime_home(value.control_home),
        "execution_home": runtime_home(value.execution_home),
        "home_epoch": value.home_epoch.get(),
        "runtime_snapshot_id": value.runtime_snapshot_id.to_string(),
    })
}

fn attempt_context(value: &AttemptContext) -> Value {
    json!({
        "attempt_id": value.attempt_id.to_string(),
        "run_id": value.run_id.to_string(),
        "attempt_number": value.attempt_number.get(),
        "worker_id": value.worker_id.as_str(),
        "lease_id": value.lease_id.to_string(),
        "fencing_token": value.fencing_token.get(),
        "lease_expires_at_ms": value.lease_expires_at.unix_millis(),
    })
}

fn route(value: &musteragent_model::ModelRoute) -> Value {
    json!({
        "route_ref": value.route_ref.as_str(),
        "resolved_model_key": value.resolved_model_key,
        "model_provider_id": value.model_provider_id.to_string(),
        "model_entry_id": value.model_entry_id.to_string(),
        "api_format": value.api_format.as_str(),
        "adapter_version": value.adapter_version.as_str(),
        "protocol_version": value.protocol_version.as_str(),
        "context_window": value.context_window.get(),
        "max_output_tokens": value.max_output_tokens.get(),
        "capabilities": value.capabilities.iter()
            .map(|item| item.as_str())
            .collect::<Vec<_>>(),
        "parameters": value.parameters.iter()
            .map(|(key, parameter)| (key.as_str().to_owned(), model_parameter(parameter)))
            .collect::<Map<_, _>>(),
        "fallback_route_refs": value.fallback_route_refs.iter()
            .map(|item| item.as_str())
            .collect::<Vec<_>>(),
        "pricing_version_ref": value.pricing_version_ref.as_str(),
        "provider_state_policy": provider_state_policy(value.provider_state_policy),
    })
}

pub(crate) fn message(value: &CanonicalMessage) -> Value {
    json!({
        "role": role(value.role),
        "content": value.content.iter().map(content_part).collect::<Vec<_>>(),
        "tool_calls": value.tool_calls.iter().map(tool_call).collect::<Vec<_>>(),
        "tool_call_id": value.tool_call_id.as_ref().map(|id| id.as_str()),
        "tool_name": value.tool_name.as_ref().map(|name| name.as_str()),
        "reasoning": value.reasoning.as_ref().map(reasoning_replay),
    })
}

fn content_part(value: &CanonicalContentPart) -> Value {
    match value {
        CanonicalContentPart::Text(value) => json!({
            "kind": "text",
            "text": value.as_str(),
        }),
        CanonicalContentPart::Image(value) => json!({
            "kind": "image",
            "content_ref": content_ref(value),
        }),
        CanonicalContentPart::Audio(value) => json!({
            "kind": "audio",
            "content_ref": content_ref(value),
        }),
        CanonicalContentPart::Video(value) => json!({
            "kind": "video",
            "content_ref": content_ref(value),
        }),
        CanonicalContentPart::File(value) => json!({
            "kind": "file",
            "content_ref": content_ref(value),
        }),
    }
}

fn tool_call(value: &CanonicalToolCall) -> Value {
    json!({
        "call_id": value.call_id.as_str(),
        "name": value.name.as_str(),
        "arguments": json_to_serde(&value.arguments),
        "provider_item_ref": value.provider_item_ref.as_ref().map(|item| item.as_str()),
        "provider_replay_ref": value.provider_replay_ref.as_ref().map(content_ref),
    })
}

fn reasoning_replay(value: &ReasoningReplay) -> Value {
    json!({
        "content_ref": content_ref(&value.content_ref),
        "source_route_ref": value.source_route_ref.as_str(),
        "continuation_ref": value.continuation_ref.as_ref().map(|item| item.as_str()),
    })
}

fn tool(value: &CanonicalToolDefinition, schema: ModelRequestSchema) -> Value {
    let mut value = json!({
        "key": value.key.as_str(),
        "model_name": value.model_name.as_str(),
        "name": value.name.as_str(),
        "description": value.description.as_str(),
        "input_schema_ref": schema_ref(&value.input_schema_ref),
        "input_schema": json_to_serde(&value.input_schema),
        "strict": value.strict,
    });
    if schema == ModelRequestSchema::LegacyV2 {
        value
            .as_object_mut()
            .expect("tool object")
            .remove("model_name");
    }
    value
}

fn content_ref(value: &ContentRef) -> Value {
    json!({
        "content_id": value.content_id.to_string(),
        "media_type": value.media_type.as_str(),
        "byte_length": value.byte_length,
        "sha256": value.sha256.to_string(),
    })
}

pub(crate) fn schema_ref(value: &SchemaRef) -> Value {
    json!({
        "schema_key": value.schema_key.as_str(),
        "schema_hash": value.schema_hash.to_string(),
    })
}

fn model_parameter(value: &ModelParameter) -> Value {
    match value {
        ModelParameter::Bool(value) => Value::Bool(*value),
        ModelParameter::Int(value) => Value::Number((*value).into()),
        ModelParameter::Double(value) => {
            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
        }
        ModelParameter::String(value) => Value::String(value.clone()),
    }
}

const fn activity_kind(value: ActivityKind) -> &'static str {
    match value {
        ActivityKind::Chat => "chat",
        ActivityKind::Coordination => "coordination",
        ActivityKind::Automation => "automation",
        ActivityKind::Delegation => "delegation",
        ActivityKind::Maintenance => "maintenance",
        ActivityKind::Evaluation => "evaluation",
    }
}

fn execution_scope(value: ExecutionScope) -> Value {
    match value {
        ExecutionScope::HostedProject { scope } => json!({
            "kind": "hosted_project", "project_id": scope.project_id.to_string(),
            "workspace_id": scope.workspace_id.to_string(), "resource_tenant_id": scope.resource_tenant_id.to_string(),
            "owner_principal_id": scope.owner_principal_id.to_string(),
            "home": {"binding_id": scope.home.binding_id.to_string(), "device_id": scope.home.device_id.to_string(),
                     "home_epoch": scope.home.home_epoch.get()},
        }),
        ExecutionScope::Tenant { tenant_id } => json!({
            "kind": "tenant",
            "tenant_id": tenant_id.to_string(),
        }),
        ExecutionScope::Workspace { workspace_id } => json!({
            "kind": "workspace",
            "workspace_id": workspace_id.to_string(),
        }),
        ExecutionScope::DevicePrivate {
            device_id,
            workspace_id,
        } => json!({
            "kind": "device_private",
            "device_id": device_id.to_string(),
            "workspace_id": workspace_id.map(|id| id.to_string()),
        }),
    }
}

fn runtime_home(value: RuntimeHome) -> Value {
    match value {
        RuntimeHome::Cloud => json!({"kind": "cloud"}),
        RuntimeHome::Device { device_id } => json!({
            "kind": "device",
            "device_id": device_id.to_string(),
        }),
    }
}

const fn provider_state_policy(value: ProviderStatePolicy) -> &'static str {
    match value {
        ProviderStatePolicy::Stateless => "stateless",
        ProviderStatePolicy::OpaqueContinuation => "opaque_continuation",
    }
}

const fn role(value: CanonicalRole) -> &'static str {
    match value {
        CanonicalRole::User => "user",
        CanonicalRole::Assistant => "assistant",
        CanonicalRole::Tool => "tool",
    }
}

const fn summary_role(value: ContextSummaryRole) -> &'static str {
    match value {
        ContextSummaryRole::User => "user",
        ContextSummaryRole::Assistant => "assistant",
        ContextSummaryRole::Tool => "tool",
        ContextSummaryRole::Runtime => "runtime",
    }
}

const fn reasoning_policy(value: ReasoningPolicy) -> &'static str {
    match value {
        ReasoningPolicy::Disabled => "disabled",
        ReasoningPolicy::Summary => "summary",
        ReasoningPolicy::Full => "full",
    }
}

const fn cache_policy(value: CachePolicy) -> &'static str {
    match value {
        CachePolicy::ProviderDefault => "provider_default",
        CachePolicy::ExplicitEphemeralPrefix => "explicit_ephemeral_prefix",
        CachePolicy::Disabled => "disabled",
    }
}

#[cfg(test)]
mod tests {
    use super::CanonicalJsonContextCodec;
    use musteragent_context::ContextCodec;
    use musteragent_runtime_model::{CanonicalToolDefinition, JsonValue, ModelText};

    #[test]
    fn schema_json_decode_is_ordered_and_rejects_invalid_numbers() {
        let codec = CanonicalJsonContextCodec;
        assert_eq!(
            codec.decode_json(br#"{"z":1,"a":true}"#),
            Ok(JsonValue::Object(std::collections::BTreeMap::from([
                ("a".to_owned(), JsonValue::Bool(true)),
                ("z".to_owned(), JsonValue::Integer(1)),
            ])))
        );
        assert!(codec.decode_json(br#"{"n":1e10000}"#).is_err());
    }

    #[test]
    fn active_device_tools_survive_post_activation_model_projection() {
        let codec = CanonicalJsonContextCodec;
        let catalog = musteragent_tool::BuiltinToolCatalog::load_device()
            .expect("active Device Tool catalog");

        for tool in catalog.definitions() {
            let projected = CanonicalToolDefinition {
                key: tool.key.clone(),
                model_name: tool.wire_name.clone(),
                name: tool.wire_name.clone(),
                description: ModelText::new(tool.description.clone())
                    .expect("bounded Tool description"),
                input_schema_ref: tool.input_schema.clone(),
                input_schema: codec
                    .decode_json(&tool.input_schema_json)
                    .expect("canonical Tool input schema"),
                strict: true,
            };
            projected.validate().unwrap_or_else(|error| {
                panic!(
                    "active Tool {} cannot enter the next model iteration: {error}",
                    tool.key.as_str(),
                )
            });
        }
    }
    #[test]
    fn acknowledged_report_compaction_preserves_runtime_origin() {
        use super::{
            CanonicalContentPart, CanonicalMessage, CanonicalRole, CanonicalRuntimeEvent,
            ContextEntry, Value,
        };
        use musteragent_context::ContextPresentationOrder;
        use musteragent_identity::{
            ActivityId, ConversationSeq, DelegationDeliveryId, DelegationId, LedgerRecordId, RunId,
        };
        use musteragent_runtime_model::ModelText;
        let sequence = ConversationSeq::new(20).expect("sequence");
        let record = LedgerRecordId::generate();
        let child_run = RunId::generate();
        let event = CanonicalRuntimeEvent::DelegationCompletion {
            delivery_id: DelegationDeliveryId::generate(),
            delegation_id: DelegationId::generate(),
            child_activity_id: ActivityId::generate(),
            child_run_id: child_run,
            report: ModelText::new("verified result; unresolved item remains").expect("report"),
        };
        let mut entry = ContextEntry {
            history_page: None,
            runtime_events: vec![event],
            source_sequence: sequence,
            presentation_order: ContextPresentationOrder::ledger(sequence),
            source_record_ids: vec![record],
            source_run_id: Some(RunId::generate()),
            assistant_origin: None,
            continuation_delta_for: None,
            message: CanonicalMessage {
                role: CanonicalRole::Assistant,
                content: vec![CanonicalContentPart::Text(
                    ModelText::new("sizing-only view").expect("text"),
                )],
                tool_calls: Vec::new(),
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
        };
        let codec = CanonicalJsonContextCodec;
        let bytes = codec
            .encode_compaction_history(&[entry.clone()])
            .expect("typed history");
        let value: Value = serde_json::from_slice(&bytes).expect("JSON");
        assert_eq!(value["messages"][0]["role"], "reference");
        let encoded = String::from_utf8(bytes).unwrap();
        assert!(encoded.contains("unresolved item remains"));
        for hidden in [
            child_run.to_string(),
            record.to_string(),
            "sizing-only view".to_owned(),
        ] {
            assert!(!encoded.contains(&hidden));
        }
        entry.runtime_events.clear();
        let original: Value =
            serde_json::from_slice(&codec.encode_compaction_history(&[entry]).unwrap()).unwrap();
        assert_eq!(
            original["messages"][0],
            serde_json::json!({"role":"assistant","text":["sizing-only view"]})
        );
    }
}
