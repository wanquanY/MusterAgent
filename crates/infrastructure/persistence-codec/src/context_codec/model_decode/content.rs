use super::*;
use musteragent_runtime_model::{ModelRouteRef, ProviderRequestRef};
use musteragent_tool::{ToolKey, WireName};

pub(super) fn message(value: &Value) -> Result<CanonicalMessage> {
    let o = object(value)?;
    Ok(CanonicalMessage {
        role: match required_str(o, "role")? {
            "user" => CanonicalRole::User,
            "assistant" => CanonicalRole::Assistant,
            "tool" => CanonicalRole::Tool,
            _ => return Err(ContextPlanError::invariant()),
        },
        content: list(o, "content", part)?,
        tool_calls: list(o, "tool_calls", call)?,
        tool_call_id: optional(o, "tool_call_id", |v| {
            checked(musteragent_identity::ToolCallId::new(string(v)?))
        })?,
        tool_name: optional(o, "tool_name", |v| checked(WireName::new(string(v)?)))?,
        reasoning: optional(o, "reasoning", reasoning)?,
    })
}
fn part(v: &Value) -> Result<CanonicalContentPart> {
    let o = object(v)?;
    match required_str(o, "kind")? {
        "text" => Ok(CanonicalContentPart::Text(text(field(o, "text")?)?)),
        "image" => Ok(CanonicalContentPart::Image(decode_content_ref(field(
            o,
            "content_ref",
        )?)?)),
        "file" => Ok(CanonicalContentPart::File(decode_content_ref(field(
            o,
            "content_ref",
        )?)?)),
        _ => Err(ContextPlanError::invariant()),
    }
}
fn call(v: &Value) -> Result<CanonicalToolCall> {
    let o = object(v)?;
    Ok(CanonicalToolCall {
        call_id: checked(musteragent_identity::ToolCallId::new(required_str(
            o, "call_id",
        )?))?,
        name: checked(WireName::new(required_str(o, "name")?))?,
        arguments: json_from_serde(field(o, "arguments")?.clone())?,
        provider_item_ref: optional(o, "provider_item_ref", |v| {
            Ok(ProviderRequestRef::from_opaque(checked(
                OpaqueExternalRef::new(string(v)?),
            )?))
        })?,
        provider_replay_ref: optional(o, "provider_replay_ref", decode_content_ref)?,
    })
}
fn reasoning(v: &Value) -> Result<ReasoningReplay> {
    let o = object(v)?;
    Ok(ReasoningReplay {
        content_ref: decode_content_ref(field(o, "content_ref")?)?,
        source_route_ref: ModelRouteRef::from_key(key(field(o, "source_route_ref")?)?),
        continuation_ref: optional(o, "continuation_ref", continuation)?,
    })
}
pub(super) fn tool(
    v: &Value,
    schema: super::super::ModelRequestSchema,
) -> Result<CanonicalToolDefinition> {
    let o = object(v)?;
    Ok(CanonicalToolDefinition {
        model_name: checked(WireName::new(required_str(
            o,
            match schema {
                super::super::ModelRequestSchema::LegacyV2 => "name",
                super::super::ModelRequestSchema::V3
                | super::super::ModelRequestSchema::ActivityV4
                | super::super::ModelRequestSchema::DeliveryRepairV5
                | super::super::ModelRequestSchema::DialogueInputV6
                | super::super::ModelRequestSchema::ApprovalObservationV7
                | super::super::ModelRequestSchema::NativePublicationV8 => "model_name",
            },
        )?))?,
        key: ToolKey::from_versioned_key(key(field(o, "key")?)?),
        name: checked(WireName::new(required_str(o, "name")?))?,
        description: text(field(o, "description")?)?,
        input_schema_ref: schema_ref(field(o, "input_schema_ref")?)?,
        input_schema: json_from_serde(field(o, "input_schema")?.clone())?,
        strict: boolean(o, "strict")?,
    })
}
pub(super) fn schema_ref(v: &Value) -> Result<SchemaRef> {
    let o = object(v)?;
    Ok(SchemaRef {
        schema_key: key(field(o, "schema_key")?)?,
        schema_hash: checked(Sha256::from_bytes(decode_hex_32(required_str(
            o,
            "schema_hash",
        )?)?))?,
    })
}
pub(super) fn runtime_event(v: &Value) -> Result<CanonicalRuntimeEvent> {
    let o = object(v)?;
    if required_str(o, "kind")? == "tool_approval_resolved" {
        use musteragent_runtime_model::{ToolApprovalObservation, ToolApprovalResolution};
        if o.len() != 8 {
            return Err(ContextPlanError::invariant());
        }
        let observation = ToolApprovalObservation {
            invocation_id: id(o, "invocation_id")?,
            tool_call_id: checked(musteragent_identity::ToolCallId::new(required_str(
                o,
                "tool_call_id",
            )?))?,
            tool_key: ToolKey::from_versioned_key(key(field(o, "tool_ref")?)?),
            interaction_id: id(o, "interaction_id")?,
            request_record_id: id(o, "request_record_id")?,
            resolution_record_id: id(o, "resolution_record_id")?,
            resolution: ToolApprovalResolution::parse(required_str(o, "resolution")?)
                .ok_or_else(ContextPlanError::invariant)?,
        };
        if observation.request_record_id == observation.resolution_record_id {
            return Err(ContextPlanError::invariant());
        }
        return Ok(CanonicalRuntimeEvent::ToolApprovalResolved(observation));
    }
    if required_str(o, "kind")? == "public_delivery_repair" {
        use base64::Engine;
        let encoded = required_str(o, "binding_protobuf_base64")?;
        if encoded.len() > crate::public_delivery_repair_codec::MAX_PUBLIC_REPAIR_METADATA_BYTES * 2
        {
            return Err(ContextPlanError::invariant());
        }
        let bytes = checked(base64::engine::general_purpose::STANDARD.decode(encoded))?;
        return Ok(CanonicalRuntimeEvent::PublicDeliveryRepair {
            source_invocation_id: id(o, "source_invocation_id")?,
            binding: checked(
                crate::public_delivery_repair_codec::decode_public_delivery_repair_binding(&bytes),
            )?,
        });
    }
    if required_str(o, "kind")? == "agent_dialogue_history" {
        if required_str(o, "source_contract_ref")?
            != musteragent_execution_context::DIALOGUE_CONTEXT_SOURCE_CONTRACT
        {
            return Err(ContextPlanError::invariant());
        }
        let frozen_cut = musteragent_execution_context::DialogueContextCut {
            tenant_id: id(o, "tenant_id")?,
            owner_id: id(o, "owner_id")?,
            agent_id: id(o, "agent_id")?,
            dialogue_id: id(o, "dialogue_id")?,
            through: musteragent_execution_context::DialogueCursor {
                epoch: required_u64(o, "epoch")?,
                seq: required_u64(o, "frozen_through_seq")?,
            },
            entry_count: required_u64(o, "entry_count")?,
            source_chain_sha256: checked(Sha256::from_bytes(decode_hex_32(required_str(
                o,
                "source_chain_sha256",
            )?)?))?,
        };
        checked(frozen_cut.validate())?;
        let from_seq = required_u64(o, "from_seq")?;
        let through_seq = required_u64(o, "through_seq")?;
        if from_seq == 0 || from_seq > through_seq || through_seq > frozen_cut.through.seq {
            return Err(ContextPlanError::invariant());
        }
        return Ok(CanonicalRuntimeEvent::AgentDialogueHistory {
            frozen_cut,
            from_seq,
            through_seq,
            source_page_ref: decode_content_ref(field(o, "source_page_ref")?)?,
            materialized_context: text(field(o, "materialized_context")?)?,
            input_attachments: match o.get("input_attachments") {
                None => Vec::new(),
                Some(value) => {
                    use base64::Engine;
                    use prost::Message;
                    let values = value
                        .as_array()
                        .filter(|v| !v.is_empty() && v.len() <= 4096)
                        .ok_or_else(ContextPlanError::invariant)?;
                    values
                        .iter()
                        .map(|value| {
                            let bytes = checked(
                                base64::engine::general_purpose::STANDARD.decode(string(value)?),
                            )?;
                            let wire =
                                checked(crate::wire::DialogueInputModelObservation::decode(
                                    bytes.as_slice(),
                                ))?;
                            let input = checked(
                                crate::dialogue_context_codec::decode_input_observation(wire),
                            )?;
                            if crate::dialogue_context_codec::input_observation(&input)
                                .encode_to_vec()
                                != bytes
                            {
                                return Err(ContextPlanError::invariant());
                            }
                            Ok(input)
                        })
                        .collect::<Result<_>>()?
                }
            },
        });
    }
    if required_str(o, "kind")? == "native_publication_required" {
        return Ok(CanonicalRuntimeEvent::NativePublicationRequired {
            source_invocation_id: id(o, "source_invocation_id")?,
        });
    }
    if required_str(o, "kind")? == "model_recovery" {
        return Ok(CanonicalRuntimeEvent::ModelRecovery {
            source_invocation_id: id(o, "source_invocation_id")?,
        });
    }
    if required_str(o, "kind")? == "output_limit_continuation" {
        return Ok(CanonicalRuntimeEvent::OutputLimitContinuation {
            source_invocation_id: id(o, "source_invocation_id")?,
        });
    }
    if required_str(o, "kind")? != "delegation_completion" {
        return Err(ContextPlanError::invariant());
    }
    Ok(CanonicalRuntimeEvent::DelegationCompletion {
        delivery_id: id(o, "delivery_id")?,
        delegation_id: id(o, "delegation_id")?,
        child_activity_id: id(o, "child_activity_id")?,
        child_run_id: id(o, "child_run_id")?,
        report: text(field(o, "report")?)?,
    })
}
