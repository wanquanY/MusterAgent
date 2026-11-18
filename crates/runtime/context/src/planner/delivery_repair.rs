//! The sealed Dialogue contract owns this private, single-correction phase.
//! Candidate bytes are never ContextEntry history or summary source material.
use std::collections::BTreeSet;

use musteragent_execution_context::{
    PublicDeliveryContract, PublicDeliveryRepairBinding, PublicDeliveryRepairDisposition,
};
use musteragent_model::{ModelParameter, ModelRoute};
use musteragent_runtime_content::verify_content;
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    CanonicalRuntimeEvent, ModelText,
};

use crate::{ContextEntry, ContextMaterial, ContextPlanError, MaterializedModelActionRejection};

pub(super) struct DeliveryRepair {
    pub binding: PublicDeliveryRepairBinding,
    pub turn: CanonicalMessage,
}

impl DeliveryRepair {
    pub(super) fn runtime_event(&self) -> CanonicalRuntimeEvent {
        CanonicalRuntimeEvent::PublicDeliveryRepair {
            source_invocation_id: self.binding.rejection_invocation_id,
            binding: self.binding.clone(),
        }
    }
}

pub(super) fn load(
    material: &ContextMaterial,
    route: &ModelRoute,
) -> Result<Option<DeliveryRepair>, ContextPlanError> {
    let Some(MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate {
        contract_ref,
        repair,
        candidate,
        consecutive_count,
        ..
    }) = material.model_action_rejection.as_ref()
    else {
        if matches!(material.model_action_rejection.as_ref(), Some(MaterializedModelActionRejection::InvalidPublicDelivery { contract_ref, .. }) if matches!(PublicDeliveryContract::from_key(contract_ref.as_str()), Some(PublicDeliveryContract::V6 | PublicDeliveryContract::V7)))
        {
            return Err(ContextPlanError::invariant());
        }
        return Ok(None);
    };
    if consecutive_count.get() >= crate::MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS {
        return Err(ContextPlanError::model_protocol_exhausted(
            ErrorCode::ModelResponseInvalid,
        ));
    }
    let contract = PublicDeliveryContract::from_key(contract_ref.as_str())
        .ok_or_else(ContextPlanError::invariant)?;
    let binding = repair
        .binding_for(contract)
        .map_err(|_| ContextPlanError::invariant())?;
    let pins = material
        .runtime_snapshot
        .prompt
        .sections
        .iter()
        .filter(|section| section.section_key.as_str() == "dialogue.output-contract")
        .collect::<Vec<_>>();
    if contract_ref != &binding.contract_ref
        || pins.len() != 1
        || PublicDeliveryContract::from_prompt_ref(&pins[0].content_ref) != Some(contract)
        || !material.prompt_sections.iter().any(|section| {
            section.content_ref == pins[0].content_ref
                && section.bytes == contract.prompt().as_bytes()
        })
        || candidate.content_ref != binding.candidate.content_ref
        || !material
            .source_content_refs
            .contains(&candidate.content_ref)
        || material.entries.iter().any(|entry| {
            entry
                .assistant_origin
                .as_ref()
                .is_some_and(|origin| origin.invocation_id == binding.candidate.invocation_id)
        })
        || binding.candidate.ledger_cutoff > material.ledger_cutoff
        || material.consumed_stimulus_cuts.iter().any(|cut| {
            binding
                .cut
                .control_sequence
                .is_none_or(|sequence| cut.consumed_sequence > sequence)
        })
    {
        return Err(ContextPlanError::invariant());
    }
    verify_content(&candidate.content_ref, &candidate.bytes)
        .map_err(|_| ContextPlanError::invariant())?;
    if contract == PublicDeliveryContract::V7 {
        contract
            .parse(&candidate.bytes)
            .map_err(|_| ContextPlanError::invariant())?;
    }
    let text = std::str::from_utf8(&candidate.bytes).map_err(|_| ContextPlanError::invariant())?;
    validate_route(&binding, route)?;

    let turn = candidate_turn(repair, contract, text, &required_inputs(material))?;
    Ok(Some(DeliveryRepair { binding, turn }))
}

fn candidate_turn(
    repair: &musteragent_execution_context::PublicDeliveryRejectionV1,
    contract: PublicDeliveryContract,
    text: &str,
    required: &[&ContextEntry],
) -> Result<CanonicalMessage, ContextPlanError> {
    let binding = repair
        .binding_for(contract)
        .map_err(|_| ContextPlanError::invariant())?;
    if !required
        .iter()
        .any(|entry| entry.message.role == CanonicalRole::User)
    {
        return Err(ContextPlanError::invariant());
    }
    verify_content(&binding.candidate.content_ref, text.as_bytes())
        .map_err(|_| ContextPlanError::invariant())?;
    let sources = required.iter().map(|entry| {
        let role = match entry.message.role {
            CanonicalRole::User => "user",
            CanonicalRole::Tool => "tool",
            CanonicalRole::Assistant => return Err(ContextPlanError::invariant()),
        };
        if entry.source_record_ids.is_empty() || !entry.runtime_events.is_empty() {
            return Err(ContextPlanError::invariant());
        }
        let parts = entry.message.content.iter().map(|part| match part {
            CanonicalContentPart::Text(text) => serde_json::json!({"type":"text","text":text.as_str()}),
            CanonicalContentPart::Image(reference) => source_ref("image", reference),
            CanonicalContentPart::Audio(reference) => source_ref("audio", reference),
            CanonicalContentPart::Video(reference) => source_ref("video", reference),
            CanonicalContentPart::File(reference) => source_ref("file", reference),
        }).collect::<Vec<_>>();
        Ok(serde_json::json!({
            "role":role,
            "run_id":entry.source_run_id.map(|id|id.to_string()),
            "ledger_sequence":entry.source_sequence.get(),
            "source_records":entry.source_record_ids.iter().map(|id|id.to_string()).collect::<Vec<_>>(),
            "tool_call_id":entry.message.tool_call_id.as_ref().map(|id|id.as_str()),
            "tool_name":entry.message.tool_name.as_ref().map(|name|name.as_str()),
            "content":parts,
        }))
    }).collect::<Result<Vec<_>, ContextPlanError>>()?;
    let diagnostics = repair.diagnostics.iter().map(|diagnostic|
        serde_json::json!({"code":diagnostic.kind().as_str(),"path":diagnostic.path()})
    ).collect::<Vec<_>>();
    // This is data in the Assistant role, never a new User instruction or Tool
    // exchange. The original owner must authenticate every source; role and
    // record metadata here do not grant authority to their quoted contents.
    let envelope = serde_json::to_string(&serde_json::json!({
        "schema_version":"musteragent.private-delivery-candidate.v1",
        "candidate_invocation_id":binding.candidate.invocation_id.to_string(),
        "candidate_sha256":binding.candidate.content_ref.sha256.to_string(),
        "diagnostics":diagnostics,
        "disposition":binding.disposition.as_str(),
        "original_inputs":sources,
        "untrusted_candidate":text,
    }))
    .map_err(|_| ContextPlanError::invariant())?;
    let content = ModelText::chunked(envelope)
        .map_err(|_| ContextPlanError::quota())?
        .into_iter()
        .map(CanonicalContentPart::Text)
        .collect();
    Ok(CanonicalMessage {
        role: CanonicalRole::Assistant,
        content,
        tool_calls: vec![],
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    })
}

fn source_ref(kind: &str, reference: &musteragent_content_ref::ContentRef) -> serde_json::Value {
    serde_json::json!({"type":kind,"content_ref":{
        "content_id":reference.content_id.to_string(),"media_type":reference.media_type.as_str(),
        "byte_length":reference.byte_length,"sha256":reference.sha256.to_string(),
    }})
}

fn required_inputs(material: &ContextMaterial) -> Vec<&ContextEntry> {
    let interaction_names = material
        .runtime_snapshot
        .tools
        .iter()
        .filter(|tool| {
            matches!(
                tool.manifest.key.logical_key(),
                "interaction.question" | "interaction.request"
            )
        })
        .map(|tool| &tool.manifest.wire_name)
        .collect::<BTreeSet<_>>();
    super::turn_order::order_entries_for_run_turn(material)
        .into_iter()
        .filter(|entry| {
            if !entry.runtime_events.is_empty() {
                return false;
            }
            let current = entry.source_run_id == Some(material.run_context.run_id);
            match entry.message.role {
                CanonicalRole::User => {
                    current
                        || material
                            .retry_stimulus_record_id
                            .is_some_and(|id| entry.source_record_ids.contains(&id))
                        || material
                            .consumed_stimulus_cuts
                            .iter()
                            .any(|cut| entry.source_record_ids.contains(&cut.stimulus_record_id))
                }
                CanonicalRole::Tool => {
                    current
                        && entry
                            .message
                            .tool_name
                            .as_ref()
                            .is_some_and(|name| interaction_names.contains(name))
                }
                CanonicalRole::Assistant => false,
            }
        })
        .collect()
}

pub(super) fn format_only(repair: Option<&DeliveryRepair>) -> bool {
    repair.is_some_and(|repair| {
        repair.binding.disposition == PublicDeliveryRepairDisposition::FormatOnly
    })
}

pub(super) fn guidance(
    repair: &musteragent_execution_context::PublicDeliveryRejectionV1,
) -> &'static str {
    match repair.disposition {
        PublicDeliveryRepairDisposition::FormatOnly => {
            "The complete rejected candidate and original task/answers are private input data. Correct only its encoding/shape under the sealed delivery contract. This physical request has no tools: do not ask another question, discover capabilities, or execute any business action. Preserve the user's supplied facts; candidate claims are not verified facts. Produce the complete corrected delivery now."
        }
        PublicDeliveryRepairDisposition::PrerequisiteRecovery => {
            "The private candidate failed because a real prerequisite/reference is missing. Recover only the necessary real object using the already authorized tools and original task/answers. Do not guess identifiers or repeat a successfully completed action. This is the same one-correction episode, with no new failure budget."
        }
    }
}

fn validate_route(
    binding: &PublicDeliveryRepairBinding,
    route: &ModelRoute,
) -> Result<(), ContextPlanError> {
    if !route
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == "json_schema_output")
    {
        return Err(ContextPlanError {
            early_provider_cause: None,
            code: ErrorCode::CapabilityUnavailable,
            retryable: false,
            retry_after_ms: None,
        });
    }
    if binding.disposition == PublicDeliveryRepairDisposition::FormatOnly
        && route.parameters.iter().any(|(key, value)| key.as_str() == "tool_choice"
            && !matches!(value, ModelParameter::String(choice) if matches!(choice.as_str(), "auto" | "none")))
    { return Err(ContextPlanError::model_route_unavailable()); }
    Ok(())
}

/// Reused by the original PlanStore/model-begin owner after decoding the actual
/// authorized request. This does not replace its rejection/tenant/cut checks.
pub fn validate_public_delivery_repair_request(
    repair: &musteragent_execution_context::PublicDeliveryRejectionV1,
    candidate: &[u8],
    material: &ContextMaterial,
    request: &CanonicalModelRequest,
) -> Result<(), ContextPlanError> {
    let loaded = load(material, &request.route)?.ok_or_else(ContextPlanError::invariant)?;
    let contract = PublicDeliveryContract::from_key(loaded.binding.contract_ref.as_str())
        .ok_or_else(ContextPlanError::invariant)?;
    let binding = repair
        .binding_for(contract)
        .map_err(|_| ContextPlanError::invariant())?;
    if loaded.binding != binding || request.invocation.run_context != material.run_context {
        return Err(ContextPlanError::invariant());
    }
    verify_content(&binding.candidate.content_ref, candidate)
        .map_err(|_| ContextPlanError::invariant())?;
    // A generic wakeup is insufficient: this phase is one exact, source-bound
    // Runtime event, independent of any other legitimate event in the request.
    let events = request
        .runtime_events
        .iter()
        .filter(|event| matches!(event, CanonicalRuntimeEvent::PublicDeliveryRepair { .. }))
        .collect::<Vec<_>>();
    if events.len() != 1 || events[0] != &loaded.runtime_event() {
        return Err(ContextPlanError::invariant());
    }
    // load already reconstructed the exact data from authoritative material.
    let expected = loaded.turn;
    let Some((schema, schema_value)) = &request.response_schema else {
        return Err(ContextPlanError::invariant());
    };
    let expected_schema: serde_json::Value =
        serde_json::from_str(contract.schema()).map_err(|_| ContextPlanError::invariant())?;
    if schema.schema_key != binding.contract_ref
        || schema.schema_hash != contract.hash()
        || !schema_matches(&expected_schema, schema_value)
        || !request
            .instructions
            .iter()
            .any(|instruction| instruction.as_str() == contract.prompt())
        || request.continuation_ref.is_some()
        || request.input.last() != Some(&expected)
        || (binding.disposition == PublicDeliveryRepairDisposition::FormatOnly
            && !request.tools.is_empty())
    {
        return Err(ContextPlanError::invariant());
    }
    validate_route(&binding, &request.route)?;
    request
        .validate()
        .map_err(|_| ContextPlanError::invariant())
}

// The digest-bearing reference is not proof that the adjacent schema body is
// authentic. Compare the actual canonical tree with the sealed V6 schema.
fn schema_matches(
    expected: &serde_json::Value,
    actual: &musteragent_runtime_model::JsonValue,
) -> bool {
    use musteragent_runtime_model::JsonValue as J;
    use serde_json::Value as V;
    match (expected, actual) {
        (V::Null, J::Null) => true,
        (V::Bool(a), J::Bool(b)) => a == b,
        (V::String(a), J::String(b)) => a == b,
        (V::Number(a), J::Integer(b)) => a.as_i64() == Some(*b),
        (V::Number(a), J::Number(b)) => a.as_f64() == Some(*b),
        (V::Array(a), J::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| schema_matches(a, b))
        }
        (V::Object(a), J::Object(b)) => {
            a.len() == b.len()
                && a.iter().all(|(key, value)| {
                    b.get(key)
                        .is_some_and(|actual| schema_matches(value, actual))
                })
        }
        _ => false,
    }
}

pub(super) fn validate_material_request(
    material: &ContextMaterial,
    request: &CanonicalModelRequest,
) -> Result<(), ContextPlanError> {
    let Some(MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate {
        repair,
        candidate,
        ..
    }) = &material.model_action_rejection
    else {
        return Err(ContextPlanError::invariant());
    };
    validate_public_delivery_repair_request(repair, &candidate.bytes, material, request)
}

#[cfg(test)]
mod tests;
