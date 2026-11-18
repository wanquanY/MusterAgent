use std::num::NonZeroU32;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{ContextSnapshotId, ConversationSeq, LedgerRecordId, RunId};
use musteragent_runtime_model::{JsonValue, ModelRouteRef};

use crate::{ContextCodec, ContextMaterial, ContextPlanError, ContextSnapshot};

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_logical_context_snapshot(
    codec: &dyn ContextCodec,
    request_bytes: &[u8],
    summary_bytes: &[u8],
    generation: NonZeroU32,
    run_id: RunId,
    iteration: u32,
    snapshot_id: ContextSnapshotId,
    parent_snapshot_id: Option<ContextSnapshotId>,
    material: &ContextMaterial,
    delivery_repair: Option<musteragent_execution_context::PublicDeliveryRepairBinding>,
    retained_record_ids: Vec<LedgerRecordId>,
    structured_summary_ref: ContentRef,
    model_request_ref: ContentRef,
    input_tokens_before: u64,
    input_tokens_after: u64,
    model_route_ref: ModelRouteRef,
) -> Result<ContextSnapshot, ContextPlanError> {
    // The material span describes retained/history entries. A repair also
    // reads a later private output through its verified Runtime binding. Cover
    // that input in the plan's provenance without changing summary provenance
    // or manufacturing a public ContextEntry. The original ledger fence still
    // bounds every source; unrelated later ledger records are not included.
    let mut source_seq_to = match delivery_repair.as_ref() {
        Some(binding) => {
            if binding.candidate.ledger_cutoff > material.ledger_cutoff {
                return Err(ContextPlanError::invariant());
            }
            material.source_seq_to.max(binding.candidate.ledger_cutoff)
        }
        None => material.source_seq_to,
    };
    let plan_lineage = material.plan_selection.and_then(|s| s.lineage);
    if let Some(selection) = material.plan_selection {
        selection.validate()?;
        if selection.generation() != generation {
            return Err(ContextPlanError::invariant());
        }
        if let Some(control) = selection.source_control {
            if control.sequence > material.ledger_cutoff {
                return Err(ContextPlanError::invariant());
            }
            // Preserve exact legacy replay; only the forward protocol expands
            // provenance to the private ControlConsumed event.
            if plan_lineage.is_some() {
                source_seq_to = source_seq_to.max(control.sequence);
            }
        }
    } else if generation != crate::CONTEXT_PLAN_GENERATION {
        return Err(ContextPlanError::invariant());
    }
    let checksum = logical_context_plan_checksum(
        codec,
        request_bytes,
        summary_bytes,
        generation,
        run_id,
        iteration,
        parent_snapshot_id,
        material.source_seq_from,
        source_seq_to,
        &retained_record_ids,
        input_tokens_before,
        input_tokens_after,
        model_route_ref.as_str(),
        delivery_repair.as_ref(),
    )?;
    let checksum = with_plan_lineage_checksum(checksum, generation, plan_lineage.as_ref())?;
    let snapshot = ContextSnapshot {
        plan_lineage,
        delivery_repair,
        snapshot_id,
        run_id,
        iteration,
        parent_snapshot_id,
        source_seq_from: material.source_seq_from,
        source_seq_to,
        retained_record_ids,
        structured_summary_ref,
        model_request_ref,
        input_tokens_before,
        input_tokens_after,
        model_route_ref,
        checksum,
    };
    snapshot
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    Ok(snapshot)
}

pub(crate) fn with_plan_lineage_checksum(
    legacy: Sha256,
    generation: NonZeroU32,
    lineage: Option<&crate::ContextPlanLineage>,
) -> Result<Sha256, ContextPlanError> {
    let Some(lineage) = lineage else {
        return if generation == crate::CONTEXT_PLAN_GENERATION {
            Ok(legacy)
        } else {
            Err(ContextPlanError::invariant())
        };
    };
    lineage.validate()?;
    if lineage.generation != generation {
        return Err(ContextPlanError::invariant());
    }
    let mut encoded = Vec::new();
    bytes(&mut encoded, b"musteragent.context-plan-lineage.v1");
    bytes(&mut encoded, &legacy.into_bytes());
    number(&mut encoded, u64::from(generation.get()));
    match lineage.predecessor {
        None => encoded.push(0),
        Some(previous) => {
            encoded.push(1);
            bytes(&mut encoded, previous.snapshot_id.into_uuid().as_bytes());
            number(&mut encoded, u64::from(previous.generation.get()));
            bytes(&mut encoded, &previous.checksum.into_bytes());
        }
    }
    match lineage.control {
        None => encoded.push(0),
        Some(control) => {
            encoded.push(1);
            number(&mut encoded, control.sequence.get());
            bytes(&mut encoded, control.record_id.into_uuid().as_bytes());
            encoded.push(match control.kind {
                musteragent_run::RunControlKind::Steering => 1,
                musteragent_run::RunControlKind::FollowUp => 2,
            });
        }
    }
    Ok(Sha256::digest(encoded))
}

/// Computes the logical plan fingerprint independently from the physical Attempt lease.
///
/// The canonical request remains immutable physical provenance. Only its ephemeral
/// Attempt and legacy physical deadline are removed so a takeover can resolve
/// the original logical plan. Activity policy and business-deadline provenance
/// are immutable logical intent and remain part of the new fingerprint.
#[allow(clippy::too_many_arguments)]
pub(crate) fn logical_context_plan_checksum(
    codec: &dyn ContextCodec,
    request_bytes: &[u8],
    summary_bytes: &[u8],
    generation: NonZeroU32,
    run_id: RunId,
    iteration: u32,
    parent_snapshot_id: Option<ContextSnapshotId>,
    source_seq_from: ConversationSeq,
    source_seq_to: ConversationSeq,
    retained_record_ids: &[LedgerRecordId],
    input_tokens_before: u64,
    input_tokens_after: u64,
    model_route_ref: &str,
    delivery_repair: Option<&musteragent_execution_context::PublicDeliveryRepairBinding>,
) -> Result<Sha256, ContextPlanError> {
    let LogicalRequest {
        value: request,
        activity,
        repair_schema,
    } = normalize_logical_request(codec, request_bytes)?;
    if repair_schema != delivery_repair.is_some() {
        return Err(ContextPlanError::invariant());
    }

    let mut encoded = Vec::new();
    bytes(
        &mut encoded,
        if activity {
            b"musteragent.context-plan.logical.v2"
        } else {
            b"musteragent.legacy.context-plan.logical.v1"
        },
    );
    bytes(&mut encoded, run_id.into_uuid().as_bytes());
    number(&mut encoded, u64::from(generation.get()));
    number(&mut encoded, u64::from(iteration));
    match parent_snapshot_id {
        Some(snapshot_id) => {
            encoded.push(1);
            bytes(&mut encoded, snapshot_id.into_uuid().as_bytes());
        }
        None => encoded.push(0),
    }
    number(&mut encoded, source_seq_from.get());
    number(&mut encoded, source_seq_to.get());
    number(
        &mut encoded,
        u64::try_from(retained_record_ids.len()).map_err(|_| ContextPlanError::invariant())?,
    );
    for record_id in retained_record_ids {
        bytes(&mut encoded, record_id.into_uuid().as_bytes());
    }
    bytes(&mut encoded, summary_bytes);
    number(&mut encoded, input_tokens_before);
    number(&mut encoded, input_tokens_after);
    bytes(&mut encoded, model_route_ref.as_bytes());
    encode_json_fingerprint(&mut encoded, &request)?;
    if let Some(binding) = delivery_repair {
        encode_delivery_repair(&mut encoded, binding)?;
    }
    Ok(Sha256::digest(encoded))
}

/// Checks the complete sealed logical request using the same normalization as
/// the immutable plan checksum. The Store remains responsible for authenticating
/// both ContentRefs, the current plan, and the canonical request contract.
/// Only physical Attempt ownership and the legacy physical deadline may differ.
pub fn validate_context_plan_request(
    codec: &dyn ContextCodec,
    stored_bytes: &[u8],
    candidate_bytes: &[u8],
) -> Result<(), ContextPlanError> {
    let fingerprint = |bytes: &[u8]| {
        let request = normalize_logical_request(codec, bytes)?;
        let mut encoded = Vec::new();
        encode_json_fingerprint(&mut encoded, &request.value)?;
        Ok::<_, ContextPlanError>(Sha256::digest(encoded))
    };
    if fingerprint(stored_bytes)? != fingerprint(candidate_bytes)? {
        return Err(ContextPlanError::idempotency_conflict());
    }
    Ok(())
}

struct LogicalRequest {
    value: JsonValue,
    activity: bool,
    repair_schema: bool,
}

fn normalize_logical_request(
    codec: &dyn ContextCodec,
    request_bytes: &[u8],
) -> Result<LogicalRequest, ContextPlanError> {
    let mut request = codec.decode_json(request_bytes)?;
    let JsonValue::Object(root) = &mut request else {
        return Err(ContextPlanError::invariant());
    };
    let dialogue_input = matches!(root.get("schema_version"), Some(JsonValue::String(schema))
        if matches!(schema.as_str(), "musteragent.canonical-model-request.v6" | "musteragent.canonical-model-request.v7" | "musteragent.canonical-model-request.v8"));
    let repair_schema = if dialogue_input {
        let Some(JsonValue::Array(events)) = root.get("runtime_events") else {
            return Err(ContextPlanError::invariant());
        };
        let mut repairs = 0;
        for event in events {
            let JsonValue::Object(event) = event else {
                return Err(ContextPlanError::invariant());
            };
            if matches!(event.get("kind"), Some(JsonValue::String(kind)) if kind == "public_delivery_repair")
            {
                if !matches!(event.get("binding_protobuf_base64"), Some(JsonValue::String(binding)) if !binding.is_empty())
                {
                    return Err(ContextPlanError::invariant());
                }
                repairs += 1;
            }
        }
        if repairs > 1 {
            return Err(ContextPlanError::invariant());
        }
        repairs == 1
    } else {
        matches!(root.get("schema_version"), Some(JsonValue::String(schema))
            if schema == "musteragent.canonical-model-request.v5")
    };
    let activity = match root.get("schema_version") {
        Some(JsonValue::String(schema))
            if matches!(
                schema.as_str(),
                "musteragent.canonical-model-request.v5"
                    | "musteragent.canonical-model-request.v6"
                    | "musteragent.canonical-model-request.v7"
                    | "musteragent.canonical-model-request.v8"
            ) =>
        {
            matches!(root.get("invocation"), Some(JsonValue::Object(invocation))
                if invocation.contains_key("activity_timing_v1"))
        }
        Some(JsonValue::String(schema)) if schema == "musteragent.canonical-model-request.v4" => {
            true
        }
        Some(JsonValue::String(schema))
            if matches!(
                schema.as_str(),
                "musteragent.canonical-model-request.v2" | "musteragent.canonical-model-request.v3"
            ) =>
        {
            false
        }
        _ => return Err(ContextPlanError::invariant()),
    };
    let Some(JsonValue::Object(invocation)) = root.get_mut("invocation") else {
        return Err(ContextPlanError::invariant());
    };
    if invocation.remove("attempt_context").is_none() {
        return Err(ContextPlanError::invariant());
    }
    if activity {
        if invocation.contains_key("deadline_ms") || !invocation.contains_key("activity_timing_v1")
        {
            return Err(ContextPlanError::invariant());
        }
    } else if invocation.contains_key("activity_timing_v1")
        || invocation.remove("deadline_ms").is_none()
    {
        return Err(ContextPlanError::invariant());
    }

    Ok(LogicalRequest {
        value: request,
        activity,
        repair_schema,
    })
}

fn encode_json_fingerprint(
    encoded: &mut Vec<u8>,
    value: &JsonValue,
) -> Result<(), ContextPlanError> {
    match value {
        JsonValue::Null => encoded.push(0),
        JsonValue::Bool(false) => encoded.push(1),
        JsonValue::Bool(true) => encoded.push(2),
        JsonValue::Integer(value) => {
            encoded.push(3);
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        JsonValue::Number(value) if value.is_finite() => {
            encoded.push(4);
            encoded.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        JsonValue::Number(_) => return Err(ContextPlanError::invariant()),
        JsonValue::String(value) => {
            encoded.push(5);
            bytes(encoded, value.as_bytes());
        }
        JsonValue::Array(values) => {
            encoded.push(6);
            number(
                encoded,
                u64::try_from(values.len()).map_err(|_| ContextPlanError::invariant())?,
            );
            for value in values {
                encode_json_fingerprint(encoded, value)?;
            }
        }
        JsonValue::Object(values) => {
            encoded.push(7);
            number(
                encoded,
                u64::try_from(values.len()).map_err(|_| ContextPlanError::invariant())?,
            );
            for (key, value) in values {
                bytes(encoded, key.as_bytes());
                encode_json_fingerprint(encoded, value)?;
            }
        }
    }
    Ok(())
}

fn number(encoded: &mut Vec<u8>, value: u64) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

fn bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    number(
        encoded,
        u64::try_from(value.len()).expect("usize length fits in the u64 fingerprint domain"),
    );
    encoded.extend_from_slice(value);
}

// Absent bindings append zero bytes: all legacy logical fingerprints remain exact.
fn encode_delivery_repair(
    encoded: &mut Vec<u8>,
    binding: &musteragent_execution_context::PublicDeliveryRepairBinding,
) -> Result<(), ContextPlanError> {
    binding
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    bytes(
        encoded,
        b"musteragent.context-plan.private-delivery-repair.v1",
    );
    bytes(encoded, binding.contract_ref.as_str().as_bytes());
    bytes(encoded, &binding.contract_prompt_sha256.into_bytes());
    bytes(encoded, binding.repair_spec_ref.as_str().as_bytes());
    bytes(encoded, &binding.repair_spec_sha256.into_bytes());
    bytes(
        encoded,
        binding.rejection_invocation_id.into_uuid().as_bytes(),
    );
    let candidate = &binding.candidate;
    bytes(encoded, candidate.invocation_id.into_uuid().as_bytes());
    bytes(
        encoded,
        candidate.assistant_message_id.into_uuid().as_bytes(),
    );
    bytes(
        encoded,
        candidate.assistant_output_id.into_uuid().as_bytes(),
    );
    number(encoded, candidate.ledger_cutoff.get());
    bytes(
        encoded,
        candidate.content_ref.content_id.into_uuid().as_bytes(),
    );
    bytes(
        encoded,
        candidate.content_ref.media_type.as_str().as_bytes(),
    );
    number(encoded, candidate.content_ref.byte_length);
    bytes(encoded, &candidate.content_ref.sha256.into_bytes());
    bytes(encoded, &binding.diagnostic_hash.into_bytes());
    bytes(encoded, binding.disposition.as_str().as_bytes());
    for sequence in [binding.cut.progress_sequence, binding.cut.control_sequence] {
        match sequence {
            Some(sequence) => {
                encoded.push(1);
                number(encoded, sequence.get());
            }
            None => encoded.push(0),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
