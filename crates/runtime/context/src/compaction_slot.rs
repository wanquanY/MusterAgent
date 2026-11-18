use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{ContextBranchId, ConversationSeq, LedgerRecordId, VersionedKey};
use musteragent_model::SignedModelRouteHash;
use musteragent_run::RunContext;
use musteragent_runtime_snapshot::{ModelStreamBudgetSnapshot, PinnedModelRoute};

use crate::{
    COMPACTION_HISTORY_CODEC_SCHEMA, COMPACTION_PROMPT_SCHEMA, ContextCompactionRequest,
    ContextMaterial, ContextPlanError,
};

pub(crate) fn validate_compaction_request(
    request: &ContextCompactionRequest,
) -> Result<(), ContextPlanError> {
    if matches!(
        request.slot.source,
        crate::CompactionSource::AfterControl(_)
    ) {
        let expected = request.slot.source.generation_ids(
            request.invocation.run_context.run_id,
            request.slot.loop_iteration,
            request.slot.generation,
        )?;
        if expected
            != (
                request.invocation.invocation_id,
                request.invocation.correlation_id,
            )
        {
            return Err(ContextPlanError::idempotency_conflict());
        }
    }
    if let Some(continuation) = &request.continuation
        && (!musteragent_runtime_snapshot::uses_compaction_output_continuation(
            request.slot.compaction_policy_ref.as_str(),
        ) || continuation.prefix.as_str().trim().is_empty())
    {
        return Err(ContextPlanError::invariant());
    }
    request.slot.source.validate()?;
    request
        .model
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    if !request.recovery_policy.is_valid()
        || request.serialized_history.is_empty()
        || request.summarized_record_ids.is_empty()
        || request
            .summarized_record_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != request.summarized_record_ids.len()
        || request.slot.loop_iteration == 0
        || request.slot.source_seq_from > request.slot.source_seq_to
        || request.slot.source_seq_to > request.slot.ledger_cutoff
        || request.slot.retained_record_ids.is_empty()
        || request.slot.input_budget == 0
        || request.slot.input_tokens_before == 0
        || request.slot.summary_budget == 0
        || request.invocation.run_context.run_id != request.invocation.attempt_context.run_id
        || std::str::from_utf8(&request.serialized_history).is_err()
    {
        return Err(ContextPlanError::invariant());
    }
    let expected = expected_compaction_plan_fingerprint(request)?;
    if expected == request.slot.plan_fingerprint {
        Ok(())
    } else {
        Err(ContextPlanError::idempotency_conflict())
    }
}

pub(crate) fn expected_compaction_plan_fingerprint(
    request: &ContextCompactionRequest,
) -> Result<Sha256, ContextPlanError> {
    compaction_plan_fingerprint_with_identity(
        request,
        request.slot.generation,
        request.slot.ledger_cutoff,
    )
}

pub(crate) fn compaction_plan_fingerprint_with_identity(
    request: &ContextCompactionRequest,
    generation: std::num::NonZeroU32,
    ledger_cutoff: ConversationSeq,
) -> Result<Sha256, ContextPlanError> {
    compaction_plan_fingerprint_with_observation(
        request,
        generation,
        ledger_cutoff,
        request.slot.input_tokens_before,
    )
}

/// Reconstructs the unchanged v1 identity using the observation sealed by its
/// original owner. This never removes a field from the complete fingerprint.
pub(crate) fn compaction_plan_fingerprint_with_observation(
    request: &ContextCompactionRequest,
    generation: std::num::NonZeroU32,
    ledger_cutoff: ConversationSeq,
    input_tokens_before: u64,
) -> Result<Sha256, ContextPlanError> {
    let base = compaction_plan_fingerprint(&CompactionPlanFingerprintInput {
        source: &request.slot.source,
        run_context: &request.invocation.run_context,
        loop_iteration: request.slot.loop_iteration,
        generation,
        active_branch_id: request.slot.active_branch_id,
        ledger_cutoff,
        source_seq_from: request.slot.source_seq_from,
        source_seq_to: request.slot.source_seq_to,
        summarized_record_ids: &request.summarized_record_ids,
        serialized_history: &request.serialized_history,
        retained_record_ids: &request.slot.retained_record_ids,
        previous_summary_ref: request.slot.previous_summary_ref.as_ref(),
        model: &request.model,
        stream_budget: request.stream_budget,
        input_budget: request.slot.input_budget,
        input_tokens_before,
        summary_budget: request.slot.summary_budget,
        max_output_tokens: request.max_output_tokens,
        compaction_policy_ref: &request.slot.compaction_policy_ref,
        causation_id: request.invocation.causation_id,
    })?;
    let Some(continuation) = &request.continuation else {
        return Ok(base);
    };
    let mut encoded = b"musteragent.compaction-continuation.v1\0".to_vec();
    field(&mut encoded, &base.into_bytes());
    field(
        &mut encoded,
        continuation.parent_invocation_id.into_uuid().as_bytes(),
    );
    field(&mut encoded, continuation.prefix.as_str().as_bytes());
    Ok(Sha256::digest(encoded))
}

pub(crate) fn last_causation_id(
    material: &ContextMaterial,
) -> Result<LedgerRecordId, ContextPlanError> {
    material
        .entries
        .last()
        .and_then(|entry| entry.source_record_ids.last())
        .copied()
        .ok_or_else(ContextPlanError::invariant)
}

pub(crate) struct CompactionPlanFingerprintInput<'a> {
    pub source: &'a crate::CompactionSource,
    pub run_context: &'a RunContext,
    pub loop_iteration: u32,
    pub generation: std::num::NonZeroU32,
    pub active_branch_id: Option<ContextBranchId>,
    pub ledger_cutoff: ConversationSeq,
    pub source_seq_from: ConversationSeq,
    pub source_seq_to: ConversationSeq,
    pub summarized_record_ids: &'a [LedgerRecordId],
    pub serialized_history: &'a [u8],
    pub retained_record_ids: &'a [LedgerRecordId],
    pub previous_summary_ref: Option<&'a ContentRef>,
    pub model: &'a PinnedModelRoute,
    pub stream_budget: ModelStreamBudgetSnapshot,
    pub input_budget: u64,
    pub input_tokens_before: u64,
    pub summary_budget: u64,
    pub max_output_tokens: std::num::NonZeroU64,
    pub compaction_policy_ref: &'a VersionedKey,
    pub causation_id: LedgerRecordId,
}

pub(crate) fn compaction_plan_fingerprint(
    input: &CompactionPlanFingerprintInput<'_>,
) -> Result<Sha256, ContextPlanError> {
    if input.loop_iteration == 0
        || input.source_seq_from > input.source_seq_to
        || input.source_seq_to > input.ledger_cutoff
        || input.summarized_record_ids.is_empty()
        || input.retained_record_ids.is_empty()
        || input.serialized_history.is_empty()
        || input.summary_budget == 0
    {
        return Err(ContextPlanError::invariant());
    }
    let route_hash = SignedModelRouteHash::digest(&input.model.route)
        .map_err(|_| ContextPlanError::invariant())?;
    let mut encoded = Vec::with_capacity(1024);
    field(
        &mut encoded,
        b"musteragent.legacy.context-compaction-slot-plan.v1",
    );
    field(
        &mut encoded,
        input.run_context.tenant_id.to_string().as_bytes(),
    );
    field(
        &mut encoded,
        input.run_context.conversation_id.to_string().as_bytes(),
    );
    field(
        &mut encoded,
        input.run_context.run_id.to_string().as_bytes(),
    );
    field(
        &mut encoded,
        input.run_context.runtime_snapshot_id.to_string().as_bytes(),
    );
    number(&mut encoded, u64::from(input.loop_iteration));
    number(&mut encoded, u64::from(input.generation.get()));
    optional_uuid(
        &mut encoded,
        input.active_branch_id.map(ContextBranchId::into_uuid),
    );
    number(&mut encoded, input.ledger_cutoff.get());
    number(&mut encoded, input.source_seq_from.get());
    number(&mut encoded, input.source_seq_to.get());
    record_ids(&mut encoded, input.summarized_record_ids)?;
    field(
        &mut encoded,
        &Sha256::digest(input.serialized_history).into_bytes(),
    );
    record_ids(&mut encoded, input.retained_record_ids)?;
    content_ref(&mut encoded, input.previous_summary_ref);
    field(
        &mut encoded,
        input.model.route.route_ref.as_str().as_bytes(),
    );
    field(&mut encoded, route_hash.as_bytes());
    field(
        &mut encoded,
        input.model.credential_grant_ref.as_str().as_bytes(),
    );
    number(
        &mut encoded,
        input.stream_budget.max_event_bytes.get() as u64,
    );
    number(
        &mut encoded,
        u64::from(input.stream_budget.max_events.get()),
    );
    number(&mut encoded, input.stream_budget.max_total_bytes.get());
    number(
        &mut encoded,
        u64::from(input.stream_budget.max_tool_calls.get()),
    );
    number(
        &mut encoded,
        input.stream_budget.max_tool_argument_bytes.get() as u64,
    );
    number(
        &mut encoded,
        input.stream_budget.max_reasoning_replay_bytes.get() as u64,
    );
    number(&mut encoded, input.input_budget);
    number(&mut encoded, input.input_tokens_before);
    number(&mut encoded, input.summary_budget);
    number(&mut encoded, input.max_output_tokens.get());
    field(
        &mut encoded,
        input.compaction_policy_ref.as_str().as_bytes(),
    );
    field(&mut encoded, COMPACTION_PROMPT_SCHEMA.as_bytes());
    field(&mut encoded, COMPACTION_HISTORY_CODEC_SCHEMA.as_bytes());
    field(&mut encoded, input.causation_id.into_uuid().as_bytes());
    if !matches!(input.source, crate::CompactionSource::RuntimeLedger) {
        field(&mut encoded, input.source.key().as_bytes());
    }
    Ok(Sha256::digest(encoded))
}

fn record_ids(
    encoded: &mut Vec<u8>,
    record_ids: &[LedgerRecordId],
) -> Result<(), ContextPlanError> {
    number(
        encoded,
        u64::try_from(record_ids.len()).map_err(|_| ContextPlanError::quota())?,
    );
    for record_id in record_ids {
        field(encoded, record_id.into_uuid().as_bytes());
    }
    Ok(())
}

fn content_ref(encoded: &mut Vec<u8>, reference: Option<&ContentRef>) {
    match reference {
        Some(reference) => {
            encoded.push(1);
            field(encoded, reference.content_id.to_string().as_bytes());
            field(encoded, reference.media_type.as_str().as_bytes());
            number(encoded, reference.byte_length);
            field(encoded, &reference.sha256.into_bytes());
        }
        None => encoded.push(0),
    }
}

fn optional_uuid(encoded: &mut Vec<u8>, value: Option<uuid::Uuid>) {
    match value {
        Some(value) => {
            encoded.push(1);
            field(encoded, value.as_bytes());
        }
        None => encoded.push(0),
    }
}

fn number(encoded: &mut Vec<u8>, value: u64) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

fn field(encoded: &mut Vec<u8>, value: &[u8]) {
    number(encoded, u64::try_from(value.len()).unwrap_or(u64::MAX));
    encoded.extend_from_slice(value);
}
