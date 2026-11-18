use super::*;
use musteragent_context::{
    CompactionGenerationObservation as Observation, CompactionGenerationState as State,
    ContextPlanError,
};
use musteragent_identity::{ConversationSeq, FencingToken};
use std::num::NonZeroU32;

pub(in crate::agent_context_codec) fn encode(
    v: &Observation,
) -> Result<wire::AgentCompactionGeneration> {
    let (state, error_code, failure_retryable, retry_after_ms) = match &v.state {
        State::SealedNotDispatched => (1, None, false, None),
        State::DispatchedUnknown => (2, None, false, None),
        State::Succeeded => (3, None, false, None),
        State::Failed(e) => (
            4,
            Some(e.code.as_str().to_owned()),
            e.retryable,
            e.retry_after_ms,
        ),
        State::Cancelled => (5, None, false, None),
    };
    Ok(wire::AgentCompactionGeneration {
        early_provider_cause: match &v.state {
            State::Failed(error) => error
                .early_provider_cause
                .map(|cause| cause.as_str().to_owned()),
            _ => None,
        },
        generation: v.generation.get(),
        generation_high_watermark: v.generation_high_watermark.get(),
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        correlation_id: v.correlation_id.to_string(),
        plan_fingerprint: Some(hash_wire(v.plan_fingerprint)),
        logical_plan_fingerprint: v.logical_plan_fingerprint.map(hash_wire),
        sealed_ledger_cutoff: v.sealed_ledger_cutoff.map(|v| v.get()),
        sealed_input_tokens_before: v.sealed_input_tokens_before,
        owner_attempt_id: Some(wire::AttemptId {
            value: v.owner_attempt_id.to_string(),
        }),
        owner_fencing_token: v.owner_fencing_token.get(),
        deadline_ms: v
            .timing
            .legacy_deadline()
            .map_or(0, UtcTimestamp::unix_millis),
        activity_v1: crate::invocation_activity_timing_message(&v.timing),
        state,
        error_code,
        failure_retryable,
        retry_after_ms,
    })
}
pub(in crate::agent_context_codec) fn decode(
    v: wire::AgentCompactionGeneration,
) -> Result<Observation> {
    if v.sealed_input_tokens_before == Some(0)
        || (v.state != 4
            && (v.early_provider_cause.is_some()
                || v.error_code.is_some()
                || v.failure_retryable
                || v.retry_after_ms.is_some()))
    {
        return Err(AgentContextCodecError::Invalid);
    }
    let state = match v.state {
        1 => State::SealedNotDispatched,
        2 => State::DispatchedUnknown,
        3 => State::Succeeded,
        5 => State::Cancelled,
        4 => State::Failed(ContextPlanError {
            early_provider_cause: v
                .early_provider_cause
                .as_deref()
                .map(|cause| {
                    musteragent_runtime_model::ProviderErrorCode::from_alternate_provider_cause(
                        cause,
                    )
                    .ok_or(AgentContextCodecError::Invalid)
                })
                .transpose()?,
            code: required(musteragent_runtime_contract::ErrorCode::from_safe_code(
                &required(v.error_code)?,
            ))?,
            retryable: v.failure_retryable,
            retry_after_ms: v.retry_after_ms,
        }),
        _ => return Err(AgentContextCodecError::Invalid),
    };
    Ok(Observation {
        generation: required(NonZeroU32::new(v.generation))?,
        generation_high_watermark: required(NonZeroU32::new(v.generation_high_watermark))?,
        invocation_id: id(v.invocation_id.map(|v| v.value))?,
        correlation_id: id(Some(v.correlation_id))?,
        plan_fingerprint: hash(required(v.plan_fingerprint)?)?,
        logical_plan_fingerprint: v.logical_plan_fingerprint.map(hash).transpose()?,
        sealed_ledger_cutoff: v
            .sealed_ledger_cutoff
            .map(|v| checked(ConversationSeq::new(v)))
            .transpose()?,
        sealed_input_tokens_before: v.sealed_input_tokens_before,
        owner_attempt_id: id(v.owner_attempt_id.map(|v| v.value))?,
        owner_fencing_token: checked(FencingToken::new(v.owner_fencing_token))?,
        timing: match (v.deadline_ms, v.activity_v1) {
            (deadline, None) => musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(deadline) },
            (0, Some(value)) => musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(checked(crate::activity_execution_timing(value))?),
            _ => return Err(AgentContextCodecError::Invalid),
        },
        state,
    })
}
