use musteragent_content_ref::Sha256;
use musteragent_identity::{IdempotencyKey, OutboxMessageId};
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_model::{
    AttributedModelUsageFact, ModelInvocation, ModelInvocationStatus, ModelUsage,
    ModelUsageEnvelope, UsageRecorded,
};

pub(crate) fn usage_fact(
    invocation: &ModelInvocation,
    usage: ModelUsage,
) -> Result<AttributedModelUsageFact, ErrorCode> {
    let fact = AttributedModelUsageFact {
        envelope: ModelUsageEnvelope {
            attribution: None,
            tenant_id: invocation.tenant_id,
            conversation_id: invocation.conversation_id,
            run_id: invocation.run_id,
            attempt_id: invocation.attempt_id,
            observed_at: invocation
                .finished_at
                .ok_or(ErrorCode::ModelResponseInvalid)?,
        },
        usage: UsageRecorded {
            invocation_id: invocation.invocation_id,
            purpose: invocation.purpose,
            model_route_ref: invocation.model_route_ref.clone(),
            credential_grant_ref: invocation.credential_grant_ref.clone(),
            requested_model_key: invocation.requested_model_key.clone(),
            response_model_key: invocation
                .response_model_key
                .clone()
                .ok_or(ErrorCode::ModelResponseInvalid)?,
            pricing_version_ref: invocation.pricing_version_ref.clone(),
            input_tokens: usage.input_tokens_uncached,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage.reasoning_tokens,
            provider_cache_read_tokens: usage.provider_cache_read_tokens,
            provider_cache_write_tokens: usage.provider_cache_write_tokens,
            provider_cache_write_1h_tokens: usage.provider_cache_write_1h_tokens,
            cancelled: invocation.status == ModelInvocationStatus::Cancelled,
            metering_dedup_key: IdempotencyKey::new(format!("usage:{}", invocation.invocation_id))
                .map_err(|_| ErrorCode::ModelResponseInvalid)?,
        },
        provider_total_tokens: usage.total_tokens,
    };
    fact.validate()
        .map_err(|_| ErrorCode::ModelResponseInvalid)?;
    Ok(fact)
}

pub(crate) fn metering_outbox_id(
    invocation_id: musteragent_identity::InvocationId,
) -> Result<OutboxMessageId, ErrorCode> {
    let mut seed = b"musteragent.legacy.model-usage.outbox.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    let digest = Sha256::digest(&seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    OutboxMessageId::from_uuid(uuid::Uuid::from_bytes(value))
        .map_err(|_| ErrorCode::ModelResponseInvalid)
}
