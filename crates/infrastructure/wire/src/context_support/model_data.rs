use super::*;
use musteragent_runtime_model::ModelUsage;
pub(crate) fn usage(v: ModelUsage) -> Result<wire::AgentJournalModelUsage> {
    checked(v.validate())?;
    Ok(wire::AgentJournalModelUsage {
        input_tokens_uncached: v.input_tokens_uncached,
        output_tokens: v.output_tokens,
        reasoning_tokens: v.reasoning_tokens,
        provider_cache_read_tokens: v.provider_cache_read_tokens,
        provider_cache_write_tokens: v.provider_cache_write_tokens,
        provider_cache_write_1h_tokens: v.provider_cache_write_1h_tokens,
        total_tokens: v.total_tokens,
    })
}
pub(crate) fn decode_usage(v: wire::AgentJournalModelUsage) -> Result<ModelUsage> {
    let value = ModelUsage {
        input_tokens_uncached: v.input_tokens_uncached,
        output_tokens: v.output_tokens,
        reasoning_tokens: v.reasoning_tokens,
        provider_cache_read_tokens: v.provider_cache_read_tokens,
        provider_cache_write_tokens: v.provider_cache_write_tokens,
        provider_cache_write_1h_tokens: v.provider_cache_write_1h_tokens,
        total_tokens: v.total_tokens,
    };
    checked(value.validate())?;
    Ok(value)
}

pub(super) fn request_ref(value: String) -> Result<musteragent_runtime_model::ProviderRequestRef> {
    Ok(musteragent_runtime_model::ProviderRequestRef::from_opaque(
        checked(OpaqueExternalRef::new(value))?,
    ))
}
