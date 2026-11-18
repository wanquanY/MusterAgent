//! Canonical model Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::ModelInvocationStarted(value) => {
            Ok(DomainEvent::Model(ModelEvent::InvocationStarted {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                purpose: model_purpose(value.purpose)?,
                model_route_ref: model_route_ref(value.model_route_ref)?,
                request_hash: sha256(value.request_hash)?,
            }))
        }
        Event::ModelInvocationCompleted(value) => {
            Ok(DomainEvent::Model(ModelEvent::InvocationCompleted {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                response_ref: content_ref(value.response_ref.ok_or_else(corrupt)?)?,
                continuation_ref: optional_text(value.continuation_ref)
                    .map(OpaqueExternalRef::new)
                    .transpose()
                    .map_err(|_| corrupt())?
                    .map(ContinuationRef::from_opaque),
            }))
        }
        Event::ModelInvocationFailed(value) => {
            Ok(DomainEvent::Model(ModelEvent::InvocationFailed {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                error: error_info(value.error)?,
                retry_disposition: retry_disposition(value.retry_disposition)?,
            }))
        }
        Event::ModelInvocationCancelled(value) => {
            Ok(DomainEvent::Model(ModelEvent::InvocationCancelled {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
            }))
        }
        Event::UsageRecorded(value) => {
            let has_credential_grant = !value.credential_grant_ref.is_empty();
            let has_response_model = !value.response_model_key.is_empty();
            if !has_credential_grant && !has_response_model {
                if value.provider_cache_write_1h_tokens != 0 || value.cancelled {
                    return Err(corrupt());
                }
                return Ok(DomainEvent::Model(ModelEvent::LegacyUsageRecordedV1(
                    LegacyUsageRecordedV1::from_pre_f1_wire(
                        required_id(value.invocation_id.map(|value| value.value))?,
                        model_purpose(value.purpose)?,
                        model_route_ref(value.model_route_ref)?,
                        ModelUsage {
                            input_tokens_uncached: value.input_tokens,
                            output_tokens: value.output_tokens,
                            reasoning_tokens: value.reasoning_tokens,
                            provider_cache_read_tokens: value.provider_cache_read_tokens,
                            provider_cache_write_tokens: value.provider_cache_write_tokens,
                            provider_cache_write_1h_tokens: 0,
                            total_tokens: value.total_tokens,
                        },
                        musteragent_identity::IdempotencyKey::new(value.metering_dedup_key)
                            .map_err(|_| corrupt())?,
                        musteragent_model::ModelKey::new(value.requested_model_key)
                            .map_err(|_| corrupt())?,
                        musteragent_model::PricingVersionRef::new(value.pricing_version_ref)
                            .map_err(|_| corrupt())?,
                    ),
                )));
            }
            if !has_credential_grant || !has_response_model || value.total_tokens.is_some() {
                return Err(corrupt());
            }
            let usage = musteragent_runtime_model::UsageRecorded {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                purpose: model_purpose(value.purpose)?,
                model_route_ref: model_route_ref(value.model_route_ref)?,
                credential_grant_ref: musteragent_model::CredentialGrantRef::new(
                    value.credential_grant_ref,
                )
                .map_err(|_| corrupt())?,
                requested_model_key: musteragent_model::ModelKey::new(value.requested_model_key)
                    .map_err(|_| corrupt())?,
                response_model_key: musteragent_model::ModelKey::new(value.response_model_key)
                    .map_err(|_| corrupt())?,
                pricing_version_ref: musteragent_model::PricingVersionRef::new(
                    value.pricing_version_ref,
                )
                .map_err(|_| corrupt())?,
                input_tokens: value.input_tokens,
                output_tokens: value.output_tokens,
                reasoning_tokens: value.reasoning_tokens,
                provider_cache_read_tokens: value.provider_cache_read_tokens,
                provider_cache_write_tokens: value.provider_cache_write_tokens,
                provider_cache_write_1h_tokens: value.provider_cache_write_1h_tokens,
                cancelled: value.cancelled,
                metering_dedup_key: musteragent_identity::IdempotencyKey::new(
                    value.metering_dedup_key,
                )
                .map_err(|_| corrupt())?,
            };
            usage.validate().map_err(|_| corrupt())?;
            Ok(DomainEvent::Model(ModelEvent::UsageRecorded(usage)))
        }
        _ => Err(corrupt()),
    }
}
