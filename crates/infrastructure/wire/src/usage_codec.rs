use musteragent_content_ref::Sha256;
use musteragent_identity::{
    AttemptId, ConversationId, IdempotencyKey, InvocationId, OutboxMessageId, RunId, TenantId,
    UtcTimestamp, VersionedKey,
};
use musteragent_model::{CredentialGrantRef, ModelKey, PricingVersionRef};
use musteragent_runtime_model::{
    AttributedModelUsageFact, MeteringPublication, MeteringPublicationEncoder, ModelContractError,
    ModelInvocationPurpose, ModelRouteRef, ModelUsageEnvelope, UsageRecorded,
};
use prost::Message as _;
use thiserror::Error;

use crate::wire;

/// Validate and encode an attributed raw usage fact as canonical protobuf.
pub fn encode_metering_usage_fact(
    fact: &AttributedModelUsageFact,
) -> Result<Vec<u8>, ModelContractError> {
    Ok(metering_usage_fact_message(fact)?.encode_to_vec())
}

/// Project validated usage into its wire message, retaining pinned route identity.
pub fn metering_usage_fact_message(
    fact: &AttributedModelUsageFact,
) -> Result<wire::MeteringUsageFact, ModelContractError> {
    fact.validate()?;
    let envelope = fact.envelope;
    let usage = &fact.usage;
    Ok(wire::MeteringUsageFact {
        parent_run_id: envelope
            .attribution
            .and_then(|a| a.parent_run_id)
            .map(|id| id.to_string()),
        billing_subject: envelope.attribution.map(|a| a.billing_subject.to_string()),
        tenant_id: Some(wire::TenantId {
            value: envelope.tenant_id.to_string(),
        }),
        conversation_id: Some(wire::ConversationId {
            value: envelope.conversation_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: envelope.run_id.to_string(),
        }),
        attempt_id: Some(wire::AttemptId {
            value: envelope.attempt_id.to_string(),
        }),
        invocation_id: Some(wire::InvocationId {
            value: usage.invocation_id.to_string(),
        }),
        purpose: purpose(usage.purpose),
        model_route_ref: Some(wire::ModelRouteRef {
            route_key: usage.model_route_ref.as_str().to_owned(),
        }),
        requested_model_key: usage.requested_model_key.as_str().to_owned(),
        pricing_version_ref: usage.pricing_version_ref.as_str().to_owned(),
        input_tokens_uncached: usage.input_tokens,
        output_tokens: usage.output_tokens,
        reasoning_tokens: usage.reasoning_tokens,
        provider_cache_read_tokens: usage.provider_cache_read_tokens,
        provider_cache_write_tokens: usage.provider_cache_write_tokens,
        total_tokens: fact.provider_total_tokens,
        metering_dedup_key: usage.metering_dedup_key.as_str().to_owned(),
        observed_at_ms: envelope.observed_at.unix_millis(),
        credential_grant_ref: usage.credential_grant_ref.as_str().to_owned(),
        response_model_key: usage.response_model_key.as_str().to_owned(),
        provider_cache_write_1h_tokens: usage.provider_cache_write_1h_tokens,
        cancelled: usage.cancelled,
    })
}

/// Decode usage and reject malformed identities or inconsistent token counters.
pub fn decode_metering_usage_fact(
    bytes: &[u8],
) -> Result<AttributedModelUsageFact, MeteringUsageCodecError> {
    let message = wire::MeteringUsageFact::decode(bytes)
        .map_err(|_| MeteringUsageCodecError::MalformedPayload)?;
    let fact = AttributedModelUsageFact {
        envelope: ModelUsageEnvelope {
            attribution: match (message.parent_run_id, message.billing_subject) {
                (parent, Some(subject)) => Some(musteragent_runtime_model::ModelUsageAttribution {
                    parent_run_id: parent
                        .map(|id| id.parse())
                        .transpose()
                        .map_err(|_| MeteringUsageCodecError::MalformedPayload)?,
                    billing_subject: subject
                        .parse()
                        .map_err(|_| MeteringUsageCodecError::MalformedPayload)?,
                }),
                (None, None) => None,
                (Some(_), None) => return Err(MeteringUsageCodecError::MalformedPayload),
            },
            tenant_id: id(message.tenant_id, TenantId::from_uuid)?,
            conversation_id: id(message.conversation_id, ConversationId::from_uuid)?,
            run_id: id(message.run_id, RunId::from_uuid)?,
            attempt_id: id(message.attempt_id, AttemptId::from_uuid)?,
            observed_at: UtcTimestamp::from_unix_millis(message.observed_at_ms),
        },
        usage: UsageRecorded {
            invocation_id: id(message.invocation_id, InvocationId::from_uuid)?,
            purpose: decode_purpose(message.purpose)?,
            model_route_ref: ModelRouteRef::from_key(VersionedKey::new(
                message
                    .model_route_ref
                    .ok_or(MeteringUsageCodecError::MalformedPayload)?
                    .route_key,
            )?),
            credential_grant_ref: CredentialGrantRef::new(message.credential_grant_ref)?,
            requested_model_key: ModelKey::new(message.requested_model_key)?,
            response_model_key: ModelKey::new(message.response_model_key)?,
            pricing_version_ref: PricingVersionRef::new(message.pricing_version_ref)?,
            input_tokens: message.input_tokens_uncached,
            output_tokens: message.output_tokens,
            reasoning_tokens: message.reasoning_tokens,
            provider_cache_read_tokens: message.provider_cache_read_tokens,
            provider_cache_write_tokens: message.provider_cache_write_tokens,
            provider_cache_write_1h_tokens: message.provider_cache_write_1h_tokens,
            cancelled: message.cancelled,
            metering_dedup_key: IdempotencyKey::new(message.metering_dedup_key)?,
        },
        provider_total_tokens: message.total_tokens,
    };
    fact.validate()?;
    Ok(fact)
}

/// Bind encoded raw usage to an outbox message ID and payload digest.
pub fn metering_publication(
    fact: &AttributedModelUsageFact,
    message_id: OutboxMessageId,
) -> Result<MeteringPublication, MeteringUsageCodecError> {
    let payload = encode_metering_usage_fact(fact)?;
    Ok(MeteringPublication {
        message_id,
        payload_hash: Sha256::digest(&payload),
        payload,
    })
}

/// Production protobuf encoder injected into persistence/application services.
#[derive(Debug, Copy, Clone, Default)]
pub struct ProtobufMeteringPublicationEncoder;

impl MeteringPublicationEncoder for ProtobufMeteringPublicationEncoder {
    fn encode(
        &self,
        fact: &AttributedModelUsageFact,
        message_id: OutboxMessageId,
    ) -> Result<MeteringPublication, ModelContractError> {
        let payload = encode_metering_usage_fact(fact)?;
        Ok(MeteringPublication {
            message_id,
            payload_hash: Sha256::digest(&payload),
            payload,
        })
    }
}

fn purpose(value: ModelInvocationPurpose) -> i32 {
    (match value {
        ModelInvocationPurpose::Primary => wire::ModelInvocationPurpose::Primary,
        ModelInvocationPurpose::ContextCompaction => {
            wire::ModelInvocationPurpose::ContextCompaction
        }
        ModelInvocationPurpose::Finalization => wire::ModelInvocationPurpose::Finalization,
        ModelInvocationPurpose::Verification => wire::ModelInvocationPurpose::Verification,
        ModelInvocationPurpose::EvolutionEvaluation => {
            wire::ModelInvocationPurpose::EvolutionEvaluation
        }
        ModelInvocationPurpose::EvolutionInduction => {
            wire::ModelInvocationPurpose::EvolutionInduction
        }
    }) as i32
}

fn decode_purpose(value: i32) -> Result<ModelInvocationPurpose, MeteringUsageCodecError> {
    match wire::ModelInvocationPurpose::try_from(value) {
        Ok(wire::ModelInvocationPurpose::Primary) => Ok(ModelInvocationPurpose::Primary),
        Ok(wire::ModelInvocationPurpose::ContextCompaction) => {
            Ok(ModelInvocationPurpose::ContextCompaction)
        }
        Ok(wire::ModelInvocationPurpose::Finalization) => Ok(ModelInvocationPurpose::Finalization),
        Ok(wire::ModelInvocationPurpose::Verification) => Ok(ModelInvocationPurpose::Verification),
        Ok(wire::ModelInvocationPurpose::EvolutionEvaluation) => {
            Ok(ModelInvocationPurpose::EvolutionEvaluation)
        }
        Ok(wire::ModelInvocationPurpose::EvolutionInduction) => {
            Ok(ModelInvocationPurpose::EvolutionInduction)
        }
        Ok(wire::ModelInvocationPurpose::Unspecified) | Err(_) => {
            Err(MeteringUsageCodecError::MalformedPayload)
        }
    }
}

fn id<T, W>(
    value: Option<W>,
    convert: impl FnOnce(uuid::Uuid) -> Result<T, musteragent_identity::IdentityError>,
) -> Result<T, MeteringUsageCodecError>
where
    W: WireId,
{
    let parsed = uuid::Uuid::parse_str(
        value
            .as_ref()
            .ok_or(MeteringUsageCodecError::MalformedPayload)?
            .value(),
    )
    .map_err(|_| MeteringUsageCodecError::MalformedPayload)?;
    convert(parsed).map_err(|_| MeteringUsageCodecError::MalformedPayload)
}

trait WireId {
    fn value(&self) -> &str;
}

macro_rules! wire_id {
    ($($type:ty),+ $(,)?) => {
        $(
            impl WireId for $type {
                fn value(&self) -> &str {
                    &self.value
                }
            }
        )+
    };
}

wire_id!(
    wire::TenantId,
    wire::ConversationId,
    wire::RunId,
    wire::AttemptId,
    wire::InvocationId,
);

/// Malformed wire values or invalid usage-domain identities.
#[derive(Debug, Error)]
pub enum MeteringUsageCodecError {
    #[error(transparent)]
    Contract(#[from] ModelContractError),
    #[error(transparent)]
    LogicalKey(#[from] musteragent_identity::LogicalKeyError),
    #[error(transparent)]
    Idempotency(#[from] musteragent_identity::IdempotencyKeyError),
    #[error(transparent)]
    Pricing(#[from] musteragent_model::ModelContractError),
    #[error("metering usage payload is malformed")]
    MalformedPayload,
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{
        AttemptId, ConversationId, IdempotencyKey, InvocationId, RunId, TenantId, UtcTimestamp,
        VersionedKey,
    };
    use musteragent_model::{CredentialGrantRef, ModelKey, PricingVersionRef};
    use musteragent_runtime_model::{
        AttributedModelUsageFact, ModelInvocationPurpose, ModelRouteRef, ModelUsageEnvelope,
        UsageRecorded,
    };

    use super::{decode_metering_usage_fact, encode_metering_usage_fact};

    #[test]
    fn metering_usage_round_trips_without_losing_pinned_identity() {
        let fact = AttributedModelUsageFact {
            envelope: ModelUsageEnvelope {
                attribution: None,
                tenant_id: TenantId::generate(),
                conversation_id: ConversationId::generate(),
                run_id: RunId::generate(),
                attempt_id: AttemptId::generate(),
                observed_at: UtcTimestamp::from_unix_millis(1_725_000_000_123),
            },
            usage: UsageRecorded {
                invocation_id: InvocationId::generate(),
                purpose: ModelInvocationPurpose::Verification,
                model_route_ref: ModelRouteRef::from_key(
                    VersionedKey::new("model.route@3").expect("route"),
                ),
                credential_grant_ref: CredentialGrantRef::new("grant_metering_fixture")
                    .expect("grant"),
                requested_model_key: ModelKey::new("openai:gpt-test").expect("requested model"),
                response_model_key: ModelKey::new("gpt-test-2026-08-01").expect("response model"),
                pricing_version_ref: PricingVersionRef::new("catalog:pricing.registry@7")
                    .expect("pricing"),
                input_tokens: 101,
                output_tokens: 23,
                reasoning_tokens: 17,
                provider_cache_read_tokens: 41,
                provider_cache_write_tokens: 5,
                provider_cache_write_1h_tokens: 3,
                cancelled: false,
                metering_dedup_key: IdempotencyKey::new("metering:fixture").expect("dedup"),
            },
            provider_total_tokens: Some(187),
        };

        let bytes = encode_metering_usage_fact(&fact).expect("encode");
        assert_eq!(decode_metering_usage_fact(&bytes).expect("decode"), fact);
        let mut child = fact.clone();
        child.envelope.attribution = Some(musteragent_runtime_model::ModelUsageAttribution {
            parent_run_id: Some(RunId::generate()),
            billing_subject: musteragent_identity::PrincipalId::generate(),
        });
        let bytes = encode_metering_usage_fact(&child).expect("lineage encode");
        assert_eq!(
            decode_metering_usage_fact(&bytes).expect("lineage decode"),
            child
        );
        use prost::Message;
        let mut malformed =
            crate::wire::MeteringUsageFact::decode(bytes.as_slice()).expect("wire envelope");
        malformed.billing_subject = None;
        assert!(
            decode_metering_usage_fact(&malformed.encode_to_vec()).is_err(),
            "parent attribution requires billing subject"
        );
    }

    #[test]
    fn malformed_metering_usage_is_rejected() {
        assert!(decode_metering_usage_fact(b"not-protobuf").is_err());
    }
}
