use super::*;
use crate::context_support::model_data;
use musteragent_model::{
    AuxiliaryModelRole, CredentialGrantRef, ModelKey, PricingVersionRef, SignedModelRouteHash,
};
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{
    CompleteModelInvocation, ContinuationRef, MeteringPublication, ModelInvocation,
    ModelInvocationPurpose, ModelInvocationRouteRole, ModelInvocationStatus, ProviderRequestRef,
};

pub(super) fn encode(v: &ModelInvocation) -> Result<wire::AgentCompactionModelInvocation> {
    checked(v.validate())?;
    if v.purpose != ModelInvocationPurpose::ContextCompaction
        || v.route_role != ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Compression)
    {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(wire::AgentCompactionModelInvocation {
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: v.tenant_id.to_string(),
        }),
        conversation_id: Some(wire::ConversationId {
            value: v.conversation_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: v.run_id.to_string(),
        }),
        attempt_id: Some(wire::AttemptId {
            value: v.attempt_id.to_string(),
        }),
        status: match v.status {
            ModelInvocationStatus::Started => 1,
            ModelInvocationStatus::Succeeded => 2,
            ModelInvocationStatus::Failed => 3,
            ModelInvocationStatus::Cancelled => 4,
        },
        model_route_ref: v.model_route_ref.as_str().to_owned(),
        route_identity_hash: Some(wire::Sha256 {
            value: v.route_identity_hash.into_bytes().to_vec(),
        }),
        credential_grant_ref: v.credential_grant_ref.as_str().to_owned(),
        requested_model_key: v.requested_model_key.as_str().to_owned(),
        response_model_key: v.response_model_key.as_ref().map(|v| v.as_str().to_owned()),
        pricing_version_ref: v.pricing_version_ref.as_str().to_owned(),
        idempotency_key: v.idempotency_key.as_str().to_owned(),
        request_ref: Some(crate::wire_common::content_ref(&v.request_ref)),
        request_hash: Some(hash_wire(v.request_hash)),
        response_ref: v.response_ref.as_ref().map(crate::wire_common::content_ref),
        provider_request_ref: v
            .provider_request_ref
            .as_ref()
            .map(|v| v.as_str().to_owned()),
        continuation_input_ref: v
            .continuation_input_ref
            .as_ref()
            .map(|v| v.as_str().to_owned()),
        continuation_output_ref: v
            .continuation_output_ref
            .as_ref()
            .map(|v| v.as_str().to_owned()),
        usage: v.usage.map(|v| checked(model_data::usage(v))).transpose()?,
        started_at_ms: v.started_at.unix_millis(),
        finished_at_ms: v.finished_at.map(|v| v.unix_millis()),
        error: v.error.as_ref().map(crate::error_codec::encode_error_info),
    })
}
pub(super) fn decode(v: wire::AgentCompactionModelInvocation) -> Result<ModelInvocation> {
    let value = ModelInvocation {
        invocation_id: id(v.invocation_id.map(|v| v.value))?,
        tenant_id: id(v.tenant_id.map(|v| v.value))?,
        conversation_id: id(v.conversation_id.map(|v| v.value))?,
        run_id: id(v.run_id.map(|v| v.value))?,
        attempt_id: id(v.attempt_id.map(|v| v.value))?,
        purpose: ModelInvocationPurpose::ContextCompaction,
        route_role: ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Compression),
        status: match v.status {
            1 => ModelInvocationStatus::Started,
            2 => ModelInvocationStatus::Succeeded,
            3 => ModelInvocationStatus::Failed,
            4 => ModelInvocationStatus::Cancelled,
            _ => return Err(AgentContextCodecError::Invalid),
        },
        model_route_ref: route_ref(Some(v.model_route_ref))?,
        route_identity_hash: checked(SignedModelRouteHash::from_bytes(
            hash(required(v.route_identity_hash)?)?.into_bytes(),
        ))?,
        credential_grant_ref: checked(CredentialGrantRef::new(v.credential_grant_ref))?,
        requested_model_key: checked(ModelKey::new(v.requested_model_key))?,
        response_model_key: v
            .response_model_key
            .map(|v| checked(ModelKey::new(v)))
            .transpose()?,
        pricing_version_ref: checked(PricingVersionRef::new(v.pricing_version_ref))?,
        idempotency_key: checked(musteragent_identity::IdempotencyKey::new(v.idempotency_key))?,
        request_ref: content(required(v.request_ref)?)?,
        request_hash: hash(required(v.request_hash)?)?,
        response_ref: v.response_ref.map(content).transpose()?,
        provider_request_ref: v
            .provider_request_ref
            .map(|v| {
                Ok(ProviderRequestRef::from_opaque(checked(
                    OpaqueExternalRef::new(v),
                )?))
            })
            .transpose()?,
        continuation_input_ref: v.continuation_input_ref.map(continuation).transpose()?,
        continuation_output_ref: v.continuation_output_ref.map(continuation).transpose()?,
        usage: v
            .usage
            .map(|v| checked(model_data::decode_usage(v)))
            .transpose()?,
        started_at: UtcTimestamp::from_unix_millis(v.started_at_ms),
        finished_at: v.finished_at_ms.map(UtcTimestamp::from_unix_millis),
        error: v
            .error
            .map(|v| checked(crate::error_codec::decode_error_info(v)))
            .transpose()?,
    };
    checked(value.validate())?;
    Ok(value)
}
fn continuation(v: String) -> Result<ContinuationRef> {
    Ok(ContinuationRef::from_opaque(checked(
        OpaqueExternalRef::new(v),
    )?))
}

pub(super) fn completion(v: &CompleteModelInvocation) -> Result<wire::AgentCompactionCompletion> {
    checked(v.validate())?;
    if let (Some(fact), Some(publication)) = (&v.usage_fact, &v.metering_publication)
        && checked(crate::usage_codec::encode_metering_usage_fact(fact))? != publication.payload
    {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(wire::AgentCompactionCompletion {
        early_provider_cause: v
            .early_provider_cause
            .map(|cause| cause.as_str().to_owned()),
        retry_not_before_ms: v.retry_not_before.map(|v| v.unix_millis()),
        invocation: Some(encode(&v.invocation)?),
        metering_message_id: v
            .metering_publication
            .as_ref()
            .map(|v| v.message_id.to_string()),
        metering_payload: v
            .metering_publication
            .as_ref()
            .map_or_else(Vec::new, |v| v.payload.clone()),
        metering_payload_hash: v
            .metering_publication
            .as_ref()
            .map(|v| hash_wire(v.payload_hash)),
    })
}
pub(super) fn decode_completion(
    v: wire::AgentCompactionCompletion,
) -> Result<CompleteModelInvocation> {
    if v.metering_payload.len() > 64 * 1024 {
        return Err(AgentContextCodecError::Bounds);
    }
    let (fact, publication) = if let Some(message) = v.metering_message_id {
        let fact = checked(crate::usage_codec::decode_metering_usage_fact(
            &v.metering_payload,
        ))?;
        if checked(crate::usage_codec::encode_metering_usage_fact(&fact))? != v.metering_payload {
            return Err(AgentContextCodecError::Invalid);
        }
        let publication = MeteringPublication {
            message_id: id(Some(message))?,
            payload: v.metering_payload,
            payload_hash: hash(required(v.metering_payload_hash)?)?,
        };
        (Some(fact), Some(publication))
    } else {
        if !v.metering_payload.is_empty() || v.metering_payload_hash.is_some() {
            return Err(AgentContextCodecError::Invalid);
        }
        (None, None)
    };
    let value = CompleteModelInvocation {
        early_provider_cause: v
            .early_provider_cause
            .as_deref()
            .map(|cause| {
                musteragent_runtime_model::ProviderErrorCode::from_alternate_provider_cause(cause)
                    .ok_or(AgentContextCodecError::Invalid)
            })
            .transpose()?,
        retry_not_before: v
            .retry_not_before_ms
            .map(musteragent_identity::UtcTimestamp::from_unix_millis),
        invocation: decode(required(v.invocation)?)?,
        usage_fact: fact,
        metering_publication: publication,
    };
    checked(value.validate())?;
    Ok(value)
}
