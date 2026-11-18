use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelEntryId,
    ModelParameter as DomainModelParameter, ModelProviderId, ModelRoute,
    PricingVersionRef as DomainPricingVersionRef, ProviderStatePolicy as DomainProviderStatePolicy,
};
use musteragent_runtime_contract::ModelClass;
use musteragent_runtime_snapshot::{
    ModelRouteSnapshot, PinnedAuxiliaryModelRoute, PinnedModelRoute,
};

use crate::wire::{
    self as wire, BoolParameter, DoubleParameter, IntParameter, ModelParameter, ModelRouteRef,
    StringParameter, model_parameter,
};

use super::SnapshotCodecError;

pub(crate) fn encode_model_snapshot(value: &ModelRouteSnapshot) -> wire::ModelRouteSnapshot {
    wire::ModelRouteSnapshot {
        requested_model_class: value.requested_model_class.as_str().to_owned(),
        resolved_route: Some(ModelRouteRef {
            route_key: value.route.route_ref.as_str().to_owned(),
        }),
        resolved_model_key: value.route.resolved_model_key.clone(),
        model_provider_id: value.route.model_provider_id.to_string(),
        model_entry_id: value.route.model_entry_id.to_string(),
        api_format: value.route.api_format.as_str().to_owned(),
        adapter_version: value.route.adapter_version.as_str().to_owned(),
        protocol_version: value.route.protocol_version.as_str().to_owned(),
        credential_grant_ref: value.credential_grant_ref.as_str().to_owned(),
        context_window: value.route.context_window.get(),
        max_output_tokens: value.route.max_output_tokens.get(),
        capabilities: value
            .route
            .capabilities
            .iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
        parameters: value
            .route
            .parameters
            .iter()
            .map(|(name, parameter)| encode_parameter(name.as_str(), parameter))
            .collect(),
        fallback_routes: value
            .route
            .fallback_route_refs
            .iter()
            .map(|route_ref| ModelRouteRef {
                route_key: route_ref.as_str().to_owned(),
            })
            .collect(),
        pricing_version_ref: Some(pricing_version_ref(&value.route.pricing_version_ref)),
        provider_state_policy: encode_provider_state(value.route.provider_state_policy) as i32,
        fallback_candidates: value
            .fallback_candidates
            .iter()
            .map(encode_pinned_model_route)
            .collect(),
    }
}

pub(crate) fn encode_auxiliary_model(
    value: &PinnedAuxiliaryModelRoute,
) -> wire::PinnedAuxiliaryModelRoute {
    wire::PinnedAuxiliaryModelRoute {
        role: encode_auxiliary_role(value.role) as i32,
        model: Some(encode_pinned_model_route(&value.model)),
    }
}

pub(crate) fn encode_pinned_model_route(value: &PinnedModelRoute) -> wire::PinnedModelRoute {
    wire::PinnedModelRoute {
        route_ref: Some(ModelRouteRef {
            route_key: value.route.route_ref.as_str().to_owned(),
        }),
        resolved_model_key: value.route.resolved_model_key.clone(),
        model_provider_id: value.route.model_provider_id.to_string(),
        model_entry_id: value.route.model_entry_id.to_string(),
        api_format: value.route.api_format.as_str().to_owned(),
        adapter_version: value.route.adapter_version.as_str().to_owned(),
        protocol_version: value.route.protocol_version.as_str().to_owned(),
        credential_grant_ref: value.credential_grant_ref.as_str().to_owned(),
        context_window: value.route.context_window.get(),
        max_output_tokens: value.route.max_output_tokens.get(),
        capabilities: value
            .route
            .capabilities
            .iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
        parameters: value
            .route
            .parameters
            .iter()
            .map(|(name, parameter)| encode_parameter(name.as_str(), parameter))
            .collect(),
        pricing_version_ref: Some(pricing_version_ref(&value.route.pricing_version_ref)),
        provider_state_policy: encode_provider_state(value.route.provider_state_policy) as i32,
    }
}

const fn encode_auxiliary_role(value: AuxiliaryModelRole) -> wire::AuxiliaryModelRole {
    match value {
        AuxiliaryModelRole::Compression => wire::AuxiliaryModelRole::Compression,
        AuxiliaryModelRole::Vision => wire::AuxiliaryModelRole::Vision,
        AuxiliaryModelRole::Embedding => wire::AuxiliaryModelRole::Embedding,
        AuxiliaryModelRole::Title => wire::AuxiliaryModelRole::Title,
        AuxiliaryModelRole::Routing => wire::AuxiliaryModelRole::Routing,
        AuxiliaryModelRole::Evolution => wire::AuxiliaryModelRole::Evolution,
        AuxiliaryModelRole::GoalJudge => wire::AuxiliaryModelRole::GoalJudge,
        AuxiliaryModelRole::Decision => wire::AuxiliaryModelRole::Decision,
    }
}

pub(crate) fn decode_auxiliary_model(
    value: wire::PinnedAuxiliaryModelRoute,
) -> Result<PinnedAuxiliaryModelRoute, SnapshotCodecError> {
    let role = match wire::AuxiliaryModelRole::try_from(value.role) {
        Ok(wire::AuxiliaryModelRole::Compression) => AuxiliaryModelRole::Compression,
        Ok(wire::AuxiliaryModelRole::Vision) => AuxiliaryModelRole::Vision,
        Ok(wire::AuxiliaryModelRole::Embedding) => AuxiliaryModelRole::Embedding,
        Ok(wire::AuxiliaryModelRole::Title) => AuxiliaryModelRole::Title,
        Ok(wire::AuxiliaryModelRole::Routing) => AuxiliaryModelRole::Routing,
        Ok(wire::AuxiliaryModelRole::Evolution) => AuxiliaryModelRole::Evolution,
        Ok(wire::AuxiliaryModelRole::GoalJudge) => AuxiliaryModelRole::GoalJudge,
        Ok(wire::AuxiliaryModelRole::Decision) => AuxiliaryModelRole::Decision,
        _ => return Err(SnapshotCodecError::InvalidScalar),
    };
    Ok(PinnedAuxiliaryModelRoute {
        role,
        model: decode_pinned_model_route(value.model.ok_or(SnapshotCodecError::MissingField)?)?,
    })
}

pub(crate) fn decode_model_snapshot(
    value: wire::ModelRouteSnapshot,
) -> Result<ModelRouteSnapshot, SnapshotCodecError> {
    let route_ref = value
        .resolved_route
        .ok_or(SnapshotCodecError::MissingField)?;
    let fallback_route_refs = value
        .fallback_routes
        .into_iter()
        .map(|route| {
            VersionedKey::new(route.route_key).map_err(|_| SnapshotCodecError::InvalidScalar)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let route = decode_route(RouteFields {
        route_ref: route_ref.route_key,
        resolved_model_key: value.resolved_model_key,
        model_provider_id: value.model_provider_id,
        model_entry_id: value.model_entry_id,
        api_format: value.api_format,
        adapter_version: value.adapter_version,
        protocol_version: value.protocol_version,
        context_window: value.context_window,
        max_output_tokens: value.max_output_tokens,
        capabilities: value.capabilities,
        parameters: value.parameters,
        fallback_route_refs,
        pricing_version_ref: value
            .pricing_version_ref
            .ok_or(SnapshotCodecError::MissingField)?
            .value,
        provider_state_policy: value.provider_state_policy,
    })?;
    Ok(ModelRouteSnapshot {
        requested_model_class: ModelClass::new(value.requested_model_class)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        route,
        credential_grant_ref: CredentialGrantRef::new(value.credential_grant_ref)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        fallback_candidates: value
            .fallback_candidates
            .into_iter()
            .map(decode_pinned_model_route)
            .collect::<Result<_, _>>()?,
    })
}

pub(crate) fn decode_pinned_model_route(
    value: wire::PinnedModelRoute,
) -> Result<PinnedModelRoute, SnapshotCodecError> {
    let route_ref = value.route_ref.ok_or(SnapshotCodecError::MissingField)?;
    Ok(PinnedModelRoute {
        route: decode_route(RouteFields {
            route_ref: route_ref.route_key,
            resolved_model_key: value.resolved_model_key,
            model_provider_id: value.model_provider_id,
            model_entry_id: value.model_entry_id,
            api_format: value.api_format,
            adapter_version: value.adapter_version,
            protocol_version: value.protocol_version,
            context_window: value.context_window,
            max_output_tokens: value.max_output_tokens,
            capabilities: value.capabilities,
            parameters: value.parameters,
            fallback_route_refs: Vec::new(),
            pricing_version_ref: value
                .pricing_version_ref
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
            provider_state_policy: value.provider_state_policy,
        })?,
        credential_grant_ref: CredentialGrantRef::new(value.credential_grant_ref)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
    })
}

struct RouteFields {
    route_ref: String,
    resolved_model_key: String,
    model_provider_id: String,
    model_entry_id: String,
    api_format: String,
    adapter_version: String,
    protocol_version: String,
    context_window: u64,
    max_output_tokens: u64,
    capabilities: Vec<String>,
    parameters: Vec<ModelParameter>,
    fallback_route_refs: Vec<VersionedKey>,
    pricing_version_ref: String,
    provider_state_policy: i32,
}

fn decode_route(value: RouteFields) -> Result<ModelRoute, SnapshotCodecError> {
    let mut capabilities = BTreeSet::new();
    for capability in value.capabilities {
        let capability =
            DottedKey::new(capability).map_err(|_| SnapshotCodecError::InvalidScalar)?;
        if !capabilities.insert(capability) {
            return Err(SnapshotCodecError::DuplicateField);
        }
    }
    let mut parameters = BTreeMap::new();
    for parameter in value.parameters {
        let name = DottedKey::new(parameter.name).map_err(|_| SnapshotCodecError::InvalidScalar)?;
        let value = match parameter.value.ok_or(SnapshotCodecError::MissingField)? {
            model_parameter::Value::BoolValue(value) => DomainModelParameter::Bool(value.value),
            model_parameter::Value::IntValue(value) => DomainModelParameter::Int(value.value),
            model_parameter::Value::DoubleValue(value) => {
                if !value.value.is_finite() {
                    return Err(SnapshotCodecError::InvalidScalar);
                }
                DomainModelParameter::Double(value.value)
            }
            model_parameter::Value::StringValue(value) => DomainModelParameter::String(value.value),
        };
        if parameters.insert(name, value).is_some() {
            return Err(SnapshotCodecError::DuplicateField);
        }
    }
    Ok(ModelRoute {
        route_ref: VersionedKey::new(value.route_ref)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        resolved_model_key: value.resolved_model_key,
        model_provider_id: value
            .model_provider_id
            .parse::<ModelProviderId>()
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        model_entry_id: value
            .model_entry_id
            .parse::<ModelEntryId>()
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        api_format: ApiFormat::parse_control_plane(&value.api_format)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        adapter_version: VersionedKey::new(value.adapter_version)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        protocol_version: VersionedKey::new(value.protocol_version)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        context_window: non_zero(value.context_window)?,
        max_output_tokens: non_zero(value.max_output_tokens)?,
        capabilities,
        parameters,
        fallback_route_refs: value.fallback_route_refs,
        pricing_version_ref: DomainPricingVersionRef::new(value.pricing_version_ref)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        provider_state_policy: decode_provider_state(value.provider_state_policy)?,
    })
}

fn encode_parameter(name: &str, value: &DomainModelParameter) -> ModelParameter {
    let value = match value {
        DomainModelParameter::Bool(value) => {
            model_parameter::Value::BoolValue(BoolParameter { value: *value })
        }
        DomainModelParameter::Int(value) => {
            model_parameter::Value::IntValue(IntParameter { value: *value })
        }
        DomainModelParameter::Double(value) => {
            model_parameter::Value::DoubleValue(DoubleParameter { value: *value })
        }
        DomainModelParameter::String(value) => {
            model_parameter::Value::StringValue(StringParameter {
                value: value.clone(),
            })
        }
    };
    ModelParameter {
        name: name.to_owned(),
        value: Some(value),
    }
}

fn pricing_version_ref(value: &DomainPricingVersionRef) -> wire::PricingVersionRef {
    wire::PricingVersionRef {
        value: value.as_str().to_owned(),
    }
}

const fn encode_provider_state(value: DomainProviderStatePolicy) -> wire::ProviderStatePolicy {
    match value {
        DomainProviderStatePolicy::Stateless => wire::ProviderStatePolicy::Stateless,
        DomainProviderStatePolicy::OpaqueContinuation => {
            wire::ProviderStatePolicy::OpaqueContinuation
        }
    }
}

fn decode_provider_state(value: i32) -> Result<DomainProviderStatePolicy, SnapshotCodecError> {
    match wire::ProviderStatePolicy::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::ProviderStatePolicy::Stateless => Ok(DomainProviderStatePolicy::Stateless),
        wire::ProviderStatePolicy::OpaqueContinuation => {
            Ok(DomainProviderStatePolicy::OpaqueContinuation)
        }
        wire::ProviderStatePolicy::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

fn non_zero(value: u64) -> Result<NonZeroU64, SnapshotCodecError> {
    NonZeroU64::new(value).ok_or(SnapshotCodecError::InvalidScalar)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_wire_capability_fails_closed_instead_of_being_deduplicated() {
        let route = wire::PinnedModelRoute {
            route_ref: Some(ModelRouteRef {
                route_key: "aux.compression@1".to_owned(),
            }),
            resolved_model_key: "compression-model".to_owned(),
            model_provider_id: "019fa6de-38fd-7000-8000-000000000611".to_owned(),
            model_entry_id: "019fa6de-38fd-7000-8000-000000000612".to_owned(),
            api_format: ApiFormat::OpenAiResponses.as_str().to_owned(),
            adapter_version: "compression.adapter@1".to_owned(),
            protocol_version: "compression.protocol@1".to_owned(),
            credential_grant_ref: "grant_compression".to_owned(),
            context_window: 200_000,
            max_output_tokens: 8_192,
            capabilities: vec!["tools".to_owned(), "tools".to_owned()],
            parameters: Vec::new(),
            pricing_version_ref: Some(wire::PricingVersionRef {
                value: "catalog:compression.price@1".to_owned(),
            }),
            provider_state_policy: wire::ProviderStatePolicy::Stateless as i32,
        };

        assert!(matches!(
            decode_pinned_model_route(route),
            Err(SnapshotCodecError::DuplicateField)
        ));
    }
}
