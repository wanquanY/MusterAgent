use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use crate::wire;
use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_model::{
    ApiFormat, ModelEntryId, ModelParameter, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};

use super::{WireDecodeError, invalid, required};

pub(super) fn decode(
    value: Option<wire::GrantedModelRoute>,
) -> Result<(ModelRoute, Vec<ModelRoute>), WireDecodeError> {
    let mut value = required(value, "signed_execution_grant.claims.model_route")?;
    let fallback_values = std::mem::take(&mut value.fallback_candidates);
    let fallback_model_routes = fallback_values
        .into_iter()
        .map(|candidate| {
            if !candidate.fallback_candidates.is_empty() {
                return Err(invalid(
                    "signed_execution_grant.claims.model_route.fallback_candidates",
                    "nested fallback candidates are not allowed",
                ));
            }
            decode_single(candidate)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut model_route = decode_single(value)?;
    model_route.fallback_route_refs = fallback_model_routes
        .iter()
        .map(|candidate| candidate.route_ref.clone())
        .collect();
    Ok((model_route, fallback_model_routes))
}

pub(super) fn decode_auxiliary(
    value: Option<wire::GrantedModelRoute>,
) -> Result<ModelRoute, WireDecodeError> {
    let value = required(
        value,
        "signed_execution_grant.claims.auxiliary_model_routes.model_route",
    )?;
    if !value.fallback_candidates.is_empty() {
        return Err(invalid(
            "signed_execution_grant.claims.auxiliary_model_routes.model_route.fallback_candidates",
            "auxiliary routes cannot contain fallback candidates",
        ));
    }
    decode_single(value)
}

fn decode_single(value: wire::GrantedModelRoute) -> Result<ModelRoute, WireDecodeError> {
    let route_ref = required(
        value.route_ref,
        "signed_execution_grant.claims.model_route.route_ref",
    )?;
    let mut parameters = BTreeMap::new();
    for parameter in value.parameters {
        let name = DottedKey::new(parameter.name).map_err(|error| {
            invalid(
                "signed_execution_grant.claims.model_route.parameters.name",
                error,
            )
        })?;
        let value = match required(
            parameter.value,
            "signed_execution_grant.claims.model_route.parameters.value",
        )? {
            wire::model_parameter::Value::BoolValue(value) => ModelParameter::Bool(value.value),
            wire::model_parameter::Value::IntValue(value) => ModelParameter::Int(value.value),
            wire::model_parameter::Value::DoubleValue(value) => {
                if !value.value.is_finite() {
                    return Err(invalid(
                        "signed_execution_grant.claims.model_route.parameters.double_value",
                        "must be finite",
                    ));
                }
                ModelParameter::Double(value.value)
            }
            wire::model_parameter::Value::StringValue(value) => ModelParameter::String(value.value),
        };
        if parameters.insert(name, value).is_some() {
            return Err(invalid(
                "signed_execution_grant.claims.model_route.parameters",
                "parameter names must be unique",
            ));
        }
    }
    let state_policy = match wire::ProviderStatePolicy::try_from(value.provider_state_policy) {
        Ok(wire::ProviderStatePolicy::Stateless) => ProviderStatePolicy::Stateless,
        Ok(wire::ProviderStatePolicy::OpaqueContinuation) => {
            ProviderStatePolicy::OpaqueContinuation
        }
        _ => {
            return Err(WireDecodeError::UnsupportedEnum {
                field: "signed_execution_grant.claims.model_route.provider_state_policy",
                value: value.provider_state_policy,
            });
        }
    };
    let capability_count = value.capabilities.len();
    let capabilities = value
        .capabilities
        .into_iter()
        .map(|capability| {
            DottedKey::new(capability).map_err(|error| {
                invalid(
                    "signed_execution_grant.claims.model_route.capabilities",
                    error,
                )
            })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if capabilities.len() != capability_count {
        return Err(invalid(
            "signed_execution_grant.claims.model_route.capabilities",
            "capabilities must be unique",
        ));
    }
    let route = ModelRoute {
        route_ref: VersionedKey::new(route_ref.route_key).map_err(|error| {
            invalid("signed_execution_grant.claims.model_route.route_ref", error)
        })?,
        resolved_model_key: value.resolved_model_key,
        model_provider_id: value
            .model_provider_id
            .parse::<ModelProviderId>()
            .map_err(|error| {
                invalid(
                    "signed_execution_grant.claims.model_route.model_provider_id",
                    error,
                )
            })?,
        model_entry_id: value
            .model_entry_id
            .parse::<ModelEntryId>()
            .map_err(|error| {
                invalid(
                    "signed_execution_grant.claims.model_route.model_entry_id",
                    error,
                )
            })?,
        api_format: ApiFormat::parse_control_plane(&value.api_format).map_err(|error| {
            invalid(
                "signed_execution_grant.claims.model_route.api_format",
                error,
            )
        })?,
        adapter_version: VersionedKey::new(value.adapter_version).map_err(|error| {
            invalid(
                "signed_execution_grant.claims.model_route.adapter_version",
                error,
            )
        })?,
        protocol_version: VersionedKey::new(value.protocol_version).map_err(|error| {
            invalid(
                "signed_execution_grant.claims.model_route.protocol_version",
                error,
            )
        })?,
        context_window: NonZeroU64::new(value.context_window).ok_or_else(|| {
            invalid(
                "signed_execution_grant.claims.model_route.context_window",
                "must be greater than zero",
            )
        })?,
        max_output_tokens: NonZeroU64::new(value.max_output_tokens).ok_or_else(|| {
            invalid(
                "signed_execution_grant.claims.model_route.max_output_tokens",
                "must be greater than zero",
            )
        })?,
        capabilities,
        parameters,
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new(
            required(
                value.pricing_version_ref,
                "signed_execution_grant.claims.model_route.pricing_version_ref",
            )?
            .value,
        )
        .map_err(|error| {
            invalid(
                "signed_execution_grant.claims.model_route.pricing_version_ref",
                error,
            )
        })?,
        provider_state_policy: state_policy,
    };
    route
        .validate()
        .map_err(|error| invalid("signed_execution_grant.claims.model_route", error))?;
    if route.max_output_tokens >= route.context_window {
        return Err(invalid(
            "signed_execution_grant.claims.model_route",
            "max_output_tokens must be smaller than context_window",
        ));
    }
    Ok(route)
}

#[cfg(test)]
mod tests {
    use super::{decode_auxiliary, decode_single, wire};

    fn granted_model_route(capabilities: &[&str]) -> wire::GrantedModelRoute {
        wire::GrantedModelRoute {
            route_ref: Some(wire::ModelRouteRef {
                route_key: "cloud.chat-primary@1".to_owned(),
            }),
            resolved_model_key: "gpt-runtime-known-answer".to_owned(),
            model_provider_id: "019fa6de-38fd-7000-8000-000000000401".to_owned(),
            model_entry_id: "019fa6de-38fd-7000-8000-000000000402".to_owned(),
            api_format: "openai.responses".to_owned(),
            adapter_version: "openai.responses-adapter@1".to_owned(),
            protocol_version: "openai.responses@1".to_owned(),
            context_window: 128_000,
            max_output_tokens: 16_384,
            capabilities: capabilities.iter().map(ToString::to_string).collect(),
            parameters: Vec::new(),
            pricing_version_ref: Some(wire::PricingVersionRef {
                value: "catalog:known-answer.price@1".to_owned(),
            }),
            provider_state_policy: wire::ProviderStatePolicy::OpaqueContinuation.into(),
            fallback_candidates: Vec::new(),
        }
    }

    #[test]
    fn capability_insertion_order_is_irrelevant_but_duplicates_fail_closed() {
        let forward = decode_single(granted_model_route(&["reasoning", "tools"]))
            .expect("forward capability insertion");
        let reverse = decode_single(granted_model_route(&["tools", "reasoning"]))
            .expect("reverse capability insertion");
        assert_eq!(forward, reverse);
        assert!(decode_single(granted_model_route(&["reasoning", "reasoning"])).is_err());
    }

    #[test]
    fn auxiliary_transport_rejects_a_nested_fallback_plan() {
        let mut route = granted_model_route(&[]);
        route.fallback_candidates.push(granted_model_route(&[]));
        assert!(decode_auxiliary(Some(route)).is_err());
    }
}
