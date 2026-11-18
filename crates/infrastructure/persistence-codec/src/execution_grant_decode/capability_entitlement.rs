use std::collections::BTreeSet;

use crate::wire;
use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_policy::{Decision, Effect};

use super::{WireDecodeError, invalid, parse_id};

pub(crate) fn decode(
    value: wire::CapabilityEntitlement,
) -> Result<CapabilityEntitlement, WireDecodeError> {
    let location = match wire::CapabilityLocation::try_from(value.location) {
        Ok(wire::CapabilityLocation::Cloud) => CapabilityLocation::Cloud,
        Ok(wire::CapabilityLocation::Backend) => CapabilityLocation::Backend,
        Ok(wire::CapabilityLocation::Device) => CapabilityLocation::Device,
        Ok(wire::CapabilityLocation::RemoteDevice) => CapabilityLocation::RemoteDevice,
        Ok(wire::CapabilityLocation::Browser) => CapabilityLocation::Browser,
        _ => {
            return Err(WireDecodeError::UnsupportedEnum {
                field: "signed_execution_grant.claims.capability_entitlements.location",
                value: value.location,
            });
        }
    };
    let effect = match wire::ToolEffect::try_from(value.effect) {
        Ok(wire::ToolEffect::Read) => Effect::Read,
        Ok(wire::ToolEffect::WriteReversible) => Effect::WriteReversible,
        Ok(wire::ToolEffect::WriteExternal) => Effect::WriteExternal,
        Ok(wire::ToolEffect::Destructive) => Effect::Destructive,
        _ => {
            return Err(WireDecodeError::UnsupportedEnum {
                field: "signed_execution_grant.claims.capability_entitlements.effect",
                value: value.effect,
            });
        }
    };
    let policy_decision = match wire::CapabilityPolicyDecision::try_from(value.policy_decision) {
        Ok(wire::CapabilityPolicyDecision::Allow) => Decision::Allow,
        Ok(wire::CapabilityPolicyDecision::Deny) => Decision::Deny,
        Ok(wire::CapabilityPolicyDecision::RequireApproval) => Decision::RequireApproval,
        _ => {
            return Err(WireDecodeError::UnsupportedEnum {
                field: "signed_execution_grant.claims.capability_entitlements.policy_decision",
                value: value.policy_decision,
            });
        }
    };
    let scope_count = value.allowed_scopes.len();
    let allowed_scopes = value
        .allowed_scopes
        .into_iter()
        .map(|scope| {
            DottedKey::new(scope).map_err(|error| {
                invalid(
                    "signed_execution_grant.claims.capability_entitlements.allowed_scopes",
                    error,
                )
            })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if allowed_scopes.len() != scope_count {
        return Err(invalid(
            "signed_execution_grant.claims.capability_entitlements.allowed_scopes",
            "scopes must be unique",
        ));
    }
    Ok(CapabilityEntitlement {
        key: CapabilityKey::from_versioned_key(VersionedKey::new(value.capability_key).map_err(
            |error| {
                invalid(
                    "signed_execution_grant.claims.capability_entitlements.capability_key",
                    error,
                )
            },
        )?),
        provider_id: parse_id(
            value.provider_id.map(|item| item.value),
            "signed_execution_grant.claims.capability_entitlements.provider_id",
        )?,
        location,
        effect,
        allowed_scopes,
        policy_decision,
    })
}

#[cfg(test)]
mod tests {
    use super::{decode, wire};

    fn entitlement(scopes: &[&str]) -> wire::CapabilityEntitlement {
        wire::CapabilityEntitlement {
            capability_key: "alpha.read@1".to_owned(),
            provider_id: Some(wire::CapabilityProviderId {
                value: "019fa6de-38fd-7000-8000-000000000421".to_owned(),
            }),
            location: wire::CapabilityLocation::Cloud.into(),
            effect: wire::ToolEffect::Read.into(),
            allowed_scopes: scopes.iter().map(ToString::to_string).collect(),
            policy_decision: wire::CapabilityPolicyDecision::Allow.into(),
        }
    }

    #[test]
    fn scope_insertion_order_is_irrelevant_but_duplicates_fail_closed() {
        let forward =
            decode(entitlement(&["alpha.read", "zone.read"])).expect("forward scope insertion");
        let reverse =
            decode(entitlement(&["zone.read", "alpha.read"])).expect("reverse scope insertion");
        assert_eq!(forward, reverse);
        assert!(decode(entitlement(&["alpha.read", "alpha.read"])).is_err());
    }
}
