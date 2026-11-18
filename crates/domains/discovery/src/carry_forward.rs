use std::collections::BTreeMap;

use musteragent_identity::RunId;
use musteragent_tool::ToolKey;

use crate::catalog::AuthorizedToolCatalog;
use crate::exposure::ActivationConstraints;
use crate::model::{
    CarryForwardEvidenceError, CarryForwardRejectionReason, DiscoveryCarryForwardDecision,
    DiscoveryCarryForwardEvidence, PriorToolExposure, RejectedSkillCarryForward,
    RejectedToolCarryForward,
};
use crate::{AuthorizedSkillCatalog, PriorSkillDisclosure, SkillDiscoveryContractError};

const CARRY_FORWARD_DECISION_SCHEMA: &str = "musteragent.discovery.carry-forward-decision.v3";
#[must_use]
pub fn reauthorize_tool_exposures(
    source_run_id: RunId,
    prior: &[PriorToolExposure],
    current: &AuthorizedToolCatalog,
    constraints: &ActivationConstraints,
    available_placements: &std::collections::BTreeSet<(
        musteragent_identity::CapabilityProviderId,
        musteragent_capability::CapabilityLocation,
    )>,
    pinned_rejections: &BTreeMap<ToolKey, CarryForwardRejectionReason>,
) -> DiscoveryCarryForwardDecision {
    let mut accepted_tool_refs = Vec::new();
    let mut rejected_tools = Vec::new();
    for exposure in prior {
        let Some(definition) = current.get_by_key(exposure.tool_ref.as_str()) else {
            let replacement = replacement_for(&exposure.tool_ref, current);
            rejected_tools.push(RejectedToolCarryForward {
                prior_tool_ref: exposure.tool_ref.clone(),
                prior_schema_hash: exposure.schema_hash,
                prior_provider_id: exposure.provider_id,
                prior_location: exposure.location,
                current_provider_id: replacement
                    .as_ref()
                    .and_then(|key| current.get_by_key(key.as_str()))
                    .map(|definition| definition.provider_id),
                current_location: replacement
                    .as_ref()
                    .and_then(|key| current.get_by_key(key.as_str()))
                    .map(|definition| definition.location),
                reason: pinned_rejections
                    .get(&exposure.tool_ref)
                    .copied()
                    .unwrap_or(if replacement.is_some() {
                        CarryForwardRejectionReason::VersionChanged
                    } else {
                        CarryForwardRejectionReason::NotAuthorized
                    }),
                replacement_tool_ref: replacement,
            });
            continue;
        };
        let reason = if definition.schema_hash != exposure.schema_hash {
            Some(CarryForwardRejectionReason::SchemaOrContentChanged)
        } else if definition.provider_id != exposure.provider_id
            || definition.location != exposure.location
            || !available_placements.contains(&(definition.provider_id, definition.location))
        {
            Some(CarryForwardRejectionReason::LocationUnavailable)
        } else if !definition
            .required_contexts
            .is_subset(&constraints.available_contexts)
        {
            Some(CarryForwardRejectionReason::RequiredContextUnavailable)
        } else {
            None
        };
        if let Some(reason) = reason {
            rejected_tools.push(RejectedToolCarryForward {
                prior_tool_ref: exposure.tool_ref.clone(),
                prior_schema_hash: exposure.schema_hash,
                prior_provider_id: exposure.provider_id,
                prior_location: exposure.location,
                current_provider_id: Some(definition.provider_id),
                current_location: Some(definition.location),
                reason,
                replacement_tool_ref: None,
            });
        } else {
            accepted_tool_refs.push(exposure.tool_ref.clone());
        }
    }
    DiscoveryCarryForwardDecision {
        source_run_id,
        accepted_tool_refs,
        rejected_tools,
        accepted_skill_refs: Vec::new(),
        rejected_skills: Vec::new(),
    }
}

/// Adds the Skill branch to an already computed Tool carry-forward decision.
/// Prior order is preserved; no current version is substituted automatically.
pub fn reauthorize_skill_disclosures(
    mut decision: DiscoveryCarryForwardDecision,
    prior: &[PriorSkillDisclosure],
    current: &AuthorizedSkillCatalog,
    already_disclosed: &std::collections::BTreeSet<musteragent_identity::VersionedKey>,
) -> Result<DiscoveryCarryForwardDecision, SkillDiscoveryContractError> {
    let canonical =
        crate::skill_disclosure::validate_prior_skill_disclosures(decision.source_run_id, prior)?;
    for disclosure in &canonical {
        let Ok(skill) = current.executable(disclosure.skill_ref.as_str()) else {
            let replacement = replacement_skill_for(&disclosure.skill_ref, current);
            decision.rejected_skills.push(RejectedSkillCarryForward {
                prior_skill_ref: disclosure.skill_ref.clone(),
                prior_instructions_hash: disclosure.instructions_hash,
                reason: if replacement.is_some() {
                    CarryForwardRejectionReason::VersionChanged
                } else {
                    CarryForwardRejectionReason::NotAuthorized
                },
                replacement_skill_ref: replacement,
            });
            continue;
        };
        if skill.skill_version_id != disclosure.skill_version_id
            || skill.instructions_hash != disclosure.instructions_hash
            || skill.instructions_ref != disclosure.instructions_ref
        {
            return Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure);
        }
        if !already_disclosed.contains(&disclosure.skill_ref) {
            decision
                .accepted_skill_refs
                .push(disclosure.skill_ref.clone());
        }
    }
    Ok(decision)
}

impl DiscoveryCarryForwardDecision {
    /// Canonical, content-addressable admission evidence. Every collection is
    /// already ordered by the prior exposure prefix or canonical catalog.
    pub fn encode_canonical_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&serde_json::json!({
            "accepted_skill_refs": self.accepted_skill_refs.iter()
                .map(|value| value.as_str()).collect::<Vec<_>>(),
            "accepted_tool_refs": self.accepted_tool_refs.iter()
                .map(|value| value.as_str()).collect::<Vec<_>>(),
            "rejected_skills": self.rejected_skills.iter().map(|value| serde_json::json!({
                "prior_instructions_hash": value.prior_instructions_hash.to_string(),
                "prior_skill_ref": value.prior_skill_ref.as_str(),
                "reason": rejection_reason(value.reason),
                "replacement_skill_ref": value.replacement_skill_ref.as_ref()
                    .map(|replacement| replacement.as_str()),
            })).collect::<Vec<_>>(),
            "rejected_tools": self.rejected_tools.iter().map(|value| serde_json::json!({
                "current_location": value.current_location.map(location),
                "current_provider_id": value.current_provider_id.map(|provider| provider.to_string()),
                "prior_location": location(value.prior_location),
                "prior_provider_id": value.prior_provider_id.to_string(),
                "prior_schema_hash": value.prior_schema_hash.to_string(),
                "prior_tool_ref": value.prior_tool_ref.as_str(),
                "reason": rejection_reason(value.reason),
                "replacement_tool_ref": value.replacement_tool_ref.as_ref()
                    .map(|replacement| replacement.as_str()),
            })).collect::<Vec<_>>(),
            "schema_version": CARRY_FORWARD_DECISION_SCHEMA,
            "source_run_id": self.source_run_id.to_string(),
        }))
    }
}

impl DiscoveryCarryForwardEvidence {
    pub fn decode_canonical_json(bytes: &[u8]) -> Result<Self, CarryForwardEvidenceError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| CarryForwardEvidenceError::Invalid)?;
        let object = value
            .as_object()
            .ok_or(CarryForwardEvidenceError::Invalid)?;
        if !has_exact_keys(
            object,
            &[
                "accepted_skill_refs",
                "accepted_tool_refs",
                "rejected_skills",
                "rejected_tools",
                "schema_version",
                "source_run_id",
            ],
        ) || !value
            .get("accepted_skill_refs")
            .is_some_and(serde_json::Value::is_array)
            || !value
                .get("rejected_skills")
                .is_some_and(serde_json::Value::is_array)
        {
            return Err(CarryForwardEvidenceError::Invalid);
        }
        let schema = value
            .get("schema_version")
            .and_then(serde_json::Value::as_str)
            .ok_or(CarryForwardEvidenceError::Invalid)?;
        if schema != CARRY_FORWARD_DECISION_SCHEMA {
            return Err(CarryForwardEvidenceError::Invalid);
        }
        let source_run_id = value
            .get("source_run_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(CarryForwardEvidenceError::Invalid)?
            .parse()
            .map_err(|_| CarryForwardEvidenceError::Invalid)?;
        let accepted_tool_refs = decode_tool_refs(&value, "accepted_tool_refs")?;
        let rejected = value
            .get("rejected_tools")
            .and_then(serde_json::Value::as_array)
            .ok_or(CarryForwardEvidenceError::Invalid)?;
        let rejected_tool_refs = rejected
            .iter()
            .map(|item| {
                let object = item.as_object().ok_or(CarryForwardEvidenceError::Invalid)?;
                if !has_exact_keys(
                    object,
                    &[
                        "current_location",
                        "current_provider_id",
                        "prior_location",
                        "prior_provider_id",
                        "prior_schema_hash",
                        "prior_tool_ref",
                        "reason",
                        "replacement_tool_ref",
                    ],
                ) {
                    return Err(CarryForwardEvidenceError::Invalid);
                }
                item.get("prior_tool_ref")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(CarryForwardEvidenceError::Invalid)
                    .and_then(parse_tool_ref)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if has_duplicates(&accepted_tool_refs)
            || has_duplicates(&rejected_tool_refs)
            || accepted_tool_refs
                .iter()
                .any(|tool| rejected_tool_refs.contains(tool))
        {
            return Err(CarryForwardEvidenceError::Invalid);
        }
        Ok(Self {
            source_run_id,
            accepted_tool_refs,
            rejected_tool_refs,
            exhaustive: true,
        })
    }
}

fn has_exact_keys(object: &serde_json::Map<String, serde_json::Value>, expected: &[&str]) -> bool {
    object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
}

fn decode_tool_refs(
    value: &serde_json::Value,
    field: &str,
) -> Result<Vec<ToolKey>, CarryForwardEvidenceError> {
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or(CarryForwardEvidenceError::Invalid)?
        .iter()
        .map(|item| {
            item.as_str()
                .ok_or(CarryForwardEvidenceError::Invalid)
                .and_then(parse_tool_ref)
        })
        .collect()
}

fn parse_tool_ref(value: &str) -> Result<ToolKey, CarryForwardEvidenceError> {
    musteragent_identity::VersionedKey::new(value)
        .map(ToolKey::from_versioned_key)
        .map_err(|_| CarryForwardEvidenceError::Invalid)
}

fn has_duplicates(values: &[ToolKey]) -> bool {
    values
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != values.len()
}

const fn location(value: musteragent_capability::CapabilityLocation) -> &'static str {
    match value {
        musteragent_capability::CapabilityLocation::Cloud => "cloud",
        musteragent_capability::CapabilityLocation::Backend => "backend",
        musteragent_capability::CapabilityLocation::Device => "device",
        musteragent_capability::CapabilityLocation::RemoteDevice => "remote_device",
        musteragent_capability::CapabilityLocation::Browser => "browser",
    }
}

const fn rejection_reason(value: CarryForwardRejectionReason) -> &'static str {
    match value {
        CarryForwardRejectionReason::NotAuthorized => "not_authorized",
        CarryForwardRejectionReason::VersionChanged => "version_changed",
        CarryForwardRejectionReason::SchemaOrContentChanged => "schema_or_content_changed",
        CarryForwardRejectionReason::GrantDenied => "grant_denied",
        CarryForwardRejectionReason::PolicyDenied => "policy_denied",
        CarryForwardRejectionReason::LocationUnavailable => "location_unavailable",
        CarryForwardRejectionReason::RequiredContextUnavailable => "required_context_unavailable",
    }
}

fn replacement_for(
    prior: &musteragent_tool::ToolKey,
    current: &AuthorizedToolCatalog,
) -> Option<musteragent_tool::ToolKey> {
    let family = prior
        .as_str()
        .rsplit_once('@')
        .map_or(prior.as_str(), |value| value.0);
    current
        .definitions()
        .iter()
        .find(|candidate| {
            candidate
                .tool_ref
                .as_str()
                .rsplit_once('@')
                .map_or(candidate.tool_ref.as_str(), |value| value.0)
                == family
        })
        .map(|candidate| candidate.tool_ref.clone())
}

fn replacement_skill_for(
    prior: &musteragent_identity::VersionedKey,
    current: &AuthorizedSkillCatalog,
) -> Option<musteragent_identity::VersionedKey> {
    let family = prior
        .as_str()
        .rsplit_once('@')
        .map_or(prior.as_str(), |value| value.0);
    current
        .definitions()
        .iter()
        .filter(|candidate| candidate.availability.is_available())
        .find(|candidate| {
            candidate
                .skill_ref
                .as_str()
                .rsplit_once('@')
                .map_or(candidate.skill_ref.as_str(), |value| value.0)
                == family
        })
        .map(|candidate| candidate.skill_ref.clone())
}
