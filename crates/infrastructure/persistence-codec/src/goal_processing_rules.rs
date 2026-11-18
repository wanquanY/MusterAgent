//! Canonical business-rule versioning. State/control revisions never change
//! this version; changing the objective, criteria or executor does.
use crate::wire;
use musteragent_content_ref::Sha256;
use musteragent_goal::{GoalCompletionContract, GoalProcessingRules, GoalRiskClass};
use prost::Message;

#[derive(Debug, thiserror::Error)]
#[error("invalid Goal processing rules")]
pub struct RulesCodecError;

pub const MAX_RULES_BYTES: usize = 512 * 1024;

pub fn encode(value: &GoalProcessingRules) -> Result<Vec<u8>, RulesCodecError> {
    value.validate().map_err(|_| RulesCodecError)?;
    let bytes = wire::GoalProcessingRules {
        target_agent_id: Some(wire::AgentId {
            value: value.target_agent_id.to_string(),
        }),
        title: value.title.clone(),
        objective_ref: Some(crate::encode_content_ref(&value.objective_ref)),
        constraints: value.constraints.clone(),
        completion: Some(wire::StandingGoalCompletionContract {
            success_criteria: value.completion.success_criteria.clone(),
            evidence_requirements: value.completion.evidence_requirements.clone(),
            failure_conditions: value.completion.failure_conditions.clone(),
            minimum_evidence_items: u32::from(value.completion.minimum_evidence_items),
            risk_class: match value.completion.risk_class {
                GoalRiskClass::Standard => wire::StandingGoalRiskClass::Standard as i32,
                GoalRiskClass::HighRisk => wire::StandingGoalRiskClass::HighRisk as i32,
            },
        }),
    }
    .encode_to_vec();
    if bytes.len() > MAX_RULES_BYTES {
        return Err(RulesCodecError);
    }
    Ok(bytes)
}

pub fn version(bytes: &[u8]) -> Sha256 {
    let mut material = b"standing-goal-processing-rules@1\0".to_vec();
    material.extend_from_slice(bytes);
    Sha256::digest(material)
}

pub fn decode(bytes: &[u8]) -> Result<GoalProcessingRules, RulesCodecError> {
    if bytes.len() > MAX_RULES_BYTES {
        return Err(RulesCodecError);
    }
    let value = wire::GoalProcessingRules::decode(bytes).map_err(|_| RulesCodecError)?;
    let completion = value.completion.ok_or(RulesCodecError)?;
    let rules = GoalProcessingRules {
        target_agent_id: value
            .target_agent_id
            .ok_or(RulesCodecError)?
            .value
            .parse()
            .map_err(|_| RulesCodecError)?,
        title: value.title,
        objective_ref: crate::wire_common::decode_content_ref(
            value.objective_ref.ok_or(RulesCodecError)?,
        )
        .map_err(|_| RulesCodecError)?,
        constraints: value.constraints,
        completion: GoalCompletionContract {
            success_criteria: completion.success_criteria,
            evidence_requirements: completion.evidence_requirements,
            failure_conditions: completion.failure_conditions,
            minimum_evidence_items: completion
                .minimum_evidence_items
                .try_into()
                .map_err(|_| RulesCodecError)?,
            risk_class: match wire::StandingGoalRiskClass::try_from(completion.risk_class) {
                Ok(wire::StandingGoalRiskClass::Standard) => GoalRiskClass::Standard,
                Ok(wire::StandingGoalRiskClass::HighRisk) => GoalRiskClass::HighRisk,
                _ => return Err(RulesCodecError),
            },
        },
    };
    if encode(&rules)? != bytes {
        return Err(RulesCodecError);
    }
    Ok(rules)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{ContentRef, MediaType};
    use musteragent_identity::{AgentId, ContentId};

    fn rules() -> GoalProcessingRules {
        GoalProcessingRules {
            target_agent_id: AgentId::generate(),
            title: "Process accepted source events".into(),
            objective_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain").unwrap(),
                byte_length: 9,
                sha256: Sha256::digest(b"objective"),
            },
            constraints: vec!["Only act on the user's action items".into()],
            completion: GoalCompletionContract {
                risk_class: GoalRiskClass::Standard,
                success_criteria: vec!["Action items reconciled".into()],
                evidence_requirements: vec!["Original source reference".into()],
                failure_conditions: vec![],
                minimum_evidence_items: 1,
            },
        }
    }

    #[test]
    fn canonical_rules_round_trip_and_semantic_changes_have_distinct_versions() {
        let mut rules = rules();
        let initial = encode(&rules).unwrap();
        assert_eq!(decode(&initial).unwrap(), rules);
        assert_eq!(version(&encode(&rules).unwrap()), version(&initial));
        rules.completion.minimum_evidence_items = 2;
        assert_ne!(version(&encode(&rules).unwrap()), version(&initial));
        rules.completion.minimum_evidence_items = 1;
        rules.target_agent_id = AgentId::generate();
        assert_ne!(version(&encode(&rules).unwrap()), version(&initial));
    }

    #[test]
    fn noncanonical_unknown_fields_and_invalid_text_cannot_create_a_rule_version() {
        let rules = rules();
        let bytes = encode(&rules).unwrap();
        let mut unknown = bytes.clone();
        unknown.extend_from_slice(&[0x78, 0x01]);
        assert!(decode(&unknown).is_err());
        let mut wire = wire::GoalProcessingRules::decode(bytes.as_slice()).unwrap();
        wire.completion.as_mut().unwrap().risk_class = 0;
        assert!(decode(&wire.encode_to_vec()).is_err());
        let mut invalid = rules;
        invalid.constraints[0].push('\0');
        assert!(encode(&invalid).is_err());
    }
}
