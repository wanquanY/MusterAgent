//! Versioned proposal inputs stop model-authored display keys and operation
//! statuses from becoming approval facts. Both versions still enter the same
//! original candidate, evaluation, approval and publication owners.
use crate::InvalidInput;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

/// The retained v2 wire shape is also the internal input to the original
/// candidate creation path. Never reinterpret historical v2 bytes as v3.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalInput {
    pub scope: String,
    pub kind: String,
    pub claim: String,
    pub conditions: Map<String, Value>,
    pub outcome: String,
    pub confidence: f64,
    pub sensitivity: String,
    pub supersedes_candidate_id: Option<Uuid>,
}

impl ProposalInput {
    /// Host validation binds original facts and scope, independently of the
    /// package's mutable v3 purpose text. These checks do not grant publication.
    pub(crate) fn matches_request(&self, key: &str, bytes: &[u8]) -> Result<bool, InvalidInput> {
        self.validate()?;
        match key {
            "experience.propose@2" => Ok(*self == Self::decode(key, bytes)?),
            "experience.propose@3" => {
                let original: ProposalInputV3 =
                    serde_json::from_slice(bytes).map_err(|_| InvalidInput)?;
                original.validate()?;
                Ok(self.scope == original.scope
                    && self.kind == original.kind.key()
                    && self.claim == original.claim
                    && self.confidence == original.confidence
                    && self.sensitivity == original.sensitivity
                    && self.supersedes_candidate_id == original.supersedes_candidate_id
                    && self.conditions.len() == original.conditions.len()
                    && original.conditions.iter().enumerate().all(|(index, text)| {
                        self.conditions.get(&format!("{:02}", index + 1))
                            == Some(&Value::String(text.clone()))
                    }))
            }
            _ => Err(InvalidInput),
        }
    }

    pub fn decode(key: &str, bytes: &[u8]) -> Result<Self, InvalidInput> {
        let input: Self = match key {
            "experience.propose@2" => serde_json::from_slice(bytes).map_err(|_| InvalidInput)?,
            "experience.propose@3" => {
                let input: ProposalInputV3 =
                    serde_json::from_slice(bytes).map_err(|_| InvalidInput)?;
                input.into_canonical()?
            }
            _ => return Err(InvalidInput),
        };
        input.validate()?;
        Ok(input)
    }

    fn validate(&self) -> Result<(), InvalidInput> {
        if !matches!(self.scope.as_str(), "user" | "agent_profile")
            || !matches!(
                self.kind.as_str(),
                "semantic"
                    | "procedure"
                    | "failure"
                    | "preference"
                    | "policy_proposal"
                    | "skill"
                    | "instinct"
                    | "persona_patch"
            )
            || self.claim.trim().is_empty()
            || self.claim.len() > 8_000
            || self.conditions.len() > 50
            || self.outcome.trim().is_empty()
            || self.outcome.len() > 64
            || !self.confidence.is_finite()
            || !(0.0..=1.0).contains(&self.confidence)
            || !matches!(self.sensitivity.as_str(), "private" | "internal")
        {
            Err(InvalidInput)
        } else {
            Ok(())
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProposalInputV3 {
    scope: String,
    kind: ProposalKind,
    claim: String,
    conditions: Vec<String>,
    confidence: f64,
    sensitivity: String,
    supersedes_candidate_id: Option<Uuid>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProposalKind {
    Semantic,
    Procedure,
    Failure,
    Preference,
    PolicyProposal,
}

impl ProposalKind {
    fn key(self) -> &'static str {
        match self {
            Self::Semantic => "semantic",
            Self::Procedure => "procedure",
            Self::Failure => "failure",
            Self::Preference => "preference",
            Self::PolicyProposal => "policy_proposal",
        }
    }

    // This describes how a candidate could be used after publication. It is
    // never a claim about saving, consent, evaluation or current head state.
    fn material(self) -> (&'static str, &'static str) {
        match self {
            Self::Semantic => ("semantic", "为后续相关问题提供事实参考"),
            Self::Procedure => ("procedure", "为后续相似任务提供步骤参考"),
            Self::Failure => ("failure", "为后续相似问题提供经验参考"),
            Self::Preference => ("preference", "用于后续对话中的相关偏好"),
            Self::PolicyProposal => ("policy_proposal", "作为后续规则评估的参考建议"),
        }
    }
}

impl ProposalInputV3 {
    fn validate(&self) -> Result<(), InvalidInput> {
        if self.claim.chars().count() > 2_000
            || self.supersedes_candidate_id.is_some_and(|id| id.is_nil())
            || self.conditions.len() > 50
            || self
                .conditions
                .iter()
                .any(|condition| condition.trim().is_empty() || condition.chars().count() > 512)
        {
            return Err(InvalidInput);
        }
        Ok(())
    }

    fn into_canonical(self) -> Result<ProposalInput, InvalidInput> {
        self.validate()?;
        let (kind, purpose) = self.kind.material();
        // One stable numbered key per condition preserves the existing claim
        // contract and canonical approval binding without arbitrary model keys.
        // Zero-padding preserves original order in serde_json's sorted Map.
        let conditions = self
            .conditions
            .into_iter()
            .enumerate()
            .map(|(index, text)| (format!("{:02}", index + 1), Value::String(text)))
            .collect();
        Ok(ProposalInput {
            scope: self.scope,
            kind: kind.into(),
            claim: self.claim,
            conditions,
            outcome: purpose.into(),
            confidence: self.confidence,
            sensitivity: self.sensitivity,
            supersedes_candidate_id: self.supersedes_candidate_id,
        })
    }
}
