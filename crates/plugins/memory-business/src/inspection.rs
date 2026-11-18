//! Model-facing projection of original, already-authorized inspection facts.
//! Publication eligibility is a Host fact, not permission granted by this code.
use crate::InvalidInput;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Account,
    Agent,
}
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateStatus {
    Candidate,
    Quarantined,
    Verified,
    Rejected,
    Promoted,
}
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    Eligible,
    AwaitingEvaluation,
    EvaluationRejected,
    AlreadyPublished,
    UnsupportedKind,
    ClaimContentUnavailable,
    ClaimContractInvalid,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evaluation {
    pub evaluation_id: Uuid,
    pub passed: bool,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publication {
    pub memory_id: Uuid,
    pub saved_version_id: Uuid,
    pub saved_revision: u64,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Current {
    NotPublished,
    Unavailable,
    Active {
        version_id: Uuid,
        revision: u64,
        saved_version_is_current: bool,
    },
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Proposal {
    pub proposal_id: Uuid,
    pub scope: Scope,
    pub scope_id: Uuid,
    pub kind: String,
    pub candidate_status: CandidateStatus,
    pub readiness: Readiness,
    pub evaluation: Option<Evaluation>,
    pub publication: Option<Publication>,
    pub current: Current,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionFacts {
    pub proposal: Option<Proposal>,
}

impl InspectionFacts {
    pub fn validate(&self) -> Result<(), InvalidInput> {
        let Some(value) = &self.proposal else {
            return Ok(());
        };
        if value.proposal_id.is_nil()
            || value.scope_id.is_nil()
            || ![
                "semantic",
                "procedure",
                "failure",
                "preference",
                "policy_proposal",
                "skill",
                "instinct",
                "persona_patch",
            ]
            .contains(&value.kind.as_str())
            || value
                .evaluation
                .as_ref()
                .is_some_and(|evaluation| evaluation.evaluation_id.is_nil())
            || value.publication.as_ref().is_some_and(|publication| {
                publication.memory_id.is_nil()
                    || publication.saved_version_id.is_nil()
                    || publication.saved_revision == 0
            })
        {
            return Err(InvalidInput);
        }
        if let Current::Active {
            version_id,
            revision,
            saved_version_is_current,
        } = value.current
        {
            let saved = value.publication.as_ref().ok_or(InvalidInput)?;
            if version_id.is_nil()
                || revision == 0
                || saved_version_is_current != (version_id == saved.saved_version_id)
                || saved_version_is_current && revision != saved.saved_revision
            {
                return Err(InvalidInput);
            }
        }
        Ok(())
    }
}

pub fn project(facts: &InspectionFacts) -> Result<Value, InvalidInput> {
    facts.validate()?;
    let Some(value) = &facts.proposal else {
        return Ok(json!({"status":"not_found","proposal":null}));
    };
    let readiness = match value.readiness {
        Readiness::Eligible => "ready_for_publication",
        Readiness::AwaitingEvaluation => "awaiting_evaluation",
        Readiness::EvaluationRejected => "rejected",
        Readiness::AlreadyPublished => "already_published",
        Readiness::UnsupportedKind => "unsupported_kind",
        Readiness::ClaimContentUnavailable => "claim_unavailable",
        Readiness::ClaimContractInvalid => "claim_invalid",
    };
    let evaluation = value.evaluation.as_ref().map_or(
        if matches!(
            value.candidate_status,
            CandidateStatus::Candidate | CandidateStatus::Quarantined
        ) {
            "pending"
        } else {
            "not_evaluated"
        },
        |evaluation| {
            if evaluation.passed {
                "passed"
            } else {
                "failed"
            }
        },
    );
    let current = match value.current {
        Current::Active {
            version_id,
            revision,
            saved_version_is_current,
        } => {
            json!({"status":"active","version_id":version_id,"revision":revision.to_string(),"saved_version_is_current":saved_version_is_current})
        }
        Current::NotPublished => {
            json!({"status":"not_published","version_id":null,"revision":null,"saved_version_is_current":null})
        }
        Current::Unavailable => {
            json!({"status":"unavailable","version_id":null,"revision":null,"saved_version_is_current":null})
        }
    };
    Ok(json!({"status":"found","proposal":{
        "proposal_id":value.proposal_id,"scope":value.scope,"scope_id":value.scope_id,"kind":value.kind,
        "candidate_status":value.candidate_status,
        "evaluation":{"status":evaluation,"evaluation_id":value.evaluation.as_ref().map(|item|item.evaluation_id)},
        "publication_readiness":readiness,
        "publication":value.publication.as_ref().map(|item|json!({"memory_id":item.memory_id,"saved_version_id":item.saved_version_id,"saved_revision":item.saved_revision.to_string()})),
        "current":current,
    }}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn advisory_readiness_never_creates_a_publication_receipt() {
        let facts = InspectionFacts {
            proposal: Some(Proposal {
                proposal_id: Uuid::from_u128(1),
                scope: Scope::Agent,
                scope_id: Uuid::from_u128(2),
                kind: "preference".into(),
                candidate_status: CandidateStatus::Verified,
                readiness: Readiness::Eligible,
                evaluation: Some(Evaluation {
                    evaluation_id: Uuid::from_u128(3),
                    passed: true,
                }),
                publication: None,
                current: Current::NotPublished,
            }),
        };
        let output = project(&facts).unwrap();
        assert_eq!(
            output["proposal"]["publication_readiness"],
            "ready_for_publication"
        );
        assert!(output["proposal"]["publication"].is_null());
        assert_eq!(output["proposal"]["current"]["status"], "not_published");
        assert_eq!(
            project(&InspectionFacts { proposal: None }).unwrap(),
            json!({"status":"not_found","proposal":null})
        );
    }
}
