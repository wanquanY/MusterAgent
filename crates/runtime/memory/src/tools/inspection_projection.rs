//! Data supplied to a private formatter is already verified by the original
//! inspection owner. A formatter cannot invent identity or publication facts.
use super::*;
use crate::{EvolutionProposalPublicationReadiness as Ready, ExperienceProposalInspection};
use musteragent_memory_business::inspection as wire;

pub(super) async fn project(
    planner: &dyn crate::MemoryReadPlanner,
    inspected: Option<&ExperienceProposalInspection>,
    current: Value,
    execution: Option<&musteragent_runtime_capability::OperationContext>,
) -> Result<Value, MemoryToolError> {
    let facts = facts(inspected, &current)?;
    facts
        .validate()
        .map_err(|_| MemoryToolError::InvalidResult)?;
    let value =
        crate::read_planner::complete_computation(execution, planner.project_inspection(&facts))
            .await?;
    verify_projection(&facts, &current, &value)?;
    Ok(value)
}

fn facts(
    inspected: Option<&ExperienceProposalInspection>,
    current: &Value,
) -> Result<wire::InspectionFacts, MemoryToolError> {
    let Some(inspected) = inspected else {
        return Ok(wire::InspectionFacts { proposal: None });
    };
    let proposal = &inspected.proposal;
    let candidate = &proposal.candidate;
    let (scope, scope_id) = match candidate.target_scope {
        EvolutionScope::User { principal_id } => (wire::Scope::Account, principal_id.into_uuid()),
        EvolutionScope::Agent { agent_id } => (wire::Scope::Agent, agent_id.into_uuid()),
    };
    let candidate_status = match candidate.status {
        CandidateStatus::Candidate => wire::CandidateStatus::Candidate,
        CandidateStatus::Quarantined => wire::CandidateStatus::Quarantined,
        CandidateStatus::Verified => wire::CandidateStatus::Verified,
        CandidateStatus::Rejected => wire::CandidateStatus::Rejected,
        CandidateStatus::Promoted => wire::CandidateStatus::Promoted,
    };
    let readiness = if !candidate.kind.is_memory() {
        wire::Readiness::UnsupportedKind
    } else {
        match proposal.publication_readiness() {
            Ready::Eligible => wire::Readiness::Eligible,
            Ready::AwaitingEvaluation => wire::Readiness::AwaitingEvaluation,
            Ready::EvaluationRejected => wire::Readiness::EvaluationRejected,
            Ready::AlreadyPublished => wire::Readiness::AlreadyPublished,
            Ready::PolicyReviewRequired => wire::Readiness::UnsupportedKind,
            Ready::ClaimContentUnavailable => wire::Readiness::ClaimContentUnavailable,
            Ready::ClaimContractInvalid => wire::Readiness::ClaimContractInvalid,
        }
    };
    let current = match current["status"].as_str() {
        Some("not_published") => wire::Current::NotPublished,
        Some("unavailable") => wire::Current::Unavailable,
        Some("active") => wire::Current::Active {
            version_id: Uuid::parse_str(
                current["version_id"]
                    .as_str()
                    .ok_or(MemoryToolError::InvalidResult)?,
            )
            .map_err(|_| MemoryToolError::InvalidResult)?,
            revision: current["revision"]
                .as_str()
                .ok_or(MemoryToolError::InvalidResult)?
                .parse()
                .map_err(|_| MemoryToolError::InvalidResult)?,
            saved_version_is_current: current["saved_version_is_current"]
                .as_bool()
                .ok_or(MemoryToolError::InvalidResult)?,
        },
        _ => return Err(MemoryToolError::InvalidResult),
    };
    Ok(wire::InspectionFacts {
        proposal: Some(wire::Proposal {
            proposal_id: candidate.proposal_id.into_uuid(),
            scope,
            scope_id,
            kind: experience_kind_name(candidate.kind).into(),
            candidate_status,
            readiness,
            evaluation: proposal.evaluation.as_ref().map(|value| wire::Evaluation {
                evaluation_id: value.evaluation_id.into_uuid(),
                passed: value.passed,
            }),
            publication: inspected
                .publication
                .as_ref()
                .map(|value| wire::Publication {
                    memory_id: value.memory_id.into_uuid(),
                    saved_version_id: value.memory_version_id.into_uuid(),
                    saved_revision: value.revision.get(),
                }),
            current,
        }),
    })
}

fn verify_projection(
    facts: &wire::InspectionFacts,
    current: &Value,
    value: &Value,
) -> Result<(), MemoryToolError> {
    let Some(fact) = &facts.proposal else {
        return if value["status"] == "not_found" && value.get("proposal") == Some(&Value::Null) {
            Ok(())
        } else {
            Err(MemoryToolError::InvalidResult)
        };
    };
    let proposal = &value["proposal"];
    let matches = value["status"] == "found"
        && proposal["proposal_id"] == fact.proposal_id.to_string()
        && proposal["scope_id"] == fact.scope_id.to_string()
        && proposal["scope"]
            == serde_json::to_value(&fact.scope).map_err(|_| MemoryToolError::InvalidResult)?
        && proposal["kind"] == fact.kind
        && proposal["candidate_status"]
            == serde_json::to_value(fact.candidate_status)
                .map_err(|_| MemoryToolError::InvalidResult)?
        && proposal["current"] == *current;
    let evaluation = &proposal["evaluation"];
    let evaluation_matches = match &fact.evaluation {
        Some(actual) => {
            evaluation["evaluation_id"] == actual.evaluation_id.to_string()
                && evaluation["status"] == if actual.passed { "passed" } else { "failed" }
        }
        None => {
            evaluation.get("evaluation_id") == Some(&Value::Null)
                && matches!(
                    evaluation["status"].as_str(),
                    Some("pending" | "not_evaluated")
                )
        }
    };
    let publication_matches = match &fact.publication {
        Some(actual) => {
            // The public contract carries revisions as decimal strings, not JSON numbers.
            let saved_revision = actual.saved_revision.to_string();
            proposal["publication"]["memory_id"] == actual.memory_id.to_string()
                && proposal["publication"]["saved_version_id"]
                    == actual.saved_version_id.to_string()
                && proposal["publication"]["saved_revision"].as_str()
                    == Some(saved_revision.as_str())
        }
        None => proposal.get("publication") == Some(&Value::Null),
    };
    // These labels describe original governance facts; they cannot confer
    // eligibility or turn a missing receipt into a successful publication.
    let readiness = match fact.readiness {
        wire::Readiness::Eligible => "ready_for_publication",
        wire::Readiness::AwaitingEvaluation => "awaiting_evaluation",
        wire::Readiness::EvaluationRejected => "rejected",
        wire::Readiness::AlreadyPublished => "already_published",
        wire::Readiness::UnsupportedKind => "unsupported_kind",
        wire::Readiness::ClaimContentUnavailable => "claim_unavailable",
        wire::Readiness::ClaimContractInvalid => "claim_invalid",
    };
    if !matches
        || !evaluation_matches
        || !publication_matches
        || proposal["publication_readiness"] != readiness
    {
        return Err(MemoryToolError::InvalidResult);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_revision_must_match_the_original_decimal_string_contract() {
        let facts: wire::InspectionFacts = serde_json::from_value(serde_json::json!({
            "proposal": {
                "proposal_id": Uuid::from_u128(1), "scope": "agent", "scope_id": Uuid::from_u128(2),
                "kind": "preference", "candidate_status": "promoted", "readiness": "already_published",
                "evaluation": {"evaluation_id": Uuid::from_u128(3), "passed": true},
                "publication": {"memory_id": Uuid::from_u128(4), "saved_version_id": Uuid::from_u128(5), "saved_revision": 7},
                "current": {"status": "active", "version_id": Uuid::from_u128(5), "revision": 7, "saved_version_is_current": true}
            }
        })).unwrap();
        let good = wire::project(&facts).unwrap();
        let current = &good["proposal"]["current"];
        assert!(verify_projection(&facts, current, &good).is_ok());
        for revision in [
            serde_json::json!(7),
            serde_json::json!("07"),
            serde_json::json!("8"),
            Value::Null,
        ] {
            let mut forged = good.clone();
            forged["proposal"]["publication"]["saved_revision"] = revision;
            assert!(verify_projection(&facts, current, &forged).is_err());
        }
    }

    #[test]
    fn formatter_cannot_turn_pending_facts_into_identity_evaluation_or_publication_receipts() {
        let facts = wire::InspectionFacts {
            proposal: Some(wire::Proposal {
                proposal_id: Uuid::from_u128(1),
                scope: wire::Scope::Agent,
                scope_id: Uuid::from_u128(2),
                kind: "preference".into(),
                candidate_status: wire::CandidateStatus::Candidate,
                readiness: wire::Readiness::AwaitingEvaluation,
                evaluation: None,
                publication: None,
                current: wire::Current::NotPublished,
            }),
        };
        let good = wire::project(&facts).unwrap();
        let current = good["proposal"]["current"].clone();
        assert!(verify_projection(&facts, &current, &good).is_ok());
        for change in [
            "scope_id",
            "proposal_id",
            "readiness",
            "evaluation",
            "publication",
            "current",
            "absence",
        ] {
            let mut value = good.clone();
            match change {
                "scope_id" | "proposal_id" => {
                    value["proposal"][change] = Uuid::from_u128(3).to_string().into()
                }
                "readiness" => {
                    value["proposal"]["publication_readiness"] = "ready_for_publication".into()
                }
                "evaluation" => value["proposal"]["evaluation"]["status"] = "passed".into(),
                "publication" => {
                    value["proposal"]["publication"] =
                        serde_json::json!({"memory_id":Uuid::from_u128(4)})
                }
                "current" => value["proposal"]["current"]["status"] = "active".into(),
                "absence" => value = serde_json::json!({"status":"not_found","proposal":null}),
                _ => unreachable!(),
            }
            assert!(
                verify_projection(&facts, &current, &value).is_err(),
                "{change}"
            );
        }
        assert!(
            verify_projection(&wire::InspectionFacts { proposal: None }, &current, &good).is_err()
        );
    }
}
