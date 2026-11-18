//! Read-only candidate status. Never triggers Evaluation, Approval or Publication.
use super::*;
use crate::{
    EvolutionProposalPublicationReadiness as Readiness, ExperienceProposalInspection,
    ExperienceProposalInspectionQuery,
};
use musteragent_runtime_content::verify_content;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectInput {
    proposal_id: Uuid,
}
impl<M, E> BuiltinMemoryTools<M, E>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
{
    pub(super) async fn prepare_inspect(
        &self,
        context: MemoryToolContext,
        bytes: &[u8],
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        let input = match self.read_plan(&context, bytes, execution).await? {
            Some(crate::ReadPlan::ExperienceInspect { proposal_id }) => {
                InspectInput { proposal_id }
            }
            Some(_) => return Err(MemoryToolError::InvalidResult),
            None => serde_json::from_slice(bytes).map_err(|_| MemoryToolError::InvalidInput)?,
        };
        let query = ExperienceProposalInspectionQuery {
            tenant_id: context.actor.tenant_id,
            principal_id: context.actor.principal_id,
            agent_id: context.agent_id,
            proposal_id: EvolutionProposalId::from_uuid(input.proposal_id)
                .map_err(|_| MemoryToolError::InvalidInput)?,
        };
        let inspected =
            crate::retrieval::read_stage(execution, self.evolution.inspect_proposal(query.clone()))
                .await
                .map_err(map_retrieval_error)?
                .map_err(|_| MemoryToolError::Store)?;
        let Some(mut inspected) = inspected else {
            let output = if let Some(planner) = &self.read_planner {
                super::inspection_projection::project(
                    planner.as_ref(),
                    None,
                    Value::Null,
                    execution,
                )
                .await?
            } else {
                serde_json::json!({"status":"not_found", "proposal":null})
            };
            return self.inspection_result(&context, output).await;
        };
        inspected
            .validate_for(&query)
            .map_err(|_| MemoryToolError::InvalidResult)?;
        crate::retrieval::read_stage(execution, async {
            crate::inspection_materialization::materialize_evolution_proposal_claim(
                &mut inspected.proposal,
                self.content.clone(),
            )
            .await;
            Ok::<(), MemoryToolError>(())
        })
        .await
        .map_err(map_retrieval_error)??;
        inspected
            .validate_for(&query)
            .map_err(|_| MemoryToolError::InvalidResult)?;
        let mut current = serde_json::json!({"status":"not_published", "version_id":null, "revision":null, "saved_version_is_current":null});
        if let Some(receipt) = &inspected.publication {
            current["status"] = "unavailable".into();
            let head = crate::retrieval::read_stage(
                execution,
                self.memory
                    .load_published_head(memory_visibility(&context, 1)?, receipt.memory_id),
            )
            .await
            .map_err(map_retrieval_error)?
            .map_err(|_| MemoryToolError::Store)?;
            if let Some(head) = head {
                let scope_matches = match inspected.proposal.candidate.target_scope {
                    EvolutionScope::User { principal_id } => {
                        head.record.scope == MemoryScope::User { principal_id }
                    }
                    EvolutionScope::Agent { agent_id } => {
                        head.record.scope == MemoryScope::Agent { agent_id }
                    }
                };
                if head.record.tenant_id != query.tenant_id
                    || head.record.memory_id != receipt.memory_id
                    || !scope_matches
                    || head.record.lifecycle != MemoryLifecycle::Active
                    || head.record.current_version_id != head.version.memory_version_id
                    || head.version.memory_id != head.record.memory_id
                    || head.record.kind != evolution_memory_kind(inspected.proposal.candidate.kind)?
                    || head.version.created_at > context.now
                    || head
                        .version
                        .valid_until
                        .is_some_and(|until| until <= context.now)
                {
                    return Err(MemoryToolError::InvalidResult);
                }
                head.record
                    .validate()
                    .map_err(|_| MemoryToolError::InvalidResult)?;
                head.version
                    .validate()
                    .map_err(|_| MemoryToolError::InvalidResult)?;
                if crate::is_experience_claim_media_type(
                    head.version.content_ref.media_type.as_str(),
                ) && head.version.content_ref.byte_length <= 1024 * 1024
                {
                    let bytes = crate::retrieval::read_stage(
                        execution,
                        self.content
                            .read(query.tenant_id, &head.version.content_ref),
                    )
                    .await
                    .map_err(map_retrieval_error)?;
                    if bytes.is_ok_and(|bytes| {
                        if verify_content(&head.version.content_ref, &bytes).is_err() {
                            return false;
                        }
                        ExperienceClaimDocument::decode(&bytes)
                            .and_then(|document| {
                                document.validate_for_kind(inspected.proposal.candidate.kind)
                            })
                            .is_ok()
                    }) {
                        current = serde_json::json!({"status":"active", "version_id":head.version.memory_version_id.to_string(), "revision":head.version.version.get().to_string(), "saved_version_is_current": head.version.memory_version_id == receipt.memory_version_id});
                    }
                }
            }
        } else if inspected.proposal.candidate.status == CandidateStatus::Promoted {
            current["status"] = "unavailable".into();
        }
        let output = if let Some(planner) = &self.read_planner {
            super::inspection_projection::project(
                planner.as_ref(),
                Some(&inspected),
                current,
                execution,
            )
            .await?
        } else {
            inspection_output(&inspected, current)
        };
        self.inspection_result(&context, output).await
    }
    async fn inspection_result(
        &self,
        context: &MemoryToolContext,
        output: Value,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        let bytes = serde_json::to_vec(&output).map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(MemoryToolPreparation::ReadOnly(
            self.result(&context.invocation, bytes).await?,
        ))
    }
}
fn inspection_output(inspected: &ExperienceProposalInspection, current: Value) -> Value {
    let proposal = &inspected.proposal;
    let candidate = &proposal.candidate;
    let (scope, scope_id) = match candidate.target_scope {
        EvolutionScope::User { principal_id } => ("account", principal_id.to_string()),
        EvolutionScope::Agent { agent_id } => ("agent", agent_id.to_string()),
    };
    let status = match candidate.status {
        CandidateStatus::Candidate => "candidate",
        CandidateStatus::Quarantined => "quarantined",
        CandidateStatus::Verified => "verified",
        CandidateStatus::Rejected => "rejected",
        CandidateStatus::Promoted => "promoted",
    };
    let readiness = if !candidate.kind.is_memory() {
        "unsupported_kind"
    } else {
        match proposal.publication_readiness() {
            Readiness::Eligible => "ready_for_publication",
            Readiness::AwaitingEvaluation => "awaiting_evaluation",
            Readiness::EvaluationRejected => "rejected",
            Readiness::AlreadyPublished => "already_published",
            Readiness::PolicyReviewRequired => "unsupported_kind",
            Readiness::ClaimContentUnavailable => "claim_unavailable",
            Readiness::ClaimContractInvalid => "claim_invalid",
        }
    };
    serde_json::json!({"status":"found", "proposal":{
        "proposal_id":candidate.proposal_id.to_string(), "scope":scope, "scope_id":scope_id,
        "kind":experience_kind_name(candidate.kind), "candidate_status":status,
        "evaluation": {"status":proposal.evaluation.as_ref().map_or(if matches!(candidate.status, CandidateStatus::Candidate | CandidateStatus::Quarantined) {"pending"} else {"not_evaluated"}, |value| if value.passed {"passed"} else {"failed"}), "evaluation_id":proposal.evaluation.as_ref().map(|value|value.evaluation_id.to_string())},
        "publication_readiness":readiness,
        "publication":inspected.publication.as_ref().map(|receipt|serde_json::json!({"memory_id":receipt.memory_id.to_string(), "saved_version_id":receipt.memory_version_id.to_string(), "saved_revision":receipt.revision.get().to_string()})),
        "current":current,
    }})
}
#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (
        ExperienceProposalInspection,
        ExperienceProposalInspectionQuery,
    ) {
        use musteragent_content_ref::ContentRef;
        use musteragent_evolution::{EvolutionEvaluation, ExperienceCandidateOrigin};
        use musteragent_identity::{ContentId, EvolutionEvaluationId, PrincipalId, RunId};
        let query = ExperienceProposalInspectionQuery {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            proposal_id: EvolutionProposalId::generate(),
        };
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(EXPERIENCE_CLAIM_MEDIA_TYPE).unwrap(),
            byte_length: 100,
            sha256: Sha256::digest(b"claim"),
        };
        let candidate = ExperienceCandidate {
            proposal_id: query.proposal_id,
            tenant_id: query.tenant_id,
            proposed_by: query.principal_id,
            origin: ExperienceCandidateOrigin::RuntimeTool,
            source_draft_id: None,
            target_scope: EvolutionScope::Agent {
                agent_id: query.agent_id,
            },
            target_memory_id: None,
            supersedes_proposal_id: None,
            kind: ExperienceKind::Preference,
            claim_ref: reference.clone(),
            source_run_ids: BTreeSet::from([RunId::generate()]),
            evidence_record_ids: BTreeSet::from([LedgerRecordId::generate()]),
            status: CandidateStatus::Verified,
            confidence: Some(ConfidenceBps::new(9000).unwrap()),
            created_at: UtcTimestamp::from_unix_millis(1000),
        };
        let claim = ExperienceClaimDocument::manual(
            "Use Chinese".into(),
            Default::default(),
            "Explicit request".into(),
            "private".into(),
        )
        .materialize_inspection()
        .unwrap()
        .inspection;
        (
            ExperienceProposalInspection {
                proposal: crate::EvolutionProposalInspection {
                    candidate,
                    claim_detail: claim,
                    evaluation: Some(EvolutionEvaluation {
                        evaluation_id: EvolutionEvaluationId::generate(),
                        proposal_id: query.proposal_id,
                        evaluated_by: query.principal_id,
                        passed: true,
                        report_ref: reference,
                        evaluated_at: UtcTimestamp::from_unix_millis(1100),
                    }),
                    approval_interaction_id: None,
                    published_memory_version_id: None,
                    published_instinct_id: None,
                    studio_revision_draft_id: None,
                    studio_artifact_kind: None,
                },
                publication: None,
            },
            query,
        )
    }
    fn current_unavailable() -> Value {
        serde_json::json!({"status":"unavailable", "version_id":null, "revision":null, "saved_version_is_current":null})
    }
    #[test]
    fn pending_failed_and_approved_candidates_never_become_publication_receipts() {
        let (mut value, query) = fixture();
        value.proposal.approval_interaction_id = Some(InteractionId::generate());
        value.validate_for(&query).unwrap();
        let output = inspection_output(&value, current_unavailable());
        assert_eq!(
            output["proposal"]["publication_readiness"],
            "ready_for_publication"
        );
        assert!(output["proposal"]["publication"].is_null());
        value.proposal.candidate.status = CandidateStatus::Rejected;
        value.proposal.evaluation.as_mut().unwrap().passed = false;
        value.proposal.approval_interaction_id = None;
        value.validate_for(&query).unwrap();
        let output = inspection_output(&value, current_unavailable());
        assert_eq!(output["proposal"]["evaluation"]["status"], "failed");
        assert_eq!(output["proposal"]["publication_readiness"], "rejected");
        for status in [CandidateStatus::Candidate, CandidateStatus::Quarantined] {
            value.proposal.candidate.status = status;
            value.proposal.evaluation = None;
            value.validate_for(&query).unwrap();
            let output = inspection_output(&value, current_unavailable());
            assert_eq!(output["proposal"]["evaluation"]["status"], "pending");
            assert_eq!(
                output["proposal"]["publication_readiness"],
                "awaiting_evaluation"
            );
            assert!(output["proposal"]["publication"].is_null());
        }
    }
    #[test]
    fn promotion_requires_exact_saved_version_and_does_not_imply_current_active() {
        let (mut value, query) = fixture();
        value.proposal.candidate.status = CandidateStatus::Promoted;
        assert!(value.validate_for(&query).is_err());
        let version_id = MemoryVersionId::generate();
        value.proposal.published_memory_version_id = Some(version_id);
        value.proposal.approval_interaction_id = Some(InteractionId::generate());
        value.publication = Some(crate::ExperiencePublicationReceipt {
            memory_id: MemoryId::generate(),
            memory_version_id: version_id,
            revision: VersionNumber::new(1).unwrap(),
        });
        value.validate_for(&query).unwrap();
        let output = inspection_output(&value, current_unavailable());
        assert_eq!(
            output["proposal"]["publication_readiness"],
            "already_published"
        );
        assert_eq!(output["proposal"]["current"]["status"], "unavailable");
        value.publication.as_mut().unwrap().memory_version_id = MemoryVersionId::generate();
        assert!(value.validate_for(&query).is_err());
        value.publication = None;
        let mut other = query.clone();
        other.agent_id = AgentId::generate();
        assert!(value.validate_for(&other).is_err());
        other = query.clone();
        other.tenant_id = TenantId::generate();
        assert!(value.validate_for(&other).is_err());
    }
    #[test]
    fn tool_input_has_no_client_selected_scope_authority() {
        let proposal_id = Uuid::parse_str("019fa6de-38fd-7000-8000-000000000001").expect("id");
        assert!(
            serde_json::from_value::<InspectInput>(serde_json::json!({"proposal_id":proposal_id}))
                .is_ok()
        );
        for field in ["scope", "scope_id", "agent_id", "principal_id"] {
            let mut input = serde_json::json!({"proposal_id":proposal_id});
            input[field] = "other".into();
            assert!(serde_json::from_value::<InspectInput>(input).is_err());
        }
    }
}
