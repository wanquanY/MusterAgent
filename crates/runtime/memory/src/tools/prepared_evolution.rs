//! Exact evaluation/approval facts for Host-private publication recovery.
use super::prepared_facts::Content;
use super::*;
use crate::{ApprovedEvolutionInteraction, EvolutionReview};
use musteragent_evolution::{EvolutionEvaluation, ExperienceCandidateOrigin};
use musteragent_identity::{EvolutionEvaluationId, MemoryDraftId, PrincipalId, RunId};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Review {
    proposal_id: Uuid,
    tenant_id: Uuid,
    proposed_by: Uuid,
    origin: Origin,
    source_draft_id: Option<Uuid>,
    scope: Scope,
    target_memory_id: Option<Uuid>,
    supersedes_proposal_id: Option<Uuid>,
    kind: Kind,
    claim: Content,
    source_run_ids: BTreeSet<Uuid>,
    evidence_record_ids: BTreeSet<Uuid>,
    confidence_bps: u16,
    created_at: i64,
    evaluation_id: Uuid,
    evaluated_by: Uuid,
    report: Content,
    evaluated_at: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Origin {
    RuntimeTool,
    DreamInduction,
    UserDraft,
}
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Scope {
    User(Uuid),
    Agent(Uuid),
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Semantic,
    Procedure,
    Failure,
    Preference,
}

macro_rules! id {
    ($kind:ident, $value:expr) => {
        $kind::from_uuid($value).map_err(|_| MemoryToolError::InvalidResult)?
    };
}

impl Review {
    pub(super) fn capture(review: &EvolutionReview) -> Result<Self, MemoryToolError> {
        let c = &review.candidate;
        let e = &review.evaluation;
        c.validate().map_err(|_| MemoryToolError::InvalidResult)?;
        if c.status != CandidateStatus::Verified
            || !e.passed
            || e.proposal_id != c.proposal_id
            || e.evaluated_at < c.created_at
        {
            return Err(MemoryToolError::PublicationNotReady);
        }
        let kind = match c.kind {
            ExperienceKind::Semantic => Kind::Semantic,
            ExperienceKind::Procedure => Kind::Procedure,
            ExperienceKind::Failure => Kind::Failure,
            ExperienceKind::Preference => Kind::Preference,
            _ => return Err(MemoryToolError::InvalidInput),
        };
        Ok(Self {
            proposal_id: c.proposal_id.into_uuid(),
            tenant_id: c.tenant_id.into_uuid(),
            proposed_by: c.proposed_by.into_uuid(),
            origin: match c.origin {
                ExperienceCandidateOrigin::RuntimeTool => Origin::RuntimeTool,
                ExperienceCandidateOrigin::DreamInduction => Origin::DreamInduction,
                ExperienceCandidateOrigin::UserDraft => Origin::UserDraft,
            },
            source_draft_id: c.source_draft_id.map(|id| id.into_uuid()),
            scope: match c.target_scope {
                EvolutionScope::User { principal_id } => Scope::User(principal_id.into_uuid()),
                EvolutionScope::Agent { agent_id } => Scope::Agent(agent_id.into_uuid()),
            },
            target_memory_id: c.target_memory_id.map(|id| id.into_uuid()),
            supersedes_proposal_id: c.supersedes_proposal_id.map(|id| id.into_uuid()),
            kind,
            claim: (&c.claim_ref).into(),
            source_run_ids: c.source_run_ids.iter().map(|id| id.into_uuid()).collect(),
            evidence_record_ids: c
                .evidence_record_ids
                .iter()
                .map(|id| id.into_uuid())
                .collect(),
            confidence_bps: c.confidence.ok_or(MemoryToolError::InvalidResult)?.get(),
            created_at: c.created_at.unix_millis(),
            evaluation_id: e.evaluation_id.into_uuid(),
            evaluated_by: e.evaluated_by.into_uuid(),
            report: (&e.report_ref).into(),
            evaluated_at: e.evaluated_at.unix_millis(),
        })
    }

    pub(super) fn restore(self) -> Result<EvolutionReview, MemoryToolError> {
        let candidate = ExperienceCandidate {
            proposal_id: id!(EvolutionProposalId, self.proposal_id),
            tenant_id: id!(TenantId, self.tenant_id),
            proposed_by: id!(PrincipalId, self.proposed_by),
            origin: match self.origin {
                Origin::RuntimeTool => ExperienceCandidateOrigin::RuntimeTool,
                Origin::DreamInduction => ExperienceCandidateOrigin::DreamInduction,
                Origin::UserDraft => ExperienceCandidateOrigin::UserDraft,
            },
            source_draft_id: self
                .source_draft_id
                .map(|id| MemoryDraftId::from_uuid(id).map_err(|_| MemoryToolError::InvalidResult))
                .transpose()?,
            target_scope: match self.scope {
                Scope::User(id) => EvolutionScope::User {
                    principal_id: id!(PrincipalId, id),
                },
                Scope::Agent(id) => EvolutionScope::Agent {
                    agent_id: id!(AgentId, id),
                },
            },
            target_memory_id: self
                .target_memory_id
                .map(|id| MemoryId::from_uuid(id).map_err(|_| MemoryToolError::InvalidResult))
                .transpose()?,
            supersedes_proposal_id: self
                .supersedes_proposal_id
                .map(|id| {
                    EvolutionProposalId::from_uuid(id).map_err(|_| MemoryToolError::InvalidResult)
                })
                .transpose()?,
            kind: match self.kind {
                Kind::Semantic => ExperienceKind::Semantic,
                Kind::Procedure => ExperienceKind::Procedure,
                Kind::Failure => ExperienceKind::Failure,
                Kind::Preference => ExperienceKind::Preference,
            },
            claim_ref: self.claim.restore()?,
            source_run_ids: self
                .source_run_ids
                .into_iter()
                .map(|id| RunId::from_uuid(id).map_err(|_| MemoryToolError::InvalidResult))
                .collect::<Result<_, _>>()?,
            evidence_record_ids: self
                .evidence_record_ids
                .into_iter()
                .map(|id| LedgerRecordId::from_uuid(id).map_err(|_| MemoryToolError::InvalidResult))
                .collect::<Result<_, _>>()?,
            status: CandidateStatus::Verified,
            confidence: Some(
                ConfidenceBps::new(self.confidence_bps)
                    .map_err(|_| MemoryToolError::InvalidResult)?,
            ),
            created_at: UtcTimestamp::from_unix_millis(self.created_at),
        };
        let evaluation = EvolutionEvaluation {
            evaluation_id: id!(EvolutionEvaluationId, self.evaluation_id),
            proposal_id: candidate.proposal_id,
            evaluated_by: id!(PrincipalId, self.evaluated_by),
            passed: true,
            report_ref: self.report.restore()?,
            evaluated_at: UtcTimestamp::from_unix_millis(self.evaluated_at),
        };
        let review = EvolutionReview {
            candidate,
            evaluation,
        };
        Self::capture(&review)?;
        Ok(review)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Approval {
    interaction_id: Uuid,
    prompt: Content,
    approved_by: Uuid,
    approved_at: i64,
}
impl From<&ApprovedEvolutionInteraction> for Approval {
    fn from(value: &ApprovedEvolutionInteraction) -> Self {
        Self {
            interaction_id: value.interaction_id.into_uuid(),
            prompt: (&value.prompt_ref).into(),
            approved_by: value.approved_by.into_uuid(),
            approved_at: value.approved_at.unix_millis(),
        }
    }
}
impl Approval {
    pub(super) fn restore(self) -> Result<ApprovedEvolutionInteraction, MemoryToolError> {
        Ok(ApprovedEvolutionInteraction {
            interaction_id: id!(InteractionId, self.interaction_id),
            prompt_ref: self.prompt.restore()?,
            approved_by: id!(PrincipalId, self.approved_by),
            approved_at: UtcTimestamp::from_unix_millis(self.approved_at),
        })
    }
}
