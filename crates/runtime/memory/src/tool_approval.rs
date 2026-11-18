//! Memory business evidence remains bound to both the actual Tool envelope and
//! the unchanged canonical Evolution statement in the original Content Store.
use crate::{EvolutionReview, ExperienceClaimDocument, MemoryApplicationError, MemoryToolError};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{
    EvolutionApproval, EvolutionScope, ExperienceCandidate, ExperienceKind,
    evolution_approval_prompt_bytes,
};
use musteragent_identity::{Actor, AgentId, ContentId, InvocationId, MemoryVersionId, TenantId};
use musteragent_runtime_capability::CapabilityInvocation;
use musteragent_runtime_capability::approval_prompt::{
    VerifiedApprovalCapability, verify_approval_capability,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use serde::Serialize;

const MEDIA: &str = "application/vnd.musteragent.agent-memory-publication-approval+json";
const SCHEMA: &str = "musteragent.agent-memory.publication-approval.v1";

/// Original immutable Tool identity plus the trusted current Run's Agent.
/// This input conveys no approval: the original Interaction and publication
/// transaction must independently establish consent and live authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolEvolutionApprovalIdentity {
    pub actor: Actor,
    pub tenant_id: TenantId,
    pub agent_id: AgentId,
    pub invocation_id: InvocationId,
    pub request_ref: ContentRef,
}
impl ToolEvolutionApprovalIdentity {
    pub(crate) fn from_invocation(
        invocation: &CapabilityInvocation,
        agent_id: AgentId,
    ) -> Result<Self, MemoryToolError> {
        if invocation.tool_key.as_str() != "experience.publish@1"
            || invocation.capability_key.as_str() != "experience.publish@1"
        {
            return Err(MemoryToolError::IdentityMismatch);
        }
        Ok(Self {
            actor: invocation.actor,
            tenant_id: invocation.tenant_id,
            agent_id,
            invocation_id: invocation.invocation_id,
            request_ref: invocation.request_ref.clone(),
        })
    }
}

/// Exact display bytes and the separately stored canonical statement. The
/// executor owns the enclosing ApprovalPromptEnvelope and its immutable ref.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolEvolutionApprovalPrompt {
    pub media_type: MediaType,
    pub bytes: Vec<u8>,
    pub canonical_statement: ContentRef,
}

pub async fn prepare_tool_evolution_approval_prompt(
    content: &dyn ContentStore,
    identity: &ToolEvolutionApprovalIdentity,
    review: &EvolutionReview,
    head: Option<MemoryVersionId>,
) -> Result<ToolEvolutionApprovalPrompt, MemoryToolError> {
    authorize_identity_scope(&review.candidate, identity)?;
    let (media_type, bytes, canonical_statement) =
        publication_statement(content, identity, review, head, true).await?;
    Ok(ToolEvolutionApprovalPrompt {
        media_type,
        bytes,
        canonical_statement,
    })
}

pub async fn verify_tool_evolution_approval(
    content: &dyn ContentStore,
    identity: &ToolEvolutionApprovalIdentity,
    review: &EvolutionReview,
    head: Option<MemoryVersionId>,
    interaction: &crate::ApprovedEvolutionInteraction,
) -> Result<(EvolutionApproval, ToolEvolutionApproval), MemoryToolError> {
    authorize_identity_scope(&review.candidate, identity)?;
    if interaction.approved_by != identity.actor.principal_id {
        return Err(MemoryToolError::IdentityMismatch);
    }
    let statement = publication_statement(content, identity, review, head, false).await?;
    let binding = bind_approved_publication(content, identity, interaction, &statement).await?;
    let approval = EvolutionApproval {
        approval_interaction_id: interaction.interaction_id,
        proposal_id: review.candidate.proposal_id,
        evaluation_id: review.evaluation.evaluation_id,
        expected_current_memory_version_id: head,
        prompt_ref: statement.2,
        approved_by: interaction.approved_by,
        approved_at: interaction.approved_at,
    };
    approval
        .validate_for(&review.candidate, &review.evaluation)
        .map_err(|_| MemoryToolError::IdentityMismatch)?;
    Ok((approval, binding))
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolEvolutionApproval {
    envelope: VerifiedApprovalCapability,
    canonical_statement: ContentRef,
    interaction: crate::ApprovedEvolutionInteraction,
    agent_id: AgentId,
}
impl ToolEvolutionApproval {
    pub fn agent_id(&self) -> AgentId {
        self.agent_id
    }
    pub fn invocation_id(&self) -> InvocationId {
        self.envelope.invocation_id()
    }
    pub fn prompt_ref(&self) -> &ContentRef {
        self.envelope.prompt_ref()
    }
    pub fn request_ref(&self) -> &ContentRef {
        self.envelope.request_ref()
    }
    pub(crate) fn validate_for(
        &self,
        approval: &EvolutionApproval,
    ) -> Result<(), MemoryApplicationError> {
        if self.canonical_statement != approval.prompt_ref
            || self.interaction.interaction_id != approval.approval_interaction_id
            || self.interaction.approved_by != approval.approved_by
            || self.interaction.approved_at != approval.approved_at
        {
            return Err(MemoryApplicationError::InvalidPromotion);
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct Identity<'a> {
    content_id: String,
    media_type: &'a str,
    byte_length: u64,
    sha256: String,
}
impl<'a> From<&'a ContentRef> for Identity<'a> {
    fn from(value: &'a ContentRef) -> Self {
        Self {
            content_id: value.content_id.to_string(),
            media_type: value.media_type.as_str(),
            byte_length: value.byte_length,
            sha256: value.sha256.to_string(),
        }
    }
}
#[derive(Serialize)]
struct Claim<'a> {
    kind: &'static str,
    claim: &'a str,
    conditions: &'a serde_json::Map<String, serde_json::Value>,
    outcome: &'a str,
    sensitivity: &'a str,
}
#[derive(Serialize)]
struct Statement<'a> {
    schema: &'static str,
    proposal_id: String,
    evaluation_id: String,
    scope: &'static str,
    scope_id: String,
    claim: Claim<'a>,
    claim_ref: Identity<'a>,
    canonical_statement: Identity<'a>,
    request: Identity<'a>,
}

pub(crate) fn authorize_scope(
    candidate: &ExperienceCandidate,
    invocation: &CapabilityInvocation,
    agent_id: AgentId,
) -> Result<(), MemoryToolError> {
    let identity = ToolEvolutionApprovalIdentity::from_invocation(invocation, agent_id)?;
    authorize_identity_scope(candidate, &identity)
}
fn authorize_identity_scope(
    candidate: &ExperienceCandidate,
    identity: &ToolEvolutionApprovalIdentity,
) -> Result<(), MemoryToolError> {
    let owns_scope = match candidate.target_scope {
        EvolutionScope::Agent { agent_id: target } => target == identity.agent_id,
        EvolutionScope::User { principal_id } => principal_id == identity.actor.principal_id,
    };
    if candidate.tenant_id != identity.tenant_id
        || identity.actor.tenant_id != identity.tenant_id
        || identity.actor.principal_kind != musteragent_identity::PrincipalKind::User
        || !owns_scope
    {
        return Err(MemoryToolError::IdentityMismatch);
    }
    Ok(())
}

/// Store the canonical statement before asking for consent; a resumed execution
/// reads this same identity. A changed evaluation/head cannot replace its bytes.
pub(crate) async fn publication_statement(
    content: &dyn ContentStore,
    invocation: &ToolEvolutionApprovalIdentity,
    review: &EvolutionReview,
    head: Option<MemoryVersionId>,
    create: bool,
) -> Result<(MediaType, Vec<u8>, ContentRef), MemoryToolError> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        proposal_id: String,
    }
    let request_bytes = content
        .read(invocation.tenant_id, &invocation.request_ref)
        .await
        .map_err(|_| MemoryToolError::Content)?;
    verify_content(&invocation.request_ref, &request_bytes)
        .map_err(|_| MemoryToolError::Content)?;
    let request: Request =
        serde_json::from_slice(&request_bytes).map_err(|_| MemoryToolError::InvalidInput)?;
    let requested_proposal =
        uuid::Uuid::parse_str(&request.proposal_id).map_err(|_| MemoryToolError::InvalidInput)?;
    if invocation.request_ref.media_type.as_str() != "application/json"
        || requested_proposal != review.candidate.proposal_id.into_uuid()
        || !crate::is_experience_claim_media_type(review.candidate.claim_ref.media_type.as_str())
    {
        return Err(MemoryToolError::IdentityMismatch);
    }
    let canonical = evolution_approval_prompt_bytes(&review.candidate, &review.evaluation, head)
        .map_err(|_| MemoryToolError::InvalidResult)?;
    let canonical_ref = ContentRef {
        content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
            &invocation.invocation_id.into_uuid(),
            b"memory-evolution-approval-statement@1",
        ))
        .map_err(|_| MemoryToolError::InvalidResult)?,
        media_type: MediaType::new(musteragent_evolution::EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE)
            .map_err(|_| MemoryToolError::InvalidResult)?,
        byte_length: canonical.len() as u64,
        sha256: Sha256::digest(&canonical),
    };
    if create {
        let stored = content
            .put_at(
                canonical_ref.content_id,
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: canonical_ref.media_type.clone(),
                    bytes: canonical.clone(),
                },
            )
            .await
            .map_err(|_| MemoryToolError::Content)?;
        if stored != canonical_ref {
            return Err(MemoryToolError::Content);
        }
    } else {
        let stored = content
            .read(invocation.tenant_id, &canonical_ref)
            .await
            .map_err(|_| MemoryToolError::IdentityMismatch)?;
        verify_content(&canonical_ref, &stored).map_err(|_| MemoryToolError::IdentityMismatch)?;
        if stored != canonical {
            return Err(MemoryToolError::IdentityMismatch);
        }
    }
    let claim_bytes = content
        .read(invocation.tenant_id, &review.candidate.claim_ref)
        .await
        .map_err(|_| MemoryToolError::Content)?;
    verify_content(&review.candidate.claim_ref, &claim_bytes)
        .map_err(|_| MemoryToolError::Content)?;
    let document =
        ExperienceClaimDocument::decode(&claim_bytes).map_err(|_| MemoryToolError::InvalidInput)?;
    document
        .validate_for_kind(review.candidate.kind)
        .map_err(|_| MemoryToolError::InvalidInput)?;
    let (scope, scope_id) = match review.candidate.target_scope {
        EvolutionScope::Agent { agent_id } => ("agent", agent_id.to_string()),
        EvolutionScope::User { principal_id } => ("account", principal_id.to_string()),
    };
    let kind = match review.candidate.kind {
        ExperienceKind::Semantic => "semantic",
        ExperienceKind::Procedure => "procedure",
        ExperienceKind::Failure => "failure",
        ExperienceKind::Preference => "preference",
        _ => return Err(MemoryToolError::InvalidInput),
    };
    let bytes = serde_json::to_vec(&Statement {
        schema: SCHEMA,
        proposal_id: review.candidate.proposal_id.to_string(),
        evaluation_id: review.evaluation.evaluation_id.to_string(),
        scope,
        scope_id,
        claim: Claim {
            kind,
            claim: &document.claim,
            conditions: &document.conditions,
            outcome: &document.outcome,
            sensitivity: &document.sensitivity,
        },
        claim_ref: (&review.candidate.claim_ref).into(),
        canonical_statement: (&canonical_ref).into(),
        request: (&invocation.request_ref).into(),
    })
    .map_err(|_| MemoryToolError::InvalidResult)?;
    Ok((
        MediaType::new(MEDIA).map_err(|_| MemoryToolError::InvalidResult)?,
        bytes,
        canonical_ref,
    ))
}

pub(crate) async fn bind_approved_publication(
    content: &dyn ContentStore,
    invocation: &ToolEvolutionApprovalIdentity,
    interaction: &crate::ApprovedEvolutionInteraction,
    statement: &(MediaType, Vec<u8>, ContentRef),
) -> Result<ToolEvolutionApproval, MemoryToolError> {
    let prompt_ref = &interaction.prompt_ref;
    let prompt = content
        .read(invocation.tenant_id, prompt_ref)
        .await
        .map_err(|_| MemoryToolError::Content)?;
    let request = content
        .read(invocation.tenant_id, &invocation.request_ref)
        .await
        .map_err(|_| MemoryToolError::Content)?;
    let envelope = verify_approval_capability(
        prompt_ref,
        &prompt,
        invocation.invocation_id,
        "experience__publish__v1",
        &invocation.request_ref,
        &request,
        &statement.0,
        &statement.1,
    )
    .map_err(|_| MemoryToolError::IdentityMismatch)?;
    Ok(ToolEvolutionApproval {
        envelope,
        canonical_statement: statement.2.clone(),
        interaction: interaction.clone(),
        agent_id: invocation.agent_id,
    })
}
