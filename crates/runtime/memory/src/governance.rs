use std::sync::Arc;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_evolution::{
    CandidateStatus, EvolutionApproval, EvolutionScope, ExperienceKind, PublishInstinct,
    evolution_approval_prompt_bytes,
};
use musteragent_identity::{
    AgentId, ContentId, DottedKey, EvolutionEvaluationId, EvolutionPromotionId,
    EvolutionProposalId, IdempotencyKey, InstinctId, InteractionId, MemoryId, MemoryVersionId,
    PrincipalId, StudioRevisionDraftId, TenantId, UtcTimestamp,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    EvolutionStore, MemoryPersistenceDisposition, MemoryStore, PrepareEvolutionPublication,
    PrepareInstinctPublication, PromoteExperience, prepare_evolution_publication,
    prepare_instinct_publication,
};

const MANAGEMENT_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x9f, 0xee, 0x91, 0x10, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x01,
]);
const MANAGEMENT_PROMPT_CONTENT_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x9f, 0xee, 0x91, 0x10, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x02,
]);
const MANAGEMENT_MEMORY_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x9f, 0xee, 0x91, 0x10, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x03,
]);
const MANAGEMENT_MEMORY_VERSION_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x9f, 0xee, 0x91, 0x10, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x04,
]);
const MANAGEMENT_PROMOTION_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x9f, 0xee, 0x91, 0x10, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x05,
]);
const MANAGEMENT_INSTINCT_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x9f, 0xee, 0x91, 0x10, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x06,
]);
const STUDIO_REVISION_DRAFT_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x9f, 0xee, 0x91, 0x10, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x07,
]);

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StudioRevisionArtifactKind {
    Skill,
    PersonaPatch,
}

impl StudioRevisionArtifactKind {
    fn from_experience_kind(
        value: ExperienceKind,
    ) -> Result<Self, ManagementEvolutionPublicationError> {
        match value {
            ExperienceKind::Skill => Ok(Self::Skill),
            ExperienceKind::PersonaPatch => Ok(Self::PersonaPatch),
            _ => Err(ManagementEvolutionPublicationError::InvalidCandidate),
        }
    }
}

/// Immutable Studio input. Approval creates this draft but never activates a
/// Skill or changes Persona behavior; Studio validation and publication remain
/// separate governed boundaries.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StudioRevisionDraft {
    pub studio_revision_draft_id: StudioRevisionDraftId,
    pub tenant_id: TenantId,
    pub target_agent_id: AgentId,
    pub proposal_id: EvolutionProposalId,
    pub evaluation_id: EvolutionEvaluationId,
    pub approval_interaction_id: InteractionId,
    pub promotion_id: EvolutionPromotionId,
    pub artifact_kind: StudioRevisionArtifactKind,
    pub payload_ref: musteragent_content_ref::ContentRef,
    pub created_by: PrincipalId,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishStudioRevisionDraft {
    pub candidate: musteragent_evolution::ExperienceCandidate,
    pub evaluation: musteragent_evolution::EvolutionEvaluation,
    pub approval: EvolutionApproval,
    pub draft: StudioRevisionDraft,
}

impl PublishStudioRevisionDraft {
    pub fn validate(&self) -> Result<(), ManagementEvolutionPublicationError> {
        self.candidate
            .validate()
            .map_err(|_| ManagementEvolutionPublicationError::InvalidCandidate)?;
        self.approval
            .validate_for(&self.candidate, &self.evaluation)
            .map_err(|_| ManagementEvolutionPublicationError::InvalidApproval)?;
        let agent_id = match self.candidate.target_scope {
            EvolutionScope::Agent { agent_id } => agent_id,
            EvolutionScope::User { .. } => {
                return Err(ManagementEvolutionPublicationError::InvalidCandidate);
            }
        };
        if !self.candidate.kind.is_studio_revision()
            || self.candidate.status != CandidateStatus::Promoted
            || self.candidate.target_memory_id.is_some()
            || self.approval.expected_current_memory_version_id.is_some()
            || self.draft.tenant_id != self.candidate.tenant_id
            || self.draft.target_agent_id != agent_id
            || self.draft.proposal_id != self.candidate.proposal_id
            || self.draft.evaluation_id != self.evaluation.evaluation_id
            || self.draft.approval_interaction_id != self.approval.approval_interaction_id
            || self.draft.artifact_kind
                != StudioRevisionArtifactKind::from_experience_kind(self.candidate.kind)?
            || self.draft.payload_ref != self.candidate.claim_ref
            || self.draft.created_by != self.approval.approved_by
            || self.draft.created_at < self.approval.approved_at
        {
            return Err(ManagementEvolutionPublicationError::InvalidCandidate);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ManagementEvolutionTarget {
    Memory(Box<PromoteExperience>),
    Instinct(Box<PublishInstinct>),
    StudioRevisionDraft(Box<PublishStudioRevisionDraft>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishManagementEvolution {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub proposal_id: EvolutionProposalId,
    pub idempotency_key: IdempotencyKey,
    pub approved_at: UtcTimestamp,
    pub target: ManagementEvolutionTarget,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ManagementEvolutionPublicationDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ManagementEvolutionPublishedTarget {
    Memory {
        memory_id: MemoryId,
        memory_version_id: MemoryVersionId,
        promotion_id: EvolutionPromotionId,
    },
    Instinct {
        instinct_id: InstinctId,
        promotion_id: EvolutionPromotionId,
    },
    StudioRevisionDraft {
        studio_revision_draft_id: StudioRevisionDraftId,
        promotion_id: EvolutionPromotionId,
        artifact_kind: StudioRevisionArtifactKind,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagementEvolutionPublicationReceipt {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub proposal_id: EvolutionProposalId,
    pub evaluation_id: EvolutionEvaluationId,
    pub approval_interaction_id: InteractionId,
    pub idempotency_key: IdempotencyKey,
    pub approved_at: UtcTimestamp,
    pub target: ManagementEvolutionPublishedTarget,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagementEvolutionPublicationMutation {
    pub disposition: ManagementEvolutionPublicationDisposition,
    pub receipt: ManagementEvolutionPublicationReceipt,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ManagementEvolutionRejectionDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagementEvolutionRejectionReceipt {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub proposal_id: EvolutionProposalId,
    pub evaluation_id: EvolutionEvaluationId,
    pub reason_code: DottedKey,
    pub idempotency_key: IdempotencyKey,
    pub rejected_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagementEvolutionRejectionMutation {
    pub disposition: ManagementEvolutionRejectionDisposition,
    pub receipt: ManagementEvolutionRejectionReceipt,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GetManagementEvolutionRejection {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PersistManagementEvolutionRejection {
    pub receipt: ManagementEvolutionRejectionReceipt,
}

pub trait ManagementEvolutionRejectionStore: Send + Sync {
    type Error: Send;

    fn get_management_rejection(
        &self,
        query: GetManagementEvolutionRejection,
    ) -> ApplicationFuture<'_, Result<Option<ManagementEvolutionRejectionReceipt>, Self::Error>>;

    fn reject_management_evolution(
        &self,
        command: PersistManagementEvolutionRejection,
    ) -> ApplicationFuture<'_, Result<ManagementEvolutionRejectionMutation, Self::Error>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RejectEvolutionProposal {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub proposal_id: EvolutionProposalId,
    pub reason_code: DottedKey,
    pub idempotency_key: IdempotencyKey,
    pub rejected_at: UtcTimestamp,
}

pub struct ManagementEvolutionRejectionService<S>
where
    S: EvolutionStore + ManagementEvolutionRejectionStore,
{
    store: Arc<S>,
}

impl<S> ManagementEvolutionRejectionService<S>
where
    S: EvolutionStore + ManagementEvolutionRejectionStore,
{
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    pub async fn reject(
        &self,
        request: RejectEvolutionProposal,
    ) -> Result<ManagementEvolutionRejectionMutation, ManagementEvolutionRejectionError> {
        if let Some(receipt) = self
            .store
            .get_management_rejection(GetManagementEvolutionRejection {
                tenant_id: request.tenant_id,
                actor_principal_id: request.actor_principal_id,
                idempotency_key: request.idempotency_key.clone(),
            })
            .await
            .map_err(|_| ManagementEvolutionRejectionError::Store)?
        {
            if receipt.agent_id != request.agent_id
                || receipt.proposal_id != request.proposal_id
                || receipt.reason_code != request.reason_code
            {
                return Err(ManagementEvolutionRejectionError::IdempotencyConflict);
            }
            return Ok(ManagementEvolutionRejectionMutation {
                disposition: ManagementEvolutionRejectionDisposition::Duplicate,
                receipt,
            });
        }
        let review = self
            .store
            .load_verified_review(request.proposal_id)
            .await
            .map_err(|_| ManagementEvolutionRejectionError::Store)?
            .ok_or(ManagementEvolutionRejectionError::NotRejectable)?;
        if review.candidate.tenant_id != request.tenant_id
            || !matches!(
                review.candidate.target_scope,
                EvolutionScope::Agent { agent_id } if agent_id == request.agent_id
            )
            || request.rejected_at < review.evaluation.evaluated_at
        {
            return Err(ManagementEvolutionRejectionError::Unauthorized);
        }
        self.store
            .reject_management_evolution(PersistManagementEvolutionRejection {
                receipt: ManagementEvolutionRejectionReceipt {
                    tenant_id: request.tenant_id,
                    actor_principal_id: request.actor_principal_id,
                    agent_id: request.agent_id,
                    proposal_id: request.proposal_id,
                    evaluation_id: review.evaluation.evaluation_id,
                    reason_code: request.reason_code,
                    idempotency_key: request.idempotency_key,
                    rejected_at: request.rejected_at,
                },
            })
            .await
            .map_err(|_| ManagementEvolutionRejectionError::Store)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GetManagementEvolutionPublication {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub idempotency_key: IdempotencyKey,
}

pub trait ManagementEvolutionPublicationStore: Send + Sync {
    type Error: Send;

    fn get_management_publication(
        &self,
        query: GetManagementEvolutionPublication,
    ) -> ApplicationFuture<'_, Result<Option<ManagementEvolutionPublicationReceipt>, Self::Error>>;

    fn publish_management_evolution(
        &self,
        command: PublishManagementEvolution,
    ) -> ApplicationFuture<'_, Result<ManagementEvolutionPublicationMutation, Self::Error>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApproveEvolutionProposal {
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub proposal_id: EvolutionProposalId,
    pub idempotency_key: IdempotencyKey,
    pub approved_at: UtcTimestamp,
}

pub struct ManagementEvolutionPublicationService<S>
where
    S: EvolutionStore + MemoryStore + ManagementEvolutionPublicationStore,
{
    store: Arc<S>,
    content: Arc<dyn ContentStore>,
}

impl<S> ManagementEvolutionPublicationService<S>
where
    S: EvolutionStore + MemoryStore + ManagementEvolutionPublicationStore,
{
    #[must_use]
    pub fn new(store: Arc<S>, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn approve(
        &self,
        request: ApproveEvolutionProposal,
    ) -> Result<ManagementEvolutionPublicationMutation, ManagementEvolutionPublicationError> {
        if let Some(receipt) = self
            .store
            .get_management_publication(GetManagementEvolutionPublication {
                tenant_id: request.tenant_id,
                actor_principal_id: request.actor_principal_id,
                idempotency_key: request.idempotency_key.clone(),
            })
            .await
            .map_err(|_| ManagementEvolutionPublicationError::Store)?
        {
            if receipt.agent_id != request.agent_id
                || receipt.proposal_id != request.proposal_id
                || receipt.tenant_id != request.tenant_id
            {
                return Err(ManagementEvolutionPublicationError::IdempotencyConflict);
            }
            return Ok(ManagementEvolutionPublicationMutation {
                disposition: ManagementEvolutionPublicationDisposition::Duplicate,
                receipt,
            });
        }
        let review = self
            .store
            .load_verified_review(request.proposal_id)
            .await
            .map_err(|_| ManagementEvolutionPublicationError::Store)?
            .ok_or(ManagementEvolutionPublicationError::NotPublishable)?;
        if review.candidate.tenant_id != request.tenant_id
            || !matches!(
                review.candidate.target_scope,
                EvolutionScope::Agent { agent_id } if agent_id == request.agent_id
            )
            || request.approved_at < review.evaluation.evaluated_at
        {
            return Err(ManagementEvolutionPublicationError::Unauthorized);
        }
        let current_head = if review.candidate.kind.is_memory() {
            match review.candidate.target_memory_id {
                Some(memory_id) => Some(
                    self.store
                        .load_active_head(request.tenant_id, memory_id)
                        .await
                        .map_err(|_| ManagementEvolutionPublicationError::Store)?
                        .ok_or(ManagementEvolutionPublicationError::StaleHead)?,
                ),
                None => None,
            }
        } else {
            None
        };
        let expected_current_memory_version_id = current_head
            .as_ref()
            .map(|head| head.version.memory_version_id);
        let prompt_bytes = evolution_approval_prompt_bytes(
            &review.candidate,
            &review.evaluation,
            expected_current_memory_version_id,
        )
        .map_err(|_| ManagementEvolutionPublicationError::InvalidApproval)?;
        let prompt_hash = Sha256::digest(&prompt_bytes);
        let prompt_ref = self
            .content
            .put_at(
                content_id(MANAGEMENT_PROMPT_CONTENT_NAMESPACE, prompt_hash)?,
                PutContent {
                    tenant_id: request.tenant_id,
                    media_type: MediaType::new(
                        musteragent_evolution::EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE,
                    )
                    .map_err(|_| ManagementEvolutionPublicationError::InvalidApproval)?,
                    bytes: prompt_bytes,
                },
            )
            .await
            .map_err(|_| ManagementEvolutionPublicationError::ContentStore)?;
        let seed = management_seed(&request);
        let interaction_id = typed_id(MANAGEMENT_ID_NAMESPACE, &seed)?;
        let approval = EvolutionApproval {
            approval_interaction_id: interaction_id,
            proposal_id: request.proposal_id,
            evaluation_id: review.evaluation.evaluation_id,
            expected_current_memory_version_id,
            prompt_ref,
            approved_by: request.actor_principal_id,
            approved_at: request.approved_at,
        };
        approval
            .validate_for(&review.candidate, &review.evaluation)
            .map_err(|_| ManagementEvolutionPublicationError::InvalidApproval)?;
        let promotion_id = typed_id(MANAGEMENT_PROMOTION_NAMESPACE, &seed)?;
        let target = match review.candidate.kind {
            kind if kind.is_memory() => {
                let memory_id = review.candidate.target_memory_id.unwrap_or(typed_id(
                    MANAGEMENT_MEMORY_NAMESPACE,
                    review.candidate.proposal_id.to_string().as_bytes(),
                )?);
                ManagementEvolutionTarget::Memory(Box::new(
                    prepare_evolution_publication(
                        self.content.as_ref(),
                        PrepareEvolutionPublication {
                            candidate: review.candidate,
                            evaluation: review.evaluation,
                            approval,
                            current_head,
                            new_memory_id: memory_id,
                            memory_version_id: typed_id(
                                MANAGEMENT_MEMORY_VERSION_NAMESPACE,
                                &seed,
                            )?,
                            promotion_id,
                            promoted_by: request.actor_principal_id,
                            promoted_at: request.approved_at,
                        },
                    )
                    .await
                    .map_err(|_| ManagementEvolutionPublicationError::InvalidCandidate)?,
                ))
            }
            ExperienceKind::Instinct => ManagementEvolutionTarget::Instinct(Box::new(
                prepare_instinct_publication(
                    self.content.as_ref(),
                    PrepareInstinctPublication {
                        candidate: review.candidate,
                        evaluation: review.evaluation,
                        approval,
                        instinct_id: typed_id(MANAGEMENT_INSTINCT_NAMESPACE, &seed)?,
                        promotion_id,
                        promoted_by: request.actor_principal_id,
                        promoted_at: request.approved_at,
                    },
                )
                .await
                .map_err(|_| ManagementEvolutionPublicationError::InvalidCandidate)?,
            )),
            kind if kind.is_studio_revision() => {
                let bytes = self
                    .content
                    .read(request.tenant_id, &review.candidate.claim_ref)
                    .await
                    .map_err(|_| ManagementEvolutionPublicationError::ContentStore)?;
                verify_content(&review.candidate.claim_ref, &bytes)
                    .map_err(|_| ManagementEvolutionPublicationError::ContentIntegrity)?;
                let mut candidate = review.candidate;
                candidate
                    .promote_artifact(&review.evaluation, &approval, request.approved_at)
                    .map_err(|_| ManagementEvolutionPublicationError::InvalidCandidate)?;
                let publication = PublishStudioRevisionDraft {
                    draft: StudioRevisionDraft {
                        studio_revision_draft_id: typed_id(STUDIO_REVISION_DRAFT_NAMESPACE, &seed)?,
                        tenant_id: request.tenant_id,
                        target_agent_id: request.agent_id,
                        proposal_id: request.proposal_id,
                        evaluation_id: review.evaluation.evaluation_id,
                        approval_interaction_id: interaction_id,
                        promotion_id,
                        artifact_kind: StudioRevisionArtifactKind::from_experience_kind(kind)?,
                        payload_ref: candidate.claim_ref.clone(),
                        created_by: request.actor_principal_id,
                        created_at: request.approved_at,
                    },
                    candidate,
                    evaluation: review.evaluation,
                    approval,
                };
                publication.validate()?;
                ManagementEvolutionTarget::StudioRevisionDraft(Box::new(publication))
            }
            ExperienceKind::PolicyProposal => {
                return Err(ManagementEvolutionPublicationError::NotPublishable);
            }
            ExperienceKind::Semantic
            | ExperienceKind::Procedure
            | ExperienceKind::Failure
            | ExperienceKind::Preference
            | ExperienceKind::Skill
            | ExperienceKind::PersonaPatch => unreachable!("guarded above"),
        };
        self.store
            .publish_management_evolution(PublishManagementEvolution {
                tenant_id: request.tenant_id,
                actor_principal_id: request.actor_principal_id,
                agent_id: request.agent_id,
                proposal_id: request.proposal_id,
                idempotency_key: request.idempotency_key,
                approved_at: request.approved_at,
                target,
            })
            .await
            .map_err(|_| ManagementEvolutionPublicationError::Store)
    }
}

fn management_seed(request: &ApproveEvolutionProposal) -> Vec<u8> {
    format!(
        "{}:{}:{}:{}",
        request.tenant_id,
        request.actor_principal_id,
        request.proposal_id,
        request.idempotency_key.as_str()
    )
    .into_bytes()
}

fn content_id(
    namespace: Uuid,
    hash: Sha256,
) -> Result<ContentId, ManagementEvolutionPublicationError> {
    typed_id(namespace, &hash.into_bytes())
}

fn typed_id<T>(namespace: Uuid, seed: &[u8]) -> Result<T, ManagementEvolutionPublicationError>
where
    T: DerivedIdentity,
{
    T::from_derived_uuid(Uuid::new_v5(&namespace, seed))
}

trait DerivedIdentity: Sized {
    fn from_derived_uuid(value: Uuid) -> Result<Self, ManagementEvolutionPublicationError>;
}

macro_rules! derived_identity {
    ($($identity:ty),+ $(,)?) => {
        $(
            impl DerivedIdentity for $identity {
                fn from_derived_uuid(
                    value: Uuid,
                ) -> Result<Self, ManagementEvolutionPublicationError> {
                    Self::from_uuid(value)
                        .map_err(|_| ManagementEvolutionPublicationError::Identity)
                }
            }
        )+
    };
}

derived_identity!(
    ContentId,
    EvolutionPromotionId,
    InteractionId,
    MemoryId,
    MemoryVersionId,
    InstinctId,
    StudioRevisionDraftId,
);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum ManagementEvolutionPublicationError {
    #[error("Evolution proposal is not publishable")]
    NotPublishable,
    #[error("Evolution proposal is not authorized for this Agent")]
    Unauthorized,
    #[error("Evolution proposal or target kind is invalid")]
    InvalidCandidate,
    #[error("Evolution Approval binding is invalid")]
    InvalidApproval,
    #[error("Evolution Memory head changed before publication")]
    StaleHead,
    #[error("Evolution publication idempotency key was reused")]
    IdempotencyConflict,
    #[error("Evolution publication Content is unavailable")]
    ContentStore,
    #[error("Evolution publication Content failed integrity verification")]
    ContentIntegrity,
    #[error("Evolution publication identity could not be derived")]
    Identity,
    #[error("Evolution publication store is unavailable")]
    Store,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum ManagementEvolutionRejectionError {
    #[error("Evolution proposal is not rejectable")]
    NotRejectable,
    #[error("Evolution proposal is not authorized for this Agent")]
    Unauthorized,
    #[error("Evolution rejection idempotency key was reused")]
    IdempotencyConflict,
    #[error("Evolution rejection store is unavailable")]
    Store,
}

impl From<MemoryPersistenceDisposition> for ManagementEvolutionPublicationDisposition {
    fn from(value: MemoryPersistenceDisposition) -> Self {
        match value {
            MemoryPersistenceDisposition::Applied => Self::Applied,
            MemoryPersistenceDisposition::Duplicate => Self::Duplicate,
        }
    }
}
