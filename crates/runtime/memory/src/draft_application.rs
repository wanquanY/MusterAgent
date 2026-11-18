use std::sync::Arc;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_evolution::{
    CandidateStatus, ConfidenceBps, EvolutionApproval, EvolutionScope, ExperienceCandidate,
    ExperienceCandidateOrigin, ExperienceKind, evolution_approval_prompt_bytes,
};
use musteragent_identity::{
    AgentId, ContentId, EvolutionPromotionId, EvolutionProposalId, IdempotencyKey, InteractionId,
    MemoryDraftBatchId, MemoryDraftId, MemoryId, MemoryVersionId, PrincipalId, TenantId,
    UtcTimestamp,
};
use musteragent_memory::{MemoryKind, MemoryLifecycle, MemoryScope};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use thiserror::Error;
use uuid::Uuid;

mod inspection;
pub use inspection::{AgentMemoryDraftContent, GetAgentMemoryDraftContent};

use crate::{
    AgentMemoryDraft, AgentMemoryDraftBatchMutation, AgentMemoryDraftMutation,
    AgentMemoryDraftPublicationBatchMutation, AgentMemoryDraftReviewBatch, AgentMemoryDraftStatus,
    AgentMemoryDraftStore, ConfirmAgentMemoryDraftBatch, ExperienceClaimDocument,
    GetAgentMemoryDraftConfirmation, GetAgentMemoryDraftSubmission, PrepareEvolutionPublication,
    SaveAgentMemoryDraft, SubmitAgentMemoryDraft, SubmitAgentMemoryDraftBatch,
    prepare_evolution_publication,
};

const DRAFT_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe3, 0x01,
]);
const DRAFT_PROPOSAL_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe3, 0x02,
]);
const DRAFT_BATCH_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe3, 0x03,
]);
const DRAFT_APPROVAL_INTERACTION_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe3, 0x04,
]);
const DRAFT_APPROVAL_PROMPT_CONTENT_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe3, 0x07,
]);
const DRAFT_PUBLICATION_VERSION_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe3, 0x05,
]);
const DRAFT_PROMOTION_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe3, 0x06,
]);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryCheckout {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub memory_id: MemoryId,
    pub baseline_memory_version_id: MemoryVersionId,
    pub memory_key: IdempotencyKey,
    pub kind: MemoryKind,
    pub published_content: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SaveAgentMemoryDraftContent {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub memory_id: MemoryId,
    pub expected_baseline_memory_version_id: MemoryVersionId,
    pub edited_claim: String,
    pub supersedes_draft_id: Option<MemoryDraftId>,
    pub idempotency_key: IdempotencyKey,
    pub saved_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubmitAgentMemoryDrafts {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub draft_ids: Vec<MemoryDraftId>,
    pub idempotency_key: IdempotencyKey,
    pub submitted_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfirmAgentMemoryDrafts {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub batch_id: MemoryDraftBatchId,
    pub idempotency_key: IdempotencyKey,
    pub confirmed_at: UtcTimestamp,
}

/// Shared Cloud/Device owner for Draft checkout and proposal submission. It
/// writes only immutable Draft Content and Candidate rows; Evaluation,
/// Approval and CAS Publication remain downstream mandatory boundaries.
pub struct AgentMemoryDraftApplicationService<S>
where
    S: AgentMemoryDraftStore + crate::MemoryStore,
{
    store: Arc<S>,
    content: Arc<dyn ContentStore>,
}

impl<S> AgentMemoryDraftApplicationService<S>
where
    S: AgentMemoryDraftStore + crate::MemoryStore,
{
    #[must_use]
    pub fn new(store: Arc<S>, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn checkout(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        agent_id: AgentId,
        memory_id: MemoryId,
    ) -> Result<AgentMemoryCheckout, AgentMemoryDraftApplicationError> {
        self.checkout_bounded(
            tenant_id,
            principal_id,
            agent_id,
            memory_id,
            musteragent_runtime_content::MAX_BUFFERED_CONTENT_BYTES,
        )
        .await
    }

    /// Explicit current-head read for rebase. The caller's content budget is
    /// checked before IO; this does not save, merge or supersede a Draft.
    pub async fn checkout_bounded(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        agent_id: AgentId,
        memory_id: MemoryId,
        max_content_bytes: u64,
    ) -> Result<AgentMemoryCheckout, AgentMemoryDraftApplicationError> {
        self.checkout_inner(
            tenant_id,
            principal_id,
            agent_id,
            memory_id,
            None,
            max_content_bytes,
        )
        .await
    }

    /// Checkout the exact published version the user selected. The bound is
    /// checked against the immutable ContentRef before reading its bytes.
    pub async fn checkout_at_version(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        agent_id: AgentId,
        memory_id: MemoryId,
        expected_version: MemoryVersionId,
        max_content_bytes: u64,
    ) -> Result<AgentMemoryCheckout, AgentMemoryDraftApplicationError> {
        self.checkout_inner(
            tenant_id,
            principal_id,
            agent_id,
            memory_id,
            Some(expected_version),
            max_content_bytes,
        )
        .await
    }

    async fn checkout_inner(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        agent_id: AgentId,
        memory_id: MemoryId,
        expected_version: Option<MemoryVersionId>,
        max_content_bytes: u64,
    ) -> Result<AgentMemoryCheckout, AgentMemoryDraftApplicationError> {
        let head = self
            .load_checkout_head(tenant_id, principal_id, agent_id, memory_id)
            .await?;
        if expected_version.is_some_and(|expected| expected != head.version.memory_version_id) {
            return Err(AgentMemoryDraftApplicationError::Conflict);
        }
        if max_content_bytes == 0 || head.version.content_ref.byte_length > max_content_bytes {
            return Err(AgentMemoryDraftApplicationError::InvalidContent);
        }
        let published_content = self
            .content
            .read(tenant_id, &head.version.content_ref)
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::ContentStore)?;
        verify_content(&head.version.content_ref, &published_content)
            .map_err(|_| AgentMemoryDraftApplicationError::ContentIntegrity)?;
        Ok(AgentMemoryCheckout {
            tenant_id,
            principal_id,
            agent_id,
            memory_id,
            baseline_memory_version_id: head.version.memory_version_id,
            memory_key: head.record.memory_key,
            kind: head.record.kind,
            published_content,
        })
    }

    pub async fn save(
        &self,
        command: SaveAgentMemoryDraftContent,
    ) -> Result<AgentMemoryDraftMutation, AgentMemoryDraftApplicationError> {
        if command.edited_claim.trim().is_empty() || command.edited_claim.len() > 256 * 1_024 {
            return Err(AgentMemoryDraftApplicationError::InvalidContent);
        }
        let draft_id = deterministic_draft_id(
            command.tenant_id,
            command.principal_id,
            &command.idempotency_key,
        )?;
        if let Some(existing) = self.saved_receipt(&command, draft_id).await? {
            return Ok(existing);
        }
        match self.save_new(&command, draft_id).await {
            Ok(mutation) => Ok(mutation),
            // Another writer may have committed after the first receipt read.
            // Only its exact authenticated durable intent can resolve ambiguity.
            Err(error) => self.saved_receipt(&command, draft_id).await?.ok_or(error),
        }
    }

    async fn saved_receipt(
        &self,
        command: &SaveAgentMemoryDraftContent,
        draft_id: MemoryDraftId,
    ) -> Result<Option<AgentMemoryDraftMutation>, AgentMemoryDraftApplicationError> {
        let Some(draft) = self
            .store
            .load_draft(command.tenant_id, command.principal_id, draft_id)
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)?
        else {
            return Ok(None);
        };
        if draft.draft_id != draft_id
            || draft.tenant_id != command.tenant_id
            || draft.owner_principal_id != command.principal_id
            || draft.agent_id != command.agent_id
            || draft.memory_id != command.memory_id
            || draft.baseline_memory_version_id != command.expected_baseline_memory_version_id
            || draft.supersedes_draft_id != command.supersedes_draft_id
            || draft.idempotency_key != command.idempotency_key
        {
            return Err(AgentMemoryDraftApplicationError::Conflict);
        }
        let (bytes, saved) = self.read_saved_claim(&draft).await?;
        let expected = manual_claim(command.edited_claim.clone(), saved.sensitivity)?;
        if expected != bytes {
            return Err(AgentMemoryDraftApplicationError::Conflict);
        }
        Ok(Some(AgentMemoryDraftMutation {
            disposition: crate::AgentMemoryDraftDisposition::Duplicate,
            draft,
        }))
    }

    async fn save_new(
        &self,
        command: &SaveAgentMemoryDraftContent,
        draft_id: MemoryDraftId,
    ) -> Result<AgentMemoryDraftMutation, AgentMemoryDraftApplicationError> {
        let head = self
            .load_checkout_head(
                command.tenant_id,
                command.principal_id,
                command.agent_id,
                command.memory_id,
            )
            .await?;
        if head.version.memory_version_id != command.expected_baseline_memory_version_id {
            return Err(AgentMemoryDraftApplicationError::Conflict);
        }
        let claim_bytes = manual_claim(
            command.edited_claim.clone(),
            classification_label(head.version.data_classification).to_owned(),
        )?;
        let content_ref = self
            .content
            .put(PutContent {
                tenant_id: command.tenant_id,
                media_type: MediaType::new(crate::EXPERIENCE_CLAIM_MEDIA_TYPE)
                    .map_err(|_| AgentMemoryDraftApplicationError::InvalidContent)?,
                bytes: claim_bytes,
            })
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::ContentStore)?;
        let draft = AgentMemoryDraft {
            draft_id,
            tenant_id: command.tenant_id,
            owner_principal_id: command.principal_id,
            agent_id: command.agent_id,
            memory_id: command.memory_id,
            baseline_memory_version_id: command.expected_baseline_memory_version_id,
            content_ref,
            status: AgentMemoryDraftStatus::Draft,
            proposal_id: None,
            conflicting_memory_version_id: None,
            supersedes_draft_id: command.supersedes_draft_id,
            idempotency_key: command.idempotency_key.clone(),
            created_at: command.saved_at,
            updated_at: command.saved_at,
        };
        self.store
            .save_draft(SaveAgentMemoryDraft { draft })
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)
    }

    pub async fn submit(
        &self,
        command: SubmitAgentMemoryDrafts,
    ) -> Result<AgentMemoryDraftBatchMutation, AgentMemoryDraftApplicationError> {
        if command.draft_ids.is_empty()
            || command.draft_ids.len() > 100
            || command
                .draft_ids
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != command.draft_ids.len()
        {
            return Err(AgentMemoryDraftApplicationError::InvalidBatch);
        }
        let batch_id = deterministic_batch_id(
            command.tenant_id,
            command.principal_id,
            &command.idempotency_key,
        )?;
        let query = GetAgentMemoryDraftSubmission {
            batch_id,
            tenant_id: command.tenant_id,
            owner_principal_id: command.principal_id,
            agent_id: command.agent_id,
            draft_ids: command.draft_ids.clone(),
            idempotency_key: command.idempotency_key.clone(),
        };
        if let Some(existing) = self
            .store
            .load_submission(query.clone())
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)?
        {
            return Ok(existing);
        }
        match self.submit_new(command, batch_id).await {
            Ok(mutation) => Ok(mutation),
            Err(error) => self
                .store
                .load_submission(query)
                .await
                .map_err(|_| AgentMemoryDraftApplicationError::Store)?
                .ok_or(error),
        }
    }

    async fn submit_new(
        &self,
        command: SubmitAgentMemoryDrafts,
        batch_id: MemoryDraftBatchId,
    ) -> Result<AgentMemoryDraftBatchMutation, AgentMemoryDraftApplicationError> {
        let mut drafts = Vec::with_capacity(command.draft_ids.len());
        for draft_id in command.draft_ids {
            let draft = self
                .store
                .load_draft(command.tenant_id, command.principal_id, draft_id)
                .await
                .map_err(|_| AgentMemoryDraftApplicationError::Store)?
                .ok_or(AgentMemoryDraftApplicationError::NotFound)?;
            if draft.agent_id != command.agent_id || draft.status != AgentMemoryDraftStatus::Draft {
                return Err(AgentMemoryDraftApplicationError::Conflict);
            }
            let head = self
                .load_checkout_head(
                    command.tenant_id,
                    command.principal_id,
                    command.agent_id,
                    draft.memory_id,
                )
                .await?;
            let candidate = ExperienceCandidate {
                proposal_id: deterministic_proposal_id(draft.draft_id)?,
                tenant_id: command.tenant_id,
                proposed_by: command.principal_id,
                origin: ExperienceCandidateOrigin::UserDraft,
                source_draft_id: Some(draft.draft_id),
                target_scope: evolution_scope(head.record.scope),
                target_memory_id: Some(draft.memory_id),
                supersedes_proposal_id: None,
                kind: experience_kind(head.record.kind),
                claim_ref: draft.content_ref.clone(),
                source_run_ids: std::collections::BTreeSet::new(),
                evidence_record_ids: std::collections::BTreeSet::new(),
                status: CandidateStatus::Candidate,
                confidence: Some(
                    ConfidenceBps::new(10_000)
                        .map_err(|_| AgentMemoryDraftApplicationError::InvalidContent)?,
                ),
                created_at: command.submitted_at,
            };
            drafts.push(SubmitAgentMemoryDraft {
                draft_id: draft.draft_id,
                tenant_id: command.tenant_id,
                owner_principal_id: command.principal_id,
                agent_id: command.agent_id,
                expected_baseline_memory_version_id: draft.baseline_memory_version_id,
                candidate,
                submitted_at: command.submitted_at,
            });
        }
        self.store
            .submit_batch(SubmitAgentMemoryDraftBatch {
                batch_id,
                tenant_id: command.tenant_id,
                owner_principal_id: command.principal_id,
                agent_id: command.agent_id,
                drafts,
                idempotency_key: command.idempotency_key,
                submitted_at: command.submitted_at,
            })
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)
    }

    pub async fn review(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        batch_id: MemoryDraftBatchId,
    ) -> Result<AgentMemoryDraftReviewBatch, AgentMemoryDraftApplicationError> {
        let mut review = self
            .store
            .load_batch_review(tenant_id, principal_id, batch_id)
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)?
            .ok_or(AgentMemoryDraftApplicationError::NotFound)?;
        if review.tenant_id != tenant_id
            || review.owner_principal_id != principal_id
            || review.batch_id != batch_id
        {
            return Err(AgentMemoryDraftApplicationError::Store);
        }
        review
            .validate()
            .map_err(|_| AgentMemoryDraftApplicationError::Store)?;
        crate::inspection_materialization::materialize_agent_memory_draft_claims(
            &mut review,
            self.content.clone(),
        )
        .await;
        Ok(review)
    }

    pub async fn list_reviews(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        agent_id: AgentId,
        limit: u32,
    ) -> Result<Vec<AgentMemoryDraftReviewBatch>, AgentMemoryDraftApplicationError> {
        if !(1..=50).contains(&limit) {
            return Err(AgentMemoryDraftApplicationError::InvalidBatch);
        }
        let mut reviews = self
            .store
            .list_batch_reviews(tenant_id, principal_id, agent_id, limit)
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)?;
        if reviews.len() > limit as usize {
            return Err(AgentMemoryDraftApplicationError::Store);
        }
        let mut batches = std::collections::BTreeSet::new();
        for review in &mut reviews {
            if review.tenant_id != tenant_id
                || review.owner_principal_id != principal_id
                || review.agent_id != agent_id
                || !batches.insert(review.batch_id)
            {
                return Err(AgentMemoryDraftApplicationError::Store);
            }
            review
                .validate()
                .map_err(|_| AgentMemoryDraftApplicationError::Store)?;
            crate::inspection_materialization::materialize_agent_memory_draft_claims(
                review,
                self.content.clone(),
            )
            .await;
        }
        Ok(reviews)
    }

    pub async fn confirm(
        &self,
        command: ConfirmAgentMemoryDrafts,
    ) -> Result<AgentMemoryDraftPublicationBatchMutation, AgentMemoryDraftApplicationError> {
        let query = GetAgentMemoryDraftConfirmation {
            batch_id: command.batch_id,
            tenant_id: command.tenant_id,
            owner_principal_id: command.principal_id,
            agent_id: command.agent_id,
            idempotency_key: command.idempotency_key.clone(),
        };
        if let Some(existing) = self
            .store
            .load_confirmation(query.clone())
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)?
        {
            return Ok(existing);
        }
        match self.confirm_new(command).await {
            Ok(mutation) => Ok(mutation),
            Err(error) => self
                .store
                .load_confirmation(query)
                .await
                .map_err(|_| AgentMemoryDraftApplicationError::Store)?
                .ok_or(error),
        }
    }

    async fn confirm_new(
        &self,
        command: ConfirmAgentMemoryDrafts,
    ) -> Result<AgentMemoryDraftPublicationBatchMutation, AgentMemoryDraftApplicationError> {
        let review = self
            .review(command.tenant_id, command.principal_id, command.batch_id)
            .await?;
        if review.agent_id != command.agent_id
            || review.items.is_empty()
            || review.items.len() > 100
        {
            return Err(AgentMemoryDraftApplicationError::InvalidBatch);
        }
        let mut publications = Vec::with_capacity(review.items.len());
        for item in review.items {
            let evaluation = item
                .proposal
                .evaluation
                .ok_or(AgentMemoryDraftApplicationError::EvaluationPending)?;
            if item.draft.status != AgentMemoryDraftStatus::Submitted
                || item.proposal.candidate.status != CandidateStatus::Verified
                || !evaluation.passed
                || item.draft.proposal_id != Some(item.proposal.candidate.proposal_id)
                || item.proposal.candidate.target_memory_id != Some(item.draft.memory_id)
            {
                return Err(AgentMemoryDraftApplicationError::NotPublishable);
            }
            let head = self
                .load_checkout_head(
                    command.tenant_id,
                    command.principal_id,
                    command.agent_id,
                    item.draft.memory_id,
                )
                .await?;
            if head.version.memory_version_id != item.draft.baseline_memory_version_id {
                return Err(AgentMemoryDraftApplicationError::Conflict);
            }
            let prompt_bytes = evolution_approval_prompt_bytes(
                &item.proposal.candidate,
                &evaluation,
                Some(head.version.memory_version_id),
            )
            .map_err(|_| AgentMemoryDraftApplicationError::InvalidContent)?;
            let prompt_content_id = ContentId::from_uuid(Uuid::new_v5(
                &DRAFT_APPROVAL_PROMPT_CONTENT_ID_NAMESPACE,
                &Sha256::digest(&prompt_bytes).into_bytes(),
            ))
            .map_err(|_| AgentMemoryDraftApplicationError::InvalidIdentity)?;
            let prompt_ref = self
                .content
                .put_at(
                    prompt_content_id,
                    PutContent {
                        tenant_id: command.tenant_id,
                        media_type: MediaType::new(
                            musteragent_evolution::EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE,
                        )
                        .map_err(|_| AgentMemoryDraftApplicationError::InvalidContent)?,
                        bytes: prompt_bytes,
                    },
                )
                .await
                .map_err(|_| AgentMemoryDraftApplicationError::ContentStore)?;
            let approval = EvolutionApproval {
                approval_interaction_id: deterministic_interaction_id(
                    command.batch_id,
                    item.proposal.candidate.proposal_id,
                )?,
                proposal_id: item.proposal.candidate.proposal_id,
                evaluation_id: evaluation.evaluation_id,
                expected_current_memory_version_id: Some(head.version.memory_version_id),
                prompt_ref,
                approved_by: command.principal_id,
                approved_at: command.confirmed_at,
            };
            let publication = prepare_evolution_publication(
                self.content.as_ref(),
                PrepareEvolutionPublication {
                    candidate: item.proposal.candidate,
                    evaluation,
                    approval,
                    current_head: Some(head),
                    new_memory_id: item.draft.memory_id,
                    memory_version_id: deterministic_version_id(
                        command.batch_id,
                        item.draft.draft_id,
                    )?,
                    promotion_id: deterministic_promotion_id(
                        command.batch_id,
                        item.draft.draft_id,
                    )?,
                    promoted_by: command.principal_id,
                    promoted_at: command.confirmed_at,
                },
            )
            .await
            .map_err(|error| match error {
                crate::EvolutionPublicationPreparationError::StaleApproval
                | crate::EvolutionPublicationPreparationError::StaleHead => {
                    AgentMemoryDraftApplicationError::Conflict
                }
                crate::EvolutionPublicationPreparationError::ContentStore => {
                    AgentMemoryDraftApplicationError::ContentStore
                }
                crate::EvolutionPublicationPreparationError::ContentIntegrity => {
                    AgentMemoryDraftApplicationError::ContentIntegrity
                }
                crate::EvolutionPublicationPreparationError::InvalidCandidate
                | crate::EvolutionPublicationPreparationError::VersionOverflow => {
                    AgentMemoryDraftApplicationError::NotPublishable
                }
            })?;
            publications.push(publication);
        }
        self.store
            .confirm_batch(ConfirmAgentMemoryDraftBatch {
                batch_id: command.batch_id,
                tenant_id: command.tenant_id,
                owner_principal_id: command.principal_id,
                agent_id: command.agent_id,
                publications,
                idempotency_key: command.idempotency_key,
                confirmed_at: command.confirmed_at,
            })
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)
    }

    async fn load_checkout_head(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        agent_id: AgentId,
        memory_id: MemoryId,
    ) -> Result<crate::MemorySearchHit, AgentMemoryDraftApplicationError> {
        let head = self
            .store
            .load_active_head(tenant_id, memory_id)
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)?
            .ok_or(AgentMemoryDraftApplicationError::NotFound)?;
        if head.record.lifecycle != MemoryLifecycle::Active
            || head.record.current_version_id != head.version.memory_version_id
            || !matches!(
                head.record.scope,
                MemoryScope::Agent { agent_id: actual } if actual == agent_id
            ) && !matches!(
                head.record.scope,
                MemoryScope::User { principal_id: actual } if actual == principal_id
            )
        {
            return Err(AgentMemoryDraftApplicationError::Unauthorized);
        }
        Ok(head)
    }
}

fn manual_claim(
    claim: String,
    sensitivity: String,
) -> Result<Vec<u8>, AgentMemoryDraftApplicationError> {
    ExperienceClaimDocument::manual(
        claim,
        serde_json::Map::new(),
        "Explicit user edit submitted from the Learning Memory checkout".to_owned(),
        sensitivity,
    )
    .encode()
    .map_err(|_| AgentMemoryDraftApplicationError::InvalidContent)
}

fn deterministic_draft_id(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    key: &IdempotencyKey,
) -> Result<MemoryDraftId, AgentMemoryDraftApplicationError> {
    let mut bytes = tenant_id.to_string().into_bytes();
    bytes.extend_from_slice(principal_id.to_string().as_bytes());
    bytes.extend_from_slice(key.as_str().as_bytes());
    MemoryDraftId::from_uuid(Uuid::new_v5(&DRAFT_ID_NAMESPACE, &bytes))
        .map_err(|_| AgentMemoryDraftApplicationError::InvalidIdentity)
}

fn deterministic_proposal_id(
    draft_id: MemoryDraftId,
) -> Result<EvolutionProposalId, AgentMemoryDraftApplicationError> {
    EvolutionProposalId::from_uuid(Uuid::new_v5(
        &DRAFT_PROPOSAL_ID_NAMESPACE,
        draft_id.into_uuid().as_bytes(),
    ))
    .map_err(|_| AgentMemoryDraftApplicationError::InvalidIdentity)
}

fn deterministic_batch_id(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    key: &IdempotencyKey,
) -> Result<MemoryDraftBatchId, AgentMemoryDraftApplicationError> {
    let mut bytes = tenant_id.to_string().into_bytes();
    bytes.extend_from_slice(principal_id.to_string().as_bytes());
    bytes.extend_from_slice(key.as_str().as_bytes());
    MemoryDraftBatchId::from_uuid(Uuid::new_v5(&DRAFT_BATCH_ID_NAMESPACE, &bytes))
        .map_err(|_| AgentMemoryDraftApplicationError::InvalidIdentity)
}

fn deterministic_interaction_id(
    batch_id: MemoryDraftBatchId,
    proposal_id: EvolutionProposalId,
) -> Result<InteractionId, AgentMemoryDraftApplicationError> {
    deterministic_pair_id::<InteractionId, _>(
        DRAFT_APPROVAL_INTERACTION_ID_NAMESPACE,
        batch_id,
        proposal_id,
    )
}

fn deterministic_version_id(
    batch_id: MemoryDraftBatchId,
    draft_id: MemoryDraftId,
) -> Result<MemoryVersionId, AgentMemoryDraftApplicationError> {
    deterministic_pair_id::<MemoryVersionId, _>(
        DRAFT_PUBLICATION_VERSION_ID_NAMESPACE,
        batch_id,
        draft_id,
    )
}

fn deterministic_promotion_id(
    batch_id: MemoryDraftBatchId,
    draft_id: MemoryDraftId,
) -> Result<EvolutionPromotionId, AgentMemoryDraftApplicationError> {
    deterministic_pair_id::<EvolutionPromotionId, _>(
        DRAFT_PROMOTION_ID_NAMESPACE,
        batch_id,
        draft_id,
    )
}

fn deterministic_pair_id<T, U>(
    namespace: Uuid,
    batch_id: MemoryDraftBatchId,
    item_id: U,
) -> Result<T, AgentMemoryDraftApplicationError>
where
    T: std::str::FromStr,
    U: std::fmt::Display,
{
    let mut bytes = batch_id.to_string().into_bytes();
    bytes.extend_from_slice(item_id.to_string().as_bytes());
    Uuid::new_v5(&namespace, &bytes)
        .to_string()
        .parse()
        .map_err(|_| AgentMemoryDraftApplicationError::InvalidIdentity)
}

const fn experience_kind(value: MemoryKind) -> ExperienceKind {
    match value {
        MemoryKind::Preference => ExperienceKind::Preference,
        MemoryKind::Semantic => ExperienceKind::Semantic,
        MemoryKind::Procedure => ExperienceKind::Procedure,
        MemoryKind::FailureLesson => ExperienceKind::Failure,
    }
}

const fn evolution_scope(value: MemoryScope) -> EvolutionScope {
    match value {
        MemoryScope::User { principal_id } => EvolutionScope::User { principal_id },
        MemoryScope::Agent { agent_id } => EvolutionScope::Agent { agent_id },
        MemoryScope::Tenant { .. } | MemoryScope::Workspace { .. } => {
            // These scopes are rejected by load_checkout_head before this
            // conversion can be reached.
            unreachable!()
        }
    }
}

const fn classification_label(value: musteragent_policy::DataClassification) -> &'static str {
    match value {
        musteragent_policy::DataClassification::Public
        | musteragent_policy::DataClassification::Internal => "internal",
        musteragent_policy::DataClassification::Confidential
        | musteragent_policy::DataClassification::Restricted => "private",
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum AgentMemoryDraftApplicationError {
    #[error("Memory Draft resource was not found")]
    NotFound,
    #[error("Memory Draft resource is not authorized for this Agent")]
    Unauthorized,
    #[error("Memory Draft content is invalid")]
    InvalidContent,
    #[error("Memory Draft batch is invalid")]
    InvalidBatch,
    #[error("Memory Draft inspection query is invalid")]
    InvalidQuery,
    #[error("Memory Draft Evaluation has not completed")]
    EvaluationPending,
    #[error("Memory Draft failed Evaluation or is no longer publishable")]
    NotPublishable,
    #[error("Memory Draft baseline or state conflicts with the published head")]
    Conflict,
    #[error("Memory Draft identity could not be derived")]
    InvalidIdentity,
    #[error("Memory Draft Content Store is unavailable")]
    ContentStore,
    #[error("Memory Draft ContentRef does not match immutable bytes")]
    ContentIntegrity,
    #[error("Memory Draft persistence is unavailable")]
    Store,
}
