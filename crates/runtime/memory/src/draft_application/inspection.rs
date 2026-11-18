use super::*;
use crate::{AgentMemoryDraftPage, ExperienceClaimOrigin, ListAgentMemoryDrafts};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GetAgentMemoryDraftContent {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub draft_id: MemoryDraftId,
}

/// Original immutable user input and current durable lifecycle. Reading does
/// not rebase, evaluate, submit or publish a Draft, even if its head has moved.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftContent {
    pub draft: AgentMemoryDraft,
    /// Exact bytes verified against the immutable ContentRef. Re-encoding the
    /// document can change whitespace/order and no longer match that digest.
    pub content: Vec<u8>,
    pub document: ExperienceClaimDocument,
}

impl<S> AgentMemoryDraftApplicationService<S>
where
    S: AgentMemoryDraftStore + crate::MemoryStore,
{
    pub async fn list_drafts(
        &self,
        query: ListAgentMemoryDrafts,
    ) -> Result<AgentMemoryDraftPage, AgentMemoryDraftApplicationError> {
        query
            .validate()
            .map_err(|_| AgentMemoryDraftApplicationError::InvalidQuery)?;
        self.store
            .list_drafts(query)
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)
    }

    pub async fn read_draft(
        &self,
        query: GetAgentMemoryDraftContent,
    ) -> Result<AgentMemoryDraftContent, AgentMemoryDraftApplicationError> {
        let draft = self
            .store
            .load_draft(query.tenant_id, query.principal_id, query.draft_id)
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::Store)?
            .ok_or(AgentMemoryDraftApplicationError::NotFound)?;
        if draft.tenant_id != query.tenant_id
            || draft.owner_principal_id != query.principal_id
            || draft.agent_id != query.agent_id
            || draft.draft_id != query.draft_id
        {
            return Err(AgentMemoryDraftApplicationError::NotFound);
        }
        let (content, document) = self.read_saved_claim(&draft).await?;
        Ok(AgentMemoryDraftContent {
            draft,
            content,
            document,
        })
    }

    pub(super) async fn read_saved_claim(
        &self,
        draft: &AgentMemoryDraft,
    ) -> Result<(Vec<u8>, ExperienceClaimDocument), AgentMemoryDraftApplicationError> {
        draft
            .validate()
            .map_err(|_| AgentMemoryDraftApplicationError::ContentIntegrity)?;
        // The original validator bounds manual Claims to 8,000 UTF-8 bytes.
        // 64 KiB permits worst-case JSON escaping without an unbounded read.
        if draft.content_ref.byte_length == 0 || draft.content_ref.byte_length > 64 * 1_024 {
            return Err(AgentMemoryDraftApplicationError::InvalidContent);
        }
        if draft.content_ref.media_type.as_str() != crate::EXPERIENCE_CLAIM_MEDIA_TYPE {
            return Err(AgentMemoryDraftApplicationError::ContentIntegrity);
        }
        let bytes = self
            .content
            .read(draft.tenant_id, &draft.content_ref)
            .await
            .map_err(|_| AgentMemoryDraftApplicationError::ContentStore)?;
        verify_content(&draft.content_ref, &bytes)
            .map_err(|_| AgentMemoryDraftApplicationError::ContentIntegrity)?;
        let document = ExperienceClaimDocument::decode(&bytes)
            .map_err(|_| AgentMemoryDraftApplicationError::ContentIntegrity)?;
        if document.origin != ExperienceClaimOrigin::Manual {
            return Err(AgentMemoryDraftApplicationError::ContentIntegrity);
        }
        // Retain the whole validated document: the original Markdown workspace
        // has different provenance than single-Memory checkout. Neither may be
        // silently rewritten just to restore a client editor.
        Ok((bytes, document))
    }
}
