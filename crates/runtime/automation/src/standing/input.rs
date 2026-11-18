//! Input admission is an internal composition port. Source/Collaboration
//! adapters must first verify their own current grants and activation fences;
//! no user-facing transport or Agent tool accepts this record directly.
use super::*;
use musteragent_content_ref::Sha256;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AcceptStandingGoalInput {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub expected_goal_revision: u64,
    /// Canonical source-change identity, independent of delivery attempt,
    /// worker, Run and provider reconnection generation.
    pub input_key: String,
    pub content: ContentRef,
}

impl AcceptStandingGoalInput {
    pub fn validate(&self) -> Result<(), StandingGoalProductError> {
        if self.expected_goal_revision == 0
            || self.input_key.is_empty()
            || self.input_key.len() > 512
            || self.input_key.contains('\0')
            || self.content.byte_length > 2 * 1024 * 1024
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        Ok(())
    }

    pub fn fingerprint(&self) -> Sha256 {
        let mut bytes = b"standing-goal-input@1\0".to_vec();
        bytes.extend_from_slice(self.content.content_id.into_uuid().as_bytes());
        bytes.extend_from_slice(&self.content.sha256.into_bytes());
        bytes.extend_from_slice(&self.content.byte_length.to_be_bytes());
        bytes.extend_from_slice(self.content.media_type.as_str().as_bytes());
        Sha256::digest(bytes)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalInputOutcome {
    Accepted {
        sequence: u64,
    },
    Existing {
        sequence: u64,
    },
    /// Not active, a changed Goal revision, mismatched repeated material, or
    /// ownership mismatch. The source keeps its durable delivery obligation.
    Rejected,
}

impl StandingGoalProductApplication {
    pub async fn accept_authorized_input(
        &self,
        request: AcceptStandingGoalInput,
    ) -> Result<StandingGoalInputOutcome, StandingGoalProductError> {
        request.validate()?;
        self.store.accept_input(request).await.map_err(Into::into)
    }
}
