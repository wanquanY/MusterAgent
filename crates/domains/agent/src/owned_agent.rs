//! Account-owned persistent Agent instances, independent of execution hosts.

use std::num::NonZeroU64;

use musteragent_content_ref::ContentRef;
use musteragent_identity::{
    AgentDialogueId, AgentId, AgentVersionId, OwnerSpaceId, PrincipalId, TenantId, UtcTimestamp,
};
use musteragent_policy::PermissionMode;
use thiserror::Error;

use crate::{AgentContractError, AgentDefinition, AgentKind, AgentLifecycle, AgentVersion};

#[derive(Debug, Error)]
pub enum OwnedAgentError {
    #[error(transparent)]
    Definition(#[from] AgentContractError),
    #[error("owned instances require an active Custom or qualified Installed Agent")]
    InvalidDefinition,
    #[error("account-owned Agent references a different definition or version")]
    IdentityMismatch,
    #[error("owned profile is invalid")]
    InvalidProfile,
    #[error("account-owned Agent configuration content is invalid")]
    InvalidContent,
    #[error("permission mode is not a supported desktop mode")]
    InvalidPermissionMode,
    #[error("configuration revision conflicts with the current value")]
    RevisionConflict,
    #[error("configuration revision is exhausted")]
    RevisionExhausted,
    #[error("deleted account-owned Agent cannot be reactivated")]
    Deleted,
    #[error("the default Agent must remain active")]
    DefaultMustRemainActive,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OwnerSpaceProduct {
    MusterAgent,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerSpace {
    pub space_id: OwnerSpaceId,
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub product: OwnerSpaceProduct,
    pub default_agent_id: Option<AgentId>,
    pub revision: NonZeroU64,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OwnedAgentStatus {
    Active,
    Archived,
    Deleted,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnedAgent {
    pub agent_id: AgentId,
    pub space_id: OwnerSpaceId,
    pub dialogue_id: AgentDialogueId,
    pub published_version_id: AgentVersionId,
    pub status: OwnedAgentStatus,
    pub revision: NonZeroU64,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl OwnedAgent {
    /// The prepared definition/version is immutable; activation rechecks the
    /// authority pointer in its transaction, never copies another Agent's memory.
    pub fn validate_definition(
        &self,
        definition: &AgentDefinition,
        version: &AgentVersion,
    ) -> Result<(), OwnedAgentError> {
        definition.validate()?;
        version.validate_complete_for_publication()?;
        if !matches!(definition.kind, AgentKind::Custom | AgentKind::Installed)
            || definition.lifecycle != AgentLifecycle::Active
        {
            return Err(OwnedAgentError::InvalidDefinition);
        }
        if definition.agent_id != self.agent_id
            || version.agent_id != self.agent_id
            || version.agent_version_id != self.published_version_id
        {
            return Err(OwnedAgentError::IdentityMismatch);
        }
        Ok(())
    }

    pub fn change_status(
        &self,
        expected: NonZeroU64,
        status: OwnedAgentStatus,
        is_default: bool,
        at: UtcTimestamp,
    ) -> Result<Self, OwnedAgentError> {
        if expected != self.revision {
            return Err(OwnedAgentError::RevisionConflict);
        }
        if self.status == OwnedAgentStatus::Deleted && status != OwnedAgentStatus::Deleted {
            return Err(OwnedAgentError::Deleted);
        }
        if is_default && status != OwnedAgentStatus::Active {
            return Err(OwnedAgentError::DefaultMustRemainActive);
        }
        if status == self.status {
            return Ok(self.clone());
        }
        Ok(Self {
            status,
            revision: next_revision(self.revision)?,
            updated_at: at,
            ..self.clone()
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnedAgentProfile {
    pub revision: NonZeroU64,
    pub display_name: String,
    pub description: String,
    pub avatar_ref: Option<ContentRef>,
}

impl OwnedAgentProfile {
    pub fn validate(&self) -> Result<(), OwnedAgentError> {
        if self.display_name.trim().is_empty()
            || self.display_name.len() > 120
            || self.description.len() > 2048
            || self.display_name.chars().any(char::is_control)
        {
            return Err(OwnedAgentError::InvalidProfile);
        }
        if let Some(avatar) = &self.avatar_ref
            && (avatar.byte_length == 0 || !avatar.media_type.as_str().starts_with("image/"))
        {
            return Err(OwnedAgentError::InvalidContent);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnedAgentConfiguration {
    pub agent_id: AgentId,
    pub manifest_revision: NonZeroU64,
    pub profile: OwnedAgentProfile,
    pub instructions_revision: NonZeroU64,
    pub instructions_ref: Option<ContentRef>,
    pub permission_revision: NonZeroU64,
    pub permission_mode: PermissionMode,
}

impl OwnedAgentConfiguration {
    pub fn initial(
        agent_id: AgentId,
        display_name: String,
        description: String,
        avatar_ref: Option<ContentRef>,
        instructions_ref: Option<ContentRef>,
    ) -> Result<Self, OwnedAgentError> {
        let value = Self {
            agent_id,
            manifest_revision: NonZeroU64::MIN,
            profile: OwnedAgentProfile {
                revision: NonZeroU64::MIN,
                display_name,
                description,
                avatar_ref,
            },
            instructions_revision: NonZeroU64::MIN,
            instructions_ref,
            permission_revision: NonZeroU64::MIN,
            permission_mode: PermissionMode::Trusted,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), OwnedAgentError> {
        self.profile.validate()?;
        if let Some(content) = &self.instructions_ref
            && (content.byte_length == 0
                || !matches!(
                    content.media_type.as_str(),
                    "text/plain"
                        | "text/markdown"
                        | "text/plain;charset=utf-8"
                        | "text/markdown;charset=utf-8"
                ))
        {
            return Err(OwnedAgentError::InvalidContent);
        }
        if !matches!(
            self.permission_mode,
            PermissionMode::Supervised | PermissionMode::Trusted | PermissionMode::FullAccess
        ) {
            return Err(OwnedAgentError::InvalidPermissionMode);
        }
        Ok(())
    }

    /// Each owner has its own CAS. Independent avatar/instruction edits can
    /// both commit; two stale edits of the same instructions cannot overwrite.
    pub fn update_profile(
        &self,
        expected: NonZeroU64,
        mut profile: OwnedAgentProfile,
    ) -> Result<Self, OwnedAgentError> {
        if expected != self.profile.revision {
            return Err(OwnedAgentError::RevisionConflict);
        }
        profile.revision = next_revision(expected)?;
        let result = Self {
            manifest_revision: next_revision(self.manifest_revision)?,
            profile,
            ..self.clone()
        };
        result.validate()?;
        Ok(result)
    }

    pub fn update_instructions(
        &self,
        expected: NonZeroU64,
        instructions_ref: Option<ContentRef>,
    ) -> Result<Self, OwnedAgentError> {
        if expected != self.instructions_revision {
            return Err(OwnedAgentError::RevisionConflict);
        }
        let result = Self {
            manifest_revision: next_revision(self.manifest_revision)?,
            instructions_revision: next_revision(expected)?,
            instructions_ref,
            ..self.clone()
        };
        result.validate()?;
        Ok(result)
    }

    pub fn update_permission(
        &self,
        expected: NonZeroU64,
        permission_mode: PermissionMode,
    ) -> Result<Self, OwnedAgentError> {
        if expected != self.permission_revision {
            return Err(OwnedAgentError::RevisionConflict);
        }
        let result = Self {
            manifest_revision: next_revision(self.manifest_revision)?,
            permission_revision: next_revision(expected)?,
            // Legacy clients may replay the original wire value. Only a new
            // preference write adopts smart approval; stored snapshots and
            // command fingerprints keep their original meaning.
            permission_mode: match permission_mode {
                PermissionMode::Supervised => PermissionMode::Trusted,
                value => value,
            },
            ..self.clone()
        };
        result.validate()?;
        Ok(result)
    }
}

fn next_revision(value: NonZeroU64) -> Result<NonZeroU64, OwnedAgentError> {
    value
        .get()
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .ok_or(OwnedAgentError::RevisionExhausted)
}
