use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    IdempotencyKey, PrincipalId, SkillId, SkillPackagePreviewId, SkillVersionId, TenantId,
    UtcTimestamp,
};
use thiserror::Error;

use crate::SkillKey;

/// A private, mutable checkout of one complete Skill Package. A Preview is
/// not a public release and is never admitted to a Run. Saving and installing
/// are lifecycle operations over the immutable package version created from
/// this source.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SkillPackagePreviewStatus {
    Invalid,
    Ready,
    Archived,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct SkillPackagePreviewGuard {
    pub preview_id: SkillPackagePreviewId,
    pub revision: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillPackagePreview {
    pub preview_id: SkillPackagePreviewId,
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub identity_key: SkillKey,
    pub target_skill_id: Option<SkillId>,
    pub baseline_skill_version_id: Option<SkillVersionId>,
    pub revision: u64,
    pub source_archive_ref: ContentRef,
    pub source_payload_hash: Sha256,
    pub validation_report_ref: ContentRef,
    pub validation_passed: bool,
    pub status: SkillPackagePreviewStatus,
    pub creation_idempotency_key: IdempotencyKey,
    pub last_mutation_idempotency_key: IdempotencyKey,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl SkillPackagePreview {
    pub fn validate(&self) -> Result<(), SkillPackagePreviewContractError> {
        if self.revision == 0
            || self.source_archive_ref.byte_length == 0
            || self.validation_report_ref.byte_length == 0
            || self.updated_at < self.created_at
            || self.target_skill_id.is_some() != self.baseline_skill_version_id.is_some()
        {
            return Err(SkillPackagePreviewContractError::InvalidPreview);
        }
        if matches!(self.status, SkillPackagePreviewStatus::Ready) != self.validation_passed
            && self.status != SkillPackagePreviewStatus::Archived
        {
            return Err(SkillPackagePreviewContractError::InvalidPreview);
        }
        Ok(())
    }

    #[must_use]
    pub const fn guard(&self) -> SkillPackagePreviewGuard {
        SkillPackagePreviewGuard {
            preview_id: self.preview_id,
            revision: self.revision,
        }
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SkillPackagePreviewContractError {
    #[error("Skill Package Preview violates its identity, revision or content contract")]
    InvalidPreview,
}
