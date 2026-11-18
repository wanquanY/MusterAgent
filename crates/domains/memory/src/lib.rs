//! Versioned durable knowledge selected into immutable Run snapshots.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::str::FromStr;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    AgentId, ContentId, IdempotencyKey, LedgerRecordId, MemoryId, MemoryVersionId, PrincipalId,
    RunId, TenantId, UtcTimestamp, VersionNumber, VersionedKey, WorkspaceId,
};
use thiserror::Error;

pub const MUSTERAGENT_MEMORY_SELECTION_POLICY_REF: &str =
    "musteragent.memory-selection.scope-confidence-recency@1";
pub const MUSTERAGENT_MEMORY_SELECTION_POLICY_CONTENT_ID: &str =
    "01982f80-0000-7000-8000-00000000000f";
pub const MUSTERAGENT_MEMORY_SELECTION_POLICY_V1: &str =
    include_str!("../assets/memory-selection-policy-v1.txt");

/// Returns the immutable built-in Memory selection-policy material used by
/// admission. The asset deliberately has no trailing newline, so its hash is
/// stable across Cloud and Device distributions.
pub fn musteragent_memory_selection_policy_content_ref()
-> Result<ContentRef, MemoryPolicyContentRefError> {
    Ok(ContentRef {
        content_id: ContentId::from_str(MUSTERAGENT_MEMORY_SELECTION_POLICY_CONTENT_ID)?,
        media_type: MediaType::new("text/plain;charset=utf-8")?,
        byte_length: MUSTERAGENT_MEMORY_SELECTION_POLICY_V1.len() as u64,
        sha256: Sha256::digest(MUSTERAGENT_MEMORY_SELECTION_POLICY_V1.as_bytes()),
    })
}

#[derive(Debug, Error)]
pub enum MemoryPolicyContentRefError {
    #[error(transparent)]
    Identity(#[from] musteragent_identity::IdentityError),
    #[error(transparent)]
    MediaType(#[from] musteragent_content_ref::MediaTypeError),
}

/// The authorization and retrieval boundary of a Memory.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MemoryScope {
    User { principal_id: PrincipalId },
    Tenant { tenant_id: TenantId },
    Workspace { workspace_id: WorkspaceId },
    Agent { agent_id: AgentId },
}

/// Product meaning of durable knowledge.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MemoryKind {
    Preference,
    Semantic,
    Procedure,
    FailureLesson,
}

/// Lifecycle independent from Evolution candidate status.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MemoryLifecycle {
    Active,
    Superseded,
    Revoked,
    Expired,
}

/// How a Memory version entered the Memory context.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MemorySource {
    UserAuthored,
    VerifiedExperience,
    GovernedImport,
}

/// Confidence without floating-point persistence or comparison drift.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemoryConfidenceBps(u16);

impl MemoryConfidenceBps {
    pub fn new(value: u16) -> Result<Self, MemoryContractError> {
        if value <= 10_000 {
            Ok(Self(value))
        } else {
            Err(MemoryContractError::InvalidConfidence)
        }
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// Stable Memory aggregate identity and current version pointer.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemoryRecord {
    pub memory_id: MemoryId,
    pub tenant_id: TenantId,
    pub scope: MemoryScope,
    pub memory_key: IdempotencyKey,
    pub kind: MemoryKind,
    pub lifecycle: MemoryLifecycle,
    pub current_version_id: MemoryVersionId,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub revoked_at: Option<UtcTimestamp>,
}

impl MemoryRecord {
    pub fn validate(&self) -> Result<(), MemoryContractError> {
        if matches!(self.scope, MemoryScope::Tenant { tenant_id } if tenant_id != self.tenant_id) {
            return Err(MemoryContractError::TenantScopeMismatch);
        }
        if self.updated_at < self.created_at
            || (self.lifecycle == MemoryLifecycle::Revoked) != self.revoked_at.is_some()
            || self
                .revoked_at
                .is_some_and(|revoked_at| revoked_at < self.created_at)
        {
            return Err(MemoryContractError::InvalidLifecycleTimestamps);
        }
        Ok(())
    }
}

/// Immutable Memory content with runtime-owned provenance.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemoryVersion {
    pub memory_version_id: MemoryVersionId,
    pub memory_id: MemoryId,
    pub version: VersionNumber,
    pub source: MemorySource,
    pub content_ref: ContentRef,
    pub confidence: MemoryConfidenceBps,
    pub data_classification: musteragent_policy::DataClassification,
    pub source_run_ids: BTreeSet<RunId>,
    pub evidence_record_ids: BTreeSet<LedgerRecordId>,
    pub created_at: UtcTimestamp,
    pub valid_until: Option<UtcTimestamp>,
}

impl MemoryVersion {
    /// Validates that learned knowledge has evidence owned by the Runtime.
    pub fn validate(&self) -> Result<(), MemoryContractError> {
        if self.source == MemorySource::VerifiedExperience
            && (self.source_run_ids.is_empty() || self.evidence_record_ids.is_empty())
        {
            return Err(MemoryContractError::MissingRuntimeEvidence);
        }
        if self
            .valid_until
            .is_some_and(|valid_until| valid_until <= self.created_at)
        {
            return Err(MemoryContractError::InvalidRetentionWindow);
        }
        Ok(())
    }
}

/// A version pinned into one Runtime Snapshot.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelectedMemoryVersion {
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
}

/// Deterministic Memory selection; no live query executes inside a Run.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemorySelectionSnapshot {
    pub selected: Vec<SelectedMemoryVersion>,
    pub selection_policy_ref: VersionedKey,
    pub selection_policy_hash: Sha256,
}

impl MemorySelectionSnapshot {
    /// Requires stable ordering and uniqueness for deterministic snapshot hashes.
    pub fn validate(&self) -> Result<(), MemoryContractError> {
        if self.selected.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(MemoryContractError::SelectionNotCanonical);
        }
        Ok(())
    }
}

/// A Memory contract would produce unverifiable or non-deterministic state.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum MemoryContractError {
    #[error("verified experience requires source runs and Ledger evidence")]
    MissingRuntimeEvidence,
    #[error("selected Memory versions must be unique and sorted")]
    SelectionNotCanonical,
    #[error("memory confidence must be between 0 and 10000 basis points")]
    InvalidConfidence,
    #[error("tenant-scoped Memory must use the owning tenant ID")]
    TenantScopeMismatch,
    #[error("Memory lifecycle timestamps are inconsistent")]
    InvalidLifecycleTimestamps,
    #[error("Memory retention deadline must follow its creation time")]
    InvalidRetentionWindow,
}

#[cfg(test)]
mod tests {
    use super::{
        MUSTERAGENT_MEMORY_SELECTION_POLICY_CONTENT_ID, MUSTERAGENT_MEMORY_SELECTION_POLICY_REF,
        MUSTERAGENT_MEMORY_SELECTION_POLICY_V1, musteragent_memory_selection_policy_content_ref,
    };

    #[test]
    fn built_in_memory_selection_policy_has_frozen_bytes() {
        let reference =
            musteragent_memory_selection_policy_content_ref().expect("policy ContentRef");
        assert_eq!(
            MUSTERAGENT_MEMORY_SELECTION_POLICY_REF,
            "musteragent.memory-selection.scope-confidence-recency@1"
        );
        assert_eq!(
            reference.content_id.to_string(),
            MUSTERAGENT_MEMORY_SELECTION_POLICY_CONTENT_ID
        );
        assert_eq!(reference.byte_length, 217);
        assert_eq!(
            reference.sha256.to_string(),
            "5794190c7758c9b31c2ac4900095f3d831f9d4fff671655b861fbc2d5246579d"
        );
        assert!(!MUSTERAGENT_MEMORY_SELECTION_POLICY_V1.ends_with('\n'));
    }
}
