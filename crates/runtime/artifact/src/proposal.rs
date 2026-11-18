use musteragent_artifact::{ArtifactProposal, ArtifactProposalStatus, ArtifactVersion};
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::{
    ArtifactId, ArtifactProposalId, LedgerRecordId, ParticipantId, TenantId, UtcTimestamp,
    VersionNumber,
};
use musteragent_ledger::{AppendReceipt, AppendRequest};
use musteragent_runtime_contract::ApplicationFuture;

use crate::ArtifactRuntimeError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProposeArtifactEdit {
    pub proposal: ArtifactProposal,
    pub append: AppendRequest,
    pub edit_proposed_record_id: LedgerRecordId,
}

impl ProposeArtifactEdit {
    pub fn validate(&self) -> Result<(), ArtifactRuntimeError> {
        self.proposal
            .validate()
            .map_err(|_| ArtifactRuntimeError::InvalidProposal)?;
        self.append
            .validate()
            .map_err(|_| ArtifactRuntimeError::InvalidLedgerAppend)?;
        if self.proposal.status != ArtifactProposalStatus::Pending
            || self.append.tenant_id != self.proposal.tenant_id
        {
            return Err(ArtifactRuntimeError::InvalidProposal);
        }
        require_domain_records(&self.append, &[self.edit_proposed_record_id])
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApplyArtifactProposal {
    pub tenant_id: TenantId,
    pub proposal_id: ArtifactProposalId,
    pub artifact_id: ArtifactId,
    pub expected_base_version: VersionNumber,
    pub version: ArtifactVersion,
    pub title: Option<String>,
    pub applied_by: ParticipantId,
    pub applied_at: UtcTimestamp,
    pub append: AppendRequest,
    pub edit_applied_record_id: LedgerRecordId,
    pub version_committed_record_id: LedgerRecordId,
}

impl ApplyArtifactProposal {
    pub fn validate(&self) -> Result<(), ArtifactRuntimeError> {
        self.version
            .validate()
            .map_err(|_| ArtifactRuntimeError::VersionMismatch)?;
        self.append
            .validate()
            .map_err(|_| ArtifactRuntimeError::InvalidLedgerAppend)?;
        let next = self
            .expected_base_version
            .get()
            .checked_add(1)
            .ok_or(ArtifactRuntimeError::VersionMismatch)?;
        if self.append.tenant_id != self.tenant_id
            || self.version.artifact_id != self.artifact_id
            || self.version.version.get() != next
            || self.version.committed_by.participant_id() != Some(self.applied_by)
            || self.version.committed_at != self.applied_at
            || self
                .title
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.chars().count() > 512)
        {
            return Err(ArtifactRuntimeError::InvalidProposal);
        }
        require_domain_records(
            &self.append,
            &[
                self.edit_applied_record_id,
                self.version_committed_record_id,
            ],
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CloseArtifactProposal {
    pub tenant_id: TenantId,
    pub proposal_id: ArtifactProposalId,
    pub artifact_id: ArtifactId,
    pub resolution: ArtifactProposalStatus,
    pub resolved_by: ParticipantId,
    pub resolved_at: UtcTimestamp,
    pub append: AppendRequest,
    pub resolution_record_id: LedgerRecordId,
}

impl CloseArtifactProposal {
    pub fn validate(&self) -> Result<(), ArtifactRuntimeError> {
        self.append
            .validate()
            .map_err(|_| ArtifactRuntimeError::InvalidLedgerAppend)?;
        if self.append.tenant_id != self.tenant_id
            || !matches!(
                self.resolution,
                ArtifactProposalStatus::Rejected | ArtifactProposalStatus::Expired
            )
        {
            return Err(ArtifactRuntimeError::InvalidProposal);
        }
        require_domain_records(&self.append, &[self.resolution_record_id])
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactProposalDisposition {
    Committed,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactProposalReceipt {
    pub disposition: ArtifactProposalDisposition,
    pub proposal_id: ArtifactProposalId,
    pub status: ArtifactProposalStatus,
    pub ledger: AppendReceipt,
}

pub trait ArtifactProposalStore: Send + Sync {
    type Error: Send;

    fn propose(
        &self,
        command: ProposeArtifactEdit,
    ) -> ApplicationFuture<'_, Result<ArtifactProposalReceipt, Self::Error>>;

    fn apply(
        &self,
        command: ApplyArtifactProposal,
    ) -> ApplicationFuture<'_, Result<ArtifactProposalReceipt, Self::Error>>;

    fn close(
        &self,
        command: CloseArtifactProposal,
    ) -> ApplicationFuture<'_, Result<ArtifactProposalReceipt, Self::Error>>;

    fn load_proposal(
        &self,
        tenant_id: TenantId,
        proposal_id: ArtifactProposalId,
    ) -> ApplicationFuture<'_, Result<ArtifactProposal, Self::Error>>;
}

fn require_domain_records(
    append: &AppendRequest,
    record_ids: &[LedgerRecordId],
) -> Result<(), ArtifactRuntimeError> {
    if record_ids.iter().all(|record_id| {
        append.records.iter().any(|record| {
            record.record_id == *record_id && record.record_kind == LedgerRecordKind::DomainEvent
        })
    }) {
        Ok(())
    } else {
        Err(ArtifactRuntimeError::MissingSemanticRecord)
    }
}
