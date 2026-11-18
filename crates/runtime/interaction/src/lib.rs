//! Durable Interaction request/response/expiry contracts.

#![forbid(unsafe_code)]

pub mod approval_prompt;
mod capability_request;
#[cfg(test)]
mod question_tests;
mod structured_response;
pub use capability_request::{
    CAPABILITY_PROMPT_SCHEMA, CAPABILITY_RESPONSE_SCHEMA, CapabilityDescriptor, CapabilityRequest,
};
mod model_capability_request;
pub use model_capability_request::{
    CapabilityRequestResolution, CapabilityRequestResolver, CapabilityResolveError,
    ModelCapabilityRequest,
};
mod device_permissions;
pub use device_permissions::{DevicePermission, DevicePermissionPrompt};
mod task_acceptance_review;
pub use task_acceptance_review::{
    TaskAcceptancePrompt, TaskAcceptanceReview, TaskReviewContentRef, TaskReviewCriterion,
    TaskReviewEvidence, TaskReviewEvidenceKind,
};

pub use structured_response::{
    InteractionResponseValidationError, InteractionResponseValidator,
    StrictInteractionResponseValidator, StructuredInteractionError, StructuredInteractionKind,
    StructuredInteractionRequest,
};

use musteragent_approval::{
    Interaction, InteractionClosure, InteractionContractError, InteractionResponseRef,
    InteractionStatus,
};
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::{
    Actor, InteractionId, LedgerRecordId, PrincipalKind, RunSeq, TenantId, UtcTimestamp,
};
use musteragent_ledger::{AppendReceipt, AppendRequest, AttemptFence};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct InteractionSemanticRecords {
    pub interaction_record_id: LedgerRecordId,
    /// Interaction responses can queue a resumed Attempt without fabricating
    /// a `RunResumed` fact before the Scheduler has claimed that Attempt.
    pub run_record_id: Option<LedgerRecordId>,
    pub tool_record_id: Option<LedgerRecordId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RequestInteractionCommit {
    pub interaction: Interaction,
    pub fence: AttemptFence,
    pub expected_last_run_sequence: RunSeq,
    pub append: AppendRequest,
    pub semantic_records: InteractionSemanticRecords,
}

impl RequestInteractionCommit {
    pub fn validate(&self) -> Result<(), InteractionRuntimeError> {
        self.append
            .validate()
            .map_err(|_| InteractionRuntimeError::InvalidLedgerAppend)?;
        if self.interaction.status != InteractionStatus::Pending
            || self.interaction.response_ref.is_some()
            || self.interaction.responded_by.is_some()
            || self.interaction.responded_at.is_some()
            || self.interaction.tenant_id != self.append.tenant_id
            || self.interaction.conversation_id != self.append.conversation_id
            || self.interaction.run_id != self.fence.run_id
            || self.append.expected_home_epoch != self.fence.home_epoch
            || !has_consecutive_run_sequences(&self.append, self.expected_last_run_sequence)
        {
            return Err(InteractionRuntimeError::IdentityMismatch);
        }
        validate_semantic_records(&self.append, self.semantic_records)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RespondInteractionCommit {
    pub actor: Actor,
    pub interaction_id: InteractionId,
    pub response: InteractionResponseRef,
    pub response_hash: Sha256,
    pub responded_at: UtcTimestamp,
    pub expected_last_run_sequence: RunSeq,
    pub append: AppendRequest,
}

impl RespondInteractionCommit {
    pub fn validate(&self) -> Result<(), InteractionRuntimeError> {
        self.append
            .validate()
            .map_err(|_| InteractionRuntimeError::InvalidLedgerAppend)?;
        if let InteractionResponseRef::Secure(value) = &self.response {
            let secret = value.as_str().as_bytes();
            if self
                .append
                .records
                .iter()
                .any(|record| contains_bytes(&record.payload, secret))
                || self
                    .append
                    .outbox
                    .iter()
                    .any(|message| contains_bytes(&message.payload, secret))
            {
                return Err(InteractionRuntimeError::SecretLeak);
            }
        }
        if self.actor.principal_kind != PrincipalKind::User
            || self.actor.tenant_id != self.append.tenant_id
            || self.response_hash != response_hash(&self.response)
            || self.append.records.len() != 1
            || self.append.records[0].record_kind != LedgerRecordKind::DomainEvent
            || self.append.outbox.len() != 1
            || self.append.outbox[0].topic.as_str() != "run.dispatch"
            || !has_consecutive_run_sequences(&self.append, self.expected_last_run_sequence)
        {
            return Err(InteractionRuntimeError::IdentityMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CloseInteractionCommit {
    pub tenant_id: TenantId,
    pub interaction_id: InteractionId,
    pub closure: InteractionClosure,
    pub closed_at: UtcTimestamp,
    pub expected_last_run_sequence: RunSeq,
    pub append: AppendRequest,
    pub semantic_records: InteractionSemanticRecords,
}

impl CloseInteractionCommit {
    pub fn validate(&self) -> Result<(), InteractionRuntimeError> {
        self.append
            .validate()
            .map_err(|_| InteractionRuntimeError::InvalidLedgerAppend)?;
        if self.tenant_id != self.append.tenant_id
            || self.semantic_records.tool_record_id.is_some()
            || self.append.outbox.len() != 1
            || self.append.outbox[0].topic.as_str() != "run.dispatch"
            || !has_consecutive_run_sequences(&self.append, self.expected_last_run_sequence)
        {
            return Err(InteractionRuntimeError::IdentityMismatch);
        }
        validate_semantic_records(&self.append, self.semantic_records)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum InteractionCommitDisposition {
    Committed,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InteractionCommitReceipt {
    pub disposition: InteractionCommitDisposition,
    pub interaction_id: InteractionId,
    pub ledger: AppendReceipt,
}

pub trait InteractionStore: Send + Sync {
    type Error: Send;

    fn request(
        &self,
        command: RequestInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, Self::Error>>;

    fn respond(
        &self,
        command: RespondInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, Self::Error>>;

    fn close(
        &self,
        command: CloseInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, Self::Error>>;

    fn load<'a>(
        &'a self,
        tenant_id: TenantId,
        interaction_id: InteractionId,
    ) -> ApplicationFuture<'a, Result<Interaction, Self::Error>>;
}

#[must_use]
pub fn response_hash(response: &InteractionResponseRef) -> Sha256 {
    match response {
        InteractionResponseRef::Approval(value) => match value {
            musteragent_approval::ApprovalDecision::Approve => {
                Sha256::digest(b"musteragent.legacy.interaction.approve.v1")
            }
            musteragent_approval::ApprovalDecision::ApproveSession => {
                Sha256::digest(b"musteragent.legacy.interaction.approve-session.v1")
            }
            musteragent_approval::ApprovalDecision::Deny => {
                Sha256::digest(b"musteragent.legacy.interaction.deny.v1")
            }
        },
        InteractionResponseRef::Content(value) => value.sha256,
        InteractionResponseRef::Secure(value) => {
            let mut bytes = b"musteragent.legacy.interaction.secure-ref.v1\0".to_vec();
            bytes.extend_from_slice(value.as_str().as_bytes());
            Sha256::digest(bytes)
        }
    }
}

fn validate_semantic_records(
    append: &AppendRequest,
    records: InteractionSemanticRecords,
) -> Result<(), InteractionRuntimeError> {
    let ids = [
        Some(records.interaction_record_id),
        records.run_record_id,
        records.tool_record_id,
    ];
    let expected = ids.into_iter().flatten().count();
    let found = append
        .records
        .iter()
        .filter(|record| {
            record.record_kind == LedgerRecordKind::DomainEvent
                && ids.contains(&Some(record.record_id))
        })
        .count();
    if found == expected {
        Ok(())
    } else {
        Err(InteractionRuntimeError::MissingSemanticRecord)
    }
}

fn has_consecutive_run_sequences(append: &AppendRequest, current: RunSeq) -> bool {
    append.records.iter().enumerate().all(|(index, record)| {
        let Ok(increment) = u64::try_from(index + 1) else {
            return false;
        };
        current
            .get()
            .checked_add(increment)
            .and_then(|value| RunSeq::new(value).ok())
            == record.run_seq
    })
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum InteractionRuntimeError {
    #[error("Interaction mutation identity does not match its Run or Ledger")]
    IdentityMismatch,
    #[error("Interaction mutation has an invalid Ledger append")]
    InvalidLedgerAppend,
    #[error("Interaction mutation is missing a semantic Ledger record")]
    MissingSemanticRecord,
    #[error("secure Interaction response leaked into Ledger or Outbox payload")]
    SecretLeak,
    #[error(transparent)]
    Contract(#[from] InteractionContractError),
}

/// Maximum UTF-8 bytes accepted as one command-scoped interaction response.
pub const MAX_INTERACTION_INPUT_BYTES: usize = 32 * 1024;
