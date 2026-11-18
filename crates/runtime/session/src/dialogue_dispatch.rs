//! Durable input-to-Run handoff. This queue is not an execution state machine:
//! once admission is committed, the existing Runtime owns every Run transition.
use crate::{DialogueExecutionPreparation, DialogueExecutionReceipt, DialogueStoreError};
use musteragent_execution_context::SignedDialogueAdmissionRejection;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_identity::{Actor, UserIntentId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_scheduler::AdmissionSelection;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueIntentClaim {
    pub actor: Actor,
    pub intent_id: UserIntentId,
    pub owner: uuid::Uuid,
    pub generation: u64,
    pub attempt_count: u64,
    pub lease_expires_at: UtcTimestamp,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueHandoffFailure {
    AuthorityUnavailable,
    PolicyDenied,
    PreparationChanged,
    AdmissionFailed,
    DependencyUnavailable,
    InvalidContract,
}
impl DialogueHandoffFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AuthorityUnavailable => "authority_unavailable",
            Self::PolicyDenied => "policy_denied",
            Self::PreparationChanged => "preparation_changed",
            Self::AdmissionFailed => "admission_failed",
            Self::DependencyUnavailable => "dependency_unavailable",
            Self::InvalidContract => "invalid_contract",
        }
    }
}
pub trait DialogueIntentHandoffStore: Send + Sync {
    fn claim(
        &self,
        owner: uuid::Uuid,
        lease: Duration,
    ) -> ApplicationFuture<'_, Result<Option<DialogueIntentClaim>, DialogueStoreError>>;
    fn complete(
        &self,
        claim: DialogueIntentClaim,
        receipt: DialogueExecutionReceipt,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>>;
    fn reject(
        &self,
        claim: DialogueIntentClaim,
        preparation: DialogueExecutionPreparation,
        proof: SignedDialogueAdmissionRejection,
    ) -> ApplicationFuture<'_, Result<DialogueAdmissionTermination, DialogueStoreError>>;
    fn fail_local(
        &self,
        claim: DialogueIntentClaim,
        failure: LocalAdmissionFailure,
    ) -> ApplicationFuture<'_, Result<DialogueAdmissionTermination, DialogueStoreError>>;
    fn cancel_inactive(
        &self,
        claim: DialogueIntentClaim,
    ) -> ApplicationFuture<'_, Result<DialogueAdmissionTermination, DialogueStoreError>>;
    fn retry(
        &self,
        claim: DialogueIntentClaim,
        failure: DialogueHandoffFailure,
        delay: Duration,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>>;
}
pub struct DialogueExecutionGrant {
    pub signed: SignedExecutionGrant,
    pub selection: AdmissionSelection,
}
#[expect(
    clippy::large_enum_variant,
    reason = "The public enum exposes these payloads by value to host implementations."
)]
pub enum DialogueExecutionGrantDecision {
    Granted(DialogueExecutionGrant),
    Rejected(SignedDialogueAdmissionRejection),
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueAdmissionTermination {
    Committed,
    Replayed,
    AlreadyAdmitted,
    StalePreparation,
    LeaseLost,
    StillActive,
}
pub trait DialogueExecutionGrantPort: Send + Sync {
    fn issue<'a>(
        &'a self,
        preparation: &'a DialogueExecutionPreparation,
    ) -> ApplicationFuture<'a, Result<DialogueExecutionGrantDecision, DialogueHandoffFailure>>;
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueDispatchOutcome {
    Idle,
    HandedOff,
    Rejected,
    Cancelled,
    Failed,
    Deferred,
    LeaseLost,
}
#[derive(Debug, Clone)]
pub struct LocalAdmissionFailure {
    preparation: Option<DialogueExecutionPreparation>,
    kind: LocalFailureKind,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LocalFailureKind {
    PreparationCorrupt,
    SnapshotInvalid,
    AdmissionInvalid,
}
impl LocalFailureKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreparationCorrupt => "invalid_preparation",
            Self::SnapshotInvalid => "invalid_snapshot",
            Self::AdmissionInvalid => "invalid_admission",
        }
    }
}
impl LocalAdmissionFailure {
    /// Requests the original source owner to recheck corruption under its input
    /// and claim locks. This value is not evidence or authority to fail a queue;
    /// the durable owner must independently reproduce the canonical failure.
    pub const fn preparation_corrupt() -> Self {
        Self {
            preparation: None,
            kind: LocalFailureKind::PreparationCorrupt,
        }
    }
    pub fn preparation(&self) -> Option<&DialogueExecutionPreparation> {
        self.preparation.as_ref()
    }
    pub const fn kind(&self) -> LocalFailureKind {
        self.kind
    }
    pub const fn message(&self) -> &'static str {
        "这次请求未能启动，系统遇到了问题。请稍后重新发送。"
    }
}
