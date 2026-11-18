//! Fenced native publication port; the host owns delivery.
use musteragent_content_ref::{ContentRef, SchemaRef};
use musteragent_identity::InvocationId;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ApplicationFuture;
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum DialogueToolPublicationError {
    #[error("publication input does not match the registered tool contract")]
    InvalidArguments,
    #[error("publication is not authorized by this execution's sealed protocol")]
    Forbidden,
    #[error("the referenced artifact receipt is unavailable or no longer authorized")]
    ReferenceUnavailable,
    #[error("publication execution was cancelled or lost its lease")]
    StaleFence,
    #[error("publication storage is temporarily unavailable; reconcile the original invocation")]
    Infrastructure,
    #[error("publication durable state failed its integrity checks")]
    Corrupt,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueToolPublicationReceipt {
    pub result_ref: ContentRef,
    pub result_schema: SchemaRef,
}

/// An invocation ID is only a lookup key. The implementation must bind its Run,
/// tenant, native tool key, sealed publication protocol, schema and original
/// request content before publishing. It commits the group, Tool receipt,
/// Ledger and outbox together, and returns the original receipt on replay.
pub trait FencedDialogueToolPublication: Send + Sync {
    fn commit_with_activity<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        invocation_id: InvocationId,
        _activity: &'a dyn DialoguePublicationActivity,
    ) -> ApplicationFuture<'a, Result<DialogueToolPublicationReceipt, DialogueToolPublicationError>>
    {
        self.commit(attempt, invocation_id)
    }

    fn commit<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        invocation_id: InvocationId,
    ) -> ApplicationFuture<'a, Result<DialogueToolPublicationReceipt, DialogueToolPublicationError>>;
}

/// Observation of the original Tool operation, not another execution owner.
/// Report only verified completed chunks/stages, never polling or retry ticks.
pub trait DialoguePublicationActivity: Send + Sync {
    fn completed_stage(&self) -> Result<(), DialogueToolPublicationError>;
    fn cancelled(&self) -> ApplicationFuture<'_, ()>;
}
