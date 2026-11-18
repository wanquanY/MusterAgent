//! Durable provider completion facts, separate from Artifact materialization.
use crate::BackendMediaSourceProof;
use musteragent_identity::UtcTimestamp;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityInvocation;
use musteragent_runtime_contract::ApplicationFuture;

/// Exact provider result retained before fetching assets. The observation time
/// is frozen so retrying a lost Artifact commit produces identical facts.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordedMediaResult {
    pub source: BackendMediaSourceProof,
    pub observed_at: UtcTimestamp,
}

/// Immutable result journal keyed by tenant and original invocation. Implementors
/// bind provider, request, run and actor, reject conflicts, and fence new facts.
/// Asset bytes belong to Content; the journal retains fetchable result metadata.
pub trait MediaGenerationJournal: Send + Sync {
    /// Read the original completion without invoking the provider.
    fn load<'a>(
        &'a self,
        invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<RecordedMediaResult>, MediaJournalError>>;
    /// Append under a live fence, or return the original identical fact on replay.
    fn record<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        invocation: &'a CapabilityInvocation,
        result: RecordedMediaResult,
    ) -> ApplicationFuture<'a, Result<RecordedMediaResult, MediaJournalError>>;
}

/// A journal failure never proves an external generation failed.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum MediaJournalError {
    #[error("media result persistence is unavailable")]
    Unavailable,
    #[error("media result conflicts with the original invocation")]
    Conflict,
    #[error("media result writer has a stale execution fence")]
    StaleFence,
    #[error("media result proof is invalid")]
    Invalid,
}
