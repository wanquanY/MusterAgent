use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::ContentRef;
use musteragent_identity::InvocationId;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityReconciliation, OperationContext,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
};
use uuid::Uuid;

use crate::BackendMediaSourceKind;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BackendMediaAssetProof {
    pub content_ref: ContentRef,
    pub delivery_url: Option<String>,
    pub content: Vec<u8>,
    pub dimensions: Option<(u32, u32)>,
}

/// Authenticated Backend materialization proof. It remains private Runtime
/// input until the Artifact owner validates and commits the public Tool result.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BackendMediaSourceProof {
    pub kind: BackendMediaSourceKind,
    pub source_id: Uuid,
    pub task_id: String,
    pub title: String,
    pub prompt: String,
    pub asset_urls: Vec<String>,
    pub assets: Vec<BackendMediaAssetProof>,
    pub thumbnail_url: Option<String>,
    pub provider_request_ref: Option<OpaqueExternalRef>,
    pub failure_code: Option<String>,
    pub provider_model: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BackendMediaProviderOutcome {
    /// A durable, nonterminal provider task. Admission is not completion and
    /// neither queued nor running is an unknown external outcome.
    Active {
        source: BackendMediaSourceProof,
        phase: BackendMediaPhase,
        reconciliation_ref: OpaqueExternalRef,
    },
    Completed(BackendMediaSourceProof),
    Failed {
        source: BackendMediaSourceProof,
        error: ErrorInfo,
        retry_disposition: RetryDisposition,
    },
    NotApplied,
    Pending {
        reason: SafeDisplayText,
        reconciliation_ref: OpaqueExternalRef,
    },
    /// Reconciliation cannot continue because its immutable authority or
    /// provider proof contract is invalid. The external effect remains
    /// unproven, so callers must surface an unknown outcome rather than a
    /// provider failure or a retry.
    Unrecoverable {
        reason: SafeDisplayText,
        reconciliation_ref: OpaqueExternalRef,
    },
    Rejected {
        error: ErrorInfo,
        retry_disposition: RetryDisposition,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum BackendMediaPhase {
    Queued,
    Running,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MediaMaterializationError {
    Unavailable,
    InvalidContent,
    Cancelled,
}

/// Provider-neutral media execution port shared by Cloud and Device homes.
/// Transport authentication and protocol mapping stay in the Backend adapter.
pub trait BackendMediaCapabilityProvider: Send + Sync {
    /// Exact method and media output implemented by this adapter.
    fn operation(&self) -> &musteragent_tool::BackendOperation;

    fn manifest(&self) -> &CapabilityManifest;

    /// Provider-owned reconciliation identity. Runtime seals this exact value
    /// before dispatch and treats it as opaque thereafter.
    fn reconciliation_identity(&self, invocation_id: InvocationId) -> OpaqueExternalRef;

    /// Submit under the original idempotency key. The adapter must retain a durable
    /// mapping from reconciliation identity to provider job, including ambiguous
    /// submission outcomes. Repeated transport calls must not generate a second job.
    fn invoke_media(
        &self,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'_, BackendMediaProviderOutcome>;

    /// Query the original job without resubmission. Missing/ambiguous evidence is
    /// Pending, never Completed, Failed or NotApplied without provider proof.
    fn reconcile_media(
        &self,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'_, BackendMediaProviderOutcome>;
    /// Transfer an already completed result into the owning Content Store.
    /// This is separate from bounded status observation and never generates media.
    /// Implementations must fetch or verify the exact assets through the Content owner.
    /// A retry receives the recorded result and must never submit another job.
    /// Existing ContentRefs and all result metadata must remain unchanged. When
    /// completion supplies URLs without ContentRefs, fetch each immutable asset
    /// using `media_asset_content_id` so repeated materialization cannot choose
    /// fresh content identities or silently replace previously fetched bytes.
    fn materialize_media(
        &self,
        invocation: CapabilityInvocation,
        source: BackendMediaSourceProof,
        execution: Option<OperationContext>,
    ) -> ApplicationFuture<'_, Result<BackendMediaSourceProof, MediaMaterializationError>>;
}

/// Stable Content identity for an asset whose provider completion supplied a URL
/// rather than a ContentRef. The ordinal is the provider result's original order.
#[must_use]
pub fn media_asset_content_id(
    invocation: InvocationId,
    ordinal: u16,
) -> musteragent_identity::ContentId {
    musteragent_identity::ContentId::from_uuid(Uuid::new_v5(
        &invocation.into_uuid(),
        format!("media-asset@1:{ordinal}").as_bytes(),
    ))
    .expect("derived UUID")
}
