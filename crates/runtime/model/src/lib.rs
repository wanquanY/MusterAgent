//! Canonical model invocation and physical usage facts owned by the Rust Runtime.

#![forbid(unsafe_code)]

mod annotations;
pub use annotations::{ModelAnnotationError, ModelRequestAnnotations};

mod recovery;
pub use recovery::{model_retry_not_before, remaining_model_retry_delay};

mod approval_observation;
mod argument_diagnostic;
mod canonical;
pub use approval_observation::{ToolApprovalObservation, ToolApprovalResolution};
mod dialogue_input;
mod native_publication;
mod public_delivery_repair;
pub use argument_diagnostic::{
    ArgumentDiagnostic, ArgumentSource, JsonParseCategory, JsonParseIssue,
};
pub use dialogue_input::{DialogueInputObservation, InputAttachmentKind, input_attachment_kind};
mod credential_authorization;
pub mod dependency_recovery;
mod execution;
mod failure;
mod fallback;
mod port;
mod stream_state;
pub mod transport_authority;

pub use canonical::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelError,
    CanonicalModelRequest, CanonicalRole, CanonicalRuntimeEvent, CanonicalToolCall,
    CanonicalToolDefinition, GenerationLimit, JsonValue, ModelReplayText, ModelStreamEvent,
    ModelText, ProviderError, ProviderErrorCode, ProviderErrorKind, ProviderReasoningReplayItem,
    ProviderToolCallReplay, ReasoningPolicy, ReasoningReplay, StopReason, validate_tool_exchanges,
};
pub use credential_authorization::{
    ModelCredentialAuthorization, ModelCredentialAuthorizationError,
};
pub use execution::{
    ModelExecution, validate_model_execution_binding, validate_model_invocation_binding,
    validate_model_recovery_binding,
};
pub use fallback::{FallbackContractError, ModelFallbackPlan};
pub use port::{ModelEventStream, ModelProvider};
pub use stream_state::{ModelStreamProgress, ModelStreamState, ModelStreamStateError};

/// Canonical media type for every newly sealed provider-neutral model request.
pub const MODEL_REQUEST_MEDIA_TYPE: &str = "application/vnd.musteragent.model-request+json";
/// Frozen historical media type retained only for persisted Content metadata.
pub const LEGACY_MODEL_REQUEST_MEDIA_TYPE: &str =
    "application/vnd.musteragent.legacy.model-request+json";

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    AttemptId, ConversationId, IdempotencyKey, InvocationId, OutboxMessageId, RunId, TenantId,
    UtcTimestamp, VersionedKey,
};
use musteragent_model::{
    AuxiliaryModelRole, CredentialGrantRef, ModelKey, ModelRoute, PricingVersionRef,
    SignedModelRouteHash,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorInfo, OpaqueExternalRef, RetryDisposition,
};
use thiserror::Error;

/// Immutable, admission-pinned route identity; never a provider SDK model string.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ModelRouteRef(VersionedKey);

impl ModelRouteRef {
    #[must_use]
    pub const fn from_key(value: VersionedKey) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ProviderRequestRef(OpaqueExternalRef);

impl ProviderRequestRef {
    #[must_use]
    pub const fn from_opaque(value: OpaqueExternalRef) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Encrypted, provider-bound continuation state.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ContinuationRef(OpaqueExternalRef);

impl ContinuationRef {
    #[must_use]
    pub const fn from_opaque(value: OpaqueExternalRef) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ModelInvocationPurpose {
    Primary,
    ContextCompaction,
    Finalization,
    Verification,
    EvolutionEvaluation,
    EvolutionInduction,
}

/// Signed route namespace consumed by one physical model invocation.
///
/// Purpose describes why the invocation exists; it is never authorization.
/// Chat includes the primary and ordered fallback routes, while auxiliary
/// roles bind to the corresponding V4 Execution Grant entry.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ModelInvocationRouteRole {
    Chat,
    Auxiliary(AuxiliaryModelRole),
}

impl ModelInvocationRouteRole {
    #[must_use]
    pub const fn canonical_code(self) -> u8 {
        match self {
            Self::Chat => 0,
            Self::Auxiliary(role) => role.canonical_code(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ModelInvocationStatus {
    Started,
    Succeeded,
    Failed,
    Cancelled,
}

/// Recovery proof implemented by the exact production Provider adapter used
/// for a physical invocation. Persisted values are pinned before dispatch;
/// replacement workers never infer this from provider or model strings.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProviderRecoveryCapability {
    ResumableStream,
    QueryableResponse,
    IdempotentRequest,
    NonResumable,
}

impl ModelInvocationStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Started)
    }
}

/// Physical token fact. Pricing and account charging remain Backend concerns.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub struct ModelUsage {
    pub input_tokens_uncached: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub provider_cache_read_tokens: u64,
    pub provider_cache_write_tokens: u64,
    pub provider_cache_write_1h_tokens: u64,
    pub total_tokens: Option<u64>,
}

/// Canonical `usage.recorded` payload from exactly one Provider invocation.
///
/// Ledger identities and observation time deliberately do not live here. They
/// are attached by `AttributedModelUsageFact`, so this payload stays identical
/// to the field-level domain contract and cannot become a second envelope.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UsageRecorded {
    pub invocation_id: InvocationId,
    pub purpose: ModelInvocationPurpose,
    pub model_route_ref: ModelRouteRef,
    pub credential_grant_ref: CredentialGrantRef,
    pub requested_model_key: ModelKey,
    pub response_model_key: ModelKey,
    pub pricing_version_ref: PricingVersionRef,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub provider_cache_read_tokens: u64,
    pub provider_cache_write_tokens: u64,
    pub provider_cache_write_1h_tokens: u64,
    pub cancelled: bool,
    pub metering_dedup_key: IdempotencyKey,
}

/// Read-only representation of the pre-F1 `UsageRecorded` wire payload.
///
/// The historical payload did not retain credential-grant identity, Provider
/// response-model identity, one-hour cache writes, or cancellation. It must
/// therefore never be promoted to [`UsageRecorded`] or used for a new Ledger
/// append. The transport decoder is the only production caller allowed to
/// construct this type; replay consumers may inspect it and re-encode the
/// original wire semantics without mutating the immutable Ledger record.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LegacyUsageRecordedV1 {
    invocation_id: InvocationId,
    purpose: ModelInvocationPurpose,
    model_route_ref: ModelRouteRef,
    usage: ModelUsage,
    metering_dedup_key: IdempotencyKey,
    resolved_model_key: ModelKey,
    pricing_version_ref: PricingVersionRef,
}

impl LegacyUsageRecordedV1 {
    /// Constructs a decode-only historical value from an exact pre-F1 wire
    /// payload. New model execution code must construct [`UsageRecorded`].
    #[doc(hidden)]
    #[must_use]
    pub const fn from_pre_f1_wire(
        invocation_id: InvocationId,
        purpose: ModelInvocationPurpose,
        model_route_ref: ModelRouteRef,
        usage: ModelUsage,
        metering_dedup_key: IdempotencyKey,
        resolved_model_key: ModelKey,
        pricing_version_ref: PricingVersionRef,
    ) -> Self {
        Self {
            invocation_id,
            purpose,
            model_route_ref,
            usage,
            metering_dedup_key,
            resolved_model_key,
            pricing_version_ref,
        }
    }

    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    #[must_use]
    pub const fn purpose(&self) -> ModelInvocationPurpose {
        self.purpose
    }

    #[must_use]
    pub fn model_route_ref(&self) -> &ModelRouteRef {
        &self.model_route_ref
    }

    #[must_use]
    pub const fn input_tokens(&self) -> u64 {
        self.usage.input_tokens_uncached
    }

    #[must_use]
    pub const fn output_tokens(&self) -> u64 {
        self.usage.output_tokens
    }

    #[must_use]
    pub const fn reasoning_tokens(&self) -> u64 {
        self.usage.reasoning_tokens
    }

    #[must_use]
    pub const fn provider_cache_read_tokens(&self) -> u64 {
        self.usage.provider_cache_read_tokens
    }

    #[must_use]
    pub const fn provider_cache_write_tokens(&self) -> u64 {
        self.usage.provider_cache_write_tokens
    }

    #[must_use]
    pub fn metering_dedup_key(&self) -> &IdempotencyKey {
        &self.metering_dedup_key
    }

    #[must_use]
    pub fn resolved_model_key(&self) -> &ModelKey {
        &self.resolved_model_key
    }

    #[must_use]
    pub fn pricing_version_ref(&self) -> &PricingVersionRef {
        &self.pricing_version_ref
    }

    #[must_use]
    pub const fn total_tokens(&self) -> Option<u64> {
        self.usage.total_tokens
    }
}

impl UsageRecorded {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.provider_cache_write_1h_tokens > self.provider_cache_write_tokens {
            return Err(ModelContractError::CacheWriteOneHourExceedsTotal);
        }
        Ok(())
    }

    #[must_use]
    pub const fn usage(&self, provider_total_tokens: Option<u64>) -> ModelUsage {
        ModelUsage {
            input_tokens_uncached: self.input_tokens,
            output_tokens: self.output_tokens,
            reasoning_tokens: self.reasoning_tokens,
            provider_cache_read_tokens: self.provider_cache_read_tokens,
            provider_cache_write_tokens: self.provider_cache_write_tokens,
            provider_cache_write_1h_tokens: self.provider_cache_write_1h_tokens,
            total_tokens: provider_total_tokens,
        }
    }
}

/// Ledger attribution for one canonical Usage payload. These fields are
/// storage/transport envelope data, not part of `usage.recorded` itself.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ModelUsageAttribution {
    /// Immediate parent; absent on the root Run.
    pub parent_run_id: Option<RunId>,
    /// Host billing identity inherited unchanged by every descendant.
    pub billing_subject: musteragent_identity::PrincipalId,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ModelUsageEnvelope {
    /// Absent only on historical facts written before lineage publication.
    pub attribution: Option<ModelUsageAttribution>,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub observed_at: UtcTimestamp,
}

/// Persistence and Billing-Outbox representation. There remains exactly one
/// canonical usage payload (`usage`); the envelope only makes attribution
/// self-contained for adapters that cannot join Runtime-owned tables.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AttributedModelUsageFact {
    pub envelope: ModelUsageEnvelope,
    pub usage: UsageRecorded,
    /// Provider-reported aggregate retained for protocol validation only. It
    /// is not a field of the canonical `usage.recorded` domain payload.
    pub provider_total_tokens: Option<u64>,
}

impl AttributedModelUsageFact {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        self.usage.validate()?;
        self.usage.usage(self.provider_total_tokens).validate()
    }
}

/// Already encoded public metering payload staged in the same transaction as
/// the usage fact. Encoding belongs to the protocol adapter, not PostgreSQL.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MeteringPublication {
    pub message_id: OutboxMessageId,
    pub payload: Vec<u8>,
    pub payload_hash: Sha256,
}

impl MeteringPublication {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.payload.is_empty() || Sha256::digest(&self.payload) != self.payload_hash {
            return Err(ModelContractError::MeteringPublicationInvalid);
        }
        Ok(())
    }
}

/// Protocol-owned encoder for the public metering Outbox contract.
///
/// Persistence injects this port so a database adapter never constructs or
/// guesses the wire representation used by Backend billing consumers.
pub trait MeteringPublicationEncoder: Send + Sync {
    /// Preserve all validated usage locally. Only an admission-pinned priced
    /// route produces a platform billing message; BYOK must never produce one.
    fn encode_billable_usage(
        &self,
        fact: &AttributedModelUsageFact,
        message_id: OutboxMessageId,
    ) -> Result<Option<MeteringPublication>, ModelContractError> {
        fact.validate()?;
        if fact.usage.pricing_version_ref.requires_platform_metering() {
            self.encode(fact, message_id).map(Some)
        } else {
            Ok(None)
        }
    }

    fn encode(
        &self,
        fact: &AttributedModelUsageFact,
        message_id: OutboxMessageId,
    ) -> Result<MeteringPublication, ModelContractError>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompleteModelInvocation {
    /// A determinate physical-provider rejection before any model output.
    /// Missing or local failures cannot authorize an auxiliary route change.
    pub early_provider_cause: Option<ProviderErrorCode>,
    /// Provider lower bound, committed atomically with a retryable failure.
    pub retry_not_before: Option<UtcTimestamp>,
    pub invocation: ModelInvocation,
    pub usage_fact: Option<AttributedModelUsageFact>,
    pub metering_publication: Option<MeteringPublication>,
}

impl CompleteModelInvocation {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        self.invocation.validate()?;
        if let Some(cause) = self.early_provider_cause
            && (!cause.permits_alternate_provider()
                || self.invocation.status != ModelInvocationStatus::Failed
                || !matches!(
                    self.invocation.route_role,
                    ModelInvocationRouteRole::Auxiliary(
                        musteragent_model::AuxiliaryModelRole::Compression
                            | musteragent_model::AuxiliaryModelRole::Vision
                    )
                )
                || self
                    .invocation
                    .error
                    .as_ref()
                    .is_none_or(|error| error.code != cause.runtime_code()))
        {
            return Err(ModelContractError::InvalidEarlyProviderCause);
        }
        if let Some(due) = self.retry_not_before
            && (self.invocation.status != ModelInvocationStatus::Failed
                || !self
                    .invocation
                    .error
                    .as_ref()
                    .is_some_and(|error| error.retryable)
                || self
                    .invocation
                    .finished_at
                    .is_none_or(|finished| due < finished))
        {
            return Err(ModelContractError::TerminalTimestampMismatch);
        }
        if !self.invocation.status.is_terminal() {
            return Err(ModelContractError::InvocationNotTerminal);
        }
        if self.usage_fact.is_some() != self.invocation.usage.is_some()
            || self.metering_publication.is_some()
                != (self.usage_fact.is_some()
                    && self
                        .invocation
                        .pricing_version_ref
                        .requires_platform_metering())
        {
            return Err(ModelContractError::UsageFactPublicationMismatch);
        }
        if let Some(fact) = &self.usage_fact {
            fact.validate()?;
            let usage = &fact.usage;
            if usage.invocation_id != self.invocation.invocation_id
                || fact.envelope.tenant_id != self.invocation.tenant_id
                || fact.envelope.conversation_id != self.invocation.conversation_id
                || fact.envelope.run_id != self.invocation.run_id
                || fact.envelope.attempt_id != self.invocation.attempt_id
                || usage.purpose != self.invocation.purpose
                || usage.model_route_ref != self.invocation.model_route_ref
                || usage.credential_grant_ref != self.invocation.credential_grant_ref
                || usage.requested_model_key != self.invocation.requested_model_key
                || Some(&usage.response_model_key) != self.invocation.response_model_key.as_ref()
                || usage.pricing_version_ref != self.invocation.pricing_version_ref
                || usage.cancelled != (self.invocation.status == ModelInvocationStatus::Cancelled)
                || Some(usage.usage(fact.provider_total_tokens)) != self.invocation.usage
            {
                return Err(ModelContractError::UsageFactIdentityMismatch);
            }
        }
        if let Some(publication) = &self.metering_publication {
            publication.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ModelPersistenceDisposition {
    Applied,
    Duplicate,
}

/// One physical invocation together with the complete admission-pinned route
/// that authorizes it. Device persistence needs the full signed route to stage
/// restart-stable credential authority atomically with the invocation journal;
/// a route reference or Registry lookup is not sufficient authority.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmittedModelInvocation {
    /// Stable enclosing Tool identity across replacement Attempts.
    pub parent_invocation_id: Option<InvocationId>,
    pub invocation: ModelInvocation,
    pub route: ModelRoute,
}

impl AdmittedModelInvocation {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        self.invocation.validate()?;
        self.route
            .validate()
            .map_err(|_| ModelContractError::AdmissionRouteInvalid)?;
        let route_hash = SignedModelRouteHash::digest(&self.route)
            .map_err(|_| ModelContractError::AdmissionRouteInvalid)?;
        if self.invocation.model_route_ref.as_str() != self.route.route_ref.as_str()
            || self.invocation.route_identity_hash != route_hash
            || self.invocation.requested_model_key.as_str() != self.route.resolved_model_key
            || self.invocation.pricing_version_ref != self.route.pricing_version_ref
        {
            return Err(ModelContractError::AdmissionRouteMismatch);
        }
        Ok(())
    }
}

/// Consumer-owned durability port. Terminal invocation, usage fact and
/// metering Outbox message commit atomically.
pub trait ModelInvocationStore: Send + Sync {
    type Error: Send;

    fn begin(
        &self,
        invocation: ModelInvocation,
    ) -> ApplicationFuture<'_, Result<ModelPersistenceDisposition, Self::Error>>;

    fn complete(
        &self,
        completion: CompleteModelInvocation,
    ) -> ApplicationFuture<'_, Result<ModelPersistenceDisposition, Self::Error>>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ModelRecoveryCooldown {
    pub early_provider_cause: Option<ProviderErrorCode>,
    pub code: musteragent_runtime_contract::ErrorCode,
    pub not_before: UtcTimestamp,
}

/// Persistence port for invocations that must bind credential authorization
/// to an admission-pinned route before Provider dispatch.
pub trait AdmittedModelInvocationStore: Send + Sync {
    type Error: Send;

    /// Authoritative billing lineage for newly recorded usage; no inferred or
    /// caller-selected billing identity is accepted.
    fn usage_attribution(
        &self,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<ModelUsageAttribution, Self::Error>>;

    /// Unsupported stores reject the new contract before provider dispatch.
    /// Implementations must atomically admit the original model row, signed
    /// credential authority and its original Home liveness checkpoint.
    fn begin_supervised(
        &self,
        _admission: SupervisedModelAdmission,
    ) -> ApplicationFuture<'_, Result<SupervisedModelStart, SupervisedModelStoreError<Self::Error>>>
    {
        Box::pin(async { Err(SupervisedModelStoreError::Unsupported) })
    }

    fn complete_supervised(
        &self,
        _completion: SupervisedModelCompletion,
    ) -> ApplicationFuture<
        '_,
        Result<ModelPersistenceDisposition, SupervisedModelStoreError<Self::Error>>,
    > {
        Box::pin(async { Err(SupervisedModelStoreError::Unsupported) })
    }

    /// Storage owns the distinction between temporary unavailability,
    /// stale execution authority and invalid durable state.
    fn failure_code(error: &Self::Error) -> musteragent_runtime_contract::ErrorCode;

    /// Reads the unexpired cooldown of this logical Tool on its pinned route.
    /// No invocation is admitted and no provider work occurs while deferred.
    fn recovery_cooldown(
        &self,
        admission: AdmittedModelInvocation,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Option<ModelRecoveryCooldown>, Self::Error>>;

    fn begin_admitted(
        &self,
        admission: AdmittedModelInvocation,
    ) -> ApplicationFuture<'_, Result<ModelPersistenceDisposition, Self::Error>>;

    fn complete(
        &self,
        completion: CompleteModelInvocation,
    ) -> ApplicationFuture<'_, Result<ModelPersistenceDisposition, Self::Error>>;
}

impl ModelUsage {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if let Some(total) = self.total_tokens {
            // Reasoning is commonly a detail of output_tokens, while cache
            // read/write counters are provider-specific details of input
            // accounting. Adding every detail would double count otherwise
            // valid responses. The two non-overlapping physical dimensions
            // are the portable lower bound.
            let portable_lower_bound = self
                .input_tokens_uncached
                .saturating_add(self.output_tokens);
            if total < portable_lower_bound {
                return Err(ModelContractError::UsageTotalBelowComponents);
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn dominates(self, earlier: Self) -> bool {
        self.input_tokens_uncached >= earlier.input_tokens_uncached
            && self.output_tokens >= earlier.output_tokens
            && self.reasoning_tokens >= earlier.reasoning_tokens
            && self.provider_cache_read_tokens >= earlier.provider_cache_read_tokens
            && self.provider_cache_write_tokens >= earlier.provider_cache_write_tokens
            && self.provider_cache_write_1h_tokens >= earlier.provider_cache_write_1h_tokens
    }
}

/// One physical provider invocation, never duplicated by the Python Backend.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModelInvocation {
    pub invocation_id: InvocationId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub purpose: ModelInvocationPurpose,
    pub route_role: ModelInvocationRouteRole,
    pub status: ModelInvocationStatus,
    pub model_route_ref: ModelRouteRef,
    pub route_identity_hash: SignedModelRouteHash,
    pub credential_grant_ref: CredentialGrantRef,
    pub requested_model_key: ModelKey,
    pub response_model_key: Option<ModelKey>,
    pub pricing_version_ref: PricingVersionRef,
    pub idempotency_key: IdempotencyKey,
    pub request_ref: ContentRef,
    pub request_hash: Sha256,
    pub response_ref: Option<ContentRef>,
    pub provider_request_ref: Option<ProviderRequestRef>,
    pub continuation_input_ref: Option<ContinuationRef>,
    pub continuation_output_ref: Option<ContinuationRef>,
    pub usage: Option<ModelUsage>,
    pub started_at: UtcTimestamp,
    pub finished_at: Option<UtcTimestamp>,
    pub error: Option<ErrorInfo>,
}

impl ModelInvocation {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.request_ref.sha256 != self.request_hash {
            return Err(ModelContractError::RequestHashMismatch);
        }
        if self.status.is_terminal() != self.finished_at.is_some() {
            return Err(ModelContractError::TerminalTimestampMismatch);
        }
        match self.status {
            ModelInvocationStatus::Started
                if self.response_ref.is_some()
                    || self.response_model_key.is_some()
                    || self.usage.is_some()
                    || self.error.is_some() =>
            {
                Err(ModelContractError::StartedContainsOutcome)
            }
            ModelInvocationStatus::Succeeded
                if self.response_ref.is_none()
                    || self.response_model_key.is_none()
                    || self.usage.is_none()
                    || self.error.is_some() =>
            {
                Err(ModelContractError::SucceededOutcomeIncomplete)
            }
            ModelInvocationStatus::Failed if self.error.is_none() => {
                Err(ModelContractError::FailedWithoutError)
            }
            ModelInvocationStatus::Cancelled if self.error.is_some() => {
                Err(ModelContractError::CancelledContainsFailure)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ModelEvent {
    InvocationStarted {
        invocation_id: InvocationId,
        purpose: ModelInvocationPurpose,
        model_route_ref: ModelRouteRef,
        request_hash: Sha256,
    },
    InvocationCompleted {
        invocation_id: InvocationId,
        response_ref: ContentRef,
        continuation_ref: Option<ContinuationRef>,
    },
    InvocationFailed {
        invocation_id: InvocationId,
        error: ErrorInfo,
        retry_disposition: RetryDisposition,
    },
    InvocationCancelled {
        invocation_id: InvocationId,
    },
    UsageRecorded(UsageRecorded),
    /// Decode-only pre-F1 payload retained for immutable Ledger replay.
    LegacyUsageRecordedV1(LegacyUsageRecordedV1),
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ModelContractError {
    #[error("an early auxiliary failure must match the terminal provider rejection")]
    InvalidEarlyProviderCause,
    #[error("model request hash does not match its ContentRef")]
    RequestHashMismatch,
    #[error("finished_at must be present exactly for terminal model invocations")]
    TerminalTimestampMismatch,
    #[error("started model invocation already contains outcome fields")]
    StartedContainsOutcome,
    #[error("successful model invocation requires response and usage without an error")]
    SucceededOutcomeIncomplete,
    #[error("failed model invocation requires a safe error")]
    FailedWithoutError,
    #[error("cancelled model invocation cannot claim a failure")]
    CancelledContainsFailure,
    #[error("Provider usage total is below the sum of reported components")]
    UsageTotalBelowComponents,
    #[error("one-hour cache-write tokens cannot exceed total cache-write tokens")]
    CacheWriteOneHourExceedsTotal,
    #[error("metering Outbox payload is empty or its digest does not match")]
    MeteringPublicationInvalid,
    #[error("model invocation completion is not terminal")]
    InvocationNotTerminal,
    #[error("usage fact, invocation usage and metering publication must be present together")]
    UsageFactPublicationMismatch,
    #[error("usage fact identity does not match its model invocation")]
    UsageFactIdentityMismatch,
    #[error("admission-pinned model route is invalid")]
    AdmissionRouteInvalid,
    #[error("model invocation does not match its admission-pinned route")]
    AdmissionRouteMismatch,
}

#[cfg(test)]
mod tests {
    use super::MeteringPublicationEncoder;
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        AttemptId, ContentId, IdempotencyKey, InvocationId, OutboxMessageId, RunId, UtcTimestamp,
        VersionedKey,
    };
    use musteragent_model::{
        CredentialGrantRef, ModelKey, PricingVersionRef, SignedModelRouteHash,
    };

    use super::{
        AttributedModelUsageFact, CompleteModelInvocation, MeteringPublication, ModelContractError,
        ModelInvocation, ModelInvocationPurpose, ModelInvocationRouteRole, ModelInvocationStatus,
        ModelRouteRef, ModelText, ModelUsage, ModelUsageEnvelope, UsageRecorded,
    };

    fn request_ref() -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("media type"),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        }
    }

    #[test]
    fn chunked_model_text_preserves_large_utf8_message_exactly() {
        let input = format!("{}{}", "上下文".repeat(140_000), "tail".repeat(80_000));
        assert!(input.len() > 1_048_576);

        let chunks = ModelText::chunked(input.clone()).expect("chunk large UTF-8 text");

        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| chunk.as_str().len() <= 1_048_576));
        assert_eq!(
            chunks.iter().map(ModelText::as_str).collect::<String>(),
            input
        );
    }

    #[test]
    fn chunked_model_text_retains_validation_contract() {
        assert!(ModelText::chunked("").is_err());
        assert!(ModelText::chunked("valid-prefix\u{0000}invalid").is_err());
    }

    #[test]
    fn started_invocation_cannot_claim_terminal_outputs() {
        let request_ref = request_ref();
        let invocation = ModelInvocation {
            invocation_id: InvocationId::generate(),
            tenant_id: musteragent_identity::TenantId::generate(),
            conversation_id: musteragent_identity::ConversationId::generate(),
            run_id: RunId::generate(),
            attempt_id: AttemptId::generate(),
            purpose: ModelInvocationPurpose::Primary,
            route_role: ModelInvocationRouteRole::Chat,
            status: ModelInvocationStatus::Started,
            model_route_ref: ModelRouteRef::from_key(
                VersionedKey::new("default.chat@1").expect("route key"),
            ),
            route_identity_hash: SignedModelRouteHash::from_bytes([7_u8; 32]).expect("route hash"),
            credential_grant_ref: CredentialGrantRef::new("grant_model_test").expect("grant"),
            requested_model_key: ModelKey::new("model-fixture").expect("model key"),
            response_model_key: None,
            pricing_version_ref: PricingVersionRef::new("catalog:pricing.fixture@1")
                .expect("pricing key"),
            idempotency_key: IdempotencyKey::new("model:test").expect("idempotency"),
            request_hash: request_ref.sha256,
            request_ref,
            response_ref: None,
            provider_request_ref: None,
            continuation_input_ref: None,
            continuation_output_ref: None,
            usage: None,
            started_at: UtcTimestamp::from_unix_millis(1),
            finished_at: Some(UtcTimestamp::from_unix_millis(2)),
            error: None,
        };
        assert_eq!(
            invocation.validate(),
            Err(ModelContractError::TerminalTimestampMismatch)
        );
    }

    #[test]
    fn byok_compression_retains_usage_and_rejects_platform_billing() {
        let mut completion = metered_cancelled_completion();
        completion.invocation.purpose = ModelInvocationPurpose::ContextCompaction;
        completion.invocation.route_role =
            ModelInvocationRouteRole::Auxiliary(musteragent_model::AuxiliaryModelRole::Compression);
        completion.invocation.pricing_version_ref =
            PricingVersionRef::new(musteragent_model::DEVICE_DIRECT_PRICING_VERSION_REF)
                .expect("BYOK pricing");
        let fact = completion.usage_fact.as_mut().expect("usage");
        fact.usage.purpose = ModelInvocationPurpose::ContextCompaction;
        fact.usage.pricing_version_ref = completion.invocation.pricing_version_ref.clone();
        assert_eq!(
            completion.validate(),
            Err(ModelContractError::UsageFactPublicationMismatch)
        );
        completion.metering_publication = None;
        assert_eq!(completion.validate(), Ok(()));
        completion.invocation.pricing_version_ref =
            PricingVersionRef::new("catalog:priced.fixture@1").expect("priced route");
        completion
            .usage_fact
            .as_mut()
            .expect("usage")
            .usage
            .pricing_version_ref = completion.invocation.pricing_version_ref.clone();
        assert_eq!(
            completion.validate(),
            Err(ModelContractError::UsageFactPublicationMismatch),
            "priced usage must retain its billing publication"
        );
    }

    fn metered_cancelled_completion() -> CompleteModelInvocation {
        let request_ref = request_ref();
        let invocation_id = InvocationId::generate();
        let tenant_id = musteragent_identity::TenantId::generate();
        let conversation_id = musteragent_identity::ConversationId::generate();
        let run_id = RunId::generate();
        let attempt_id = AttemptId::generate();
        let model_route_ref =
            ModelRouteRef::from_key(VersionedKey::new("default.chat@1").expect("route key"));
        let credential_grant_ref = CredentialGrantRef::new("grant_model_cancelled").expect("grant");
        let requested_model_key = ModelKey::new("model-requested").expect("model key");
        let response_model_key = ModelKey::new("model-provider-response").expect("model key");
        let pricing_version_ref =
            PricingVersionRef::new("catalog:pricing.fixture@1").expect("pricing key");
        let usage = ModelUsage {
            input_tokens_uncached: 3,
            output_tokens: 1,
            reasoning_tokens: 1,
            provider_cache_read_tokens: 2,
            provider_cache_write_tokens: 5,
            provider_cache_write_1h_tokens: 4,
            total_tokens: Some(4),
        };
        let invocation = ModelInvocation {
            invocation_id,
            tenant_id,
            conversation_id,
            run_id,
            attempt_id,
            purpose: ModelInvocationPurpose::Primary,
            route_role: ModelInvocationRouteRole::Chat,
            status: ModelInvocationStatus::Cancelled,
            model_route_ref: model_route_ref.clone(),
            route_identity_hash: SignedModelRouteHash::from_bytes([8_u8; 32]).expect("route hash"),
            credential_grant_ref: credential_grant_ref.clone(),
            requested_model_key: requested_model_key.clone(),
            response_model_key: Some(response_model_key.clone()),
            pricing_version_ref: pricing_version_ref.clone(),
            idempotency_key: IdempotencyKey::new("model:cancelled").expect("idempotency"),
            request_hash: request_ref.sha256,
            request_ref,
            response_ref: None,
            provider_request_ref: None,
            continuation_input_ref: None,
            continuation_output_ref: None,
            usage: Some(usage),
            started_at: UtcTimestamp::from_unix_millis(1),
            finished_at: Some(UtcTimestamp::from_unix_millis(2)),
            error: None,
        };
        let fact = AttributedModelUsageFact {
            envelope: ModelUsageEnvelope {
                attribution: None,
                tenant_id,
                conversation_id,
                run_id,
                attempt_id,
                observed_at: UtcTimestamp::from_unix_millis(2),
            },
            usage: UsageRecorded {
                invocation_id,
                purpose: ModelInvocationPurpose::Primary,
                model_route_ref,
                credential_grant_ref,
                requested_model_key,
                response_model_key,
                pricing_version_ref,
                input_tokens: 3,
                output_tokens: 1,
                reasoning_tokens: 1,
                provider_cache_read_tokens: 2,
                provider_cache_write_tokens: 5,
                provider_cache_write_1h_tokens: 4,
                cancelled: true,
                metering_dedup_key: IdempotencyKey::new("metering:cancelled")
                    .expect("metering idempotency"),
            },
            provider_total_tokens: Some(4),
        };
        let payload = b"metering".to_vec();
        CompleteModelInvocation {
            early_provider_cause: None,
            retry_not_before: None,
            invocation,
            usage_fact: Some(fact),
            metering_publication: Some(MeteringPublication {
                message_id: OutboxMessageId::generate(),
                payload_hash: Sha256::digest(&payload),
                payload,
            }),
        }
    }

    #[test]
    fn cancelled_invocation_may_publish_exact_usage_without_becoming_a_failure() {
        let mut completion = metered_cancelled_completion();
        assert_eq!(completion.validate(), Ok(()));

        let mut cancelled_without_usage = completion.invocation.clone();
        cancelled_without_usage.usage = None;
        cancelled_without_usage.response_model_key = None;
        assert_eq!(
            CompleteModelInvocation {
                early_provider_cause: None,
                retry_not_before: None,
                invocation: cancelled_without_usage,
                usage_fact: None,
                metering_publication: None,
            }
            .validate(),
            Ok(())
        );

        completion
            .usage_fact
            .as_mut()
            .expect("usage fact")
            .usage
            .cancelled = false;
        assert_eq!(
            completion.validate(),
            Err(ModelContractError::UsageFactIdentityMismatch)
        );
    }

    #[test]
    fn completion_publication_matrix_preserves_byok_usage_without_charging() {
        for direct in [false, true] {
            for usage in [false, true] {
                for fact in [false, true] {
                    for publication in [false, true] {
                        let mut completion = metered_cancelled_completion();
                        if direct {
                            let pricing = PricingVersionRef::new(
                                musteragent_execution_grant::DEVICE_DIRECT_PRICING_VERSION_REF,
                            )
                            .expect("direct pricing");
                            completion.invocation.pricing_version_ref = pricing.clone();
                            completion
                                .usage_fact
                                .as_mut()
                                .expect("fact")
                                .usage
                                .pricing_version_ref = pricing;
                        }
                        if !usage {
                            completion.invocation.usage = None;
                            completion.invocation.response_model_key = None;
                        }
                        if !fact {
                            completion.usage_fact = None;
                        }
                        if !publication {
                            completion.metering_publication = None;
                        }
                        let expected = usage == fact && publication == (fact && !direct);
                        assert_eq!(
                            completion.validate().is_ok(),
                            expected,
                            "direct={direct} usage={usage} fact={fact} publication={publication}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn unmetered_completion_still_requires_exact_usage_and_signed_route_identity() {
        let mut completion = metered_cancelled_completion();
        completion.invocation.pricing_version_ref =
            PricingVersionRef::new(musteragent_execution_grant::DEVICE_DIRECT_PRICING_VERSION_REF)
                .expect("direct pricing");
        completion.metering_publication = None;
        assert_eq!(
            completion.validate(),
            Err(ModelContractError::UsageFactIdentityMismatch)
        );
        completion
            .usage_fact
            .as_mut()
            .expect("fact")
            .usage
            .pricing_version_ref = completion.invocation.pricing_version_ref.clone();
        assert!(completion.validate().is_ok());
        completion
            .usage_fact
            .as_mut()
            .expect("fact")
            .envelope
            .run_id = RunId::generate();
        assert_eq!(
            completion.validate(),
            Err(ModelContractError::UsageFactIdentityMismatch)
        );
    }

    #[test]
    fn byok_skips_the_encoder_but_never_skips_usage_validation() {
        struct RejectEncoding(std::sync::atomic::AtomicUsize);
        impl MeteringPublicationEncoder for RejectEncoding {
            fn encode(
                &self,
                _: &AttributedModelUsageFact,
                _: OutboxMessageId,
            ) -> Result<MeteringPublication, ModelContractError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(ModelContractError::MeteringPublicationInvalid)
            }
        }
        let encoder = RejectEncoding(std::sync::atomic::AtomicUsize::new(0));
        let mut fact = metered_cancelled_completion().usage_fact.expect("fact");
        assert!(
            encoder
                .encode_billable_usage(&fact, OutboxMessageId::generate())
                .is_err()
        );
        assert_eq!(encoder.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        fact.usage.pricing_version_ref =
            PricingVersionRef::new(musteragent_execution_grant::DEVICE_DIRECT_PRICING_VERSION_REF)
                .expect("direct pricing");
        assert_eq!(
            encoder.encode_billable_usage(&fact, OutboxMessageId::generate()),
            Ok(None)
        );
        assert_eq!(encoder.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        fact.provider_total_tokens = Some(0);
        assert!(
            encoder
                .encode_billable_usage(&fact, OutboxMessageId::generate())
                .is_err()
        );
        assert_eq!(encoder.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}

mod decision;
pub use decision::*;

mod supervised_invocation;
pub use supervised_invocation::{
    SupervisedModelAdmission, SupervisedModelCompletion, SupervisedModelStart,
    SupervisedModelStoreError,
};

mod credential_demand;
pub use credential_demand::{CredentialDemand, RefreshFailure};
