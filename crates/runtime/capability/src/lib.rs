//! Capability admission, registry and execution ports shared by the cloud and
//! device runtimes.

#![forbid(unsafe_code)]

pub mod approval_prompt;
mod browser_provider;
mod builtin_time;
mod consent;
mod input_provenance;
pub use consent::{ToolApprovalPosture, ToolConsentAuthority};
pub use input_provenance::dialogue_input_provenance;
mod execution;
mod original_tool_receipt;
pub use original_tool_receipt::{
    OriginalToolReceiptQuery, OriginalToolReceiptReader, original_tool_receipt_identity,
};
mod operation_content;
mod physical;
mod private_tool;
pub use operation_content::OperationContentStore;
pub use physical::{
    CapabilityExecution, CooperativeExecution, OperationContext, OperationDependency,
    validate_capability_execution_binding,
};
pub use private_tool::{PrivateToolRequest, PrivateToolRuntime};
mod auxiliary_execution;
mod model_usage;
mod remote_content;
mod vision;
mod vision_journal;
mod vision_model;

pub use browser_provider::{
    BrowserCandidateSettlement, BrowserExecutionProgress, BrowserGatewayAdmission,
    BrowserGatewayStore, BrowserJobDelivery, BrowserJobDeliveryState, BrowserJobQueue,
    BrowserJobTerminalReceipt, BrowserLeaseRegistration, BrowserLeaseRegistry,
    BrowserProviderRuntimeError, BrowserProviderScope, DurableBrowserJob,
    MAX_BROWSER_LEASE_TTL_MILLIS,
};
pub use builtin_time::BuiltinTimeCapability;
pub use execution::{
    ApprovalRequest, AuthorizationReceipt, AuthorizationReceiptQuery, DurableToolInvocation,
    DurableToolObservation, FencedToolInvocationJournal, InputInteractionRequest,
    InteractionResolution, InvocationPolicyBasis, ReconciliationWaitRequest, RuntimeClock,
    TaskProvenanceRecord, TaskProvenanceSourceKind, ToolExecutionMaterial,
    ToolExecutionMaterialSource, ToolExecutionPortError, ToolExecutionStart, ToolStartMode,
};
mod tool_journal_command;
pub use musteragent_capability::{BrowserLeaseProof, CapabilityKey};
pub use remote_content::{
    FetchedImage, MAX_REMOTE_IMAGE_BYTES, RemoteImageFetchError, RemoteImageFetchRequest,
    RemoteImageFetcher,
};
pub use tool_journal_command::{ToolExposureCommit, ToolJournalCommand, ToolJournalReply};
pub use vision::{
    BuiltinVisionCapability, VisionAnalysisMode, VisionCapabilityError, VisionDetail,
    VisionExecutionMode, VisionExecutionRequest, VisionModelImage, VisionModelInvocation,
    VisionModelInvoker, VisionModelResult, active_pinned_model, model_supports_vision,
    pinned_model_supports_vision,
};
pub use vision::{VisionAdmissionBasis, validate_vision_admission};
pub use vision_journal::VisionJournalCommand;
pub use vision_model::PersistedVisionModelInvoker;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use musteragent_capability::{CapabilityGrantSnapshot, CapabilityManifest};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_execution_grant::EncodedExecutionGrantProof;
use musteragent_identity::{
    Actor, AttemptId, AttemptNumber, CapabilityProviderId, ConversationId, DottedKey,
    IdempotencyKey, InvocationId, RunId, TenantId, ToolCallId, UtcTimestamp, WorkspaceId,
};
use musteragent_policy::{Decision, RequiredContext};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
    TraceContext,
};
use musteragent_runtime_snapshot::execution_target_accepts_location;
use musteragent_tool::{
    BackendOperation, CapabilityExecutionTarget, ToolKey, ToolManifest, ToolRetryContract,
};
use thiserror::Error;

/// Authorization evidence evaluated immediately before one Tool invocation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InvocationAuthorization {
    pub actor_scopes: BTreeSet<DottedKey>,
    pub available_contexts: BTreeSet<RequiredContext>,
    pub policy_requires_approval: bool,
    pub approval_forbidden: bool,
    /// Tool-manifest interactive approval is not part of this surface. This
    /// does not bypass a signed Capability `RequireApproval` decision.
    pub manifest_approval_not_applicable: bool,
    pub approval_satisfied: bool,
}

/// Structural and policy authorization stays deterministic and side-effect free.
#[must_use]
pub fn authorize_invocation(
    tool: &ToolManifest,
    grant: &CapabilityGrantSnapshot,
    target: &CapabilityExecutionTarget,
    authorization: &InvocationAuthorization,
) -> Decision {
    if grant.policy_decision == musteragent_policy::Decision::Deny {
        return Decision::Deny;
    }
    let structurally_allowed = grant.key.as_str() == tool.capability_key.as_str()
        && grant.provider_id == tool.capability_provider_id
        && execution_target_accepts_location(target, grant.location)
        && grant.effect == tool.effect
        && tool.required_scopes.is_subset(&grant.allowed_scopes)
        && tool.required_scopes.is_subset(&authorization.actor_scopes)
        && tool
            .required_contexts
            .is_subset(&authorization.available_contexts);
    if !structurally_allowed {
        return Decision::Deny;
    }
    if grant.policy_decision == Decision::RequireApproval
        && authorization.manifest_approval_not_applicable
    {
        return Decision::Deny;
    }
    let approval_required = grant.policy_decision == Decision::RequireApproval
        || match tool.approval {
            musteragent_policy::ApprovalRequirement::Never => false,
            musteragent_policy::ApprovalRequirement::ByPolicy => {
                authorization.policy_requires_approval
            }
            musteragent_policy::ApprovalRequirement::Always => {
                !authorization.manifest_approval_not_applicable
            }
        };
    if approval_required && authorization.approval_forbidden {
        Decision::Deny
    } else if approval_required && !authorization.approval_satisfied {
        Decision::RequireApproval
    } else {
        Decision::Allow
    }
}

/// Immutable, content-addressed request delivered to a pinned provider.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CapabilityInvocation {
    pub actor: Actor,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub run_id: RunId,
    pub requested_by_attempt_id: AttemptId,
    pub execution_attempt_id: AttemptId,
    pub execution_attempt_number: AttemptNumber,
    pub invocation_id: InvocationId,
    pub tool_call_id: ToolCallId,
    pub tool_key: ToolKey,
    pub capability_key: CapabilityKey,
    pub capability_provider_id: CapabilityProviderId,
    pub idempotency_key: IdempotencyKey,
    pub request_ref: ContentRef,
    pub request_schema: SchemaRef,
    pub result_schema: SchemaRef,
    pub authorized_content: Vec<ContentRef>,
    pub workspace_id: Option<WorkspaceId>,
    pub timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
    pub trace_context: TraceContext,
    pub execution_grant_proof: Option<EncodedExecutionGrantProof>,
}

/// Successful provider output. Large payloads remain in the Content Store.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CapabilityResult {
    pub result_ref: ContentRef,
    pub result_schema: SchemaRef,
    pub result_hash: Sha256,
    pub provider_request_ref: Option<OpaqueExternalRef>,
}

impl CapabilityResult {
    pub fn validate(&self) -> Result<(), CapabilityRuntimeError> {
        if self.result_ref.sha256 != self.result_hash {
            return Err(CapabilityRuntimeError::ResultHashMismatch);
        }
        Ok(())
    }
}

/// Outcome from exactly one physical Capability attempt.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CapabilityOutcome {
    Succeeded(CapabilityResult),
    /// Successful capability output that also introduces immutable content
    /// into the current Run authorization set. The grant is committed with
    /// the terminal Tool event; returning a `ContentRef` in arbitrary JSON is
    /// deliberately insufficient to expand authority.
    SucceededWithAuthorizedContent {
        result: CapabilityResult,
        authorized_content: Vec<ContentRef>,
    },
    Failed {
        error: ErrorInfo,
        retry_disposition: RetryDisposition,
        /// Optional immutable structured execution receipt. This keeps a Tool
        /// failure terminal while preserving bounded stdout/stderr,
        /// diagnostics and metrics for the model and product projection.
        diagnostic_result: Option<CapabilityResult>,
    },
    Indeterminate {
        reason: SafeDisplayText,
        reconciliation_ref: OpaqueExternalRef,
        /// An observed failure remains evidence even when physical stop or
        /// external effects could not be confirmed. This never permits replay.
        observed_error: Option<ErrorInfo>,
        diagnostic_result: Option<CapabilityResult>,
    },
}

/// Request used only for providers declaring `QueryBeforeRetry`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CapabilityReconciliation {
    pub actor: Actor,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub run_id: RunId,
    /// The immutable workspace scope from the admitted Run. Reconciliation is
    /// a continuation of the original invocation and must present the same
    /// execution-grant identity as the initial dispatch.
    pub workspace_id: Option<WorkspaceId>,
    pub execution_attempt_id: AttemptId,
    pub execution_attempt_number: AttemptNumber,
    pub invocation_id: InvocationId,
    pub provider_id: CapabilityProviderId,
    pub idempotency_key: IdempotencyKey,
    pub result_schema: SchemaRef,
    pub reconciliation_ref: OpaqueExternalRef,
    pub deadline: UtcTimestamp,
    pub trace_context: TraceContext,
    pub execution_grant_proof: Option<EncodedExecutionGrantProof>,
}

impl CapabilityReconciliation {
    pub fn validate(&self) -> Result<(), CapabilityRuntimeError> {
        if self.actor.tenant_id == self.tenant_id {
            Ok(())
        } else {
            Err(CapabilityRuntimeError::ReconciliationIdentityMismatch)
        }
    }
}

/// Reconciliation result cannot silently turn an unknown effect into a retry.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ReconciliationOutcome {
    ProvenSucceeded(CapabilityResult),
    /// A Runtime-owned read completed after recovery and proved its content
    /// closure. Retain the same authorization as an initial successful read.
    ProvenSucceededWithAuthorizedContent {
        result: CapabilityResult,
        authorized_content: Vec<ContentRef>,
    },
    /// The provider proved that the accepted operation reached a terminal
    /// failure. This is distinct from a status-query transport failure.
    ProvenFailed {
        error: ErrorInfo,
        retry_disposition: RetryDisposition,
        /// Structured receipt proven by the same reconciliation authority as
        /// the terminal failure.
        diagnostic_result: Option<CapabilityResult>,
    },
    ProvenNotApplied,
    /// The status-query authority or proof contract is irrecoverably invalid.
    /// The provider operation may already exist, so this must not be reported
    /// as a proven Tool failure or authorize a redispatch.
    Unrecoverable {
        reason: SafeDisplayText,
        reconciliation_ref: OpaqueExternalRef,
    },
    StillIndeterminate {
        reason: SafeDisplayText,
        reconciliation_ref: OpaqueExternalRef,
    },
}

/// In-process provider port. Backend adapters implement this port by selecting
/// one compile-time named Tonic method, never by passing a dynamic Tool name.
pub trait CapabilityProvider: Send + Sync {
    /// The single ordinary backend method this adapter implements, when placed at Backend.
    fn backend_operation(&self) -> Option<&BackendOperation> {
        None
    }

    fn manifest(&self) -> &CapabilityManifest;

    /// Returns the provider-owned opaque identity that must be durably sealed
    /// before a physical external effect is dispatched. Providers without a
    /// reconciliation contract return `None`; Runtime must never synthesize a
    /// provider identity from its own invocation identifiers.
    fn reconciliation_identity(
        &self,
        _invocation: &CapabilityInvocation,
    ) -> Option<OpaqueExternalRef> {
        None
    }

    fn invoke(&self, invocation: CapabilityInvocation) -> ApplicationFuture<'_, CapabilityOutcome>;

    /// Creates one prepared physical operation without doing I/O. Activity-mode
    /// support is explicit; the legacy invoke port is never a fallback here.
    fn execution(
        self: Arc<Self>,
        _owner: musteragent_run::AttemptContext,
        _invocation: CapabilityInvocation,
        _binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<CapabilityExecution, CapabilityRuntimeError> {
        Err(CapabilityRuntimeError::UnsupportedExecutionTiming)
    }

    fn reconcile(
        &self,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'_, ReconciliationOutcome>;
}

/// Exact provider registration map for one Runtime process.
#[derive(Default)]
pub struct CapabilityRegistry {
    providers: BTreeMap<(CapabilityProviderId, CapabilityKey), Arc<dyn CapabilityProvider>>,
}

impl CapabilityRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        provider: Arc<dyn CapabilityProvider>,
    ) -> Result<(), CapabilityRuntimeError> {
        provider.manifest().validate()?;
        let operation = provider.backend_operation();
        if (provider.manifest().location == musteragent_capability::CapabilityLocation::Backend)
            != operation.is_some()
            || operation.is_some_and(|op| op.media_artifact().is_some())
        {
            return Err(CapabilityRuntimeError::ProviderGrantMismatch);
        }
        let key = (
            provider.manifest().provider_id,
            provider.manifest().key.clone(),
        );
        if self.providers.contains_key(&key) {
            return Err(CapabilityRuntimeError::DuplicateProvider);
        }
        self.providers.insert(key, provider);
        Ok(())
    }

    pub fn resolve(
        &self,
        grant: &CapabilityGrantSnapshot,
        target: &CapabilityExecutionTarget,
    ) -> Result<Arc<dyn CapabilityProvider>, CapabilityRuntimeError> {
        let provider = self
            .providers
            .get(&(grant.provider_id, grant.key.clone()))
            .ok_or(CapabilityRuntimeError::UnregisteredProvider)?;
        let manifest = provider.manifest();
        if provider.backend_operation() != target.backend_operation()
            || manifest.key != grant.key
            || manifest.location != grant.location
            || manifest.effect != grant.effect
            || !manifest.required_scopes.is_subset(&grant.allowed_scopes)
        {
            return Err(CapabilityRuntimeError::ProviderGrantMismatch);
        }
        Ok(Arc::clone(provider))
    }
}

/// Persistence disposition for an idempotent Tool invocation transition.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolPersistenceDisposition {
    Applied,
    Duplicate,
}

/// Terminal Tool facts committed atomically with the Tool's Ledger event.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ToolTerminalOutcome {
    Succeeded(CapabilityResult),
    SucceededWithAuthorizedContent {
        result: CapabilityResult,
        authorized_content: Vec<ContentRef>,
    },
    Failed {
        error: ErrorInfo,
        retry_disposition: RetryDisposition,
        diagnostic_result: Option<CapabilityResult>,
    },
    Indeterminate {
        reason: SafeDisplayText,
        reconciliation_ref: OpaqueExternalRef,
        observed_error: Option<ErrorInfo>,
        diagnostic_result: Option<CapabilityResult>,
    },
    Cancelled(musteragent_runtime_contract::CancelReason),
}

impl ToolTerminalOutcome {
    /// Evidence is retained and authorized even when the physical outcome is unknown.
    pub fn retained_result(&self) -> Option<&CapabilityResult> {
        match self {
            Self::Succeeded(result) | Self::SucceededWithAuthorizedContent { result, .. } => {
                Some(result)
            }
            Self::Failed {
                diagnostic_result, ..
            }
            | Self::Indeterminate {
                diagnostic_result, ..
            } => diagnostic_result.as_ref(),
            Self::Cancelled(_) => None,
        }
    }

    /// Ledger success has one representation when no extra content is granted.
    /// Both durable homes restore an empty authorization closure as Succeeded.
    pub fn succeeded(result: CapabilityResult, authorized_content: Vec<ContentRef>) -> Self {
        if authorized_content.is_empty() {
            Self::Succeeded(result)
        } else {
            Self::SucceededWithAuthorizedContent {
                result,
                authorized_content,
            }
        }
    }
}

/// Consumer-owned durability port. `begin`, `start`, and `complete` enforce the
/// closed Tool state machine and idempotency at the database boundary.
pub trait ToolInvocationStore: Send + Sync {
    type Error: Send;

    fn begin(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        invocation: musteragent_tool::ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> ApplicationFuture<'_, Result<ToolPersistenceDisposition, Self::Error>>;

    fn start(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        execution_attempt_id: AttemptId,
        started_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<ToolPersistenceDisposition, Self::Error>>;

    fn wait_for_approval(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        interaction_id: musteragent_identity::InteractionId,
    ) -> ApplicationFuture<'_, Result<ToolPersistenceDisposition, Self::Error>>;

    fn complete(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        outcome: ToolTerminalOutcome,
        finished_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<ToolPersistenceDisposition, Self::Error>>;
}

/// Capability registration or execution contract violation.
#[derive(Debug, Error)]
pub enum CapabilityRuntimeError {
    #[error("Capability does not support the admitted execution timing")]
    UnsupportedExecutionTiming,
    #[error("Capability physical execution differs from its original binding")]
    ExecutionBindingMismatch,
    #[error(transparent)]
    CapabilityContract(#[from] musteragent_capability::CapabilityContractError),
    #[error("Capability result hash differs from its ContentRef")]
    ResultHashMismatch,
    #[error("Capability provider is already registered")]
    DuplicateProvider,
    #[error("Capability provider is not registered")]
    UnregisteredProvider,
    #[error("Capability provider registration differs from the pinned grant")]
    ProviderGrantMismatch,
    #[error("Capability reconciliation identity is inconsistent")]
    ReconciliationIdentityMismatch,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use musteragent_capability::{
        CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation, CapabilityManifest,
        IdempotencyContract,
    };
    use musteragent_content_ref::{SchemaRef, Sha256};
    use musteragent_identity::{CapabilityId, CapabilityProviderId, DottedKey, VersionedKey};
    use musteragent_policy::{
        ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy,
    };
    use musteragent_tool::{
        BackendOperation, CapabilityExecutionTarget, ToolKey, ToolManifest, ToolRetryContract,
        WireName,
    };

    use super::{
        CapabilityInvocation, CapabilityOutcome, CapabilityProvider, CapabilityReconciliation,
        CapabilityRegistry, InvocationAuthorization, ReconciliationOutcome, authorize_invocation,
    };

    fn scope() -> DottedKey {
        DottedKey::new("web.search").expect("valid scope")
    }

    fn provider_id() -> CapabilityProviderId {
        CapabilityProviderId::generate()
    }

    fn manifest(provider_id: CapabilityProviderId) -> CapabilityManifest {
        CapabilityManifest {
            capability_id: CapabilityId::generate(),
            key: CapabilityKey::from_versioned_key(
                VersionedKey::new("web.search@2").expect("valid key"),
            ),
            effect: Effect::Read,
            approval: ApprovalRequirement::Never,
            location: CapabilityLocation::Backend,
            provider_id,
            timeout: Duration::from_secs(5),
            max_attempts: NonZeroU32::new(2).expect("non-zero"),
            idempotency: IdempotencyContract::ReadOnly,
            required_scopes: BTreeSet::from([scope()]),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Public,
            egress_policy: EgressPolicy::ProviderBound,
        }
    }

    fn tool(provider_id: CapabilityProviderId) -> ToolManifest {
        let hash = Sha256::digest(b"schema");
        ToolManifest {
            application: None,
            key: ToolKey::from_versioned_key(VersionedKey::new("web.search@2").expect("valid key")),
            wire_name: WireName::new("web_search").expect("valid wire name"),
            input_schema: SchemaRef {
                schema_key: VersionedKey::new("web.search.input@2").expect("valid key"),
                schema_hash: hash,
            },
            output_schema: SchemaRef {
                schema_key: VersionedKey::new("web.search.output@2").expect("valid key"),
                schema_hash: hash,
            },
            capability_key: VersionedKey::new("web.search@2").expect("valid key"),
            capability_provider_id: provider_id,
            effect: Effect::Read,
            approval: ApprovalRequirement::Never,
            execution_policy: musteragent_tool::ToolExecutionPolicy::LegacyFixed {
                timeout: Duration::from_secs(5),
            },
            max_attempts: NonZeroU32::new(2).expect("non-zero"),
            retry_contract: ToolRetryContract::ReadOnly,
            required_scopes: BTreeSet::from([scope()]),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Public,
            egress_policy: EgressPolicy::ProviderBound,
        }
    }

    struct StubProvider(CapabilityManifest, BackendOperation);

    impl StubProvider {
        fn new(manifest: CapabilityManifest, name: &str) -> Self {
            Self(
                manifest,
                BackendOperation::new(name, None).expect("fixture operation"),
            )
        }
    }

    impl CapabilityProvider for StubProvider {
        fn backend_operation(&self) -> Option<&BackendOperation> {
            Some(&self.1)
        }
        fn manifest(&self) -> &CapabilityManifest {
            &self.0
        }

        fn invoke(
            &self,
            _invocation: CapabilityInvocation,
        ) -> musteragent_runtime_contract::ApplicationFuture<'_, CapabilityOutcome> {
            Box::pin(std::future::pending())
        }

        fn reconcile(
            &self,
            _request: CapabilityReconciliation,
        ) -> musteragent_runtime_contract::ApplicationFuture<'_, ReconciliationOutcome> {
            Box::pin(std::future::pending())
        }
    }

    #[test]
    fn policy_rejects_missing_scope_and_accepts_exact_grant() {
        let provider_id = provider_id();
        let tool = tool(provider_id);
        let grant = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(
                VersionedKey::new("web.search@2").expect("valid key"),
            ),
            provider_id,
            location: CapabilityLocation::Backend,
            effect: Effect::Read,
            allowed_scopes: BTreeSet::from([scope()]),
            policy_decision: Decision::Allow,
        };
        let denied = InvocationAuthorization {
            actor_scopes: BTreeSet::new(),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: false,
        };
        assert_eq!(
            authorize_invocation(
                &tool,
                &grant,
                &CapabilityExecutionTarget::parse("Web.Search", None)
                    .expect("fixture backend operation"),
                &denied,
            ),
            Decision::Deny
        );
        let allowed = InvocationAuthorization {
            actor_scopes: BTreeSet::from([scope()]),
            ..denied
        };
        assert_eq!(
            authorize_invocation(
                &tool,
                &grant,
                &CapabilityExecutionTarget::parse("Web.Search", None)
                    .expect("fixture backend operation"),
                &allowed,
            ),
            Decision::Allow
        );
    }

    #[test]
    fn trusted_policy_cannot_bypass_tool_or_signed_capability_approval_floors() {
        let provider_id = provider_id();
        let mut tool = tool(provider_id);
        let mut grant = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(
                VersionedKey::new("web.search@2").expect("valid key"),
            ),
            provider_id,
            location: CapabilityLocation::Backend,
            effect: Effect::Read,
            allowed_scopes: BTreeSet::from([scope()]),
            policy_decision: Decision::Allow,
        };
        let trusted = InvocationAuthorization {
            actor_scopes: BTreeSet::from([scope()]),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: false,
        };
        let supervised = InvocationAuthorization {
            policy_requires_approval: true,
            ..trusted.clone()
        };
        let target = CapabilityExecutionTarget::parse("Web.Search", None)
            .expect("fixture backend operation");

        tool.approval = ApprovalRequirement::ByPolicy;
        assert_eq!(
            authorize_invocation(&tool, &grant, &target, &supervised),
            Decision::RequireApproval,
        );
        assert_eq!(
            authorize_invocation(&tool, &grant, &target, &trusted),
            Decision::Allow,
        );

        tool.approval = ApprovalRequirement::Always;
        assert_eq!(
            authorize_invocation(&tool, &grant, &target, &trusted),
            Decision::RequireApproval,
        );

        tool.approval = ApprovalRequirement::Never;
        grant.policy_decision = Decision::RequireApproval;
        assert_eq!(
            authorize_invocation(&tool, &grant, &target, &trusted),
            Decision::RequireApproval,
        );

        let deny = InvocationAuthorization {
            policy_requires_approval: true,
            approval_forbidden: true,
            ..trusted
        };
        assert_eq!(
            authorize_invocation(&tool, &grant, &target, &deny),
            Decision::Deny,
        );
    }

    #[test]
    fn service_managed_surface_never_creates_interactive_tool_approval() {
        let provider_id = provider_id();
        let mut tool = tool(provider_id);
        tool.approval = ApprovalRequirement::Always;
        let mut grant = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(
                VersionedKey::new("web.search@2").expect("valid key"),
            ),
            provider_id,
            location: CapabilityLocation::Backend,
            effect: Effect::Read,
            allowed_scopes: BTreeSet::from([scope()]),
            policy_decision: Decision::Allow,
        };
        let service_managed = InvocationAuthorization {
            actor_scopes: BTreeSet::from([scope()]),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: true,
            manifest_approval_not_applicable: true,
            approval_satisfied: false,
        };
        let target = CapabilityExecutionTarget::parse("Web.Search", None)
            .expect("fixture backend operation");

        assert_eq!(
            authorize_invocation(&tool, &grant, &target, &service_managed),
            Decision::Allow,
        );

        grant.policy_decision = Decision::RequireApproval;
        assert_eq!(
            authorize_invocation(&tool, &grant, &target, &service_managed),
            Decision::Deny,
        );
    }

    #[test]
    fn rust_runtime_authorization_accepts_cloud_or_device_but_not_backend() {
        let provider_id = provider_id();
        let tool = tool(provider_id);
        let authorization = InvocationAuthorization {
            actor_scopes: BTreeSet::from([scope()]),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: false,
        };
        let grant = |location| CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(
                VersionedKey::new("web.search@2").expect("valid key"),
            ),
            provider_id,
            location,
            effect: Effect::Read,
            allowed_scopes: BTreeSet::from([scope()]),
            policy_decision: Decision::Allow,
        };

        for location in [CapabilityLocation::Cloud, CapabilityLocation::Device] {
            assert_eq!(
                authorize_invocation(
                    &tool,
                    &grant(location),
                    &CapabilityExecutionTarget::RustRuntime,
                    &authorization,
                ),
                Decision::Allow,
            );
        }
        for location in [
            CapabilityLocation::Backend,
            CapabilityLocation::RemoteDevice,
        ] {
            assert_eq!(
                authorize_invocation(
                    &tool,
                    &grant(location),
                    &CapabilityExecutionTarget::RustRuntime,
                    &authorization,
                ),
                Decision::Deny,
            );
        }
    }

    #[test]
    fn registry_resolves_only_exact_pinned_provider() {
        let provider_id = provider_id();
        let manifest = manifest(provider_id);
        let grant = CapabilityGrantSnapshot {
            key: manifest.key.clone(),
            provider_id,
            location: manifest.location,
            effect: manifest.effect,
            allowed_scopes: manifest.required_scopes.clone(),
            policy_decision: Decision::Allow,
        };
        let mut registry = CapabilityRegistry::new();
        registry
            .register(Arc::new(StubProvider::new(manifest, "Web.Search")))
            .expect("registration succeeds");
        let target = CapabilityExecutionTarget::parse("Web.Search", None).expect("fixture");
        assert!(registry.resolve(&grant, &target).is_ok());
        let other = CapabilityExecutionTarget::parse("Web.Other", None).expect("fixture");
        assert!(registry.resolve(&grant, &other).is_err());
        assert!(
            registry
                .resolve(&grant, &CapabilityExecutionTarget::RustRuntime)
                .is_err()
        );

        let stale = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(
                VersionedKey::new("web.search@1").expect("valid old key"),
            ),
            ..grant
        };
        assert!(registry.resolve(&stale, &target).is_err());
    }

    #[test]
    fn account_application_write_needs_a_human_and_denies_unattended_execution() {
        use super::{ToolApprovalPosture, ToolConsentAuthority};
        use musteragent_policy::PermissionMode;
        use musteragent_tool::workspace::{WorkspaceReleaseCatalog, WorkspaceToolSelection};
        let catalog = WorkspaceReleaseCatalog::from_json(include_bytes!(
            "../../../../applications/reference-api/examples/workspace-releases.json"
        ))
        .unwrap();
        let release = catalog
            .resolve("workspace.example-docs", "1.0.0", &"1".repeat(64))
            .unwrap()
            .release();
        let app = WorkspaceToolSelection {
            plugin_id: release.plugin_id.clone(),
            version: release.version.clone(),
            release_sha256: release.release_sha256.clone(),
            operation_ref: release.tools[1].key.clone(),
            selection_sha256: "c".repeat(64),
        }
        .definition_from_release(release)
        .unwrap()
        .pin(provider_id());
        assert_eq!(app.approval, ApprovalRequirement::Always);
        assert_eq!(app.effect, Effect::WriteExternal);
        for interactive in [
            ToolConsentAuthority::InteractiveChat,
            ToolConsentAuthority::AccountUserInput,
        ] {
            let posture =
                ToolApprovalPosture::resolve(&app, PermissionMode::ServiceManaged, interactive);
            assert!(!posture.approval_forbidden && !posture.manifest_approval_not_applicable);
        }
        let unattended = ToolApprovalPosture::resolve(
            &app,
            PermissionMode::ServiceManaged,
            ToolConsentAuthority::ExecutionSurface,
        );
        assert!(unattended.approval_forbidden && !unattended.manifest_approval_not_applicable);
    }

    #[test]
    fn dialogue_business_consent_is_exact_and_never_changes_other_tool_postures() {
        use super::{ToolApprovalPosture, ToolConsentAuthority};
        use musteragent_policy::PermissionMode;
        let mut memory = tool(provider_id());
        memory.key =
            ToolKey::from_versioned_key(VersionedKey::new("user_memory.remember@2").unwrap());
        memory.capability_key = VersionedKey::new("user_memory.remember@2").unwrap();
        memory.approval = ApprovalRequirement::Always;
        let dialogue = ToolConsentAuthority::AccountUserInput;
        let surface = ToolConsentAuthority::ExecutionSurface;
        assert_eq!(
            ToolApprovalPosture::resolve(&memory, PermissionMode::ServiceManaged, dialogue),
            ToolApprovalPosture {
                approval_forbidden: false,
                manifest_approval_not_applicable: false,
            }
        );
        assert!(
            ToolApprovalPosture::resolve(&memory, PermissionMode::Deny, dialogue)
                .approval_forbidden
        );
        let ordinary =
            ToolApprovalPosture::resolve(&memory, PermissionMode::ServiceManaged, surface);
        assert!(ordinary.approval_forbidden && !ordinary.manifest_approval_not_applicable);
        let environmental_only = ToolApprovalPosture {
            approval_forbidden: true,
            manifest_approval_not_applicable: true,
        };
        for key in [
            "user_memory.remember@1",
            "user_memory.remember@3",
            "resource.file.create@1",
            "interaction.request@2",
            "agents.create@1",
        ] {
            let mut other = memory.clone();
            other.key = ToolKey::from_versioned_key(VersionedKey::new(key).unwrap());
            other.capability_key = VersionedKey::new(key).unwrap();
            assert_eq!(
                ToolApprovalPosture::resolve(&other, PermissionMode::ServiceManaged, dialogue),
                environmental_only
            );
        }
        memory.approval = ApprovalRequirement::ByPolicy;
        assert_eq!(
            ToolApprovalPosture::resolve(&memory, PermissionMode::ServiceManaged, dialogue),
            environmental_only
        );
    }

    #[test]
    fn dialogue_publication_consent_preserves_exact_grants_and_requires_real_approval() {
        use super::{ToolApprovalPosture, ToolConsentAuthority};
        use musteragent_policy::PermissionMode;
        let mut manifest = tool(provider_id());
        manifest.key =
            ToolKey::from_versioned_key(VersionedKey::new("user_memory.remember@2").unwrap());
        manifest.capability_key = VersionedKey::new("user_memory.remember@2").unwrap();
        manifest.approval = ApprovalRequirement::Always;
        manifest.effect = Effect::WriteReversible;
        let posture = ToolApprovalPosture::resolve(
            &manifest,
            PermissionMode::ServiceManaged,
            ToolConsentAuthority::AccountUserInput,
        );
        let mut authorization = InvocationAuthorization {
            actor_scopes: manifest.required_scopes.clone(),
            available_contexts: manifest.required_contexts.clone(),
            policy_requires_approval: false,
            approval_forbidden: posture.approval_forbidden,
            manifest_approval_not_applicable: posture.manifest_approval_not_applicable,
            approval_satisfied: false,
        };
        let mut grant = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(manifest.capability_key.clone()),
            provider_id: manifest.capability_provider_id,
            location: CapabilityLocation::Cloud,
            effect: manifest.effect,
            allowed_scopes: manifest.required_scopes.clone(),
            policy_decision: Decision::Allow,
        };
        let target = CapabilityExecutionTarget::RustRuntime;
        for decision in [Decision::Allow, Decision::RequireApproval] {
            grant.policy_decision = decision;
            authorization.approval_satisfied = false;
            assert_eq!(
                authorize_invocation(&manifest, &grant, &target, &authorization),
                Decision::RequireApproval
            );
            authorization.approval_satisfied = true;
            assert_eq!(
                authorize_invocation(&manifest, &grant, &target, &authorization),
                Decision::Allow
            );
        }
        grant.policy_decision = Decision::Deny;
        assert_eq!(
            authorize_invocation(&manifest, &grant, &target, &authorization),
            Decision::Deny
        );
        grant.policy_decision = Decision::Allow;
        grant.allowed_scopes.clear();
        assert_eq!(
            authorize_invocation(&manifest, &grant, &target, &authorization),
            Decision::Deny
        );
        grant.allowed_scopes = manifest.required_scopes.clone();
        authorization.actor_scopes.clear();
        assert_eq!(
            authorize_invocation(&manifest, &grant, &target, &authorization),
            Decision::Deny
        );
        for key in ["user_memory.remember@1", "user_memory.remember@3"] {
            manifest.key = ToolKey::from_versioned_key(VersionedKey::new(key).unwrap());
            assert!(
                ToolApprovalPosture::resolve(
                    &manifest,
                    PermissionMode::ServiceManaged,
                    ToolConsentAuthority::AccountUserInput
                )
                .approval_forbidden
            );
        }
        manifest.key =
            ToolKey::from_versioned_key(VersionedKey::new("user_memory.remember@2").unwrap());
        {
            let authority = ToolConsentAuthority::ExecutionSurface;
            assert!(
                ToolApprovalPosture::resolve(&manifest, PermissionMode::ServiceManaged, authority)
                    .approval_forbidden
            );
        }
        assert!(
            ToolApprovalPosture::resolve(
                &manifest,
                PermissionMode::Deny,
                ToolConsentAuthority::AccountUserInput
            )
            .approval_forbidden
        );
    }

    #[test]
    fn registry_allows_multiple_named_capabilities_from_one_provider() {
        let provider_id = provider_id();
        let first = manifest(provider_id);
        let mut second = first.clone();
        second.key =
            CapabilityKey::from_versioned_key(VersionedKey::new("web.fetch@1").expect("valid key"));
        let second_grant = CapabilityGrantSnapshot {
            key: second.key.clone(),
            provider_id,
            location: second.location,
            effect: second.effect,
            allowed_scopes: second.required_scopes.clone(),
            policy_decision: Decision::Allow,
        };
        let mut registry = CapabilityRegistry::new();
        registry
            .register(Arc::new(StubProvider::new(first, "Web.Search")))
            .expect("first capability");
        registry
            .register(Arc::new(StubProvider::new(second, "Web.Fetch")))
            .expect("same service can expose another named capability");
        let fetch = CapabilityExecutionTarget::parse("Web.Fetch", None).expect("fixture");
        assert!(registry.resolve(&second_grant, &fetch).is_ok());
    }
}

mod decision;
pub use decision::*;

mod dialogue_tool;
pub use dialogue_tool::*;

mod dialogue_store_error;
pub use dialogue_store_error::*;

mod executor_environment;
pub use executor_environment::*;

mod dialogue_tool_publication;
pub use dialogue_tool_publication::*;

mod work_item;
pub use work_item::*;
mod work_item_evidence;
pub use work_item_evidence::require_work_item_evidence;

mod sandbox_routing;
pub use sandbox_routing::{SandboxRouteCommit, SandboxRoutingStore};
