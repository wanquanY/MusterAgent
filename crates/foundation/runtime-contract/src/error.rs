use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::CorrelationId;
use musteragent_identity::ProjectionSnapshotId;
use thiserror::Error;

use crate::{ResourceRef, SafeDisplayText};

/// The single public authority for recovering any invalid durable Ledger Cursor.
///
/// The Snapshot and Cursor are an inseparable pair: clients must install this
/// exact Snapshot and resume from its exact canonical Cursor. They must never
/// replace either member with a later independently queried value.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CursorRecoveryAuthority {
    pub snapshot_id: ProjectionSnapshotId,
    pub resume_cursor: LedgerCursor,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ErrorCategory {
    Validation,
    Authentication,
    Authorization,
    Conflict,
    NotFound,
    Policy,
    Quota,
    Dependency,
    Timeout,
    Internal,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ErrorCode {
    InvalidCommand,
    Unauthenticated,
    UnsupportedSchemaVersion,
    IdempotencyConflict,
    ConversationNotFound,
    ConversationArchived,
    ConversationEpochStale,
    ConversationSeqConflict,
    ParticipantNotAuthorized,
    AgentNotAvailable,
    RuntimeSnapshotInvalid,
    ModelRouteUnavailable,
    ModelResponseInvalid,
    /// The provider stopped without a qualified native public delivery after
    /// the admission-sealed recovery budget. No response body was parsed.
    PublicDeliveryMissing,
    ModelRequestRejected,
    ToolNotExposed,
    CapabilityUnavailable,
    /// No browser execution provider (an open Web page running the browser
    /// sandbox) is available for the routed work. Retryable: the work can run
    /// once a browser client reconnects. Hosts localize this code.
    BrowserExecutionUnavailable,
    RunNotFound,
    RunTerminal,
    RunTransitionInvalid,
    AttemptLeaseStale,
    InteractionNotFound,
    ArtifactNotFound,
    InteractionNotPending,
    InteractionExpired,
    InteractionResponseInvalid,
    ContentIntegrityFailed,
    CredentialRefreshRequired,
    ContentUnavailable,
    CursorEpochStale,
    CursorCompacted,
    /// Rolling-upgrade compatibility only. Live keyset catalogs do not emit it.
    CatalogCursorInvalidated,
    PolicyDenied,
    NativePermissionReduced,
    QuotaExceeded,
    CreditsExhausted,
    ModelNotEntitled,
    ModelPaymentRequired,

    ModelContextExceeded,
    DependencyUnavailable,
    DependencyTimeout,
    InternalInvariantViolated,
    ResourceRevisionConflict,
    ResourceNameConflict,
    ResourceMoveCycle,
    ResourceNodeNotFound,
    ResourceParentNotFound,
    ResourceFileUnavailable,
    MemoryNotFound,
    MemoryConflict,
    MemoryEvaluationPending,
    MemoryNotPublishable,
}

impl ErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidCommand => "invalid_command",
            Self::Unauthenticated => "unauthenticated",
            Self::UnsupportedSchemaVersion => "unsupported_schema_version",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::ConversationNotFound => "conversation_not_found",
            Self::ConversationArchived => "conversation_archived",
            Self::ConversationEpochStale => "conversation_epoch_stale",
            Self::ConversationSeqConflict => "conversation_seq_conflict",
            Self::ParticipantNotAuthorized => "participant_not_authorized",
            Self::AgentNotAvailable => "agent_not_available",
            Self::RuntimeSnapshotInvalid => "runtime_snapshot_invalid",
            Self::ModelRouteUnavailable => "model_route_unavailable",
            Self::ModelResponseInvalid => "model_response_invalid",
            Self::PublicDeliveryMissing => "public_delivery_missing",
            Self::ModelRequestRejected => "model_request_rejected",
            Self::ToolNotExposed => "tool_not_exposed",
            Self::CapabilityUnavailable => "capability_unavailable",
            Self::BrowserExecutionUnavailable => "browser_execution_unavailable",
            Self::RunNotFound => "run_not_found",
            Self::RunTerminal => "run_terminal",
            Self::RunTransitionInvalid => "run_transition_invalid",
            Self::AttemptLeaseStale => "attempt_lease_stale",
            Self::InteractionNotFound => "interaction_not_found",
            Self::ArtifactNotFound => "artifact_not_found",
            Self::InteractionNotPending => "interaction_not_pending",
            Self::InteractionExpired => "interaction_expired",
            Self::InteractionResponseInvalid => "interaction_response_invalid",
            Self::ContentIntegrityFailed => "content_integrity_failed",
            Self::CredentialRefreshRequired => "credential_refresh_required",
            Self::ContentUnavailable => "content_unavailable",
            Self::CursorEpochStale => "cursor_epoch_stale",
            Self::CursorCompacted => "cursor_compacted",
            Self::CatalogCursorInvalidated => "catalog_cursor_invalidated",
            Self::PolicyDenied => "policy_denied",
            Self::NativePermissionReduced => "native_permission_reduced",
            Self::QuotaExceeded => "quota_exceeded",
            Self::CreditsExhausted => "credits_exhausted",
            Self::ModelNotEntitled => "model_not_entitled",
            Self::ModelPaymentRequired => "model_payment_required",

            Self::ModelContextExceeded => "model_context_exceeded",
            Self::DependencyUnavailable => "dependency_unavailable",
            Self::DependencyTimeout => "dependency_timeout",
            Self::InternalInvariantViolated => "internal_invariant_violated",
            Self::ResourceRevisionConflict => "resource_revision_conflict",
            Self::ResourceNameConflict => "resource_name_conflict",
            Self::ResourceMoveCycle => "resource_move_cycle",
            Self::ResourceNodeNotFound => "resource_node_not_found",
            Self::ResourceParentNotFound => "resource_parent_not_found",
            Self::ResourceFileUnavailable => "resource_file_unavailable",
            Self::MemoryNotFound => "memory_not_found",
            Self::MemoryConflict => "memory_conflict",
            Self::MemoryEvaluationPending => "memory_evaluation_pending",
            Self::MemoryNotPublishable => "memory_not_publishable",
        }
    }

    #[must_use]
    pub fn from_safe_code(value: &str) -> Option<Self> {
        [
            Self::InvalidCommand,
            Self::Unauthenticated,
            Self::UnsupportedSchemaVersion,
            Self::IdempotencyConflict,
            Self::ConversationNotFound,
            Self::ConversationArchived,
            Self::ConversationEpochStale,
            Self::ConversationSeqConflict,
            Self::ParticipantNotAuthorized,
            Self::AgentNotAvailable,
            Self::RuntimeSnapshotInvalid,
            Self::ModelRouteUnavailable,
            Self::ModelResponseInvalid,
            Self::PublicDeliveryMissing,
            Self::ModelRequestRejected,
            Self::ToolNotExposed,
            Self::CapabilityUnavailable,
            Self::BrowserExecutionUnavailable,
            Self::RunNotFound,
            Self::RunTerminal,
            Self::RunTransitionInvalid,
            Self::AttemptLeaseStale,
            Self::InteractionNotFound,
            Self::ArtifactNotFound,
            Self::InteractionNotPending,
            Self::InteractionExpired,
            Self::InteractionResponseInvalid,
            Self::ContentIntegrityFailed,
            Self::CredentialRefreshRequired,
            Self::ContentUnavailable,
            Self::CursorEpochStale,
            Self::CursorCompacted,
            Self::CatalogCursorInvalidated,
            Self::PolicyDenied,
            Self::NativePermissionReduced,
            Self::QuotaExceeded,
            Self::CreditsExhausted,
            Self::ModelNotEntitled,
            Self::ModelPaymentRequired,
            Self::ModelContextExceeded,
            Self::DependencyUnavailable,
            Self::DependencyTimeout,
            Self::InternalInvariantViolated,
            Self::ResourceRevisionConflict,
            Self::ResourceNameConflict,
            Self::ResourceMoveCycle,
            Self::ResourceNodeNotFound,
            Self::ResourceParentNotFound,
            Self::ResourceFileUnavailable,
            Self::MemoryNotFound,
            Self::MemoryConflict,
            Self::MemoryEvaluationPending,
            Self::MemoryNotPublishable,
        ]
        .into_iter()
        .find(|code| code.as_str() == value)
    }

    #[must_use]
    pub const fn category(self) -> ErrorCategory {
        match self {
            Self::InvalidCommand
            | Self::UnsupportedSchemaVersion
            | Self::RuntimeSnapshotInvalid
            | Self::ToolNotExposed
            | Self::InteractionResponseInvalid
            | Self::ContentIntegrityFailed => ErrorCategory::Validation,
            Self::Unauthenticated => ErrorCategory::Authentication,
            Self::ParticipantNotAuthorized => ErrorCategory::Authorization,
            Self::IdempotencyConflict
            | Self::ConversationArchived
            | Self::ConversationEpochStale
            | Self::ConversationSeqConflict
            | Self::RunTerminal
            | Self::RunTransitionInvalid
            | Self::AttemptLeaseStale
            | Self::InteractionNotPending
            | Self::InteractionExpired
            | Self::CursorEpochStale
            | Self::CursorCompacted
            | Self::CatalogCursorInvalidated
            | Self::ResourceRevisionConflict
            | Self::ResourceNameConflict
            | Self::ResourceFileUnavailable
            | Self::MemoryConflict
            | Self::MemoryEvaluationPending
            | Self::MemoryNotPublishable => ErrorCategory::Conflict,
            Self::ConversationNotFound
            | Self::AgentNotAvailable
            | Self::RunNotFound
            | Self::InteractionNotFound
            | Self::ArtifactNotFound
            | Self::ResourceNodeNotFound
            | Self::ResourceParentNotFound
            | Self::MemoryNotFound => ErrorCategory::NotFound,
            Self::PolicyDenied | Self::ModelNotEntitled | Self::NativePermissionReduced => {
                ErrorCategory::Policy
            }
            Self::QuotaExceeded | Self::CreditsExhausted | Self::ModelPaymentRequired => {
                ErrorCategory::Quota
            }
            Self::ModelContextExceeded | Self::ResourceMoveCycle => ErrorCategory::Validation,
            Self::ModelRouteUnavailable
            | Self::ModelResponseInvalid
            | Self::PublicDeliveryMissing
            | Self::ModelRequestRejected
            | Self::CapabilityUnavailable
            | Self::BrowserExecutionUnavailable
            | Self::DependencyUnavailable
            | Self::CredentialRefreshRequired
            | Self::ContentUnavailable => ErrorCategory::Dependency,
            Self::DependencyTimeout => ErrorCategory::Timeout,
            Self::InternalInvariantViolated => ErrorCategory::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ErrorCode;

    #[test]
    fn every_persisted_safe_code_round_trips() {
        let codes = [
            ErrorCode::InvalidCommand,
            ErrorCode::Unauthenticated,
            ErrorCode::UnsupportedSchemaVersion,
            ErrorCode::IdempotencyConflict,
            ErrorCode::ConversationNotFound,
            ErrorCode::ConversationArchived,
            ErrorCode::ConversationEpochStale,
            ErrorCode::ConversationSeqConflict,
            ErrorCode::ParticipantNotAuthorized,
            ErrorCode::AgentNotAvailable,
            ErrorCode::RuntimeSnapshotInvalid,
            ErrorCode::ModelRouteUnavailable,
            ErrorCode::ModelResponseInvalid,
            ErrorCode::PublicDeliveryMissing,
            ErrorCode::ModelRequestRejected,
            ErrorCode::ToolNotExposed,
            ErrorCode::CapabilityUnavailable,
            ErrorCode::BrowserExecutionUnavailable,
            ErrorCode::RunNotFound,
            ErrorCode::RunTerminal,
            ErrorCode::RunTransitionInvalid,
            ErrorCode::AttemptLeaseStale,
            ErrorCode::InteractionNotFound,
            ErrorCode::ArtifactNotFound,
            ErrorCode::InteractionNotPending,
            ErrorCode::InteractionExpired,
            ErrorCode::InteractionResponseInvalid,
            ErrorCode::ContentIntegrityFailed,
            ErrorCode::CredentialRefreshRequired,
            ErrorCode::ContentUnavailable,
            ErrorCode::CursorEpochStale,
            ErrorCode::CursorCompacted,
            ErrorCode::CatalogCursorInvalidated,
            ErrorCode::PolicyDenied,
            ErrorCode::NativePermissionReduced,
            ErrorCode::QuotaExceeded,
            ErrorCode::ModelContextExceeded,
            ErrorCode::DependencyUnavailable,
            ErrorCode::DependencyTimeout,
            ErrorCode::InternalInvariantViolated,
            ErrorCode::ResourceRevisionConflict,
            ErrorCode::ResourceNameConflict,
            ErrorCode::ResourceMoveCycle,
            ErrorCode::ResourceNodeNotFound,
            ErrorCode::ResourceParentNotFound,
            ErrorCode::ResourceFileUnavailable,
            ErrorCode::MemoryNotFound,
            ErrorCode::MemoryConflict,
            ErrorCode::MemoryEvaluationPending,
            ErrorCode::MemoryNotPublishable,
        ];
        for code in codes {
            assert_eq!(ErrorCode::from_safe_code(code.as_str()), Some(code));
        }
        assert_eq!(ErrorCode::from_safe_code("provider-secret-detail"), None);
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpaqueDiagnosticRef(String);

impl OpaqueDiagnosticRef {
    pub fn new(value: impl Into<String>) -> Result<Self, DiagnosticRefError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 512
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(DiagnosticRefError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("diagnostic reference must be bounded visible ASCII")]
pub struct DiagnosticRefError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ErrorDetails {
    None,
    InvalidField { field: SafeDisplayText },
    Dependency { reference: OpaqueDiagnosticRef },
    Diagnostic { reference: OpaqueDiagnosticRef },
    CursorEpochStale { authority: CursorRecoveryAuthority },
    CursorCompacted { authority: CursorRecoveryAuthority },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErrorInfo {
    pub code: ErrorCode,
    pub category: ErrorCategory,
    pub safe_message: SafeDisplayText,
    pub retryable: bool,
    pub dependency_ref: Option<OpaqueDiagnosticRef>,
    pub diagnostic_ref: Option<OpaqueDiagnosticRef>,
}

impl ErrorInfo {
    pub fn validate(&self) -> Result<(), RuntimeErrorContractError> {
        if self.category != self.code.category() {
            return Err(RuntimeErrorContractError::CategoryMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeError {
    pub schema_version: musteragent_identity::DottedKey,
    pub code: ErrorCode,
    pub category: ErrorCategory,
    pub message: SafeDisplayText,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
    pub resource_ref: Option<Box<ResourceRef>>,
    pub correlation_id: CorrelationId,
    pub details: Box<ErrorDetails>,
}

impl RuntimeError {
    pub fn validate(&self) -> Result<(), RuntimeErrorContractError> {
        if !crate::is_supported_runtime_error_schema(self.schema_version.as_str()) {
            return Err(RuntimeErrorContractError::UnsupportedSchemaVersion);
        }
        if self.category != self.code.category() {
            return Err(RuntimeErrorContractError::CategoryMismatch);
        }
        if !self.retryable && self.retry_after_ms.is_some() {
            return Err(RuntimeErrorContractError::UnexpectedRetryAfter);
        }
        match (&self.code, self.details.as_ref()) {
            (ErrorCode::CursorEpochStale, ErrorDetails::CursorEpochStale { .. })
            | (ErrorCode::CursorCompacted, ErrorDetails::CursorCompacted { .. }) => Ok(()),
            (ErrorCode::CursorEpochStale | ErrorCode::CursorCompacted, _) => {
                Err(RuntimeErrorContractError::MissingCursorRecovery)
            }
            (_, ErrorDetails::CursorEpochStale { .. } | ErrorDetails::CursorCompacted { .. }) => {
                Err(RuntimeErrorContractError::UnexpectedCursorRecovery)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RuntimeErrorContractError {
    #[error("RuntimeError schema_version is unsupported")]
    UnsupportedSchemaVersion,
    #[error("RuntimeError category does not match its stable error code")]
    CategoryMismatch,
    #[error("non-retryable RuntimeError cannot contain retry_after_ms")]
    UnexpectedRetryAfter,
    #[error("cursor error requires a typed recovery detail")]
    MissingCursorRecovery,
    #[error("cursor recovery detail is invalid for this error code")]
    UnexpectedCursorRecovery,
}
