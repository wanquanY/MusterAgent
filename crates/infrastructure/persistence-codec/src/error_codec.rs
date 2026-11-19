//! Shared encoding for safe, structured Runtime failure data.
use crate::wire;
use crate::wire_common::WireContractError;
use musteragent_runtime_contract::{
    ErrorCategory, ErrorCode, ErrorInfo, OpaqueDiagnosticRef, SafeDisplayText,
};

/// Encode a safe structured failure for shared and RPC messages.
pub fn encode_error_info(value: &ErrorInfo) -> wire::ErrorInfo {
    wire::ErrorInfo {
        code: error_code(value.code) as i32,
        category: error_category(value.category) as i32,
        safe_message: value.safe_message.as_str().to_owned(),
        retryable: value.retryable,
        dependency_ref: value
            .dependency_ref
            .as_ref()
            .map(|reference| reference.as_str().to_owned()),
        diagnostic_ref: value
            .diagnostic_ref
            .as_ref()
            .map(|reference| reference.as_str().to_owned()),
    }
}

/// Encode the canonical error code without transport-specific remapping.
pub const fn error_code(value: ErrorCode) -> wire::ErrorCode {
    match value {
        ErrorCode::InvalidCommand => wire::ErrorCode::InvalidCommand,
        ErrorCode::Unauthenticated => wire::ErrorCode::Unauthenticated,
        ErrorCode::UnsupportedSchemaVersion => wire::ErrorCode::UnsupportedSchemaVersion,
        ErrorCode::IdempotencyConflict => wire::ErrorCode::IdempotencyConflict,
        ErrorCode::ConversationNotFound => wire::ErrorCode::ConversationNotFound,
        ErrorCode::ConversationArchived => wire::ErrorCode::ConversationArchived,
        ErrorCode::ConversationEpochStale => wire::ErrorCode::ConversationEpochStale,
        ErrorCode::ConversationSeqConflict => wire::ErrorCode::ConversationSeqConflict,
        ErrorCode::ParticipantNotAuthorized => wire::ErrorCode::ParticipantNotAuthorized,
        ErrorCode::AgentNotAvailable => wire::ErrorCode::AgentNotAvailable,
        ErrorCode::RuntimeSnapshotInvalid => wire::ErrorCode::RuntimeSnapshotInvalid,
        ErrorCode::ModelRouteUnavailable => wire::ErrorCode::ModelRouteUnavailable,
        ErrorCode::ModelResponseInvalid => wire::ErrorCode::ModelResponseInvalid,
        ErrorCode::PublicDeliveryMissing => wire::ErrorCode::PublicDeliveryMissing,
        ErrorCode::ModelRequestRejected => wire::ErrorCode::ModelRequestRejected,
        ErrorCode::CapabilityUnavailable => wire::ErrorCode::CapabilityUnavailable,
        ErrorCode::RunNotFound => wire::ErrorCode::RunNotFound,
        ErrorCode::RunTerminal => wire::ErrorCode::RunTerminal,
        ErrorCode::RunTransitionInvalid => wire::ErrorCode::RunTransitionInvalid,
        ErrorCode::AttemptLeaseStale => wire::ErrorCode::AttemptLeaseStale,
        ErrorCode::InteractionNotFound => wire::ErrorCode::InteractionNotFound,
        ErrorCode::ArtifactNotFound => wire::ErrorCode::ArtifactNotFound,
        ErrorCode::InteractionNotPending => wire::ErrorCode::InteractionNotPending,
        ErrorCode::InteractionExpired => wire::ErrorCode::InteractionExpired,
        ErrorCode::InteractionResponseInvalid => wire::ErrorCode::InteractionResponseInvalid,
        ErrorCode::ContentIntegrityFailed => wire::ErrorCode::ContentIntegrityFailed,
        ErrorCode::CredentialRefreshRequired => wire::ErrorCode::CredentialRefreshRequired,
        ErrorCode::ContentUnavailable => wire::ErrorCode::ContentUnavailable,
        ErrorCode::CursorEpochStale => wire::ErrorCode::CursorEpochStale,
        ErrorCode::CursorCompacted => wire::ErrorCode::CursorCompacted,
        ErrorCode::PolicyDenied => wire::ErrorCode::PolicyDenied,
        ErrorCode::NativePermissionReduced => wire::ErrorCode::NativePermissionReduced,
        ErrorCode::BrowserExecutionUnavailable => wire::ErrorCode::BrowserExecutionUnavailable,
        ErrorCode::QuotaExceeded => wire::ErrorCode::QuotaExceeded,
        ErrorCode::CreditsExhausted => wire::ErrorCode::CreditsExhausted,
        ErrorCode::ModelNotEntitled => wire::ErrorCode::ModelNotEntitled,
        ErrorCode::ModelPaymentRequired => wire::ErrorCode::ModelPaymentRequired,
        ErrorCode::MemoryNotFound => wire::ErrorCode::MemoryNotFound,
        ErrorCode::MemoryConflict => wire::ErrorCode::MemoryConflict,
        ErrorCode::MemoryEvaluationPending => wire::ErrorCode::MemoryEvaluationPending,
        ErrorCode::MemoryNotPublishable => wire::ErrorCode::MemoryNotPublishable,
        ErrorCode::ModelContextExceeded => wire::ErrorCode::ModelContextExceeded,
        ErrorCode::ToolNotExposed => wire::ErrorCode::ToolNotExposed,
        ErrorCode::DependencyUnavailable => wire::ErrorCode::DependencyUnavailable,
        ErrorCode::DependencyTimeout => wire::ErrorCode::DependencyTimeout,
        ErrorCode::InternalInvariantViolated => wire::ErrorCode::InternalInvariantViolated,
        ErrorCode::ResourceRevisionConflict => wire::ErrorCode::ResourceRevisionConflict,
        ErrorCode::ResourceNameConflict => wire::ErrorCode::ResourceNameConflict,
        ErrorCode::ResourceMoveCycle => wire::ErrorCode::ResourceMoveCycle,
        ErrorCode::ResourceNodeNotFound => wire::ErrorCode::ResourceNodeNotFound,
        ErrorCode::ResourceParentNotFound => wire::ErrorCode::ResourceParentNotFound,
        ErrorCode::ResourceFileUnavailable => wire::ErrorCode::ResourceFileUnavailable,
        ErrorCode::CatalogCursorInvalidated => wire::ErrorCode::CatalogCursorInvalidated,
    }
}

const fn error_category(value: ErrorCategory) -> wire::ErrorCategory {
    match value {
        ErrorCategory::Validation => wire::ErrorCategory::Validation,
        ErrorCategory::Authentication => wire::ErrorCategory::Authentication,
        ErrorCategory::Authorization => wire::ErrorCategory::Authorization,
        ErrorCategory::Conflict => wire::ErrorCategory::Conflict,
        ErrorCategory::NotFound => wire::ErrorCategory::NotFound,
        ErrorCategory::Policy => wire::ErrorCategory::Policy,
        ErrorCategory::Quota => wire::ErrorCategory::Quota,
        ErrorCategory::Dependency => wire::ErrorCategory::Dependency,
        ErrorCategory::Timeout => wire::ErrorCategory::Timeout,
        ErrorCategory::Internal => wire::ErrorCategory::Internal,
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
#[error("invalid Runtime error information")]
pub struct ErrorInfoDecodeError;

pub fn decode_runtime_error_info(
    value: wire::ErrorInfo,
) -> Result<ErrorInfo, ErrorInfoDecodeError> {
    decode_error_info(value).map_err(|_| ErrorInfoDecodeError)
}

/// Decode and validate safe structured failure data.
pub fn decode_error_info(value: wire::ErrorInfo) -> Result<ErrorInfo, WireContractError> {
    let code = decode_error_code(value.code)?;
    if value.category != error_category(code.category()) as i32 {
        return Err(WireContractError::InvalidContract);
    }
    Ok(ErrorInfo {
        code,
        category: code.category(),
        safe_message: SafeDisplayText::new(value.safe_message)
            .map_err(|_| WireContractError::InvalidContract)?,
        retryable: value.retryable,
        dependency_ref: value
            .dependency_ref
            .map(OpaqueDiagnosticRef::new)
            .transpose()
            .map_err(|_| WireContractError::InvalidContract)?,
        diagnostic_ref: value
            .diagnostic_ref
            .map(OpaqueDiagnosticRef::new)
            .transpose()
            .map_err(|_| WireContractError::InvalidContract)?,
    })
}

/// Reject unknown protocol error discriminants.
pub fn decode_error_code(value: i32) -> Result<ErrorCode, WireContractError> {
    Ok(match wire::ErrorCode::try_from(value) {
        Ok(wire::ErrorCode::InvalidCommand) => ErrorCode::InvalidCommand,
        Ok(wire::ErrorCode::Unauthenticated) => ErrorCode::Unauthenticated,
        Ok(wire::ErrorCode::UnsupportedSchemaVersion) => ErrorCode::UnsupportedSchemaVersion,
        Ok(wire::ErrorCode::IdempotencyConflict) => ErrorCode::IdempotencyConflict,
        Ok(wire::ErrorCode::ConversationNotFound) => ErrorCode::ConversationNotFound,
        Ok(wire::ErrorCode::ConversationArchived) => ErrorCode::ConversationArchived,
        Ok(wire::ErrorCode::ConversationEpochStale) => ErrorCode::ConversationEpochStale,
        Ok(wire::ErrorCode::ConversationSeqConflict) => ErrorCode::ConversationSeqConflict,
        Ok(wire::ErrorCode::ParticipantNotAuthorized) => ErrorCode::ParticipantNotAuthorized,
        Ok(wire::ErrorCode::AgentNotAvailable) => ErrorCode::AgentNotAvailable,
        Ok(wire::ErrorCode::RuntimeSnapshotInvalid) => ErrorCode::RuntimeSnapshotInvalid,
        Ok(wire::ErrorCode::ModelRouteUnavailable) => ErrorCode::ModelRouteUnavailable,
        Ok(wire::ErrorCode::ModelResponseInvalid) => ErrorCode::ModelResponseInvalid,
        Ok(wire::ErrorCode::PublicDeliveryMissing) => ErrorCode::PublicDeliveryMissing,
        Ok(wire::ErrorCode::ModelRequestRejected) => ErrorCode::ModelRequestRejected,
        Ok(wire::ErrorCode::CapabilityUnavailable) => ErrorCode::CapabilityUnavailable,
        Ok(wire::ErrorCode::RunNotFound) => ErrorCode::RunNotFound,
        Ok(wire::ErrorCode::RunTerminal) => ErrorCode::RunTerminal,
        Ok(wire::ErrorCode::RunTransitionInvalid) => ErrorCode::RunTransitionInvalid,
        Ok(wire::ErrorCode::AttemptLeaseStale) => ErrorCode::AttemptLeaseStale,
        Ok(wire::ErrorCode::InteractionNotFound) => ErrorCode::InteractionNotFound,
        Ok(wire::ErrorCode::ArtifactNotFound) => ErrorCode::ArtifactNotFound,
        Ok(wire::ErrorCode::InteractionNotPending) => ErrorCode::InteractionNotPending,
        Ok(wire::ErrorCode::InteractionExpired) => ErrorCode::InteractionExpired,
        Ok(wire::ErrorCode::InteractionResponseInvalid) => ErrorCode::InteractionResponseInvalid,
        Ok(wire::ErrorCode::ContentIntegrityFailed) => ErrorCode::ContentIntegrityFailed,
        Ok(wire::ErrorCode::CredentialRefreshRequired) => ErrorCode::CredentialRefreshRequired,
        Ok(wire::ErrorCode::ContentUnavailable) => ErrorCode::ContentUnavailable,
        Ok(wire::ErrorCode::CursorEpochStale) => ErrorCode::CursorEpochStale,
        Ok(wire::ErrorCode::CursorCompacted) => ErrorCode::CursorCompacted,
        Ok(wire::ErrorCode::PolicyDenied) => ErrorCode::PolicyDenied,
        Ok(wire::ErrorCode::NativePermissionReduced) => ErrorCode::NativePermissionReduced,
        Ok(wire::ErrorCode::BrowserExecutionUnavailable) => ErrorCode::BrowserExecutionUnavailable,
        Ok(wire::ErrorCode::QuotaExceeded) => ErrorCode::QuotaExceeded,
        Ok(wire::ErrorCode::CreditsExhausted) => ErrorCode::CreditsExhausted,
        Ok(wire::ErrorCode::ModelNotEntitled) => ErrorCode::ModelNotEntitled,
        Ok(wire::ErrorCode::ModelPaymentRequired) => ErrorCode::ModelPaymentRequired,
        Ok(wire::ErrorCode::MemoryNotFound) => ErrorCode::MemoryNotFound,
        Ok(wire::ErrorCode::MemoryConflict) => ErrorCode::MemoryConflict,
        Ok(wire::ErrorCode::MemoryEvaluationPending) => ErrorCode::MemoryEvaluationPending,
        Ok(wire::ErrorCode::MemoryNotPublishable) => ErrorCode::MemoryNotPublishable,
        Ok(wire::ErrorCode::ModelContextExceeded) => ErrorCode::ModelContextExceeded,
        Ok(wire::ErrorCode::ToolNotExposed) => ErrorCode::ToolNotExposed,
        Ok(wire::ErrorCode::DependencyUnavailable) => ErrorCode::DependencyUnavailable,
        Ok(wire::ErrorCode::DependencyTimeout) => ErrorCode::DependencyTimeout,
        Ok(wire::ErrorCode::InternalInvariantViolated) => ErrorCode::InternalInvariantViolated,
        Ok(wire::ErrorCode::ResourceRevisionConflict) => ErrorCode::ResourceRevisionConflict,
        Ok(wire::ErrorCode::ResourceNameConflict) => ErrorCode::ResourceNameConflict,
        Ok(wire::ErrorCode::ResourceMoveCycle) => ErrorCode::ResourceMoveCycle,
        Ok(wire::ErrorCode::ResourceNodeNotFound) => ErrorCode::ResourceNodeNotFound,
        Ok(wire::ErrorCode::ResourceParentNotFound) => ErrorCode::ResourceParentNotFound,
        Ok(wire::ErrorCode::ResourceFileUnavailable) => ErrorCode::ResourceFileUnavailable,
        Ok(wire::ErrorCode::CatalogCursorInvalidated) => ErrorCode::CatalogCursorInvalidated,
        _ => return Err(WireContractError::InvalidContract),
    })
}

#[cfg(test)]
mod memory_tests {
    use super::*;

    #[test]
    fn memory_governance_codes_preserve_identity_and_reject_changed_category() {
        for code in [
            ErrorCode::MemoryNotFound,
            ErrorCode::MemoryConflict,
            ErrorCode::MemoryEvaluationPending,
            ErrorCode::MemoryNotPublishable,
            ErrorCode::PublicDeliveryMissing,
        ] {
            let info = ErrorInfo {
                code,
                category: code.category(),
                safe_message: SafeDisplayText::new("Memory governance result")
                    .expect("bounded static text"),
                retryable: false,
                dependency_ref: None,
                diagnostic_ref: None,
            };
            let wire = encode_error_info(&info);
            assert_eq!(
                decode_runtime_error_info(wire.clone()).expect("shared codec"),
                info
            );
            assert!(
                decode_runtime_error_info(wire::ErrorInfo {
                    category: wire::ErrorCategory::Internal as i32,
                    ..wire
                })
                .is_err()
            );
        }
    }
}
