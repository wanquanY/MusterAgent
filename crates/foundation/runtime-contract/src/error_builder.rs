use crate::{ErrorCode, ErrorDetails, OpaqueDiagnosticRef, RuntimeError, SafeDisplayText};
use musteragent_identity::{CorrelationId, DottedKey};

#[must_use]
pub fn runtime_error(
    code: ErrorCode,
    correlation_id: CorrelationId,
    retryable: bool,
    message: &'static str,
) -> RuntimeError {
    let schema_version = DottedKey::new(crate::RUNTIME_ERROR_SCHEMA_VERSION)
        .unwrap_or_else(|_| unreachable!("static Runtime error schema is valid"));
    let message = SafeDisplayText::new(message)
        .unwrap_or_else(|_| unreachable!("static Runtime error message is valid"));
    RuntimeError {
        schema_version,
        code,
        category: code.category(),
        message,
        retryable,
        retry_after_ms: None,
        resource_ref: None,
        correlation_id,
        details: Box::new(ErrorDetails::None),
    }
}

/// Attach a bounded, non-secret operator lookup key to a safe Runtime error.
///
/// Diagnostic references are stable correlation handles, not user-facing
/// explanations. They may cross process boundaries and be retained by denial
/// audit stores, so callers must never include credentials or prompt content.
#[must_use]
pub fn runtime_error_with_diagnostic(
    code: ErrorCode,
    correlation_id: CorrelationId,
    retryable: bool,
    message: &'static str,
    diagnostic_ref: impl Into<String>,
) -> RuntimeError {
    let mut error = runtime_error(code, correlation_id, retryable, message);
    let reference = OpaqueDiagnosticRef::new(diagnostic_ref)
        .unwrap_or_else(|_| unreachable!("internal diagnostic references are bounded ASCII"));
    error.details = Box::new(ErrorDetails::Diagnostic { reference });
    error
}

#[cfg(test)]
mod tests {
    use crate::{ErrorCode, ErrorDetails};
    use musteragent_identity::CorrelationId;

    use super::runtime_error_with_diagnostic;

    #[test]
    fn diagnosed_errors_preserve_the_safe_contract_and_operator_lookup_key() {
        let correlation_id = CorrelationId::generate();
        let error = runtime_error_with_diagnostic(
            ErrorCode::RuntimeSnapshotInvalid,
            correlation_id,
            false,
            "Runtime Snapshot could not be constructed",
            format!("snapshot.build-content:{correlation_id}"),
        );

        assert!(error.validate().is_ok());
        assert_eq!(error.correlation_id, correlation_id);
        assert!(matches!(
            error.details.as_ref(),
            ErrorDetails::Diagnostic { reference }
                if reference.as_str() == format!("snapshot.build-content:{correlation_id}")
        ));
    }
}
