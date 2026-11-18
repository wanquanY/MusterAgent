use musteragent_identity::{CorrelationId, DottedKey};
use musteragent_runtime_contract::{ErrorCode, ErrorDetails, RuntimeError, SafeDisplayText};

#[must_use]
pub fn runtime_error(
    code: ErrorCode,
    correlation_id: CorrelationId,
    retryable: bool,
    message: &'static str,
) -> RuntimeError {
    let schema_version = DottedKey::new(musteragent_runtime_contract::RUNTIME_ERROR_SCHEMA_VERSION)
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
