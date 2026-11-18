use musteragent_conversation::SubmitMessage;
use musteragent_identity::CorrelationId;
use musteragent_run::RetryRun;
use musteragent_runtime_contract::{
    CommandEnvelope, ErrorCode, ErrorDetails, OpaqueDiagnosticRef, RuntimeError,
};

use crate::runtime_error;
use musteragent_runtime_session::AdmissionDenialReceipt;

pub(super) fn trace_admission_stage_error(
    stage: &'static str,
    error: RuntimeError,
) -> RuntimeError {
    tracing::warn!(
        failure.stage = stage,
        correlation_id = %error.correlation_id,
        error.code = error.code.as_str(),
        error.category = ?error.category,
        error.retryable = error.retryable,
        error.message = error.message.as_str(),
        "Submit Message admission stage failed"
    );
    error
}

pub(super) fn invalid_submit(command: &CommandEnvelope<SubmitMessage>) -> RuntimeError {
    runtime_error(
        ErrorCode::InvalidCommand,
        command.metadata.correlation_id,
        false,
        "Submit Message command is invalid",
    )
}

pub(super) fn invalid_retry(command: &CommandEnvelope<RetryRun>) -> RuntimeError {
    runtime_error(
        ErrorCode::InvalidCommand,
        command.metadata.correlation_id,
        false,
        "Retry Run command is invalid",
    )
}

pub(super) fn is_audited_admission_denial(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Unauthenticated
            | ErrorCode::ParticipantNotAuthorized
            | ErrorCode::ConversationArchived
            | ErrorCode::ConversationEpochStale
            | ErrorCode::AgentNotAvailable
            | ErrorCode::RuntimeSnapshotInvalid
            | ErrorCode::ModelRouteUnavailable
            | ErrorCode::CapabilityUnavailable
            | ErrorCode::PolicyDenied
            | ErrorCode::QuotaExceeded
    )
}

pub(super) fn canonical_denial_error(
    receipt: AdmissionDenialReceipt,
    correlation_id: CorrelationId,
) -> RuntimeError {
    let mut error = runtime_error(
        receipt.error_code,
        correlation_id,
        receipt.retryable,
        "Admission was denied by the canonical command decision",
    );
    if let Some(reference) = receipt.diagnostic_ref {
        error.details = Box::new(ErrorDetails::Diagnostic { reference });
    }
    error
}

pub(super) fn finalize_admission_denial(
    original: RuntimeError,
    expected_diagnostic_ref: Option<&OpaqueDiagnosticRef>,
    audit_result: Result<AdmissionDenialReceipt, RuntimeError>,
    correlation_id: CorrelationId,
) -> RuntimeError {
    match audit_result {
        Ok(receipt)
            if receipt.error_code == original.code
                && receipt.retryable == original.retryable
                && receipt.diagnostic_ref.as_ref() == expected_diagnostic_ref =>
        {
            original
        }
        Ok(receipt) => canonical_denial_error(receipt, correlation_id),
        Err(audit_error) => {
            // Audit persistence is mandatory operational evidence, but the
            // rejected action remains rejected even if that evidence cannot be
            // committed. Returning the audit dependency error used to erase
            // the actual policy/snapshot failure and made a non-retryable
            // denial look like a transient 503. Preserve the primary decision
            // while emitting a separately alertable audit failure.
            tracing::error!(
                failure.stage = "admission_denial_audit",
                correlation_id = %correlation_id,
                error.code = audit_error.code.as_str(),
                error.category = ?audit_error.category,
                error.retryable = audit_error.retryable,
                "Admission denial audit persistence failed; original denial was preserved"
            );
            original
        }
    }
}

pub(super) fn invariant<T>(command: &CommandEnvelope<T>) -> RuntimeError {
    runtime_error(
        ErrorCode::InternalInvariantViolated,
        command.metadata.correlation_id,
        false,
        "Runtime application invariant was violated",
    )
}

pub(super) fn build_error<T>(code: ErrorCode, command: &CommandEnvelope<T>) -> RuntimeError {
    runtime_error(
        code,
        command.metadata.correlation_id,
        false,
        "Runtime application could not build a canonical commit",
    )
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{CorrelationId, UtcTimestamp};
    use musteragent_runtime_contract::{ErrorCode, ErrorDetails};

    use super::{finalize_admission_denial, runtime_error};

    #[test]
    fn audit_outage_never_replaces_the_original_admission_denial() {
        let correlation_id = CorrelationId::generate();
        let original = runtime_error(
            ErrorCode::RuntimeSnapshotInvalid,
            correlation_id,
            false,
            "Runtime Snapshot could not be constructed",
        );
        let audit_error = runtime_error(
            ErrorCode::DependencyUnavailable,
            correlation_id,
            true,
            "Security Audit admission authority is unavailable",
        );

        let returned =
            finalize_admission_denial(original.clone(), None, Err(audit_error), correlation_id);

        assert_eq!(returned, original);
        assert_eq!(returned.code, ErrorCode::RuntimeSnapshotInvalid);
        assert!(!returned.retryable);
        assert!(matches!(returned.details.as_ref(), ErrorDetails::None));
    }

    #[test]
    fn matching_canonical_receipt_preserves_the_detailed_original_error() {
        let correlation_id = CorrelationId::generate();
        let diagnostic_ref = musteragent_runtime_contract::OpaqueDiagnosticRef::new(format!(
            "snapshot.build-content:{correlation_id}"
        ))
        .expect("diagnostic reference");
        let mut original = runtime_error(
            ErrorCode::RuntimeSnapshotInvalid,
            correlation_id,
            false,
            "Runtime Snapshot could not be constructed",
        );
        *original.details = ErrorDetails::Diagnostic {
            reference: diagnostic_ref.clone(),
        };
        let receipt = musteragent_runtime_session::AdmissionDenialReceipt {
            error_code: original.code,
            retryable: original.retryable,
            diagnostic_ref: Some(diagnostic_ref.clone()),
            occurred_at: UtcTimestamp::from_unix_millis(1),
        };

        let returned = finalize_admission_denial(
            original.clone(),
            Some(&diagnostic_ref),
            Ok(receipt),
            correlation_id,
        );

        assert_eq!(returned, original);
    }
}
