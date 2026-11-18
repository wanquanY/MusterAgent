//! One prepared physical model operation. Preparation cannot dispatch; the
//! original Journal owns the dispatch marker and the Runtime owns supervision.
use std::sync::Arc;

use musteragent_execution_liveness::ExecutionStopControl;
use musteragent_runtime_contract::{
    ApplicationFuture,
    execution_liveness::{ExecutionBinding, ExecutionScope, InvocationExecutionTiming},
};

use crate::{
    CanonicalModelRequest, ModelEventStream, ProviderError, ProviderErrorCode, ProviderErrorKind,
};

/// An adapter keeps the exact prepared request in this owned operation rather
/// than looking up a mutable invocation-ID cache after the dispatch marker.
/// The enclosing tracked Runtime task polls preparation, stream and cleanup;
/// adapters must not spawn an untracked request task.
pub trait ModelExecution<'provider>: Send {
    fn binding(&self) -> ExecutionBinding;

    /// Apply host annotations before local preparation. Unsupported adapters
    /// reject nonempty annotations explicitly; they must never drop headers.
    fn annotate(
        &mut self,
        annotations: crate::ModelRequestAnnotations,
    ) -> Result<(), ProviderError> {
        if annotations.is_empty() {
            Ok(())
        } else {
            Err(unsupported_model_execution())
        }
    }

    /// Binds cancellation to this physical generation. Closing the local HTTP
    /// response cannot by itself confirm that remote model computation stopped.
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl>;

    /// Local/auth/content preparation only. No model request may be sent here.
    /// A cancellation request permanently prevents subsequent dispatch, even
    /// when it arrives in the gap between preparation and the Journal marker.
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>>;

    /// Consume the prepared operation once, after the original dispatch marker
    /// commits. Failure must never silently prepare or dispatch a replacement.
    fn into_stream(self: Box<Self>) -> ModelEventStream<'provider>;
}

/// Shared adapter check. Home already verifies the sealed request/policy digest
/// and creates the binding; the adapter must also reject cross-scope use before
/// touching credentials, content or a provider connection.
pub fn validate_model_execution_binding(
    request: &CanonicalModelRequest,
    binding: ExecutionBinding,
) -> Result<(), ProviderError> {
    validate_model_invocation_binding(&request.invocation, binding)
}

/// A recovery owner may change only the Attempt fence, never the immutable
/// invocation authority. Home verifies the original request digest and commits
/// a recovery ticket before this port is opened.
pub fn validate_model_recovery_binding(
    request: &CanonicalModelRequest,
    binding: ExecutionBinding,
) -> Result<(), ProviderError> {
    let original = &request.invocation.attempt_context;
    if binding.owner_fence <= original.fencing_token
        || binding.owner_attempt_id == original.attempt_id
    {
        return Err(unsupported_model_execution());
    }
    let original_binding = ExecutionBinding {
        owner_attempt_id: original.attempt_id,
        owner_fence: original.fencing_token,
        ..binding
    };
    validate_model_execution_binding(request, original_binding)
}

pub fn validate_model_invocation_binding(
    invocation: &musteragent_run::InvocationContext,
    binding: ExecutionBinding,
) -> Result<(), ProviderError> {
    if !matches!(invocation.timing, InvocationExecutionTiming::ActivityV1(_))
        || binding.tenant_id != invocation.run_context.tenant_id
        || binding.run_id != invocation.run_context.run_id
        || binding.run_id != invocation.attempt_context.run_id
        || binding.scope != ExecutionScope::Invocation(invocation.invocation_id)
        || binding.owner_attempt_id != invocation.attempt_context.attempt_id
        || binding.owner_fence != invocation.attempt_context.fencing_token
    {
        return Err(unsupported_model_execution());
    }
    Ok(())
}

pub(crate) fn unsupported_model_execution() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        safe_code: ProviderErrorCode::RouteConfigurationInvalid,
        retryable: false,
        retry_after_ms: None,
        diagnostic_ref: None,
    }
}
