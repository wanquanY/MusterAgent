use std::pin::Pin;

use futures_core::Stream;
use musteragent_runtime_contract::ApplicationFuture;

use crate::{CanonicalModelRequest, ModelStreamEvent, ProviderError, ProviderRecoveryCapability};
use musteragent_identity::InvocationId;

pub type ModelEventStream<'a> =
    Pin<Box<dyn Stream<Item = Result<ModelStreamEvent, ProviderError>> + Send + 'a>>;

/// Consumer-owned model port. Provider transports and SDK/wire types cannot
/// cross this boundary.
pub trait ModelProvider: Send + Sync {
    /// Whether this adapter implements the fenced same-invocation recovery
    /// port. A capability declaration alone is never permission to redispatch.
    fn supports_invocation_recovery(&self) -> bool {
        false
    }

    /// Reattach to the original immutable request under a successor's binding.
    /// Implementations must use the original idempotency key and must never
    /// fall back to another generation or provider.
    fn recover_execution<'provider>(
        &'provider self,
        _request: CanonicalModelRequest,
        _binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<Box<dyn crate::ModelExecution<'provider> + 'provider>, ProviderError> {
        Err(crate::execution::unsupported_model_execution())
    }

    /// Open an activity-supervised physical operation without performing I/O.
    /// The binding comes from the original Home begin transaction. Legacy
    /// adapters reject this mode instead of silently imposing a fixed deadline.
    fn execution<'provider>(
        &'provider self,
        _request: CanonicalModelRequest,
        _binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<Box<dyn crate::ModelExecution<'provider> + 'provider>, ProviderError> {
        Err(crate::execution::unsupported_model_execution())
    }

    /// Returns the adapter's closed proof profile for this admission-pinned
    /// request. The default is deliberately non-resumable.
    fn recovery_capability(&self, _request: &CanonicalModelRequest) -> ProviderRecoveryCapability {
        ProviderRecoveryCapability::NonResumable
    }

    /// Completes every fallible local preparation step before the caller
    /// persists an irreversible Provider-dispatch marker. Implementations
    /// must not send the Provider request from this method. The default keeps
    /// deterministic/test providers source-compatible.
    fn preflight<'a>(
        &'a self,
        _request: &'a CanonicalModelRequest,
    ) -> ApplicationFuture<'a, Result<(), ProviderError>> {
        Box::pin(async { Ok(()) })
    }

    /// Destroys any secret-bearing prepared request when the caller cannot
    /// cross the durable dispatch boundary after a successful preflight.
    fn discard_preflight(&self, _invocation_id: InvocationId) {}

    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_>;
}
