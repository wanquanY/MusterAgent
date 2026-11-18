use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_run::InvocationContext;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_model::{
    CompleteModelInvocation, ModelPersistenceDisposition, ModelRouteRef,
};
use thiserror::Error;

use crate::ContextCompactionRequest;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextCompactionDispatchState {
    SealedNotDispatched,
    Dispatched,
    Terminal,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PersistedContextCompactionSlot {
    pub invocation: InvocationContext,
    pub request_ref: ContentRef,
    pub request_hash: Sha256,
    pub model_route_ref: ModelRouteRef,
    pub started_at: UtcTimestamp,
    pub dispatch_state: ContextCompactionDispatchState,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextCompactionRecovery {
    pub early_provider_cause: Option<musteragent_runtime_model::ProviderErrorCode>,
    pub request_hash: Sha256,
    pub model_route_ref: ModelRouteRef,
    pub state: ContextCompactionRecoveryState,
    pub failure_retryable: bool,
    pub retry_not_before: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ContextCompactionRecoveryState {
    SealedNotDispatched,
    /// The Provider may have accepted the request. A replacement process must
    /// never redispatch without provider-native recovery proof.
    Indeterminate,
    Succeeded(ContentRef),
    Failed(ErrorCode),
    Cancelled,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum ContextCompactionStoreError {
    #[error("Context compaction identity conflicts with the sealed slot")]
    IdempotencyConflict,
    #[error("Context compaction lost its active Attempt fence")]
    FenceConflict,
    #[error("Context compaction persistence is corrupt")]
    CorruptState,
    #[error("Context compaction persistence is unavailable")]
    Unavailable,
}

/// Durable exactly-once boundary for one auxiliary Compression invocation.
///
/// Logical selection remains in `ContextPlanner`; implementations only seal,
/// dispatch and recover the exact request supplied by that planner.
pub trait ContextCompactionInvocationStore: Send + Sync {
    fn latest_generation<'a>(
        &'a self,
        request: &'a ContextCompactionRequest,
    ) -> ApplicationFuture<
        'a,
        Result<Option<crate::CompactionGenerationObservation>, ContextCompactionStoreError>,
    >;

    fn latest_role_candidate<'a>(
        &'a self,
        request: &'a ContextCompactionRequest,
    ) -> ApplicationFuture<
        'a,
        Result<Option<crate::CompactionGenerationObservation>, ContextCompactionStoreError>,
    > {
        self.latest_generation(request)
    }

    fn load_or_claim<'a>(
        &'a self,
        request: &'a ContextCompactionRequest,
    ) -> ApplicationFuture<
        'a,
        Result<Option<PersistedContextCompactionSlot>, ContextCompactionStoreError>,
    >;

    fn seal_or_claim<'a>(
        &'a self,
        request: &'a ContextCompactionRequest,
        invocation: &'a musteragent_runtime_model::ModelInvocation,
    ) -> ApplicationFuture<'a, Result<PersistedContextCompactionSlot, ContextCompactionStoreError>>;

    fn mark_dispatched<'a>(
        &'a self,
        request: &'a ContextCompactionRequest,
    ) -> ApplicationFuture<'a, Result<ModelPersistenceDisposition, ContextCompactionStoreError>>;

    fn load_recovery(
        &self,
        invocation_id: InvocationId,
    ) -> ApplicationFuture<'_, Result<Option<ContextCompactionRecovery>, ContextCompactionStoreError>>;

    fn complete(
        &self,
        completion: CompleteModelInvocation,
    ) -> ApplicationFuture<'_, Result<ModelPersistenceDisposition, ContextCompactionStoreError>>;

    /// Claim physical supervision of the already sealed original slot before
    /// provider preflight. A duplicate never authorizes another dispatch.
    fn begin_supervised<'a>(
        &'a self,
        _request: &'a ContextCompactionRequest,
        _invocation: &'a musteragent_runtime_model::ModelInvocation,
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_runtime_model::SupervisedModelStart, ContextCompactionStoreError>,
    > {
        Box::pin(async { Err(ContextCompactionStoreError::CorruptState) })
    }

    /// Original model outcome, usage and liveness are one fenced transaction.
    fn complete_supervised(
        &self,
        _completion: musteragent_runtime_model::SupervisedModelCompletion,
    ) -> ApplicationFuture<'_, Result<ModelPersistenceDisposition, ContextCompactionStoreError>>
    {
        Box::pin(async { Err(ContextCompactionStoreError::CorruptState) })
    }
}
