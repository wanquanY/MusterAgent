//! Native model transport reads the original durable admission and invalidation
//! owners. These ports carry product facts, never HTTP or protobuf types.

use musteragent_execution_grant::{EncodedExecutionGrantProof, SignedExecutionGrant};
use musteragent_identity::{Actor, ConversationId, DeviceId, RunId, TenantId};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};

use crate::{ModelCredentialAuthorizationError, ModelRequestAuthority};

/// Historical identity/signature verification only, after original durable
/// admission lookup. This cannot authorize a new Run or fresh credential use.
pub trait PersistedModelGrantAuthentication: Send + Sync {
    fn authenticate_retained(&self, signed: &SignedExecutionGrant) -> Result<(), ErrorCode>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ModelTransportInvalidationKind {
    EraseCache,
    CancelRun,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModelTransportInvalidation {
    pub event_id: String,
    pub revision: u64,
    pub kind: ModelTransportInvalidationKind,
    pub conversation_id: Option<ConversationId>,
    pub run_id: Option<RunId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModelTransportInvalidationAck {
    pub event_id: String,
    pub revision: u64,
    pub erasure_completed: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
pub enum ModelTransportStoreError {
    #[error("model transport state is invalid")]
    Invalid,
    #[error("model transport state is unavailable")]
    Unavailable,
}

pub trait ModelTransportAuthorityStore: Send + Sync {
    fn admitted_proof<'a>(
        &'a self,
        actor: Actor,
        request: &'a ModelRequestAuthority,
    ) -> ApplicationFuture<
        'a,
        Result<Option<EncodedExecutionGrantProof>, ModelCredentialAuthorizationError>,
    >;

    fn register_subject(
        &self,
        actor: Actor,
        device: DeviceId,
    ) -> ApplicationFuture<'_, Result<(), ModelTransportStoreError>>;

    fn renewal_snapshot<'a>(
        &'a self,
        tenant: TenantId,
        cursor: &'a str,
        exact: bool,
    ) -> ApplicationFuture<
        'a,
        Result<Option<(String, EncodedExecutionGrantProof)>, ModelTransportStoreError>,
    >;

    fn pending_invalidations(
        &self,
        actor: Actor,
        device: DeviceId,
        now_ms: i64,
    ) -> ApplicationFuture<'_, Result<Vec<ModelTransportInvalidation>, ModelTransportStoreError>>;

    fn acknowledge_invalidations<'a>(
        &'a self,
        actor: Actor,
        device: DeviceId,
        acknowledgements: &'a [ModelTransportInvalidationAck],
        now_ms: i64,
    ) -> ApplicationFuture<'a, Result<(), ModelTransportStoreError>>;
}
