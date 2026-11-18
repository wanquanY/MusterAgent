//! Credential exposure requires the original durable invocation admission.
//! A login token or a caller-supplied route is never that admission.

use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_model::{CredentialGrantRef, ModelRoute};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

use crate::ModelInvocationRouteRole;

pub trait ModelCredentialAuthorization: Send + Sync {
    /// Check and consume the exact admitted binding under the repository's
    /// writer fence. Repeated use of the same binding is idempotent; a different
    /// grant, route, role or execution owner must fail closed.
    fn authorize<'a>(
        &'a self,
        grant_ref: &'a CredentialGrantRef,
        route: &'a ModelRoute,
        role: ModelInvocationRouteRole,
        invocation_id: InvocationId,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<(), ModelCredentialAuthorizationError>>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum ModelCredentialAuthorizationError {
    #[error("model invocation was not durably admitted")]
    InvocationNotAdmitted,
    #[error("model invocation does not match its admitted route")]
    RouteBindingMismatch,
    #[error("execution grant did not admit this credential route")]
    GrantRouteNotAdmitted,
    #[error("persisted execution authority is incomplete")]
    GrantAuthorityIncomplete,
    #[error("invocation was consumed by another execution grant")]
    InvocationAlreadyConsumed,
    #[error("credential writer fence is stale")]
    WriterFenceStale,
    #[error("credential authorization repository is unavailable")]
    Unavailable,
}

impl ModelCredentialAuthorizationError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvocationNotAdmitted => "invocation_not_admitted",
            Self::RouteBindingMismatch => "route_binding_mismatch",
            Self::GrantRouteNotAdmitted => "grant_route_not_admitted",
            Self::GrantAuthorityIncomplete => "grant_authority_incomplete",
            Self::InvocationAlreadyConsumed => "invocation_already_consumed",
            Self::WriterFenceStale => "writer_fence_stale",
            Self::Unavailable => "database_unavailable",
        }
    }
}
