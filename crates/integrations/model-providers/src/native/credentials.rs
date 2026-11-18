use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_model::{CredentialGrantRef, ModelRoute};
use musteragent_runtime_model::{
    ModelCredentialAuthorization, ModelCredentialAuthorizationError, ModelInvocationRouteRole,
};

use crate::auth::CredentialFuture;
use crate::{CredentialResolver, CredentialResolverError};

/// Platform credential access, called only after durable authorization by the
/// enclosing resolver. Implementations must retain their provider/expiry checks.
pub trait CredentialMaterialSource: Send + Sync {
    fn resolve_material<'a>(
        &'a self,
        route: &'a ModelRoute,
        invocation_id: InvocationId,
    ) -> CredentialFuture<'a>;
}

pub struct AdmittedCredentialResolver {
    authorization: Arc<dyn ModelCredentialAuthorization>,
    source: Arc<dyn CredentialMaterialSource>,
    clock: fn() -> UtcTimestamp,
    failures: Mutex<BTreeMap<&'static str, u64>>,
}

impl AdmittedCredentialResolver {
    pub fn new(
        authorization: Arc<dyn ModelCredentialAuthorization>,
        source: Arc<dyn CredentialMaterialSource>,
        clock: fn() -> UtcTimestamp,
    ) -> Self {
        Self {
            authorization,
            source,
            clock,
            failures: Mutex::default(),
        }
    }

    fn reject(
        &self,
        error: ModelCredentialAuthorizationError,
        invocation_id: InvocationId,
        role: ModelInvocationRouteRole,
    ) -> CredentialResolverError {
        let mut failures = self
            .failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let occurrence = failures.entry(error.code()).or_default();
        *occurrence = occurrence.saturating_add(1);
        if occurrence.is_power_of_two() {
            tracing::warn!(target: "musteragent_model_providers::security",
                authorization_failure = error.code(), occurrence = *occurrence,
                %invocation_id, route_role = role.canonical_code(),
                "Native model credential authorization failed");
        }
        match error {
            ModelCredentialAuthorizationError::WriterFenceStale
            | ModelCredentialAuthorizationError::Unavailable => {
                CredentialResolverError::Unavailable
            }
            _ => CredentialResolverError::BindingMismatch,
        }
    }
}

impl CredentialResolver for AdmittedCredentialResolver {
    fn resolve<'a>(
        &'a self,
        grant_ref: &'a CredentialGrantRef,
        route: &'a ModelRoute,
        role: ModelInvocationRouteRole,
        invocation_id: InvocationId,
    ) -> CredentialFuture<'a> {
        Box::pin(async move {
            self.authorization
                .authorize(grant_ref, route, role, invocation_id, (self.clock)())
                .await
                .map_err(|error| self.reject(error, invocation_id, role))?;
            self.source.resolve_material(route, invocation_id).await
        })
    }
}
