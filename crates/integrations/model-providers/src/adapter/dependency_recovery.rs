//! Transport startup coordination; successful streams and Tools own their
//! normal lifecycles. All waits return to the durable scheduler.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_runtime_model::dependency_recovery::{
    DependencyAdmission, DependencyObservation, DependencyPermit, DependencyRecoveryError,
    DependencyRepair, ModelDependencyKey, ModelDependencyRecoveryStore, REPAIR_PROBE_MINIMUM_MS,
};

pub(super) struct SharedDependencyRequest {
    store: Arc<dyn ModelDependencyRecoveryStore>,
    endpoint: HeldDependency,
    credential: HeldDependency,
}
struct HeldDependency {
    key: ModelDependencyKey,
    permit: DependencyPermit,
}
impl DirectModelProvider {
    /// Shared state never broadens model authority or chooses another route.
    #[must_use]
    pub fn with_dependency_recovery(
        mut self,
        store: Arc<dyn ModelDependencyRecoveryStore>,
    ) -> Self {
        self.dependency_recovery = Some(store);
        self
    }
    pub(super) async fn acquire_dependency(
        &self,
        request: &CanonicalModelRequest,
        endpoint: &ProviderEndpoint,
        provider_revision: Option<&str>,
    ) -> Result<Option<SharedDependencyRequest>, ProviderError> {
        let Some(store) = &self.dependency_recovery else {
            return Ok(None);
        };
        let key = ModelDependencyKey {
            tenant_id: request.invocation.run_context.tenant_id,
            provider_id: endpoint.model_provider_id,
            model_entry_id: Some(endpoint.model_entry_id),
            endpoint_hash: Sha256::digest(format!(
                "provider-v2\n{}\n{}\n{}\n{}\n{}",
                endpoint.api_format.as_str(),
                endpoint.invocation_url.origin().ascii_serialization(),
                endpoint.invocation_url.path(),
                request.route.route_ref.as_str(),
                provider_revision.unwrap_or("")
            )),
        };
        let credential_key = ModelDependencyKey {
            model_entry_id: None,
            endpoint_hash: Sha256::digest(format!(
                "credential-v1\n{}\n{}",
                endpoint.invocation_url.origin().ascii_serialization(),
                provider_revision.unwrap_or("")
            )),
            ..key.clone()
        };
        let id = request.invocation.invocation_id;
        let lifetime = endpoint
            .total_timeout
            .saturating_add(Duration::from_secs(30));
        let credential = acquire(store.as_ref(), credential_key, id, lifetime).await?;
        let endpoint = match acquire(store.as_ref(), key, id, lifetime).await {
            Ok(permit) => permit,
            Err(error) => {
                publish_one(store.as_ref(), credential, DependencyObservation::Abandoned).await;
                return Err(error);
            }
        };
        Ok(Some(SharedDependencyRequest {
            store: store.clone(),
            endpoint,
            credential,
        }))
    }
}

async fn acquire(
    store: &dyn ModelDependencyRecoveryStore,
    key: ModelDependencyKey,
    id: musteragent_identity::InvocationId,
    lifetime: Duration,
) -> Result<HeldDependency, ProviderError> {
    match store
        .acquire(&key, id, lifetime)
        .await
        .map_err(store_failure)?
    {
        DependencyAdmission::Ready(permit) => Ok(HeldDependency { key, permit }),
        DependencyAdmission::RepairRequired {
            reason,
            retry_after_ms,
        } => {
            let (kind, code) = match reason {
                DependencyRepair::Credential => (
                    ProviderErrorKind::Authentication,
                    ProviderErrorCode::AuthenticationFailed,
                ),
                DependencyRepair::Payment => {
                    (ProviderErrorKind::Quota, ProviderErrorCode::PaymentRequired)
                }
                DependencyRepair::Quota => {
                    (ProviderErrorKind::Quota, ProviderErrorCode::QuotaExceeded)
                }
            };
            Err(provider_error(kind, code, false, Some(retry_after_ms)))
        }
        DependencyAdmission::Deferred { retry_after_ms } => Err(provider_error(
            ProviderErrorKind::Server,
            ProviderErrorCode::ProviderServer,
            true,
            Some(retry_after_ms),
        )),
    }
}

/// Only a wire response classified at its physical trust boundary can certify a
/// permanent upstream repair. Local budget and normalizer errors can also use
/// the quota code, so those must never enter this branch.
pub(super) async fn observe_upstream(
    shared: Option<SharedDependencyRequest>,
    error: &mut ProviderError,
) {
    if shared.is_none() {
        return;
    }
    let reason = match error.safe_code {
        ProviderErrorCode::PaymentRequired => Some(DependencyRepair::Payment),
        ProviderErrorCode::AuthenticationFailed => Some(DependencyRepair::Credential),
        ProviderErrorCode::QuotaExceeded => Some(DependencyRepair::Quota),
        _ => None,
    };
    if let Some(reason) = reason {
        let retry_after_ms = Some(
            error
                .retry_after_ms
                .unwrap_or(0)
                .max(REPAIR_PROBE_MINIMUM_MS),
        );
        error.retry_after_ms = retry_after_ms;
        publish(
            shared,
            DependencyObservation::RepairNeeded {
                reason,
                retry_after_ms,
            },
        )
        .await;
    } else {
        observe(shared, Some(error)).await;
    }
}
pub(super) async fn observe(
    shared: Option<SharedDependencyRequest>,
    error: Option<&ProviderError>,
) {
    let observation = match error {
        Some(error)
            if error.retryable
                && matches!(
                    error.safe_code,
                    ProviderErrorCode::ProviderServer
                        | ProviderErrorCode::RateLimited
                        | ProviderErrorCode::TransportFailed
                        | ProviderErrorCode::DeadlineExceeded
                ) =>
        {
            DependencyObservation::Unavailable {
                retry_after_ms: error.retry_after_ms,
            }
        }
        Some(_) => DependencyObservation::Abandoned,
        None => DependencyObservation::Healthy,
    };
    publish(shared, observation).await;
}
async fn publish(shared: Option<SharedDependencyRequest>, observation: DependencyObservation) {
    let Some(shared) = shared else { return };
    // Per-endpoint congestion and per-credential repair have independent
    // scopes. One real request can hold the bounded probes for both.
    let (endpoint_outcome, credential_outcome) = match observation {
        DependencyObservation::RepairNeeded { .. } => {
            (DependencyObservation::Abandoned, observation)
        }
        DependencyObservation::Unavailable { .. } => {
            (observation, DependencyObservation::Abandoned)
        }
        _ => (observation, observation),
    };
    tokio::join!(
        publish_one(shared.store.as_ref(), shared.endpoint, endpoint_outcome),
        publish_one(shared.store.as_ref(), shared.credential, credential_outcome)
    );
}
async fn publish_one(
    store: &dyn ModelDependencyRecoveryStore,
    shared: HeldDependency,
    observation: DependencyObservation,
) {
    if matches!(
        observation,
        DependencyObservation::Healthy | DependencyObservation::Abandoned
    ) && !shared.permit.probe
    {
        return;
    }
    let published = tokio::time::timeout(
        Duration::from_secs(2),
        store.observe(&shared.key, shared.permit, observation),
    )
    .await;
    if !matches!(published, Ok(Ok(()))) {
        // The physical response has already arrived. Preserve it even if
        // optional health publication fails; never repeat a successful model
        // operation solely to repair coordination state. A probe lease expires.
        tracing::warn!(dependency_recovery.error = ?published, "Model dependency health publication failed");
    }
}
fn store_failure(error: DependencyRecoveryError) -> ProviderError {
    let (code, retryable) = match error {
        DependencyRecoveryError::Unavailable => {
            (ProviderErrorCode::DependencyStoreUnavailable, true)
        }
        DependencyRecoveryError::InvalidState => (ProviderErrorCode::DependencyStateInvalid, false),
    };
    provider_error(ProviderErrorKind::Server, code, retryable, None)
}
