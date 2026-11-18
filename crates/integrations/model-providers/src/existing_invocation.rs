//! Host-owned reconciliation of an already accepted, idempotent model request.
//!
//! Resolution never dispatches inference. Completed responses contain the original
//! native event stream and pass through the ordinary adapter normalizer and ledger.
use std::{sync::Arc, time::Duration};

use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{
    ModelRequestAuthority, ProviderError, ProviderErrorCode, ProviderErrorKind,
};
use reqwest::{Response, header::HeaderMap};
use serde::Deserialize;
use url::Url;

/// An exact host-defined HTTP status and JSON string field match. A status alone
/// never proves that inference was accepted.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlreadyAcceptedResponse {
    pub status: u16,
    /// RFC 6901 JSON pointer, for example `/error/code`.
    pub code_pointer: String,
    pub code: String,
}
impl AlreadyAcceptedResponse {
    pub fn validate(&self) -> Result<(), ProviderError> {
        if !(400..600).contains(&self.status)
            || !self.code_pointer.starts_with('/')
            || self.code.is_empty()
        {
            return Err(failure(ProviderErrorCode::RouteConfigurationInvalid));
        }
        Ok(())
    }
    pub(crate) fn matches(&self, status: u16, body: &[u8]) -> bool {
        self.status == status
            && serde_json::from_slice::<serde_json::Value>(body)
                .ok()
                .and_then(|v| {
                    v.pointer(&self.code_pointer)
                        .and_then(|v| v.as_str())
                        .map(str::to_owned)
                })
                .as_deref()
                == Some(self.code.as_str())
    }
}

/// Authority and the exact key selected for the dispatched request. These values
/// must also scope the gateway's lookup authorization; keys are not bearer tokens.
pub struct ExistingInvocationRequest<'a> {
    pub authority: &'a ModelRequestAuthority,
    pub idempotency_key: &'a str,
    pub invocation_url: &'a Url,
    /// Credentials and host metadata used for dispatch. Never log these headers.
    pub headers: &'a HeaderMap,
}

/// A lookup observes the original operation, never starts another one.
pub enum ExistingInvocationResolution {
    /// Original native SSE or AWS event stream, including its terminal and raw usage.
    Completed(Response),
    Pending,
    /// Verified terminal failure. Arbitrary remote strings cannot become error codes.
    Failed,
}

/// Host port for an idempotent gateway's result endpoint. Implementations must
/// authenticate and bind results to the supplied route, actor and original key.
pub trait ExistingInvocationResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        request: ExistingInvocationRequest<'a>,
    ) -> ApplicationFuture<'a, Result<ExistingInvocationResolution, ProviderError>>;
}

/// Bounded lookup policy; applies to lookup I/O and backoff together. A pending
/// response is not progress and never renews this deadline.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExistingInvocationLookupPolicy {
    pub timeout_ms: u64,
    pub max_lookups: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}
impl Default for ExistingInvocationLookupPolicy {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_lookups: 10,
            initial_backoff_ms: 100,
            max_backoff_ms: 2_000,
        }
    }
}
impl ExistingInvocationLookupPolicy {
    pub fn validate(&self) -> Result<(), ProviderError> {
        if self.timeout_ms == 0
            || self.timeout_ms > 300_000
            || self.max_lookups == 0
            || self.max_lookups > 1_000
            || self.initial_backoff_ms == 0
            || self.max_backoff_ms < self.initial_backoff_ms
            || self.max_backoff_ms > self.timeout_ms
        {
            return Err(failure(ProviderErrorCode::RouteConfigurationInvalid));
        }
        Ok(())
    }
}

/// Installed per immutable provider/entry route. Classifier-only configuration
/// explicitly reports missing resolution capability; it cannot authorize replay.
#[derive(Clone)]
pub struct ExistingInvocationRecovery {
    pub already_accepted: AlreadyAcceptedResponse,
    pub resolver: Option<Arc<dyn ExistingInvocationResolver>>,
    pub policy: ExistingInvocationLookupPolicy,
}
impl ExistingInvocationRecovery {
    pub fn validate(&self) -> Result<(), ProviderError> {
        self.already_accepted.validate()?;
        self.policy.validate()
    }

    pub(crate) async fn resolve(
        &self,
        request: ExistingInvocationRequest<'_>,
    ) -> Result<Response, ProviderError> {
        let resolver = self
            .resolver
            .as_ref()
            .ok_or_else(|| failure(ProviderErrorCode::ExistingInvocationResolverUnavailable))?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(self.policy.timeout_ms);
        let mut delay = self.policy.initial_backoff_ms;
        for index in 0..self.policy.max_lookups {
            let lookup = ExistingInvocationRequest {
                authority: request.authority,
                idempotency_key: request.idempotency_key,
                invocation_url: request.invocation_url,
                headers: request.headers,
            };
            match tokio::time::timeout_at(deadline, resolver.resolve(lookup)).await {
                Ok(Ok(ExistingInvocationResolution::Completed(response)))
                    if response.status().is_success() =>
                {
                    return Ok(response);
                }
                Ok(Ok(ExistingInvocationResolution::Pending)) => {}
                Ok(Ok(ExistingInvocationResolution::Failed)) => {
                    return Err(failure(ProviderErrorCode::ExistingInvocationFailed));
                }
                Ok(_) => return Err(failure(ProviderErrorCode::ExistingInvocationLookupFailed)),
                Err(_) => return Err(failure(ProviderErrorCode::ExistingInvocationTimeout)),
            }
            if index + 1 == self.policy.max_lookups {
                break;
            }
            if tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(delay)))
                .await
                .is_err()
            {
                break;
            }
            delay = delay.saturating_mul(2).min(self.policy.max_backoff_ms);
        }
        Err(failure(ProviderErrorCode::ExistingInvocationTimeout))
    }
}

/// Standard GET result transport. The host pins an endpoint on the invocation
/// origin; redirects are disabled so credentials never follow a remote location.
/// 200 returns the original native stream, 202 means pending, and 410 means failed.
/// Other statuses are lookup failures, never permission to dispatch inference.
pub struct HttpExistingInvocationResolver {
    endpoint: Url,
    client: reqwest::Client,
}
impl HttpExistingInvocationResolver {
    pub fn new(endpoint: Url) -> Result<Self, ProviderError> {
        if !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
            || endpoint.query().is_some()
        {
            return Err(failure(ProviderErrorCode::RouteConfigurationInvalid));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never());
        let client = crate::endpoint::apply_proxy_policy(
            client,
            crate::endpoint::is_loopback_origin(&endpoint),
        )
        .build()
        .map_err(|_| failure(ProviderErrorCode::RouteConfigurationInvalid))?;
        Ok(Self { endpoint, client })
    }
}
impl ExistingInvocationResolver for HttpExistingInvocationResolver {
    fn resolve<'a>(
        &'a self,
        request: ExistingInvocationRequest<'a>,
    ) -> ApplicationFuture<'a, Result<ExistingInvocationResolution, ProviderError>> {
        Box::pin(async move {
            if self.endpoint.origin() != request.invocation_url.origin() {
                return Err(failure(ProviderErrorCode::RouteConfigurationInvalid));
            }
            let mut url = self.endpoint.clone();
            url.query_pairs_mut()
                .append_pair("key", request.idempotency_key);
            let mut headers = request.headers.clone();
            for name in [
                "content-length",
                "content-type",
                "transfer-encoding",
                "host",
            ] {
                headers.remove(name);
            }
            let response = self
                .client
                .get(url)
                .headers(headers)
                .send()
                .await
                .map_err(|_| failure(ProviderErrorCode::ExistingInvocationLookupFailed))?;
            match response.status().as_u16() {
                200 => Ok(ExistingInvocationResolution::Completed(response)),
                202 => Ok(ExistingInvocationResolution::Pending),
                410 => Ok(ExistingInvocationResolution::Failed),
                _ => Err(failure(ProviderErrorCode::ExistingInvocationLookupFailed)),
            }
        })
    }
}

pub(crate) fn failure(code: ProviderErrorCode) -> ProviderError {
    ProviderError {
        kind: if code == ProviderErrorCode::ExistingInvocationTimeout {
            ProviderErrorKind::Timeout
        } else {
            ProviderErrorKind::Protocol
        },
        safe_code: code,
        retryable: false,
        retry_after_ms: None,
        diagnostic_ref: None,
    }
}
