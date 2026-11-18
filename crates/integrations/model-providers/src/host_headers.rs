//! Host-owned HTTP metadata, scoped to a pinned model invocation.

use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{ModelRequestAuthority, ProviderError};
use reqwest::header::HeaderMap;

/// Produces fresh signed context for each physical request preparation.
///
/// Authentication, content framing and the stable invocation idempotency key
/// remain adapter-owned. Conflicting headers fail before network dispatch.
pub trait ModelRequestHeaderHook: Send + Sync {
    fn headers<'a>(
        &'a self,
        authority: &'a ModelRequestAuthority,
    ) -> ApplicationFuture<'a, Result<HeaderMap, ProviderError>>;
}

/// Host metadata is installed explicitly, never read from process-global state.
#[derive(Clone, Default)]
pub struct HostRequestHeaders {
    pub static_headers: HeaderMap,
    pub hook: Option<std::sync::Arc<dyn ModelRequestHeaderHook>>,
}

impl HostRequestHeaders {
    pub(crate) async fn apply(
        &self,
        authority: &ModelRequestAuthority,
        outbound: &mut HeaderMap,
    ) -> Result<HeaderMap, ProviderError> {
        let dynamic = match &self.hook {
            Some(hook) => hook.headers(authority).await?,
            None => Default::default(),
        };
        for headers in [&self.static_headers, &dynamic] {
            for (name, value) in headers {
                if outbound.contains_key(name)
                    || matches!(
                        name.as_str(),
                        "host"
                            | "content-type"
                            | "content-length"
                            | "transfer-encoding"
                            | "connection"
                    )
                {
                    return Err(crate::adapter::protocol_error());
                }
                outbound.insert(name.clone(), value.clone());
            }
        }
        Ok(dynamic)
    }
}
