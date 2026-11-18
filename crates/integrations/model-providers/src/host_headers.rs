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
