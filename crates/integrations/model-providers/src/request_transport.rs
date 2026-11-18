//! Optional transport for authenticated first-party endpoints. Native Provider
//! mapping, credentials, response normalization and model lifetime stay here.

use std::future::Future;
use std::pin::Pin;

use musteragent_runtime_model::{ModelRequestAuthority, ProviderError};

use crate::{NativeWireBody, ProviderEndpoint};

pub type FirstPartyPrepareFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PreparedModelHttpRequest, ProviderError>> + Send + 'a>>;

pub type FirstPartySendFuture =
    Pin<Box<dyn Future<Output = Result<reqwest::Response, ProviderError>> + Send>>;

/// Prepare is local: it may resolve already-admitted authority and serialize,
/// but it must never send an inference request or acquire a dispatch permit.
pub trait FirstPartyRequestTransport: Send + Sync {
    fn prepare<'a>(
        &'a self,
        request: &'a ModelRequestAuthority,
        endpoint: &'a ProviderEndpoint,
        body: NativeWireBody,
        native: reqwest::Request,
    ) -> FirstPartyPrepareFuture<'a>;
}

/// The Agent Loop has committed its dispatch marker before invoking send.
/// Implementations may perform one proof-authorized full repair under this
/// same invocation. An uncertain send must NEVER be replayed here. A retryable
/// error permits the Loop to seal this invocation, durably back off and create
/// a NEW invocation; it never authorizes another send with the old identity.
pub trait PreparedFirstPartyRequest: Send {
    /// Renew only the HTTP credential for this original invocation. The sealed
    /// route, request body and private transport capability remain unchanged.
    fn replace_bearer_credential(&mut self, value: reqwest::header::HeaderValue);

    fn send(self: Box<Self>, client: reqwest::Client) -> FirstPartySendFuture;
}

pub enum PreparedModelHttpRequest {
    Native(Box<reqwest::Request>),
    FirstParty(Box<dyn PreparedFirstPartyRequest>),
}

impl PreparedModelHttpRequest {
    pub(crate) fn replace_bearer_credential(&mut self, value: reqwest::header::HeaderValue) {
        match self {
            Self::Native(request) => {
                request
                    .headers_mut()
                    .insert(reqwest::header::AUTHORIZATION, value);
            }
            Self::FirstParty(request) => request.replace_bearer_credential(value),
        }
    }

    pub(crate) async fn send(
        self,
        client: reqwest::Client,
    ) -> Result<reqwest::Response, ProviderError> {
        match self {
            Self::Native(request) => client
                .execute(*request)
                .await
                .map_err(|error| crate::adapter::http_clients::transport_error(&error)),
            Self::FirstParty(request) => request.send(client).await,
        }
    }
}
