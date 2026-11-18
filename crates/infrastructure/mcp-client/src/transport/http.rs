//! MCP Streamable HTTP transport (protocol 2025-03-26 and later).
//!
//! Every client message is POSTed to the single MCP endpoint. A server
//! answers a request with either one `application/json` body or a
//! `text/event-stream` carrying the response (optionally preceded by server
//! notifications and requests). Notifications and responses are accepted
//! with `202 Accepted`. The optional standalone GET stream is not opened.

use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use musteragent_mcp::McpAuthorizationKind;
use musteragent_runtime_content::ApplicationFuture;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use url::Url;
use zeroize::Zeroizing;

use super::{McpTransport, McpTransportKind};
use crate::McpClientError;
use crate::credentials::{McpCredentialPort, McpCredentialRequest};
use crate::sse::SseParser;

const MCP_SESSION_ID: HeaderName = HeaderName::from_static("mcp-session-id");
const MCP_PROTOCOL_VERSION: HeaderName = HeaderName::from_static("mcp-protocol-version");
const MAX_SESSION_ID_BYTES: usize = 1_024;
const INBOUND_QUEUE: usize = 32;

#[derive(Debug, Clone)]
pub struct StreamableHttpConfig {
    pub endpoint: Url,
    pub credentials: McpCredentialRequest,
    pub connect_timeout: Duration,
    /// Deadline for the response status line and headers of each POST.
    pub response_timeout: Duration,
    pub max_message_bytes: usize,
    pub max_response_bytes: usize,
    /// Explicit egress proxy (the cloud egress gateway). Ambient proxy
    /// environment variables are never honoured.
    pub proxy: Option<Url>,
    /// Plain `http://` is accepted only for loopback hosts unless set.
    pub allow_insecure_http: bool,
}

impl StreamableHttpConfig {
    #[must_use]
    pub fn new(endpoint: Url, credentials: McpCredentialRequest) -> Self {
        Self {
            endpoint,
            credentials,
            connect_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(60),
            max_message_bytes: crate::McpClientLimits::default().max_message_bytes,
            max_response_bytes: crate::McpClientLimits::default().max_http_response_bytes,
            proxy: None,
            allow_insecure_http: false,
        }
    }

    pub fn validate(&self) -> Result<(), McpClientError> {
        let url = &self.endpoint;
        let host_is_loopback = match url.host() {
            Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => IpAddr::V4(address).is_loopback(),
            Some(url::Host::Ipv6(address)) => IpAddr::V6(address).is_loopback(),
            None => return Err(McpClientError::InvalidConfiguration("endpoint host")),
        };
        let scheme_ok = match url.scheme() {
            "https" => true,
            "http" => host_is_loopback || self.allow_insecure_http,
            _ => false,
        };
        if !scheme_ok
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.as_str().len() > musteragent_mcp::MAX_MCP_URL_BYTES
        {
            return Err(McpClientError::InvalidConfiguration("endpoint"));
        }
        if self.connect_timeout.is_zero()
            || self.response_timeout.is_zero()
            || self.max_message_bytes == 0
            || self.max_response_bytes < self.max_message_bytes
        {
            return Err(McpClientError::InvalidConfiguration("http limits"));
        }
        Ok(())
    }
}

type Inbound = Result<Vec<u8>, McpClientError>;

#[derive(Default)]
struct SessionState {
    session_id: Option<HeaderValue>,
    protocol_version: Option<HeaderValue>,
}

pub struct StreamableHttpTransport {
    client: reqwest::Client,
    config: StreamableHttpConfig,
    credentials: Arc<dyn McpCredentialPort>,
    state: StdMutex<SessionState>,
    inbound_tx: StdMutex<Option<mpsc::Sender<Inbound>>>,
    inbound_rx: Mutex<mpsc::Receiver<Inbound>>,
    readers: StdMutex<JoinSet<()>>,
    closed: AtomicBool,
}

impl StreamableHttpTransport {
    pub fn new(
        config: StreamableHttpConfig,
        credentials: Arc<dyn McpCredentialPort>,
    ) -> Result<Self, McpClientError> {
        config.validate()?;
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(config.connect_timeout)
            .https_only(!config.allow_insecure_http && config.endpoint.scheme() == "https");
        builder = match &config.proxy {
            Some(proxy) => builder.proxy(
                reqwest::Proxy::all(proxy.as_str())
                    .map_err(|_| McpClientError::InvalidConfiguration("proxy"))?,
            ),
            None => builder.no_proxy(),
        };
        let client = builder
            .build()
            .map_err(|_| McpClientError::InvalidConfiguration("http client"))?;
        let (tx, rx) = mpsc::channel(INBOUND_QUEUE);
        Ok(Self {
            client,
            config,
            credentials,
            state: StdMutex::new(SessionState::default()),
            inbound_tx: StdMutex::new(Some(tx)),
            inbound_rx: Mutex::new(rx),
            readers: StdMutex::new(JoinSet::new()),
            closed: AtomicBool::new(false),
        })
    }

    /// Whether the server assigned an `Mcp-Session-Id`.
    #[must_use]
    pub fn has_session(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.session_id.is_some())
            .unwrap_or(false)
    }

    fn session_headers(&self) -> (Option<HeaderValue>, Option<HeaderValue>) {
        self.state
            .lock()
            .map(|state| (state.session_id.clone(), state.protocol_version.clone()))
            .unwrap_or((None, None))
    }

    async fn authorize(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, McpClientError> {
        if self.config.credentials.authorization == McpAuthorizationKind::None {
            return Ok(request);
        }
        let credentials = self
            .credentials
            .http_credentials(&self.config.credentials)
            .await?;
        if let Some(token) = credentials.bearer_token() {
            let header = Zeroizing::new(format!("Bearer {token}"));
            let mut value = HeaderValue::from_str(&header)
                .map_err(|_| McpClientError::Credentials("bearer token"))?;
            value.set_sensitive(true);
            request = request.header(AUTHORIZATION, value);
        }
        for (name, value) in credentials.headers() {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| McpClientError::Credentials("header"))?;
            let mut value =
                HeaderValue::from_str(value).map_err(|_| McpClientError::Credentials("header"))?;
            value.set_sensitive(true);
            request = request.header(name, value);
        }
        Ok(request)
    }

    fn record_session(&self, response: &reqwest::Response) -> Result<(), McpClientError> {
        let Some(value) = response.headers().get(&MCP_SESSION_ID) else {
            return Ok(());
        };
        if value.is_empty()
            || value.len() > MAX_SESSION_ID_BYTES
            || !value
                .as_bytes()
                .iter()
                .all(|byte| matches!(byte, 0x21..=0x7e))
        {
            return Err(McpClientError::ProtocolViolation("session id"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| McpClientError::Unavailable("state"))?;
        match &state.session_id {
            Some(current) if current != value => {
                Err(McpClientError::ProtocolViolation("session id changed"))
            }
            Some(_) => Ok(()),
            None => {
                let mut value = value.clone();
                value.set_sensitive(true);
                state.session_id = Some(value);
                Ok(())
            }
        }
    }

    fn spawn_reader(
        &self,
        response: reqwest::Response,
        stream: bool,
    ) -> Result<(), McpClientError> {
        let sender = self
            .inbound_tx
            .lock()
            .map_err(|_| McpClientError::Unavailable("state"))?
            .clone()
            .ok_or(McpClientError::SessionClosed)?;
        let max_message = self.config.max_message_bytes;
        let max_response = self.config.max_response_bytes;
        let mut readers = self
            .readers
            .lock()
            .map_err(|_| McpClientError::Unavailable("state"))?;
        while readers.try_join_next().is_some() {}
        readers.spawn(async move {
            let outcome = if stream {
                read_event_stream(response, &sender, max_message, max_response).await
            } else {
                read_json_body(response, &sender, max_message).await
            };
            if let Err(error) = outcome {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok(())
    }
}

async fn read_json_body(
    mut response: reqwest::Response,
    sender: &mpsc::Sender<Inbound>,
    limit: usize,
) -> Result<(), McpClientError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(McpClientError::MessageTooLarge { limit });
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| McpClientError::Unavailable("response body"))?
    {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(McpClientError::MessageTooLarge { limit });
        }
        body.extend_from_slice(&chunk);
    }
    if !body.iter().all(u8::is_ascii_whitespace) {
        let _ = sender.send(Ok(body)).await;
    }
    Ok(())
}

async fn read_event_stream(
    mut response: reqwest::Response,
    sender: &mpsc::Sender<Inbound>,
    max_message: usize,
    max_response: usize,
) -> Result<(), McpClientError> {
    let mut parser = SseParser::new(max_message);
    let mut total = 0_usize;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| McpClientError::Unavailable("event stream"))?
    {
        total = total.saturating_add(chunk.len());
        if total > max_response {
            return Err(McpClientError::MessageTooLarge {
                limit: max_response,
            });
        }
        for event in parser.push(&chunk)? {
            if event.is_message()
                && !event.data.trim().is_empty()
                && sender.send(Ok(event.data.into_bytes())).await.is_err()
            {
                return Ok(());
            }
        }
    }
    parser.finish()
}

fn media_type(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
        })
}

impl McpTransport for StreamableHttpTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::StreamableHttp
    }

    fn send(&self, message: Vec<u8>) -> ApplicationFuture<'_, Result<(), McpClientError>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return Err(McpClientError::SessionClosed);
            }
            let (session_id, protocol_version) = self.session_headers();
            let mut request = self
                .client
                .post(self.config.endpoint.clone())
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json, text/event-stream")
                .body(message);
            if let Some(version) = protocol_version {
                request = request.header(MCP_PROTOCOL_VERSION, version);
            }
            let had_session = session_id.is_some();
            if let Some(session_id) = session_id {
                request = request.header(MCP_SESSION_ID, session_id);
            }
            let request = self.authorize(request).await?;
            let response = tokio::time::timeout(self.config.response_timeout, request.send())
                .await
                .map_err(|_| McpClientError::Timeout)?
                .map_err(|error| {
                    if error.is_timeout() {
                        McpClientError::Timeout
                    } else {
                        McpClientError::Unavailable("http request")
                    }
                })?;
            let status = response.status().as_u16();
            match status {
                401 | 403 => return Err(McpClientError::AuthorizationRequired),
                404 if had_session => return Err(McpClientError::SessionExpired),
                202 => return Ok(()),
                200 => {}
                status => return Err(McpClientError::HttpStatus(status)),
            }
            self.record_session(&response)?;
            match media_type(&response).as_deref() {
                Some("text/event-stream") => self.spawn_reader(response, true),
                Some("application/json") => self.spawn_reader(response, false),
                None if response.content_length() == Some(0) => Ok(()),
                _ => Err(McpClientError::ProtocolViolation("response content type")),
            }
        })
    }

    fn receive(&self) -> ApplicationFuture<'_, Result<Option<Vec<u8>>, McpClientError>> {
        Box::pin(async move {
            match self.inbound_rx.lock().await.recv().await {
                Some(Ok(message)) => Ok(Some(message)),
                Some(Err(error)) => Err(error),
                None => Ok(None),
            }
        })
    }

    fn set_protocol_version(&self, version: &str) {
        if let (Ok(value), Ok(mut state)) = (HeaderValue::from_str(version), self.state.lock()) {
            state.protocol_version = Some(value);
        }
    }

    fn close(&self) -> ApplicationFuture<'_, Result<(), McpClientError>> {
        Box::pin(async move {
            if self.closed.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            if let Ok(mut readers) = self.readers.lock() {
                readers.abort_all();
            }
            if let Ok(mut sender) = self.inbound_tx.lock() {
                sender.take();
            }
            let (session_id, protocol_version) = self.session_headers();
            let Some(session_id) = session_id else {
                return Ok(());
            };
            let mut request = self
                .client
                .delete(self.config.endpoint.clone())
                .header(MCP_SESSION_ID, session_id);
            if let Some(version) = protocol_version {
                request = request.header(MCP_PROTOCOL_VERSION, version);
            }
            let request = self.authorize(request).await?;
            // 405 means the server does not support explicit termination.
            match tokio::time::timeout(self.config.connect_timeout, request.send()).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(_)) => Err(McpClientError::Unavailable("session delete")),
                Err(_) => Err(McpClientError::Timeout),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::TenantId;
    use musteragent_mcp::McpServerName;

    fn config(url: &str) -> StreamableHttpConfig {
        StreamableHttpConfig::new(
            Url::parse(url).expect("url"),
            McpCredentialRequest {
                tenant_id: TenantId::generate(),
                server_name: McpServerName::new("srv").expect("name"),
                authorization: McpAuthorizationKind::None,
            },
        )
    }

    #[test]
    fn endpoints_require_tls_except_loopback_and_reject_credentials() {
        assert!(config("https://mcp.example.com/mcp").validate().is_ok());
        assert!(config("http://127.0.0.1:9/mcp").validate().is_ok());
        assert!(config("http://localhost:9/mcp").validate().is_ok());
        assert!(config("http://[::1]:9/mcp").validate().is_ok());
        for url in [
            "http://mcp.example.com/mcp",
            "https://user:pw@mcp.example.com/mcp",
            "https://mcp.example.com/mcp#frag",
            "ftp://mcp.example.com/mcp",
        ] {
            assert!(config(url).validate().is_err(), "{url}");
        }
        let mut insecure = config("http://mcp.internal/mcp");
        insecure.allow_insecure_http = true;
        assert!(insecure.validate().is_ok());
    }
}
