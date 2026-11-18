//! Minimal forward proxy for sandbox egress: HTTP `CONNECT` tunnels and
//! plain `http://` requests in absolute form. The proxy resolves each
//! destination, checks every address with [`EgressPolicy`], and connects to
//! the exact address it checked. Every decision is logged.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderName, HeaderValue};
use hyper::server::conn::http1 as server_http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use super::filter::{DenyReason, EgressPolicy};
use super::{EgressAudit, EgressEvent, ResolutionMode, Resolver, SystemResolver, UpstreamProxy};
use crate::config::{ConfigError, EgressProxyConfig};

const LOG_TARGET: &str = "musteragent_sandbox_server::egress";

type ProxyBody = BoxBody<Bytes, hyper::Error>;

/// Resolved, policy-checked destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckedTarget {
    pub address: SocketAddr,
}

/// Outcome of a policy check, suitable for logging and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EgressDecision {
    Allowed(CheckedTarget),
    Denied(DenyReason),
    ResolveFailed,
    /// Private-address enforcement explicitly delegated to the configured upstream.
    Delegated {
        host: String,
        port: u16,
    },
}

#[derive(Debug, Clone)]
pub struct EgressProxy {
    policy: EgressPolicy,
    connect_timeout: Duration,
    resolver: Arc<dyn Resolver>,
    upstream: Option<UpstreamProxy>,
    audit: EgressAudit,
}

impl EgressProxy {
    pub fn from_config(config: &EgressProxyConfig) -> Result<Self, ConfigError> {
        if !(1..=60_000).contains(&config.connect_timeout_millis) {
            return Err(ConfigError::InvalidEgress);
        }
        Ok(Self {
            policy: EgressPolicy {
                deny_cidrs: config.parsed_deny_cidrs()?,
                deny_hosts: config
                    .deny_hosts
                    .iter()
                    .map(|host| host.to_ascii_lowercase())
                    .collect(),
                allow_cidrs: config.parsed_allow_cidrs()?,
                allowed_ports: config.allowed_ports.clone(),
            },
            connect_timeout: Duration::from_millis(config.connect_timeout_millis),
            resolver: Arc::new(SystemResolver),
            upstream: config.upstream,
            audit: EgressAudit::default(),
        })
    }

    #[must_use]
    pub fn new(policy: EgressPolicy, connect_timeout: Duration) -> Self {
        Self {
            policy,
            connect_timeout,
            resolver: Arc::new(SystemResolver),
            upstream: None,
            audit: EgressAudit::default(),
        }
    }

    /// Inject the host DNS authority (also useful for deterministic rebinding tests).
    #[must_use]
    pub fn with_resolver(mut self, resolver: Arc<dyn Resolver>) -> Self {
        self.resolver = resolver;
        self
    }
    /// Explicit upstream configuration; None is direct checked-address egress.
    #[must_use]
    pub fn with_upstream(mut self, upstream: UpstreamProxy) -> Self {
        self.upstream = Some(upstream);
        self
    }
    /// A fresh audit for one container/session, with the same host policy.
    #[must_use]
    pub fn session(&self) -> Self {
        Self {
            audit: EgressAudit::default(),
            ..self.clone()
        }
    }
    #[must_use]
    pub fn audit(&self) -> EgressAudit {
        self.audit.clone()
    }

    /// Resolves and checks a destination.
    pub async fn decide(&self, host: &str, port: u16) -> EgressDecision {
        if let Err(reason) = self.policy.check_host(host, port) {
            return EgressDecision::Denied(reason);
        }
        if host.len() > 253
            || host
                .bytes()
                .any(|b| b.is_ascii_control() || b == b' ' || b == b'@')
        {
            return EgressDecision::Denied(DenyReason::HostDenyList);
        }
        let literal = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .trim_end_matches('.')
            .parse::<IpAddr>()
            .ok();
        // Numeric aliases (short, integer, octal IPv4 or scoped IPv6) must
        // never become unchecked upstream hostnames.
        if literal.is_none()
            && (host.contains(':')
                || host.trim_end_matches('.').split('.').all(|part| {
                    !part.is_empty()
                        && (part.bytes().all(|b| b.is_ascii_digit())
                            || part.strip_prefix("0x").is_some_and(|s| {
                                !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit())
                            }))
                }))
        {
            return EgressDecision::Denied(DenyReason::Reserved);
        }
        let addresses: Vec<IpAddr> = match literal {
            Some(address) => vec![address],
            None if self.upstream.is_some_and(|u| u.upstream_resolves) => {
                return EgressDecision::Delegated {
                    host: host.to_owned(),
                    port,
                };
            }
            None => {
                match tokio::time::timeout(self.connect_timeout, self.resolver.resolve(host, port))
                    .await
                {
                    Ok(Ok(addresses)) if !addresses.is_empty() => addresses,
                    _ => return EgressDecision::ResolveFailed,
                }
            }
        };
        match self.policy.check_all(&addresses) {
            Ok(()) => match addresses.first() {
                Some(address) => EgressDecision::Allowed(CheckedTarget {
                    address: SocketAddr::new(*address, port),
                }),
                None => EgressDecision::ResolveFailed,
            },
            Err(reason) => EgressDecision::Denied(reason),
        }
    }

    /// Serves on `listener` until `shutdown` fires.
    pub async fn serve(self: Arc<Self>, listener: TcpListener, shutdown: CancellationToken) {
        loop {
            let accepted = tokio::select! {
                () = shutdown.cancelled() => return,
                accepted = listener.accept() => accepted,
            };
            let Ok((stream, peer)) = accepted else {
                continue;
            };
            let proxy = Arc::clone(&self);
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                proxy.serve_stream(stream, peer, shutdown).await;
            });
        }
    }

    /// Serve a session-owned stream, including a Docker stdio tunnel. This
    /// accepts HTTP proxy traffic only; it is not an arbitrary TCP forwarder.
    pub async fn serve_stream<S>(
        self: Arc<Self>,
        stream: S,
        peer: SocketAddr,
        shutdown: CancellationToken,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let connection_shutdown = shutdown.clone();
        let service = service_fn(move |request| {
            let proxy = Arc::clone(&self);
            let shutdown = shutdown.clone();
            async move { Ok::<_, hyper::Error>(proxy.handle(peer, request, shutdown).await) }
        });
        let connection = server_http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .with_upgrades();
        tokio::select! { () = connection_shutdown.cancelled() => {}, _ = connection => {} }
    }

    async fn handle(
        &self,
        peer: SocketAddr,
        request: Request<Incoming>,
        shutdown: CancellationToken,
    ) -> Response<ProxyBody> {
        if request.method() == Method::CONNECT {
            self.handle_connect(peer, request, shutdown).await
        } else {
            self.handle_http(peer, request, shutdown).await
        }
    }

    async fn handle_connect(
        &self,
        peer: SocketAddr,
        request: Request<Incoming>,
        shutdown: CancellationToken,
    ) -> Response<ProxyBody> {
        let Some((host, port)) = authority_of(request.uri(), None) else {
            return status(StatusCode::BAD_REQUEST, "CONNECT requires host:port");
        };
        let target = match self.checked(peer, "connect", &host, port).await {
            Ok(target) => target,
            Err(response) => return response,
        };
        let upstream = match self.connect(&target).await {
            Ok(stream) => stream,
            Err(response) => return response,
        };
        tokio::spawn(async move {
            match hyper::upgrade::on(request).await {
                Ok(upgraded) => {
                    let mut client = TokioIo::new(upgraded);
                    let mut upstream = upstream;
                    tokio::select! {
                        () = shutdown.cancelled() => {},
                        _ = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {},
                    }
                }
                Err(error) => {
                    tracing::debug!(target: LOG_TARGET, %error, "CONNECT upgrade failed");
                }
            }
        });
        Response::new(empty())
    }

    async fn handle_http(
        &self,
        peer: SocketAddr,
        request: Request<Incoming>,
        shutdown: CancellationToken,
    ) -> Response<ProxyBody> {
        if request.uri().scheme_str() != Some("http") {
            return status(
                StatusCode::BAD_REQUEST,
                "only absolute http:// requests or CONNECT are proxied",
            );
        }
        let Some((host, port)) = authority_of(request.uri(), Some(80)) else {
            return status(StatusCode::BAD_REQUEST, "request has no host");
        };
        let target = match self.checked(peer, "http", &host, port).await {
            Ok(target) => target,
            Err(response) => return response,
        };
        let upstream = match self.connect(&target).await {
            Ok(stream) => stream,
            Err(response) => return response,
        };
        let (mut parts, body) = request.into_parts();
        let origin_form = parts
            .uri
            .path_and_query()
            .map_or("/", hyper::http::uri::PathAndQuery::as_str)
            .to_owned();
        let Ok(uri) = origin_form.parse::<Uri>() else {
            return status(StatusCode::BAD_REQUEST, "invalid request target");
        };
        parts.uri = uri;
        strip_hop_by_hop(&mut parts.headers);
        let host_header = if port == 80 {
            host.clone()
        } else {
            format!("{host}:{port}")
        };
        if let Ok(value) = HeaderValue::from_str(&host_header) {
            parts.headers.insert(hyper::header::HOST, value);
        }
        let outgoing = Request::from_parts(parts, body);
        let Ok((mut sender, connection)) =
            hyper::client::conn::http1::handshake(TokioIo::new(upstream)).await
        else {
            return status(StatusCode::BAD_GATEWAY, "upstream handshake failed");
        };
        tokio::spawn(async move {
            tokio::select! { ()=shutdown.cancelled()=>{}, _=connection=>{} }
        });
        match sender.send_request(outgoing).await {
            Ok(response) => {
                let (mut parts, body) = response.into_parts();
                strip_hop_by_hop(&mut parts.headers);
                Response::from_parts(parts, body.boxed())
            }
            Err(_) => status(StatusCode::BAD_GATEWAY, "upstream request failed"),
        }
    }

    async fn checked(
        &self,
        _peer: SocketAddr,
        _kind: &'static str,
        host: &str,
        port: u16,
    ) -> Result<EgressDecision, Response<ProxyBody>> {
        let decision = self.decide(host, port).await;
        let denial = match decision {
            EgressDecision::Denied(reason) => Some(reason),
            EgressDecision::ResolveFailed => Some(DenyReason::ResolutionFailed),
            _ => None,
        };
        let mode = if matches!(decision, EgressDecision::Delegated { .. }) {
            ResolutionMode::UpstreamResolves
        } else {
            ResolutionMode::CheckedAddress
        };
        let event = EgressEvent {
            host: host.chars().take(253).collect(),
            port,
            mode,
            denial,
        };
        if let Err(reason) = self.audit.record(event) {
            return Err(denied(reason));
        }
        if let Some(reason) = denial {
            return Err(denied(reason));
        }
        Ok(decision)
    }

    async fn connect(&self, target: &EgressDecision) -> Result<TcpStream, Response<ProxyBody>> {
        let result = tokio::time::timeout(self.connect_timeout, async {
            match target {
                EgressDecision::Allowed(target) => match self.upstream {
                    Some(upstream) => {
                        super::upstream::tunnel(upstream, &target.address.to_string()).await
                    }
                    None => TcpStream::connect(target.address).await,
                },
                EgressDecision::Delegated { host, port } => match self.upstream {
                    Some(upstream) if upstream.upstream_resolves => {
                        super::upstream::tunnel(upstream, &format!("{host}:{port}")).await
                    }
                    _ => Err(std::io::Error::other("upstream resolution not configured")),
                },
                _ => Err(std::io::Error::other("unchecked destination")),
            }
        })
        .await;
        match result {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(_)) => Err(status(StatusCode::BAD_GATEWAY, "upstream connect failed")),
            Err(_) => Err(status(
                StatusCode::GATEWAY_TIMEOUT,
                "upstream connect timed out",
            )),
        }
    }
}

/// Binds the proxy listener.
pub async fn bind(config: &EgressProxyConfig) -> std::io::Result<TcpListener> {
    TcpListener::bind(config.listen).await
}

fn authority_of(uri: &Uri, default_port: Option<u16>) -> Option<(String, u16)> {
    let authority = uri.authority()?;
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    if host.is_empty() {
        return None;
    }
    let port = authority.port_u16().or(default_port)?;
    Some((host.to_owned(), port))
}

fn strip_hop_by_hop(headers: &mut hyper::HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(hyper::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in [
        "connection",
        "proxy-connection",
        "keep-alive",
        "proxy-authorization",
        "proxy-authenticate",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn denied(reason: DenyReason) -> Response<ProxyBody> {
    let mut response = status(StatusCode::FORBIDDEN, reason.as_str());
    response.headers_mut().insert(
        "x-sandbox-denial",
        HeaderValue::from_static(reason.as_str()),
    );
    response
}

fn empty() -> ProxyBody {
    Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed()
}

fn status(code: StatusCode, message: &'static str) -> Response<ProxyBody> {
    let mut response = Response::new(
        Full::new(Bytes::from_static(message.as_bytes()))
            .map_err(|never| match never {})
            .boxed(),
    );
    *response.status_mut() = code;
    // Refusals end the client connection instead of keeping it alive.
    response
        .headers_mut()
        .insert(hyper::header::CONNECTION, HeaderValue::from_static("close"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_parsing_handles_ipv6_and_defaults() {
        let uri: Uri = "http://[::1]:8080/x".parse().expect("uri");
        assert_eq!(authority_of(&uri, Some(80)), Some(("::1".to_owned(), 8080)));
        let uri: Uri = "http://example.org/x".parse().expect("uri");
        assert_eq!(
            authority_of(&uri, Some(80)),
            Some(("example.org".to_owned(), 80))
        );
        let uri: Uri = "example.org:443".parse().expect("uri");
        assert_eq!(
            authority_of(&uri, None),
            Some(("example.org".to_owned(), 443))
        );
        let uri: Uri = "example.org".parse().expect("uri");
        assert_eq!(authority_of(&uri, None), None);
    }

    #[test]
    fn hop_by_hop_and_proxy_credentials_are_stripped() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            "connection",
            HeaderValue::from_static("x-secret, keep-alive"),
        );
        headers.insert("x-secret", HeaderValue::from_static("1"));
        headers.insert("proxy-authorization", HeaderValue::from_static("Basic x"));
        headers.insert("accept", HeaderValue::from_static("*/*"));
        strip_hop_by_hop(&mut headers);
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key("accept"));
    }

    #[tokio::test]
    async fn literal_private_and_localhost_destinations_are_denied() {
        let proxy = EgressProxy::new(EgressPolicy::default(), Duration::from_secs(1));
        assert_eq!(
            proxy.decide("169.254.169.254", 80).await,
            EgressDecision::Denied(DenyReason::CloudMetadata)
        );
        assert_eq!(
            proxy.decide("[::1]", 80).await,
            EgressDecision::Denied(DenyReason::Loopback)
        );
        assert!(matches!(
            proxy.decide("localhost", 80).await,
            EgressDecision::Denied(DenyReason::Loopback) | EgressDecision::ResolveFailed
        ));
    }
}
