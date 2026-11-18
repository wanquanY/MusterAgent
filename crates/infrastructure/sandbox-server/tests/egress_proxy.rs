//! Proxy transport tests use an explicit local upstream with a checked public
//! address. Private destinations remain denied even with allow-list entries.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use musteragent_sandbox_server::egress::{EgressPolicy, EgressProxy};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// Minimal HTTP/1.1 origin that answers every request with its request line.
async fn origin() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind origin");
    let address = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buffer = vec![0_u8; 4096];
                let read = stream.read(&mut buffer).await.unwrap_or(0);
                let first = String::from_utf8_lossy(&buffer[..read]).into_owned();
                let request = if first.starts_with("CONNECT ") {
                    stream
                        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                        .await
                        .expect("upstream CONNECT");
                    let read = stream.read(&mut buffer).await.expect("tunnel request");
                    String::from_utf8_lossy(&buffer[..read]).into_owned()
                } else {
                    first
                };
                let line = request.lines().next().unwrap_or_default().to_owned();
                let host = request
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("host:"))
                    .unwrap_or_default()
                    .to_owned();
                let leaked = request.to_ascii_lowercase().contains("proxy-authorization");
                let body = format!("{line}|{host}|leaked={leaked}");
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    address
}

async fn proxy(policy: EgressPolicy) -> (SocketAddr, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let address = listener.local_addr().expect("addr");
    let shutdown = CancellationToken::new();
    let proxy = Arc::new(EgressProxy::new(policy, Duration::from_secs(2)));
    tokio::spawn(proxy.serve(listener, shutdown.clone()));
    (address, shutdown)
}

async fn roundtrip(proxy: SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(proxy).await.expect("connect proxy");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await;
    String::from_utf8_lossy(&response).into_owned()
}

#[derive(Debug)]
struct PublicResolver;
impl musteragent_sandbox_server::egress::Resolver for PublicResolver {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<Vec<std::net::IpAddr>, musteragent_sandbox_server::egress::ResolveError>,
    > {
        Box::pin(async { Ok(vec!["93.184.216.34".parse().expect("public IP")]) })
    }
}
async fn chained(origin: SocketAddr) -> (SocketAddr, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy");
    let addr = listener.local_addr().expect("address");
    let shutdown = CancellationToken::new();
    let proxy = EgressProxy::new(EgressPolicy::default(), Duration::from_secs(2))
        .with_resolver(Arc::new(PublicResolver))
        .with_upstream(musteragent_sandbox_server::egress::UpstreamProxy {
            address: origin,
            upstream_resolves: false,
        });
    tokio::spawn(Arc::new(proxy).serve(listener, shutdown.clone()));
    (addr, shutdown)
}

#[tokio::test]
async fn plain_http_is_forwarded_in_origin_form_without_proxy_credentials() {
    let origin = origin().await;
    let (proxy, shutdown) = chained(origin).await;
    let response = roundtrip(
        proxy,
        &format!(
            "GET http://public.example:{port}/data?x=1 HTTP/1.1\r\nHost: ignored\r\nProxy-Authorization: Basic c2VjcmV0\r\nConnection: close\r\n\r\n",
            port = origin.port()
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("GET /data?x=1 HTTP/1.1"), "{response}");
    assert!(
        response.contains(&format!("public.example:{}", origin.port())),
        "{response}"
    );
    assert!(response.contains("leaked=false"), "{response}");
    shutdown.cancel();
}

#[tokio::test]
async fn connect_tunnels_bytes_to_an_allowed_destination() {
    let origin = origin().await;
    let (proxy, shutdown) = chained(origin).await;
    let mut stream = TcpStream::connect(proxy).await.expect("connect proxy");
    stream
        .write_all(
            format!(
                "CONNECT public.example:{0} HTTP/1.1\r\nHost: public.example:{0}\r\n\r\n",
                origin.port()
            )
            .as_bytes(),
        )
        .await
        .expect("connect request");
    let mut head = vec![0_u8; 1024];
    let read = stream.read(&mut head).await.expect("read head");
    let head = String::from_utf8_lossy(&head[..read]).into_owned();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    stream
        .write_all(b"GET /tunnel HTTP/1.1\r\nHost: origin\r\nConnection: close\r\n\r\n")
        .await
        .expect("tunnel write");
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await;
    let response = String::from_utf8_lossy(&response);
    assert!(response.contains("GET /tunnel HTTP/1.1"), "{response}");
    shutdown.cancel();
}

#[tokio::test]
async fn default_policy_refuses_loopback_metadata_and_private_targets() {
    let origin = origin().await;
    let (proxy, shutdown) = proxy(EgressPolicy {
        allowed_ports: None,
        ..Default::default()
    })
    .await;
    for target in [
        format!("127.0.0.1:{}", origin.port()),
        "169.254.169.254:80".to_owned(),
        "100.100.100.200:80".to_owned(),
        "10.0.0.1:443".to_owned(),
        "[fd00::1]:443".to_owned(),
        "[::ffff:127.0.0.1]:80".to_owned(),
    ] {
        let response = roundtrip(
            proxy,
            &format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n"),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 403"), "{target}: {response}");
    }
    let response = roundtrip(
        proxy,
        &format!(
            "GET http://localhost:{}/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            origin.port()
        ),
    )
    .await;
    assert!(
        response.starts_with("HTTP/1.1 403") || response.starts_with("HTTP/1.1 502"),
        "{response}"
    );
    shutdown.cancel();
}

#[tokio::test]
async fn port_and_host_deny_lists_apply_before_resolution() {
    let (proxy, shutdown) = proxy(EgressPolicy {
        deny_hosts: vec!["blocked.example".to_owned()],
        allowed_ports: Some(BTreeSet::from([443])),
        ..Default::default()
    })
    .await;
    let response = roundtrip(
        proxy,
        "CONNECT api.blocked.example:443 HTTP/1.1\r\nHost: x\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    let response = roundtrip(proxy, "CONNECT 1.1.1.1:22 HTTP/1.1\r\nHost: x\r\n\r\n").await;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    let response = roundtrip(
        proxy,
        "GET /relative HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    shutdown.cancel();
}

#[derive(Debug)]
struct RebindingResolver(std::sync::atomic::AtomicUsize);
impl musteragent_sandbox_server::egress::Resolver for RebindingResolver {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<Vec<std::net::IpAddr>, musteragent_sandbox_server::egress::ResolveError>,
    > {
        Box::pin(async move {
            let previous = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(vec![
                if previous == 0 {
                    "93.184.216.34"
                } else {
                    "10.0.0.1"
                }
                .parse()
                .expect("IP"),
            ])
        })
    }
}
#[tokio::test]
async fn checked_upstream_connect_uses_checked_ip_and_rebinding_is_denied() {
    use musteragent_sandbox_server::egress::{DenyReason, ResolutionMode, UpstreamProxy};
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("upstream");
    let address = listener.local_addr().expect("address");
    let observed = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("first checked request");
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await.expect("header"));
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\n\r\n")
            .await
            .expect("reply");
        drop(stream);
        assert!(
            tokio::time::timeout(Duration::from_millis(250), listener.accept())
                .await
                .is_err(),
            "denied second request reached upstream"
        );
        String::from_utf8(request).expect("ASCII")
    });
    let resolver = Arc::new(RebindingResolver(std::sync::atomic::AtomicUsize::new(0)));
    let proxy = Arc::new(
        EgressProxy::new(EgressPolicy::default(), Duration::from_secs(2))
            .with_resolver(resolver.clone())
            .with_upstream(UpstreamProxy {
                address,
                upstream_resolves: false,
            }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy");
    let address = listener.local_addr().expect("address");
    let stop = CancellationToken::new();
    tokio::spawn(proxy.clone().serve(listener, stop.clone()));
    let first = roundtrip(
        address,
        "CONNECT rebound.example:443 HTTP/1.1\r\nHost: rebound.example\r\n\r\n",
    )
    .await;
    assert!(first.starts_with("HTTP/1.1 200"));
    let second=roundtrip(address,"CONNECT rebound.example:443 HTTP/1.1\r\nHost: rebound.example\r\nConnection: close\r\n\r\n").await;
    assert!(second.starts_with("HTTP/1.1 403"));
    assert!(second.contains("x-sandbox-denial: private"));
    assert!(
        observed
            .await
            .expect("listener")
            .starts_with("CONNECT 93.184.216.34:443 HTTP/1.1")
    );
    assert_eq!(resolver.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    let facts = proxy.audit().events();
    assert_eq!(facts.len(), 2);
    assert_eq!(facts[0].mode, ResolutionMode::CheckedAddress);
    assert_eq!(facts[1].denial, Some(DenyReason::Private));
    stop.cancel();
}
