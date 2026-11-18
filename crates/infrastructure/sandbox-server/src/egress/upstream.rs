//! Explicit HTTP upstream proxy chaining. Checked targets use IP CONNECT,
//! so the upstream cannot re-resolve a locally approved hostname.
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Host configuration, never inferred from process environment by the kernel.
/// Endpoint is an address so an upstream can work without local DNS. URL
/// credentials are deliberately unsupported; hosts supply a private endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamProxy {
    pub address: SocketAddr,
    #[serde(default)]
    pub upstream_resolves: bool,
}

pub(crate) async fn tunnel(upstream: UpstreamProxy, authority: &str) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(upstream.address).await?;
    stream
        .write_all(format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n").as_bytes())
        .await?;
    // Read exactly the header: do not consume any tunneled response bytes.
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        if header.len() >= 16 * 1024 {
            return Err(std::io::Error::other("upstream header too large"));
        }
        header.push(stream.read_u8().await?);
    }
    let status = header.split(|b| *b == b' ').nth(1);
    if status != Some(b"200") {
        return Err(std::io::Error::other("upstream refused CONNECT"));
    }
    Ok(stream)
}
