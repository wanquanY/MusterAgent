//! Message transports. A transport moves whole, size-bounded JSON-RPC
//! messages; correlation, deadlines and protocol state live in the session.

pub mod http;
#[cfg(any(test, feature = "test-support"))]
pub mod local_process;
pub mod process;
pub mod stdio;

use musteragent_runtime_content::ApplicationFuture;

use crate::McpClientError;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum McpTransportKind {
    Stdio,
    StreamableHttp,
}

/// A bidirectional MCP message channel.
///
/// * `send` writes one serialized JSON-RPC message (no trailing newline).
/// * `receive` yields the next complete inbound message, `Ok(None)` once the
///   peer closed the channel. Only one task (the session reader) calls it.
/// * `close` releases the channel (stdio: close stdin, wait, kill; HTTP:
///   terminate the session). It is idempotent.
pub trait McpTransport: Send + Sync {
    fn kind(&self) -> McpTransportKind;

    fn send(&self, message: Vec<u8>) -> ApplicationFuture<'_, Result<(), McpClientError>>;

    fn receive(&self) -> ApplicationFuture<'_, Result<Option<Vec<u8>>, McpClientError>>;

    /// Called once after `initialize` negotiated a protocol version.
    fn set_protocol_version(&self, _version: &str) {}

    fn close(&self) -> ApplicationFuture<'_, Result<(), McpClientError>>;
}
