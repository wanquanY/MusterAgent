//! Cloud-side Model Context Protocol client.
//!
//! * [`transport`]: one [`McpTransport`] trait with a Streamable HTTP
//!   implementation and a stdio implementation over a host-supplied
//!   [`ProcessLauncher`] (the job sandbox in production).
//! * [`McpSession`]: initialize/version negotiation, `tools/list`
//!   pagination, `tools/call`, ping, cancellation, shutdown, strict JSON-RPC
//!   id correlation and bounded messages.
//! * [`materialize_tool_result`]: treats every byte of server output as
//!   untrusted data, validates structured output against the declared
//!   schema and stores content as `ContentRef`s with explicit truncation.
//! * [`McpToolInvoker`]: the adapter shape the tool executor calls.
//!
//! Credentials are only ever obtained from an [`McpCredentialPort`]; they are
//! never part of a configuration value.

#![forbid(unsafe_code)]

pub mod client;
pub mod credentials;
mod error;
pub mod invoker;
pub mod jsonrpc;
mod limits;
pub mod result;
pub mod sse;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod tools;
pub mod transport;
mod untrusted;

pub use client::{
    LATEST_PROTOCOL_VERSION, McpCancellation, McpClientOptions, McpServerHandshake, McpSession,
    RawToolResult, SUPPORTED_PROTOCOL_VERSIONS,
};
pub use credentials::{
    McpCredentialPort, McpCredentialRequest, McpHttpCredentials, NoCredentials, SecretEnvironment,
};
pub use error::{McpClientError, McpRemoteError};
pub use invoker::{
    McpClientInvoker, McpHttpSettings, McpServerBinding, McpToolInvocation, McpToolInvoker,
};
pub use limits::McpClientLimits;
pub use result::{
    McpContentItem, McpContentKind, McpProvenance, McpStructuredContent, McpToolResult,
    OmissionReason, OmittedContent, StructuredValidation, Truncation, materialize_tool_result,
};
pub use tools::McpToolDescriptor;
pub use transport::http::{StreamableHttpConfig, StreamableHttpTransport};
pub use transport::process::{
    LaunchedMcpProcess, McpProcessControl, McpProcessExit, McpProcessSpec, ProcessLauncher,
};
pub use transport::stdio::StdioTransport;
pub use transport::{McpTransport, McpTransportKind};
pub use untrusted::UntrustedText;
