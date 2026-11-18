use musteragent_runtime_content::ContentStoreError;
use thiserror::Error;

use crate::untrusted::UntrustedText;

/// Largest server-supplied error message retained, in bytes.
pub const MAX_REMOTE_ERROR_MESSAGE_BYTES: usize = 2_000;

/// A JSON-RPC error object returned by an MCP server.
///
/// The message is server-controlled text: it is retained only as untrusted
/// data and is deliberately excluded from `Display`, so it never reaches logs
/// or model-facing diagnostics without an explicit, provenance-aware choice.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpRemoteError {
    pub code: i64,
    pub message: UntrustedText,
}

impl std::fmt::Display for McpRemoteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "JSON-RPC error code {}", self.code)
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum McpClientError {
    #[error("MCP request is invalid: {0}")]
    InvalidRequest(&'static str),
    #[error("MCP server configuration is invalid: {0}")]
    InvalidConfiguration(&'static str),
    #[error("MCP server is unavailable: {0}")]
    Unavailable(&'static str),
    #[error("MCP request exceeded its deadline")]
    Timeout,
    #[error("MCP request was cancelled")]
    Cancelled,
    #[error("MCP session is closed")]
    SessionClosed,
    #[error("MCP HTTP session expired")]
    SessionExpired,
    #[error("MCP server requires authorization")]
    AuthorizationRequired,
    #[error("MCP HTTP transport returned status {0}")]
    HttpStatus(u16),
    #[error("MCP message exceeds the {limit} byte limit")]
    MessageTooLarge { limit: usize },
    #[error("MCP server violated the protocol: {0}")]
    ProtocolViolation(&'static str),
    #[error("MCP server negotiated an unsupported protocol version")]
    UnsupportedProtocolVersion(UntrustedText),
    #[error("MCP server rejected the request: {0}")]
    Remote(McpRemoteError),
    #[error("MCP Tool is not offered by the server")]
    UnknownTool,
    #[error("MCP Tool declared an output schema that cannot be compiled")]
    InvalidOutputSchema,
    #[error("MCP Tool structured output violates its declared output schema")]
    OutputSchemaViolation(Vec<UntrustedText>),
    #[error("MCP credential port failed: {0}")]
    Credentials(&'static str),
    #[error("MCP process launch failed: {0}")]
    Launch(&'static str),
    #[error("MCP result could not be stored: {0}")]
    Content(ContentStoreError),
}

impl McpClientError {
    /// Whether a fresh session may succeed. A tool call itself is never
    /// retried by this crate because MCP tools are not idempotent.
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self {
            Self::Unavailable(_)
            | Self::Timeout
            | Self::SessionClosed
            | Self::SessionExpired
            | Self::Launch(_) => true,
            Self::HttpStatus(status) => *status >= 500 || *status == 429,
            Self::Content(error) => error.retryable(),
            _ => false,
        }
    }

    /// Whether the session that produced this error is no longer usable.
    #[must_use]
    pub fn poisons_session(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(_)
                | Self::SessionClosed
                | Self::SessionExpired
                | Self::MessageTooLarge { .. }
                | Self::ProtocolViolation(_)
                | Self::AuthorizationRequired
        )
    }
}

impl From<ContentStoreError> for McpClientError {
    fn from(error: ContentStoreError) -> Self {
        Self::Content(error)
    }
}
