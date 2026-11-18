use std::time::Duration;

use crate::McpClientError;

/// Every bound the client enforces against an MCP server. All values are
/// hard caps: exceeding a protocol bound fails the message, exceeding a
/// result bound truncates or omits content and reports it explicitly.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct McpClientLimits {
    /// Largest single inbound JSON-RPC message (one stdio line, one SSE
    /// event, or one `application/json` body).
    pub max_message_bytes: usize,
    /// Largest outbound JSON-RPC message.
    pub max_outbound_message_bytes: usize,
    /// Largest total body of one HTTP response (an SSE stream may carry
    /// several messages).
    pub max_http_response_bytes: usize,
    /// Messages in one JSON-RPC batch received from a server.
    pub max_batch_messages: usize,
    /// `tools/list` pagination bounds.
    pub max_tool_pages: usize,
    pub max_tools: usize,
    /// Largest single stored content item of a tool result.
    pub max_content_item_bytes: usize,
    /// Total stored bytes of one tool result across all items.
    pub max_result_bytes: usize,
    /// Content items of one tool result.
    pub max_content_items: usize,
    /// Largest stored `structuredContent` JSON.
    pub max_structured_bytes: usize,
    /// Server-provided descriptive text (descriptions, instructions).
    pub max_description_bytes: usize,
    /// Deadline applied to `initialize` when the caller gives none.
    pub initialize_timeout: Duration,
    /// Grace for a stdio server to exit after stdin closes.
    pub shutdown_grace: Duration,
    /// Late responses for cancelled ids remembered (and silently dropped).
    pub retired_request_ids: usize,
}

impl Default for McpClientLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 8 * 1024 * 1024,
            max_outbound_message_bytes: 4 * 1024 * 1024,
            max_http_response_bytes: 32 * 1024 * 1024,
            max_batch_messages: 64,
            max_tool_pages: 100,
            max_tools: 2_048,
            max_content_item_bytes: 4 * 1024 * 1024,
            max_result_bytes: 16 * 1024 * 1024,
            max_content_items: 64,
            max_structured_bytes: 1024 * 1024,
            max_description_bytes: 16 * 1024,
            initialize_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(2),
            retired_request_ids: 1_024,
        }
    }
}

impl McpClientLimits {
    pub fn validate(&self) -> Result<(), McpClientError> {
        let sizes = [
            self.max_message_bytes,
            self.max_outbound_message_bytes,
            self.max_http_response_bytes,
            self.max_batch_messages,
            self.max_tool_pages,
            self.max_tools,
            self.max_content_item_bytes,
            self.max_result_bytes,
            self.max_content_items,
            self.max_structured_bytes,
            self.max_description_bytes,
        ];
        if sizes.contains(&0)
            || self.max_http_response_bytes < self.max_message_bytes
            || self.max_message_bytes > 256 * 1024 * 1024
            || self.initialize_timeout.is_zero()
        {
            return Err(McpClientError::InvalidConfiguration("limits"));
        }
        Ok(())
    }
}
