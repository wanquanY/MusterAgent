//! MCP session: lifecycle, request/response correlation, deadlines and
//! cancellation over any [`McpTransport`].

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{Value, json};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::jsonrpc::{
    Incoming, METHOD_NOT_FOUND, RequestId, decode_incoming, encode_error, encode_notification,
    encode_request, encode_result,
};
use crate::tools::{McpToolDescriptor, decode_tool_page};
use crate::transport::McpTransport;
use crate::untrusted::UntrustedText;
use crate::{McpClientError, McpClientLimits};

/// Protocol revisions this client implements, newest first. The newest is
/// offered in `initialize`; any listed revision is accepted in response.
pub const SUPPORTED_PROTOCOL_VERSIONS: [&str; 4] =
    ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
pub const LATEST_PROTOCOL_VERSION: &str = SUPPORTED_PROTOCOL_VERSIONS[0];

const MAX_SERVER_INFO_BYTES: usize = 256;

#[derive(Debug, Clone)]
pub struct McpClientOptions {
    pub client_name: String,
    pub client_version: String,
    pub limits: McpClientLimits,
}

impl Default for McpClientOptions {
    fn default() -> Self {
        Self {
            client_name: "musteragent".to_owned(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            limits: McpClientLimits::default(),
        }
    }
}

/// What the server reported during `initialize`. All text is untrusted.
#[derive(Debug, Clone)]
pub struct McpServerHandshake {
    pub protocol_version: String,
    pub server_name: UntrustedText,
    pub server_version: UntrustedText,
    pub instructions: Option<UntrustedText>,
    pub capabilities: Value,
}

impl McpServerHandshake {
    #[must_use]
    pub fn supports_tools(&self) -> bool {
        self.capabilities.get("tools").is_some_and(Value::is_object)
    }
}

/// Cooperative cancellation signal for an in-flight request.
#[derive(Debug, Clone, Default)]
pub struct McpCancellation {
    inner: Arc<(AtomicBool, Notify)>,
}

impl McpCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.inner.0.store(true, Ordering::Release);
        self.inner.1.notify_waiters();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.0.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.inner.1.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

/// Raw, shape-checked `tools/call` result. Use
/// [`crate::result::materialize_tool_result`] to store its content.
#[derive(Debug, Clone, PartialEq)]
pub struct RawToolResult {
    pub content: Vec<Value>,
    pub structured_content: Option<Value>,
    pub is_error: bool,
}

type Reply = Result<Value, McpClientError>;

struct PendingTable {
    next_id: u64,
    waiting: HashMap<u64, oneshot::Sender<Reply>>,
    retired: VecDeque<u64>,
    retired_set: HashSet<u64>,
}

impl PendingTable {
    fn retire(&mut self, id: u64, capacity: usize) {
        if self.retired_set.insert(id) {
            self.retired.push_back(id);
        }
        while self.retired.len() > capacity {
            if let Some(oldest) = self.retired.pop_front() {
                self.retired_set.remove(&oldest);
            }
        }
    }
}

struct Shared {
    transport: Arc<dyn McpTransport>,
    limits: McpClientLimits,
    pending: StdMutex<PendingTable>,
    failure: StdMutex<Option<McpClientError>>,
    closed: AtomicBool,
    tools_changed: AtomicBool,
    uncorrelated_errors: AtomicU64,
}

impl Shared {
    fn fail(&self, error: McpClientError) {
        if let Ok(mut failure) = self.failure.lock() {
            failure.get_or_insert(error.clone());
        }
        self.closed.store(true, Ordering::Release);
        let waiting = self
            .pending
            .lock()
            .map(|mut pending| std::mem::take(&mut pending.waiting))
            .unwrap_or_default();
        for (_, sender) in waiting {
            let _ = sender.send(Err(error.clone()));
        }
    }

    fn check_open(&self) -> Result<(), McpClientError> {
        if !self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        Err(self
            .failure
            .lock()
            .ok()
            .and_then(|failure| failure.clone())
            .unwrap_or(McpClientError::SessionClosed))
    }

    async fn notify(&self, method: &str, params: Value) -> Result<(), McpClientError> {
        let bytes =
            encode_notification(method, Some(params), self.limits.max_outbound_message_bytes)?;
        self.transport.send(bytes).await
    }
}

/// Removes the pending entry and, when the request was sent but not
/// answered, tells the server it was cancelled. Runs on every exit path,
/// including the caller dropping the future.
struct InFlight {
    shared: Arc<Shared>,
    id: u64,
    sent: bool,
    done: bool,
    cancellable: bool,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let capacity = self.shared.limits.retired_request_ids;
        if let Ok(mut pending) = self.shared.pending.lock() {
            pending.waiting.remove(&self.id);
            pending.retire(self.id, capacity);
        }
        if self.sent
            && !self.done
            && self.cancellable
            && !self.shared.closed.load(Ordering::Acquire)
        {
            let shared = Arc::clone(&self.shared);
            let id = self.id;
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = shared
                        .notify(
                            "notifications/cancelled",
                            json!({"requestId": id, "reason": "client cancelled the request"}),
                        )
                        .await;
                });
            }
        }
    }
}

/// An initialized MCP session.
pub struct McpSession {
    shared: Arc<Shared>,
    reader: StdMutex<Option<JoinHandle<()>>>,
    handshake: McpServerHandshake,
    tools: StdMutex<Option<Arc<BTreeMap<String, McpToolDescriptor>>>>,
}

impl McpSession {
    /// Starts the reader and performs the `initialize` handshake before
    /// `deadline`. On failure the transport is closed.
    pub async fn connect(
        transport: Arc<dyn McpTransport>,
        options: McpClientOptions,
        deadline: Instant,
    ) -> Result<Self, McpClientError> {
        options.limits.validate()?;
        let shared = Arc::new(Shared {
            transport: Arc::clone(&transport),
            limits: options.limits,
            pending: StdMutex::new(PendingTable {
                next_id: 1,
                waiting: HashMap::new(),
                retired: VecDeque::new(),
                retired_set: HashSet::new(),
            }),
            failure: StdMutex::new(None),
            closed: AtomicBool::new(false),
            tools_changed: AtomicBool::new(false),
            uncorrelated_errors: AtomicU64::new(0),
        });
        let reader = tokio::spawn(read_loop(Arc::clone(&shared)));
        let deadline = deadline.min(Instant::now() + options.limits.initialize_timeout);
        match initialize(&shared, &options, deadline).await {
            Ok(handshake) => Ok(Self {
                shared,
                reader: StdMutex::new(Some(reader)),
                handshake,
                tools: StdMutex::new(None),
            }),
            Err(error) => {
                shared.fail(McpClientError::SessionClosed);
                let _ = transport.close().await;
                reader.abort();
                Err(error)
            }
        }
    }

    #[must_use]
    pub fn handshake(&self) -> &McpServerHandshake {
        &self.handshake
    }

    #[must_use]
    pub fn is_open(&self) -> bool {
        self.shared.check_open().is_ok()
    }

    /// Error responses the server sent with `id: null`.
    #[must_use]
    pub fn uncorrelated_errors(&self) -> u64 {
        self.shared.uncorrelated_errors.load(Ordering::Relaxed)
    }

    /// True after the server sent `notifications/tools/list_changed` since
    /// the last `list_tools`.
    #[must_use]
    pub fn tools_changed(&self) -> bool {
        self.shared.tools_changed.load(Ordering::Acquire)
    }

    pub async fn ping(&self, deadline: Instant) -> Result<(), McpClientError> {
        let result = request(&self.shared, "ping", Some(json!({})), deadline, None, true).await?;
        if !result.is_object() {
            return Err(McpClientError::ProtocolViolation("ping result"));
        }
        Ok(())
    }

    /// Lists all tools (following pagination) and caches the descriptors
    /// used to validate structured output.
    pub async fn list_tools(
        &self,
        deadline: Instant,
    ) -> Result<Arc<BTreeMap<String, McpToolDescriptor>>, McpClientError> {
        let limits = self.shared.limits;
        self.shared.tools_changed.store(false, Ordering::Release);
        let mut tools = BTreeMap::new();
        let mut seen_cursors = HashSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..limits.max_tool_pages {
            let params = cursor
                .as_ref()
                .map_or_else(|| json!({}), |cursor| json!({"cursor": cursor}));
            let result = request(
                &self.shared,
                "tools/list",
                Some(params),
                deadline,
                None,
                true,
            )
            .await?;
            let page = decode_tool_page(&result, limits.max_tools, limits.max_description_bytes)?;
            for tool in page.tools {
                if tools.len() >= limits.max_tools {
                    return Err(McpClientError::ProtocolViolation("too many tools"));
                }
                if tools.insert(tool.name.clone(), tool).is_some() {
                    return Err(McpClientError::ProtocolViolation("duplicate tool name"));
                }
            }
            match page.next_cursor {
                None => {
                    let tools = Arc::new(tools);
                    if let Ok(mut cache) = self.tools.lock() {
                        *cache = Some(Arc::clone(&tools));
                    }
                    return Ok(tools);
                }
                Some(next) => {
                    if !seen_cursors.insert(next.clone()) {
                        return Err(McpClientError::ProtocolViolation("repeated cursor"));
                    }
                    cursor = Some(next);
                }
            }
        }
        Err(McpClientError::ProtocolViolation("too many tool pages"))
    }

    /// Descriptors from the most recent `list_tools`, if any.
    #[must_use]
    pub fn cached_tools(&self) -> Option<Arc<BTreeMap<String, McpToolDescriptor>>> {
        self.tools.lock().ok().and_then(|cache| cache.clone())
    }

    /// Calls a tool. On deadline expiry or cancellation the server receives
    /// `notifications/cancelled` and a late response is discarded.
    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        deadline: Instant,
        cancellation: Option<&McpCancellation>,
    ) -> Result<RawToolResult, McpClientError> {
        if name.is_empty()
            || name.len() > musteragent_mcp::MAX_MCP_TOOL_NAME_BYTES
            || name.chars().any(char::is_control)
        {
            return Err(McpClientError::InvalidRequest("tool name"));
        }
        if !arguments.is_object() {
            return Err(McpClientError::InvalidRequest(
                "arguments must be an object",
            ));
        }
        let result = request(
            &self.shared,
            "tools/call",
            Some(json!({"name": name, "arguments": arguments})),
            deadline,
            cancellation,
            true,
        )
        .await?;
        decode_call_result(result)
    }

    /// Fails pending requests and closes the transport (stdio: close stdin,
    /// wait, kill; HTTP: DELETE the session).
    pub async fn shutdown(&self) -> Result<(), McpClientError> {
        self.shared.fail(McpClientError::SessionClosed);
        let result = self.shared.transport.close().await;
        let reader = self.reader.lock().ok().and_then(|mut reader| reader.take());
        if let Some(mut reader) = reader
            && tokio::time::timeout(std::time::Duration::from_secs(1), &mut reader)
                .await
                .is_err()
        {
            reader.abort();
        }
        result
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        if let Ok(mut reader) = self.reader.lock()
            && let Some(reader) = reader.take()
        {
            reader.abort();
        }
    }
}

fn decode_call_result(result: Value) -> Result<RawToolResult, McpClientError> {
    let Value::Object(mut object) = result else {
        return Err(McpClientError::ProtocolViolation("tools/call result"));
    };
    let content = match object.remove("content") {
        Some(Value::Array(items)) => items,
        None => Vec::new(),
        Some(_) => return Err(McpClientError::ProtocolViolation("content")),
    };
    let is_error = match object.remove("isError") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(flag)) => flag,
        Some(_) => return Err(McpClientError::ProtocolViolation("isError")),
    };
    let structured_content = match object.remove("structuredContent") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value),
    };
    Ok(RawToolResult {
        content,
        structured_content,
        is_error,
    })
}

async fn initialize(
    shared: &Arc<Shared>,
    options: &McpClientOptions,
    deadline: Instant,
) -> Result<McpServerHandshake, McpClientError> {
    let params = json!({
        "protocolVersion": LATEST_PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": {"name": options.client_name, "version": options.client_version}
    });
    // The initialize request must not be cancelled (MCP lifecycle rule).
    let result = request(shared, "initialize", Some(params), deadline, None, false).await?;
    let version = result
        .get("protocolVersion")
        .and_then(Value::as_str)
        .ok_or(McpClientError::ProtocolViolation("protocolVersion"))?;
    if !SUPPORTED_PROTOCOL_VERSIONS.contains(&version) {
        return Err(McpClientError::UnsupportedProtocolVersion(
            UntrustedText::bounded(version, 64),
        ));
    }
    let capabilities = result
        .get("capabilities")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or(McpClientError::ProtocolViolation("capabilities"))?;
    let info = result
        .get("serverInfo")
        .ok_or(McpClientError::ProtocolViolation("serverInfo"))?;
    let info_text = |key: &str| {
        UntrustedText::bounded(
            info.get(key).and_then(Value::as_str).unwrap_or_default(),
            MAX_SERVER_INFO_BYTES,
        )
    };
    let handshake = McpServerHandshake {
        protocol_version: version.to_owned(),
        server_name: info_text("name"),
        server_version: info_text("version"),
        instructions: result
            .get("instructions")
            .and_then(Value::as_str)
            .map(|text| UntrustedText::bounded(text, shared.limits.max_description_bytes)),
        capabilities,
    };
    shared.transport.set_protocol_version(version);
    shared
        .notify("notifications/initialized", json!({}))
        .await?;
    Ok(handshake)
}

async fn request(
    shared: &Arc<Shared>,
    method: &str,
    params: Option<Value>,
    deadline: Instant,
    cancellation: Option<&McpCancellation>,
    cancellable: bool,
) -> Result<Value, McpClientError> {
    shared.check_open()?;
    if cancellation.is_some_and(McpCancellation::is_cancelled) {
        return Err(McpClientError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(McpClientError::Timeout);
    }
    let (sender, receiver) = oneshot::channel();
    let id = {
        let mut pending = shared
            .pending
            .lock()
            .map_err(|_| McpClientError::Unavailable("pending table"))?;
        let id = pending.next_id;
        pending.next_id = id
            .checked_add(1)
            .filter(|next| *next <= i64::MAX as u64)
            .ok_or(McpClientError::Unavailable("request ids exhausted"))?;
        pending.waiting.insert(id, sender);
        id
    };
    let mut in_flight = InFlight {
        shared: Arc::clone(shared),
        id,
        sent: false,
        done: false,
        cancellable,
    };
    let bytes = encode_request(id, method, params, shared.limits.max_outbound_message_bytes)?;
    // Mark as sent before writing: a partially written request may still
    // reach the server, so cancellation must be announced.
    in_flight.sent = true;
    let exchange = async {
        shared.transport.send(bytes).await?;
        receiver.await.map_err(|_| McpClientError::SessionClosed)?
    };
    let cancelled = async {
        match cancellation {
            Some(cancellation) => cancellation.cancelled().await,
            None => std::future::pending().await,
        }
    };
    let outcome = tokio::select! {
        outcome = exchange => outcome,
        () = tokio::time::sleep_until(deadline) => Err(McpClientError::Timeout),
        () = cancelled => Err(McpClientError::Cancelled),
    };
    in_flight.done = !matches!(
        outcome,
        Err(McpClientError::Timeout | McpClientError::Cancelled)
    );
    if let Err(error) = &outcome
        && error.poisons_session()
    {
        shared.fail(error.clone());
    }
    outcome
}

async fn read_loop(shared: Arc<Shared>) {
    loop {
        let message = match shared.transport.receive().await {
            Ok(Some(message)) => message,
            Ok(None) => {
                shared.fail(McpClientError::Unavailable("transport closed"));
                return;
            }
            Err(error) => {
                shared.fail(error);
                return;
            }
        };
        let decoded = match decode_incoming(
            &message,
            shared.limits.max_message_bytes,
            shared.limits.max_batch_messages,
        ) {
            Ok(decoded) => decoded,
            Err(error) => {
                shared.fail(error);
                let _ = shared.transport.close().await;
                return;
            }
        };
        for incoming in decoded {
            if let Err(error) = dispatch(&shared, incoming).await {
                shared.fail(error);
                let _ = shared.transport.close().await;
                return;
            }
        }
    }
}

async fn dispatch(shared: &Arc<Shared>, incoming: Incoming) -> Result<(), McpClientError> {
    match incoming {
        Incoming::Response { id, outcome } => {
            let RequestId::Number(number) = id else {
                return Err(McpClientError::ProtocolViolation(
                    "response id was never issued",
                ));
            };
            let number = u64::try_from(number)
                .map_err(|_| McpClientError::ProtocolViolation("response id was never issued"))?;
            let mut pending = shared
                .pending
                .lock()
                .map_err(|_| McpClientError::Unavailable("pending table"))?;
            if let Some(sender) = pending.waiting.remove(&number) {
                let capacity = shared.limits.retired_request_ids;
                pending.retire(number, capacity);
                drop(pending);
                let _ = sender.send(outcome.map_err(McpClientError::Remote));
                Ok(())
            } else if pending.retired_set.contains(&number) {
                // Late answer to a request that already timed out, was
                // cancelled or was answered: discard.
                Ok(())
            } else {
                Err(McpClientError::ProtocolViolation(
                    "response id was never issued",
                ))
            }
        }
        Incoming::UncorrelatedError(_) => {
            shared.uncorrelated_errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("mcp server sent an uncorrelated JSON-RPC error");
            Ok(())
        }
        Incoming::Request { id, method, .. } => {
            let limit = shared.limits.max_outbound_message_bytes;
            let reply = if method == "ping" {
                encode_result(&id, json!({}), limit)?
            } else {
                // The client advertises no capabilities (sampling, roots,
                // elicitation), so every other server request is refused.
                encode_error(&id, METHOD_NOT_FOUND, "Method not found", limit)?
            };
            shared.transport.send(reply).await
        }
        Incoming::Notification { method, .. } => {
            if method == "notifications/tools/list_changed" {
                shared.tools_changed.store(true, Ordering::Release);
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests;
