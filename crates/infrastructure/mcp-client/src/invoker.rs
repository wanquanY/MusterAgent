//! Capability adapter shape for the tool executor.
//!
//! `McpToolInvoker` takes a server binding, a tool name, JSON arguments and a
//! deadline, and returns stored, provenance-marked content. It knows nothing
//! about tool-executor internals (ledger, grants, invocations): the executor
//! resolves a granted MCP tool to an [`McpToolInvocation`] and maps the
//! result back into its own output contract.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use musteragent_identity::TenantId;
use musteragent_mcp::{McpAuthorizationKind, McpServerName, McpTransport as McpTransportConfig};
use musteragent_runtime_content::{ApplicationFuture, ContentStore};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;
use url::Url;

use crate::McpClientError;
use crate::client::{McpCancellation, McpClientOptions, McpSession};
use crate::credentials::{McpCredentialPort, McpCredentialRequest};
use crate::result::{McpToolResult, materialize_tool_result};
use crate::transport::McpTransport;
use crate::transport::http::{StreamableHttpConfig, StreamableHttpTransport};
use crate::transport::process::{McpProcessSpec, ProcessLauncher};
use crate::transport::stdio::StdioTransport;

/// Admitted, validated server configuration for one tenant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpServerBinding {
    pub tenant_id: TenantId,
    pub server_name: McpServerName,
    pub transport: McpTransportConfig,
    pub authorization: McpAuthorizationKind,
}

impl McpServerBinding {
    pub fn validate(&self) -> Result<(), McpClientError> {
        self.transport
            .validate()
            .map_err(|_| McpClientError::InvalidConfiguration("transport"))?;
        if self.authorization != McpAuthorizationKind::None
            && !matches!(self.transport, McpTransportConfig::StreamableHttp { .. })
        {
            return Err(McpClientError::InvalidConfiguration("authorization"));
        }
        Ok(())
    }

    fn fingerprint(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"musteragent.mcp-client.binding.v1\0");
        let mut field = |bytes: &[u8]| {
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        };
        field(self.server_name.as_str().as_bytes());
        match &self.transport {
            McpTransportConfig::Stdio { command, arguments } => {
                field(b"stdio");
                field(command.as_bytes());
                for argument in arguments {
                    field(argument.as_bytes());
                }
            }
            McpTransportConfig::StreamableHttp { url } => {
                field(b"http");
                field(url.as_bytes());
            }
        }
        field(format!("{:?}", self.authorization).as_bytes());
        hasher.finalize().into()
    }

    fn credential_request(&self) -> McpCredentialRequest {
        McpCredentialRequest {
            tenant_id: self.tenant_id,
            server_name: self.server_name.clone(),
            authorization: self.authorization,
        }
    }
}

#[derive(Debug, Clone)]
pub struct McpToolInvocation {
    pub binding: McpServerBinding,
    pub tool_name: String,
    pub arguments: Value,
    pub deadline: Instant,
    pub cancellation: Option<McpCancellation>,
}

pub trait McpToolInvoker: Send + Sync {
    fn invoke(
        &self,
        invocation: McpToolInvocation,
    ) -> ApplicationFuture<'_, Result<McpToolResult, McpClientError>>;
}

/// HTTP transport settings shared by every binding of an invoker.
#[derive(Debug, Clone)]
pub struct McpHttpSettings {
    pub connect_timeout: Duration,
    pub response_timeout: Duration,
    pub proxy: Option<Url>,
    pub allow_insecure_http: bool,
}

impl Default for McpHttpSettings {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(60),
            proxy: None,
            allow_insecure_http: false,
        }
    }
}

type Slot = Arc<Mutex<Option<Arc<McpSession>>>>;

/// Session-pooling invoker over this crate's transports.
pub struct McpClientInvoker {
    content_store: Arc<dyn ContentStore>,
    credentials: Arc<dyn McpCredentialPort>,
    launcher: Option<Arc<dyn ProcessLauncher>>,
    options: McpClientOptions,
    http: McpHttpSettings,
    working_directory: Option<PathBuf>,
    sessions: StdMutex<HashMap<(TenantId, [u8; 32]), Slot>>,
}

impl McpClientInvoker {
    /// `launcher` is required for stdio bindings; without it they fail.
    #[must_use]
    pub fn new(
        content_store: Arc<dyn ContentStore>,
        credentials: Arc<dyn McpCredentialPort>,
        launcher: Option<Arc<dyn ProcessLauncher>>,
    ) -> Self {
        Self {
            content_store,
            credentials,
            launcher,
            options: McpClientOptions::default(),
            http: McpHttpSettings::default(),
            working_directory: None,
            sessions: StdMutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn with_options(mut self, options: McpClientOptions) -> Self {
        self.options = options;
        self
    }

    #[must_use]
    pub fn with_http_settings(mut self, http: McpHttpSettings) -> Self {
        self.http = http;
        self
    }

    #[must_use]
    pub fn with_working_directory(mut self, directory: PathBuf) -> Self {
        self.working_directory = Some(directory);
        self
    }

    /// Closes every pooled session.
    pub async fn shutdown(&self) {
        let slots: Vec<Slot> = self
            .sessions
            .lock()
            .map(|mut sessions| sessions.drain().map(|(_, slot)| slot).collect())
            .unwrap_or_default();
        for slot in slots {
            if let Some(session) = slot.lock().await.take() {
                let _ = session.shutdown().await;
            }
        }
    }

    async fn transport(
        &self,
        binding: &McpServerBinding,
    ) -> Result<Arc<dyn McpTransport>, McpClientError> {
        let limits = self.options.limits;
        match &binding.transport {
            McpTransportConfig::Stdio { command, arguments } => {
                let launcher = self
                    .launcher
                    .as_ref()
                    .ok_or(McpClientError::InvalidConfiguration("no process launcher"))?;
                let environment = self
                    .credentials
                    .stdio_environment(&binding.credential_request())
                    .await?;
                let spec = McpProcessSpec {
                    tenant_id: binding.tenant_id,
                    server_name: binding.server_name.clone(),
                    program: command.clone(),
                    arguments: arguments.clone(),
                    environment,
                    working_directory: self.working_directory.clone(),
                };
                let transport = StdioTransport::launch(
                    launcher.as_ref(),
                    spec,
                    limits.max_message_bytes,
                    limits.shutdown_grace,
                )
                .await?;
                Ok(Arc::new(transport))
            }
            McpTransportConfig::StreamableHttp { url } => {
                let endpoint = Url::parse(url)
                    .map_err(|_| McpClientError::InvalidConfiguration("endpoint"))?;
                let mut config = StreamableHttpConfig::new(endpoint, binding.credential_request());
                config.connect_timeout = self.http.connect_timeout;
                config.response_timeout = self.http.response_timeout;
                config.proxy = self.http.proxy.clone();
                config.allow_insecure_http = self.http.allow_insecure_http;
                config.max_message_bytes = limits.max_message_bytes;
                config.max_response_bytes = limits.max_http_response_bytes;
                Ok(Arc::new(StreamableHttpTransport::new(
                    config,
                    Arc::clone(&self.credentials),
                )?))
            }
        }
    }

    fn slot(&self, binding: &McpServerBinding) -> Result<Slot, McpClientError> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| McpClientError::Unavailable("session pool"))?;
        Ok(Arc::clone(
            sessions
                .entry((binding.tenant_id, binding.fingerprint()))
                .or_default(),
        ))
    }

    async fn session(
        &self,
        binding: &McpServerBinding,
        deadline: Instant,
    ) -> Result<Arc<McpSession>, McpClientError> {
        let slot = self.slot(binding)?;
        let mut slot = tokio::time::timeout_at(deadline, slot.lock())
            .await
            .map_err(|_| McpClientError::Timeout)?;
        if let Some(session) = slot.as_ref() {
            if session.is_open() {
                return Ok(Arc::clone(session));
            }
            if let Some(stale) = slot.take() {
                let _ = stale.shutdown().await;
            }
        }
        let transport = self.transport(binding).await?;
        let session =
            Arc::new(McpSession::connect(transport, self.options.clone(), deadline).await?);
        *slot = Some(Arc::clone(&session));
        Ok(session)
    }

    async fn invoke_once(
        &self,
        invocation: &McpToolInvocation,
    ) -> Result<McpToolResult, McpClientError> {
        let binding = &invocation.binding;
        let session = self.session(binding, invocation.deadline).await?;
        let tools = match session.cached_tools() {
            Some(tools) if !session.tools_changed() => tools,
            _ => session.list_tools(invocation.deadline).await?,
        };
        let descriptor = tools
            .get(&invocation.tool_name)
            .ok_or(McpClientError::UnknownTool)?;
        let raw = session
            .call_tool(
                &invocation.tool_name,
                invocation.arguments.clone(),
                invocation.deadline,
                invocation.cancellation.as_ref(),
            )
            .await?;
        materialize_tool_result(
            raw,
            &binding.server_name,
            descriptor,
            self.content_store.as_ref(),
            binding.tenant_id,
            &self.options.limits,
        )
        .await
    }
}

impl McpToolInvoker for McpClientInvoker {
    fn invoke(
        &self,
        invocation: McpToolInvocation,
    ) -> ApplicationFuture<'_, Result<McpToolResult, McpClientError>> {
        Box::pin(async move {
            invocation.binding.validate()?;
            if !invocation.arguments.is_object() {
                return Err(McpClientError::InvalidRequest(
                    "arguments must be an object",
                ));
            }
            // Never retried here: a tool call may have side effects. A stale
            // pooled session is replaced before the call is sent.
            self.invoke_once(&invocation).await
        })
    }
}
