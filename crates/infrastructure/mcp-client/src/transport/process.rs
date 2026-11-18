//! Process launch contract for stdio MCP servers.
//!
//! The client never spawns processes itself. A host supplies a
//! [`ProcessLauncher`]; in the cloud this is the job sandbox, which starts the
//! server inside its per-job container behind the egress proxy and hands back
//! the attached stdio streams.

use std::path::PathBuf;

use musteragent_identity::TenantId;
use musteragent_mcp::McpServerName;
use musteragent_runtime_content::ApplicationFuture;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::McpClientError;
use crate::credentials::SecretEnvironment;

/// What to start. `program` and `arguments` come from validated server
/// configuration (`musteragent_mcp::McpTransport::Stdio`); `environment`
/// comes only from the credential port.
#[derive(Debug)]
pub struct McpProcessSpec {
    pub tenant_id: TenantId,
    pub server_name: McpServerName,
    pub program: String,
    pub arguments: Vec<String>,
    pub environment: SecretEnvironment,
    pub working_directory: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct McpProcessExit {
    /// `None` when terminated by a signal or the launcher cannot tell.
    pub code: Option<i32>,
}

/// Lifetime control of a launched server.
pub trait McpProcessControl: Send + Sync {
    /// Resolves when the process has exited and been reaped.
    fn wait(&mut self) -> ApplicationFuture<'_, Result<McpProcessExit, McpClientError>>;

    /// Forcibly terminates the process (and anything it spawned, where the
    /// launcher can), then reaps it. Must be safe to call after exit.
    fn kill(&mut self) -> ApplicationFuture<'_, Result<McpProcessExit, McpClientError>>;
}

pub type BoxedAsyncWrite = Box<dyn AsyncWrite + Send + Unpin>;
pub type BoxedAsyncRead = Box<dyn AsyncRead + Send + Unpin>;

pub struct LaunchedMcpProcess {
    /// Server stdin; dropping it signals end of input.
    pub stdin: BoxedAsyncWrite,
    /// Server stdout carrying newline-delimited JSON-RPC.
    pub stdout: BoxedAsyncRead,
    /// Server stderr (diagnostics only, never parsed as protocol).
    pub stderr: Option<BoxedAsyncRead>,
    pub control: Box<dyn McpProcessControl>,
}

pub trait ProcessLauncher: Send + Sync {
    fn launch(
        &self,
        spec: McpProcessSpec,
    ) -> ApplicationFuture<'_, Result<LaunchedMcpProcess, McpClientError>>;
}
