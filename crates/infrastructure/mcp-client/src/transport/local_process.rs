//! Test-only launcher running the server as a local child process.
//! Production hosts must provide a sandboxed launcher instead.

use musteragent_runtime_content::ApplicationFuture;
use tokio::process::{Child, Command};

use super::process::{
    LaunchedMcpProcess, McpProcessControl, McpProcessExit, McpProcessSpec, ProcessLauncher,
};
use crate::McpClientError;

/// Spawns with a cleared environment containing only `spec.environment`
/// plus the explicitly allowed variables.
#[derive(Debug, Default, Clone)]
pub struct LocalProcessLauncher {
    inherited_environment: Vec<String>,
}

impl LocalProcessLauncher {
    #[must_use]
    pub fn inheriting(variables: &[&str]) -> Self {
        Self {
            inherited_environment: variables.iter().map(|name| (*name).to_owned()).collect(),
        }
    }
}

impl ProcessLauncher for LocalProcessLauncher {
    fn launch(
        &self,
        spec: McpProcessSpec,
    ) -> ApplicationFuture<'_, Result<LaunchedMcpProcess, McpClientError>> {
        Box::pin(async move {
            let mut command = Command::new(&spec.program);
            command
                .args(&spec.arguments)
                .env_clear()
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);
            for name in &self.inherited_environment {
                if let Some(value) = std::env::var_os(name) {
                    command.env(name, value);
                }
            }
            for (key, value) in spec.environment.iter() {
                command.env(key, value);
            }
            if let Some(directory) = &spec.working_directory {
                command.current_dir(directory);
            }
            let mut child = command
                .spawn()
                .map_err(|_| McpClientError::Launch("spawn"))?;
            let stdin = child.stdin.take().ok_or(McpClientError::Launch("stdin"))?;
            let stdout = child
                .stdout
                .take()
                .ok_or(McpClientError::Launch("stdout"))?;
            let stderr = child
                .stderr
                .take()
                .ok_or(McpClientError::Launch("stderr"))?;
            Ok(LaunchedMcpProcess {
                stdin: Box::new(stdin),
                stdout: Box::new(stdout),
                stderr: Some(Box::new(stderr)),
                control: Box::new(LocalChild(child)),
            })
        })
    }
}

struct LocalChild(Child);

fn exit(status: std::process::ExitStatus) -> McpProcessExit {
    McpProcessExit {
        code: status.code(),
    }
}

impl McpProcessControl for LocalChild {
    fn wait(&mut self) -> ApplicationFuture<'_, Result<McpProcessExit, McpClientError>> {
        Box::pin(async move {
            self.0
                .wait()
                .await
                .map(exit)
                .map_err(|_| McpClientError::Unavailable("wait"))
        })
    }

    fn kill(&mut self) -> ApplicationFuture<'_, Result<McpProcessExit, McpClientError>> {
        Box::pin(async move {
            if let Ok(Some(status)) = self.0.try_wait() {
                return Ok(exit(status));
            }
            let _ = self.0.start_kill();
            self.0
                .wait()
                .await
                .map(exit)
                .map_err(|_| McpClientError::Unavailable("kill"))
        })
    }
}
