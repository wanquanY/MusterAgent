//! Tenant-bound stdio launcher. One fresh network-none Docker container per
//! MCP session; no process, filesystem or environment reuse across sessions.
use crate::{
    ContainerNetwork, ContainerSpec, DockerRuntime, ServerSandboxConfig,
    runtime::{CapturedStream, ContainerExit, ContainerOutcome},
    spec::EffectiveLimits,
};
use futures_util::StreamExt;
use musteragent_identity::{BrowserJobId, TenantId};
use musteragent_mcp_client::{
    LaunchedMcpProcess, McpClientError, McpProcessControl, McpProcessExit, McpProcessSpec,
    ProcessLauncher,
};
use musteragent_runtime_contract::ApplicationFuture;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Session terminal record. Hosts drain and persist these with their MCP run.
#[derive(Debug, Clone)]
pub struct McpSandboxResult {
    pub tenant_id: TenantId,
    pub session_id: BrowserJobId,
    pub outcome: ContainerOutcome,
    pub idle_timeout: bool,
}

/// Explicit limits for a long-lived MCP process. Activity on stdin/stdout
/// resets idle time; stderr does not keep a session alive.
#[derive(Debug, Clone, Copy)]
pub struct McpSandboxLimits {
    pub idle_timeout: Duration,
    pub wall_time: Duration,
    pub stdin_bytes: u64,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
}
impl Default for McpSandboxLimits {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(60),
            wall_time: Duration::from_secs(300),
            stdin_bytes: 8 * 1024 * 1024,
            stdout_bytes: 8 * 1024 * 1024,
            stderr_bytes: 1024 * 1024,
        }
    }
}

pub struct SandboxProcessLauncher {
    tenant: TenantId,
    config: ServerSandboxConfig,
    runtime: DockerRuntime,
    limits: McpSandboxLimits,
    results: Arc<Mutex<Vec<(McpSandboxResult, tokio::sync::OwnedSemaphorePermit)>>>,
    receipt_slots: Arc<tokio::sync::Semaphore>,
    sessions: Arc<tokio::sync::Semaphore>,
}
impl SandboxProcessLauncher {
    pub fn new(
        tenant: TenantId,
        config: ServerSandboxConfig,
        runtime: DockerRuntime,
        limits: McpSandboxLimits,
    ) -> Result<Self, McpClientError> {
        config
            .validate()
            .map_err(|_| McpClientError::Launch("invalid sandbox configuration"))?;
        if limits.idle_timeout.is_zero()
            || limits.wall_time.is_zero()
            || limits.stdin_bytes == 0
            || limits.stdout_bytes == 0
            || limits.stderr_bytes == 0
        {
            return Err(McpClientError::Launch("invalid MCP sandbox limits"));
        }
        let sessions = Arc::new(tokio::sync::Semaphore::new(
            config.max_concurrent_jobs as usize,
        ));
        Ok(Self {
            tenant,
            config,
            runtime,
            limits,
            results: Arc::default(),
            receipt_slots: Arc::new(tokio::sync::Semaphore::new(128)),
            sessions,
        })
    }
    /// Drains completed session receipts; only this tenant's sessions can launch.
    pub fn take_results(&self) -> Vec<McpSandboxResult> {
        self.results
            .lock()
            .map(|mut r| {
                std::mem::take(&mut *r)
                    .into_iter()
                    .map(|(result, _)| result)
                    .collect()
            })
            .unwrap_or_default()
    }
    fn spec(
        &self,
        request: &McpProcessSpec,
        id: BrowserJobId,
    ) -> Result<ContainerSpec, McpClientError> {
        if request.tenant_id != self.tenant {
            return Err(McpClientError::Launch("cross-tenant sandbox launch denied"));
        }
        if request.program.is_empty()
            || request.program.contains('\0')
            || request.arguments.iter().any(|s| s.contains('\0'))
        {
            return Err(McpClientError::Launch("invalid process command"));
        }
        let host = self.config.limits;
        let mut env: Vec<_> = self
            .config
            .image
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (key, value) in request.environment.iter() {
            if crate::config::RESERVED_ENV_KEYS.contains(&key)
                || key.contains('=')
                || key.contains('\0')
                || value.contains('\0')
            {
                return Err(McpClientError::Launch("reserved process environment"));
            }
            env.push((key.to_owned(), value.to_owned()));
        }
        env.push(("HOME".into(), "/tmp".into()));
        env.push(("TMPDIR".into(), "/tmp".into()));
        let mut labels = self.config.labels.clone();
        labels.insert(crate::LABEL_JOB.into(), id.to_string());
        labels.insert(crate::LABEL_PREFIX.into(), "mcp".into());
        let mut cmd = vec![request.program.clone()];
        cmd.extend(request.arguments.clone());
        let cwd = request.working_directory.as_ref().map_or_else(
            || "/sandbox/work".into(),
            |p| p.to_string_lossy().into_owned(),
        );
        if !cwd.starts_with('/') || cwd.contains('\0') {
            return Err(McpClientError::Launch(
                "invalid container working directory",
            ));
        }
        Ok(ContainerSpec {
            name: format!("musteragent-mcp-{id}"),
            image: self.config.image.reference.clone(),
            cmd,
            python_bin: self.config.image.python_bin().into(),
            env,
            user: self.config.container_user.clone(),
            working_dir: cwd,
            hostname: "sandbox".into(),
            labels,
            mounts: vec![],
            tmpfs: BTreeMap::from([
                (
                    "/tmp".into(),
                    format!("rw,noexec,nosuid,nodev,size={},mode=1777", host.tmpfs_bytes),
                ),
                (
                    "/sandbox/work".into(),
                    "rw,nosuid,nodev,size=8388608,mode=1777".into(),
                ),
            ]),
            read_only_rootfs: true,
            cap_drop_all: true,
            no_new_privileges: true,
            network: ContainerNetwork::None,
            limits: EffectiveLimits {
                wall_time: self
                    .limits
                    .wall_time
                    .min(Duration::from_millis(host.max_wall_time_millis)),
                nano_cpus: i64::from(host.cpu_millis) * 1_000_000,
                memory_bytes: host.memory_bytes as i64,
                pids: i64::from(host.pids),
                tmpfs_bytes: host.tmpfs_bytes,
                nofile: host.nofile,
                stdout_bytes: self.limits.stdout_bytes,
                stderr_bytes: self.limits.stderr_bytes,
                output_bytes: 8 * 1024 * 1024,
                output_files: 128,
            },
        }
        .with_network(ContainerNetwork::EgressProxy))
    }
}
impl ProcessLauncher for SandboxProcessLauncher {
    fn launch(
        &self,
        request: McpProcessSpec,
    ) -> ApplicationFuture<'_, Result<LaunchedMcpProcess, McpClientError>> {
        Box::pin(async move {
            let id = BrowserJobId::generate();
            let spec = self.spec(&request, id)?;
            let receipt_slot = Arc::clone(&self.receipt_slots)
                .try_acquire_owned()
                .map_err(|_| {
                    McpClientError::Launch(
                        "drain MCP sandbox receipts before launching more sessions",
                    )
                })?;
            let session_slot = Arc::clone(&self.sessions)
                .try_acquire_owned()
                .map_err(|_| McpClientError::Launch("MCP sandbox concurrency limit"))?;
            let mut session = crate::docker_session::DockerSession::start(
                self.runtime.docker.clone(),
                &spec,
                self.runtime.proxy.clone(),
            )
            .await
            .map_err(|e| {
                tracing::warn!(%e,"MCP sandbox launch failed");
                match e {
                    crate::RuntimeError::ImageMissing(_) => {
                        McpClientError::Launch("sandbox image missing")
                    }
                    crate::RuntimeError::Unavailable(_) => {
                        McpClientError::Unavailable("sandbox Docker unavailable")
                    }
                    _ => McpClientError::Launch("sandbox process or proxy could not start"),
                }
            })?;
            let (stdin, mut from_client) = tokio::io::duplex(65536);
            let (mut to_client, stdout) = tokio::io::duplex(65536);
            let (mut to_diagnostics, stderr) = tokio::io::duplex(65536);
            let cancel = CancellationToken::new();
            let token = cancel.clone();
            let (terminal, receiver) = watch::channel(None);
            let (activity, mut active) = watch::channel(Instant::now());
            let results = Arc::clone(&self.results);
            let tenant = self.tenant;
            let limits = self.limits;
            let mut input = std::mem::replace(&mut session.input, Box::pin(tokio::io::sink()));
            let input_cancel = token.clone();
            let input_activity = activity.clone();
            let (input_exceeded, mut input_limit) = watch::channel(false);
            let input_task = tokio::spawn(async move {
                let mut buffer = [0_u8; 16384];
                let mut total = 0_u64;
                loop {
                    let read = tokio::select! { ()=input_cancel.cancelled()=>break, r=from_client.read(&mut buffer)=>r };
                    match read {
                        Ok(n) if n > 0 => {
                            total = total.saturating_add(n as u64);
                            if total > limits.stdin_bytes {
                                input_exceeded.send_replace(true);
                                break;
                            }
                            let sent = tokio::select! { ()=input_cancel.cancelled()=>break, r=input.write_all(&buffer[..n])=>r };
                            if sent.is_err() {
                                break;
                            }
                            input_activity.send_replace(Instant::now());
                        }
                        _ => {
                            let _ = input.shutdown().await;
                            break;
                        }
                    }
                }
            });
            tokio::spawn(async move {
                let started = Instant::now();
                let deadline = started + spec.limits.wall_time;
                let mut out =
                    CapturedStream::new(limits.stdout_bytes, crate::MAX_BROWSER_JOB_LOG_TAIL_BYTES);
                let mut err =
                    CapturedStream::new(limits.stderr_bytes, crate::MAX_BROWSER_JOB_LOG_TAIL_BYTES);
                let mut tick = tokio::time::interval(Duration::from_millis(50));
                let _session_slot = session_slot;
                let mut runtime_error = None;
                let mut idle_timeout = false;
                let mut open = true;
                let exit = loop {
                    let idle = *active.borrow() + limits.idle_timeout;
                    tokio::select! {
                        biased;
                        ()=token.cancelled()=>break ContainerExit::Cancelled,
                        ()=tokio::time::sleep_until(deadline)=>break ContainerExit::TimedOut,
                        ()=tokio::time::sleep_until(idle)=>{idle_timeout=true;break ContainerExit::TimedOut;},
                        _=active.changed()=>{},
                        Ok(())=input_limit.changed()=>{if *input_limit.borrow(){break ContainerExit::InputLimitExceeded;}},
                        chunk=session.output.next(),if open=>match chunk {
                            Some(Ok(chunk))=>{
                                let (writer,message,exceeded)=match chunk {
                                    bollard::container::LogOutput::StdOut{message}|bollard::container::LogOutput::Console{message}=>{
                                        activity.send_replace(Instant::now());
                                        let exceeded=out.push(&message); (&mut to_client,message,exceeded)
                                    },
                                    bollard::container::LogOutput::StdErr{message}=>{let exceeded=err.push(&message);(&mut to_diagnostics,message,exceeded)},
                                    _=>continue,
                                };
                                if exceeded {break ContainerExit::OutputLimitExceeded;}
                                let sent=tokio::select! { ()=token.cancelled()=>break ContainerExit::Cancelled,
                                    r=tokio::time::timeout(Duration::from_secs(1),writer.write_all(&message))=>r };
                                if !matches!(sent,Ok(Ok(()))) {break ContainerExit::OutputLimitExceeded;}
                            },
                            Some(Err(e))=>{runtime_error=Some(crate::RuntimeError::Indeterminate(e.to_string()));break ContainerExit::RuntimeFailed;},
                            None=>open=false,
                        },
                        _=tick.tick()=>{
                            match session.exit_code().await {Ok(Some(code))=>break ContainerExit::Exited(code),Err(e)=>{runtime_error=Some(e);break ContainerExit::RuntimeFailed;},_=>{}}
                            if session.tunnel.as_ref().is_some_and(crate::egress::tunnel::ProxyTunnel::is_closed) {runtime_error=Some(crate::RuntimeError::Indeterminate("egress relay stopped".into()));break ContainerExit::RuntimeFailed;}
                        }
                    }
                };
                token.cancel();
                input_task.abort();
                if let Some(tunnel) = session.tunnel.as_mut() {
                    tunnel.stop().await;
                }
                let removed = session.guard.remove_now().await;
                if let Err(e) = &removed {
                    runtime_error = Some(e.clone());
                }
                let runtime_failed = runtime_error.is_some();
                let egress = session.egress();
                // Close both transport writers before publishing terminal completion.
                drop(to_client);
                drop(to_diagnostics);
                drop(session);
                let outcome = ContainerOutcome {
                    exit,
                    oom_killed: false,
                    stdout: out,
                    stderr: err,
                    elapsed: started.elapsed(),
                    egress,
                    runtime_error,
                };
                if let Ok(mut receipts) = results.lock() {
                    receipts.push((
                        McpSandboxResult {
                            tenant_id: tenant,
                            session_id: id,
                            outcome,
                            idle_timeout,
                        },
                        receipt_slot,
                    ));
                }
                let code = if let ContainerExit::Exited(c) = exit {
                    i32::try_from(c).ok()
                } else {
                    None
                };
                terminal.send_replace(Some(if runtime_failed {
                    Err(McpClientError::Unavailable(
                        "sandbox session runtime failed",
                    ))
                } else {
                    Ok(McpProcessExit { code })
                }));
            });
            Ok(LaunchedMcpProcess {
                stdin: Box::new(stdin),
                stdout: Box::new(stdout),
                stderr: Some(Box::new(stderr)),
                control: Box::new(SessionControl { cancel, receiver }),
            })
        })
    }
}
struct SessionControl {
    cancel: CancellationToken,
    receiver: watch::Receiver<Option<Result<McpProcessExit, McpClientError>>>,
}
impl SessionControl {
    async fn terminal(&mut self) -> Result<McpProcessExit, McpClientError> {
        loop {
            if let Some(result) = self.receiver.borrow().clone() {
                return result;
            }
            self.receiver
                .changed()
                .await
                .map_err(|_| McpClientError::Unavailable("sandbox supervisor stopped"))?;
        }
    }
}
impl McpProcessControl for SessionControl {
    fn wait(&mut self) -> ApplicationFuture<'_, Result<McpProcessExit, McpClientError>> {
        Box::pin(self.terminal())
    }
    fn kill(&mut self) -> ApplicationFuture<'_, Result<McpProcessExit, McpClientError>> {
        self.cancel.cancel();
        Box::pin(self.terminal())
    }
}
impl Drop for SessionControl {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
