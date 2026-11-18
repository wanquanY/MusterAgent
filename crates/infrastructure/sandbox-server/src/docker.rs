//! Docker adapter for [`ContainerRuntime`] using `bollard`.
//!
//! One container per job: create → attach → start → wait (bounded by wall
//! time, stream caps and cancellation) → kill if needed → inspect → remove.
//! The container is removed on every path, including drop of the future.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bollard::Docker;
use bollard::container::LogOutput;
use bollard::errors::Error as DockerError;
use bollard::models::{
    ContainerCreateBody, HostConfig, HostConfigLogConfig, Mount, MountTypeEnum, ResourcesUlimits,
};
use bollard::query_parameters::{ListContainersOptionsBuilder, RemoveContainerOptionsBuilder};
use futures_util::StreamExt;
use musteragent_runtime_contract::ApplicationFuture;
use tokio_util::sync::CancellationToken;

use crate::runtime::{
    CapturedStream, ContainerExit, ContainerOutcome, ContainerRuntime, RuntimeError,
};
#[cfg(test)]
use crate::spec::ContainerNetwork;
use crate::spec::{ContainerSpec, EffectiveLimits};

const KILL_SETTLE: Duration = Duration::from_secs(10);
const OUTPUT_DRAIN: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct DockerRuntime {
    pub(crate) docker: Docker,
    pub(crate) proxy: Option<Arc<crate::egress::EgressProxy>>,
}

impl DockerRuntime {
    /// Connects to the local daemon socket (`DOCKER_HOST` with a `unix://`
    /// scheme, else `/var/run/docker.sock`).
    pub fn connect_local() -> Result<Self, RuntimeError> {
        Docker::connect_with_local_defaults()
            .map(|docker| Self {
                docker,
                proxy: None,
            })
            .map_err(|error| RuntimeError::Unavailable(error.to_string()))
    }

    #[must_use]
    pub const fn from_client(docker: Docker) -> Self {
        Self {
            docker,
            proxy: None,
        }
    }

    /// Configure the only egress authority available to networked workloads.
    #[must_use]
    pub fn with_proxy(mut self, proxy: crate::egress::EgressProxy) -> Self {
        self.proxy = Some(Arc::new(proxy));
        self
    }

    pub async fn ping(&self) -> Result<(), RuntimeError> {
        self.docker
            .ping()
            .await
            .map(|_| ())
            .map_err(|error| RuntimeError::Unavailable(error.to_string()))
    }

    pub async fn image_present(&self, image: &str) -> Result<bool, RuntimeError> {
        match self.docker.inspect_image(image).await {
            Ok(_) => Ok(true),
            Err(DockerError::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(false),
            Err(error) => Err(RuntimeError::Unavailable(error.to_string())),
        }
    }

    /// Force-removes every container carrying `key=value` (crash leftovers of
    /// a previous process). Returns how many were removed.
    pub async fn remove_labelled(&self, key: &str, value: &str) -> Result<usize, RuntimeError> {
        let filters = HashMap::from([("label", vec![format!("{key}={value}")])]);
        let containers = self
            .docker
            .list_containers(Some(
                ListContainersOptionsBuilder::default()
                    .all(true)
                    .filters(&filters)
                    .build(),
            ))
            .await
            .map_err(|error| RuntimeError::Unavailable(error.to_string()))?;
        let mut removed = 0;
        for container in containers {
            if let Some(id) = container.id {
                force_remove(&self.docker, &id).await;
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn run_inner(
        &self,
        spec: &ContainerSpec,
        cancel: CancellationToken,
    ) -> Result<ContainerOutcome, RuntimeError> {
        let mut session = crate::docker_session::DockerSession::start(
            self.docker.clone(),
            spec,
            self.proxy.clone(),
        )
        .await?;
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + spec.limits.wall_time;
        let mut stdout =
            CapturedStream::new(spec.limits.stdout_bytes, EffectiveLimits::tail_bytes());
        let mut stderr =
            CapturedStream::new(spec.limits.stderr_bytes, EffectiveLimits::tail_bytes());
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        let mut output_open = true;
        let mut runtime_error = None;
        let mut exit = loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break ContainerExit::Cancelled,
                () = tokio::time::sleep_until(deadline) => break ContainerExit::TimedOut,
                item = session.output.next(), if output_open => match item {
                    Some(Ok(chunk)) => if capture(&mut stdout, &mut stderr, chunk) { break ContainerExit::OutputLimitExceeded; },
                    Some(Err(e)) => {runtime_error=Some(RuntimeError::Indeterminate(e.to_string()));break ContainerExit::RuntimeFailed;},
                    None => output_open = false,
                },
                _ = tick.tick() => {
                    if session.tunnel.as_ref().is_some_and(crate::egress::tunnel::ProxyTunnel::is_closed) {
                        runtime_error=Some(RuntimeError::Indeterminate("egress tunnel stopped".into()));
                        break ContainerExit::RuntimeFailed;
                    }
                    match session.exit_code().await {
                        Ok(Some(code)) => break ContainerExit::Exited(code),
                        Ok(None) => {},
                        Err(e) => { runtime_error=Some(e);break ContainerExit::RuntimeFailed; }
                    }
                }
            }
        };
        // The attached collector stops every workload process before streaming
        // tmpfs; Docker archive APIs omit tmpfs mounts. Then force-remove.
        // Collection is bounded even on cancellation.
        let collector = session
            .capture
            .take()
            .ok_or_else(|| RuntimeError::Indeterminate("output collector missing".into()))?;
        let captured = tokio::time::timeout(
            KILL_SETTLE,
            crate::docker_outputs::collect(&self.docker, spec, collector),
        )
        .await;
        match captured {
            Ok(Ok(crate::docker_outputs::CaptureStatus::Quota)) => {
                if matches!(exit, ContainerExit::Exited(_)) {
                    exit = ContainerExit::DiskQuotaExceeded;
                }
            }
            Ok(Ok(crate::docker_outputs::CaptureStatus::Unsafe)) => {
                exit = ContainerExit::InvalidOutput
            }
            Ok(Err(e)) => runtime_error = Some(e),
            Err(_) => {
                runtime_error = Some(RuntimeError::Indeterminate(
                    "output capture timed out".into(),
                ))
            }
            _ => {}
        }
        if matches!(exit, ContainerExit::Exited(122 | 153)) {
            exit = ContainerExit::DiskQuotaExceeded;
        }
        let oom_killed = self
            .docker
            .inspect_container(&spec.name, None)
            .await
            .ok()
            .and_then(|i| i.state)
            .and_then(|s| s.oom_killed)
            .unwrap_or(false);
        let egress = session.egress();
        if let Err(e) = session.guard.remove_now().await {
            runtime_error = Some(e);
        }
        // Removal closes the workload pipes on every terminal path, including
        // cancellation. Drain bytes already written before termination rather
        // than losing the final tail when cancellation wins the select race.
        if output_open {
            let drained = tokio::time::timeout(OUTPUT_DRAIN, async {
                while let Some(chunk) = session.output.next().await {
                    match chunk {
                        Ok(chunk) => {
                            capture(&mut stdout, &mut stderr, chunk);
                        }
                        Err(error) => {
                            return Err(RuntimeError::Indeterminate(error.to_string()));
                        }
                    }
                }
                Ok(())
            })
            .await;
            match drained {
                Ok(Ok(())) => {}
                Ok(Err(error)) => runtime_error = Some(error),
                Err(_) => {
                    runtime_error = Some(RuntimeError::Indeterminate(
                        "stdout/stderr drain timed out".into(),
                    ));
                }
            }
        }
        if stdout.exceeded() || stderr.exceeded() {
            exit = ContainerExit::OutputLimitExceeded;
        }
        Ok(ContainerOutcome {
            exit,
            oom_killed,
            stdout,
            stderr,
            elapsed: started.elapsed(),
            egress,
            runtime_error,
        })
    }
}

impl ContainerRuntime for DockerRuntime {
    fn run<'a>(
        &'a self,
        spec: &'a ContainerSpec,
        cancel: CancellationToken,
    ) -> ApplicationFuture<'a, Result<ContainerOutcome, RuntimeError>> {
        Box::pin(self.run_inner(spec, cancel))
    }
}

fn capture(stdout: &mut CapturedStream, stderr: &mut CapturedStream, chunk: LogOutput) -> bool {
    match chunk {
        LogOutput::StdOut { message } | LogOutput::Console { message } => stdout.push(&message),
        LogOutput::StdErr { message } => stderr.push(&message),
        LogOutput::StdIn { .. } => false,
    }
}

#[cfg(test)]
fn wait_status(
    status: Option<Result<bollard::models::ContainerWaitResponse, DockerError>>,
) -> Result<i64, RuntimeError> {
    match status {
        Some(Ok(response)) => Ok(response.status_code),
        Some(Err(DockerError::DockerContainerWaitError { code, .. })) => Ok(code),
        Some(Err(error)) => Err(RuntimeError::Indeterminate(error.to_string())),
        None => Err(RuntimeError::Indeterminate(
            "container wait stream ended without a status".to_owned(),
        )),
    }
}

/// Maps a [`ContainerSpec`] to the Docker create body. Public for tests and
/// for hosts that audit the effective isolation settings.
#[must_use]
pub fn create_body(spec: &ContainerSpec) -> ContainerCreateBody {
    let host_config = HostConfig {
        mounts: Some(
            spec.mounts
                .iter()
                .map(|mount| Mount {
                    target: Some(mount.container.clone()),
                    source: Some(mount.host.clone()),
                    typ: Some(MountTypeEnum::BIND),
                    read_only: Some(mount.read_only),
                    ..Default::default()
                })
                .collect(),
        ),
        tmpfs: Some(spec.tmpfs.clone().into_iter().collect()),
        readonly_rootfs: Some(spec.read_only_rootfs),
        cap_drop: spec.cap_drop_all.then(|| vec!["ALL".to_owned()]),
        security_opt: spec
            .no_new_privileges
            .then(|| vec!["no-new-privileges:true".to_owned()]),
        network_mode: Some("none".into()),
        nano_cpus: Some(spec.limits.nano_cpus),
        memory: Some(spec.limits.memory_bytes),
        // Equal to memory: no swap.
        memory_swap: Some(spec.limits.memory_bytes),
        pids_limit: Some(spec.limits.pids),
        ulimits: Some(vec![ResourcesUlimits {
            name: Some("nofile".to_owned()),
            soft: Some(i64::from(spec.limits.nofile)),
            hard: Some(i64::from(spec.limits.nofile)),
        }]),
        init: Some(true),
        ipc_mode: Some("none".to_owned()),
        privileged: Some(false),
        publish_all_ports: Some(false),
        // Output is streamed through attach; nothing is persisted by the
        // daemon, so job output cannot fill the host disk through logs.
        log_config: Some(HostConfigLogConfig {
            typ: Some("none".to_owned()),
            config: None,
        }),
        auto_remove: Some(false),
        ..Default::default()
    };
    ContainerCreateBody {
        image: Some(spec.image.clone()),
        cmd: Some(spec.cmd.clone()),
        entrypoint: Some(Vec::new()),
        env: Some(
            spec.env
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect(),
        ),
        user: Some(spec.user.clone()),
        working_dir: Some(spec.working_dir.clone()),
        hostname: Some(spec.hostname.clone()),
        labels: Some(spec.labels.clone().into_iter().collect()),
        network_disabled: Some(true),
        attach_stdin: Some(false),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        open_stdin: Some(false),
        tty: Some(false),
        host_config: Some(host_config),
        ..Default::default()
    }
}

async fn force_remove(docker: &Docker, name: &str) {
    let _ = docker
        .remove_container(
            name,
            Some(
                RemoveContainerOptionsBuilder::default()
                    .force(true)
                    .v(true)
                    .build(),
            ),
        )
        .await;
}

/// Removes the container when dropped (for example when the job future is
/// aborted) unless it was already removed.
pub(crate) struct RemovalGuard {
    docker: Docker,
    name: String,
    armed: bool,
}

impl RemovalGuard {
    pub(crate) fn new(docker: Docker, name: String) -> Self {
        Self {
            docker,
            name,
            armed: true,
        }
    }

    pub(crate) async fn remove_now(&mut self) -> Result<(), RuntimeError> {
        match self
            .docker
            .remove_container(
                &self.name,
                Some(
                    RemoveContainerOptionsBuilder::default()
                        .force(true)
                        .v(true)
                        .build(),
                ),
            )
            .await
        {
            Ok(())
            | Err(DockerError::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                self.armed = false;
                Ok(())
            }
            Err(e) => Err(RuntimeError::Indeterminate(format!(
                "container removal failed: {e}"
            ))),
        }
    }
}

impl Drop for RemovalGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let docker = self.docker.clone();
            let name = std::mem::take(&mut self.name);
            handle.spawn(async move { force_remove(&docker, &name).await });
        } else {
            tracing::warn!(container = %self.name, "sandbox container leaked: no runtime to remove it");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::*;
    use crate::spec::{BindMount, EffectiveLimits};

    fn spec(network: ContainerNetwork) -> ContainerSpec {
        ContainerSpec {
            name: "musteragent-sandbox-x".to_owned(),
            image: "python:3.12-slim".to_owned(),
            cmd: vec!["python3".to_owned(), "main.py".to_owned()],
            python_bin: "python3".into(),
            env: vec![("A".to_owned(), "1".to_owned())],
            user: "65534:65534".to_owned(),
            working_dir: "/sandbox/work".to_owned(),
            hostname: "sandbox".to_owned(),
            labels: BTreeMap::from([("k".to_owned(), "v".to_owned())]),
            mounts: vec![BindMount {
                host: "/h/in".to_owned(),
                container: "/sandbox/input".to_owned(),
                read_only: true,
            }],
            tmpfs: BTreeMap::from([("/tmp".to_owned(), "size=1".to_owned())]),
            read_only_rootfs: true,
            cap_drop_all: true,
            no_new_privileges: true,
            network,
            limits: EffectiveLimits {
                wall_time: Duration::from_secs(1),
                nano_cpus: 500_000_000,
                memory_bytes: 64 * 1024 * 1024,
                pids: 32,
                tmpfs_bytes: 1,
                nofile: 256,
                stdout_bytes: 1,
                stderr_bytes: 1,
                output_bytes: 1,
                output_files: 1,
            },
        }
    }

    #[test]
    fn create_body_carries_every_isolation_setting() {
        let body = create_body(&spec(ContainerNetwork::None));
        let host = body.host_config.expect("host config");
        assert_eq!(host.readonly_rootfs, Some(true));
        assert_eq!(host.cap_drop, Some(vec!["ALL".to_owned()]));
        assert_eq!(
            host.security_opt,
            Some(vec!["no-new-privileges:true".to_owned()])
        );
        assert_eq!(host.network_mode.as_deref(), Some("none"));
        assert_eq!(host.nano_cpus, Some(500_000_000));
        assert_eq!(host.memory, Some(64 * 1024 * 1024));
        assert_eq!(host.memory_swap, host.memory);
        assert_eq!(host.pids_limit, Some(32));
        assert_eq!(host.privileged, Some(false));
        assert_eq!(host.ipc_mode.as_deref(), Some("none"));
        assert_eq!(
            host.log_config.and_then(|config| config.typ).as_deref(),
            Some("none")
        );
        let mounts = host.mounts.expect("mounts");
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].read_only, Some(true));
        assert_eq!(mounts[0].typ, Some(MountTypeEnum::BIND));
        assert!(host.binds.is_none());
        assert_eq!(body.user.as_deref(), Some("65534:65534"));
        assert_eq!(body.network_disabled, Some(true));
        assert_eq!(body.entrypoint, Some(Vec::new()));
        assert_eq!(body.env, Some(vec!["A=1".to_owned()]));
    }

    #[test]
    fn egress_jobs_have_no_network_interface() {
        let body = create_body(&spec(ContainerNetwork::EgressProxy));
        assert_eq!(body.network_disabled, Some(true));
        assert_eq!(
            body.host_config
                .and_then(|host| host.network_mode)
                .as_deref(),
            Some("none")
        );
    }

    #[test]
    fn wait_status_maps_nonzero_exit_codes() {
        assert_eq!(
            wait_status(Some(Err(DockerError::DockerContainerWaitError {
                error: String::new(),
                code: 3,
            }))),
            Ok(3)
        );
        assert!(matches!(
            wait_status(None),
            Err(RuntimeError::Indeterminate(_))
        ));
    }
}
