//! Shared one-container lifecycle for bounded jobs and stdio MCP sessions.
use crate::{
    docker::{RemovalGuard, create_body},
    egress::{EgressProxy, tunnel::ProxyTunnel},
    runtime::RuntimeError,
    spec::{ContainerNetwork, ContainerSpec},
};
use bollard::{
    Docker,
    container::LogOutput,
    errors::Error as DockerError,
    exec::{CreateExecOptions, StartExecResults},
    query_parameters::CreateContainerOptionsBuilder,
};
use futures_util::{Stream, StreamExt};
use std::{pin::Pin, sync::Arc};
use tokio::io::AsyncWrite;

pub(crate) type Output = Pin<Box<dyn Stream<Item = Result<LogOutput, DockerError>> + Send>>;
pub(crate) type Input = Pin<Box<dyn AsyncWrite + Send>>;

pub(crate) struct DockerSession {
    pub docker: Docker,
    pub exec_id: String,
    pub input: Input,
    pub output: Output,
    pub tunnel: Option<ProxyTunnel>,
    pub capture: Option<crate::docker_outputs::Capture>,
    pub guard: RemovalGuard,
}
impl DockerSession {
    pub async fn start(
        docker: Docker,
        spec: &ContainerSpec,
        proxy: Option<Arc<EgressProxy>>,
    ) -> Result<Self, RuntimeError> {
        docker
            .ping()
            .await
            .map_err(|e| RuntimeError::Unavailable(e.to_string()))?;
        let needs_proxy = spec.network == ContainerNetwork::EgressProxy;
        if needs_proxy && proxy.is_none() {
            return Err(RuntimeError::Start(
                "proxy egress profile requires a configured egress policy".into(),
            ));
        }
        let mut body = create_body(spec);
        // Keep tmpfs alive after workload exit so failures can be collected.
        body.cmd = Some(vec![
            spec.python_bin.clone(),
            "-u".into(),
            "-c".into(),
            "import time; time.sleep(86400)".into(),
        ]);
        body.working_dir = Some("/tmp".into());
        docker
            .create_container(
                Some(
                    CreateContainerOptionsBuilder::default()
                        .name(&spec.name)
                        .build(),
                ),
                body,
            )
            .await
            .map_err(|e| match e {
                DockerError::DockerResponseServerError {
                    status_code: 404,
                    message,
                } => RuntimeError::ImageMissing(message),
                e => RuntimeError::Start(e.to_string()),
            })?;
        let guard = RemovalGuard::new(docker.clone(), spec.name.clone());
        docker
            .start_container(&spec.name, None)
            .await
            .map_err(start_error)?;
        let init = "import os,shutil
from pathlib import Path
p=Path('/sandbox/seed')
for src in p.iterdir() if p.exists() else []:
 dst=Path('/sandbox/work')/src.name
 if src.is_symlink(): dst.symlink_to(os.readlink(src))
 elif src.is_dir(): shutil.copytree(src,dst,symlinks=True)
 else: shutil.copyfile(src,dst)";
        let (init_id, _, mut init_out) = exec(
            &docker,
            &spec.name,
            vec![spec.python_bin.clone(), "-c".into(), init.into()],
            "/tmp",
            false,
        )
        .await?;
        while let Some(chunk) = init_out.next().await {
            chunk.map_err(start_error)?;
        }
        if docker
            .inspect_exec(&init_id)
            .await
            .map_err(start_error)?
            .exit_code
            != Some(0)
        {
            return Err(RuntimeError::Start(
                "could not initialize bounded work filesystem".into(),
            ));
        }
        let tunnel = if needs_proxy {
            Some(
                ProxyTunnel::start(
                    &docker,
                    &spec.name,
                    &spec.python_bin,
                    proxy
                        .as_deref()
                        .ok_or_else(|| RuntimeError::Start("egress unavailable".into()))?,
                )
                .await?,
            )
        } else {
            None
        };
        let capture = if spec.mounts.iter().any(|m| m.container == "/sandbox/seed") {
            Some(
                crate::docker_outputs::Capture::start(&docker, &spec.name, &spec.python_bin)
                    .await?,
            )
        } else {
            None
        };
        let (exec_id, input, output) = exec(
            &docker,
            &spec.name,
            spec.cmd.clone(),
            &spec.working_dir,
            true,
        )
        .await?;
        Ok(Self {
            docker,
            exec_id,
            input,
            output,
            tunnel,
            capture,
            guard,
        })
    }
    pub async fn exit_code(&mut self) -> Result<Option<i64>, RuntimeError> {
        let state = self
            .docker
            .inspect_exec(&self.exec_id)
            .await
            .map_err(|e| RuntimeError::Indeterminate(e.to_string()))?;
        Ok(if state.running == Some(true) {
            None
        } else {
            state.exit_code
        })
    }
    pub fn egress(&self) -> Vec<crate::egress::EgressEvent> {
        self.tunnel
            .as_ref()
            .map(|t| t.audit.events())
            .unwrap_or_default()
    }
}

async fn exec(
    docker: &Docker,
    name: &str,
    cmd: Vec<String>,
    cwd: &str,
    stdin: bool,
) -> Result<(String, Input, Output), RuntimeError> {
    let created = docker
        .create_exec(
            name,
            CreateExecOptions {
                attach_stdin: Some(stdin),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                cmd: Some(cmd),
                working_dir: Some(cwd.to_owned()),
                ..Default::default()
            },
        )
        .await
        .map_err(start_error)?;
    match docker
        .start_exec(&created.id, None)
        .await
        .map_err(start_error)?
    {
        StartExecResults::Attached { input, output } => Ok((created.id, input, output)),
        StartExecResults::Detached => {
            Err(RuntimeError::Start("workload stdio did not attach".into()))
        }
    }
}
fn start_error(error: DockerError) -> RuntimeError {
    RuntimeError::Start(error.to_string())
}
