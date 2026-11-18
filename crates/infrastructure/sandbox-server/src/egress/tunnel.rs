//! Network-none Docker transport for the HTTP proxy. No host socket or Docker
//! socket is mounted in the workload. The attached exec accepts only bounded
//! proxy streams and each stream is evaluated by the ordinary egress policy.
use super::{EgressAudit, EgressProxy};
use crate::runtime::RuntimeError;
use bollard::{
    Docker,
    container::LogOutput,
    exec::{CreateExecOptions, StartExecResults},
};
use futures_util::StreamExt;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const MAX_FRAME: usize = 32768;
const MAX_TRAFFIC: u64 = 64 * 1024 * 1024;
type Frame = (u8, u32, Vec<u8>);

pub(crate) struct ProxyTunnel {
    pub audit: EgressAudit,
    cancel: CancellationToken,
    task: JoinHandle<Result<(), RuntimeError>>,
}
impl ProxyTunnel {
    pub async fn start(
        docker: &Docker,
        container: &str,
        python: &str,
        proxy: &EgressProxy,
    ) -> Result<Self, RuntimeError> {
        let proxy = Arc::new(proxy.session());
        let audit = proxy.audit();
        let exec = docker
            .create_exec(
                container,
                CreateExecOptions {
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    cmd: Some(vec![
                        python.to_owned(),
                        "-u".into(),
                        "-c".into(),
                        include_str!("../scripts/proxy_relay.py").into(),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .map_err(start_error)?;
        let StartExecResults::Attached {
            mut input,
            mut output,
        } = docker
            .start_exec(&exec.id, None)
            .await
            .map_err(start_error)?
        else {
            return Err(RuntimeError::Start("proxy tunnel did not attach".into()));
        };
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut reader, mut feed) = tokio::io::duplex(MAX_FRAME * 2);
            let (tx, mut rx) = mpsc::channel::<Frame>(16);
            let mut tasks = tokio::task::JoinSet::new();
            tasks.spawn(async move {
                while let Some(chunk) = output.next().await {
                    match chunk {
                        Ok(LogOutput::StdOut { message }) => feed.write_all(&message).await?,
                        Ok(LogOutput::StdErr { .. }) => {
                            return Err(std::io::Error::other("proxy relay stderr"));
                        }
                        Err(_) => return Err(std::io::Error::other("proxy attach failed")),
                        _ => {}
                    }
                }
                Ok(())
            });
            tasks.spawn(async move {
                let mut total = 0_u64;
                while let Some((kind, id, bytes)) = rx.recv().await {
                    total += bytes.len() as u64;
                    if total > MAX_TRAFFIC {
                        return Err(std::io::Error::other("proxy traffic limit"));
                    }
                    input.write_u8(kind).await?;
                    input.write_u32(id).await?;
                    input.write_u32(bytes.len() as u32).await?;
                    input.write_all(&bytes).await?;
                    input.flush().await?;
                }
                Ok(())
            });
            let mut connection_tasks = tokio::task::JoinSet::new();
            let mut connections = HashMap::new();
            let mut ready = Some(ready_tx);
            let mut total = 0_u64;
            let result: Result<(), std::io::Error> = async {
                loop {
                    let kind = tokio::select! {
                        () = token.cancelled() => return Ok(()),
                        done = tasks.join_next(), if !tasks.is_empty() => {
                            match done {
                                Some(Ok(Err(e))) => return Err(e),
                                Some(Err(_)) => return Err(std::io::Error::other("proxy task failed")),
                                _ => return Err(std::io::Error::other("proxy transport closed")),
                            }
                        },
                        _ = connection_tasks.join_next(), if !connection_tasks.is_empty() => continue,
                        kind = reader.read_u8() => kind?,
                    };
                    let id = reader.read_u32().await?;
                    let len = reader.read_u32().await? as usize;
                    total += len as u64;
                    if len > MAX_FRAME || total > MAX_TRAFFIC { return Err(std::io::Error::other("proxy traffic limit")); }
                    let mut bytes = vec![0; len];
                    reader.read_exact(&mut bytes).await?;
                    match kind {
                        0 if id == 0 && len == 0 => { if let Some(ready) = ready.take() { let _ = ready.send(()); } },
                        1 if len == 0 && connections.len() < 16 && !connections.contains_key(&id) => {
                            let (client, server) = tokio::io::duplex(MAX_FRAME * 2);
                            let (mut receive, send) = tokio::io::split(client);
                            connections.insert(id, send);
                            let proxy = Arc::clone(&proxy);
                            let shutdown = token.clone();
                            let tx = tx.clone();
                            // Session cancellation owns both upgraded tunnels and HTTP streams.
                            connection_tasks.spawn(async move {
                                tokio::select! {
                                    () = shutdown.cancelled() => {},
                                    () = proxy.serve_stream(server, "127.0.0.1:0".parse().expect("literal"), shutdown.clone()) => {},
                                }
                            });
                            let shutdown = token.clone();
                            connection_tasks.spawn(async move {
                                let mut buffer = vec![0; MAX_FRAME];
                                loop {
                                    let read = tokio::select! {
                                        () = shutdown.cancelled() => break,
                                        read = receive.read(&mut buffer) => read,
                                    };
                                    match read {
                                        Ok(n) if n > 0 => if tx.send((2, id, buffer[..n].to_vec())).await.is_err() { break; },
                                        _ => { let _ = tx.send((3, id, Vec::new())).await; break; },
                                    }
                                }
                            });
                        },
                        2 => if let Some(send) = connections.get_mut(&id) {
                            tokio::time::timeout(Duration::from_secs(5), send.write_all(&bytes)).await
                                .map_err(|_| std::io::Error::other("proxy stream blocked"))??;
                        },
                        3 if len == 0 => { connections.remove(&id); },
                        _ => return Err(std::io::Error::other("invalid proxy frame")),
                    }
                }
            }.await;
            token.cancel();
            tasks.abort_all();
            connection_tasks.abort_all();
            while connection_tasks.join_next().await.is_some() {}
            result.map_err(|e| RuntimeError::Indeterminate(e.to_string()))
        });
        let tunnel = Self {
            audit,
            cancel,
            task,
        };
        tokio::time::timeout(Duration::from_secs(10), ready_rx)
            .await
            .map_err(|_| RuntimeError::Start("proxy relay readiness timeout".into()))?
            .map_err(|_| RuntimeError::Start("proxy relay exited before readiness".into()))?;
        Ok(tunnel)
    }
    pub async fn stop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
        let _ = (&mut self.task).await;
    }
    pub fn is_closed(&self) -> bool {
        self.task.is_finished()
    }
}
impl Drop for ProxyTunnel {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
fn start_error(error: bollard::errors::Error) -> RuntimeError {
    RuntimeError::Start(error.to_string())
}
