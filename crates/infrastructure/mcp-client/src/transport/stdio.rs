//! stdio transport: newline-delimited JSON-RPC over a launched process.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use musteragent_runtime_content::ApplicationFuture;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::process::{
    BoxedAsyncRead, BoxedAsyncWrite, McpProcessControl, McpProcessSpec, ProcessLauncher,
};
use super::{McpTransport, McpTransportKind};
use crate::McpClientError;

/// Bytes of the most recent stderr output kept for diagnostics.
pub const STDERR_TAIL_BYTES: usize = 8 * 1024;

pub struct StdioTransport {
    writer: Mutex<Option<BoxedAsyncWrite>>,
    reader: Mutex<BufReader<BoxedAsyncRead>>,
    control: Mutex<Option<Box<dyn McpProcessControl>>>,
    stderr_tail: Arc<StdMutex<Vec<u8>>>,
    stderr_task: StdMutex<Option<JoinHandle<()>>>,
    max_message_bytes: usize,
    shutdown_grace: Duration,
}

impl StdioTransport {
    pub async fn launch(
        launcher: &dyn ProcessLauncher,
        spec: McpProcessSpec,
        max_message_bytes: usize,
        shutdown_grace: Duration,
    ) -> Result<Self, McpClientError> {
        let process = launcher.launch(spec).await?;
        let stderr_tail = Arc::new(StdMutex::new(Vec::new()));
        let stderr_task = process.stderr.map(|stderr| {
            let tail = Arc::clone(&stderr_tail);
            tokio::spawn(drain_stderr(stderr, tail))
        });
        Ok(Self {
            writer: Mutex::new(Some(process.stdin)),
            reader: Mutex::new(BufReader::new(process.stdout)),
            control: Mutex::new(Some(process.control)),
            stderr_tail,
            stderr_task: StdMutex::new(stderr_task),
            max_message_bytes,
            shutdown_grace,
        })
    }

    /// Most recent stderr bytes. Server-controlled diagnostics: untrusted.
    #[must_use]
    pub fn stderr_tail(&self) -> Vec<u8> {
        self.stderr_tail
            .lock()
            .map(|tail| tail.clone())
            .unwrap_or_default()
    }
}

async fn drain_stderr(mut stderr: BoxedAsyncRead, tail: Arc<StdMutex<Vec<u8>>>) {
    let mut buffer = [0_u8; 4 * 1024];
    while let Ok(read) = stderr.read(&mut buffer).await {
        if read == 0 {
            break;
        }
        if let Ok(mut tail) = tail.lock() {
            tail.extend_from_slice(&buffer[..read]);
            if tail.len() > STDERR_TAIL_BYTES {
                let excess = tail.len() - STDERR_TAIL_BYTES;
                tail.drain(..excess);
            }
        }
    }
}

impl McpTransport for StdioTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::Stdio
    }

    fn send(&self, mut message: Vec<u8>) -> ApplicationFuture<'_, Result<(), McpClientError>> {
        Box::pin(async move {
            if message.contains(&b'\n') || message.contains(&b'\r') {
                return Err(McpClientError::InvalidRequest("embedded newline"));
            }
            message.push(b'\n');
            let mut writer = self.writer.lock().await;
            let stdin = writer.as_mut().ok_or(McpClientError::SessionClosed)?;
            stdin
                .write_all(&message)
                .await
                .map_err(|_| McpClientError::Unavailable("stdin closed"))?;
            stdin
                .flush()
                .await
                .map_err(|_| McpClientError::Unavailable("stdin closed"))
        })
    }

    fn receive(&self) -> ApplicationFuture<'_, Result<Option<Vec<u8>>, McpClientError>> {
        Box::pin(async move {
            let mut reader = self.reader.lock().await;
            loop {
                let mut line = Vec::new();
                if !read_limited_line(&mut *reader, &mut line, self.max_message_bytes).await? {
                    return Ok(None);
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                return Ok(Some(line));
            }
        })
    }

    fn close(&self) -> ApplicationFuture<'_, Result<(), McpClientError>> {
        Box::pin(async move {
            // MCP stdio shutdown: close stdin, wait for exit, then kill.
            if let Some(mut stdin) = self.writer.lock().await.take() {
                let _ = stdin.shutdown().await;
            }
            let mut control = self.control.lock().await;
            let result = if let Some(process) = control.as_mut() {
                match tokio::time::timeout(self.shutdown_grace, process.wait()).await {
                    Ok(Ok(_)) => Ok(()),
                    _ => process.kill().await.map(drop),
                }
            } else {
                Ok(())
            };
            *control = None;
            let task = self
                .stderr_task
                .lock()
                .ok()
                .and_then(|mut task| task.take());
            if let Some(mut task) = task {
                // stderr closes once the process is gone; bound the join anyway.
                if tokio::time::timeout(Duration::from_millis(500), &mut task)
                    .await
                    .is_err()
                {
                    task.abort();
                    tracing::debug!("mcp stdio stderr reader did not finish after exit");
                }
            }
            result
        })
    }
}

/// Reads one `\n`-terminated line of at most `limit` bytes (excluding the
/// terminator, `\r\n` accepted). Returns `false` at a clean end of stream.
/// An over-long line is an error: the stream cannot be resynchronized.
pub(crate) async fn read_limited_line(
    source: &mut (impl AsyncBufRead + Unpin),
    output: &mut Vec<u8>,
    limit: usize,
) -> Result<bool, McpClientError> {
    loop {
        let available = source
            .fill_buf()
            .await
            .map_err(|_| McpClientError::Unavailable("stdout read"))?;
        if available.is_empty() {
            if output.is_empty() {
                return Ok(false);
            }
            return Err(McpClientError::ProtocolViolation(
                "stdout ended inside a message",
            ));
        }
        let end = available.iter().position(|byte| *byte == b'\n');
        let retained = end.unwrap_or(available.len());
        if output.len().saturating_add(retained) > limit.saturating_add(1) {
            return Err(McpClientError::MessageTooLarge { limit });
        }
        output.extend_from_slice(&available[..retained]);
        source.consume(retained + usize::from(end.is_some()));
        if end.is_some() {
            if output.last() == Some(&b'\r') {
                output.pop();
            }
            if output.len() > limit {
                return Err(McpClientError::MessageTooLarge { limit });
            }
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn lines(input: &[u8], limit: usize) -> Result<Vec<Vec<u8>>, McpClientError> {
        let mut reader = BufReader::with_capacity(3, input);
        let mut lines = Vec::new();
        loop {
            let mut line = Vec::new();
            if !read_limited_line(&mut reader, &mut line, limit).await? {
                return Ok(lines);
            }
            lines.push(line);
        }
    }

    #[tokio::test]
    async fn lines_are_bounded_and_crlf_tolerant() {
        assert_eq!(
            lines(b"{\"a\":1}\r\n{}\n", 7).await.expect("lines"),
            vec![b"{\"a\":1}".to_vec(), b"{}".to_vec()]
        );
        assert_eq!(
            lines(b"{\"a\":12}\n", 7).await,
            Err(McpClientError::MessageTooLarge { limit: 7 })
        );
        assert!(matches!(
            lines(b"{}\n{\"partial\"", 64).await,
            Err(McpClientError::ProtocolViolation(_))
        ));
    }
}
