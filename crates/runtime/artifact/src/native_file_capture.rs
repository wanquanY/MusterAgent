//! Native adapters capture bytes; the original Artifact owner prepares versions.
use crate::{ArtifactToolContext, ArtifactToolError, GeneratedFile};
use musteragent_runtime_contract::ApplicationFuture;

pub trait NativeFileCapture: Send + Sync {
    fn capture<'a>(
        &'a self,
        context: &'a ArtifactToolContext,
        request: &'a [u8],
    ) -> ApplicationFuture<'a, Result<GeneratedFile, ArtifactToolError>>;
}

/// Bytes between two liveness reports while a large file is verified or
/// copied. One report per 64 MiB keeps a multi-GB transfer well inside the
/// tool idle window at any realistic storage throughput (≥ 1 MiB/s).
pub const CONTENT_PROGRESS_STEP_BYTES: u64 = 64 * 1024 * 1024;

/// Counts transferred bytes and reports each completed 64 MiB step as tool
/// activity. Reports come only from bytes that actually moved, never a timer.
pub struct ContentProgress {
    execution: Option<musteragent_runtime_capability::OperationContext>,
    moved: u64,
}

impl ContentProgress {
    #[must_use]
    pub fn new(execution: Option<musteragent_runtime_capability::OperationContext>) -> Self {
        Self {
            execution,
            moved: 0,
        }
    }

    pub fn advance(&mut self, bytes: u64) {
        let before = self.moved / CONTENT_PROGRESS_STEP_BYTES;
        self.moved = self.moved.saturating_add(bytes);
        if self.moved / CONTENT_PROGRESS_STEP_BYTES > before
            && let Some(execution) = &self.execution
        {
            let _ = execution.completed_stage();
        }
    }
}

/// Verification-only sink: `ContentStore::copy_to` checks the exact length,
/// digest and current authority while bytes stream through, with no spool.
pub struct VerifyingSink(pub ContentProgress);

impl musteragent_runtime_content::ContentSink for VerifyingSink {
    fn write<'a>(
        &'a mut self,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), musteragent_runtime_content::ContentStoreError>> {
        self.0.advance(bytes.len() as u64);
        Box::pin(async { Ok(()) })
    }
}
