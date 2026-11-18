//! Container runtime port. The Docker adapter lives in [`crate::docker`];
//! tests substitute scripted runtimes.

use std::collections::VecDeque;
use std::time::Duration;

use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::spec::ContainerSpec;

/// Why a container stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerExit {
    /// The process exited by itself with this status code.
    Exited(i64),
    /// Killed because the wall-time limit elapsed.
    TimedOut,
    /// Killed because the cancellation token fired.
    Cancelled,
    /// Killed because stdout or stderr exceeded its byte cap.
    OutputLimitExceeded,
    /// Killed because the MCP stdin byte budget was exhausted.
    InputLimitExceeded,
    /// A runtime or relay failure made execution indeterminate.
    RuntimeFailed,
    /// The bounded work/tmp filesystem or output quota was exhausted.
    DiskQuotaExceeded,
    /// Output contained a symlink or non-regular object outside the input projection.
    InvalidOutput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerOutcome {
    pub exit: ContainerExit,
    /// The kernel OOM killer terminated the job.
    pub oom_killed: bool,
    pub stdout: CapturedStream,
    pub stderr: CapturedStream,
    pub elapsed: Duration,
    /// Policy facts recorded outside the workload, including delegated DNS.
    pub egress: Vec<crate::egress::EgressEvent>,
    /// Typed infrastructure failure, retaining partial streams and files.
    pub runtime_error: Option<RuntimeError>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("container runtime is unavailable: {0}")]
    Unavailable(String),
    #[error("container image is not available: {0}")]
    ImageMissing(String),
    #[error("container could not be created or started: {0}")]
    Start(String),
    #[error("container state is unknown after a runtime failure: {0}")]
    Indeterminate(String),
}

/// Runs one job container to completion, enforcing wall time, stream caps
/// and cancellation, and always removing the container afterwards.
pub trait ContainerRuntime: Send + Sync {
    fn run<'a>(
        &'a self,
        spec: &'a ContainerSpec,
        cancel: CancellationToken,
    ) -> ApplicationFuture<'a, Result<ContainerOutcome, RuntimeError>>;
}

/// Bounded capture of one output stream: counts every byte, keeps only the
/// last `tail_limit` bytes, and reports when `cap` is exceeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedStream {
    cap: u64,
    tail_limit: usize,
    total: u64,
    tail: VecDeque<u8>,
}

impl CapturedStream {
    #[must_use]
    pub fn new(cap: u64, tail_limit: usize) -> Self {
        Self {
            cap,
            tail_limit,
            total: 0,
            tail: VecDeque::new(),
        }
    }

    /// Appends bytes. Returns `true` when the stream is now over its cap.
    pub fn push(&mut self, bytes: &[u8]) -> bool {
        self.total = self
            .total
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let keep = bytes.len().min(self.tail_limit);
        let start = bytes.len() - keep;
        self.tail.extend(&bytes[start..]);
        while self.tail.len() > self.tail_limit {
            self.tail.pop_front();
        }
        self.exceeded()
    }

    #[must_use]
    pub const fn exceeded(&self) -> bool {
        self.total > self.cap
    }

    #[must_use]
    pub const fn total(&self) -> u64 {
        self.total
    }

    /// The retained tail as UTF-8 (lossy), trimmed from the front so it fits
    /// in `tail_limit` bytes on a character boundary.
    #[must_use]
    pub fn tail_text(&self) -> String {
        let bytes: Vec<u8> = self.tail.iter().copied().collect();
        let text = String::from_utf8_lossy(&bytes).into_owned();
        if text.len() <= self.tail_limit {
            return text;
        }
        let mut start = text.len() - self.tail_limit;
        while !text.is_char_boundary(start) {
            start += 1;
        }
        text[start..].to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_the_tail_and_counts_everything() {
        let mut stream = CapturedStream::new(100, 4);
        assert!(!stream.push(b"hello "));
        assert!(!stream.push(b"world"));
        assert_eq!(stream.total(), 11);
        assert_eq!(stream.tail_text(), "orld");
    }

    #[test]
    fn reports_cap_overflow() {
        let mut stream = CapturedStream::new(5, 64);
        assert!(!stream.push(b"12345"));
        assert!(stream.push(b"6"));
        assert!(stream.exceeded());
    }

    #[test]
    fn tail_text_is_valid_utf8_within_limit() {
        let mut stream = CapturedStream::new(1_000, 5);
        // "é" is two bytes; a five byte tail may start mid-character.
        stream.push("aéééé".as_bytes());
        let text = stream.tail_text();
        assert!(text.len() <= 5);
        assert!(text.ends_with('é'));
        let mut invalid = CapturedStream::new(1_000, 4);
        invalid.push(&[0xff, 0xfe, 0xfd, 0xfc]);
        assert!(invalid.tail_text().len() <= 4);
    }
}
