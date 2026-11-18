//! Credits for extra first-party wire buffers, held from serialization until
//! response headers (including an authorized repair). Saturation rejects local
//! preflight with retryable quota; it never drops context or queues full bodies.
//! Native/BYOK endpoints do not use this optional transport budget.
use musteragent_runtime_model::{ProviderError, ProviderErrorCode, ProviderErrorKind};
use serde_json::Value;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

// Fits the first-party ingress's default 128 MiB native maximum plus wire
// working space. The 32 MiB optional block-cache limit is not an input limit.
const PROCESS_BYTES: usize = 768 * 1024 * 1024;
static PROCESS: TransportMemory = TransportMemory {
    held: AtomicUsize::new(0),
    capacity: PROCESS_BYTES,
};

struct TransportMemory {
    held: AtomicUsize,
    capacity: usize,
}
pub(crate) struct TransportCredit<'a> {
    owner: &'a TransportMemory,
    bytes: usize,
}

/// Counts without allocating an encoded copy. The four-body allowance covers
/// native bytes, framed Vec growth, encoder temporaries and repair upload. Two
/// MiB cover bounded header clones, codec scratch and at most 2048 manifest
/// entries. Provider mapping/Canonical context have their existing owners.
pub(crate) fn reserve(
    value: &Value,
) -> Result<(usize, Option<TransportCredit<'static>>), ProviderError> {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(|_| exhausted())?;
    let credit = credit_for_body_size(counter.0)?;
    Ok((counter.0, credit))
}

fn credit_for_body_size(
    body_bytes: usize,
) -> Result<Option<TransportCredit<'static>>, ProviderError> {
    let bytes = body_bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(2 * 1024 * 1024))
        .ok_or_else(exhausted)?;
    // An individually oversized native request is still legal. Decline the
    // optional framing before allocating its extra buffers; the transport
    // owner must validate existing authority before returning the native path.
    if bytes > PROCESS.capacity {
        return Ok(None);
    }
    let credit = PROCESS.acquire(bytes)?;
    tracing::debug!(
        context_transport.reserved_bytes = bytes,
        context_transport.process_held_bytes = PROCESS.held.load(Ordering::Relaxed),
        "first-party wire memory reserved"
    );
    Ok(Some(credit))
}

impl TransportMemory {
    fn acquire(&self, bytes: usize) -> Result<TransportCredit<'_>, ProviderError> {
        self.held
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |held| {
                held.checked_add(bytes)
                    .filter(|next| *next <= self.capacity)
            })
            .map_err(|_| exhausted())?;
        Ok(TransportCredit { owner: self, bytes })
    }
}
impl Drop for TransportCredit<'_> {
    fn drop(&mut self) {
        self.owner.held.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct Counter(usize);
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("wire size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn exhausted() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Quota,
        safe_code: ProviderErrorCode::QuotaExceeded,
        retryable: true,
        retry_after_ms: None,
        diagnostic_ref: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_native_body_declines_extra_framing_without_reducing_input_limit() {
        assert!(
            credit_for_body_size(PROCESS_BYTES)
                .expect("oversized body uses native transport")
                .is_none()
        );
        let credit = credit_for_body_size(128 * 1024 * 1024)
            .expect("framing credit available")
            .expect("body fits framing capacity");
        assert_eq!(credit.bytes, 514 * 1024 * 1024);
    }

    #[tokio::test]
    async fn cancellation_releases_credits_and_admission_never_exceeds_process_capacity() {
        let memory = std::sync::Arc::new(TransportMemory {
            held: AtomicUsize::new(0),
            capacity: 16,
        });
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let owner = memory.clone();
        let task = tokio::spawn(async move {
            let _credit = owner.acquire(12).expect("first admission");
            ready.send(()).expect("test waiting");
            std::future::pending::<()>().await;
        });
        waiting.await.expect("credit acquired");
        assert!(memory.acquire(5).is_err());
        let remaining = memory.acquire(4).expect("exact remaining capacity");
        assert!(memory.acquire(1).is_err());
        task.abort();
        let _ = task.await;
        let reclaimed = memory
            .acquire(12)
            .expect("canceled preflight releases credits");
        assert_eq!(memory.held.load(Ordering::Relaxed), 16);
        drop((remaining, reclaimed));
        assert_eq!(memory.held.load(Ordering::Relaxed), 0);
        assert!(memory.acquire(17).is_err());
    }
}
