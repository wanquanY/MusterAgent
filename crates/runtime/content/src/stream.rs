//! Bounded transfer contracts. A sink is private staging, never a live response.
//!
//! Bytes become usable only after the complete operation succeeds. On failure
//! or future cancellation the caller discards its sink, including partial bytes.
//! No method here grants Artifact/Conversation authority or commits a result.
use crate::{ApplicationFuture, ContentStoreError, MAX_CONTENT_BYTES};
use musteragent_content_ref::Sha256;
use sha2::{Digest as _, Sha256 as Hasher};

pub const CONTENT_TRANSFER_CHUNK_BYTES: usize = 64 * 1024;

/// Sequential input with caller-owned buffers. Zero means EOF, including for
/// an empty input. Implementations must return at most the supplied length.
pub trait ContentSource: Send {
    fn read<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> ApplicationFuture<'a, Result<usize, ContentStoreError>>;
}

/// Private destination. Success must consume all bytes before returning. No
/// resource may be published until transfer, storage and owner checks succeed.
pub trait ContentSink: Send {
    fn write<'a>(
        &'a mut self,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>>;
}

impl ContentSource for &[u8] {
    fn read<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> ApplicationFuture<'a, Result<usize, ContentStoreError>> {
        Box::pin(async move {
            let count = buffer.len().min(self.len());
            buffer[..count].copy_from_slice(&self[..count]);
            *self = &self[count..];
            Ok(count)
        })
    }
}

/// Check exact length and digest while applying backpressure with one bounded
/// buffer. An extra EOF probe rejects trailing data, even for zero-byte input.
pub async fn transfer_content(
    source: &mut dyn ContentSource,
    sink: &mut dyn ContentSink,
    byte_length: u64,
    sha256: Sha256,
) -> Result<(), ContentStoreError> {
    if byte_length > MAX_CONTENT_BYTES {
        return Err(ContentStoreError::TooLarge);
    }
    let mut buffer = zeroize::Zeroizing::new(vec![0; CONTENT_TRANSFER_CHUNK_BYTES]);
    let mut remaining = byte_length;
    let mut hash = Hasher::new();
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| ContentStoreError::TooLarge)?;
        let count = source.read(&mut buffer[..limit]).await?;
        if count > limit {
            return Err(ContentStoreError::Invariant);
        }
        if count == 0 {
            return Err(ContentStoreError::LengthMismatch);
        }
        remaining -= count as u64;
        hash.update(&buffer[..count]);
        sink.write(&buffer[..count]).await?;
    }
    let count = source.read(&mut buffer[..1]).await?;
    if count > 1 {
        return Err(ContentStoreError::Invariant);
    }
    if count != 0 {
        return Err(ContentStoreError::LengthMismatch);
    }
    if hash.finalize().as_slice() != sha256.into_bytes() {
        return Err(ContentStoreError::HashMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
