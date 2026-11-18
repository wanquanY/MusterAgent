//! Retained verified bytes for native computation, without whole-file buffers.
//! A range source is material only; it grants no tenant/conversation authority.
use crate::{CONTENT_TRANSFER_CHUNK_BYTES, ContentStoreError as Error, MAX_CONTENT_BYTES};
use musteragent_content_ref::{ContentRef, Sha256};
use sha2::{Digest as _, Sha256 as Hasher};
use std::fmt;

/// Blocking random access over host-owned material. Call native computations
/// on a blocking executor. Returning more than the supplied length is invalid.
pub trait ContentRangeSource: Send + Sync {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize, Error>;
}

impl ContentRangeSource for Vec<u8> {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize, Error> {
        if offset >= self.len() as u64 || buffer.is_empty() {
            return Ok(0);
        }
        let offset = usize::try_from(offset).map_err(|_| Error::TooLarge)?;
        let count = buffer.len().min(self.len() - offset);
        buffer[..count].copy_from_slice(&self[offset..offset + count]);
        Ok(count)
    }
}

/// Ranges are checked against blocks from one complete, SHA-256-verified read.
/// Later source mutation can only yield the pinned bytes or an integrity error.
/// Holds one 32-byte digest per 64 KiB, no path, writer or mutable source handle.
/// This is not an authorization grant: the caller must retain its read set and
/// recheck current authority/cancellation in its existing commit transaction.
pub struct VerifiedContentReader {
    reference: ContentRef,
    fingerprints: Vec<Sha256>,
    source: Box<dyn ContentRangeSource>,
}

impl fmt::Debug for VerifiedContentReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedContentReader")
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}

impl VerifiedContentReader {
    /// Pure verification; the storage adapter authorizes before and after this
    /// potentially slow scan. Cancellation is checked before each bounded read.
    pub fn verify(
        reference: ContentRef,
        source: Box<dyn ContentRangeSource>,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<Self, Error> {
        if reference.byte_length > MAX_CONTENT_BYTES {
            return Err(Error::TooLarge);
        }
        let chunk = CONTENT_TRANSFER_CHUNK_BYTES as u64;
        let count =
            usize::try_from(reference.byte_length.div_ceil(chunk)).map_err(|_| Error::TooLarge)?;
        let mut fingerprints = Vec::new();
        fingerprints
            .try_reserve_exact(count)
            .map_err(|_| Error::Unavailable)?;
        let mut buffer = zeroize::Zeroizing::new(vec![0; CONTENT_TRANSFER_CHUNK_BYTES]);
        let mut complete = Hasher::new();
        let mut offset = 0;
        while offset < reference.byte_length {
            let count = (reference.byte_length - offset).min(chunk) as usize;
            read_source_exact(source.as_ref(), &mut buffer[..count], offset, cancelled)?;
            complete.update(&buffer[..count]);
            fingerprints.push(Sha256::digest(&buffer[..count]));
            offset += count as u64;
        }
        if cancelled() {
            return Err(Error::Unavailable);
        }
        match source.read_at(&mut buffer[..1], offset)? {
            0 => {}
            1 => return Err(Error::LengthMismatch),
            _ => return Err(Error::Invariant),
        }
        if complete.finalize().as_slice() != reference.sha256.into_bytes() {
            return Err(Error::HashMismatch);
        }
        if cancelled() {
            return Err(Error::Unavailable);
        }
        Ok(Self {
            reference,
            fingerprints,
            source,
        })
    }

    pub fn reference(&self) -> &ContentRef {
        &self.reference
    }

    /// Returns up to the next verified block boundary. Bytes are copied into
    /// the caller's buffer only after that whole block passes verification.
    pub fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize, Error> {
        if offset >= self.reference.byte_length || buffer.is_empty() {
            return Ok(0);
        }
        let chunk = CONTENT_TRANSFER_CHUNK_BYTES as u64;
        let index = usize::try_from(offset / chunk).map_err(|_| Error::TooLarge)?;
        let start = offset / chunk * chunk;
        let length = (self.reference.byte_length - start).min(chunk) as usize;
        let mut bytes = zeroize::Zeroizing::new(vec![0; length]);
        read_source_exact(self.source.as_ref(), &mut bytes, start, &|| false)?;
        if Sha256::digest(&bytes) != self.fingerprints[index] {
            return Err(Error::HashMismatch);
        }
        let inside = (offset - start) as usize;
        let count = buffer.len().min(length - inside);
        buffer[..count].copy_from_slice(&bytes[inside..inside + count]);
        Ok(count)
    }

    /// On any error discard the complete destination, including earlier valid
    /// blocks. An out-of-bounds request is rejected before touching the buffer.
    pub fn read_exact_at(&self, mut buffer: &mut [u8], mut offset: u64) -> Result<(), Error> {
        if offset
            .checked_add(buffer.len() as u64)
            .is_none_or(|end| end > self.reference.byte_length)
        {
            return Err(Error::LengthMismatch);
        }
        while !buffer.is_empty() {
            let count = self.read_at(buffer, offset)?;
            if count == 0 {
                return Err(Error::LengthMismatch);
            }
            offset += count as u64;
            buffer = &mut buffer[count..];
        }
        Ok(())
    }
}

fn read_source_exact(
    source: &dyn ContentRangeSource,
    mut buffer: &mut [u8],
    mut offset: u64,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), Error> {
    while !buffer.is_empty() {
        if cancelled() {
            return Err(Error::Unavailable);
        }
        let count = source.read_at(buffer, offset)?;
        if count > buffer.len() {
            return Err(Error::Invariant);
        }
        if count == 0 {
            return Err(Error::LengthMismatch);
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or(Error::LengthMismatch)?;
        buffer = &mut buffer[count..];
    }
    Ok(())
}

#[cfg(test)]
mod tests;
