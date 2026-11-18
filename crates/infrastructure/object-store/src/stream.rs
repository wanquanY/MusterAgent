//! Bounded multipart staging followed by atomic create. A partially uploaded or
//! unverified object never occupies the immutable public storage identity.
//!
//! One atomic create is a server-side copy with conditional completion. S3
//! UploadPartCopy caps each copied part at 5 GiB. Content above
//! `SEGMENT_BYTES` is therefore stored as consecutive immutable segments next
//! to the locator; the immutable length in the ContentRef selects the layout,
//! so objects at or below one segment keep the original single-key layout.
use super::*;
use crate::staging::StagingObject;
use futures_util::StreamExt;
use musteragent_content_ref::Sha256;
use musteragent_runtime_content::{
    ApplicationFuture, CONTENT_TRANSFER_CHUNK_BYTES, ContentSink, ContentSource,
    ContentStoreError as Error,
};
use sha2::{Digest, Sha256 as Hasher};

const PART_BYTES: usize = 8 << 20;
/// Largest segment created by one atomic copy: 4 GiB, below the 5 GiB copy cap.
pub(super) const SEGMENT_BYTES: u64 = 4 << 30;

struct Discard;
impl ContentSink for Discard {
    fn write<'a>(&'a mut self, _: &'a [u8]) -> ApplicationFuture<'a, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}
fn storage(error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { .. } => Error::NotFound,
        _ => Error::Unavailable,
    }
}

/// Physical layout of one immutable object: `(path, byte length)` per segment.
pub(super) fn segments(
    base: &Path,
    length: u64,
    segment_bytes: u64,
) -> Result<Vec<(Path, u64)>, Error> {
    if length <= segment_bytes {
        return Ok(vec![(base.clone(), length)]);
    }
    let count = length.div_ceil(segment_bytes);
    (0..count)
        .map(|index| {
            let start = index * segment_bytes;
            Ok((
                segment_path(base, index)?,
                (length - start).min(segment_bytes),
            ))
        })
        .collect()
}
pub(super) fn segment_path(base: &Path, index: u64) -> Result<Path, Error> {
    Path::parse(format!("{base}.segment-{index:05}")).map_err(|_| Error::Invariant)
}

/// A segment fully staged and digested, waiting for the whole-object check.
struct Staged {
    upload: StagingObject,
    target: Path,
    length: u64,
    sha256: Sha256,
}

impl ObjectStoreBlobStore {
    pub(super) async fn copy_stream_inner(
        &self,
        identity: BlobIdentity,
        locator: &StorageLocator,
        length: u64,
        hash: Sha256,
        sink: &mut dyn ContentSink,
    ) -> Result<(), Error> {
        let path = self
            .verified_path(identity, locator)
            .map_err(|_| Error::Unauthorized)?;
        if length > MAX_CONTENT_BYTES {
            return Err(Error::TooLarge);
        }
        let mut digest = Hasher::new();
        for (segment, size) in segments(&path, length, self.segment_bytes)? {
            self.copy_segment(&segment, size, &mut digest, sink).await?;
        }
        if digest.finalize().as_slice() != hash.into_bytes() {
            return Err(Error::HashMismatch);
        }
        Ok(())
    }
    /// Streams one stored object in bounded chunks, feeding `digest`.
    async fn copy_segment(
        &self,
        path: &Path,
        length: u64,
        digest: &mut Hasher,
        sink: &mut dyn ContentSink,
    ) -> Result<(), Error> {
        let object = self.store.get(path).await.map_err(storage)?;
        if object.meta.size != length {
            return Err(Error::LengthMismatch);
        }
        let mut source = object.into_stream();
        let mut count = 0u64;
        while let Some(bytes) = source.next().await {
            let bytes = bytes.map_err(storage)?;
            count = count
                .checked_add(bytes.len() as u64)
                .ok_or(Error::TooLarge)?;
            if count > length {
                return Err(Error::LengthMismatch);
            }
            for part in bytes.chunks(CONTENT_TRANSFER_CHUNK_BYTES) {
                digest.update(part);
                sink.write(part).await?;
            }
        }
        if count != length {
            return Err(Error::LengthMismatch);
        }
        Ok(())
    }
    async fn verify_segment(&self, path: &Path, length: u64, hash: Sha256) -> Result<(), Error> {
        let mut digest = Hasher::new();
        self.copy_segment(path, length, &mut digest, &mut Discard)
            .await?;
        if digest.finalize().as_slice() != hash.into_bytes() {
            return Err(Error::HashMismatch);
        }
        Ok(())
    }
    /// Uploads exactly `length` source bytes to a private staging object.
    async fn stage_segment(
        &self,
        source: &mut dyn ContentSource,
        buffer: &mut [u8],
        length: u64,
        whole: &mut Hasher,
    ) -> Result<(StagingObject, Sha256), Error> {
        let mut staging = StagingObject::new(self);
        staging.upload = Some(
            self.store
                .put_multipart(&staging.path)
                .await
                .map_err(storage)?,
        );
        let mut digest = Hasher::new();
        let mut remaining = length;
        // Only one part can be in flight: source reads and upload apply backpressure.
        while remaining > 0 {
            let desired = remaining.min(PART_BYTES as u64) as usize;
            let mut filled = 0;
            while filled < desired {
                let limit = (desired - filled).min(CONTENT_TRANSFER_CHUNK_BYTES);
                let size = source.read(&mut buffer[filled..filled + limit]).await?;
                if size == 0 || size > limit {
                    return Err(Error::LengthMismatch);
                }
                filled += size;
            }
            digest.update(&buffer[..desired]);
            whole.update(&buffer[..desired]);
            staging
                .upload
                .as_mut()
                .ok_or(Error::Invariant)?
                .put_part(Bytes::copy_from_slice(&buffer[..desired]).into())
                .await
                .map_err(storage)?;
            remaining -= desired as u64;
        }
        if length == 0 {
            staging
                .upload
                .as_mut()
                .ok_or(Error::Invariant)?
                .put_part(Bytes::new().into())
                .await
                .map_err(storage)?;
        }
        staging
            .upload
            .as_mut()
            .ok_or(Error::Invariant)?
            .complete()
            .await
            .map_err(storage)?;
        staging.upload = None;
        let digest = Sha256::from_bytes(digest.finalize()).map_err(|_| Error::Invariant)?;
        Ok((staging, digest))
    }
    pub(super) async fn put_stream_inner(
        &self,
        identity: BlobIdentity,
        length: u64,
        hash: Sha256,
        source: &mut dyn ContentSource,
    ) -> Result<PutBlobReceipt, Error> {
        if length > MAX_CONTENT_BYTES {
            return Err(Error::TooLarge);
        }
        let locator = self.locator(identity).map_err(|_| Error::Invariant)?;
        let target = self
            .verified_path(identity, &locator)
            .map_err(|_| Error::Invariant)?;
        self.check_write_once(Publication::MultipartCopy)
            .await
            .map_err(storage)?;
        let layout = segments(&target, length, self.segment_bytes)?;
        let mut buffer = vec![0; PART_BYTES];
        let mut whole = Hasher::new();
        // Every segment is staged and the complete digest checked before any
        // byte reaches the immutable identity, so a wrong or truncated source
        // can never occupy part of it.
        let mut staged = Vec::with_capacity(layout.len());
        for (segment, size) in layout {
            let (upload, sha256) = self
                .stage_segment(source, &mut buffer, size, &mut whole)
                .await?;
            staged.push(Staged {
                upload,
                target: segment,
                length: size,
                sha256,
            });
        }
        if source.read(&mut buffer[..1]).await? != 0 {
            return Err(Error::LengthMismatch);
        }
        if whole.finalize().as_slice() != hash.into_bytes() {
            return Err(Error::HashMismatch);
        }
        let mut disposition = BlobPutDisposition::AlreadyPresent;
        for mut segment in staged {
            match self
                .store
                .copy_if_not_exists(&segment.upload.path, &segment.target)
                .await
            {
                Ok(()) => disposition = BlobPutDisposition::Created,
                Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(error) => return Err(storage(error)),
            }
            // Both a new copy and a replay must agree with the exact stored bytes.
            self.verify_segment(&segment.target, segment.length, segment.sha256)
                .await?;
            let _ = segment.upload.cleanup().await;
        }
        Ok(PutBlobReceipt {
            locator,
            disposition,
        })
    }
    /// Removes the single object and every segment of a segmented one. The
    /// segment count is probed because deletion does not carry the length.
    pub(super) async fn delete_all(&self, path: &Path) -> Result<(), BlobStoreError> {
        match self.store.delete(path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(_) => return Err(BlobStoreError::Unavailable),
        }
        // Find the contiguous segment run, then delete it from the end so an
        // interrupted delete always leaves a discoverable prefix for retry.
        let limit = MAX_CONTENT_BYTES.div_ceil(self.segment_bytes);
        let mut present = Vec::new();
        for index in 0..limit {
            let segment = segment_path(path, index).map_err(|_| BlobStoreError::InvalidLocator)?;
            match self.store.head(&segment).await {
                Ok(_) => present.push(segment),
                Err(object_store::Error::NotFound { .. }) => break,
                Err(_) => return Err(BlobStoreError::Unavailable),
            }
        }
        for segment in present.iter().rev() {
            match self.store.delete(segment).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(_) => return Err(BlobStoreError::Unavailable),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
