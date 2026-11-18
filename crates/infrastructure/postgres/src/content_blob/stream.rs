//! Preserve the same immutable tier boundary for buffered and streaming I/O.
//! Only small inline objects may be collected; large transfers remain streaming.
use super::*;
use musteragent_runtime_content::{MAX_CONTENT_BYTES, transfer_content};

impl PostgresTieredBlobStore {
    pub(super) async fn put_tiered_stream(
        &self,
        identity: BlobIdentity,
        length: u64,
        hash: Sha256,
        source: &mut dyn ContentSource,
    ) -> Result<PutBlobReceipt, ContentStoreError> {
        if length > MAX_CONTENT_BYTES {
            return Err(ContentStoreError::TooLarge);
        }
        if length > INLINE_CONTENT_MAX_BYTES as u64 {
            return self
                .large_blobs
                .put_stream(identity, length, hash, source)
                .await;
        }
        let mut inline = InlineBuffer(Vec::with_capacity(length as usize));
        // Validate EOF and hash before making any bytes durable. Cancellation or
        // an invalid source leaves neither a partial row nor an occupied identity.
        transfer_content(source, &mut inline, length, hash).await?;
        self.put(PutBlob {
            identity,
            sha256: hash,
            bytes: inline.0,
        })
        .await
        .map_err(Into::into)
    }

    pub(super) async fn copy_tiered_stream(
        &self,
        identity: BlobIdentity,
        locator: &StorageLocator,
        length: u64,
        hash: Sha256,
        sink: &mut dyn ContentSink,
    ) -> Result<(), ContentStoreError> {
        if length > MAX_CONTENT_BYTES {
            return Err(ContentStoreError::TooLarge);
        }
        self.expected_locator(identity, locator)?;
        // Look up the actual tier. A caller's incorrect length must not route a
        // persisted inline object to the remote store and hide an integrity error.
        match self.read_inline(identity).await? {
            Some(bytes) => transfer_content(&mut bytes.as_slice(), sink, length, hash).await,
            None => {
                self.large_blobs
                    .copy_to(identity, locator, length, hash, sink)
                    .await
            }
        }
    }
}

struct InlineBuffer(Vec<u8>);
impl ContentSink for InlineBuffer {
    fn write<'a>(
        &'a mut self,
        bytes: &'a [u8],
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async move {
            if self.0.len().saturating_add(bytes.len()) > INLINE_CONTENT_MAX_BYTES {
                return Err(ContentStoreError::TooLarge);
            }
            self.0.extend_from_slice(bytes);
            Ok(())
        })
    }
}
