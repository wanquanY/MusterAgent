//! Bounded complete-content transfer within one original source observation.
use super::*;

pub const DEVICE_CONTEXT_CONTENTS_COUNT: usize = 64;
pub const DEVICE_CONTEXT_CONTENTS_BYTES: usize = 4 * 1024 * 1024;
pub const DEVICE_CONTEXT_CONTENTS_LIMIT: usize = DEVICE_CONTEXT_CONTENTS_BYTES + 64 * 8192;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextContentsRequest {
    pub state: DeviceContextStateRequest,
    pub fingerprint: Sha256,
    pub locators: Vec<DeviceContextContentLocator>,
}
impl DeviceContextContentsRequest {
    pub fn content_request(
        &self,
        locator: DeviceContextContentLocator,
    ) -> DeviceContextContentRequest {
        DeviceContextContentRequest {
            include_approval_context: self.state.include_approval_context,
            include_execution_context: false,
            binding: self.state.binding.clone(),
            compaction_policy_ref: self.state.compaction_policy_ref.clone(),
            fingerprint: self.fingerprint,
            locator,
            offset: 0,
        }
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.state.validate()?;
        nonzero(self.fingerprint)?;
        if self.state.include_execution_context
            || self.locators.is_empty()
            || self.locators.len() > DEVICE_CONTEXT_CONTENTS_COUNT
        {
            return Err(DialogueStoreError::Corrupt);
        }
        for (i, locator) in self.locators.iter().enumerate() {
            if self.locators[..i].contains(locator) {
                return Err(DialogueStoreError::Corrupt);
            }
            self.content_request(*locator).validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceContextContents {
    pub chunks: Vec<DeviceContextChunk>,
}

/// Default port implementations enforce the batch bound while accumulating,
/// not after reading every requested object into memory.
pub(super) fn append_complete(
    chunks: &mut Vec<DeviceContextChunk>,
    request: &DeviceContextContentsRequest,
    chunk: DeviceContextChunk,
) -> Result<(), DialogueStoreError> {
    let locator = request
        .locators
        .get(chunks.len())
        .ok_or(DialogueStoreError::Corrupt)?;
    chunk.validate_for(&request.content_request(*locator))?;
    let prior: usize = chunks.iter().map(|c| c.bytes.len()).sum();
    if !chunk.eof || prior.saturating_add(chunk.bytes.len()) > DEVICE_CONTEXT_CONTENTS_BYTES {
        return Err(DialogueStoreError::Corrupt);
    }
    chunks.push(chunk);
    Ok(())
}
impl DeviceContextContents {
    pub fn validate_for(
        &self,
        request: &DeviceContextContentsRequest,
    ) -> Result<(), DialogueStoreError> {
        request.validate()?;
        if self.chunks.len() != request.locators.len() {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut size = 0usize;
        for (chunk, locator) in self.chunks.iter().zip(&request.locators) {
            chunk.validate_for(&request.content_request(*locator))?;
            size = size
                .checked_add(chunk.bytes.len())
                .ok_or(DialogueStoreError::Corrupt)?;
            if !chunk.eof || size > DEVICE_CONTEXT_CONTENTS_BYTES {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::MediaType;
    use musteragent_identity::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Source(AtomicUsize);
    impl DeviceContextTransport for Source {
        fn get_state<'a>(
            &'a self,
            _: &'a DeviceContextStateRequest,
        ) -> ApplicationFuture<'a, Result<DeviceContextState, DialogueStoreError>> {
            Box::pin(async { Err(DialogueStoreError::Unavailable) })
        }
        fn read_page<'a>(
            &'a self,
            _: &'a DeviceContextPageRequest,
        ) -> ApplicationFuture<'a, Result<DeviceContextPage, DialogueStoreError>> {
            Box::pin(async { Err(DialogueStoreError::Unavailable) })
        }
        fn read_content<'a>(
            &'a self,
            request: &'a DeviceContextContentRequest,
        ) -> ApplicationFuture<'a, Result<DeviceContextChunk, DialogueStoreError>> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                let bytes = vec![0; DEVICE_CONTEXT_CHUNK_BYTES];
                Ok(DeviceContextChunk {
                    binding: request.binding.clone(),
                    compaction_policy_ref: request.compaction_policy_ref.clone(),
                    fingerprint: request.fingerprint,
                    locator: request.locator,
                    content_ref: ContentRef {
                        content_id: ContentId::generate(),
                        media_type: MediaType::new("text/plain").expect("valid test fixture"),
                        byte_length: bytes.len() as u64,
                        sha256: Sha256::digest(&bytes),
                    },
                    offset: 0,
                    total: bytes.len() as u64,
                    eof: true,
                    bytes,
                })
            })
        }
    }

    #[tokio::test]
    async fn default_batch_stops_reading_as_soon_as_aggregate_limit_is_exceeded() {
        let source = Source(AtomicUsize::new(0));
        let request = DeviceContextContentsRequest {
            state: DeviceContextStateRequest {
                include_scene_members: false,
                binding: DeviceDeliveryBinding {
                    intent_id: UserIntentId::generate(),
                    work_item: None,
                    run_id: RunId::generate(),
                    device_id: DeviceId::generate(),
                    installation_id: DeviceInstallationId::generate(),
                    source_digest: Sha256::digest(b"source"),
                    snapshot_hash: Sha256::digest(b"snapshot"),
                    grant_proof_hash: Sha256::digest(b"grant"),
                },
                compaction_policy_ref: VersionedKey::new("musteragent.context-compaction@8")
                    .expect("valid test fixture"),
                include_execution_context: false,
                include_approval_context: false,
            },
            fingerprint: Sha256::digest(b"observation"),
            locators: (1..=64)
                .map(|seq| DeviceContextContentLocator::EntryBody { seq })
                .collect(),
        };
        assert_eq!(
            source
                .read_contents(&request)
                .await
                .expect_err("fixture must be rejected"),
            DialogueStoreError::Corrupt
        );
        assert_eq!(
            source.0.load(Ordering::SeqCst),
            5,
            "reject on the first over-budget body, without reading the remaining 59 objects"
        );
    }
}
