use super::*;
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::{
    ApplicationFuture, ContentStore, PutContent, VerifiedContentCache,
};
use musteragent_runtime_model::ModelText;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Source {
    tenant: TenantId,
    reference: ContentRef,
    bytes: Vec<u8>,
    allowed: AtomicBool,
    reads: AtomicUsize,
    cache: Arc<VerifiedContentCache>,
}
impl Source {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            tenant: TenantId::generate(),
            reference: ContentRef {
                content_id: ContentId::generate(),
                media_type: musteragent_content_ref::MediaType::new("text/plain").expect("media"),
                byte_length: bytes.len() as u64,
                sha256: musteragent_content_ref::Sha256::digest(&bytes),
            },
            bytes,
            allowed: AtomicBool::new(true),
            reads: AtomicUsize::new(0),
            cache: Arc::new(VerifiedContentCache::default()),
        }
    }
    async fn batch(&self) -> ContentBatch {
        ContentBatch::load(self, self.tenant, std::slice::from_ref(&self.reference))
            .await
            .expect("fresh authorized read")
    }
}
impl ContentStore for Source {
    fn material_cache(&self) -> Option<Arc<VerifiedContentCache>> {
        Some(self.cache.clone())
    }
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }
    fn read<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if !self.allowed.load(Ordering::SeqCst)
                || tenant != self.tenant
                || reference != &self.reference
            {
                return Err(ContentStoreError::NotFound);
            }
            Ok(self.bytes.clone())
        })
    }
}

fn decode(bytes: &[u8], count: &AtomicUsize) -> Result<CanonicalMessage, ContentStoreError> {
    count.fetch_add(1, Ordering::SeqCst);
    Ok(CanonicalMessage {
        role: CanonicalRole::Tool,
        content: vec![CanonicalContentPart::Text(
            ModelText::new(
                std::str::from_utf8(bytes)
                    .map_err(|_| ContentStoreError::Invariant)?
                    .to_owned(),
            )
            .map_err(|_| ContentStoreError::Invariant)?,
        )],
        tool_calls: Vec::new(),
        tool_call_id: Some(ToolCallId::new("call-1").expect("call")),
        tool_name: Some(WireName::new("test_tool").expect("name")),
        reasoning: None,
    })
}

#[tokio::test]
async fn warm_decoding_still_checks_current_expected_identity_and_source_authorization() {
    let source = Source::new(b"complete tool result".to_vec());
    let count = AtomicUsize::new(0);
    let call = ToolCallId::new("call-1").expect("call");
    let name = WireName::new("test_tool").expect("name");
    let first = materialize_tool_message_from_batch(
        &source.batch().await,
        &source.reference,
        "test.tool-codec.v1",
        &call,
        &name,
        |bytes| decode(bytes, &count),
    )
    .expect("cold");
    let batch = source.batch().await;
    let hot = materialize_tool_message_from_batch(
        &batch,
        &source.reference,
        "test.tool-codec.v1",
        &call,
        &name,
        |bytes| decode(bytes, &count),
    )
    .expect("hot");
    assert_eq!(first, hot);
    assert_eq!(
        source.reads.load(Ordering::SeqCst),
        2,
        "every new batch reauthorizes"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1, "decoder is reused");
    let wrong = ToolCallId::new("call-other").expect("call");
    assert!(
        materialize_tool_message_from_batch(
            &batch,
            &source.reference,
            "test.tool-codec.v1",
            &wrong,
            &name,
            |bytes| decode(bytes, &count)
        )
        .is_err()
    );
    let wrong_name = WireName::new("other_tool").expect("name");
    assert!(
        materialize_tool_message_from_batch(
            &batch,
            &source.reference,
            "test.tool-codec.v1",
            &call,
            &wrong_name,
            |bytes| decode(bytes, &count)
        )
        .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    source.allowed.store(false, Ordering::SeqCst);
    assert!(
        ContentBatch::load(
            &source,
            source.tenant,
            std::slice::from_ref(&source.reference)
        )
        .await
        .is_err(),
        "retained decoded bytes do not grant authorization"
    );
}

#[tokio::test]
async fn decode_generation_fences_late_publication_and_codec_identity_is_exact() {
    let source = Source::new(b"full response".to_vec());
    let count = AtomicUsize::new(0);
    let call = ToolCallId::new("call-1").expect("call");
    let name = WireName::new("test_tool").expect("name");
    let batch = source.batch().await;
    for codec in ["test.tool-codec.v1", "test.tool-codec.v2"] {
        materialize_tool_message_from_batch(
            &batch,
            &source.reference,
            codec,
            &call,
            &name,
            |bytes| decode(bytes, &count),
        )
        .expect("decoded");
    }
    assert_eq!(count.load(Ordering::SeqCst), 2);
    source.cache.clear().expect("erase");
    materialize_tool_message_from_batch(
        &batch,
        &source.reference,
        "test.tool-codec.v1",
        &call,
        &name,
        |bytes| decode(bytes, &count),
    )
    .expect("current computation remains complete");
    assert_eq!(source.cache.stats().expect("stats").entries, 0);
}

#[tokio::test]
async fn shared_text_decoder_preserves_utf8_and_original_per_part_limits() {
    let text = "中🦀 abc\n".repeat(90_000);
    let source = Source::new(text.as_bytes().to_vec());
    let first =
        crate::materialize_message_text_from_batch(&source.batch().await, &source.reference)
            .expect("chunked");
    let hot = crate::materialize_message_text_from_batch(&source.batch().await, &source.reference)
        .expect("hot");
    assert_eq!(first, hot);
    assert!(hot.len() > 1);
    let mut combined = String::new();
    for (cold, warm) in first.iter().zip(&hot) {
        let (CanonicalContentPart::Text(cold), CanonicalContentPart::Text(warm)) = (cold, warm)
        else {
            panic!("text")
        };
        assert!(
            std::ptr::eq(cold.as_str().as_ptr(), warm.as_str().as_ptr()),
            "warm material shares its immutable text allocation"
        );
        combined.push_str(warm.as_str());
    }
    assert_eq!(combined, text);
    assert!(
        crate::materialize_tool_assistant_text_from_batch(&source.batch().await, &source.reference)
            .is_err(),
        "existing single-part Tool-selecting response limit is preserved"
    );
}
