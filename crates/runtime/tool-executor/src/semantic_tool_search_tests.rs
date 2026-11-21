use super::*;
use musteragent_capability::CapabilityLocation;
use musteragent_discovery::AuthorizedToolDescriptor;
use musteragent_identity::VersionedKey;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::MemoryEmbeddingBatch;
use musteragent_tool::{ToolKey, WireName};
use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    sync::atomic::{AtomicU32, Ordering},
};

struct Embeddings {
    calls: AtomicU32,
    revision: AtomicU32,
    mismatch: bool,
}
impl MemoryEmbeddingProvider for Embeddings {
    fn configured_model(
        &self,
        _: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingModel, MemoryEmbeddingError>> {
        Box::pin(async {
            Ok(MemoryEmbeddingModel {
                model_key: "multilingual".into(),
                model_revision: self.revision.load(Ordering::SeqCst).to_string(),
                max_input_tokens: NonZeroU32::new(2048).unwrap(),
            })
        })
    }
    fn embed(
        &self,
        inputs: Vec<String>,
        _: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingBatch, MemoryEmbeddingError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(MemoryEmbeddingBatch {
                model_key: "multilingual".into(),
                model_revision: (self.revision.load(Ordering::SeqCst) + u32::from(self.mismatch))
                    .to_string(),
                dimensions: NonZeroU32::new(2).unwrap(),
                vectors: inputs
                    .iter()
                    .map(|text| {
                        if text.contains("web_search") || text.contains("网页") {
                            vec![1.0, 0.0]
                        } else {
                            vec![0.0, 1.0]
                        }
                    })
                    .collect(),
            })
        })
    }
}
fn catalog(names: &[&str]) -> AuthorizedToolCatalog {
    AuthorizedToolCatalog::new(
        names
            .iter()
            .map(|name| {
                let key = VersionedKey::new(format!("{name}@1")).unwrap();
                let schema = b"{\"type\":\"object\"}".to_vec();
                AuthorizedToolDescriptor {
                    provided_capability: None,
                    tool_ref: ToolKey::from_versioned_key(key.clone()),
                    capability_ref: key,
                    wire_name: WireName::new(*name).unwrap(),
                    title: name.to_string().into(),
                    description: format!("{name} English documentation").into(),
                    tags: Vec::new(),
                    when_to_use: None,
                    schema_hash: Sha256::digest(&schema),
                    schema: schema.into(),
                    provider_id: "019fa6de-38fd-7000-8000-000000000402".parse().unwrap(),
                    location: CapabilityLocation::Backend,
                    required_contexts: BTreeSet::new(),
                }
            })
            .collect(),
    )
    .unwrap()
}
#[tokio::test]
async fn multilingual_results_reuse_the_index_but_never_another_authorized_catalogue_or_model() {
    let provider = Arc::new(Embeddings {
        calls: AtomicU32::new(0),
        revision: AtomicU32::new(1),
        mismatch: false,
    });
    let search = SemanticToolSearch::new(provider.clone());
    let tools = catalog(&["web_search", "calendar"]);
    assert!(
        search
            .scores(&tools, "网页搜索")
            .await
            .unwrap()
            .contains_key("web_search@1")
    );
    search.scores(&tools, "网页搜索").await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert!(
        search
            .scores(&catalog(&["calendar"]), "网页搜索")
            .await
            .unwrap()
            .is_empty()
    );
    provider.revision.store(2, Ordering::SeqCst);
    search.scores(&tools, "网页搜索").await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 6);
}
#[tokio::test]
async fn route_changes_cannot_mix_incompatible_embedding_spaces() {
    let search = SemanticToolSearch::new(Arc::new(Embeddings {
        calls: AtomicU32::new(0),
        revision: AtomicU32::new(1),
        mismatch: true,
    }));
    assert!(
        search
            .scores(&catalog(&["web_search"]), "网页搜索")
            .await
            .is_err()
    );
}
