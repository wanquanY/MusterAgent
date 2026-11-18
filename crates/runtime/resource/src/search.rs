use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{ArtifactId, ArtifactVersionId, ResourceNodeId, VersionNumber};
use musteragent_resource::ResourceScope;
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

use crate::{ResourceCommandContext, ResourceStoreError};

pub const DEFAULT_RESOURCE_SEARCH_LIMIT: usize = 10;
pub const MAX_RESOURCE_SEARCH_LIMIT: usize = 50;
pub const MAX_RESOURCE_SEARCH_QUERY_BYTES: usize = 512;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchResourceFiles {
    pub context: ResourceCommandContext,
    pub query: String,
    pub limit: usize,
}

impl SearchResourceFiles {
    pub fn validate(&self) -> Result<(), ResourceSearchError> {
        self.context
            .validate()
            .map_err(|_| ResourceSearchError::IdentityMismatch)?;
        let query = self.query.trim();
        if query.is_empty()
            || query.len() > MAX_RESOURCE_SEARCH_QUERY_BYTES
            || self.limit == 0
            || self.limit > MAX_RESOURCE_SEARCH_LIMIT
        {
            return Err(ResourceSearchError::InvalidQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ResourceSearchLocator {
    pub page_number: Option<u32>,
    pub slide_number: Option<u32>,
    pub line_start: Option<u32>,
    pub line_end: Option<u32>,
    pub start_byte: u64,
    pub end_byte: u64,
    pub section_path: Vec<String>,
    pub cell_range: Option<String>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ResourceSearchMatchedBy {
    Lexical,
    Semantic,
    Hybrid,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ResourceSemanticSearchState {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceSearchHit {
    pub node_id: ResourceNodeId,
    pub artifact_id: ArtifactId,
    pub artifact_version_id: ArtifactVersionId,
    pub file_version: VersionNumber,
    pub path: String,
    pub media_type: MediaType,
    pub content_sha256: Sha256,
    pub manifest_id: uuid::Uuid,
    pub chunk_id: uuid::Uuid,
    pub chunk_ordinal: u32,
    pub snippet: String,
    pub locator: ResourceSearchLocator,
    /// Deterministic fixed-point score, where 1_000_000 represents 1.0.
    pub lexical_score_micros: u32,
    pub semantic_score_micros: Option<u32>,
    pub matched_by: ResourceSearchMatchedBy,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceSearchPage {
    pub scope: ResourceScope,
    pub hits: Vec<ResourceSearchHit>,
    pub semantic_state: ResourceSemanticSearchState,
}

pub trait ResourceSearchStore: Send + Sync {
    fn search_lexical(
        &self,
        query: &SearchResourceFiles,
    ) -> ApplicationFuture<'_, Result<Vec<ResourceSearchHit>, ResourceStoreError>>;
}

pub trait ResourceSearchApplicationPort: Send + Sync {
    fn search(
        &self,
        query: SearchResourceFiles,
    ) -> ApplicationFuture<'_, Result<ResourceSearchPage, ResourceSearchError>>;
}

/// Implemented by the configured semantic provider, including current-model
/// index coverage checks. A catalog flag alone cannot establish availability.
pub trait ResourceSemanticSearch: Send + Sync {
    fn search<'a>(
        &'a self,
        query: &'a SearchResourceFiles,
    ) -> ApplicationFuture<'a, Result<Option<Vec<ResourceSearchHit>>, ResourceStoreError>>;
}

pub struct ResourceSearchApplication<S> {
    store: S,
    semantic: Option<std::sync::Arc<dyn ResourceSemanticSearch>>,
}

impl<S> ResourceSearchApplication<S>
where
    S: ResourceSearchStore,
{
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self {
            store,
            semantic: None,
        }
    }

    #[must_use]
    pub fn with_semantic(mut self, semantic: std::sync::Arc<dyn ResourceSemanticSearch>) -> Self {
        self.semantic = Some(semantic);
        self
    }

    pub async fn search(
        &self,
        query: SearchResourceFiles,
    ) -> Result<ResourceSearchPage, ResourceSearchError> {
        query.validate()?;
        let scope = query.context.scope();
        let mut hits = self.store.search_lexical(&query).await?;
        let semantic_hits = match &self.semantic {
            Some(provider) => match provider.search(&query).await {
                Ok(hits) => hits,
                Err(ResourceStoreError::DependencyUnavailable) => None,
                Err(error) => return Err(error.into()),
            },
            None => None,
        };
        let semantic_state = if let Some(semantic_hits) = semantic_hits {
            // Reciprocal rank fusion does not compare unrelated lexical and
            // cosine score scales. Stable IDs break ties deterministically.
            let mut ranked = std::collections::BTreeMap::new();
            for (rank, hit) in hits.drain(..).enumerate() {
                ranked.insert(hit.chunk_id, (1_000_000 / (60 + rank as u64), hit));
            }
            for (rank, hit) in semantic_hits.into_iter().enumerate() {
                let score = 1_000_000 / (60 + rank as u64);
                if let Some((weight, existing)) = ranked.get_mut(&hit.chunk_id) {
                    *weight += score;
                    existing.semantic_score_micros = hit.semantic_score_micros;
                    existing.matched_by = ResourceSearchMatchedBy::Hybrid;
                } else {
                    ranked.insert(hit.chunk_id, (score, hit));
                }
            }
            let mut ranked = ranked.into_values().collect::<Vec<_>>();
            ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.chunk_id.cmp(&b.1.chunk_id)));
            hits = ranked
                .into_iter()
                .take(query.limit)
                .map(|(_, hit)| hit)
                .collect();
            ResourceSemanticSearchState::Available
        } else {
            ResourceSemanticSearchState::Unavailable
        };
        Ok(ResourceSearchPage {
            scope,
            hits,
            semantic_state,
        })
    }
}

impl<S> ResourceSearchApplicationPort for ResourceSearchApplication<S>
where
    S: ResourceSearchStore,
{
    fn search(
        &self,
        query: SearchResourceFiles,
    ) -> ApplicationFuture<'_, Result<ResourceSearchPage, ResourceSearchError>> {
        Box::pin(ResourceSearchApplication::search(self, query))
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ResourceSearchError {
    #[error("Resource search identity is invalid")]
    IdentityMismatch,
    #[error("Resource search query is invalid")]
    InvalidQuery,
    #[error(transparent)]
    Store(#[from] ResourceStoreError),
}

#[cfg(test)]
mod tests {
    use super::{MAX_RESOURCE_SEARCH_QUERY_BYTES, ResourceSearchError, SearchResourceFiles};
    use crate::ResourceCommandContext;
    use musteragent_identity::{
        Actor, ConversationId, PrincipalId, PrincipalKind, TenantId, UtcTimestamp,
    };

    fn context() -> ResourceCommandContext {
        let tenant_id = TenantId::generate();
        ResourceCommandContext {
            tenant_id,
            actor: Actor {
                tenant_id,
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            source: crate::ResourceCommandSource::Conversation {
                conversation_id: ConversationId::generate(),
                workspace_id: None,
            },
            invocation_id: None,
            tool_key: None,
            now: UtcTimestamp::from_unix_millis(1),
        }
    }

    #[test]
    fn query_contract_is_bounded() {
        let mut query = SearchResourceFiles {
            context: context(),
            query: "architecture".to_owned(),
            limit: 10,
        };
        assert_eq!(query.validate(), Ok(()));
        query.query = " ".to_owned();
        assert_eq!(query.validate(), Err(ResourceSearchError::InvalidQuery));
        query.query = "x".repeat(MAX_RESOURCE_SEARCH_QUERY_BYTES + 1);
        assert_eq!(query.validate(), Err(ResourceSearchError::InvalidQuery));
    }
}
