//! Rebuildable semantic-search projection contracts.
//!
//! Memory records and immutable versions remain authoritative. Embeddings are
//! deliberately modeled as a replaceable projection so model changes, vector
//! extension failures, and backfills cannot block Memory writes or retrieval.

use crate::MemoryEmbeddingDeadline;

use std::num::{NonZeroU16, NonZeroU32};
use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_identity::{MemoryId, MemoryVersionId, TenantId, UtcTimestamp, VersionedKey};
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;
use uuid::Uuid;

use crate::retrieval::canonical_memory_semantic_text;
use crate::{
    MemoryPersistenceDisposition, MemorySearch, MemorySearchBackfillSource, MemorySearchHit,
};

pub const MEMORY_EMBEDDING_MODEL_CLASS: &str = "memory.embedding";
pub const MAX_MEMORY_EMBEDDING_DIMENSIONS: u32 = 16_000;
pub const MIN_MEMORY_EMBEDDING_INPUT_TOKENS: u32 = 256;
pub const MAX_MEMORY_EMBEDDING_INPUT_TOKENS: u32 = 1_000_000;
pub const MAX_MEMORY_EMBEDDING_BATCH_INPUTS: usize = 32;
pub const MAX_MEMORY_EMBEDDING_CHUNKS: usize = 1_024;
pub const MEMORY_EMBEDDING_CONTENT_POLICY: &str =
    "musteragent.memory-embedding.content.canonical-semantic-chunks-utf8-upper-bound@3";
pub const MEMORY_VECTOR_INDEX_VERSION: &str = "musteragent.memory-vector-index@2";
const LEGACY_MEMORY_EMBEDDING_CONTENT_POLICY: &str =
    "musteragent.legacy.memory-embedding.content.canonical-semantic-chunks-utf8-upper-bound@2";
const LEGACY_MEMORY_VECTOR_INDEX_VERSION: &str = "musteragent.legacy.memory-vector-index@1";
const MEMORY_EMBEDDING_SPACE_PROBE: &str = "Musteragent Memory semantic index capability probe";
const TARGET_MEMORY_EMBEDDING_CHUNK_BYTES: usize = 4 * 1_024;
const MEMORY_EMBEDDING_TOKEN_SAFETY_NUMERATOR: usize = 15;
const MEMORY_EMBEDDING_TOKEN_SAFETY_DENOMINATOR: usize = 16;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MemoryEmbeddingMetric {
    Cosine,
}

impl MemoryEmbeddingMetric {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
        }
    }

    pub fn parse(value: &str) -> Result<Self, MemoryVectorError> {
        match value {
            "cosine" => Ok(Self::Cosine),
            _ => Err(MemoryVectorError::Corrupt),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MemoryEmbeddingNormalization {
    None,
}

impl MemoryEmbeddingNormalization {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
        }
    }

    pub fn parse(value: &str) -> Result<Self, MemoryVectorError> {
        match value {
            "none" => Ok(Self::None),
            _ => Err(MemoryVectorError::Corrupt),
        }
    }
}

#[must_use]
pub fn memory_embedding_content_policy_hash() -> Sha256 {
    Sha256::digest(MEMORY_EMBEDDING_CONTENT_POLICY.as_bytes())
}

#[must_use]
pub fn memory_embedding_space_id(model_key: &str, model_revision: &str, dimensions: u32) -> Uuid {
    embedding_space_id(
        model_key,
        model_revision,
        dimensions,
        MEMORY_EMBEDDING_CONTENT_POLICY,
        MEMORY_VECTOR_INDEX_VERSION,
    )
}

fn embedding_space_id(
    model_key: &str,
    model_revision: &str,
    dimensions: u32,
    content_policy: &str,
    index_version: &str,
) -> Uuid {
    const NAMESPACE: Uuid = Uuid::from_bytes([
        0x01, 0x9f, 0xf5, 0x02, 0xe8, 0xfd, 0x75, 0x62, 0x93, 0x66, 0x92, 0x16, 0x8a, 0x7d, 0x19,
        0x48,
    ]);
    Uuid::new_v5(
        &NAMESPACE,
        format!(
            "{model_key}\0{model_revision}\0{dimensions}\0{}\0{}\0{}\0{}",
            MemoryEmbeddingMetric::Cosine.as_str(),
            MemoryEmbeddingNormalization::None.as_str(),
            Sha256::digest(content_policy.as_bytes()),
            index_version,
        )
        .as_bytes(),
    )
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MemoryEmbeddingSpaceState {
    Building,
    Active,
    Retired,
    Failed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemoryEmbeddingSpace {
    pub space_id: Uuid,
    pub model_key: String,
    /// Immutable provider/model revision or configuration digest. A changed
    /// revision creates a new Space; vectors from different Spaces never mix.
    pub model_revision: String,
    pub dimensions: NonZeroU32,
    pub metric: MemoryEmbeddingMetric,
    pub normalization: MemoryEmbeddingNormalization,
    /// Changing canonical content selection creates a new immutable Space even
    /// when Admin keeps the same provider model and dimensionality.
    pub content_policy_hash: Sha256,
    /// Storage/index semantics are versioned independently from the model.
    pub index_version: VersionedKey,
    pub state: MemoryEmbeddingSpaceState,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl MemoryEmbeddingSpace {
    pub fn building(
        model_key: String,
        model_revision: String,
        dimensions: NonZeroU32,
        now: UtcTimestamp,
    ) -> Result<Self, MemoryVectorError> {
        let space = Self {
            space_id: memory_embedding_space_id(&model_key, &model_revision, dimensions.get()),
            model_key,
            model_revision,
            dimensions,
            metric: MemoryEmbeddingMetric::Cosine,
            normalization: MemoryEmbeddingNormalization::None,
            content_policy_hash: memory_embedding_content_policy_hash(),
            index_version: VersionedKey::new(MEMORY_VECTOR_INDEX_VERSION)
                .map_err(|_| MemoryVectorError::Corrupt)?,
            state: MemoryEmbeddingSpaceState::Building,
            created_at: now,
            updated_at: now,
        };
        space.validate()?;
        Ok(space)
    }

    pub fn validate(&self) -> Result<(), MemoryVectorError> {
        let current_contract = self.content_policy_hash == memory_embedding_content_policy_hash()
            && self.index_version.as_str() == MEMORY_VECTOR_INDEX_VERSION
            && self.space_id
                == memory_embedding_space_id(
                    &self.model_key,
                    &self.model_revision,
                    self.dimensions.get(),
                );
        let legacy_contract = self.content_policy_hash
            == Sha256::digest(LEGACY_MEMORY_EMBEDDING_CONTENT_POLICY.as_bytes())
            && self.index_version.as_str() == LEGACY_MEMORY_VECTOR_INDEX_VERSION
            && self.space_id
                == embedding_space_id(
                    &self.model_key,
                    &self.model_revision,
                    self.dimensions.get(),
                    LEGACY_MEMORY_EMBEDDING_CONTENT_POLICY,
                    LEGACY_MEMORY_VECTOR_INDEX_VERSION,
                );
        if self.model_key.trim().is_empty()
            || self.model_key.len() > 512
            || self.model_revision.trim().is_empty()
            || self.model_revision.len() > 512
            || self.dimensions.get() > MAX_MEMORY_EMBEDDING_DIMENSIONS
            || self.updated_at < self.created_at
            || self.metric != MemoryEmbeddingMetric::Cosine
            || self.normalization != MemoryEmbeddingNormalization::None
            || !(current_contract || legacy_contract)
        {
            return Err(MemoryVectorError::InvalidInput);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryEmbeddingVector {
    pub tenant_id: TenantId,
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
    pub space_id: Uuid,
    pub chunk_ordinal: u16,
    pub source_content_hash: Sha256,
    pub values: Vec<f32>,
    pub embedded_at: UtcTimestamp,
}

impl MemoryEmbeddingVector {
    pub fn validate_for(&self, space: &MemoryEmbeddingSpace) -> Result<(), MemoryVectorError> {
        space.validate()?;
        if self.space_id != space.space_id
            || self.values.len()
                != usize::try_from(space.dimensions.get())
                    .map_err(|_| MemoryVectorError::InvalidInput)?
            || self.values.is_empty()
            || self.values.iter().any(|value| !value.is_finite())
        {
            return Err(MemoryVectorError::DimensionMismatch);
        }
        Ok(())
    }

    pub fn validate_batch_for(
        space: &MemoryEmbeddingSpace,
        vectors: &[Self],
    ) -> Result<(), MemoryVectorError> {
        let Some(first) = vectors.first() else {
            return Err(MemoryVectorError::InvalidInput);
        };
        if vectors.len() > MAX_MEMORY_EMBEDDING_CHUNKS {
            return Err(MemoryVectorError::InvalidInput);
        }
        for (ordinal, vector) in vectors.iter().enumerate() {
            vector.validate_for(space)?;
            if vector.tenant_id != first.tenant_id
                || vector.memory_id != first.memory_id
                || vector.memory_version_id != first.memory_version_id
                || vector.source_content_hash != first.source_content_hash
                || usize::from(vector.chunk_ordinal) != ordinal
            {
                return Err(MemoryVectorError::InvalidInput);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryVectorSearch {
    pub visibility: MemorySearch,
    pub space_id: Uuid,
    pub values: Vec<f32>,
    pub limit: NonZeroU16,
}

impl MemoryVectorSearch {
    pub fn validate(&self) -> Result<(), MemoryVectorError> {
        self.visibility.validate()?;
        if self.values.is_empty()
            || self.values.len()
                > usize::try_from(MAX_MEMORY_EMBEDDING_DIMENSIONS)
                    .map_err(|_| MemoryVectorError::InvalidInput)?
            || self.values.iter().any(|value| !value.is_finite())
            || self.limit.get() > 100
        {
            return Err(MemoryVectorError::InvalidInput);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemoryVectorSearchHit {
    pub memory: MemorySearchHit,
    /// Cosine similarity normalized into an implementation-independent range.
    pub similarity_bps: u16,
}

/// Storage projection shared by PostgreSQL/pgvector and SQLite/sqlite-vec.
/// `Unavailable` is an expected capability state and must be handled by FTS.
pub trait MemoryVectorIndex: Send + Sync {
    fn ensure_space(
        &self,
        space: MemoryEmbeddingSpace,
    ) -> ApplicationFuture<'_, Result<(), MemoryVectorError>>;

    /// Atomically replaces every semantic chunk for one immutable Memory
    /// version. A partial chunk set must never make a backfill look complete.
    fn replace_vectors(
        &self,
        space: MemoryEmbeddingSpace,
        vectors: Vec<MemoryEmbeddingVector>,
    ) -> ApplicationFuture<'_, Result<(), MemoryVectorError>>;

    fn search_vector(
        &self,
        query: MemoryVectorSearch,
    ) -> ApplicationFuture<'_, Result<Vec<MemoryVectorSearchHit>, MemoryVectorError>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryEmbeddingBatch {
    pub model_key: String,
    pub model_revision: String,
    pub dimensions: NonZeroU32,
    pub vectors: Vec<Vec<f32>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemoryEmbeddingModel {
    pub model_key: String,
    pub model_revision: String,
    /// Provider-declared maximum tokens for one input item. Chunking uses a
    /// conservative UTF-8 byte upper bound, so it remains safe when an
    /// OpenAI-compatible provider does not expose its exact tokenizer.
    pub max_input_tokens: NonZeroU32,
}

impl MemoryEmbeddingModel {
    pub fn validate(&self) -> Result<(), MemoryEmbeddingError> {
        if self.model_key.trim().is_empty()
            || self.model_key.len() > 512
            || self.model_revision.trim().is_empty()
            || self.model_revision.len() > 512
            || self.max_input_tokens.get() < MIN_MEMORY_EMBEDDING_INPUT_TOKENS
            || self.max_input_tokens.get() > MAX_MEMORY_EMBEDDING_INPUT_TOKENS
        {
            return Err(MemoryEmbeddingError::InvalidResponse);
        }
        Ok(())
    }
}

pub trait MemoryEmbeddingProvider: Send + Sync {
    /// Cheap control-plane identity lookup used to detect Admin model changes
    /// without repeatedly paying for probe embeddings.
    fn configured_model(
        &self,
        deadline: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingModel, MemoryEmbeddingError>>;

    fn embed(
        &self,
        inputs: Vec<String>,
        deadline: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingBatch, MemoryEmbeddingError>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimedMemoryEmbeddingBackfill {
    pub space: MemoryEmbeddingSpace,
    pub source: MemorySearchBackfillSource,
    pub attempt_count: u16,
    pub lease_owner: String,
    pub lease_expires_at: UtcTimestamp,
}

impl ClaimedMemoryEmbeddingBackfill {
    pub fn validate(&self) -> Result<(), MemoryVectorError> {
        self.space.validate()?;
        self.source
            .validate()
            .map_err(|_| MemoryVectorError::InvalidInput)?;
        if self.attempt_count == 0 || self.attempt_count > 10 || self.lease_owner.trim().is_empty()
        {
            return Err(MemoryVectorError::InvalidInput);
        }
        Ok(())
    }
}

/// Durable source selection and leasing for the rebuildable semantic
/// projection. Implementations always join back to authoritative active heads.
pub trait MemoryEmbeddingBackfillStore: Send + Sync {
    type Error: Send;

    fn projection_seed(
        &self,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchBackfillSource>, Self::Error>>;

    fn embedding_projection_space(
        &self,
    ) -> ApplicationFuture<'_, Result<Option<MemoryEmbeddingSpace>, Self::Error>>;

    /// Atomically retires the old Active Space once every eligible source is
    /// either projected or durably terminal. Terminal sources remain
    /// retrievable through FTS and cannot hold the whole semantic Space open.
    fn activate_embedding_space_if_ready(
        &self,
        space: MemoryEmbeddingSpace,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<bool, Self::Error>>;

    fn enqueue_missing_embeddings(
        &self,
        space: MemoryEmbeddingSpace,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<u64, Self::Error>>;

    fn claim_embedding_backfill(
        &self,
        space: MemoryEmbeddingSpace,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Option<ClaimedMemoryEmbeddingBackfill>, Self::Error>>;

    fn complete_embedding_backfill(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;

    fn fail_embedding_backfill(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        error_code: VersionedKey,
        failed_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;

    fn reject_embedding_backfill(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        error_code: VersionedKey,
        rejected_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MemoryEmbeddingBackfillRun {
    Idle,
    Projected {
        memory_version_id: MemoryVersionId,
        space_id: Uuid,
    },
    Deferred {
        memory_version_id: Option<MemoryVersionId>,
        reason: MemoryEmbeddingBackfillDegradation,
    },
    Rejected {
        memory_version_id: MemoryVersionId,
        reason: MemoryEmbeddingBackfillDegradation,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MemoryEmbeddingBackfillDegradation {
    EmbeddingNotConfigured,
    EmbeddingUnavailable,
    EmbeddingTimeout,
    EmbeddingInvalidResponse,
    EmbeddingInputRejected,
    VectorUnavailable,
    VectorFailed,
}

pub struct MemoryEmbeddingBackfiller<S>
where
    S: MemoryEmbeddingBackfillStore,
{
    store: Arc<S>,
    content: Arc<dyn ContentStore>,
    embeddings: Arc<dyn MemoryEmbeddingProvider>,
    vectors: Arc<dyn MemoryVectorIndex>,
}

impl<S> MemoryEmbeddingBackfiller<S>
where
    S: MemoryEmbeddingBackfillStore,
{
    #[must_use]
    pub fn new(
        store: Arc<S>,
        content: Arc<dyn ContentStore>,
        embeddings: Arc<dyn MemoryEmbeddingProvider>,
        vectors: Arc<dyn MemoryVectorIndex>,
    ) -> Self {
        Self {
            store,
            content,
            embeddings,
            vectors,
        }
    }

    /// Performs at most one projection attempt. Dependency failures become an
    /// explicit degraded outcome and never escape into chat/Memory writes.
    pub async fn run_once(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> Result<MemoryEmbeddingBackfillRun, MemoryEmbeddingBackfillError> {
        if worker_id.trim().is_empty() || lease_expires_at <= now || retry_at <= now {
            return Err(MemoryEmbeddingBackfillError::InvalidCommand);
        }
        let deadline = MemoryEmbeddingDeadline::before_lease_deadline(now, lease_expires_at);
        let catalog_deadline = deadline.catalog();
        let configured = match catalog_deadline
            .run(self.embeddings.configured_model(catalog_deadline))
            .await
        {
            Ok(configured) => configured,
            Err(MemoryEmbeddingError::NotConfigured) => {
                return Ok(MemoryEmbeddingBackfillRun::Deferred {
                    memory_version_id: None,
                    reason: MemoryEmbeddingBackfillDegradation::EmbeddingNotConfigured,
                });
            }
            Err(MemoryEmbeddingError::Timeout) => {
                return Ok(MemoryEmbeddingBackfillRun::Deferred {
                    memory_version_id: None,
                    reason: MemoryEmbeddingBackfillDegradation::EmbeddingTimeout,
                });
            }
            Err(MemoryEmbeddingError::Unavailable) => {
                return Ok(MemoryEmbeddingBackfillRun::Deferred {
                    memory_version_id: None,
                    reason: MemoryEmbeddingBackfillDegradation::EmbeddingUnavailable,
                });
            }
            Err(MemoryEmbeddingError::InvalidResponse) => {
                return Ok(MemoryEmbeddingBackfillRun::Deferred {
                    memory_version_id: None,
                    reason: MemoryEmbeddingBackfillDegradation::EmbeddingInvalidResponse,
                });
            }
            Err(MemoryEmbeddingError::InvalidInput) => {
                return Ok(MemoryEmbeddingBackfillRun::Deferred {
                    memory_version_id: None,
                    reason: MemoryEmbeddingBackfillDegradation::EmbeddingInputRejected,
                });
            }
        };
        configured.validate()?;
        let existing_space = self
            .store
            .embedding_projection_space()
            .await
            .map_err(|_| MemoryEmbeddingBackfillError::Store)?;
        let space = match existing_space {
            Some(space)
                if space.model_key == configured.model_key
                    && space.model_revision == configured.model_revision
                    && space.validate().is_ok() =>
            {
                space
            }
            _ => {
                if self
                    .store
                    .projection_seed()
                    .await
                    .map_err(|_| MemoryEmbeddingBackfillError::Store)?
                    .is_none()
                {
                    return Ok(MemoryEmbeddingBackfillRun::Idle);
                }
                match self.initialize_space(&configured, now, deadline).await {
                    Ok(space) => space,
                    Err(error) => {
                        let reason = match error {
                            MemoryEmbeddingBackfillError::Vector(vector) => {
                                vector_backfill_degradation(vector)
                            }
                            error => embedding_backfill_degradation(error)?,
                        };
                        return Ok(MemoryEmbeddingBackfillRun::Deferred {
                            memory_version_id: None,
                            reason,
                        });
                    }
                }
            }
        };
        self.store
            .enqueue_missing_embeddings(space.clone(), now)
            .await
            .map_err(|_| MemoryEmbeddingBackfillError::Store)?;
        if let Some(claimed) = self
            .store
            .claim_embedding_backfill(space.clone(), worker_id, now, lease_expires_at)
            .await
            .map_err(|_| MemoryEmbeddingBackfillError::Store)?
        {
            return self
                .project_claimed(claimed, &configured, now, retry_at, deadline)
                .await;
        }
        self.store
            .activate_embedding_space_if_ready(space, now)
            .await
            .map_err(|_| MemoryEmbeddingBackfillError::Store)?;
        Ok(MemoryEmbeddingBackfillRun::Idle)
    }

    async fn initialize_space(
        &self,
        configured: &MemoryEmbeddingModel,
        now: UtcTimestamp,
        deadline: MemoryEmbeddingDeadline,
    ) -> Result<MemoryEmbeddingSpace, MemoryEmbeddingBackfillError> {
        let deadline = deadline.request();
        let batch = deadline
            .run(
                self.embeddings
                    .embed(vec![MEMORY_EMBEDDING_SPACE_PROBE.to_owned()], deadline),
            )
            .await?;
        validate_embedding_batch(&batch, configured, 1)?;
        let space = MemoryEmbeddingSpace::building(
            batch.model_key,
            batch.model_revision,
            batch.dimensions,
            now,
        )?;
        self.vectors
            .ensure_space(space.clone())
            .await
            .map_err(MemoryEmbeddingBackfillError::Vector)?;
        Ok(space)
    }

    async fn project_claimed(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        configured: &MemoryEmbeddingModel,
        now: UtcTimestamp,
        retry_at: UtcTimestamp,
        deadline: MemoryEmbeddingDeadline,
    ) -> Result<MemoryEmbeddingBackfillRun, MemoryEmbeddingBackfillError> {
        claimed.validate()?;
        let memory_version_id = claimed.source.memory_version_id;
        let projected = match self
            .project_source(&claimed.source, &claimed.space, configured, now, deadline)
            .await
        {
            Ok(projected) => projected,
            Err(error) => {
                let reason = embedding_backfill_degradation(error)?;
                if reason == MemoryEmbeddingBackfillDegradation::EmbeddingInputRejected {
                    self.reject_claim(
                        claimed,
                        "musteragent.memory-embedding.input-rejected@3",
                        now,
                    )
                    .await?;
                    return Ok(MemoryEmbeddingBackfillRun::Rejected {
                        memory_version_id,
                        reason,
                    });
                }
                self.defer_claim(
                    claimed,
                    if reason == MemoryEmbeddingBackfillDegradation::EmbeddingTimeout {
                        "musteragent.memory-embedding.timeout@1"
                    } else {
                        "musteragent.memory-embedding.unavailable@3"
                    },
                    now,
                    retry_at,
                )
                .await?;
                return Ok(MemoryEmbeddingBackfillRun::Deferred {
                    memory_version_id: Some(memory_version_id),
                    reason,
                });
            }
        };
        if let Err(error) = self
            .vectors
            .replace_vectors(projected.0.clone(), projected.1)
            .await
        {
            let reason = vector_backfill_degradation(error);
            self.defer_claim(
                claimed,
                "musteragent.memory-vector.unavailable@2",
                now,
                retry_at,
            )
            .await?;
            return Ok(MemoryEmbeddingBackfillRun::Deferred {
                memory_version_id: Some(memory_version_id),
                reason,
            });
        }
        self.store
            .complete_embedding_backfill(claimed, now)
            .await
            .map_err(|_| MemoryEmbeddingBackfillError::Store)?;
        Ok(MemoryEmbeddingBackfillRun::Projected {
            memory_version_id,
            space_id: projected.0.space_id,
        })
    }

    async fn project_source(
        &self,
        source: &MemorySearchBackfillSource,
        space: &MemoryEmbeddingSpace,
        configured: &MemoryEmbeddingModel,
        now: UtcTimestamp,
        deadline: MemoryEmbeddingDeadline,
    ) -> Result<(MemoryEmbeddingSpace, Vec<MemoryEmbeddingVector>), MemoryEmbeddingBackfillError>
    {
        source
            .validate()
            .map_err(|_| MemoryEmbeddingBackfillError::InvalidSource)?;
        let bytes = self
            .content
            .read(source.tenant_id, &source.content_ref)
            .await
            .map_err(|_| MemoryEmbeddingBackfillError::ContentUnavailable)?;
        verify_content(&source.content_ref, &bytes)
            .map_err(|_| MemoryEmbeddingBackfillError::InvalidSource)?;
        let text = canonical_memory_semantic_text(&bytes)
            .map_err(|_| MemoryEmbeddingBackfillError::InvalidSource)?;
        let chunks = memory_embedding_chunks(&text, configured.max_input_tokens)?;
        let mut embedded_chunks = Vec::with_capacity(chunks.len());
        for inputs in chunks.chunks(MAX_MEMORY_EMBEDDING_BATCH_INPUTS) {
            let request_deadline = deadline.request();
            let batch = request_deadline
                .run(self.embeddings.embed(inputs.to_vec(), request_deadline))
                .await?;
            validate_embedding_batch(&batch, configured, inputs.len())?;
            if batch.dimensions != space.dimensions {
                return Err(MemoryEmbeddingBackfillError::EmbeddingInvalidResponse);
            }
            embedded_chunks.extend(batch.vectors);
        }
        if space.model_key != configured.model_key
            || space.model_revision != configured.model_revision
            || space.validate().is_err()
        {
            return Err(MemoryEmbeddingBackfillError::EmbeddingInvalidResponse);
        }
        let vectors = embedded_chunks
            .into_iter()
            .enumerate()
            .map(|(ordinal, values)| {
                Ok(MemoryEmbeddingVector {
                    tenant_id: source.tenant_id,
                    memory_id: source.memory_id,
                    memory_version_id: source.memory_version_id,
                    space_id: space.space_id,
                    chunk_ordinal: u16::try_from(ordinal)
                        .map_err(|_| MemoryEmbeddingBackfillError::InvalidSource)?,
                    source_content_hash: source.content_ref.sha256,
                    values,
                    embedded_at: now,
                })
            })
            .collect::<Result<Vec<_>, MemoryEmbeddingBackfillError>>()?;
        MemoryEmbeddingVector::validate_batch_for(space, &vectors)?;
        Ok((space.clone(), vectors))
    }

    async fn defer_claim(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        code: &str,
        now: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> Result<(), MemoryEmbeddingBackfillError> {
        let code =
            VersionedKey::new(code).map_err(|_| MemoryEmbeddingBackfillError::InvalidCommand)?;
        self.store
            .fail_embedding_backfill(claimed, code, now, retry_at)
            .await
            .map_err(|_| MemoryEmbeddingBackfillError::Store)?;
        Ok(())
    }

    async fn reject_claim(
        &self,
        claimed: ClaimedMemoryEmbeddingBackfill,
        code: &str,
        now: UtcTimestamp,
    ) -> Result<(), MemoryEmbeddingBackfillError> {
        let code =
            VersionedKey::new(code).map_err(|_| MemoryEmbeddingBackfillError::InvalidCommand)?;
        self.store
            .reject_embedding_backfill(claimed, code, now)
            .await
            .map_err(|_| MemoryEmbeddingBackfillError::Store)?;
        Ok(())
    }
}

fn validate_embedding_batch(
    batch: &MemoryEmbeddingBatch,
    configured: &MemoryEmbeddingModel,
    expected_vectors: usize,
) -> Result<(), MemoryEmbeddingBackfillError> {
    if batch.model_key != configured.model_key
        || batch.model_revision != configured.model_revision
        || batch.vectors.len() != expected_vectors
        || batch.vectors.is_empty()
        || batch.vectors.iter().any(|vector| {
            vector.len() != usize::try_from(batch.dimensions.get()).unwrap_or(usize::MAX)
                || vector.iter().any(|value| !value.is_finite())
        })
    {
        return Err(MemoryEmbeddingBackfillError::EmbeddingInvalidResponse);
    }
    Ok(())
}

fn memory_embedding_chunks(
    text: &str,
    max_input_tokens: NonZeroU32,
) -> Result<Vec<String>, MemoryEmbeddingBackfillError> {
    if max_input_tokens.get() < MIN_MEMORY_EMBEDDING_INPUT_TOKENS
        || max_input_tokens.get() > MAX_MEMORY_EMBEDDING_INPUT_TOKENS
    {
        return Err(MemoryEmbeddingBackfillError::InvalidSource);
    }
    let token_ceiling = usize::try_from(max_input_tokens.get()).unwrap_or(usize::MAX);
    let budget = token_ceiling
        .saturating_mul(MEMORY_EMBEDDING_TOKEN_SAFETY_NUMERATOR)
        .checked_div(MEMORY_EMBEDDING_TOKEN_SAFETY_DENOMINATOR)
        .unwrap_or(1)
        .clamp(1, TARGET_MEMORY_EMBEDDING_CHUNK_BYTES);
    let mut remaining = text.trim();
    let mut chunks = Vec::new();
    while !remaining.is_empty() {
        if chunks.len() >= MAX_MEMORY_EMBEDDING_CHUNKS {
            return Err(MemoryEmbeddingBackfillError::InvalidSource);
        }
        if remaining.len() <= budget {
            chunks.push(remaining.to_owned());
            break;
        }
        let hard_end = floor_char_boundary(remaining, budget);
        if hard_end == 0 {
            return Err(MemoryEmbeddingBackfillError::InvalidSource);
        }
        let minimum_soft_boundary = hard_end / 2;
        let soft_end = remaining[..hard_end]
            .char_indices()
            .rev()
            .find_map(|(index, character)| {
                let end = index + character.len_utf8();
                (end >= minimum_soft_boundary && is_embedding_chunk_boundary(character))
                    .then_some(end)
            })
            .unwrap_or(hard_end);
        let chunk = remaining[..soft_end].trim();
        if chunk.is_empty() {
            return Err(MemoryEmbeddingBackfillError::InvalidSource);
        }
        chunks.push(chunk.to_owned());
        remaining = remaining[soft_end..].trim_start();
    }
    if chunks.is_empty() || chunks.iter().any(|chunk| chunk.len() > budget) {
        return Err(MemoryEmbeddingBackfillError::InvalidSource);
    }
    Ok(chunks)
}

fn floor_char_boundary(value: &str, maximum: usize) -> usize {
    let mut boundary = maximum.min(value.len());
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    boundary
}

fn is_embedding_chunk_boundary(character: char) -> bool {
    character.is_whitespace()
        || matches!(
            character,
            '.' | ',' | ';' | ':' | '!' | '?' | '。' | '，' | '；' | '：' | '！' | '？'
        )
}

fn embedding_backfill_degradation(
    error: MemoryEmbeddingBackfillError,
) -> Result<MemoryEmbeddingBackfillDegradation, MemoryEmbeddingBackfillError> {
    match error {
        MemoryEmbeddingBackfillError::EmbeddingTimeout => {
            Ok(MemoryEmbeddingBackfillDegradation::EmbeddingTimeout)
        }
        MemoryEmbeddingBackfillError::EmbeddingNotConfigured => {
            Ok(MemoryEmbeddingBackfillDegradation::EmbeddingNotConfigured)
        }
        MemoryEmbeddingBackfillError::EmbeddingUnavailable
        | MemoryEmbeddingBackfillError::ContentUnavailable => {
            Ok(MemoryEmbeddingBackfillDegradation::EmbeddingUnavailable)
        }
        MemoryEmbeddingBackfillError::EmbeddingInvalidResponse => {
            Ok(MemoryEmbeddingBackfillDegradation::EmbeddingInvalidResponse)
        }
        MemoryEmbeddingBackfillError::InvalidSource => {
            Ok(MemoryEmbeddingBackfillDegradation::EmbeddingInputRejected)
        }
        MemoryEmbeddingBackfillError::EmbeddingInputRejected => {
            Ok(MemoryEmbeddingBackfillDegradation::EmbeddingInputRejected)
        }
        error => Err(error),
    }
}

fn vector_backfill_degradation(error: MemoryVectorError) -> MemoryEmbeddingBackfillDegradation {
    match error {
        MemoryVectorError::Unavailable => MemoryEmbeddingBackfillDegradation::VectorUnavailable,
        _ => MemoryEmbeddingBackfillDegradation::VectorFailed,
    }
}

#[derive(Debug, Error)]
pub enum MemoryEmbeddingBackfillError {
    #[error("Memory Embedding backfill command is invalid")]
    InvalidCommand,
    #[error("Memory Embedding source is invalid")]
    InvalidSource,
    #[error("Memory Embedding source content is unavailable")]
    ContentUnavailable,
    #[error("Admin memory.embedding is not configured")]
    EmbeddingNotConfigured,
    #[error("Memory Embedding deadline elapsed")]
    EmbeddingTimeout,
    #[error("Memory Embedding provider is unavailable")]
    EmbeddingUnavailable,
    #[error("Memory Embedding provider returned an invalid response")]
    EmbeddingInvalidResponse,
    #[error("Memory Embedding provider rejected the input")]
    EmbeddingInputRejected,
    #[error("Memory vector projection failed")]
    Vector(#[from] MemoryVectorError),
    #[error("Memory Embedding backfill store failed")]
    Store,
}

impl From<MemoryEmbeddingError> for MemoryEmbeddingBackfillError {
    fn from(error: MemoryEmbeddingError) -> Self {
        match error {
            MemoryEmbeddingError::Timeout => Self::EmbeddingTimeout,
            MemoryEmbeddingError::NotConfigured => Self::EmbeddingNotConfigured,
            MemoryEmbeddingError::Unavailable => Self::EmbeddingUnavailable,
            MemoryEmbeddingError::InvalidResponse => Self::EmbeddingInvalidResponse,
            MemoryEmbeddingError::InvalidInput => Self::EmbeddingInputRejected,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum MemoryEmbeddingError {
    #[error("Memory Embedding deadline elapsed")]
    Timeout,
    #[error("Admin memory.embedding is not configured")]
    NotConfigured,
    #[error("Memory Embedding provider is currently unavailable")]
    Unavailable,
    #[error("Embedding provider returned an invalid or dimensionally inconsistent response")]
    InvalidResponse,
    #[error("Embedding input violates the configured provider contract")]
    InvalidInput,
}

#[derive(Debug, Error)]
pub enum MemoryVectorError {
    #[error(transparent)]
    Application(#[from] crate::MemoryApplicationError),
    #[error("Vector projection input is invalid")]
    InvalidInput,
    #[error("Embedding vector dimensions do not match its immutable Space")]
    DimensionMismatch,
    #[error("Vector extension or projection is unavailable")]
    Unavailable,
    #[error("Vector projection contains inconsistent authority state")]
    Corrupt,
    #[error("Vector projection storage failed")]
    Store,
}

#[cfg(test)]
mod tests;
