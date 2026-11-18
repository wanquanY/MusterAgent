use std::sync::Mutex;

use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::ContentId;
use musteragent_runtime_content::{ContentStoreError, PutContent};

use super::*;

struct TestContentStore {
    tenant_id: TenantId,
    content_ref: ContentRef,
    bytes: Vec<u8>,
}

impl ContentStore for TestContentStore {
    fn put(
        &self,
        _request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    fn read<'a>(
        &'a self,
        tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            if tenant_id != self.tenant_id || content != &self.content_ref {
                return Err(ContentStoreError::NotFound);
            }
            Ok(self.bytes.clone())
        })
    }
}

#[derive(Default)]
struct TestBackfillState {
    space: Option<MemoryEmbeddingSpace>,
    claimed: bool,
    completed: bool,
    failed: bool,
    rejected: bool,
}

struct TestBackfillStore {
    source: MemorySearchBackfillSource,
    state: Mutex<TestBackfillState>,
}

impl MemoryEmbeddingBackfillStore for TestBackfillStore {
    type Error = ();

    fn projection_seed(
        &self,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchBackfillSource>, Self::Error>> {
        Box::pin(async { Ok(Some(self.source.clone())) })
    }

    fn embedding_projection_space(
        &self,
    ) -> ApplicationFuture<'_, Result<Option<MemoryEmbeddingSpace>, Self::Error>> {
        Box::pin(async { Ok(self.state.lock().expect("backfill state").space.clone()) })
    }

    fn activate_embedding_space_if_ready(
        &self,
        _space: MemoryEmbeddingSpace,
        _now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<bool, Self::Error>> {
        Box::pin(async { Ok(true) })
    }

    fn enqueue_missing_embeddings(
        &self,
        space: MemoryEmbeddingSpace,
        _now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<u64, Self::Error>> {
        Box::pin(async move {
            self.state.lock().expect("backfill state").space = Some(space);
            Ok(1)
        })
    }

    fn claim_embedding_backfill(
        &self,
        space: MemoryEmbeddingSpace,
        worker_id: String,
        _now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<Option<ClaimedMemoryEmbeddingBackfill>, Self::Error>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("backfill state");
            if state.claimed {
                return Ok(None);
            }
            state.claimed = true;
            Ok(Some(ClaimedMemoryEmbeddingBackfill {
                space,
                source: self.source.clone(),
                attempt_count: 1,
                lease_owner: worker_id,
                lease_expires_at,
            }))
        })
    }

    fn complete_embedding_backfill(
        &self,
        _claimed: ClaimedMemoryEmbeddingBackfill,
        _completed_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>> {
        Box::pin(async {
            self.state.lock().expect("backfill state").completed = true;
            Ok(MemoryPersistenceDisposition::Applied)
        })
    }

    fn fail_embedding_backfill(
        &self,
        _claimed: ClaimedMemoryEmbeddingBackfill,
        _error_code: VersionedKey,
        _failed_at: UtcTimestamp,
        _retry_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>> {
        Box::pin(async {
            self.state.lock().expect("backfill state").failed = true;
            Ok(MemoryPersistenceDisposition::Applied)
        })
    }

    fn reject_embedding_backfill(
        &self,
        _claimed: ClaimedMemoryEmbeddingBackfill,
        _error_code: VersionedKey,
        _rejected_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>> {
        Box::pin(async {
            self.state.lock().expect("backfill state").rejected = true;
            Ok(MemoryPersistenceDisposition::Applied)
        })
    }
}

struct TestEmbeddingProvider {
    inputs: Mutex<Vec<Vec<String>>>,
    reject_business_input: bool,
}

impl MemoryEmbeddingProvider for TestEmbeddingProvider {
    fn configured_model(
        &self,
        _deadline: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingModel, MemoryEmbeddingError>> {
        Box::pin(async {
            Ok(MemoryEmbeddingModel {
                model_key: "test.embedding".to_owned(),
                model_revision: "revision-1".to_owned(),
                max_input_tokens: NonZeroU32::new(8_192).expect("token ceiling"),
            })
        })
    }

    fn embed(
        &self,
        inputs: Vec<String>,
        _deadline: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingBatch, MemoryEmbeddingError>> {
        Box::pin(async move {
            let is_probe = inputs.as_slice() == [MEMORY_EMBEDDING_SPACE_PROBE];
            self.inputs
                .lock()
                .expect("embedding inputs")
                .push(inputs.clone());
            if self.reject_business_input && !is_probe {
                return Err(MemoryEmbeddingError::InvalidInput);
            }
            Ok(MemoryEmbeddingBatch {
                model_key: "test.embedding".to_owned(),
                model_revision: "revision-1".to_owned(),
                dimensions: NonZeroU32::new(2).expect("dimensions"),
                vectors: inputs.iter().map(|_| vec![0.25, 0.75]).collect(),
            })
        })
    }
}

#[derive(Default)]
struct TestVectorIndex {
    ensured: Mutex<Option<MemoryEmbeddingSpace>>,
    replacements: Mutex<Vec<Vec<MemoryEmbeddingVector>>>,
}

impl MemoryVectorIndex for TestVectorIndex {
    fn ensure_space(
        &self,
        space: MemoryEmbeddingSpace,
    ) -> ApplicationFuture<'_, Result<(), MemoryVectorError>> {
        Box::pin(async move {
            self.ensured.lock().expect("ensured Space").replace(space);
            Ok(())
        })
    }

    fn replace_vectors(
        &self,
        space: MemoryEmbeddingSpace,
        vectors: Vec<MemoryEmbeddingVector>,
    ) -> ApplicationFuture<'_, Result<(), MemoryVectorError>> {
        Box::pin(async move {
            MemoryEmbeddingVector::validate_batch_for(&space, &vectors)?;
            self.replacements
                .lock()
                .expect("vector replacements")
                .push(vectors);
            Ok(())
        })
    }

    fn search_vector(
        &self,
        _query: MemoryVectorSearch,
    ) -> ApplicationFuture<'_, Result<Vec<MemoryVectorSearchHit>, MemoryVectorError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn backfill_fixture(
    text: String,
    reject_business_input: bool,
) -> (
    MemoryEmbeddingBackfiller<TestBackfillStore>,
    Arc<TestBackfillStore>,
    Arc<TestEmbeddingProvider>,
    Arc<TestVectorIndex>,
) {
    let tenant_id = TenantId::generate();
    let bytes = text.into_bytes();
    let content_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("bounded test content"),
        sha256: Sha256::digest(&bytes),
    };
    let source = MemorySearchBackfillSource {
        tenant_id,
        memory_id: MemoryId::generate(),
        memory_version_id: MemoryVersionId::generate(),
        content_ref: content_ref.clone(),
        version_created_at: UtcTimestamp::from_unix_millis(50),
    };
    let store = Arc::new(TestBackfillStore {
        source,
        state: Mutex::new(TestBackfillState::default()),
    });
    let content: Arc<dyn ContentStore> = Arc::new(TestContentStore {
        tenant_id,
        content_ref,
        bytes,
    });
    let embeddings = Arc::new(TestEmbeddingProvider {
        inputs: Mutex::new(Vec::new()),
        reject_business_input,
    });
    let vectors = Arc::new(TestVectorIndex::default());
    let backfiller = MemoryEmbeddingBackfiller::new(
        Arc::clone(&store),
        content,
        embeddings.clone(),
        vectors.clone(),
    );
    (backfiller, store, embeddings, vectors)
}

#[test]
fn vector_rejects_non_finite_or_dimension_mismatched_values() {
    let now = UtcTimestamp::from_unix_millis(100);
    let mut space = MemoryEmbeddingSpace::building(
        "embedding-admin".to_owned(),
        "sha256:revision".to_owned(),
        NonZeroU32::new(3).expect("dimension"),
        now,
    )
    .expect("valid Space");
    space.state = MemoryEmbeddingSpaceState::Active;
    let mut vector = MemoryEmbeddingVector {
        tenant_id: TenantId::generate(),
        memory_id: MemoryId::generate(),
        memory_version_id: MemoryVersionId::generate(),
        space_id: space.space_id,
        chunk_ordinal: 0,
        source_content_hash: Sha256::digest(b"source"),
        values: vec![0.1, 0.2],
        embedded_at: now,
    };
    assert!(matches!(
        vector.validate_for(&space),
        Err(MemoryVectorError::DimensionMismatch)
    ));
    vector.values = vec![0.1, f32::NAN, 0.3];
    assert!(matches!(
        vector.validate_for(&space),
        Err(MemoryVectorError::DimensionMismatch)
    ));
}

#[test]
fn writers_publish_musteragent_v2_and_validation_reads_frozen_legacy_v1_spaces() {
    let now = UtcTimestamp::from_unix_millis(100);
    let current = MemoryEmbeddingSpace::building(
        "embedding-admin".to_owned(),
        "sha256:revision".to_owned(),
        NonZeroU32::new(3).expect("dimension"),
        now,
    )
    .expect("current Space");
    assert_eq!(
        current.index_version.as_str(),
        "musteragent.memory-vector-index@2"
    );

    let mut legacy = current.clone();
    legacy.content_policy_hash = Sha256::digest(LEGACY_MEMORY_EMBEDDING_CONTENT_POLICY.as_bytes());
    legacy.index_version =
        VersionedKey::new(LEGACY_MEMORY_VECTOR_INDEX_VERSION).expect("legacy index");
    legacy.space_id = embedding_space_id(
        &legacy.model_key,
        &legacy.model_revision,
        legacy.dimensions.get(),
        LEGACY_MEMORY_EMBEDDING_CONTENT_POLICY,
        LEGACY_MEMORY_VECTOR_INDEX_VERSION,
    );
    assert!(legacy.validate().is_ok());

    legacy.index_version = VersionedKey::new(MEMORY_VECTOR_INDEX_VERSION).expect("current index");
    assert!(matches!(
        legacy.validate(),
        Err(MemoryVectorError::InvalidInput)
    ));
}

#[test]
fn semantic_text_is_split_deterministically_below_the_model_input_ceiling() {
    let text = (0..2_000)
        .map(|index| format!("memory fact {index}."))
        .collect::<Vec<_>>()
        .join(" ");
    let ceiling = NonZeroU32::new(8_192).expect("token ceiling");

    let first = memory_embedding_chunks(&text, ceiling).expect("semantic chunks");
    let second = memory_embedding_chunks(&text, ceiling).expect("deterministic chunks");

    assert_eq!(first, second);
    assert!(first.len() > 1);
    assert!(first.len() <= MAX_MEMORY_EMBEDDING_BATCH_INPUTS);
    assert!(first.iter().all(|chunk| !chunk.is_empty()));
    assert!(first.iter().all(|chunk| chunk.len() <= 4_096));
    assert_eq!(first.join(" "), text);
}

#[test]
fn semantic_chunking_preserves_utf8_boundaries_under_a_small_profile() {
    let text = "数据库迁移应使用有界队列。".repeat(200);
    let ceiling = NonZeroU32::new(256).expect("token ceiling");

    let chunks = memory_embedding_chunks(&text, ceiling).expect("unicode chunks");

    assert!(chunks.len() > 1);
    assert!(chunks.iter().all(|chunk| chunk.len() <= 240));
    assert_eq!(chunks.concat(), text);
}

#[test]
fn embedding_vector_batch_must_be_contiguous_and_single_source() {
    let now = UtcTimestamp::from_unix_millis(100);
    let space = MemoryEmbeddingSpace::building(
        "embedding-admin".to_owned(),
        "sha256:revision".to_owned(),
        NonZeroU32::new(2).expect("dimension"),
        now,
    )
    .expect("valid Space");
    let tenant_id = TenantId::generate();
    let memory_id = MemoryId::generate();
    let memory_version_id = MemoryVersionId::generate();
    let source_content_hash = Sha256::digest(b"source");
    let vector = |chunk_ordinal| MemoryEmbeddingVector {
        tenant_id,
        memory_id,
        memory_version_id,
        space_id: space.space_id,
        chunk_ordinal,
        source_content_hash,
        values: vec![0.1, 0.2],
        embedded_at: now,
    };

    assert!(MemoryEmbeddingVector::validate_batch_for(&space, &[vector(0), vector(1)]).is_ok());
    assert!(matches!(
        MemoryEmbeddingVector::validate_batch_for(&space, &[vector(0), vector(2)]),
        Err(MemoryVectorError::InvalidInput)
    ));
}

#[test]
fn backfill_uses_a_controlled_probe_then_atomically_projects_semantic_chunks() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime")
        .block_on(async {
            let text = (0..2_000)
                .map(|index| format!("durable memory fact {index}."))
                .collect::<Vec<_>>()
                .join(" ");
            let (backfiller, store, embeddings, vectors) = backfill_fixture(text.clone(), false);

            let run = backfiller
                .run_once(
                    "worker-1".to_owned(),
                    UtcTimestamp::from_unix_millis(100),
                    UtcTimestamp::from_unix_millis(60_100),
                    UtcTimestamp::from_unix_millis(120_100),
                )
                .await
                .expect("backfill run");

            assert!(matches!(run, MemoryEmbeddingBackfillRun::Projected { .. }));
            let calls = embeddings.inputs.lock().expect("embedding inputs");
            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0], [MEMORY_EMBEDDING_SPACE_PROBE]);
            assert!(calls[1].len() > 1);
            assert!(calls[1].iter().all(|chunk| chunk.len() <= 4_096));
            assert_eq!(calls[1].join(" "), text);
            let replacements = vectors.replacements.lock().expect("vector replacements");
            assert_eq!(replacements.len(), 1);
            assert_eq!(replacements[0].len(), calls[1].len());
            assert!(
                replacements[0]
                    .iter()
                    .enumerate()
                    .all(|(index, vector)| usize::from(vector.chunk_ordinal) == index)
            );
            let state = store.state.lock().expect("backfill state");
            assert!(state.completed);
            assert!(!state.failed);
            assert!(!state.rejected);
        });
}

#[test]
fn provider_input_rejection_is_terminal_and_is_not_claimed_again() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime")
        .block_on(async {
            let (backfiller, store, embeddings, vectors) =
                backfill_fixture("durable memory fact".to_owned(), true);

            let first = backfiller
                .run_once(
                    "worker-1".to_owned(),
                    UtcTimestamp::from_unix_millis(100),
                    UtcTimestamp::from_unix_millis(60_100),
                    UtcTimestamp::from_unix_millis(120_100),
                )
                .await
                .expect("rejected backfill run");
            assert!(matches!(
                first,
                MemoryEmbeddingBackfillRun::Rejected {
                    reason: MemoryEmbeddingBackfillDegradation::EmbeddingInputRejected,
                    ..
                }
            ));

            let second = backfiller
                .run_once(
                    "worker-1".to_owned(),
                    UtcTimestamp::from_unix_millis(400),
                    UtcTimestamp::from_unix_millis(60_400),
                    UtcTimestamp::from_unix_millis(120_400),
                )
                .await
                .expect("settled backfill run");
            assert_eq!(second, MemoryEmbeddingBackfillRun::Idle);
            assert_eq!(embeddings.inputs.lock().expect("embedding inputs").len(), 2);
            assert!(
                vectors
                    .replacements
                    .lock()
                    .expect("vector replacements")
                    .is_empty()
            );
            let state = store.state.lock().expect("backfill state");
            assert!(!state.completed);
            assert!(!state.failed);
            assert!(state.rejected);
        });
}

#[tokio::test]
async fn nearly_expired_backfill_lease_defers_without_dispatching_embeddings() {
    let (backfiller, _store, embeddings, _vectors) = backfill_fixture("memory".to_owned(), false);
    let result = backfiller
        .run_once(
            "worker".to_owned(),
            UtcTimestamp::from_unix_millis(100),
            UtcTimestamp::from_unix_millis(200),
            UtcTimestamp::from_unix_millis(300),
        )
        .await
        .expect("expired lease is deferred");
    assert!(matches!(
        result,
        MemoryEmbeddingBackfillRun::Deferred {
            memory_version_id: None,
            reason: MemoryEmbeddingBackfillDegradation::EmbeddingTimeout,
        }
    ));
    assert!(
        embeddings
            .inputs
            .lock()
            .expect("recorded embedding inputs")
            .is_empty()
    );
}
