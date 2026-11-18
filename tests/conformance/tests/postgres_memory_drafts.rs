//! Shared draft owner against a disposable PostgreSQL instance and an in-memory
//! blob adapter. No existing database, Backend, model or account is contacted.
use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::MediaType;
use musteragent_evolution::EvolutionEvaluation;
use musteragent_identity::*;
use musteragent_postgres::{PostgresContentStore, PostgresMemoryEvolutionStore};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_memory::*;
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use std::sync::Arc;

#[path = "postgres_memory_drafts/evaluation.rs"]
mod evaluation;
#[path = "postgres_memory_drafts/inspection.rs"]
mod inspection;

struct Fixture {
    postgres: IsolatedPostgres,
    pool: sqlx::PgPool,
    blobs: Arc<MemoryBlobs>,
    content: Arc<PostgresContentStore>,
    save: SaveAgentMemoryDraftContent,
}

impl Fixture {
    async fn new() -> Self {
        Self::new_for_agent(AgentId::generate()).await
    }

    async fn new_for_agent(agent_id: AgentId) -> Self {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("isolated Postgres");
        let pool = postgres.pool().await.expect("isolated pool");
        musteragent_conformance::migrate(&pool)
            .await
            .expect("original migrations");
        let blobs = Arc::new(MemoryBlobs::in_memory());
        let content = Self::content(&pool, blobs.clone());
        let save = SaveAgentMemoryDraftContent {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            agent_id,
            memory_id: MemoryId::generate(),
            expected_baseline_memory_version_id: MemoryVersionId::generate(),
            edited_claim: "Preserve the user's naming convention".to_owned(),
            supersedes_draft_id: None,
            idempotency_key: IdempotencyKey::new("draft:save").expect("key"),
            saved_at: UtcTimestamp::from_unix_millis(200),
        };
        let reference = content
            .put(PutContent {
                tenant_id: save.tenant_id,
                media_type: MediaType::new("text/plain").expect("media"),
                bytes: b"Original governed memory".to_vec(),
            })
            .await
            .expect("real content owner");
        let mut tx = pool.begin().await.expect("fixture transaction");
        sqlx::query("INSERT INTO memory.memories (memory_id,tenant_id,scope_kind,scope_id,memory_key,lifecycle,created_at_ms,updated_at_ms,kind,current_version_id,revoked_at_ms) VALUES ($1,$2,4,$3,'draft:memory',1,100,100,2,$4,NULL)")
            .bind(save.memory_id.into_uuid()).bind(save.tenant_id.into_uuid())
            .bind(save.agent_id.into_uuid()).bind(save.expected_baseline_memory_version_id.into_uuid())
            .execute(&mut *tx).await.expect("fixture head");
        sqlx::query("INSERT INTO memory.memory_versions (memory_version_id,memory_id,version,content_id,content_hash,sensitivity,confidence,provenance,valid_from_ms,valid_to_ms,source,confidence_bps) VALUES ($1,$2,1,$3,$4,2,1,$5,100,NULL,3,10000)")
            .bind(save.expected_baseline_memory_version_id.into_uuid()).bind(save.memory_id.into_uuid())
            .bind(reference.content_id.into_uuid()).bind(reference.sha256.into_bytes().to_vec())
            .bind(b"normalized-provenance:v1".as_slice()).execute(&mut *tx).await.expect("fixture version");
        tx.commit().await.expect("fixture commit");
        let store = PostgresMemoryEvolutionStore::new(pool.clone());
        store
            .load_active_head(save.tenant_id, save.memory_id)
            .await
            .expect("fixture original head query")
            .expect("fixture head exists");
        store
            .load_draft(save.tenant_id, save.principal_id, MemoryDraftId::generate())
            .await
            .expect("fixture original draft query");
        Self {
            postgres,
            pool,
            blobs,
            content,
            save,
        }
    }
    fn store(&self) -> Arc<PostgresMemoryEvolutionStore> {
        Arc::new(PostgresMemoryEvolutionStore::new(self.pool.clone()))
    }
    fn content(pool: &sqlx::PgPool, blobs: Arc<MemoryBlobs>) -> Arc<PostgresContentStore> {
        let key = DottedKey::new("test.memory.draft").expect("key id");
        Arc::new(PostgresContentStore::new(
            pool.clone(),
            blobs,
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [47; 32])]).expect("fixture keys")),
        ))
    }
    fn app(&self) -> AgentMemoryDraftApplicationService<PostgresMemoryEvolutionStore> {
        AgentMemoryDraftApplicationService::new(self.store(), self.content.clone())
    }
    fn submit(&self, draft_id: MemoryDraftId) -> SubmitAgentMemoryDrafts {
        SubmitAgentMemoryDrafts {
            tenant_id: self.save.tenant_id,
            principal_id: self.save.principal_id,
            agent_id: self.save.agent_id,
            draft_ids: vec![draft_id],
            idempotency_key: IdempotencyKey::new("draft:submit").expect("key"),
            submitted_at: UtcTimestamp::from_unix_millis(300),
        }
    }
    fn confirm(&self, batch_id: MemoryDraftBatchId) -> ConfirmAgentMemoryDrafts {
        ConfirmAgentMemoryDrafts {
            tenant_id: self.save.tenant_id,
            principal_id: self.save.principal_id,
            agent_id: self.save.agent_id,
            batch_id,
            idempotency_key: IdempotencyKey::new("draft:confirm").expect("key"),
            confirmed_at: UtcTimestamp::from_unix_millis(500),
        }
    }
    async fn evaluate(&self, batch_id: MemoryDraftBatchId, passed: bool) {
        let batch = self
            .app()
            .review(self.save.tenant_id, self.save.principal_id, batch_id)
            .await
            .expect("review");
        for item in batch.items {
            let report_ref = self
                .content
                .put(PutContent {
                    tenant_id: self.save.tenant_id,
                    media_type: MediaType::new(
                        "application/vnd.musteragent.legacy.evolution-evaluation+json",
                    )
                    .expect("media"),
                    bytes: format!("{{\"passed\":{passed}}}").into_bytes(),
                })
                .await
                .expect("isolated evaluator fixture report");
            self.store()
                .record_evaluation(RecordEvolutionEvaluation {
                    evaluation: EvolutionEvaluation {
                        evaluation_id: EvolutionEvaluationId::generate(),
                        proposal_id: item.proposal.candidate.proposal_id,
                        evaluated_by: self.save.principal_id,
                        passed,
                        report_ref,
                        evaluated_at: UtcTimestamp::from_unix_millis(400),
                    },
                    candidate: item.proposal.candidate,
                })
                .await
                .expect("original evaluator persistence");
        }
    }
}

#[tokio::test]
async fn store_save_retains_its_numeric_content_length_and_original_timestamp() {
    let f = Fixture::new().await;
    let content_ref = f
        .content
        .put(PutContent {
            tenant_id: f.save.tenant_id,
            media_type: MediaType::new(EXPERIENCE_CLAIM_MEDIA_TYPE).expect("claim media"),
            bytes: ExperienceClaimDocument::manual(
                f.save.edited_claim.clone(),
                serde_json::Map::new(),
                "Store fixture".to_owned(),
                "internal".to_owned(),
            )
            .encode()
            .expect("claim"),
        })
        .await
        .expect("draft content");
    let draft = AgentMemoryDraft {
        draft_id: MemoryDraftId::generate(),
        tenant_id: f.save.tenant_id,
        owner_principal_id: f.save.principal_id,
        agent_id: f.save.agent_id,
        memory_id: f.save.memory_id,
        baseline_memory_version_id: f.save.expected_baseline_memory_version_id,
        content_ref,
        status: AgentMemoryDraftStatus::Draft,
        proposal_id: None,
        conflicting_memory_version_id: None,
        supersedes_draft_id: None,
        idempotency_key: f.save.idempotency_key.clone(),
        created_at: f.save.saved_at,
        updated_at: f.save.saved_at,
    };
    let original = f
        .store()
        .save_draft(SaveAgentMemoryDraft {
            draft: draft.clone(),
        })
        .await
        .expect("original Postgres draft persistence");
    let duplicate = f
        .store()
        .save_draft(SaveAgentMemoryDraft {
            draft: AgentMemoryDraft {
                created_at: UtcTimestamp::from_unix_millis(999),
                updated_at: UtcTimestamp::from_unix_millis(999),
                ..draft
            },
        })
        .await
        .expect("replayed store save");
    assert_eq!(
        duplicate.disposition,
        AgentMemoryDraftDisposition::Duplicate
    );
    assert_eq!(duplicate.draft, original.draft);
    f.pool.close().await;
}

#[tokio::test]
async fn replay_keeps_original_draft_and_publication_after_head_advances() {
    let f = Fixture::new().await;
    let saved = f.app().save(f.save.clone()).await.expect("save");
    let submit = f.submit(saved.draft.draft_id);
    let submitted = f.app().submit(submit.clone()).await.expect("submit");
    let confirmation = f.confirm(submitted.batch_id);
    assert!(matches!(
        f.app().confirm(confirmation.clone()).await,
        Err(AgentMemoryDraftApplicationError::EvaluationPending)
    ));
    f.evaluate(submitted.batch_id, true).await;
    let published = f
        .app()
        .confirm(confirmation.clone())
        .await
        .expect("confirm");
    let replay = f
        .app()
        .confirm(ConfirmAgentMemoryDrafts {
            confirmed_at: UtcTimestamp::from_unix_millis(999),
            ..confirmation.clone()
        })
        .await
        .expect("confirmation with a new server clock");
    assert_eq!(replay.disposition, AgentMemoryDraftDisposition::Duplicate);
    assert_eq!(replay.items, published.items);
    let recovered = f
        .app()
        .save(SaveAgentMemoryDraftContent {
            saved_at: UtcTimestamp::from_unix_millis(1000),
            ..f.save.clone()
        })
        .await
        .expect("old save after publication changed the active head");
    assert_eq!(
        recovered.disposition,
        AgentMemoryDraftDisposition::Duplicate
    );
    assert_eq!(recovered.draft.status, AgentMemoryDraftStatus::Published);
    assert_eq!(recovered.draft.created_at, saved.draft.created_at);
    let submitted_again = f
        .app()
        .submit(SubmitAgentMemoryDrafts {
            submitted_at: UtcTimestamp::from_unix_millis(1100),
            ..submit
        })
        .await
        .expect("original submission receipt");
    assert_eq!(
        submitted_again.disposition,
        AgentMemoryDraftDisposition::Duplicate
    );
    assert_eq!(submitted_again.drafts, vec![recovered.draft]);
    assert!(
        f.app()
            .confirm(ConfirmAgentMemoryDrafts {
                principal_id: PrincipalId::generate(),
                ..confirmation.clone()
            })
            .await
            .is_err(),
        "a known batch and key do not authorize another actor"
    );
    assert!(
        f.app()
            .confirm(ConfirmAgentMemoryDrafts {
                tenant_id: TenantId::generate(),
                ..confirmation.clone()
            })
            .await
            .is_err()
    );
    assert!(
        f.app()
            .confirm(ConfirmAgentMemoryDrafts {
                agent_id: AgentId::generate(),
                ..confirmation.clone()
            })
            .await
            .is_err()
    );
    assert!(
        f.app()
            .confirm(ConfirmAgentMemoryDrafts {
                idempotency_key: IdempotencyKey::new("new-confirm-key").expect("key"),
                ..confirmation
            })
            .await
            .is_err()
    );
    f.pool.close().await;
}

#[tokio::test]
async fn concurrent_save_submit_and_confirm_commit_each_intent_once() {
    let f = Fixture::new().await;
    let a = f.app();
    let b = f.app();
    let (first, second) = tokio::join!(
        a.save(f.save.clone()),
        b.save(SaveAgentMemoryDraftContent {
            saved_at: UtcTimestamp::from_unix_millis(201),
            ..f.save.clone()
        })
    );
    let saves = [first.expect("first save"), second.expect("second save")];
    assert_eq!(
        saves
            .iter()
            .filter(|s| s.disposition == AgentMemoryDraftDisposition::Applied)
            .count(),
        1
    );
    assert_eq!(saves[0].draft, saves[1].draft);
    let submit = f.submit(saves[0].draft.draft_id);
    let (first, second) = tokio::join!(
        a.submit(submit.clone()),
        b.submit(SubmitAgentMemoryDrafts {
            submitted_at: UtcTimestamp::from_unix_millis(301),
            ..submit
        })
    );
    let submissions = [
        first.expect("first submission"),
        second.expect("second submission"),
    ];
    assert_eq!(
        submissions
            .iter()
            .filter(|s| s.disposition == AgentMemoryDraftDisposition::Applied)
            .count(),
        1
    );
    assert_eq!(submissions[0].drafts, submissions[1].drafts);
    let batch_id = submissions[0].batch_id;
    f.evaluate(batch_id, true).await;
    let (first, second) = tokio::join!(
        a.confirm(f.confirm(batch_id)),
        b.confirm(ConfirmAgentMemoryDrafts {
            confirmed_at: UtcTimestamp::from_unix_millis(501),
            ..f.confirm(batch_id)
        })
    );
    let confirmations = [
        first.expect("first confirmation"),
        second.expect("second confirmation"),
    ];
    assert_eq!(
        confirmations
            .iter()
            .filter(|s| s.disposition == AgentMemoryDraftDisposition::Applied)
            .count(),
        1
    );
    assert_eq!(confirmations[0].items, confirmations[1].items);
    let counts: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM memory.agent_memory_drafts),(SELECT count(*) FROM memory.agent_memory_draft_batches),(SELECT count(*) FROM memory.agent_memory_draft_publication_batches)")
        .fetch_one(&f.pool).await.expect("durable counts");
    assert_eq!(counts, (1, 1, 1));
    f.pool.close().await;
}

#[tokio::test]
async fn changed_frozen_input_and_failed_evaluation_never_publish() {
    let f = Fixture::new().await;
    let saved = f.app().save(f.save.clone()).await.expect("save");
    for changed in [
        SaveAgentMemoryDraftContent {
            edited_claim: "Different input".to_owned(),
            ..f.save.clone()
        },
        SaveAgentMemoryDraftContent {
            agent_id: AgentId::generate(),
            ..f.save.clone()
        },
        SaveAgentMemoryDraftContent {
            memory_id: MemoryId::generate(),
            ..f.save.clone()
        },
        SaveAgentMemoryDraftContent {
            expected_baseline_memory_version_id: MemoryVersionId::generate(),
            ..f.save.clone()
        },
        SaveAgentMemoryDraftContent {
            supersedes_draft_id: Some(MemoryDraftId::generate()),
            ..f.save.clone()
        },
    ] {
        assert!(f.app().save(changed).await.is_err());
    }
    let submit = f.submit(saved.draft.draft_id);
    let submitted = f.app().submit(submit.clone()).await.expect("submit");
    assert!(
        f.app()
            .submit(SubmitAgentMemoryDrafts {
                draft_ids: vec![MemoryDraftId::generate()],
                ..submit.clone()
            })
            .await
            .is_err()
    );
    assert!(
        f.app()
            .submit(SubmitAgentMemoryDrafts {
                agent_id: AgentId::generate(),
                ..submit.clone()
            })
            .await
            .is_err()
    );
    let foreign = GetAgentMemoryDraftSubmission {
        batch_id: submitted.batch_id,
        tenant_id: submit.tenant_id,
        owner_principal_id: PrincipalId::generate(),
        agent_id: submit.agent_id,
        draft_ids: submit.draft_ids,
        idempotency_key: submit.idempotency_key,
    };
    assert!(
        f.store()
            .load_submission(foreign)
            .await
            .expect("scoped lookup")
            .is_none()
    );
    f.evaluate(submitted.batch_id, false).await;
    assert!(matches!(
        f.app().confirm(f.confirm(submitted.batch_id)).await,
        Err(AgentMemoryDraftApplicationError::NotPublishable)
    ));
    let head: uuid::Uuid =
        sqlx::query_scalar("SELECT current_version_id FROM memory.memories WHERE memory_id=$1")
            .bind(f.save.memory_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .expect("unchanged head");
    assert_eq!(head, f.save.expected_baseline_memory_version_id.into_uuid());
    f.pool.close().await;
}
