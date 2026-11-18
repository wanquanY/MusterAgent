use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::sync::Arc;

#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{
    CandidateStatus, ConfidenceBps, EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE, EvolutionApproval,
    EvolutionContractError, EvolutionEvaluation, EvolutionScope, ExperienceCandidate,
    ExperienceCandidateOrigin, ExperienceKind, evolution_approval_prompt_bytes,
};
use musteragent_identity::{
    AgentId, AgentVersionId, CapabilityProviderId, ContentId, DottedKey, DreamJobId,
    EvolutionEvaluationId, EvolutionPromotionId, EvolutionProposalId, IdempotencyKey,
    InteractionId, InvocationId, LedgerRecordId, MemoryCollectionId, MemoryId, MemoryVersionId,
    PrincipalId, StudioRevisionDraftId, TenantId, UtcTimestamp, VersionNumber,
};
use musteragent_memory::{
    MemoryConfidenceBps, MemoryKind, MemoryLifecycle, MemoryRecord, MemoryScope, MemorySource,
    MemoryVersion,
};
use musteragent_policy::DataClassification;
use musteragent_postgres::{PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore};
use musteragent_runtime_memory::{
    AgentEvolutionInspectionQuery, AgentEvolutionInspectionStore, AgentMemoryLifecycleDisposition,
    AgentMemoryLifecycleStore, EvolutionStore, ExperienceSearch, ExplicitRememberAuthorization,
    ManagementEvolutionPublicationDisposition, ManagementEvolutionPublicationStore,
    ManagementEvolutionPublishedTarget, ManagementEvolutionTarget, MemoryApplicationError,
    MemoryIndexSearch, MemoryPersistenceDisposition, MemorySearch, MemorySearchDocument,
    MemorySearchIndex, MemorySearchIndexBackfillStore, MemoryStore, PromoteExperience,
    PublishManagementEvolution, PublishStudioRevisionDraft, RecordEvolutionApproval,
    RecordEvolutionEvaluation, RememberUserMemory, RevokeAgentMemory, RevokeUserMemory,
    StudioRevisionArtifactKind, StudioRevisionDraft,
};
use musteragent_testkit::IsolatedPostgres;

#[tokio::test]
async fn agent_memory_revocation_is_durable_and_exactly_idempotent_in_postgres() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let store = PostgresMemoryEvolutionStore::new(pool.clone());
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let agent_id =
        musteragent_conformance::fixtures::owned_agent::owned_agent(&pool, tenant_id, principal_id)
            .await;
    let memory_id = MemoryId::generate();
    let memory_version_id = MemoryVersionId::generate();
    let content = content_ref(b"{\"claim\":\"Prefer bounded queues\"}");
    register_content(&pool, tenant_id, &content).await;
    let mut transaction = pool.begin().await.expect("begin Memory fixture");
    sqlx::query(
        "INSERT INTO memory.memories (
             memory_id,tenant_id,scope_kind,scope_id,memory_key,lifecycle,
             created_at_ms,updated_at_ms,kind,current_version_id,revoked_at_ms
         ) VALUES ($1,$2,4,$3,'learned:bounded-queues',1,100,100,2,$4,NULL)",
    )
    .bind(memory_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(agent_id.into_uuid())
    .bind(memory_version_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .expect("seed Agent Memory head");
    sqlx::query(
        "INSERT INTO memory.memory_versions (
             memory_version_id,memory_id,version,content_id,content_hash,sensitivity,
             confidence,provenance,valid_from_ms,valid_to_ms,source,confidence_bps
         ) VALUES ($1,$2,1,$3,$4,2,0.9,$5,100,NULL,2,9000)",
    )
    .bind(memory_version_id.into_uuid())
    .bind(memory_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .bind(content.sha256.into_bytes().as_slice())
    .bind(b"normalized-provenance:v1".as_slice())
    .execute(&mut *transaction)
    .await
    .expect("seed Agent Memory version");
    transaction.commit().await.expect("commit Memory fixture");

    let readable = musteragent_runtime_memory::AgentGrowthContentQuery {
        actor: musteragent_identity::Actor {
            tenant_id,
            principal_id,
            principal_kind: musteragent_identity::PrincipalKind::User,
        },
        agent_id,
        reference: content.clone(),
        observed_at: UtcTimestamp::from_unix_millis(150),
    };
    assert!(
        store
            .authorize_growth_content(readable.clone())
            .await
            .expect(
                "agent memory revocation is durable and exactly idempotent in postgres must succeed"
            )
    );
    let mut unrelated = readable.clone();
    unrelated.agent_id = AgentId::generate();
    assert!(!store.authorize_growth_content(unrelated).await.expect(
        "agent memory revocation is durable and exactly idempotent in postgres must succeed"
    ));
    let mut tampered = readable.clone();
    tampered.reference.byte_length += 1;
    assert!(!store.authorize_growth_content(tampered).await.expect(
        "agent memory revocation is durable and exactly idempotent in postgres must succeed"
    ));
    let mut outsider = readable.clone();
    outsider.actor.tenant_id = TenantId::generate();
    assert!(!store.authorize_growth_content(outsider).await.expect(
        "agent memory revocation is durable and exactly idempotent in postgres must succeed"
    ));

    let command = RevokeAgentMemory {
        tenant_id,
        actor_principal_id: principal_id,
        agent_id,
        memory_id,
        expected_memory_version_id: memory_version_id,
        reason_code: DottedKey::new("user.requested").expect("reason"),
        idempotency_key: IdempotencyKey::new("learning-revoke:postgres").expect("idempotency"),
        revoked_at: UtcTimestamp::from_unix_millis(200),
    };
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let concurrent_revoke = |store: PostgresMemoryEvolutionStore| {
        let command = command.clone();
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            store
                .revoke_agent_memory(command)
                .await
                .expect("concurrent Agent Memory revocation")
        })
    };
    let first = concurrent_revoke(store.clone());
    let second = concurrent_revoke(store.clone());
    start.wait().await;
    let (first, second) = tokio::join!(first, second);
    let mutations = [
        first.expect("first revocation task"),
        second.expect("second revocation task"),
    ];
    assert_eq!(
        mutations
            .iter()
            .filter(|mutation| { mutation.disposition == AgentMemoryLifecycleDisposition::Applied })
            .count(),
        1
    );
    assert_eq!(
        mutations
            .iter()
            .filter(|mutation| {
                mutation.disposition == AgentMemoryLifecycleDisposition::Duplicate
            })
            .count(),
        1
    );
    assert!(
        mutations
            .iter()
            .all(|mutation| mutation.receipt.matches(&command))
    );
    let mut replay = command.clone();
    replay.revoked_at = UtcTimestamp::from_unix_millis(999);
    let duplicate = store
        .revoke_agent_memory(replay)
        .await
        .expect("replay revocation");
    assert_eq!(
        duplicate.disposition,
        AgentMemoryLifecycleDisposition::Duplicate
    );
    assert!(duplicate.receipt.matches(&command));
    let persisted: (i16, Option<i64>, i64) = sqlx::query_as(
        "SELECT lifecycle,revoked_at_ms,
                (SELECT count(*) FROM memory.agent_memory_revocations
                 WHERE tenant_id=$1 AND memory_id=$2)
         FROM memory.memories WHERE tenant_id=$1 AND memory_id=$2",
    )
    .bind(tenant_id.into_uuid())
    .bind(memory_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("load lifecycle facts");
    assert_eq!(persisted, (3, Some(200), 1));
    assert!(
        !store.authorize_growth_content(readable).await.expect(
            "agent memory revocation is durable and exactly idempotent in postgres must succeed"
        ),
        "revoked bytes cannot be resurrected through Growth"
    );
}

#[tokio::test]
async fn unpublished_user_memory_is_durable_but_never_production_searchable() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let store = PostgresMemoryEvolutionStore::new(pool.clone());
    assert_memory_store(&store);

    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let content_bytes = b"{\"fact\":\"prefers Rust\"}";
    let content = content_ref(content_bytes);
    register_content(&pool, tenant_id, &content).await;
    let (record, version) = user_memory(tenant_id, principal_id, content.clone(), None);
    let command = RememberUserMemory {
        expected_current_version_id: None,
        actor_principal_id: principal_id,
        authorization: ExplicitRememberAuthorization {
            approval_interaction_id: InteractionId::generate(),
            invocation_id: InvocationId::generate(),
            request_ref: content.clone(),
            origin: musteragent_runtime_memory::UserAuthoredRunOrigin {
                record_id: LedgerRecordId::generate(),
                content_ref: content,
            },
        },
        record: record.clone(),
        version: version.clone(),
        search_document: MemorySearchDocument::from_content(&record, &version, content_bytes)
            .expect("Memory search document"),
    };
    assert_eq!(
        store
            .remember_user(command.clone())
            .await
            .expect("remember user Memory"),
        MemoryPersistenceDisposition::Applied
    );
    assert_eq!(
        store
            .remember_user(command.clone())
            .await
            .expect("duplicate user Memory"),
        MemoryPersistenceDisposition::Duplicate
    );

    let own = store
        .search(memory_search(tenant_id, principal_id, 500))
        .await
        .expect("search own Memory");
    assert!(
        own.is_empty(),
        "a UserAuthored version without an explicit publication receipt must remain hidden"
    );
    assert!(
        store
            .load_published_head(
                memory_search(tenant_id, principal_id, 500),
                record.memory_id
            )
            .await
            .expect("load production-visible Memory by exact identity")
            .is_none(),
        "memory.get exact reads must not expose an unpublished UserAuthored version"
    );
    let staged = store
        .find_user_by_key(tenant_id, principal_id, record.memory_key.clone())
        .await
        .expect("find staged Memory")
        .expect("staged Memory remains durable");
    assert_eq!(staged.record, record);
    assert_eq!(staged.version, version);

    let other_user = store
        .search(memory_search(tenant_id, PrincipalId::generate(), 500))
        .await
        .expect("search another user's scope");
    assert!(other_user.is_empty());
    let other_tenant = store
        .search(memory_search(TenantId::generate(), principal_id, 500))
        .await
        .expect("search another tenant");
    assert!(other_tenant.is_empty());

    let mut update = command.clone();
    update.expected_current_version_id = Some(version.memory_version_id);
    update.version.memory_version_id = MemoryVersionId::generate();
    update.version.version = VersionNumber::new(2)
        .expect("unpublished user memory is durable but never production searchable must succeed");
    update.version.created_at = UtcTimestamp::from_unix_millis(600);
    update.record.current_version_id = update.version.memory_version_id;
    update.record.updated_at = update.version.created_at;
    update.search_document =
        MemorySearchDocument::from_content(&update.record, &update.version, content_bytes).expect(
            "unpublished user memory is durable but never production searchable must succeed",
        );
    let mut wrong_head = update.clone();
    wrong_head.expected_current_version_id = Some(MemoryVersionId::generate());
    assert!(matches!(
        store.remember_user(wrong_head).await,
        Err(PostgresMemoryEvolutionError::MemoryVersionConflict)
    ));
    // Expire the stored head after preparation in this isolated database.
    sqlx::query(
        "UPDATE memory.memory_versions SET valid_to_ms=valid_from_ms+1 WHERE memory_version_id=$1",
    )
    .bind(version.memory_version_id.into_uuid())
    .execute(&pool)
    .await
    .expect("unpublished user memory is durable but never production searchable must succeed");
    assert!(matches!(
        store.remember_user(update.clone()).await,
        Err(PostgresMemoryEvolutionError::MemoryVersionConflict)
    ));
    sqlx::query("UPDATE memory.memory_versions SET valid_to_ms=NULL WHERE memory_version_id=$1")
        .bind(version.memory_version_id.into_uuid())
        .execute(&pool)
        .await
        .expect("unpublished user memory is durable but never production searchable must succeed");
    store
        .revoke_user(RevokeUserMemory {
            tenant_id,
            actor_principal_id: principal_id,
            memory_id: record.memory_id,
            revoked_at: UtcTimestamp::from_unix_millis(700),
        })
        .await
        .expect("revoke Memory");
    assert!(
        store
            .search(memory_search(tenant_id, principal_id, 800))
            .await
            .expect("search after revoke")
            .is_empty()
    );
    assert!(matches!(
        store.remember_user(update).await,
        Err(PostgresMemoryEvolutionError::MemoryVersionConflict)
    ));
    assert!(matches!(
        store.remember_user(command).await,
        Err(PostgresMemoryEvolutionError::MemoryVersionConflict)
    ));
    let head = store
        .find_user_by_key(tenant_id, principal_id, record.memory_key)
        .await
        .expect("unpublished user memory is durable but never production searchable must succeed")
        .expect("unpublished user memory is durable but never production searchable must succeed");
    assert_eq!(head.record.lifecycle, MemoryLifecycle::Revoked);
    assert_eq!(head.version, version);
}

#[tokio::test]
async fn verified_experience_promotes_atomically_to_scoped_memory_with_provenance() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let store = PostgresMemoryEvolutionStore::new(pool.clone());
    assert_evolution_store(&store);

    let (fixture, _attempt) = running_loop::seed_running_loop(&pool).await;
    let tenant_id = fixture.tenant_id;
    let principal_id = fixture.principal_id;
    let agent_id = AgentId::generate();
    let source_run_id = fixture.run_id;
    let evidence_record_id = LedgerRecordId::generate();
    let claim_bytes = b"{\"claim\":\"Prefer bounded queues\"}";
    let claim = content_ref(claim_bytes);
    let report = content_ref(b"{\"passed\":true}");
    register_content(&pool, tenant_id, &claim).await;
    register_content(&pool, tenant_id, &report).await;

    let mut candidate = ExperienceCandidate {
        proposal_id: EvolutionProposalId::generate(),
        tenant_id,
        proposed_by: principal_id,
        origin: musteragent_evolution::ExperienceCandidateOrigin::RuntimeTool,
        source_draft_id: None,
        target_scope: EvolutionScope::Agent { agent_id },
        target_memory_id: None,
        supersedes_proposal_id: None,
        kind: ExperienceKind::Procedure,
        claim_ref: claim.clone(),
        source_run_ids: BTreeSet::from([source_run_id]),
        evidence_record_ids: BTreeSet::from([evidence_record_id]),
        status: CandidateStatus::Candidate,
        confidence: Some(ConfidenceBps::new(9_000).expect("confidence")),
        created_at: UtcTimestamp::from_unix_millis(100),
    };
    assert_eq!(
        store
            .propose(candidate.clone())
            .await
            .expect("propose Experience"),
        MemoryPersistenceDisposition::Applied
    );
    let dream_job_id = DreamJobId::generate();
    sqlx::query(
        "INSERT INTO evolution.dream_jobs (
             dream_job_id,tenant_id,target_agent_id,agent_version_id,memory_collection_id,
             memory_collection_version,ledger_watermark,input_manifest_content_id,
             input_manifest_media_type,input_manifest_byte_length,input_manifest_hash,
             policy_version_ref,growth_profile_ref,growth_profile_hash,
             manual_request_generation,model_route_ref,model_route_hash,model_timeout_ms,
             budget_input_tokens,budget_output_tokens,
             budget_max_proposals,idempotency_key,status,retry_count,lease_owner,
             lease_expires_at_ms,available_at_ms,last_error_code,created_at_ms,completed_at_ms
         ) VALUES ($1,$2,$3,$4,$5,1,1,$6,$7,$8::numeric,$9,
                   'musteragent.legacy.dream.policy@1','musteragent.growth-profile-default@2',$10,0,
                   'musteragent.legacy.dream.evidence-manifest@1',$11,300000,
                   4096,2048,4,$12,3,0,NULL,NULL,100,NULL,100,150)",
    )
    .bind(dream_job_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(agent_id.into_uuid())
    .bind(AgentVersionId::generate().into_uuid())
    .bind(MemoryCollectionId::generate().into_uuid())
    .bind(claim.content_id.into_uuid())
    .bind(claim.media_type.as_str())
    .bind(claim.byte_length.to_string())
    .bind(claim.sha256.into_bytes().as_slice())
    .bind(
        Sha256::digest(b"musteragent.growth-profile-default@2")
            .into_bytes()
            .as_slice(),
    )
    .bind(
        Sha256::digest(b"musteragent.legacy.dream.evidence-manifest@1")
            .into_bytes()
            .as_slice(),
    )
    .bind(
        Sha256::digest(dream_job_id.to_string())
            .into_bytes()
            .as_slice(),
    )
    .execute(&pool)
    .await
    .expect("seed completed Dream Job");
    sqlx::query(
        "INSERT INTO evolution.dream_job_candidates
         (dream_job_id,proposal_id,proposal_ordinal) VALUES ($1,$2,0)",
    )
    .bind(dream_job_id.into_uuid())
    .bind(candidate.proposal_id.into_uuid())
    .execute(&pool)
    .await
    .expect("link Dream Candidate");
    assert_eq!(
        store
            .load_next_candidate_for_evaluation()
            .await
            .expect("load pending PostgreSQL Dream Candidate"),
        Some(candidate.clone())
    );
    let evaluation = EvolutionEvaluation {
        evaluation_id: EvolutionEvaluationId::generate(),
        proposal_id: candidate.proposal_id,
        evaluated_by: principal_id,
        passed: true,
        report_ref: report,
        evaluated_at: UtcTimestamp::from_unix_millis(200),
    };
    let mut substituted_candidate = candidate.clone();
    substituted_candidate.confidence = Some(ConfidenceBps::new(9_001).expect("confidence"));
    assert!(matches!(
        store
            .record_evaluation(musteragent_runtime_memory::RecordEvolutionEvaluation {
                candidate: substituted_candidate,
                evaluation: evaluation.clone(),
            })
            .await,
        Err(PostgresMemoryEvolutionError::EvolutionConflict)
    ));
    assert_eq!(
        store
            .record_evaluation(musteragent_runtime_memory::RecordEvolutionEvaluation {
                candidate: candidate.clone(),
                evaluation: evaluation.clone(),
            })
            .await
            .expect("record passing evaluation"),
        MemoryPersistenceDisposition::Applied
    );
    assert!(
        store
            .load_next_candidate_for_evaluation()
            .await
            .expect("poll evaluated PostgreSQL Dream Candidates")
            .is_none()
    );
    candidate
        .transition(CandidateStatus::Verified)
        .expect("candidate verifies");
    let approval_prompt_bytes = evolution_approval_prompt_bytes(&candidate, &evaluation, None)
        .expect("canonical approval prompt");
    let approval_prompt =
        content_ref_with_media(&approval_prompt_bytes, EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE);
    register_content(&pool, tenant_id, &approval_prompt).await;
    assert!(
        store
            .search_published(experience_search(tenant_id, principal_id, agent_id))
            .await
            .expect("search before approval")
            .is_empty(),
        "a passing evaluation alone must never enter production retrieval"
    );
    let approval = seed_approved_interaction(
        &pool,
        fixture,
        &approval_prompt,
        candidate.proposal_id,
        evaluation.evaluation_id,
        UtcTimestamp::from_unix_millis(250),
    )
    .await;
    let approval_command = RecordEvolutionApproval {
        candidate: candidate.clone(),
        evaluation: evaluation.clone(),
        approval: approval.clone(),
    };
    assert_eq!(
        store
            .record_approval(approval_command.clone())
            .await
            .expect("record canonical approval"),
        MemoryPersistenceDisposition::Applied
    );
    assert_eq!(
        store
            .record_approval(approval_command)
            .await
            .expect("replay canonical approval"),
        MemoryPersistenceDisposition::Duplicate
    );
    assert!(
        store
            .search_published(experience_search(tenant_id, principal_id, agent_id))
            .await
            .expect("search before publication")
            .is_empty(),
        "approval authorizes publication but is not itself publication"
    );
    let memory_id = MemoryId::generate();
    let memory_version_id = MemoryVersionId::generate();
    let promotion = candidate
        .promote(
            &evaluation,
            &approval,
            EvolutionPromotionId::generate(),
            memory_version_id,
            principal_id,
            UtcTimestamp::from_unix_millis(300),
        )
        .expect("domain promotion");
    let memory_record = MemoryRecord {
        memory_id,
        tenant_id,
        scope: MemoryScope::Agent { agent_id },
        memory_key: IdempotencyKey::new("experience:bounded-queues").expect("memory key"),
        kind: MemoryKind::Procedure,
        lifecycle: MemoryLifecycle::Active,
        current_version_id: memory_version_id,
        created_at: UtcTimestamp::from_unix_millis(300),
        updated_at: UtcTimestamp::from_unix_millis(300),
        revoked_at: None,
    };
    let memory_version = MemoryVersion {
        memory_version_id,
        memory_id,
        version: VersionNumber::new(1).expect("version"),
        source: MemorySource::VerifiedExperience,
        content_ref: claim,
        confidence: MemoryConfidenceBps::new(9_000).expect("confidence"),
        data_classification: DataClassification::Internal,
        source_run_ids: BTreeSet::from([source_run_id]),
        evidence_record_ids: BTreeSet::from([evidence_record_id]),
        created_at: UtcTimestamp::from_unix_millis(300),
        valid_until: None,
    };
    let command = PromoteExperience {
        tool_approval: None,
        candidate,
        evaluation,
        approval,
        promotion,
        expected_current_version_id: None,
        search_document: MemorySearchDocument::from_content(
            &memory_record,
            &memory_version,
            claim_bytes,
        )
        .expect("published search document"),
        memory_record,
        memory_version,
    };
    assert_eq!(
        store
            .promote(command.clone())
            .await
            .expect("publish approved Memory"),
        MemoryPersistenceDisposition::Applied
    );
    assert_eq!(
        store
            .promote(command.clone())
            .await
            .expect("replay exact promotion"),
        MemoryPersistenceDisposition::Duplicate
    );
    let published = store
        .search_published(experience_search(tenant_id, principal_id, agent_id))
        .await
        .expect("search published Experience");
    assert_eq!(published.len(), 1);
    assert_eq!(
        published[0].source_run_ids,
        command.candidate.source_run_ids
    );
    assert_eq!(
        published[0].evidence_record_ids,
        command.candidate.evidence_record_ids
    );
    let indexed_search = MemoryIndexSearch {
        prepared_features: None,
        visibility: MemorySearch {
            tenant_id,
            principal_id,
            workspace_ids: BTreeSet::new(),
            agent_ids: BTreeSet::from([agent_id]),
            kinds: BTreeSet::from([MemoryKind::Procedure]),
            limit: NonZeroU16::new(10).expect("limit"),
            now: UtcTimestamp::from_unix_millis(400),
        },
        query: "bounded queue backpressure".to_owned(),
    };
    let indexed = store
        .search_indexed(indexed_search.clone())
        .await
        .expect("search task-relevant published Memory through FTS");
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].record.memory_id, memory_id);
    assert!(
        store
            .search_indexed(MemoryIndexSearch {
                query: "database migration".to_owned(),
                ..indexed_search.clone()
            })
            .await
            .expect("search unrelated task through FTS")
            .is_empty()
    );
    sqlx::query("DELETE FROM memory.search_index WHERE memory_version_id=$1")
        .bind(memory_version_id.into_uuid())
        .execute(&pool)
        .await
        .expect("drop disposable PostgreSQL search projection before rebuild");
    assert_eq!(
        store
            .enqueue_missing(UtcTimestamp::from_unix_millis(350))
            .await
            .expect("enqueue missing PostgreSQL search projection"),
        1
    );
    let backfill_claim = store
        .claim_backfill(
            "postgres-memory-index-backfill".to_owned(),
            UtcTimestamp::from_unix_millis(350),
            UtcTimestamp::from_unix_millis(450),
        )
        .await
        .expect("claim PostgreSQL search backfill")
        .expect("missing projection is claimable");
    assert_eq!(backfill_claim.source.memory_version_id, memory_version_id);
    assert_eq!(
        store
            .complete_backfill(
                backfill_claim,
                command.search_document.clone(),
                UtcTimestamp::from_unix_millis(360),
            )
            .await
            .expect("complete PostgreSQL search backfill"),
        MemoryPersistenceDisposition::Applied
    );
    let backfill_status = store
        .backfill_status()
        .await
        .expect("read PostgreSQL search backfill watermark");
    assert_eq!(backfill_status.pending, 0);
    assert_eq!(backfill_status.running, 0);
    assert_eq!(backfill_status.completed, 1);
    assert_eq!(backfill_status.dead_letter, 0);
    assert_eq!(
        store
            .search_indexed(indexed_search.clone())
            .await
            .expect("search rebuilt PostgreSQL FTS")
            .len(),
        1
    );
    let indexed_features: String =
        sqlx::query_scalar("SELECT features::text FROM memory.search_index")
            .fetch_one(&pool)
            .await
            .expect("read rebuildable PostgreSQL search projection");
    assert!(!indexed_features.contains("prefer bounded queues"));
    assert_ne!(indexed_features.as_bytes(), claim_bytes.as_slice());

    sqlx::query("UPDATE memory.memories SET lifecycle=3,revoked_at_ms=400 WHERE memory_id=$1")
        .bind(memory_id.into_uuid())
        .execute(&pool)
        .await
        .expect("leave stale FTS projection behind a revoked Memory");
    assert!(
        store
            .search_indexed(indexed_search)
            .await
            .expect("revalidate stale PostgreSQL FTS hit against Memory authority")
            .is_empty(),
        "a stale index must never resurrect a revoked Memory"
    );
    sqlx::query("UPDATE memory.memories SET lifecycle=1,revoked_at_ms=NULL WHERE memory_id=$1")
        .bind(memory_id.into_uuid())
        .execute(&pool)
        .await
        .expect("restore fixture after stale-index assertion");
    let mut forged_candidate = command.clone();
    forged_candidate.candidate.proposed_by = PrincipalId::generate();
    assert!(matches!(
        store.promote(forged_candidate).await,
        Err(PostgresMemoryEvolutionError::Application(
            MemoryApplicationError::Evolution(EvolutionContractError::ApprovalPromptMismatch)
        ))
    ));
    let mut forged_evaluation = command.clone();
    forged_evaluation.evaluation.evaluated_by = PrincipalId::generate();
    assert!(matches!(
        store.promote(forged_evaluation).await,
        Err(PostgresMemoryEvolutionError::Application(
            MemoryApplicationError::Evolution(EvolutionContractError::ApprovalPromptMismatch)
        ))
    ));
    let mut forged_approval = command.clone();
    forged_approval.approval.approved_by = PrincipalId::generate();
    assert!(matches!(
        store.promote(forged_approval).await,
        Err(PostgresMemoryEvolutionError::EvolutionConflict)
    ));
    let mut forged_memory = command.clone();
    forged_memory.memory_record.memory_key =
        IdempotencyKey::new("experience:unrelated").expect("memory key");
    assert!(matches!(
        store.promote(forged_memory).await,
        Err(PostgresMemoryEvolutionError::EvolutionConflict)
    ));

    let visible = store
        .search(MemorySearch {
            tenant_id,
            principal_id,
            workspace_ids: BTreeSet::new(),
            agent_ids: BTreeSet::from([agent_id]),
            kinds: BTreeSet::from([MemoryKind::Procedure]),
            limit: NonZeroU16::new(10).expect("limit"),
            now: UtcTimestamp::from_unix_millis(400),
        })
        .await
        .expect("search promoted Memory");
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].version.source_run_ids,
        BTreeSet::from([source_run_id])
    );
    assert_eq!(
        visible[0].version.evidence_record_ids,
        BTreeSet::from([evidence_record_id])
    );
}

#[tokio::test]
async fn approved_skill_handoff_is_atomic_idempotent_and_restart_readable_in_postgres() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let store = PostgresMemoryEvolutionStore::new(pool.clone());
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let tenant_id = fixture.tenant_id;
    let principal_id = fixture.principal_id;
    let agent_id = AgentId::generate();
    let proposal_id = EvolutionProposalId::generate();
    let claim_bytes = musteragent_runtime_memory::ExperienceClaimDocument::manual(
        "Extract a reusable bounded-queue operating procedure".to_owned(),
        serde_json::Map::new(),
        "produce a governed Skill revision input".to_owned(),
        "internal".to_owned(),
    )
    .encode()
    .expect("canonical Skill proposal claim");
    let claim = content_ref_with_media(
        &claim_bytes,
        musteragent_runtime_memory::EXPERIENCE_CLAIM_MEDIA_TYPE,
    );
    let report = content_ref_with_media(
        br#"{"passed":true,"suite":"skill-handoff"}"#,
        "application/vnd.musteragent.legacy.evolution-evaluation+json",
    );
    register_content(&pool, tenant_id, &claim).await;
    register_content(&pool, tenant_id, &report).await;
    let mut candidate = ExperienceCandidate {
        proposal_id,
        tenant_id,
        proposed_by: principal_id,
        origin: ExperienceCandidateOrigin::RuntimeTool,
        source_draft_id: None,
        target_scope: EvolutionScope::Agent { agent_id },
        target_memory_id: None,
        supersedes_proposal_id: None,
        kind: ExperienceKind::Skill,
        claim_ref: claim.clone(),
        source_run_ids: BTreeSet::from([fixture.run_id]),
        evidence_record_ids: BTreeSet::from([LedgerRecordId::generate()]),
        status: CandidateStatus::Candidate,
        confidence: Some(ConfidenceBps::new(9_200).expect("confidence")),
        created_at: UtcTimestamp::from_unix_millis(100),
    };
    assert_eq!(
        store
            .propose(candidate.clone())
            .await
            .expect("propose Skill evolution"),
        MemoryPersistenceDisposition::Applied
    );
    let evaluation = EvolutionEvaluation {
        evaluation_id: EvolutionEvaluationId::generate(),
        proposal_id,
        evaluated_by: principal_id,
        passed: true,
        report_ref: report,
        evaluated_at: UtcTimestamp::from_unix_millis(200),
    };
    assert_eq!(
        store
            .record_evaluation(RecordEvolutionEvaluation {
                candidate: candidate.clone(),
                evaluation: evaluation.clone(),
            })
            .await
            .expect("record Skill evaluation"),
        MemoryPersistenceDisposition::Applied
    );
    candidate
        .transition(CandidateStatus::Verified)
        .expect("verify Skill proposal");
    let approved_at = UtcTimestamp::from_unix_millis(300);
    let prompt_bytes = evolution_approval_prompt_bytes(&candidate, &evaluation, None)
        .expect("canonical management prompt");
    let prompt = content_ref_with_media(&prompt_bytes, EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE);
    register_content(&pool, tenant_id, &prompt).await;
    let approval = EvolutionApproval {
        approval_interaction_id: InteractionId::generate(),
        proposal_id,
        evaluation_id: evaluation.evaluation_id,
        expected_current_memory_version_id: None,
        prompt_ref: prompt,
        approved_by: principal_id,
        approved_at,
    };
    candidate
        .promote_artifact(&evaluation, &approval, approved_at)
        .expect("promote Skill proposal to governed Studio input");
    let studio_revision_draft_id = StudioRevisionDraftId::generate();
    let promotion_id = EvolutionPromotionId::generate();
    let publication = PublishStudioRevisionDraft {
        candidate,
        evaluation: evaluation.clone(),
        approval: approval.clone(),
        draft: StudioRevisionDraft {
            studio_revision_draft_id,
            tenant_id,
            target_agent_id: agent_id,
            proposal_id,
            evaluation_id: evaluation.evaluation_id,
            approval_interaction_id: approval.approval_interaction_id,
            promotion_id,
            artifact_kind: StudioRevisionArtifactKind::Skill,
            payload_ref: claim,
            created_by: principal_id,
            created_at: approved_at,
        },
    };
    publication.validate().expect("valid Studio handoff");
    let command = PublishManagementEvolution {
        tenant_id,
        actor_principal_id: principal_id,
        agent_id,
        proposal_id,
        idempotency_key: IdempotencyKey::new("postgres:skill-handoff").expect("idempotency"),
        approved_at,
        target: ManagementEvolutionTarget::StudioRevisionDraft(Box::new(publication)),
    };
    let applied = store
        .publish_management_evolution(command.clone())
        .await
        .expect("publish Studio input atomically");
    assert_eq!(
        applied.disposition,
        ManagementEvolutionPublicationDisposition::Applied
    );
    assert_eq!(
        applied.receipt.target,
        ManagementEvolutionPublishedTarget::StudioRevisionDraft {
            studio_revision_draft_id,
            promotion_id,
            artifact_kind: StudioRevisionArtifactKind::Skill,
        }
    );
    assert_eq!(
        store
            .publish_management_evolution(command)
            .await
            .expect("replay Studio input publication")
            .disposition,
        ManagementEvolutionPublicationDisposition::Duplicate
    );

    let restarted = PostgresMemoryEvolutionStore::new(pool.clone());
    let inspection = restarted
        .inspect_agent(AgentEvolutionInspectionQuery {
            tenant_id,
            principal_id,
            agent_id,
            proposal_limit: NonZeroU16::new(10).expect("limit"),
            memory_limit: NonZeroU16::new(10).expect("limit"),
            graph_limit: NonZeroU16::new(10).expect("limit"),
            dream_limit: NonZeroU16::new(10).expect("limit"),
            dream_since: None,
            dream_cursor: None,
            growth_since: None,
            observed_at: UtcTimestamp::from_unix_millis(400),
        })
        .await
        .expect("inspect restarted Studio publication");
    inspection
        .validate_for(&AgentEvolutionInspectionQuery {
            tenant_id,
            principal_id,
            agent_id,
            proposal_limit: NonZeroU16::new(10).expect("limit"),
            memory_limit: NonZeroU16::new(10).expect("limit"),
            graph_limit: NonZeroU16::new(10).expect("limit"),
            dream_limit: NonZeroU16::new(10).expect("limit"),
            dream_since: None,
            dream_cursor: None,
            growth_since: None,
            observed_at: UtcTimestamp::from_unix_millis(400),
        })
        .expect("validate exact governed target chain");
    for reference in [
        inspection.proposals[0].candidate.claim_ref.clone(),
        inspection.learning_graph[0].payload_ref.clone(),
    ] {
        assert!(
            restarted
                .authorize_growth_content(musteragent_runtime_memory::AgentGrowthContentQuery {
                    actor: musteragent_identity::Actor {
                        tenant_id,
                        principal_id,
                        principal_kind: musteragent_identity::PrincipalKind::User
                    },
                    agent_id,
                    reference,
                    observed_at: UtcTimestamp::from_unix_millis(400)
                })
                .await
                .expect("approved skill handoff is atomic idempotent and restart readable in postgres must succeed")
        );
    }
    assert_eq!(inspection.proposals.len(), 1);
    assert_eq!(
        inspection.proposals[0].studio_revision_draft_id,
        Some(studio_revision_draft_id)
    );
    assert_eq!(
        inspection.proposals[0].studio_artifact_kind,
        Some(StudioRevisionArtifactKind::Skill)
    );
    assert_eq!(inspection.learning_graph.len(), 1);
    assert_eq!(
        inspection.learning_graph[0].studio_revision_draft_id,
        Some(studio_revision_draft_id)
    );
    let persisted: (i16, i64, i64, i64) = sqlx::query_as(
        "SELECT proposal.status,
                (SELECT count(*) FROM approval.evolution_management_interactions
                 WHERE proposal_id=$1),
                (SELECT count(*) FROM evolution.artifact_promotions WHERE proposal_id=$1),
                (SELECT count(*) FROM evolution.studio_revision_drafts WHERE proposal_id=$1)
         FROM evolution.proposals AS proposal WHERE proposal.proposal_id=$1",
    )
    .bind(proposal_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("load atomic Studio handoff facts");
    assert_eq!(persisted, (5, 1, 1, 1));
}

fn assert_memory_store<T: MemoryStore>(_store: &T) {}
fn assert_evolution_store<T: EvolutionStore>(_store: &T) {}

fn experience_search(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    agent_id: AgentId,
) -> ExperienceSearch {
    ExperienceSearch {
        tenant_id,
        principal_id,
        agent_ids: BTreeSet::from([agent_id]),
        kinds: BTreeSet::from([ExperienceKind::Procedure]),
        limit: NonZeroU16::new(10).expect("limit"),
    }
}

fn memory_search(tenant_id: TenantId, principal_id: PrincipalId, now_ms: i64) -> MemorySearch {
    MemorySearch {
        tenant_id,
        principal_id,
        workspace_ids: BTreeSet::new(),
        agent_ids: BTreeSet::new(),
        kinds: BTreeSet::new(),
        limit: NonZeroU16::new(10).expect("limit"),
        now: UtcTimestamp::from_unix_millis(now_ms),
    }
}

fn user_memory(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    content_ref: ContentRef,
    valid_until: Option<UtcTimestamp>,
) -> (MemoryRecord, MemoryVersion) {
    let memory_id = MemoryId::generate();
    let memory_version_id = MemoryVersionId::generate();
    (
        MemoryRecord {
            memory_id,
            tenant_id,
            scope: MemoryScope::User { principal_id },
            memory_key: IdempotencyKey::new("preference:language").expect("memory key"),
            kind: MemoryKind::Preference,
            lifecycle: MemoryLifecycle::Active,
            current_version_id: memory_version_id,
            created_at: UtcTimestamp::from_unix_millis(100),
            updated_at: UtcTimestamp::from_unix_millis(100),
            revoked_at: None,
        },
        MemoryVersion {
            memory_version_id,
            memory_id,
            version: VersionNumber::new(1).expect("version"),
            source: MemorySource::UserAuthored,
            content_ref,
            confidence: MemoryConfidenceBps::new(10_000).expect("confidence"),
            data_classification: DataClassification::Confidential,
            source_run_ids: BTreeSet::new(),
            evidence_record_ids: BTreeSet::new(),
            created_at: UtcTimestamp::from_unix_millis(100),
            valid_until,
        },
    )
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    content_ref_with_media(bytes, "application/json")
}

fn content_ref_with_media(bytes: &[u8], media_type: &str) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media_type).expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("bounded fixture"),
        sha256: Sha256::digest(bytes),
    }
}

async fn register_content(pool: &sqlx::PgPool, tenant_id: TenantId, content: &ContentRef) {
    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id, tenant_id, media_type, byte_length,
             content_hash, storage_ref, created_at_ms
         ) VALUES ($1, $2, $3, $4::numeric, $5, $6, 1)",
    )
    .bind(content.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().as_slice())
    .bind(format!("test-content://{}", content.content_id))
    .execute(pool)
    .await
    .expect("register content");
}

async fn seed_approved_interaction(
    pool: &sqlx::PgPool,
    fixture: running_loop::Fixture,
    prompt: &ContentRef,
    proposal_id: EvolutionProposalId,
    evaluation_id: EvolutionEvaluationId,
    approved_at: UtcTimestamp,
) -> EvolutionApproval {
    let invocation_id = InvocationId::generate();
    sqlx::query(
        "INSERT INTO tool.tool_invocations (
             invocation_id, tenant_id, conversation_id, run_id,
             requested_by_attempt_id, execution_attempt_id, tool_call_id,
             tool_ref, capability_provider_id, status, effect, approval_policy,
             retry_contract, idempotency_key, request_content_id, request_hash,
             deadline_ms, started_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, $5, $6,
             'experience.publish@1', $7, 5, 2, 3,
             2, $8, $9, $10, $11, $12
         )",
    )
    .bind(invocation_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(format!("publish-{}", proposal_id))
    .bind(CapabilityProviderId::generate().into_uuid())
    .bind(format!("experience-publish:{proposal_id}"))
    .bind(prompt.content_id.into_uuid())
    .bind(prompt.sha256.into_bytes().as_slice())
    .bind(approved_at.unix_millis().saturating_add(60_000))
    .bind(approved_at.unix_millis().saturating_sub(1))
    .execute(pool)
    .await
    .expect("seed publication invocation");

    let interaction_id = InteractionId::generate();
    sqlx::query(
        "INSERT INTO approval.interactions (
             interaction_id, tenant_id, conversation_id, activity_id, run_id,
             requested_by_invocation_id, kind, status, prompt_content_id,
             response_schema_key, response_schema_hash, expires_at_ms,
             requested_at_ms, responded_by, responded_at_ms, closed_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, $6, 1, 2, $7,
             'experience.publish.approval@1', $8, NULL, $9, $10, $11, NULL
         )",
    )
    .bind(interaction_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(prompt.content_id.into_uuid())
    .bind(
        Sha256::digest(b"experience.publish.approval@1")
            .into_bytes()
            .as_slice(),
    )
    .bind(approved_at.unix_millis().saturating_sub(1))
    .bind(fixture.principal_id.into_uuid())
    .bind(approved_at.unix_millis())
    .execute(pool)
    .await
    .expect("seed responded publication interaction");
    sqlx::query(
        "INSERT INTO approval.interaction_responses (
             interaction_id, response_kind, approval_decision,
             response_hash, responded_by, responded_at_ms
         ) VALUES ($1, 1, 1, $2, $3, $4)",
    )
    .bind(interaction_id.into_uuid())
    .bind(Sha256::digest(b"approve").into_bytes().as_slice())
    .bind(fixture.principal_id.into_uuid())
    .bind(approved_at.unix_millis())
    .execute(pool)
    .await
    .expect("seed approve response");

    EvolutionApproval {
        approval_interaction_id: interaction_id,
        proposal_id,
        evaluation_id,
        expected_current_memory_version_id: None,
        prompt_ref: prompt.clone(),
        approved_by: fixture.principal_id,
        approved_at,
    }
}
