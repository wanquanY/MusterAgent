use std::sync::Arc;

use musteragent_conformance::MemoryBlobs;
use musteragent_conformance::fixtures::agent_memory_erasure::{
    assert_conflicting_intents, assert_replay,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    Actor, AuditRecordId, ContentId, CorrelationId, DottedKey, ErasureJobId, ErasureReceiptId,
    IdempotencyKey, MemoryId, MemoryVersionId, PrincipalId, PrincipalKind, TenantId, UtcTimestamp,
};
use musteragent_postgres::{
    PostgresContentStore, PostgresMemoryEvolutionStore, PostgresPrivacyErasureError,
    PostgresPrivacyErasureStore, PostgresSecurityAuditError, PostgresSecurityAuditPipeline,
    PostgresSecurityAuditTestStore,
};
use musteragent_runtime_audit::{
    ApprovedRetentionPolicies, ProposedSecurityAuditRecord, SecurityAuditAction,
    SecurityAuditOutcome, SecurityAuditQuery, SecurityAuditSourceRef,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};
use musteragent_runtime_memory::MemorySearchIndexBackfillStore;
use musteragent_runtime_privacy::{
    AgentMemoryErasureAdmissionStore, AgentMemoryErasureExecutor, ErasureAppendDisposition,
    ErasureDataClass, ErasureJobQuery, ErasureJobStatus, ErasureJobStore, ErasurePlanSnapshot,
    ErasurePlanVersion, ErasureQueryLimit, ErasureReceipt, ErasureReceiptOutcome, ErasureRequest,
    ErasureRetentionException, ErasureTransition, ErasureTransitionSequence,
    RequestAgentMemoryErasure,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;

const PRIVACY_POLICY: &str = "privacy.standard@1";
const AUDIT_POLICY: &str = "security.audit.standard@1";
const SECRET_CORPUS: &str = "sk-live-privacy-erasure-must-never-persist";

#[tokio::test]
async fn agent_memory_permanent_forget_is_atomically_lineage_bound_in_postgres() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let agent_id =
        musteragent_conformance::fixtures::owned_agent::owned_agent(&pool, tenant_id, principal_id)
            .await;
    let memory_id = MemoryId::generate();
    let memory_version_id = MemoryVersionId::generate();
    let content_bytes = b"erase this exact PostgreSQL Agent Memory";
    let content_store = PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        privacy_reference_keys(),
    );
    let content = content_store
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/plain").expect("media type"),
            bytes: content_bytes.to_vec(),
        })
        .await
        .expect("persist Agent Memory content");
    let mut transaction = pool.begin().await.expect("begin Memory fixture");
    sqlx::query(
        "INSERT INTO memory.memories (
             memory_id,tenant_id,scope_kind,scope_id,memory_key,lifecycle,
             created_at_ms,updated_at_ms,kind,current_version_id,revoked_at_ms
         ) VALUES ($1,$2,4,$3,'privacy:agent-memory',1,10,10,4,$4,NULL)",
    )
    .bind(memory_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(agent_id.into_uuid())
    .bind(memory_version_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .expect("seed Agent Memory");
    sqlx::query(
        "INSERT INTO memory.memory_versions (
             memory_version_id,memory_id,version,content_id,content_hash,
             sensitivity,confidence,provenance,valid_from_ms,valid_to_ms,
             source,confidence_bps
         ) VALUES ($1,$2,1,$3,$4,3,1.0,$5,10,NULL,2,10000)",
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

    // These are live execution tests, not historical fact replay. Keep the
    // original relative ordering without a retention deadline in 1970.
    let base: i64 = sqlx::query_scalar(
        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
    )
    .fetch_one(&pool)
    .await
    .expect("agent memory permanent forget is atomically lineage bound in postgres must succeed");
    let base = base - 1_000;
    let command = RequestAgentMemoryErasure {
        tenant_id,
        requested_by: Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        },
        agent_id,
        memory_id,
        expected_memory_version_id: memory_version_id,
        reason_code: DottedKey::new("user.permanent-forget-requested").expect("reason"),
        idempotency_key: IdempotencyKey::new("erase:agent-memory-postgres").expect("idempotency"),
        requested_at: UtcTimestamp::from_unix_millis(base + 100),
        complete_by: UtcTimestamp::from_unix_millis(base + 2_592_000_100),
    };
    let erasure = PostgresPrivacyErasureStore::new(pool.clone());
    let admitted = erasure
        .request_agent_memory_erasure(command.clone())
        .await
        .expect("admit permanent-forget request");
    assert_eq!(admitted.disposition, ErasureAppendDisposition::Applied);
    assert_eq!(admitted.job.status(), Some(ErasureJobStatus::Fenced));
    assert_eq!(admitted.lineage.content_ref, content);
    assert_eq!(admitted.job.plans[0].data_classes, ErasureDataClass::ALL);
    assert_replay(&erasure, &command, &admitted, base + 150).await;
    assert_conflicting_intents(&erasure, &command).await;
    assert_replay(&erasure, &command, &admitted, base + 160).await;
    assert_eq!(
        erasure
            .request_agent_memory_erasure(command.clone())
            .await
            .expect("replay permanent-forget request")
            .disposition,
        ErasureAppendDisposition::Duplicate
    );
    let facts: (i16, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT lifecycle FROM memory.memories WHERE memory_id=$1),
             (SELECT count(*) FROM memory.agent_memory_revocations WHERE memory_id=$1),
             (SELECT count(*) FROM memory.agent_memory_erasure_lineage WHERE memory_id=$1),
             (SELECT count(*) FROM privacy_erasure.transitions WHERE erasure_job_id=$2)",
    )
    .bind(memory_id.into_uuid())
    .bind(admitted.lineage.erasure_job_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read atomic permanent-forget facts");
    assert_eq!(facts, (3, 1, 1, 3));
    let mut conflicting = command.clone();
    conflicting.idempotency_key =
        IdempotencyKey::new("erase:agent-memory-postgres-conflict").expect("idempotency");
    assert!(matches!(
        erasure.request_agent_memory_erasure(conflicting).await,
        Err(PostgresPrivacyErasureError::IdempotencyConflict)
    ));

    let completed = AgentMemoryErasureExecutor::new(&erasure, &content_store)
        .execute(
            tenant_id,
            admitted.lineage.erasure_job_id,
            UtcTimestamp::from_unix_millis(base + 200),
        )
        .await
        .expect("execute every immediate Agent Memory erasure provider");
    let completion = completed.job.receipt.as_ref().expect(
        "agent memory permanent forget is atomically lineage bound in postgres must succeed",
    );
    let observed_now: i64 = sqlx::query_scalar(
        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
    )
    .fetch_one(&pool)
    .await
    .expect("agent memory permanent forget is atomically lineage bound in postgres must succeed");
    assert!(
        completion.completed_at.unix_millis() >= base + 1_000,
        "receipt must use the store's actual clock after providers, not execute(started_at)"
    );
    assert!(completion.completed_at.unix_millis() <= observed_now);
    assert_eq!(
        completion.proof_digest,
        musteragent_runtime_privacy::erasure_proof_digest(&completed.job, completion.completed_at)
    );
    assert_eq!(
        completed.job.status(),
        Some(ErasureJobStatus::CompletedWithRetentionException)
    );
    assert_eq!(
        completed
            .job
            .receipt
            .as_ref()
            .expect("terminal erasure receipt")
            .retention_exceptions
            .len(),
        2
    );
    assert!(matches!(
        content_store.read(tenant_id, &content).await,
        Err(musteragent_runtime_content::ContentStoreError::NotFound)
    ));
    let erased_projection: (String, i64, i64) = sqlx::query_as(
        "SELECT memory.memory_key,
                (SELECT count(*) FROM memory.search_index
                 WHERE memory_version_id=memory.current_version_id),
                (SELECT count(*) FROM memory.search_backfill_jobs
                 WHERE memory_version_id=memory.current_version_id)
         FROM memory.memories AS memory WHERE memory.memory_id=$1",
    )
    .bind(memory_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("inspect erased Agent Memory providers");
    assert_eq!(erased_projection, (format!("erased:{memory_id}"), 0, 0));
    let completed_admission = musteragent_runtime_privacy::AgentMemoryErasureAdmission {
        disposition: ErasureAppendDisposition::Duplicate,
        lineage: admitted.lineage,
        job: completed.job,
    };
    assert_replay(
        &erasure,
        &command,
        &completed_admission,
        base + 3_000_000_000,
    )
    .await;
    assert_conflicting_intents(&erasure, &command).await;
    assert_replay(
        &erasure,
        &command,
        &completed_admission,
        base + 3_000_000_001,
    )
    .await;
    // A new adapter/connection reads the same persisted receipt; no process
    // memory or expired-at-now validation is needed to recover the fact.
    pool.close().await;
    let reopened_pool = postgres.pool().await.expect("reconnect PostgreSQL");
    let reopened_erasure = PostgresPrivacyErasureStore::new(reopened_pool.clone());
    assert_replay(
        &reopened_erasure,
        &command,
        &completed_admission,
        base + 3_000_000_002,
    )
    .await;
    reopened_pool.close().await;
}

fn privacy_reference_keys() -> Arc<OpaqueRefKeyring> {
    Arc::new(
        OpaqueRefKeyring::new(
            DottedKey::new("runtime.content.privacy-test").expect("key id"),
            [(
                DottedKey::new("runtime.content.privacy-test").expect("key id"),
                [73_u8; 32],
            )],
        )
        .expect("reference keyring"),
    )
}

#[tokio::test]
async fn privacy_erasure_is_tenant_scoped_idempotent_fenced_append_only_and_audited() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let store = PostgresPrivacyErasureStore::new(pool.clone());
    assert_store(&store);

    let request = request_fixture(TenantId::generate(), "erase:primary");
    let tenant_id = request.tenant_id;
    let job_id = request.erasure_job_id;
    let content_root = match &request.root_resource_refs[0] {
        ResourceRef::Content(content) => content,
        _ => panic!("fixture root must be Content"),
    };
    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id,tenant_id,media_type,byte_length,content_hash,
             storage_ref,storage_ref_protected,state,created_at_ms,updated_at_ms
         ) VALUES ($1,$2,$3,$4::numeric,$5,$6,NULL,2,1,1)",
    )
    .bind(content_root.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(content_root.media_type.as_str())
    .bind(content_root.byte_length.to_string())
    .bind(content_root.sha256.into_bytes().as_slice())
    .bind(format!("test-content://{}", content_root.content_id))
    .execute(&pool)
    .await
    .expect("register recoverable Content root");
    let created = store.create(request.clone()).await.expect("create request");
    assert_eq!(created.disposition, ErasureAppendDisposition::Applied);
    assert_eq!(created.job.status(), Some(ErasureJobStatus::PendingPlan));
    assert_eq!(
        store
            .create(request.clone())
            .await
            .expect("exact request replay")
            .disposition,
        ErasureAppendDisposition::Duplicate
    );
    let mut drifted = request.clone();
    drifted.complete_by = UtcTimestamp::from_unix_millis(501);
    assert!(matches!(
        store.create(drifted).await,
        Err(PostgresPrivacyErasureError::IdempotencyConflict)
    ));
    assert!(
        store
            .load(TenantId::generate(), job_id)
            .await
            .expect("tenant-scoped load")
            .is_none()
    );

    let exception = ErasureRetentionException {
        data_class: ErasureDataClass::Backup,
        policy_ref: DataRetentionPolicyRef::new("legal.backup-expiry@1").expect("policy"),
        owner_principal_id: PrincipalId::generate(),
        expires_at: UtcTimestamp::from_unix_millis(
            sqlx::query_scalar::<_, i64>(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&pool)
            .await
            .expect("privacy erasure is tenant scoped idempotent fenced append only and audited must succeed")
                + 60_000,
        ),
    };
    let plan = plan(&request, vec![exception.clone()]);
    assert_eq!(
        store
            .append_plan(tenant_id, plan.clone())
            .await
            .expect("append plan")
            .disposition,
        ErasureAppendDisposition::Applied
    );
    assert_eq!(
        store
            .append_plan(tenant_id, plan.clone())
            .await
            .expect("exact plan replay")
            .disposition,
        ErasureAppendDisposition::Duplicate
    );

    store
        .append_transition(
            tenant_id,
            job_id,
            transition(2, ErasureJobStatus::Planned, 20),
        )
        .await
        .expect("append Planned");
    assert!(matches!(
        store
            .append_transition(
                tenant_id,
                job_id,
                transition(3, ErasureJobStatus::Executing, 30)
            )
            .await,
        Err(PostgresPrivacyErasureError::Database(_))
    ));
    store
        .append_transition(
            tenant_id,
            job_id,
            transition(3, ErasureJobStatus::Fenced, 30),
        )
        .await
        .expect("append read fence");
    store
        .append_transition(
            tenant_id,
            job_id,
            transition(4, ErasureJobStatus::Executing, 40),
        )
        .await
        .expect("append execution after fence");
    assert!(matches!(
        store
            .append_transition(
                tenant_id,
                job_id,
                transition(5, ErasureJobStatus::CompletedWithRetentionException, 50)
            )
            .await,
        Err(PostgresPrivacyErasureError::TerminalMustBeAtomic)
    ));

    let terminal = transition(5, ErasureJobStatus::CompletedWithRetentionException, 50);
    let receipt = ErasureReceipt {
        schema_version: request.schema_version.clone(),
        erasure_receipt_id: ErasureReceiptId::generate(),
        erasure_job_id: job_id,
        tenant_id,
        outcome: ErasureReceiptOutcome::ErasedWithRetentionException,
        plan_hash: plan.plan_hash,
        completed_data_classes: vec![ErasureDataClass::ContentCiphertext],
        retention_exceptions: vec![exception],
        proof_digest: Sha256::digest(b"provider-proof-digest-only"),
        completed_at: UtcTimestamp::from_unix_millis(50),
    };
    assert!(
        matches!(
            store.complete(terminal, receipt.clone()).await,
            Err(PostgresPrivacyErasureError::FactConflict)
        ),
        "legacy entry cannot record a caller-timed completion"
    );
    let completion = musteragent_runtime_privacy::CompleteErasureExecution::from_job(
        &store.load(tenant_id, job_id).await.expect("privacy erasure is tenant scoped idempotent fenced append only and audited must succeed").expect("privacy erasure is tenant scoped idempotent fenced append only and audited must succeed"),
        UtcTimestamp::from_unix_millis(40),
    )
    .expect("privacy erasure is tenant scoped idempotent fenced append only and audited must succeed");
    assert!(matches!(
        store.complete_execution(completion).await,
        Err(PostgresPrivacyErasureError::ContentCiphertextNotErased)
    ));
    sqlx::query(
        "UPDATE artifact.content_refs
         SET state=3,storage_ref=NULL,storage_ref_protected=NULL,updated_at_ms=49
         WHERE tenant_id=$1 AND content_id=$2",
    )
    .bind(tenant_id.into_uuid())
    .bind(content_root.content_id.into_uuid())
    .execute(&pool)
    .await
    .expect("remove recoverable Content locator");
    let completed = store
        .complete_execution(completion)
        .await
        .expect("atomically complete with DB clock receipt");
    let receipt = completed.job.receipt.clone().expect(
        "privacy erasure is tenant scoped idempotent fenced append only and audited must succeed",
    );
    let terminal = *completed.job.transitions.last().expect(
        "privacy erasure is tenant scoped idempotent fenced append only and audited must succeed",
    );
    assert!(receipt.completed_at.unix_millis() > 50);
    assert_eq!(
        store
            .complete_execution(completion)
            .await
            .expect("privacy erasure is tenant scoped idempotent fenced append only and audited must succeed")
            .disposition,
        ErasureAppendDisposition::Duplicate
    );
    assert_eq!(completed.disposition, ErasureAppendDisposition::Applied);
    assert_eq!(completed.job.receipt, Some(receipt.clone()));
    assert_eq!(
        store
            .complete(terminal, receipt.clone())
            .await
            .expect("exact completion replay")
            .disposition,
        ErasureAppendDisposition::Duplicate
    );

    let page = store
        .list(ErasureJobQuery {
            tenant_id,
            after: None,
            limit: ErasureQueryLimit::new(10).expect("limit"),
        })
        .await
        .expect("list erasure jobs");
    assert_eq!(page.jobs.len(), 1);
    assert!(page.next_cursor.is_none());

    let audit_policies =
        ApprovedRetentionPolicies::new([
            DataRetentionPolicyRef::new(AUDIT_POLICY).expect("audit policy")
        ])
        .expect("approved policies");
    let audit_store = PostgresSecurityAuditTestStore::new(pool.clone(), Arc::new(audit_policies));
    let audit_page = audit_store
        .list(SecurityAuditQuery {
            tenant_id,
            after: None,
            limit: 10,
        })
        .await
        .expect("list producer-owned erasure audit");
    assert_eq!(audit_page.records.len(), 1);
    let produced = &audit_page.records[0].proposal;
    assert_eq!(produced.action, SecurityAuditAction::DataErasure);
    assert_eq!(
        produced.source_fact_ref,
        Some(SecurityAuditSourceRef::ErasureJob(job_id))
    );
    assert!(
        produced
            .resource_refs
            .contains(&ResourceRef::ErasureReceipt(receipt.erasure_receipt_id))
    );
    assert!(matches!(
        PostgresSecurityAuditPipeline::standard(pool.clone())
            .verify_erasure_completed(TenantId::generate(), job_id)
            .await,
        Err(PostgresSecurityAuditError::FactAuthorityMismatch)
    ));
    let audit = erasure_audit(tenant_id, job_id);
    let mut duplicate_source = audit.clone();
    duplicate_source.audit_record_id = AuditRecordId::generate();
    assert!(matches!(
        audit_store.append(duplicate_source).await,
        Err(PostgresSecurityAuditError::SourceActionConflict)
    ));

    let foreign_request = request_fixture(TenantId::generate(), "erase:foreign");
    store
        .create(foreign_request.clone())
        .await
        .expect("create foreign request");
    let mut cross_tenant = erasure_audit(tenant_id, foreign_request.erasure_job_id);
    cross_tenant.resource_refs = vec![ResourceRef::ErasureJob(foreign_request.erasure_job_id)];
    assert!(matches!(
        audit_store.append(cross_tenant).await,
        Err(PostgresSecurityAuditError::SourceErasureTenantMismatch)
    ));
    let missing_job = ErasureJobId::generate();
    assert!(matches!(
        audit_store
            .append(erasure_audit(tenant_id, missing_job))
            .await,
        Err(PostgresSecurityAuditError::SourceErasureJobNotFound)
    ));

    assert!(
        sqlx::query("UPDATE privacy_erasure.requests SET complete_by_ms = 999")
            .execute(&pool)
            .await
            .is_err(),
        "request UPDATE must fail"
    );
    assert!(
        sqlx::query("DELETE FROM privacy_erasure.transitions WHERE erasure_job_id = $1")
            .bind(job_id.into_uuid())
            .execute(&pool)
            .await
            .is_err(),
        "transition DELETE must fail"
    );
    let persisted_text: String = sqlx::query_scalar(
        "SELECT coalesce(string_agg(value, '|'), '') FROM (
             SELECT schema_version AS value FROM privacy_erasure.requests
             UNION ALL SELECT retention_policy_ref FROM privacy_erasure.requests
             UNION ALL SELECT idempotency_key FROM privacy_erasure.requests
             UNION ALL SELECT coalesce(content_media_type, '') FROM privacy_erasure.root_resources
             UNION ALL SELECT policy_ref FROM privacy_erasure.plan_retention_exceptions
             UNION ALL SELECT policy_ref FROM privacy_erasure.receipt_retention_exceptions
         ) values_to_scan",
    )
    .fetch_one(&pool)
    .await
    .expect("scan persisted erasure text");
    assert!(!persisted_text.contains(SECRET_CORPUS));
    let ledger_kinds: Vec<i16> =
        sqlx::query_scalar("SELECT DISTINCT record_kind FROM event_store.ledger_records")
            .fetch_all(&pool)
            .await
            .expect("read Ledger kinds");
    assert!(
        ledger_kinds.is_empty(),
        "erasure must not invent a Ledger kind"
    );
}

#[tokio::test]
async fn full_text_erasure_removes_projection_and_rebuild_work_and_prevents_resurrection() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let privacy = PostgresPrivacyErasureStore::new(pool.clone());
    let memory = PostgresMemoryEvolutionStore::new(pool.clone());
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let memory_id = MemoryId::generate();
    let memory_version_id = MemoryVersionId::generate();
    let memory_bytes = b"private searchable memory";
    let content = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media type"),
        byte_length: u64::try_from(memory_bytes.len()).expect("bounded fixture"),
        sha256: Sha256::digest(memory_bytes),
    };
    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms
         ) VALUES ($1,$2,$3,$4::numeric,$5,$6,1)",
    )
    .bind(content.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().as_slice())
    .bind(format!("test-content://{}", content.content_id))
    .execute(&pool)
    .await
    .expect("register Memory content");
    let mut transaction = pool.begin().await.expect("begin Memory fixture");
    sqlx::query(
        "INSERT INTO memory.memories (
             memory_id,tenant_id,scope_kind,scope_id,memory_key,lifecycle,
             created_at_ms,updated_at_ms,kind,current_version_id,revoked_at_ms
         ) VALUES ($1,$2,1,$3,'privacy:searchable',1,10,10,4,$4,NULL)",
    )
    .bind(memory_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(principal_id.into_uuid())
    .bind(memory_version_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .expect("seed Memory");
    sqlx::query(
        "INSERT INTO memory.memory_versions (
             memory_version_id,memory_id,version,content_id,content_hash,
             sensitivity,confidence,provenance,valid_from_ms,valid_to_ms,
             source,confidence_bps
         ) VALUES ($1,$2,1,$3,$4,3,1.0,$5,10,NULL,2,10000)",
    )
    .bind(memory_version_id.into_uuid())
    .bind(memory_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .bind(content.sha256.into_bytes().as_slice())
    .bind(b"normalized-provenance:v1".as_slice())
    .execute(&mut *transaction)
    .await
    .expect("seed Memory version");
    sqlx::query(
        "INSERT INTO memory.search_index (
             memory_version_id,tenant_id,memory_id,source_content_hash,
             feature_hash,features,indexed_at_ms
         ) VALUES ($1,$2,$3,$4,$5,to_tsvector('simple','private searchable memory'),10)",
    )
    .bind(memory_version_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(memory_id.into_uuid())
    .bind(content.sha256.into_bytes().as_slice())
    .bind(
        Sha256::digest(b"private searchable memory")
            .into_bytes()
            .as_slice(),
    )
    .execute(&mut *transaction)
    .await
    .expect("seed search projection");
    sqlx::query(
        "INSERT INTO memory.search_backfill_jobs (
             memory_version_id,tenant_id,memory_id,status,attempt_count,
             lease_owner,lease_expires_at_ms,available_at_ms,last_error_code,
             created_at_ms,updated_at_ms,completed_at_ms
         ) VALUES ($1,$2,$3,1,0,NULL,NULL,10,NULL,10,10,NULL)",
    )
    .bind(memory_version_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(memory_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .expect("seed stale rebuild work");
    transaction.commit().await.expect("commit Memory fixture");

    let request = ErasureRequest {
        schema_version: DottedKey::new("musteragent.legacy.privacy.erasure.v1").expect("schema"),
        erasure_job_id: ErasureJobId::generate(),
        tenant_id,
        requested_by: Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        },
        root_resource_refs: vec![ResourceRef::Content(content.clone())],
        retention_policy_ref: DataRetentionPolicyRef::new(PRIVACY_POLICY).expect("policy"),
        idempotency_key: IdempotencyKey::new("erase:full-text").expect("idempotency"),
        requested_at: UtcTimestamp::from_unix_millis(20),
        complete_by: UtcTimestamp::from_unix_millis(500),
    };
    privacy
        .create(request.clone())
        .await
        .expect("create erasure");
    let plan = ErasurePlanSnapshot::new(
        request.schema_version.clone(),
        request.erasure_job_id,
        ErasurePlanVersion::new(1).expect("version"),
        request.root_resource_refs.clone(),
        vec![ErasureDataClass::FullTextIndex],
        Vec::new(),
        request.fingerprint().expect("lineage"),
        UtcTimestamp::from_unix_millis(30),
    )
    .expect("FullTextIndex plan");
    privacy
        .append_plan(tenant_id, plan.clone())
        .await
        .expect("append FullTextIndex plan");
    for (sequence, status, occurred_at) in [
        (2, ErasureJobStatus::Planned, 40),
        (3, ErasureJobStatus::Fenced, 50),
        (4, ErasureJobStatus::Executing, 60),
    ] {
        privacy
            .append_transition(
                tenant_id,
                request.erasure_job_id,
                transition(sequence, status, occurred_at),
            )
            .await
            .expect("advance erasure");
    }
    let completion = musteragent_runtime_privacy::CompleteErasureExecution::from_job(
        &privacy
            .load(tenant_id, request.erasure_job_id)
            .await
            .expect("full text erasure removes projection and rebuild work and prevents resurrection must succeed")
            .expect("full text erasure removes projection and rebuild work and prevents resurrection must succeed"),
        UtcTimestamp::from_unix_millis(60),
    )
    .expect("full text erasure removes projection and rebuild work and prevents resurrection must succeed");
    privacy
        .complete_execution(completion)
        .await
        .expect("atomically erase FullTextIndex and complete");

    let (projection_count, backfill_count, tombstone_count): (i64, i64, i64) = sqlx::query_as(
        "SELECT
                 (SELECT count(*) FROM memory.search_index WHERE memory_version_id=$1),
                 (SELECT count(*) FROM memory.search_backfill_jobs WHERE memory_version_id=$1),
                 (SELECT count(*) FROM memory.search_erasure_tombstones
                  WHERE memory_version_id=$1 AND erasure_job_id=$2)",
    )
    .bind(memory_version_id.into_uuid())
    .bind(request.erasure_job_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read FullTextIndex erasure proof");
    assert_eq!(
        (projection_count, backfill_count, tombstone_count),
        (0, 0, 1)
    );
    assert_eq!(
        memory
            .enqueue_missing(UtcTimestamp::from_unix_millis(80))
            .await
            .expect("reconcile search backfill after erasure"),
        0,
        "completed erasure must fence historical rebuild"
    );
    let resurrection = sqlx::query(
        "INSERT INTO memory.search_index (
             memory_version_id,tenant_id,memory_id,source_content_hash,
             feature_hash,features,indexed_at_ms
         ) VALUES ($1,$2,$3,$4,$5,to_tsvector('simple','private searchable memory'),90)",
    )
    .bind(memory_version_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(memory_id.into_uuid())
    .bind(content.sha256.into_bytes().as_slice())
    .bind(
        Sha256::digest(b"private searchable memory")
            .into_bytes()
            .as_slice(),
    )
    .execute(&pool)
    .await;
    assert!(
        resurrection.is_err(),
        "erased search features must not resurrect"
    );
    assert!(
        sqlx::query(
            "DELETE FROM memory.search_erasure_tombstones
             WHERE memory_version_id=$1"
        )
        .bind(memory_version_id.into_uuid())
        .execute(&pool)
        .await
        .is_err(),
        "anti-resurrection proof must be append-only"
    );
}

#[tokio::test]
async fn database_rejects_incomplete_requests_terminal_gaps_and_receipt_coverage_drift() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let store = PostgresPrivacyErasureStore::new(pool.clone());

    let mut incomplete = pool.begin().await.expect("begin incomplete request");
    sqlx::query(
        "INSERT INTO privacy_erasure.requests (
             erasure_job_id, schema_version, tenant_id,
             actor_principal_id, actor_principal_kind,
             retention_policy_ref, idempotency_key,
             requested_at_ms, complete_by_ms, root_resource_count,
             request_fingerprint
         ) VALUES ($1, 'musteragent.legacy.privacy.erasure.v1', $2, $3, 1,
                   $4, 'erase:incomplete', 1, 100, 1, $5)",
    )
    .bind(ErasureJobId::generate().into_uuid())
    .bind(TenantId::generate().into_uuid())
    .bind(PrincipalId::generate().into_uuid())
    .bind(PRIVACY_POLICY)
    .bind([1_u8; 32].as_slice())
    .execute(&mut *incomplete)
    .await
    .expect("insert before deferred validation");
    assert!(incomplete.commit().await.is_err());

    let request = request_fixture(TenantId::generate(), "erase:raw-terminal");
    let tenant_id = request.tenant_id;
    let job_id = request.erasure_job_id;
    store.create(request.clone()).await.expect("create request");
    let plan = plan(&request, Vec::new());
    store
        .append_plan(tenant_id, plan.clone())
        .await
        .expect("append plan");
    for transition in [
        transition(2, ErasureJobStatus::Planned, 20),
        transition(3, ErasureJobStatus::Fenced, 30),
        transition(4, ErasureJobStatus::Executing, 40),
    ] {
        store
            .append_transition(tenant_id, job_id, transition)
            .await
            .expect("append prerequisite transition");
    }

    let mut terminal_without_receipt = pool.begin().await.expect("begin terminal gap");
    sqlx::query(
        "INSERT INTO privacy_erasure.transitions (
             tenant_id, erasure_job_id, sequence, status, occurred_at_ms
         ) VALUES ($1, $2, 5, 7, 50)",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_id.into_uuid())
    .execute(&mut *terminal_without_receipt)
    .await
    .expect("terminal insert before deferred validation");
    assert!(terminal_without_receipt.commit().await.is_err());

    let mut incomplete_receipt = pool.begin().await.expect("begin incomplete receipt");
    sqlx::query(
        "INSERT INTO privacy_erasure.transitions (
             tenant_id, erasure_job_id, sequence, status, occurred_at_ms
         ) VALUES ($1, $2, 5, 7, 50)",
    )
    .bind(tenant_id.into_uuid())
    .bind(job_id.into_uuid())
    .execute(&mut *incomplete_receipt)
    .await
    .expect("insert terminal");
    sqlx::query(
        "INSERT INTO privacy_erasure.receipts (
             erasure_receipt_id, tenant_id, erasure_job_id, schema_version,
             outcome, plan_hash, completed_data_class_count,
             retention_exception_count, proof_digest, completed_at_ms
         ) VALUES ($1, $2, $3, 'musteragent.legacy.privacy.erasure.v1', 1, $4, 2, 0, $5, 50)",
    )
    .bind(ErasureReceiptId::generate().into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(job_id.into_uuid())
    .bind(plan.plan_hash.into_bytes().as_slice())
    .bind([2_u8; 32].as_slice())
    .execute(&mut *incomplete_receipt)
    .await
    .expect("insert receipt header");
    assert!(incomplete_receipt.commit().await.is_err());
}

#[tokio::test]
async fn baseline_blocks_new_unowned_audit_sources() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate baseline");
    let tenant_id = uuid::Uuid::now_v7();
    let ledger_record_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO event_store.ledger_records (
             record_id, schema_version, record_kind, tenant_id, conversation_id,
             conversation_seq, home_epoch, producer, occurred_at_ms, committed_at_ms,
             correlation_id, idempotency_key, payload_hash
         ) VALUES ($1, 'musteragent.runtime.record.v1', 1, $2, $3, 1, 1, 1,
                   1, 1, $4, 'legacy-skill-audit', $5)",
    )
    .bind(ledger_record_id)
    .bind(tenant_id)
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .bind([1_u8; 32].as_slice())
    .execute(&pool)
    .await
    .expect("seed legacy Ledger source");
    let new_unowned = sqlx::query(
        "INSERT INTO security_audit.records (
             audit_record_id, schema_version, tenant_id,
             actor_principal_id, actor_principal_kind, action, outcome,
             resource_count, source_ledger_record_id, correlation_id,
             retention_policy_ref, occurred_at_ms, proposal_fingerprint
         ) VALUES ($1, 'musteragent.legacy.security.audit.v1', $2, $3, 1, 4, 1, 1,
                   $4, $5, $6, 1, $7)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(tenant_id)
    .bind(uuid::Uuid::now_v7())
    .bind(ledger_record_id)
    .bind(uuid::Uuid::now_v7())
    .bind(AUDIT_POLICY)
    .bind([4_u8; 32].as_slice())
    .execute(&pool)
    .await;
    assert!(
        new_unowned.is_err(),
        "new Skill audit cannot borrow an unrelated Ledger source"
    );
}

fn request_fixture(tenant_id: TenantId, idempotency_key: &str) -> ErasureRequest {
    ErasureRequest {
        schema_version: DottedKey::new("musteragent.legacy.privacy.erasure.v1").expect("schema"),
        erasure_job_id: ErasureJobId::generate(),
        tenant_id,
        requested_by: Actor {
            tenant_id,
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        root_resource_refs: vec![ResourceRef::Content(ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/plain").expect("media type"),
            byte_length: u64::try_from(SECRET_CORPUS.len()).expect("length"),
            sha256: Sha256::digest(SECRET_CORPUS.as_bytes()),
        })],
        retention_policy_ref: DataRetentionPolicyRef::new(PRIVACY_POLICY).expect("policy"),
        idempotency_key: IdempotencyKey::new(idempotency_key).expect("idempotency"),
        requested_at: UtcTimestamp::from_unix_millis(10),
        complete_by: UtcTimestamp::from_unix_millis(500),
    }
}

fn plan(
    request: &ErasureRequest,
    exceptions: Vec<ErasureRetentionException>,
) -> ErasurePlanSnapshot {
    ErasurePlanSnapshot::new(
        request.schema_version.clone(),
        request.erasure_job_id,
        ErasurePlanVersion::new(1).expect("version"),
        request.root_resource_refs.clone(),
        vec![
            ErasureDataClass::ContentCiphertext,
            ErasureDataClass::Backup,
        ],
        exceptions,
        request.fingerprint().expect("lineage"),
        UtcTimestamp::from_unix_millis(20),
    )
    .expect("plan")
}

fn transition(sequence: u64, status: ErasureJobStatus, occurred_at: i64) -> ErasureTransition {
    ErasureTransition {
        sequence: ErasureTransitionSequence::new(sequence).expect("sequence"),
        status,
        occurred_at: UtcTimestamp::from_unix_millis(occurred_at),
    }
}

fn erasure_audit(tenant_id: TenantId, erasure_job_id: ErasureJobId) -> ProposedSecurityAuditRecord {
    ProposedSecurityAuditRecord {
        schema_version: DottedKey::new("musteragent.legacy.security.audit.v1").expect("schema"),
        audit_record_id: AuditRecordId::generate(),
        tenant_id,
        actor_principal_id: PrincipalId::generate(),
        actor_principal_kind: PrincipalKind::User,
        action: SecurityAuditAction::DataErasure,
        outcome: SecurityAuditOutcome::Succeeded,
        resource_refs: vec![ResourceRef::ErasureJob(erasure_job_id)],
        source_fact_ref: Some(SecurityAuditSourceRef::ErasureJob(erasure_job_id)),
        correlation_id: CorrelationId::generate(),
        trace_id: None,
        detail_ref: None,
        retention_policy_ref: DataRetentionPolicyRef::new(AUDIT_POLICY).expect("policy"),
        occurred_at: UtcTimestamp::from_unix_millis(50),
    }
}

fn assert_store<T: ErasureJobStore>(_store: &T) {}
