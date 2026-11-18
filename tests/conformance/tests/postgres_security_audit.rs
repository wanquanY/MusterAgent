use std::collections::HashSet;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    AuditRecordId, ContentId, ConversationId, CorrelationId, DottedKey, HomeEpoch, IdempotencyKey,
    InteractionId, LedgerOffset, LedgerRecordId, PrincipalId, PrincipalKind, RunId, TenantId,
    UtcTimestamp,
};
use musteragent_ledger::{AppendRequest, ProposedRecord};
use musteragent_postgres::{
    PostgresLedgerStore, PostgresSecurityAuditError, PostgresSecurityAuditTestStore,
};
use musteragent_runtime_audit::{
    ApprovedRetentionPolicies, DataRetentionPolicyRef, ProposedSecurityAuditRecord,
    SecurityAuditAction, SecurityAuditAppendDisposition, SecurityAuditOutcome, SecurityAuditQuery,
    SecurityAuditSourceRef,
};
use musteragent_runtime_contract::ResourceRef;
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;

const RETENTION_POLICY: &str = "security.audit.standard@1";
const SECRET_CORPUS: &str = "sk-live-never-persist-this-secret";

#[tokio::test]
async fn security_audit_is_append_only_tenant_scoped_idempotent_and_secret_free() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let policies = ApprovedRetentionPolicies::new([retention()]).expect("approved policy");
    let store = PostgresSecurityAuditTestStore::new(pool.clone(), Arc::new(policies));

    let tenant = TenantId::generate();
    let other_tenant = TenantId::generate();
    let approval_source = seed_ledger_fact(&pool, tenant, b"interaction.decision").await;
    let foreign_source = seed_ledger_fact(&pool, other_tenant, b"foreign.fact").await;

    let approval = proposal(
        tenant,
        SecurityAuditAction::ApprovalDecision,
        SecurityAuditOutcome::Succeeded,
        vec![ResourceRef::Interaction(InteractionId::generate())],
        Some(approval_source),
        None,
    );
    let applied = store
        .append(approval.clone())
        .await
        .expect("append approval audit");
    assert_eq!(applied.disposition, SecurityAuditAppendDisposition::Applied);
    let duplicate = store
        .append(approval.clone())
        .await
        .expect("replay exact audit proposal");
    assert_eq!(
        duplicate.disposition,
        SecurityAuditAppendDisposition::Duplicate
    );
    assert_eq!(duplicate.record.committed_at, applied.record.committed_at);

    let mut drifted = approval.clone();
    drifted.outcome = SecurityAuditOutcome::Failed;
    assert!(matches!(
        store.append(drifted).await,
        Err(PostgresSecurityAuditError::IdempotencyConflict)
    ));
    let mut duplicate_source = approval.clone();
    duplicate_source.audit_record_id = AuditRecordId::generate();
    assert!(matches!(
        store.append(duplicate_source).await,
        Err(PostgresSecurityAuditError::SourceActionConflict)
    ));

    let independent_denial = proposal(
        tenant,
        SecurityAuditAction::AuthorizationDenied,
        SecurityAuditOutcome::Denied,
        vec![ResourceRef::Run(RunId::generate())],
        None,
        None,
    );
    store
        .append(independent_denial)
        .await
        .expect("append independently keyed denial audit");

    let absent_detail = content_ref(b"classified denial detail");
    let denied = proposal(
        tenant,
        SecurityAuditAction::AuthorizationDenied,
        SecurityAuditOutcome::Denied,
        vec![ResourceRef::Conversation(ConversationId::generate())],
        None,
        Some(absent_detail.clone()),
    );
    store
        .append(denied.clone())
        .await
        .expect("append authorization-denied audit");

    let foreign_record = proposal(
        other_tenant,
        SecurityAuditAction::AuthorizationDenied,
        SecurityAuditOutcome::Denied,
        vec![ResourceRef::Run(RunId::generate())],
        None,
        None,
    );
    store
        .append(foreign_record)
        .await
        .expect("append other-tenant audit");

    let cross_tenant = proposal(
        tenant,
        SecurityAuditAction::ApprovalDecision,
        SecurityAuditOutcome::Succeeded,
        vec![ResourceRef::Interaction(InteractionId::generate())],
        Some(foreign_source),
        None,
    );
    assert!(matches!(
        store.append(cross_tenant).await,
        Err(PostgresSecurityAuditError::SourceLedgerTenantMismatch)
    ));
    let missing_source = proposal(
        tenant,
        SecurityAuditAction::ApprovalDecision,
        SecurityAuditOutcome::Succeeded,
        vec![ResourceRef::Interaction(InteractionId::generate())],
        Some(LedgerRecordId::generate()),
        None,
    );
    assert!(matches!(
        store.append(missing_source).await,
        Err(PostgresSecurityAuditError::SourceLedgerRecordNotFound)
    ));

    let mut cursor = None;
    let mut observed = Vec::new();
    loop {
        let page = store
            .list(SecurityAuditQuery {
                tenant_id: tenant,
                after: cursor,
                limit: 2,
            })
            .await
            .expect("list tenant audit page");
        observed.extend(page.records);
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(observed.len(), 3);
    let actions = observed
        .iter()
        .map(|record| record.proposal.action)
        .collect::<HashSet<_>>();
    assert_eq!(
        actions,
        HashSet::from([
            SecurityAuditAction::ApprovalDecision,
            SecurityAuditAction::AuthorizationDenied,
        ])
    );
    assert!(
        observed
            .iter()
            .all(|record| record.proposal.tenant_id == tenant)
    );
    let persisted_denied = observed
        .iter()
        .find(|record| record.proposal.audit_record_id == denied.audit_record_id)
        .expect("denied audit record");
    assert_eq!(persisted_denied.proposal.detail_ref, Some(absent_detail));

    let encoded = musteragent_wire::encode_security_audit_record(&applied.record)
        .expect("encode SecurityAuditRecord");
    assert!(!contains(&encoded, SECRET_CORPUS.as_bytes()));

    let database_bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT convert_to(string_agg(value, '|'), 'UTF8')
         FROM (
             SELECT schema_version AS value FROM security_audit.records
             UNION ALL
             SELECT retention_policy_ref FROM security_audit.records
             UNION ALL
             SELECT coalesce(detail_media_type, '') FROM security_audit.records
             UNION ALL
             SELECT coalesce(content_media_type, '') FROM security_audit.resource_refs
         ) AS persisted",
    )
    .fetch_one(&pool)
    .await
    .expect("read persisted audit text");
    assert!(!contains(
        database_bytes.as_slice(),
        SECRET_CORPUS.as_bytes()
    ));

    let update =
        sqlx::query("UPDATE security_audit.records SET outcome = 3 WHERE audit_record_id = $1")
            .bind(approval.audit_record_id.into_uuid())
            .execute(&pool)
            .await;
    assert!(update.is_err(), "audit fact UPDATE must fail in PostgreSQL");
    let delete = sqlx::query("DELETE FROM security_audit.resource_refs WHERE audit_record_id = $1")
        .bind(approval.audit_record_id.into_uuid())
        .execute(&pool)
        .await;
    assert!(
        delete.is_err(),
        "audit resource DELETE must fail in PostgreSQL"
    );

    let audit_count: i64 = sqlx::query_scalar("SELECT count(*) FROM security_audit.records")
        .fetch_one(&pool)
        .await
        .expect("count audit records");
    assert_eq!(audit_count, 4);
    let ledger_kinds: Vec<i16> =
        sqlx::query_scalar("SELECT DISTINCT record_kind FROM event_store.ledger_records")
            .fetch_all(&pool)
            .await
            .expect("read Ledger kinds");
    assert_eq!(ledger_kinds, vec![1]);
}

#[tokio::test]
async fn database_rejects_incomplete_resource_sets_and_unknown_enum_codes() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");

    let mut transaction = pool.begin().await.expect("begin transaction");
    sqlx::query(
        "INSERT INTO security_audit.records (
             audit_record_id, schema_version, tenant_id,
             actor_principal_id, actor_principal_kind, action, outcome,
             resource_count, correlation_id, retention_policy_ref,
             occurred_at_ms, proposal_fingerprint
         ) VALUES ($1, 'musteragent.legacy.security.audit.v1', $2, $3, 1, 3, 1, 1,
                   $4, $5, 0, $6)",
    )
    .bind(AuditRecordId::generate().into_uuid())
    .bind(TenantId::generate().into_uuid())
    .bind(PrincipalId::generate().into_uuid())
    .bind(CorrelationId::generate().into_uuid())
    .bind(RETENTION_POLICY)
    .bind([1_u8; 32].as_slice())
    .execute(&mut *transaction)
    .await
    .expect("insert incomplete record before deferred validation");
    assert!(
        transaction.commit().await.is_err(),
        "deferred constraint must reject a record without its complete resource set"
    );

    let unknown_enum = sqlx::query(
        "INSERT INTO security_audit.records (
             audit_record_id, schema_version, tenant_id,
             actor_principal_id, actor_principal_kind, action, outcome,
             resource_count, correlation_id, retention_policy_ref,
             occurred_at_ms, proposal_fingerprint
         ) VALUES ($1, 'musteragent.legacy.security.audit.v1', $2, $3, 1, 99, 1, 1,
                   $4, $5, 0, $6)",
    )
    .bind(AuditRecordId::generate().into_uuid())
    .bind(TenantId::generate().into_uuid())
    .bind(PrincipalId::generate().into_uuid())
    .bind(CorrelationId::generate().into_uuid())
    .bind(RETENTION_POLICY)
    .bind([2_u8; 32].as_slice())
    .execute(&pool)
    .await;
    assert!(
        unknown_enum.is_err(),
        "unknown audit action must fail closed"
    );
}

fn retention() -> DataRetentionPolicyRef {
    DataRetentionPolicyRef::new(RETENTION_POLICY).expect("retention policy")
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/octet-stream").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("fixture length"),
        sha256: Sha256::digest(bytes),
    }
}

fn proposal(
    tenant_id: TenantId,
    action: SecurityAuditAction,
    outcome: SecurityAuditOutcome,
    resource_refs: Vec<ResourceRef>,
    source_ledger_record_id: Option<LedgerRecordId>,
    detail_ref: Option<ContentRef>,
) -> ProposedSecurityAuditRecord {
    ProposedSecurityAuditRecord {
        schema_version: DottedKey::new("musteragent.legacy.security.audit.v1").expect("schema"),
        audit_record_id: AuditRecordId::generate(),
        tenant_id,
        actor_principal_id: PrincipalId::generate(),
        actor_principal_kind: PrincipalKind::User,
        action,
        outcome,
        resource_refs,
        source_fact_ref: source_ledger_record_id.map(SecurityAuditSourceRef::LedgerRecord),
        correlation_id: CorrelationId::generate(),
        trace_id: None,
        detail_ref,
        retention_policy_ref: retention(),
        occurred_at: UtcTimestamp::from_unix_millis(1),
    }
}

async fn seed_ledger_fact(pool: &PgPool, tenant_id: TenantId, payload: &[u8]) -> LedgerRecordId {
    let record_id = LedgerRecordId::generate();
    let conversation_id = ConversationId::generate();
    let request = AppendRequest {
        tenant_id,
        conversation_id,
        expected_home_epoch: HomeEpoch::new(1).expect("home epoch"),
        expected_offset: LedgerOffset::new(0),
        idempotency_key: IdempotencyKey::new(format!("seed:{record_id}")).expect("idempotency key"),
        command_fingerprint: Sha256::digest(record_id.to_string()),
        records: vec![ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
            record_id,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: None,
            run_id: None,
            run_seq: None,
            attempt_id: None,
            participant_id: None,
            producer: Producer::RuntimeApi,
            occurred_at: UtcTimestamp::from_unix_millis(1),
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload: payload.to_vec(),
            payload_hash: Sha256::digest(payload),
        }],
        outbox: vec![],
    };
    PostgresLedgerStore::new(pool.clone())
        .append(request)
        .await
        .expect("append canonical Ledger source");
    record_id
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
