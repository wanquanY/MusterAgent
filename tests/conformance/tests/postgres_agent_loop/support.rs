use std::sync::Arc;

use musteragent_identity::{InvocationId, RunId, TenantId};
use musteragent_loop::{DurableLoopCheckpoint, RunCompletionDisposition};
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresFencedToolInvocationJournal, PostgresSecurityAuditError,
    PostgresSecurityAuditPipeline,
};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::ProviderRequestRef;
use musteragent_storage_protection::{OpaqueRefKeyring, ProtectedContentKeyring};
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::ProtobufMeteringPublicationEncoder;
use sqlx::PgPool;

pub(super) fn provider_ref(value: &str) -> ProviderRequestRef {
    ProviderRequestRef::from_opaque(OpaqueExternalRef::new(value).expect("provider ref"))
}

pub(super) fn response_model_key() -> musteragent_model::ModelKey {
    musteragent_model::ModelKey::new("model-fixture").expect("model key")
}

pub(super) fn completed_checkpoint(disposition: RunCompletionDisposition) -> DurableLoopCheckpoint {
    match disposition {
        RunCompletionDisposition::Completed(checkpoint) => checkpoint,
        RunCompletionDisposition::Cancelled(_) => panic!("unexpected Run cancellation"),
        RunCompletionDisposition::DelegationCompletionsPending(_) => {
            panic!("unexpected pending delegation completion")
        }
        RunCompletionDisposition::ContextRefreshPending => {
            panic!("unexpected pending Context refresh")
        }
        RunCompletionDisposition::ControlsPending => panic!("unexpected pending Run control"),
        RunCompletionDisposition::RequiredWorkPending => panic!("unexpected pending required work"),
    }
}

pub(super) fn audited_loop_journal(
    pool: &PgPool,
    content: Arc<dyn ContentStore>,
    codec: Arc<ProtobufLedgerPayloadCodec>,
    keys: Arc<OpaqueRefKeyring>,
    recovery_keys: Arc<ProtectedContentKeyring>,
) -> PostgresAgentLoopJournal {
    musteragent_conformance::fixtures::model_journal::supervise(bare_loop_journal(
        pool,
        content,
        codec,
        keys,
        recovery_keys,
    ))
}
pub(super) fn bare_loop_journal(
    pool: &PgPool,
    content: Arc<dyn ContentStore>,
    codec: Arc<ProtobufLedgerPayloadCodec>,
    keys: Arc<OpaqueRefKeyring>,
    recovery_keys: Arc<ProtectedContentKeyring>,
) -> PostgresAgentLoopJournal {
    let audit = Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone()));
    PostgresAgentLoopJournal::with_security_audit(
        pool.clone(),
        content,
        codec.clone(),
        codec,
        Arc::new(ProtobufMeteringPublicationEncoder),
        keys,
        recovery_keys,
        audit,
    )
}

pub(super) fn security_audit_pipeline(pool: &PgPool) -> Arc<PostgresSecurityAuditPipeline> {
    Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone()))
}

pub(super) fn audited_tool_journal(
    pool: &PgPool,
    codec: Arc<ProtobufLedgerPayloadCodec>,
    keys: Arc<OpaqueRefKeyring>,
    audit: Arc<PostgresSecurityAuditPipeline>,
) -> PostgresFencedToolInvocationJournal {
    PostgresFencedToolInvocationJournal::with_security_audit(
        pool.clone(),
        codec.clone(),
        codec,
        keys,
        audit,
    )
}

pub(super) async fn assert_succeeded_model_audits(
    pool: &PgPool,
    tenant_id: TenantId,
    run_id: RunId,
    expected: i64,
) {
    let (audit_rows, usage_joins, content_rows): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(DISTINCT audit.audit_record_id),
                count(DISTINCT usage.invocation_id),
                count(*) FILTER (
                    WHERE audit.detail_content_id IS NOT NULL
                       OR resource.content_id IS NOT NULL
                )
         FROM security_audit.records AS audit
         JOIN runtime.model_invocations AS invocation
           ON invocation.tenant_id = audit.tenant_id
          AND invocation.invocation_id = audit.source_model_invocation_id
         JOIN runtime.runs AS run
           ON run.tenant_id = invocation.tenant_id
          AND run.run_id = invocation.run_id
         JOIN runtime.model_usage_facts AS usage
           ON usage.tenant_id = invocation.tenant_id
          AND usage.invocation_id = invocation.invocation_id
         LEFT JOIN security_audit.resource_refs AS resource
           ON resource.tenant_id = audit.tenant_id
          AND resource.audit_record_id = audit.audit_record_id
         WHERE audit.tenant_id = $1
           AND audit.action = 9
           AND audit.outcome = 1
           AND audit.trace_id = decode(
               '00000000000000000000000000000001', 'hex'
           )
           AND run.run_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("join Model Audit to Invocation, Run and Usage");
    assert_eq!(audit_rows, expected);
    assert_eq!(usage_joins, expected);
    assert_eq!(
        content_rows, 0,
        "Model request/response must not enter Audit"
    );
}

pub(super) async fn assert_cross_tenant_run_source_rejected(
    pool: &PgPool,
    invocation_id: InvocationId,
) {
    let foreign_tenant = TenantId::generate();
    let result = security_audit_pipeline(pool)
        .verify_model_terminal(foreign_tenant, invocation_id)
        .await;
    assert!(matches!(
        result,
        Err(PostgresSecurityAuditError::FactAuthorityMismatch)
    ));
    let foreign_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM security_audit.records
         WHERE tenant_id = $1 AND source_model_invocation_id = $2",
    )
    .bind(foreign_tenant.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("count cross-tenant Audit ghosts");
    assert_eq!(foreign_rows, 0, "cross-tenant source must fail closed");
}

pub(super) async fn assert_no_tool_audit(
    pool: &PgPool,
    tenant_id: TenantId,
    invocation_id: InvocationId,
) {
    let audit_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM security_audit.records
         WHERE tenant_id = $1 AND action = 10
           AND source_tool_invocation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("count rolled-back Tool Audit ghosts");
    assert_eq!(audit_rows, 0, "failed business commit must roll Audit back");
}

pub(super) async fn assert_succeeded_tool_audit(
    pool: &PgPool,
    tenant_id: TenantId,
    invocation_id: InvocationId,
) {
    let (audit_rows, content_rows, trace): (i64, i64, String) = sqlx::query_as(
        "SELECT count(DISTINCT audit.audit_record_id),
                count(*) FILTER (
                    WHERE audit.detail_content_id IS NOT NULL
                       OR resource.content_id IS NOT NULL
                ),
                min(encode(audit.trace_id, 'hex'))
         FROM security_audit.records AS audit
         JOIN tool.tool_invocations AS invocation
           ON invocation.tenant_id = audit.tenant_id
          AND invocation.invocation_id = audit.source_tool_invocation_id
         JOIN runtime.runs AS run
           ON run.tenant_id = invocation.tenant_id
          AND run.run_id = invocation.run_id
         LEFT JOIN security_audit.resource_refs AS resource
           ON resource.tenant_id = audit.tenant_id
          AND resource.audit_record_id = audit.audit_record_id
         WHERE audit.tenant_id = $1
           AND audit.action = 10
           AND audit.outcome = 1
           AND audit.source_tool_invocation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("join Tool Audit to Invocation and Run");
    assert_eq!(audit_rows, 1, "Tool completion replay is exactly once");
    assert_eq!(
        content_rows, 0,
        "Tool result and provider ref must not enter Audit"
    );
    assert_eq!(trace, "00000000000000000000000000000001");
}
