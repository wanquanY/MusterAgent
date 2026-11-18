use std::sync::Arc;

#[path = "support/running_loop.rs"]
mod running_loop;

#[path = "postgres_tool_security_audit/clock_skew.rs"]
mod clock_skew;

use musteragent_content_ref::{MediaType, SchemaRef, Sha256};
use musteragent_identity::{
    Actor, CapabilityProviderId, CorrelationId, IdempotencyKey, InvocationId, PrincipalId,
    PrincipalKind, TenantId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_loop::AgentLoopJournal;
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresContentStore, PostgresFencedToolInvocationJournal,
    PostgresSecurityAuditError, PostgresSecurityAuditPipeline, PostgresSecurityAuditTestStore,
    PostgresTransactionError,
};
use musteragent_runtime_audit::{
    ApprovedRetentionPolicies, SecurityAuditAppendDisposition, SecurityAuditFact,
    SecurityAuditProducer, SecurityAuditProducerContext, SecurityAuditTraceId,
};
use musteragent_runtime_capability::{
    CapabilityResult, FencedToolInvocationJournal, ToolExecutionStart, ToolStartMode,
    ToolTerminalOutcome,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    DataRetentionPolicyRef, ErrorCode, ErrorInfo, OpaqueExternalRef, RetryDisposition,
    SafeDisplayText,
};
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract};
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::ProtobufMeteringPublicationEncoder;

#[derive(Debug, Copy, Clone)]
struct RunningToolSpec<'a> {
    invocation_id: InvocationId,
    key: &'a str,
    effect: Effect,
    reconciliation_ref: Option<&'a str>,
}

#[tokio::test]
async fn wrong_actor_cannot_occupy_tool_audit_identity_before_canonical_owner() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
    );
    let invocation_id = InvocationId::generate();
    begin_running(
        &pool,
        &journal,
        content,
        fixture,
        &attempt,
        RunningToolSpec {
            invocation_id,
            key: "audit-identity-occupancy",
            effect: Effect::Read,
            reconciliation_ref: None,
        },
    )
    .await;
    let finished_at = running_loop::database_now(&pool).await;
    sqlx::query(
        "UPDATE tool.tool_invocations
         SET status = 4, finished_at_ms = $2
         WHERE invocation_id = $1 AND status = 3",
    )
    .bind(invocation_id.into_uuid())
    .bind(finished_at.unix_millis())
    .execute(&pool)
    .await
    .expect("seed terminal source fact without Audit");
    let (correlation_id, traceparent): (uuid::Uuid, String) = sqlx::query_as(
        "SELECT admitted.correlation_id, run.traceparent
         FROM runtime.runs AS run
         JOIN event_store.ledger_records AS admitted
           ON admitted.tenant_id = run.tenant_id
          AND admitted.conversation_id = run.conversation_id
          AND admitted.conversation_seq = run.admitted_sequence
         WHERE run.tenant_id = $1 AND run.run_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("load canonical Run context");
    let traceparent =
        musteragent_runtime_contract::TraceParent::new(traceparent).expect("canonical traceparent");
    let wrong_proposal = SecurityAuditProducer::new(
        DataRetentionPolicyRef::new("security.audit.standard@1").expect("retention policy"),
    )
    .propose(
        SecurityAuditProducerContext {
            actor: Actor {
                tenant_id: fixture.tenant_id,
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::System,
            },
            correlation_id: CorrelationId::from_uuid(correlation_id).expect("correlation"),
            trace_id: Some(
                SecurityAuditTraceId::from_trace_context(
                    &musteragent_runtime_contract::TraceContext {
                        traceparent,
                        tracestate: None,
                    },
                )
                .expect("trace identity"),
            ),
            occurred_at: finished_at,
        },
        SecurityAuditFact::ToolInvocationTerminal {
            run_id: fixture.run_id,
            invocation_id,
            outcome: musteragent_runtime_audit::SecurityAuditOutcome::Succeeded,
        },
    )
    .expect("construct adversarial typed proposal");
    let policies =
        ApprovedRetentionPolicies::new([
            DataRetentionPolicyRef::new("security.audit.standard@1").expect("retention policy")
        ])
        .expect("approved policies");
    let raw = PostgresSecurityAuditTestStore::new(pool.clone(), Arc::new(policies));
    let wrong_actor_result = raw.append(wrong_proposal).await;
    assert!(
        matches!(
            wrong_actor_result,
            Err(PostgresSecurityAuditError::Transaction(
                PostgresTransactionError::Database(_)
            ))
        ),
        "unexpected wrong-actor result: {wrong_actor_result:?}"
    );
    let rows_after_attack: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM security_audit.records
         WHERE tenant_id = $1 AND source_tool_invocation_id = $2 AND action = 10",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count rejected occupancy");
    assert_eq!(rows_after_attack, 0, "rejected actor must leave no ghost");

    let pipeline = PostgresSecurityAuditPipeline::standard(pool.clone());
    let applied = pipeline
        .verify_tool_fact(fixture.tenant_id, invocation_id)
        .await
        .expect("canonical owner claims deterministic identity");
    assert_eq!(applied.disposition, SecurityAuditAppendDisposition::Applied);
    let duplicate = pipeline
        .verify_tool_fact(fixture.tenant_id, invocation_id)
        .await
        .expect("canonical replay");
    assert_eq!(
        duplicate.disposition,
        SecurityAuditAppendDisposition::Duplicate
    );
    let actor: (uuid::Uuid, i16) = sqlx::query_as(
        "SELECT actor_principal_id, actor_principal_kind
         FROM security_audit.records
         WHERE tenant_id = $1 AND source_tool_invocation_id = $2 AND action = 10",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read canonical actor");
    assert_eq!(
        actor,
        (fixture.executor_principal_id.into_uuid(), 3),
        "Tool actor must come from the persisted Run executor"
    );
}

#[tokio::test]
async fn indeterminate_observation_and_reconciled_terminal_have_distinct_atomic_audit_facts() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
    );

    let succeeded_id = InvocationId::generate();
    begin_running(
        &pool,
        &journal,
        Arc::clone(&content),
        fixture,
        &attempt,
        RunningToolSpec {
            invocation_id: succeeded_id,
            key: "query-before-retry-succeeded",
            effect: Effect::WriteExternal,
            reconciliation_ref: Some("reconcile-provider-success"),
        },
    )
    .await;
    let mut observation = indeterminate("reconcile-provider-success");
    let ToolTerminalOutcome::Succeeded(mut diagnostic) = success_result(
        Arc::clone(&content),
        fixture.tenant_id,
        b"{\"native_error\":\"adapter failed\"}",
    )
    .await
    else {
        unreachable!()
    };
    diagnostic.provider_request_ref =
        Some(OpaqueExternalRef::new("original-native-call-17").expect("provider reference"));
    if let ToolTerminalOutcome::Indeterminate {
        observed_error,
        diagnostic_result,
        ..
    } = &mut observation
    {
        let ToolTerminalOutcome::Failed { error, .. } = failed_outcome() else {
            unreachable!()
        };
        *observed_error = Some(error);
        *diagnostic_result = Some(diagnostic);
    }
    let observed_at = running_loop::database_now(&pool).await;
    let persisted = journal
        .complete(&attempt, succeeded_id, observation.clone(), observed_at)
        .await
        .expect("persist indeterminate observation");
    assert_eq!(persisted.recorded_outcome.as_ref(), Some(&observation));
    journal
        .complete(
            &attempt,
            succeeded_id,
            observation,
            UtcTimestamp::from_unix_millis(observed_at.unix_millis() + 77),
        )
        .await
        .expect("replay observation from its canonical timestamp");

    let succeeded = success_result(Arc::clone(&content), fixture.tenant_id, b"reconciled").await;
    let terminal_at = running_loop::database_now(&pool).await;
    journal
        .complete(&attempt, succeeded_id, succeeded.clone(), terminal_at)
        .await
        .expect("reconciliation proved success");
    journal
        .complete(
            &attempt,
            succeeded_id,
            succeeded,
            UtcTimestamp::from_unix_millis(terminal_at.unix_millis() + 91),
        )
        .await
        .expect("replay terminal from its canonical timestamp");
    assert_audit_pair(&pool, fixture.tenant_id, fixture.run_id, succeeded_id).await;

    let failed_id = InvocationId::generate();
    begin_running(
        &pool,
        &journal,
        Arc::clone(&content),
        fixture,
        &attempt,
        RunningToolSpec {
            invocation_id: failed_id,
            key: "query-before-retry-failed",
            effect: Effect::WriteExternal,
            reconciliation_ref: Some("reconcile-provider-failure"),
        },
    )
    .await;
    journal
        .complete(
            &attempt,
            failed_id,
            indeterminate("reconcile-provider-failure"),
            running_loop::database_now(&pool).await,
        )
        .await
        .expect("persist second indeterminate observation");

    sqlx::raw_sql(
        "CREATE FUNCTION security_audit.reject_tool_terminal()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.action = 10 THEN
                 RAISE EXCEPTION 'injected Tool terminal Audit failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER reject_tool_terminal
         BEFORE INSERT ON security_audit.records
         FOR EACH ROW EXECUTE FUNCTION security_audit.reject_tool_terminal();",
    )
    .execute(&pool)
    .await
    .expect("install Audit failure trigger");
    let before_sequence: String =
        sqlx::query_scalar("SELECT last_run_sequence::text FROM runtime.runs WHERE run_id = $1")
            .bind(fixture.run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("read sequence before failure");
    let failed = failed_outcome();
    assert!(
        journal
            .complete(
                &attempt,
                failed_id,
                failed.clone(),
                running_loop::database_now(&pool).await,
            )
            .await
            .is_err(),
        "Audit failure must reject the terminal transition"
    );
    let rolled_back: (i16, String, i64) = sqlx::query_as(
        "SELECT invocation.status, run.last_run_sequence::text,
                count(audit.audit_record_id) FILTER (WHERE audit.action = 10)
         FROM tool.tool_invocations AS invocation
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         LEFT JOIN security_audit.records AS audit
           ON audit.tenant_id = invocation.tenant_id
          AND audit.source_tool_invocation_id = invocation.invocation_id
         WHERE invocation.invocation_id = $1
         GROUP BY invocation.status, run.last_run_sequence",
    )
    .bind(failed_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rolled-back terminal");
    assert_eq!(rolled_back, (6, before_sequence, 0));
    sqlx::raw_sql(
        "DROP TRIGGER reject_tool_terminal ON security_audit.records;
         DROP FUNCTION security_audit.reject_tool_terminal();",
    )
    .execute(&pool)
    .await
    .expect("remove Audit failure trigger");

    let restarted = journal
        .start_with_reconciliation_identity(
            &attempt,
            failed_id,
            ToolStartMode::ReconciliationProvenNotApplied,
            Some(
                OpaqueExternalRef::new("reconcile-provider-failure")
                    .expect("reconciliation identity"),
            ),
            ToolExecutionStart {
                started_at: running_loop::database_now(&pool).await,
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
            },
        )
        .await
        .expect("reconciliation proved the prior call was not applied");
    assert_eq!(
        restarted
            .prebound_reconciliation_ref
            .as_ref()
            .map(OpaqueExternalRef::as_str),
        Some("reconcile-provider-failure"),
        "ProvenNotApplied redispatch must preserve the original Provider identity"
    );
    journal
        .complete(
            &attempt,
            failed_id,
            failed,
            running_loop::database_now(&pool).await,
        )
        .await
        .expect("replayed call reached failed terminal");
    assert_audit_pair(&pool, fixture.tenant_id, fixture.run_id, failed_id).await;

    let pipeline = PostgresSecurityAuditPipeline::standard(pool.clone());
    assert!(matches!(
        pipeline
            .verify_tool_fact(TenantId::generate(), succeeded_id)
            .await,
        Err(PostgresSecurityAuditError::FactAuthorityMismatch)
    ));
    assert!(matches!(
        pipeline
            .verify_tool_fact(fixture.tenant_id, InvocationId::generate())
            .await,
        Err(PostgresSecurityAuditError::FactAuthorityMismatch)
    ));
}

#[tokio::test]
async fn repeated_indeterminate_fact_does_not_block_run_terminal_sweep() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let audit = Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone()));
    let tool_journal = PostgresFencedToolInvocationJournal::with_security_audit(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys.clone(),
        Arc::clone(&audit),
    );
    let invocation_id = InvocationId::generate();
    let reconciliation_ref = "repeat-indeterminate-before-run-terminal";
    begin_running(
        &pool,
        &tool_journal,
        Arc::clone(&content),
        fixture,
        &attempt,
        RunningToolSpec {
            invocation_id,
            key: "repeat-indeterminate-before-run-terminal",
            effect: Effect::WriteExternal,
            reconciliation_ref: Some(reconciliation_ref),
        },
    )
    .await;
    let first_observed_at = running_loop::database_now(&pool).await;
    tool_journal
        .complete(
            &attempt,
            invocation_id,
            indeterminate(reconciliation_ref),
            first_observed_at,
        )
        .await
        .expect("persist the first indeterminate fact");
    tool_journal
        .start_with_reconciliation_identity(
            &attempt,
            invocation_id,
            ToolStartMode::ReconciliationProvenNotApplied,
            Some(OpaqueExternalRef::new(reconciliation_ref).expect("reconciliation identity")),
            ToolExecutionStart {
                started_at: UtcTimestamp::from_unix_millis(first_observed_at.unix_millis() + 1),
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
            },
        )
        .await
        .expect("restart after reconciliation proved no external effect");

    let loop_journal = PostgresAgentLoopJournal::with_security_audit(
        pool.clone(),
        content,
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufMeteringPublicationEncoder),
        keys,
        running_loop::recovery_keyring(),
        audit,
    );
    loop_journal
        .fail(
            &attempt,
            ErrorCode::DependencyUnavailable,
            false,
            None,
            std::num::NonZeroU32::MIN.into(),
        )
        .await
        .expect("a repeated indeterminate fact must not roll back Run terminalization");

    let state: (i16, i16, i64, i64) = sqlx::query_as(
        "SELECT run.status, invocation.status,
                count(audit.audit_record_id), min(audit.occurred_at_ms)
         FROM runtime.runs AS run
         JOIN tool.tool_invocations AS invocation ON invocation.run_id = run.run_id
         JOIN security_audit.records AS audit
           ON audit.tenant_id = invocation.tenant_id
          AND audit.source_tool_invocation_id = invocation.invocation_id
          AND audit.action = 12
         WHERE run.run_id = $1 AND invocation.invocation_id = $2
         GROUP BY run.status, invocation.status",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read repeated indeterminate terminal state");
    assert_eq!(state, (6, 6, 1, first_observed_at.unix_millis()));
}

#[tokio::test]
async fn run_terminal_sweep_audits_active_tool_in_its_atomic_terminal_transaction() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let audit = Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone()));
    let tool_journal = PostgresFencedToolInvocationJournal::with_security_audit(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys.clone(),
        Arc::clone(&audit),
    );
    let invocation_id = InvocationId::generate();
    begin_running(
        &pool,
        &tool_journal,
        Arc::clone(&content),
        fixture,
        &attempt,
        RunningToolSpec {
            invocation_id,
            key: "run-terminal-sweep",
            effect: Effect::Read,
            reconciliation_ref: None,
        },
    )
    .await;
    let loop_journal = PostgresAgentLoopJournal::with_security_audit(
        pool.clone(),
        content,
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufMeteringPublicationEncoder),
        keys,
        running_loop::recovery_keyring(),
        audit,
    );

    sqlx::raw_sql(
        "CREATE FUNCTION security_audit.reject_swept_tool_terminal()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.action = 10 THEN
                 RAISE EXCEPTION 'injected swept Tool Audit failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER reject_swept_tool_terminal
         BEFORE INSERT ON security_audit.records
         FOR EACH ROW EXECUTE FUNCTION security_audit.reject_swept_tool_terminal();",
    )
    .execute(&pool)
    .await
    .expect("install sweep Audit failure");
    assert!(
        loop_journal
            .fail(
                &attempt,
                ErrorCode::DependencyUnavailable,
                false,
                None,
                std::num::NonZeroU32::MIN.into()
            )
            .await
            .is_err()
    );
    let rolled_back: (i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, invocation.status,
                (SELECT count(*) FROM security_audit.records
                 WHERE source_tool_invocation_id = $2 AND action = 10)
         FROM runtime.runs AS run
         JOIN tool.tool_invocations AS invocation ON invocation.run_id = run.run_id
         WHERE run.run_id = $1 AND invocation.invocation_id = $2",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rolled-back Run sweep");
    assert_eq!(rolled_back, (2, 3, 0));
    sqlx::raw_sql(
        "DROP TRIGGER reject_swept_tool_terminal ON security_audit.records;
         DROP FUNCTION security_audit.reject_swept_tool_terminal();",
    )
    .execute(&pool)
    .await
    .expect("remove sweep Audit failure");
    loop_journal
        .fail(
            &attempt,
            ErrorCode::DependencyUnavailable,
            false,
            None,
            std::num::NonZeroU32::MIN.into(),
        )
        .await
        .expect("terminal Run sweep");
    let committed: (i16, i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, invocation.status, audit.outcome, count(resource.resource_id)
         FROM runtime.runs AS run
         JOIN tool.tool_invocations AS invocation ON invocation.run_id = run.run_id
         JOIN security_audit.records AS audit
           ON audit.tenant_id = invocation.tenant_id
          AND audit.source_tool_invocation_id = invocation.invocation_id
          AND audit.action = 10
         JOIN security_audit.resource_refs AS resource
           ON resource.tenant_id = audit.tenant_id
          AND resource.audit_record_id = audit.audit_record_id
          AND resource.resource_kind = 4 AND resource.resource_id = run.run_id
         WHERE run.run_id = $1 AND invocation.invocation_id = $2
         GROUP BY run.status, invocation.status, audit.outcome",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read swept Tool Audit");
    assert_eq!(committed, (6, 5, 3, 1));
}

async fn begin_running(
    pool: &sqlx::PgPool,
    journal: &PostgresFencedToolInvocationJournal,
    content: Arc<dyn ContentStore>,
    fixture: running_loop::Fixture,
    attempt: &musteragent_run::AttemptContext,
    spec: RunningToolSpec<'_>,
) {
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: br#"{"query":"audit"}"#.to_vec(),
        })
        .await
        .expect("store request");
    let invocation = ToolInvocation {
        invocation_id: spec.invocation_id,
        run_id: fixture.run_id,
        requested_by_attempt_id: attempt.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new(format!("call-{}", spec.key)).expect("Tool call id"),
        tool_key: ToolKey::from_versioned_key(VersionedKey::new("web.search@1").expect("Tool key")),
        capability_provider_id: CapabilityProviderId::generate(),
        status: ToolInvocationStatus::Requested,
        effect: spec.effect,
        approval: ApprovalRequirement::Never,
        idempotency_key: IdempotencyKey::new(spec.key).expect("idempotency key"),
        request_ref,
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
    };
    let retry_contract = if spec.reconciliation_ref.is_some() {
        ToolRetryContract::QueryBeforeRetry
    } else {
        ToolRetryContract::ReadOnly
    };
    journal
        .load_or_begin(attempt, invocation, retry_contract)
        .await
        .expect("begin Tool");
    let started = journal
        .start_with_reconciliation_identity(
            attempt,
            spec.invocation_id,
            ToolStartMode::Initial,
            spec.reconciliation_ref
                .map(|reference| OpaqueExternalRef::new(reference).expect("reconciliation ref")),
            ToolExecutionStart {
                started_at: running_loop::database_now(pool).await,
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
            },
        )
        .await
        .expect("start Tool");
    assert_eq!(
        started
            .prebound_reconciliation_ref
            .as_ref()
            .map(OpaqueExternalRef::as_str),
        spec.reconciliation_ref,
        "Tool=Running must expose exactly the identity sealed in its start transaction"
    );
}

fn indeterminate(reference: &str) -> ToolTerminalOutcome {
    ToolTerminalOutcome::Indeterminate {
        diagnostic_result: None,
        observed_error: None,
        reason: SafeDisplayText::new("Provider outcome requires reconciliation")
            .expect("safe reason"),
        reconciliation_ref: OpaqueExternalRef::new(reference).expect("reconciliation ref"),
    }
}

async fn success_result(
    content: Arc<dyn ContentStore>,
    tenant_id: musteragent_identity::TenantId,
    bytes: &[u8],
) -> ToolTerminalOutcome {
    let result_ref = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: bytes.to_vec(),
        })
        .await
        .expect("store result");
    ToolTerminalOutcome::Succeeded(CapabilityResult {
        result_hash: result_ref.sha256,
        result_ref,
        result_schema: SchemaRef {
            schema_key: VersionedKey::new("web.search.output@1").expect("schema key"),
            schema_hash: Sha256::digest(b"web.search.output.schema.v1"),
        },
        provider_request_ref: None,
    })
}

fn failed_outcome() -> ToolTerminalOutcome {
    ToolTerminalOutcome::Failed {
        error: ErrorInfo {
            code: ErrorCode::DependencyUnavailable,
            category: ErrorCode::DependencyUnavailable.category(),
            safe_message: SafeDisplayText::new("Provider failed after safe retry")
                .expect("safe error"),
            retryable: false,
            dependency_ref: None,
            diagnostic_ref: None,
        },
        retry_disposition: RetryDisposition::DoNotRetry,
        diagnostic_result: None,
    }
}

async fn assert_audit_pair(
    pool: &sqlx::PgPool,
    tenant_id: musteragent_identity::TenantId,
    run_id: musteragent_identity::RunId,
    invocation_id: InvocationId,
) {
    let rows: Vec<(i16, i64, i64)> = sqlx::query_as(
        "SELECT audit.action,
                count(DISTINCT audit.audit_record_id),
                count(DISTINCT resource.resource_id) FILTER (
                    WHERE resource.resource_kind = 4 AND resource.resource_id = $3
                )
         FROM security_audit.records AS audit
         JOIN security_audit.resource_refs AS resource
           ON resource.tenant_id = audit.tenant_id
          AND resource.audit_record_id = audit.audit_record_id
         WHERE audit.tenant_id = $1 AND audit.source_tool_invocation_id = $2
           AND audit.action IN (10, 12)
         GROUP BY audit.action ORDER BY audit.action",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_all(pool)
    .await
    .expect("read Tool audit pair");
    assert_eq!(rows, vec![(10, 1, 1), (12, 1, 1)]);
}
