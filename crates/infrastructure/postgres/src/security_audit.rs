use std::collections::HashMap;
use std::sync::Arc;

use musteragent_identity::{
    Actor, AuditRecordId, CommandId, ConversationId, CorrelationId, ErasureJobId, InteractionId,
    InvocationId, PrincipalId, RunId, TenantId, UtcTimestamp,
};
use musteragent_runtime_audit::{
    ApprovalAuditDecision, ApprovedRetentionPolicies, DataRetentionPolicyRef,
    ProposedSecurityAuditRecord, SecurityAuditAction, SecurityAuditAppendDisposition,
    SecurityAuditAppendReceipt, SecurityAuditContractError, SecurityAuditCursor, SecurityAuditFact,
    SecurityAuditOutcome, SecurityAuditPage, SecurityAuditProducer, SecurityAuditProducerContext,
    SecurityAuditQuery, SecurityAuditReader, SecurityAuditRecord, SecurityAuditSourceRef,
    SecurityAuditTraceId, principal_kind_from_code, resource_fingerprint,
};
use musteragent_runtime_contract::ResourceRef;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row as _};
use thiserror::Error;

use crate::typed_resource::{ContentColumns, ResourceColumns, decode_content, decode_resource};

#[derive(Clone)]
pub struct PostgresSecurityAuditStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    policies: Arc<ApprovedRetentionPolicies>,
}

#[derive(Clone)]
pub struct PostgresSecurityAuditReader {
    store: PostgresSecurityAuditStore,
}

impl PostgresSecurityAuditReader {
    #[must_use]
    pub fn standard(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let retention = DataRetentionPolicyRef::new("security.audit.standard@1")
            .unwrap_or_else(|_| unreachable!("the built-in Audit retention policy is valid"));
        let policies = Arc::new(
            ApprovedRetentionPolicies::new([retention])
                .unwrap_or_else(|_| unreachable!("the built-in Audit policy set is non-empty")),
        );
        Self {
            store: PostgresSecurityAuditStore::new(database, policies),
        }
    }
}

/// Single production producer/writer injected by a composition root. It owns
/// the closed fact mapping and both standalone-denial and caller-transaction
/// append paths; there is no best-effort or no-op mode.
#[derive(Clone)]
pub struct PostgresSecurityAuditPipeline {
    store: PostgresSecurityAuditStore,
    producer: SecurityAuditProducer,
}

impl PostgresSecurityAuditPipeline {
    #[must_use]
    pub fn standard(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let retention = DataRetentionPolicyRef::new("security.audit.standard@1")
            .unwrap_or_else(|_| unreachable!("the built-in Audit retention policy is valid"));
        let policies = Arc::new(
            ApprovedRetentionPolicies::new([retention.clone()])
                .unwrap_or_else(|_| unreachable!("the built-in Audit policy set is non-empty")),
        );
        Self {
            store: PostgresSecurityAuditStore::new(database, policies),
            producer: SecurityAuditProducer::new(retention),
        }
    }

    async fn append_canonical_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        canonical: CanonicalAuditFact,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let proposal = self.producer.propose(canonical.context, canonical.fact)?;
        self.store
            .append_in_transaction(transaction, proposal)
            .await
    }

    async fn write_canonical(
        &self,
        load: impl for<'a> FnOnce(
            &'a mut sqlx::PgConnection,
        ) -> musteragent_runtime_contract::ApplicationFuture<
            'a,
            Result<CanonicalAuditFact, PostgresSecurityAuditError>,
        >,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let mut transaction = self
            .store
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::SecurityAudit)
            .await?;
        let canonical = load(&mut transaction).await?;
        let receipt = self
            .append_canonical_in_transaction(&mut transaction, canonical)
            .await?;
        transaction.commit().await?;
        Ok(receipt)
    }

    pub(crate) async fn append_admission_allowed_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let canonical = load_admission_allowed(transaction, tenant_id, run_id).await?;
        self.append_canonical_in_transaction(transaction, canonical)
            .await
    }

    pub(crate) async fn write_admission_allowed(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        self.write_canonical(|transaction| {
            Box::pin(load_admission_allowed(transaction, tenant_id, run_id))
        })
        .await
    }

    pub(crate) async fn append_admission_denied_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        command_id: CommandId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let canonical = load_admission_denied(transaction, tenant_id, command_id).await?;
        let receipt = self
            .append_canonical_in_transaction(transaction, canonical)
            .await?;
        let updated = sqlx::query(
            "UPDATE security_audit.admission_denials
             SET audit_record_id = $3
             WHERE tenant_id = $1 AND command_id = $2
               AND (audit_record_id IS NULL OR audit_record_id = $3)",
        )
        .bind(tenant_id.into_uuid())
        .bind(command_id.into_uuid())
        .bind(receipt.record.proposal.audit_record_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(PostgresSecurityAuditError::FactAuthorityMismatch);
        }
        Ok(receipt)
    }

    pub(crate) async fn append_model_terminal_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let canonical = load_model_terminal(transaction, tenant_id, invocation_id).await?;
        self.append_canonical_in_transaction(transaction, canonical)
            .await
    }

    pub(crate) async fn append_tool_fact_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let canonical = load_tool_fact(transaction, tenant_id, invocation_id).await?;
        self.append_canonical_in_transaction(transaction, canonical)
            .await
    }

    pub(crate) async fn write_tool_fact(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        self.write_canonical(|transaction| {
            Box::pin(load_tool_fact(transaction, tenant_id, invocation_id))
        })
        .await
    }

    pub(crate) async fn append_interaction_response_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        interaction_id: InteractionId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let canonical = load_interaction_response(transaction, tenant_id, interaction_id).await?;
        self.append_canonical_in_transaction(transaction, canonical)
            .await
    }

    pub(crate) async fn write_interaction_response(
        &self,
        tenant_id: TenantId,
        interaction_id: InteractionId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        self.write_canonical(|transaction| {
            Box::pin(load_interaction_response(
                transaction,
                tenant_id,
                interaction_id,
            ))
        })
        .await
    }

    /// Credential authorization is owned by Cloud Worker, so this is the one
    /// cross-crate write seam. It accepts only the canonical invocation ID;
    /// actor, Run, correlation, trace and occurrence time come from PostgreSQL.
    pub async fn append_credential_access_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let canonical = load_credential_access(transaction, tenant_id, invocation_id).await?;
        self.append_canonical_in_transaction(transaction, canonical)
            .await
    }

    pub(crate) async fn append_erasure_completed_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let canonical = load_erasure_completed(transaction, tenant_id, erasure_job_id).await?;
        self.append_canonical_in_transaction(transaction, canonical)
            .await
    }

    pub(crate) async fn write_erasure_completed(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        self.write_canonical(|transaction| {
            Box::pin(load_erasure_completed(
                transaction,
                tenant_id,
                erasure_job_id,
            ))
        })
        .await
    }

    /// Conformance-only entrypoints exercise the canonical loaders without
    /// exposing caller-authored actor/context/fact material in production.
    #[cfg(feature = "test-support")]
    pub async fn verify_model_terminal(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        self.write_canonical(|transaction| {
            Box::pin(load_model_terminal(transaction, tenant_id, invocation_id))
        })
        .await
    }

    #[cfg(feature = "test-support")]
    pub async fn verify_tool_fact(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        self.write_tool_fact(tenant_id, invocation_id).await
    }

    #[cfg(feature = "test-support")]
    pub async fn verify_erasure_completed(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        self.write_erasure_completed(tenant_id, erasure_job_id)
            .await
    }
}

struct CanonicalAuditFact {
    context: SecurityAuditProducerContext,
    fact: SecurityAuditFact,
}

fn canonical_context(
    tenant_id: TenantId,
    principal_id: uuid::Uuid,
    principal_kind: i16,
    correlation_id: uuid::Uuid,
    traceparent: Option<String>,
    occurred_at_ms: i64,
) -> Result<SecurityAuditProducerContext, PostgresSecurityAuditError> {
    let trace_id = traceparent
        .map(|value| {
            let traceparent = musteragent_runtime_contract::TraceParent::new(value)
                .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Run trace"))?;
            SecurityAuditTraceId::from_trace_context(&musteragent_runtime_contract::TraceContext {
                traceparent,
                tracestate: None,
            })
            .map_err(PostgresSecurityAuditError::from)
        })
        .transpose()?;
    Ok(SecurityAuditProducerContext {
        actor: Actor {
            tenant_id,
            principal_id: PrincipalId::from_uuid(principal_id)
                .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid principal"))?,
            principal_kind: principal_kind_from_code(principal_kind)
                .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid principal kind"))?,
        },
        correlation_id: CorrelationId::from_uuid(correlation_id)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid correlation"))?,
        trace_id,
        occurred_at: UtcTimestamp::from_unix_millis(occurred_at_ms),
    })
}

async fn load_admission_allowed(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<CanonicalAuditFact, PostgresSecurityAuditError> {
    let row: Option<(uuid::Uuid, uuid::Uuid, i16, uuid::Uuid, i64, String)> = sqlx::query_as(
        "SELECT run.conversation_id,
                CASE WHEN dialogue_session.owner_id IS NOT NULL THEN dialogue_session.owner_id
                     WHEN run.activity_kind = 5 THEN conversation.created_by
                     ELSE requester.principal_ref END AS principal_id,
                CASE WHEN dialogue_session.owner_id IS NOT NULL THEN 1::SMALLINT
                     WHEN run.activity_kind = 5 THEN 3::SMALLINT
                     ELSE 1::SMALLINT END AS principal_kind,
                admitted.record_id, admitted.occurred_at_ms, run.traceparent
         FROM runtime.runs AS run
         JOIN conversation.conversations AS conversation
           ON conversation.tenant_id = run.tenant_id
          AND conversation.conversation_id = run.conversation_id
         LEFT JOIN agent.dialogue_sessions AS dialogue_session
           ON dialogue_session.tenant_id = run.tenant_id
          AND dialogue_session.conversation_id = run.conversation_id
          AND dialogue_session.owner_id = conversation.created_by
          AND dialogue_session.conversation_purpose = conversation.purpose
          AND conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution')
          AND conversation.kind = 4
          AND conversation.execution_owner_participant_id = run.executor_participant_id
          AND EXISTS (SELECT 1 FROM conversation.participants AS internal_requester
                       WHERE internal_requester.tenant_id = run.tenant_id
                         AND internal_requester.conversation_id = run.conversation_id
                         AND internal_requester.participant_id = run.requested_by_participant_id
                         AND internal_requester.kind = 3
                         AND ((dialogue_session.subject_kind='intent' AND internal_requester.service_ref='musteragent.dialogue-intent@1')
                           OR (dialogue_session.subject_kind='work_item' AND internal_requester.service_ref='musteragent.work-item@1')
                           OR (dialogue_session.subject_kind='work_item_report' AND internal_requester.service_ref='musteragent.work-item-report@1')
                           OR (dialogue_session.subject_kind='goal' AND dialogue_session.source_goal_id IS NOT NULL AND internal_requester.service_ref='musteragent.owner-goal@1')))
         LEFT JOIN conversation.participants AS requester
           ON requester.tenant_id = run.tenant_id
          AND requester.conversation_id = run.conversation_id
          AND requester.participant_id = run.requested_by_participant_id
          AND requester.kind = 1 AND requester.principal_ref IS NOT NULL
         JOIN event_store.ledger_records AS admitted
           ON admitted.tenant_id = run.tenant_id
          AND admitted.conversation_id = run.conversation_id
          AND admitted.conversation_seq = run.admitted_sequence
          AND admitted.record_kind = 1
          AND admitted.participant_id = run.requested_by_participant_id
         WHERE run.tenant_id = $1 AND run.run_id = $2
           AND (
                dialogue_session.owner_id IS NOT NULL
                OR
                (conversation.purpose = 'standard'
                 AND run.activity_kind = 5
                 AND conversation.kind = 4
                 AND conversation.created_by IS NOT NULL)
                OR
                (conversation.purpose = 'standard' AND run.activity_kind <> 5 AND requester.participant_id IS NOT NULL)
           )",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let (conversation_id, principal_id, principal_kind, record_id, occurred_at_ms, traceparent) =
        row.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?;
    let correlation_id: uuid::Uuid = sqlx::query_scalar(
        "SELECT correlation_id FROM event_store.ledger_records WHERE record_id = $1",
    )
    .bind(record_id)
    .fetch_one(&mut *transaction)
    .await?;
    Ok(CanonicalAuditFact {
        context: canonical_context(
            tenant_id,
            principal_id,
            principal_kind,
            correlation_id,
            Some(traceparent),
            occurred_at_ms,
        )?,
        fact: SecurityAuditFact::AdmissionAllowed {
            conversation_id: ConversationId::from_uuid(conversation_id)
                .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Conversation"))?,
            run_id,
            source_ledger_record_id: musteragent_identity::LedgerRecordId::from_uuid(record_id)
                .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Ledger source"))?,
        },
    })
}

async fn load_admission_denied(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    command_id: CommandId,
) -> Result<CanonicalAuditFact, PostgresSecurityAuditError> {
    let row: Option<(uuid::Uuid, uuid::Uuid, i16, uuid::Uuid, Vec<u8>, i64)> = sqlx::query_as(
        "SELECT conversation_id, actor_principal_id, actor_principal_kind,
                correlation_id, trace_id, occurred_at_ms
         FROM security_audit.admission_denials
         WHERE tenant_id = $1 AND command_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(command_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let (conversation_id, principal_id, principal_kind, correlation_id, trace_id, occurred_at_ms) =
        row.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?;
    Ok(CanonicalAuditFact {
        context: SecurityAuditProducerContext {
            actor: Actor {
                tenant_id,
                principal_id: PrincipalId::from_uuid(principal_id).map_err(|_| {
                    PostgresSecurityAuditError::CorruptState("invalid denial principal")
                })?,
                principal_kind: principal_kind_from_code(principal_kind).map_err(|_| {
                    PostgresSecurityAuditError::CorruptState("invalid denial principal kind")
                })?,
            },
            correlation_id: CorrelationId::from_uuid(correlation_id).map_err(|_| {
                PostgresSecurityAuditError::CorruptState("invalid denial correlation")
            })?,
            trace_id: Some(
                SecurityAuditTraceId::from_bytes(trace_id).map_err(|_| {
                    PostgresSecurityAuditError::CorruptState("invalid denial trace")
                })?,
            ),
            occurred_at: UtcTimestamp::from_unix_millis(occurred_at_ms),
        },
        fact: SecurityAuditFact::AdmissionDenied {
            command_id,
            conversation_id: ConversationId::from_uuid(conversation_id)
                .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Conversation"))?,
        },
    })
}

type RunFactAuthorityRow = (
    uuid::Uuid,
    uuid::Uuid,
    i16,
    uuid::Uuid,
    String,
    i16,
    Option<i64>,
);

async fn load_model_terminal(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
) -> Result<CanonicalAuditFact, PostgresSecurityAuditError> {
    let row: Option<RunFactAuthorityRow> = sqlx::query_as(
        "SELECT invocation.run_id,
                executor.actor_principal_id,
                (executor.actor_principal_kind)::SMALLINT,
                admitted.correlation_id, run.traceparent,
                invocation.status, invocation.finished_at_ms
         FROM runtime.model_invocations AS invocation
         JOIN runtime.attempts AS attempt
           ON attempt.tenant_id = invocation.tenant_id
          AND attempt.run_id = invocation.run_id
          AND attempt.attempt_id = invocation.attempt_id
         JOIN runtime.runs AS run
           ON run.tenant_id = invocation.tenant_id AND run.run_id = invocation.run_id
         JOIN security_audit.run_executor_authorities AS executor
              ON executor.tenant_id = run.tenant_id AND executor.run_id = run.run_id
         JOIN event_store.ledger_records AS admitted
           ON admitted.tenant_id = run.tenant_id
          AND admitted.conversation_id = run.conversation_id
          AND admitted.conversation_seq = run.admitted_sequence
          AND admitted.record_kind = 1
          AND admitted.participant_id = run.requested_by_participant_id
         WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2
           AND invocation.status IN (2, 3, 4)
           AND invocation.finished_at_ms IS NOT NULL",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let (run_id, principal_id, principal_kind, correlation_id, traceparent, status, occurred_at) =
        row.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?;
    let run_id = RunId::from_uuid(run_id)
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Run"))?;
    let outcome = match status {
        2 => SecurityAuditOutcome::Succeeded,
        3 => SecurityAuditOutcome::Failed,
        4 => SecurityAuditOutcome::Denied,
        _ => return Err(PostgresSecurityAuditError::FactAuthorityMismatch),
    };
    Ok(CanonicalAuditFact {
        context: canonical_context(
            tenant_id,
            principal_id,
            principal_kind,
            correlation_id,
            Some(traceparent),
            occurred_at.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?,
        )?,
        fact: SecurityAuditFact::ModelInvocationTerminal {
            run_id,
            invocation_id,
            outcome,
        },
    })
}

async fn load_tool_fact(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
) -> Result<CanonicalAuditFact, PostgresSecurityAuditError> {
    // An indeterminate Tool can be proven not applied, re-enter Running, and
    // later become indeterminate again. Security Audit intentionally owns one
    // canonical `(Tool invocation, indeterminate)` fact, so a replay must use
    // the first observation time that formed that fact. Every other authority
    // field is reconstructed again and the proposal fingerprint still rejects
    // any non-temporal identity drift.
    let row: Option<RunFactAuthorityRow> = sqlx::query_as(
        "SELECT invocation.run_id,
                executor.actor_principal_id,
                (executor.actor_principal_kind)::SMALLINT,
                admitted.correlation_id, run.traceparent,
                invocation.status,
                CASE WHEN invocation.status = 6
                     THEN COALESCE(
                         prior_indeterminate.occurred_at_ms,
                         invocation.indeterminate_observed_at_ms
                     )
                     ELSE invocation.finished_at_ms END
         FROM tool.tool_invocations AS invocation
         JOIN runtime.attempts AS attempt
           ON attempt.tenant_id = invocation.tenant_id
          AND attempt.run_id = invocation.run_id
          AND attempt.attempt_id = invocation.requested_by_attempt_id
         JOIN runtime.runs AS run
           ON run.tenant_id = invocation.tenant_id AND run.run_id = invocation.run_id
         JOIN security_audit.run_executor_authorities AS executor
              ON executor.tenant_id = run.tenant_id AND executor.run_id = run.run_id
         JOIN event_store.ledger_records AS admitted
           ON admitted.tenant_id = run.tenant_id
          AND admitted.conversation_id = run.conversation_id
          AND admitted.conversation_seq = run.admitted_sequence
          AND admitted.record_kind = 1
          AND admitted.participant_id = run.requested_by_participant_id
         LEFT JOIN security_audit.records AS prior_indeterminate
           ON prior_indeterminate.tenant_id = invocation.tenant_id
          AND prior_indeterminate.source_tool_invocation_id = invocation.invocation_id
          AND prior_indeterminate.action = 12
         WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2
           AND invocation.status IN (4, 5, 6, 7)",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let (run_id, principal_id, principal_kind, correlation_id, traceparent, status, occurred_at) =
        row.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?;
    let run_id = RunId::from_uuid(run_id)
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Run"))?;
    let fact = match status {
        4 => SecurityAuditFact::ToolInvocationTerminal {
            run_id,
            invocation_id,
            outcome: SecurityAuditOutcome::Succeeded,
        },
        5 => SecurityAuditFact::ToolInvocationTerminal {
            run_id,
            invocation_id,
            outcome: SecurityAuditOutcome::Failed,
        },
        7 => SecurityAuditFact::ToolInvocationTerminal {
            run_id,
            invocation_id,
            outcome: SecurityAuditOutcome::Denied,
        },
        6 => SecurityAuditFact::ToolInvocationIndeterminate {
            run_id,
            invocation_id,
        },
        _ => return Err(PostgresSecurityAuditError::FactAuthorityMismatch),
    };
    Ok(CanonicalAuditFact {
        context: canonical_context(
            tenant_id,
            principal_id,
            principal_kind,
            correlation_id,
            Some(traceparent),
            occurred_at.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?,
        )?,
        fact,
    })
}

async fn load_interaction_response(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    interaction_id: InteractionId,
) -> Result<CanonicalAuditFact, PostgresSecurityAuditError> {
    let row: Option<InteractionAuditAuthorityRow> = sqlx::query_as(
        "SELECT interaction.run_id AS run_id,
                response.responded_by AS principal_id,
                response.response_ledger_record_id AS source_id,
                record.correlation_id AS correlation_id,
                response.responded_at_ms AS occurred_at,
                run.traceparent AS traceparent,
                interaction.kind AS interaction_kind,
                response.approval_decision AS approval_decision
         FROM approval.interactions AS interaction
         JOIN approval.interaction_responses AS response
           ON response.interaction_id = interaction.interaction_id
          AND response.responded_by = interaction.responded_by
          AND response.responded_at_ms = interaction.responded_at_ms
         JOIN runtime.runs AS run
           ON run.tenant_id = interaction.tenant_id AND run.run_id = interaction.run_id
         JOIN conversation.participants AS responder
           ON responder.tenant_id = interaction.tenant_id
          AND responder.conversation_id = interaction.conversation_id
          AND responder.kind = 1 AND responder.principal_ref = response.responded_by
         JOIN event_store.ledger_records AS record
           ON record.tenant_id = interaction.tenant_id
          AND record.record_id = response.response_ledger_record_id
          AND record.run_id = interaction.run_id
          AND record.participant_id = responder.participant_id
          AND record.record_kind = 1
          AND record.occurred_at_ms = response.responded_at_ms
         JOIN event_store.domain_events AS response_event
           ON response_event.record_id = record.record_id
          AND response_event.payload_hash = record.payload_hash
          AND response_event.payload_hash = response.response_ledger_payload_hash
         WHERE interaction.tenant_id = $1 AND interaction.interaction_id = $2
           AND interaction.status = 2
           AND response.response_ledger_event_kind = 1",
    )
    .bind(tenant_id.into_uuid())
    .bind(interaction_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let row = row.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?;
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Run"))?;
    let source_ledger_record_id = musteragent_identity::LedgerRecordId::from_uuid(row.source_id)
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Ledger source"))?;
    let fact = if row.interaction_kind == 1 {
        SecurityAuditFact::ApprovalDecided {
            run_id,
            interaction_id,
            source_ledger_record_id,
            decision: match row.approval_decision {
                Some(1) => ApprovalAuditDecision::Approved,
                Some(2) => ApprovalAuditDecision::Denied,
                _ => return Err(PostgresSecurityAuditError::FactAuthorityMismatch),
            },
        }
    } else {
        SecurityAuditFact::InteractionResponded {
            run_id,
            interaction_id,
            source_ledger_record_id,
        }
    };
    Ok(CanonicalAuditFact {
        context: canonical_context(
            tenant_id,
            row.principal_id,
            1,
            row.correlation_id,
            Some(row.traceparent),
            row.occurred_at,
        )?,
        fact,
    })
}

#[derive(sqlx::FromRow)]
struct InteractionAuditAuthorityRow {
    run_id: uuid::Uuid,
    principal_id: uuid::Uuid,
    source_id: uuid::Uuid,
    correlation_id: uuid::Uuid,
    occurred_at: i64,
    traceparent: String,
    interaction_kind: i16,
    approval_decision: Option<i16>,
}

async fn load_credential_access(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
) -> Result<CanonicalAuditFact, PostgresSecurityAuditError> {
    let row: Option<(uuid::Uuid, uuid::Uuid, i16, uuid::Uuid, String, i64)> = sqlx::query_as(
        "SELECT invocation.run_id, grant_use.actor_principal_id,
                grant_use.actor_principal_kind, admitted.correlation_id,
                run.traceparent, grant_use.consumed_at_ms
         FROM runtime.model_invocations AS invocation
         JOIN runtime.execution_grant_invocations AS grant_use
           ON grant_use.invocation_id = invocation.invocation_id
         JOIN runtime.execution_grant_consumptions AS grant_authority
           ON grant_authority.grant_ref = grant_use.grant_ref
          AND grant_authority.tenant_id = invocation.tenant_id
          AND grant_authority.run_id = invocation.run_id
         JOIN runtime.runs AS run
           ON run.tenant_id = invocation.tenant_id AND run.run_id = invocation.run_id
         JOIN security_audit.run_executor_authorities AS executor
              ON executor.tenant_id = run.tenant_id AND executor.run_id = run.run_id
          AND (grant_use.actor_principal_id = executor.actor_principal_id
                AND grant_use.actor_principal_kind = executor.actor_principal_kind)
         JOIN event_store.ledger_records AS admitted
           ON admitted.tenant_id = run.tenant_id
          AND admitted.conversation_id = run.conversation_id
          AND admitted.conversation_seq = run.admitted_sequence
          AND admitted.record_kind = 1
          AND admitted.participant_id = run.requested_by_participant_id
         WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let (run_id, principal_id, principal_kind, correlation_id, traceparent, occurred_at) =
        row.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?;
    let run_id = RunId::from_uuid(run_id)
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Run"))?;
    Ok(CanonicalAuditFact {
        context: canonical_context(
            tenant_id,
            principal_id,
            principal_kind,
            correlation_id,
            Some(traceparent),
            occurred_at,
        )?,
        fact: SecurityAuditFact::CredentialAccessed {
            run_id,
            invocation_id,
        },
    })
}

async fn load_erasure_completed(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    erasure_job_id: ErasureJobId,
) -> Result<CanonicalAuditFact, PostgresSecurityAuditError> {
    let row: Option<(uuid::Uuid, i16, uuid::Uuid, i64)> = sqlx::query_as(
        "SELECT request.actor_principal_id, request.actor_principal_kind,
                receipt.erasure_receipt_id, receipt.completed_at_ms
         FROM privacy_erasure.requests AS request
         JOIN privacy_erasure.receipts AS receipt
           ON receipt.tenant_id = request.tenant_id
          AND receipt.erasure_job_id = request.erasure_job_id
         WHERE request.tenant_id = $1 AND request.erasure_job_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(erasure_job_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let (principal_id, principal_kind, receipt_id, occurred_at) =
        row.ok_or(PostgresSecurityAuditError::FactAuthorityMismatch)?;
    Ok(CanonicalAuditFact {
        context: canonical_context(
            tenant_id,
            principal_id,
            principal_kind,
            erasure_job_id.into_uuid(),
            None,
            occurred_at,
        )?,
        fact: SecurityAuditFact::DataErasureCompleted {
            erasure_job_id,
            erasure_receipt_id: musteragent_identity::ErasureReceiptId::from_uuid(receipt_id)
                .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid erasure receipt"))?,
        },
    })
}

impl PostgresSecurityAuditStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        policies: Arc<ApprovedRetentionPolicies>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            pool,
            database,
            policies,
        }
    }

    #[cfg(feature = "test-support")]
    pub async fn append(
        &self,
        proposal: ProposedSecurityAuditRecord,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::SecurityAudit)
            .await?;
        let receipt = self
            .append_in_transaction(&mut transaction, proposal)
            .await?;
        transaction.commit().await?;
        Ok(receipt)
    }

    /// Appends inside the source owner's transaction. The caller remains the
    /// sole commit authority, so a source rollback cannot leave a ghost audit.
    pub(crate) async fn append_in_transaction(
        &self,
        transaction: &mut sqlx::PgConnection,
        proposal: ProposedSecurityAuditRecord,
    ) -> Result<SecurityAuditAppendReceipt, PostgresSecurityAuditError> {
        self.policies.authorize(&proposal)?;
        let fingerprint = proposal.fingerprint()?;

        if let Some(source) = proposal.source_fact_ref {
            validate_source_tenant(transaction, proposal.tenant_id, source, proposal.trace_id)
                .await?;
        }

        let source_ledger_record_id = proposal.source_fact_ref.and_then(|source| match source {
            SecurityAuditSourceRef::LedgerRecord(value) => Some(value.into_uuid()),
            SecurityAuditSourceRef::ErasureJob(_) => None,
            SecurityAuditSourceRef::ModelInvocation(_)
            | SecurityAuditSourceRef::ToolInvocation(_) => None,
        });
        let source_erasure_job_id = proposal.source_fact_ref.and_then(|source| match source {
            SecurityAuditSourceRef::ErasureJob(value) => Some(value.into_uuid()),
            SecurityAuditSourceRef::LedgerRecord(_) => None,
            SecurityAuditSourceRef::ModelInvocation(_)
            | SecurityAuditSourceRef::ToolInvocation(_) => None,
        });
        let source_model_invocation_id = proposal.source_fact_ref.and_then(|source| match source {
            SecurityAuditSourceRef::ModelInvocation(value) => Some(value.into_uuid()),
            _ => None,
        });
        let source_tool_invocation_id = proposal.source_fact_ref.and_then(|source| match source {
            SecurityAuditSourceRef::ToolInvocation(value) => Some(value.into_uuid()),
            _ => None,
        });

        let detail = proposal.detail_ref.as_ref().map(ContentColumns::from);
        let inserted_at: Option<i64> = sqlx::query_scalar(
            "INSERT INTO security_audit.records (
                 audit_record_id, schema_version, tenant_id,
                 actor_principal_id, actor_principal_kind, action, outcome,
                 resource_count, source_ledger_record_id, source_erasure_job_id,
                 source_model_invocation_id, source_tool_invocation_id,
                 correlation_id, trace_id,
                 detail_content_id, detail_media_type, detail_byte_length,
                 detail_sha256, retention_policy_ref, occurred_at_ms,
                 proposal_fingerprint
             ) VALUES (
                 $1, $2, $3,
                 $4, $5, $6, $7,
                 $8, $9, $10, $11, $12,
                 $13, $14,
                 $15, $16, $17::NUMERIC,
                 $18, $19, $20,
                 $21
             )
             ON CONFLICT DO NOTHING
             RETURNING committed_at_ms",
        )
        .bind(proposal.audit_record_id.into_uuid())
        .bind(proposal.schema_version.as_str())
        .bind(proposal.tenant_id.into_uuid())
        .bind(proposal.actor_principal_id.into_uuid())
        .bind(musteragent_runtime_audit::principal_kind_code(
            proposal.actor_principal_kind,
        ))
        .bind(proposal.action.code())
        .bind(proposal.outcome.code())
        .bind(i16::try_from(proposal.resource_refs.len()).expect("validated resource bound"))
        .bind(source_ledger_record_id)
        .bind(source_erasure_job_id)
        .bind(source_model_invocation_id)
        .bind(source_tool_invocation_id)
        .bind(proposal.correlation_id.into_uuid())
        .bind(proposal.trace_id.map(|value| value.as_bytes().to_vec()))
        .bind(detail.as_ref().map(|value| value.content_id))
        .bind(detail.as_ref().map(|value| value.media_type.as_str()))
        .bind(detail.as_ref().map(|value| value.byte_length.as_str()))
        .bind(detail.as_ref().map(|value| value.sha256.as_slice()))
        .bind(proposal.retention_policy_ref.as_str())
        .bind(proposal.occurred_at.unix_millis())
        .bind(fingerprint.into_bytes().as_slice())
        .fetch_optional(&mut *transaction)
        .await?;

        let Some(committed_at_ms) = inserted_at else {
            let existing: Option<(uuid::Uuid, Vec<u8>, i64)> = sqlx::query_as(
                "SELECT tenant_id, proposal_fingerprint, committed_at_ms
                 FROM security_audit.records
                 WHERE audit_record_id = $1",
            )
            .bind(proposal.audit_record_id.into_uuid())
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some((tenant_id, existing_fingerprint, committed_at_ms)) = existing {
                if tenant_id == proposal.tenant_id.into_uuid()
                    && existing_fingerprint == fingerprint.into_bytes()
                {
                    return Ok(SecurityAuditAppendReceipt {
                        disposition: SecurityAuditAppendDisposition::Duplicate,
                        record: SecurityAuditRecord {
                            proposal,
                            committed_at: UtcTimestamp::from_unix_millis(committed_at_ms),
                        },
                    });
                }
                return Err(PostgresSecurityAuditError::IdempotencyConflict);
            }
            if let Some(source) = proposal.source_fact_ref {
                let (column, source_id) = match source {
                    SecurityAuditSourceRef::LedgerRecord(value) => {
                        ("source_ledger_record_id", value.into_uuid())
                    }
                    SecurityAuditSourceRef::ErasureJob(value) => {
                        ("source_erasure_job_id", value.into_uuid())
                    }
                    SecurityAuditSourceRef::ModelInvocation(value) => {
                        ("source_model_invocation_id", value.into_uuid())
                    }
                    SecurityAuditSourceRef::ToolInvocation(value) => {
                        ("source_tool_invocation_id", value.into_uuid())
                    }
                };
                let source_conflict: bool = sqlx::query_scalar(&format!(
                    "SELECT EXISTS (
                         SELECT 1 FROM security_audit.records
                         WHERE tenant_id = $1
                           AND {column} = $2
                           AND action = $3
                     )"
                ))
                .bind(proposal.tenant_id.into_uuid())
                .bind(source_id)
                .bind(proposal.action.code())
                .fetch_one(&mut *transaction)
                .await?;
                if source_conflict {
                    return Err(PostgresSecurityAuditError::SourceActionConflict);
                }
            }
            return Err(PostgresSecurityAuditError::IdempotencyConflict);
        };

        for (ordinal, resource) in proposal.resource_refs.iter().enumerate() {
            let columns = ResourceColumns::from(resource);
            let resource_hash = resource_fingerprint(resource);
            sqlx::query(
                "INSERT INTO security_audit.resource_refs (
                     tenant_id, audit_record_id, ordinal, resource_kind,
                     resource_id, content_id, content_media_type,
                     content_byte_length, content_sha256, resource_fingerprint
                 ) VALUES (
                     $1, $2, $3, $4,
                     $5, $6, $7,
                     $8::NUMERIC, $9, $10
                 )",
            )
            .bind(proposal.tenant_id.into_uuid())
            .bind(proposal.audit_record_id.into_uuid())
            .bind(i16::try_from(ordinal).expect("validated resource bound"))
            .bind(columns.kind)
            .bind(columns.resource_id)
            .bind(columns.content.as_ref().map(|value| value.content_id))
            .bind(
                columns
                    .content
                    .as_ref()
                    .map(|value| value.media_type.as_str()),
            )
            .bind(
                columns
                    .content
                    .as_ref()
                    .map(|value| value.byte_length.as_str()),
            )
            .bind(
                columns
                    .content
                    .as_ref()
                    .map(|value| value.sha256.as_slice()),
            )
            .bind(resource_hash.into_bytes().as_slice())
            .execute(&mut *transaction)
            .await?;
        }
        let record = SecurityAuditRecord {
            proposal,
            committed_at: UtcTimestamp::from_unix_millis(committed_at_ms),
        };
        record.validate()?;
        Ok(SecurityAuditAppendReceipt {
            disposition: SecurityAuditAppendDisposition::Applied,
            record,
        })
    }

    async fn list_inner(
        &self,
        query: SecurityAuditQuery,
    ) -> Result<SecurityAuditPage, PostgresSecurityAuditError> {
        query.validate()?;
        let cursor_time = query.after.map(|cursor| cursor.committed_at.unix_millis());
        let cursor_id = query.after.map(|cursor| cursor.audit_record_id.into_uuid());
        let fetch_limit = i64::from(query.limit) + 1;
        let mut rows = sqlx::query(
            "SELECT audit_record_id, schema_version, tenant_id,
                    actor_principal_id, actor_principal_kind, action, outcome,
                    source_ledger_record_id, source_erasure_job_id,
                    source_model_invocation_id, source_tool_invocation_id,
                    correlation_id, trace_id,
                    detail_content_id, detail_media_type,
                    detail_byte_length::TEXT AS detail_byte_length,
                    detail_sha256, retention_policy_ref,
                    occurred_at_ms, committed_at_ms
             FROM security_audit.records
             WHERE tenant_id = $1
               AND (
                   $2::BIGINT IS NULL
                   OR (committed_at_ms, audit_record_id) > ($2, $3)
               )
             ORDER BY committed_at_ms, audit_record_id
             LIMIT $4",
        )
        .bind(query.tenant_id.into_uuid())
        .bind(cursor_time)
        .bind(cursor_id)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > usize::from(query.limit);
        rows.truncate(usize::from(query.limit));
        let record_ids = rows
            .iter()
            .map(|row| row.try_get::<uuid::Uuid, _>("audit_record_id"))
            .collect::<Result<Vec<_>, _>>()?;
        let resources = self
            .load_resources(query.tenant_id, record_ids.as_slice())
            .await?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let record_id: uuid::Uuid = row.try_get("audit_record_id")?;
            let resource_refs = resources.get(&record_id).cloned().ok_or(
                PostgresSecurityAuditError::CorruptState(
                    "persisted security audit record has no resource set",
                ),
            )?;
            records.push(decode_record(&row, resource_refs)?);
        }
        let next_cursor = has_more.then(|| records.last().map(cursor_for)).flatten();
        Ok(SecurityAuditPage {
            records,
            next_cursor,
        })
    }

    #[cfg(feature = "test-support")]
    pub async fn list(
        &self,
        query: SecurityAuditQuery,
    ) -> Result<SecurityAuditPage, PostgresSecurityAuditError> {
        self.list_inner(query).await
    }

    async fn load_resources(
        &self,
        tenant_id: TenantId,
        record_ids: &[uuid::Uuid],
    ) -> Result<HashMap<uuid::Uuid, Vec<ResourceRef>>, PostgresSecurityAuditError> {
        if record_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query(
            "SELECT audit_record_id, ordinal, resource_kind, resource_id,
                    content_id, content_media_type AS media_type,
                    content_byte_length::TEXT AS byte_length,
                    content_sha256 AS sha256
             FROM security_audit.resource_refs
             WHERE tenant_id = $1 AND audit_record_id = ANY($2)
             ORDER BY audit_record_id, ordinal",
        )
        .bind(tenant_id.into_uuid())
        .bind(record_ids)
        .fetch_all(&self.pool)
        .await?;
        let mut resources = HashMap::<uuid::Uuid, Vec<ResourceRef>>::new();
        for row in rows {
            resources
                .entry(row.try_get("audit_record_id")?)
                .or_default()
                .push(decode_resource(&row).map_err(|_| {
                    PostgresSecurityAuditError::CorruptState("invalid typed resource")
                })?);
        }
        Ok(resources)
    }
}

impl SecurityAuditReader for PostgresSecurityAuditReader {
    type Error = PostgresSecurityAuditError;

    fn list(
        &self,
        query: SecurityAuditQuery,
    ) -> musteragent_runtime_audit::ApplicationFuture<'_, Result<SecurityAuditPage, Self::Error>>
    {
        Box::pin(async move { self.store.list_inner(query).await })
    }
}

async fn validate_source_tenant(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    source: SecurityAuditSourceRef,
    trace_id: Option<SecurityAuditTraceId>,
) -> Result<(), PostgresSecurityAuditError> {
    let (query, source_id, missing, mismatch) = match source {
        SecurityAuditSourceRef::LedgerRecord(value) => (
            "SELECT record.tenant_id, run.traceparent
             FROM event_store.ledger_records AS record
             LEFT JOIN runtime.runs AS run
               ON run.tenant_id = record.tenant_id AND run.run_id = record.run_id
             WHERE record.record_id = $1",
            value.into_uuid(),
            PostgresSecurityAuditError::SourceLedgerRecordNotFound,
            PostgresSecurityAuditError::SourceLedgerTenantMismatch,
        ),
        SecurityAuditSourceRef::ErasureJob(value) => (
            "SELECT tenant_id, NULL::VARCHAR AS traceparent
             FROM privacy_erasure.requests WHERE erasure_job_id = $1",
            value.into_uuid(),
            PostgresSecurityAuditError::SourceErasureJobNotFound,
            PostgresSecurityAuditError::SourceErasureTenantMismatch,
        ),
        SecurityAuditSourceRef::ModelInvocation(value) => (
            "SELECT invocation.tenant_id, run.traceparent
             FROM runtime.model_invocations AS invocation
             JOIN runtime.runs AS run
               ON run.tenant_id = invocation.tenant_id AND run.run_id = invocation.run_id
             WHERE invocation.invocation_id = $1",
            value.into_uuid(),
            PostgresSecurityAuditError::SourceModelInvocationNotFound,
            PostgresSecurityAuditError::SourceModelInvocationTenantMismatch,
        ),
        SecurityAuditSourceRef::ToolInvocation(value) => (
            "SELECT invocation.tenant_id, run.traceparent
             FROM tool.tool_invocations AS invocation
             JOIN runtime.runs AS run
               ON run.tenant_id = invocation.tenant_id AND run.run_id = invocation.run_id
             WHERE invocation.invocation_id = $1",
            value.into_uuid(),
            PostgresSecurityAuditError::SourceToolInvocationNotFound,
            PostgresSecurityAuditError::SourceToolInvocationTenantMismatch,
        ),
    };
    let source_row: Option<(uuid::Uuid, Option<String>)> = sqlx::query_as(query)
        .bind(source_id)
        .fetch_optional(&mut *transaction)
        .await?;
    let (source_tenant, source_traceparent) = source_row.ok_or(missing)?;
    if source_tenant != tenant_id.into_uuid() {
        return Err(mismatch);
    }
    if let (Some(expected), Some(traceparent)) = (trace_id, source_traceparent) {
        let traceparent = musteragent_runtime_contract::TraceParent::new(traceparent)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid source trace"))?;
        let actual = SecurityAuditTraceId::from_trace_context(
            &musteragent_runtime_contract::TraceContext {
                traceparent,
                tracestate: None,
            },
        )?;
        if actual != expected {
            return Err(PostgresSecurityAuditError::SourceTraceMismatch);
        }
    }
    Ok(())
}

fn decode_record(
    row: &PgRow,
    resource_refs: Vec<ResourceRef>,
) -> Result<SecurityAuditRecord, PostgresSecurityAuditError> {
    let proposal = ProposedSecurityAuditRecord {
        schema_version: musteragent_identity::DottedKey::new(
            row.try_get::<String, _>("schema_version")?,
        )
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid schema version"))?,
        audit_record_id: AuditRecordId::from_uuid(row.try_get("audit_record_id")?)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid audit record id"))?,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid tenant id"))?,
        actor_principal_id: musteragent_identity::PrincipalId::from_uuid(
            row.try_get("actor_principal_id")?,
        )
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid principal id"))?,
        actor_principal_kind: principal_kind_from_code(row.try_get("actor_principal_kind")?)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid principal kind"))?,
        action: SecurityAuditAction::from_code(row.try_get("action")?)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid action"))?,
        outcome: SecurityAuditOutcome::from_code(row.try_get("outcome")?)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid outcome"))?,
        resource_refs,
        source_fact_ref: decode_source(row)?,
        correlation_id: musteragent_identity::CorrelationId::from_uuid(
            row.try_get("correlation_id")?,
        )
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid correlation id"))?,
        trace_id: row
            .try_get::<Option<Vec<u8>>, _>("trace_id")?
            .map(SecurityAuditTraceId::from_bytes)
            .transpose()
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid trace identity"))?,
        detail_ref: decode_content(row, "detail")
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid detail ContentRef"))?,
        retention_policy_ref: musteragent_runtime_audit::DataRetentionPolicyRef::new(
            row.try_get::<String, _>("retention_policy_ref")?,
        )
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid retention policy"))?,
        occurred_at: UtcTimestamp::from_unix_millis(row.try_get("occurred_at_ms")?),
    };
    let record = SecurityAuditRecord {
        proposal,
        committed_at: UtcTimestamp::from_unix_millis(row.try_get("committed_at_ms")?),
    };
    record
        .validate()
        .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid persisted audit fact"))?;
    Ok(record)
}

fn decode_source(
    row: &PgRow,
) -> Result<Option<SecurityAuditSourceRef>, PostgresSecurityAuditError> {
    let ledger = row.try_get::<Option<uuid::Uuid>, _>("source_ledger_record_id")?;
    let erasure = row.try_get::<Option<uuid::Uuid>, _>("source_erasure_job_id")?;
    let model = row.try_get::<Option<uuid::Uuid>, _>("source_model_invocation_id")?;
    let tool = row.try_get::<Option<uuid::Uuid>, _>("source_tool_invocation_id")?;
    match (ledger, erasure, model, tool) {
        (None, None, None, None) => Ok(None),
        (Some(value), None, None, None) => musteragent_identity::LedgerRecordId::from_uuid(value)
            .map(SecurityAuditSourceRef::LedgerRecord)
            .map(Some)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Ledger record id")),
        (None, Some(value), None, None) => musteragent_identity::ErasureJobId::from_uuid(value)
            .map(SecurityAuditSourceRef::ErasureJob)
            .map(Some)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid erasure job id")),
        (None, None, Some(value), None) => musteragent_identity::InvocationId::from_uuid(value)
            .map(SecurityAuditSourceRef::ModelInvocation)
            .map(Some)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Model invocation id")),
        (None, None, None, Some(value)) => musteragent_identity::InvocationId::from_uuid(value)
            .map(SecurityAuditSourceRef::ToolInvocation)
            .map(Some)
            .map_err(|_| PostgresSecurityAuditError::CorruptState("invalid Tool invocation id")),
        _ => Err(PostgresSecurityAuditError::CorruptState(
            "multiple audit source facts",
        )),
    }
}

fn cursor_for(record: &SecurityAuditRecord) -> SecurityAuditCursor {
    SecurityAuditCursor {
        committed_at: record.committed_at,
        audit_record_id: record.proposal.audit_record_id,
    }
}

#[derive(Debug, Error)]
pub enum PostgresSecurityAuditError {
    #[error("security audit fact does not bind to its canonical source authority")]
    FactAuthorityMismatch,
    #[error("security audit source Ledger record does not exist")]
    SourceLedgerRecordNotFound,
    #[error("security audit source Ledger record belongs to another tenant")]
    SourceLedgerTenantMismatch,
    #[error("security audit source erasure job does not exist")]
    SourceErasureJobNotFound,
    #[error("security audit source erasure job belongs to another tenant")]
    SourceErasureTenantMismatch,
    #[error("security audit source Model invocation does not exist")]
    SourceModelInvocationNotFound,
    #[error("security audit source Model invocation belongs to another tenant")]
    SourceModelInvocationTenantMismatch,
    #[error("security audit source Tool invocation does not exist")]
    SourceToolInvocationNotFound,
    #[error("security audit source Tool invocation belongs to another tenant")]
    SourceToolInvocationTenantMismatch,
    #[error("security audit source trace identity does not match")]
    SourceTraceMismatch,
    #[error("security audit idempotency identity conflicts with another proposal")]
    IdempotencyConflict,
    #[error("security audit source/action already has a canonical fact")]
    SourceActionConflict,
    #[error("corrupt persisted security audit state: {0}")]
    CorruptState(&'static str),
    #[error(transparent)]
    Contract(#[from] SecurityAuditContractError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Security Audit Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}
