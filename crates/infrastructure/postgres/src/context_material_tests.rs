use std::num::NonZeroU32;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::{ContextEvent, MaterializedModelActionRejection};
use musteragent_conversation::{ConversationEvent, MessageRole, PartialMessageReason};
use musteragent_identity::{
    ActivityId, AttemptId, ContentId, ContextBranchId, ConversationId, ConversationSeq,
    InvocationId, LedgerRecordId, MessageId, PrincipalId, RunId, TenantId,
};
use musteragent_ledger::DomainEvent;
use musteragent_runtime_contract::{ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText};
use musteragent_runtime_model::CanonicalRole;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{ToolEvent, WireName};

use super::{
    LedgerFactInput, LedgerFacts, ToolTerminalSourceKind, load_latest_model_action_rejection,
    terminal_result_matches,
};
use crate::migrate;

fn content(value: &str) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media"),
        byte_length: value.len() as u64,
        sha256: Sha256::digest(value.as_bytes()),
    }
}

fn append(facts: &mut LedgerFacts, sequence: u64, record_id: LedgerRecordId, event: DomainEvent) {
    facts
        .apply(LedgerFactInput {
            record_id,
            sequence: ConversationSeq::new(sequence).expect("sequence"),
            run_id: None,
            activity_id: None,
            target_activity_id: ActivityId::generate(),
            target_run_id: RunId::generate(),
            event,
        })
        .expect("apply fact");
}

#[test]
fn retry_context_uses_only_target_runs_durable_source_message_binding() {
    let target_run = RunId::generate();
    let source_message = MessageId::generate();
    let target_activity = ActivityId::generate();
    let mut facts = LedgerFacts::default();
    for (sequence, run, source) in [
        (1, RunId::generate(), Some(MessageId::generate())),
        (2, target_run, Some(source_message)),
        (3, RunId::generate(), None),
    ] {
        facts
            .apply(LedgerFactInput {
                record_id: LedgerRecordId::generate(),
                sequence: ConversationSeq::new(sequence)
                    .expect("valid context material tests fixture"),
                run_id: Some(run),
                activity_id: Some(target_activity),
                target_activity_id: target_activity,
                target_run_id: target_run,
                event: DomainEvent::Run(musteragent_run::RunEvent::Admitted {
                    runtime_snapshot_id: musteragent_identity::RuntimeSnapshotId::generate(),
                    execution_home: musteragent_event_envelope::RuntimeHome::Cloud,
                    parent_run_id: Some(RunId::generate()),
                    delegation_id: None,
                    retry_source_message_id: source,
                    retry_source_attempt_number: Some(
                        musteragent_identity::AttemptNumber::new(1)
                            .expect("valid context material tests fixture"),
                    ),
                }),
            })
            .expect("valid context material tests fixture");
    }
    assert_eq!(facts.retry_source_message_id, Some(source_message));
}

#[test]
fn failed_tool_diagnostic_is_preserved_as_an_authoritative_ledger_fact() {
    let invocation_id = InvocationId::generate();
    let diagnostic_ref = content("structured build diagnostic");
    let mut facts = LedgerFacts::default();
    append(
        &mut facts,
        1,
        LedgerRecordId::generate(),
        DomainEvent::Tool(ToolEvent::InvocationFailed {
            invocation_id,
            error: ErrorInfo {
                code: ErrorCode::InvalidCommand,
                category: ErrorCode::InvalidCommand.category(),
                safe_message: SafeDisplayText::new("Application build failed")
                    .expect("safe message"),
                retryable: false,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result_ref: Some(diagnostic_ref.clone()),
            diagnostic_result_hash: Some(diagnostic_ref.sha256),
        }),
    );

    assert!(matches!(
        &facts
            .tool_terminals
            .get(&invocation_id)
            .expect("failed Tool terminal")
            .outcome,
        ToolTerminalSourceKind::Failed {
            diagnostic_result_ref: Some(persisted),
        } if persisted == &diagnostic_ref
    ));
    let terminal = &facts
        .tool_terminals
        .get(&invocation_id)
        .expect("failed Tool terminal")
        .outcome;
    assert!(terminal_result_matches(
        terminal,
        5,
        Some(&diagnostic_ref),
        Some(diagnostic_ref.sha256),
    ));
    assert!(
        !terminal_result_matches(terminal, 5, None, None),
        "a persisted diagnostic cannot disappear during reconstruction"
    );

    let mismatched_ref = content("mismatched diagnostic");
    let result = LedgerFacts::default().apply(LedgerFactInput {
        record_id: LedgerRecordId::generate(),
        sequence: ConversationSeq::new(1).expect("sequence"),
        run_id: None,
        activity_id: None,
        target_activity_id: ActivityId::generate(),
        target_run_id: RunId::generate(),
        event: DomainEvent::Tool(ToolEvent::InvocationFailed {
            invocation_id: InvocationId::generate(),
            error: ErrorInfo {
                code: ErrorCode::InvalidCommand,
                category: ErrorCode::InvalidCommand.category(),
                safe_message: SafeDisplayText::new("Application build failed")
                    .expect("safe message"),
                retryable: false,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result_ref: Some(mismatched_ref),
            diagnostic_result_hash: Some(Sha256::digest(b"different bytes")),
        }),
    });
    assert!(
        result.is_err(),
        "a mismatched diagnostic hash must fail closed"
    );
}

#[tokio::test]
async fn model_action_rejection_order_and_fingerprint_come_from_the_context_plan_authority() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let run_id = RunId::generate();
    let attempt_id = AttemptId::generate();
    let invocation_ids = [InvocationId::generate(), InvocationId::generate()];
    let record_ids = [LedgerRecordId::generate(), LedgerRecordId::generate()];
    let attempted_names = ["legacy_time_alias", "system_time_current"];
    let summary_content_id: uuid::Uuid = "01982f80-0000-7000-8000-000000003001"
        .parse()
        .expect("summary Content id");
    let request_content_id: uuid::Uuid = "01982f80-0000-7000-8000-000000003002"
        .parse()
        .expect("request Content id");

    for id in [summary_content_id, request_content_id] {
        sqlx::query("INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES($1,$2,'application/octet-stream',0,$3,$4,1)")
            .bind(id).bind(tenant_id.into_uuid()).bind(vec![0_u8;32]).bind(format!("test://{id}")).execute(&pool).await.expect("Context plan content authority");
    }

    for (index, ((invocation_id, record_id), attempted_name)) in invocation_ids
        .iter()
        .zip(record_ids.iter())
        .zip(attempted_names)
        .enumerate()
    {
        let iteration = i32::try_from(index + 1).expect("iteration");
        sqlx::query(
            "INSERT INTO event_store.ledger_records (
                 record_id, schema_version, record_kind, tenant_id,
                 conversation_id, conversation_seq, activity_id, run_id,
                 run_seq, attempt_id, participant_id, home_epoch, producer,
                 occurred_at_ms, committed_at_ms, causation_id, correlation_id,
                 idempotency_key, payload_hash
             ) VALUES (
                 $1, 'musteragent.runtime.record.v1', 1, $2, $3, $4, NULL, $5,
                 $4, $6, NULL, 1, 2, $4, $4, NULL, $7, $8, $9
             )",
        )
        .bind(record_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(iteration)
        .bind(run_id.into_uuid())
        .bind(attempt_id.into_uuid())
        .bind(uuid::Uuid::now_v7())
        .bind(format!("context-rejection-record:{iteration}"))
        .bind(vec![
            u8::try_from(iteration).expect("hash byte");
            Sha256::LENGTH
        ])
        .execute(&pool)
        .await
        .expect("seed Context plan Ledger record");
        sqlx::query(
            "INSERT INTO runtime.context_plans (
                 snapshot_id, tenant_id, conversation_id, run_id, attempt_id,
                 iteration, parent_snapshot_id, source_sequence_from,
                 source_sequence_to, retained_record_ids, summary_content_id,
                 summary_content_hash, summary_content_bytes, request_content_id,
                 request_content_hash, request_content_bytes, input_tokens_before,
                 input_tokens_after, model_route_ref, checksum, invocation_id,
                 assistant_message_id, assistant_output_id, reasoning_output_id,
                 ledger_record_id, committed_sequence, created_at_ms
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, NULL, 1, 1, $7, $8, $9, 0,
                 $10, $9, 0, 1, 1, 'cloud.chat-primary@1', $9, $11,
                 $12, $13, $14, $15, $6, $6
             )",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(attempt_id.into_uuid())
        .bind(iteration)
        .bind(vec![record_id.into_uuid()])
        .bind(summary_content_id)
        .bind(vec![0_u8; Sha256::LENGTH])
        .bind(request_content_id)
        .bind(invocation_id.into_uuid())
        .bind(MessageId::generate().into_uuid())
        .bind(uuid::Uuid::now_v7())
        .bind(uuid::Uuid::now_v7())
        .bind(record_id.into_uuid())
        .execute(&pool)
        .await
        .expect("seed authoritative Context plan");
        sqlx::query(
            "INSERT INTO runtime.model_invocations (
                 invocation_id, tenant_id, conversation_id, run_id, attempt_id,
                 purpose, route_role, status, model_route_ref,
                 credential_grant_ref, requested_model_key, pricing_version_ref,
                 idempotency_key, request_content_id, request_hash,
                 started_at_ms, finished_at_ms, terminal_error
             ) VALUES (
                 $1, $2, $3, $4, $5, 1, 0, 3, 'cloud.chat-primary@1',
                 'grant_context_rejection_fixture', 'context-rejection-model',
                 'catalog:test.price@1', $6, $7, $8, $9, $9, $10
             )",
        )
        .bind(invocation_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(attempt_id.into_uuid())
        .bind(format!("model:{run_id}:{iteration}"))
        .bind(request_content_id)
        .bind(vec![1_u8; Sha256::LENGTH])
        .bind(iteration)
        .bind(b"tool_not_exposed".as_slice())
        .execute(&pool)
        .await
        .expect("seed rejected model invocation");
        sqlx::query(
            "INSERT INTO runtime.model_action_rejections (
                 invocation_id, error_code, attempted_wire_name, rejected_at_ms
             ) VALUES ($1, 'tool_not_exposed', $2, $3)",
        )
        .bind(invocation_id.into_uuid())
        .bind(attempted_name)
        .bind(iteration)
        .execute(&pool)
        .await
        .expect("seed private model-action rejection");
    }

    let rejection = load_latest_model_action_rejection(&pool, tenant_id, run_id)
        .await
        .expect("load model-action rejection");
    assert_eq!(
        rejection,
        Some(MaterializedModelActionRejection::ToolNotExposed {
            attempted_name: WireName::new("system_time_current").expect("wire name"),
            consecutive_count: NonZeroU32::MIN,
        })
    );
}

#[test]
fn active_branch_rebuilds_only_its_reachable_immutable_lineage() {
    let root_user = MessageId::generate();
    let abandoned_assistant = MessageId::generate();
    let replacement_user = MessageId::generate();
    let root_user_record = LedgerRecordId::generate();
    let abandoned_record = LedgerRecordId::generate();
    let old_branch = ContextBranchId::generate();
    let replacement_branch = ContextBranchId::generate();
    let actor = PrincipalId::generate();
    let edit_group_id = musteragent_identity::CorrelationId::generate();
    let mut facts = LedgerFacts::default();
    append(
        &mut facts,
        1,
        root_user_record,
        DomainEvent::Conversation(ConversationEvent::MessageAccepted {
            project_task_target: None,
            message_id: root_user,
            role: MessageRole::User,
            content_ref: content("original user"),
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
        }),
    );
    append(
        &mut facts,
        2,
        abandoned_record,
        DomainEvent::Conversation(ConversationEvent::MessageCommitted {
            message_id: abandoned_assistant,
            role: MessageRole::Assistant,
            content_ref: content("abandoned answer"),
            content_hash: content("abandoned answer").sha256,
            content_bytes: "abandoned answer".len() as u64,
        }),
    );
    append(
        &mut facts,
        3,
        LedgerRecordId::generate(),
        DomainEvent::Context(ContextEvent::BranchCreated {
            branch_id: old_branch,
            parent_branch_id: None,
            fork_from_record_id: abandoned_record,
            branch_summary_ref: None,
            origin: musteragent_conversation::ContextBranchOrigin::EditPreserved,
            edit_group_id: Some(edit_group_id),
            edit_target_message_id: Some(root_user),
            created_by: actor,
        }),
    );
    append(
        &mut facts,
        4,
        LedgerRecordId::generate(),
        DomainEvent::Context(ContextEvent::BranchCreated {
            branch_id: replacement_branch,
            parent_branch_id: None,
            fork_from_record_id: root_user_record,
            branch_summary_ref: None,
            origin: musteragent_conversation::ContextBranchOrigin::EditReplacement,
            edit_group_id: Some(edit_group_id),
            edit_target_message_id: Some(root_user),
            created_by: actor,
        }),
    );
    append(
        &mut facts,
        5,
        LedgerRecordId::generate(),
        DomainEvent::Context(ContextEvent::BranchActivated {
            branch_id: replacement_branch,
            activated_by: actor,
        }),
    );
    append(
        &mut facts,
        6,
        LedgerRecordId::generate(),
        DomainEvent::Conversation(ConversationEvent::MessageAccepted {
            project_task_target: None,
            message_id: replacement_user,
            role: MessageRole::User,
            content_ref: content("edited user"),
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
        }),
    );
    facts.retain_active_branch().expect("project active branch");
    assert!(facts.messages.contains_key(&root_user));
    assert!(facts.messages.contains_key(&replacement_user));
    assert!(!facts.messages.contains_key(&abandoned_assistant));
}

#[test]
fn partial_assistant_message_is_a_canonical_context_fact() {
    let message_id = MessageId::generate();
    let content_ref = content("partial assistant response");
    let mut facts = LedgerFacts::default();
    append(
        &mut facts,
        1,
        LedgerRecordId::generate(),
        DomainEvent::Conversation(ConversationEvent::MessagePartiallyCommitted {
            message_id,
            content_ref: content_ref.clone(),
            content_hash: content_ref.sha256,
            content_bytes: content_ref.byte_length,
            reason: PartialMessageReason::Cancelled,
        }),
    );
    let message = facts
        .messages
        .get(&message_id)
        .expect("partial Context fact");
    assert_eq!(message.role, CanonicalRole::Assistant);
}

#[test]
fn recovery_stimulus_requires_this_runs_failed_primary_and_authorized_retry() {
    use musteragent_identity::{AttemptNumber, UtcTimestamp};
    use musteragent_run::RunEvent;
    use musteragent_runtime_model::ModelEvent;
    for (foreign_failure, auxiliary, foreign_retry, retryable, expected) in [
        (false, false, false, true, true),
        (true, false, false, true, false),
        (false, true, false, true, false),
        (false, false, true, true, false),
        (false, false, false, false, false),
    ] {
        let run_id = RunId::generate();
        let activity_id = ActivityId::generate();
        let primary = InvocationId::generate();
        let mut facts = LedgerFacts {
            primary_invocation_id: Some(primary),
            ..Default::default()
        };
        let error = ErrorInfo {
            code: ErrorCode::DependencyTimeout,
            category: ErrorCode::DependencyTimeout.category(),
            safe_message: SafeDisplayText::new("Physical response interrupted").unwrap(),
            retryable,
            dependency_ref: None,
            diagnostic_ref: None,
        };
        let mut apply = |sequence, run, event| {
            facts
                .apply(LedgerFactInput {
                    record_id: LedgerRecordId::generate(),
                    sequence: ConversationSeq::new(sequence).unwrap(),
                    run_id: Some(run),
                    activity_id: Some(activity_id),
                    target_activity_id: activity_id,
                    target_run_id: run_id,
                    event,
                })
                .unwrap()
        };
        apply(
            1,
            if foreign_failure {
                RunId::generate()
            } else {
                run_id
            },
            DomainEvent::Model(ModelEvent::InvocationFailed {
                invocation_id: if auxiliary {
                    InvocationId::generate()
                } else {
                    primary
                },
                error: error.clone(),
                retry_disposition: RetryDisposition::DoNotRetry,
            }),
        );
        apply(
            2,
            if foreign_retry {
                RunId::generate()
            } else {
                run_id
            },
            DomainEvent::Run(RunEvent::RetryScheduled {
                failed_attempt_id: AttemptId::generate(),
                next_attempt_number: AttemptNumber::new(2).unwrap(),
                not_before: UtcTimestamp::from_unix_millis(10),
                error,
            }),
        );
        assert_eq!(facts.model_recovery, expected.then_some(primary));
    }
}

#[test]
fn authorized_fallback_is_a_stimulus_without_requiring_another_attempt() {
    let run_id = RunId::generate();
    let activity_id = ActivityId::generate();
    let invocation_id = InvocationId::generate();
    let mut facts = LedgerFacts {
        primary_invocation_id: Some(invocation_id),
        ..Default::default()
    };
    facts
        .apply(LedgerFactInput {
            record_id: LedgerRecordId::generate(),
            sequence: ConversationSeq::new(1).unwrap(),
            run_id: Some(run_id),
            activity_id: Some(activity_id),
            target_activity_id: activity_id,
            target_run_id: run_id,
            event: DomainEvent::Model(musteragent_runtime_model::ModelEvent::InvocationFailed {
                invocation_id,
                retry_disposition: RetryDisposition::RetryFallbackRoute,
                error: ErrorInfo {
                    code: ErrorCode::DependencyTimeout,
                    category: ErrorCode::DependencyTimeout.category(),
                    safe_message: SafeDisplayText::new("Provider failed before output").unwrap(),
                    retryable: true,
                    dependency_ref: None,
                    diagnostic_ref: None,
                },
            }),
        })
        .unwrap();
    assert_eq!(facts.model_recovery, Some(invocation_id));
}
