#![cfg(test)]

#[path = "postgres_agent_loop/cancellation_race.rs"]
mod cancellation_race;
use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

#[path = "postgres_agent_loop/model_fixture.rs"]
mod model_fixture;
#[path = "support/running_loop.rs"]
mod running_loop;
#[path = "postgres_agent_loop/support.rs"]
mod support;

use model_fixture::model_iteration;
use musteragent_activity::ActivityKind;
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
};
use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_conversation::{AdmissionMessage, ConversationEvent, MessageRole};
use musteragent_event_envelope::{ExecutionScope, LedgerRecordKind, Producer, RuntimeHome};
use musteragent_identity::{
    Actor, ArtifactId, ArtifactVersionId, CapabilityProviderId, CommandId, ContentId,
    CorrelationId, DottedKey, FencingToken, HomeEpoch, IdempotencyKey, InvocationId, LedgerOffset,
    LedgerRecordId, MessageId, OutputId, PrincipalKind, ResourceChangeSetId, ResourceNodeId,
    RunSeq, ToolCallId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_ledger::{
    AppendRequest, DomainEvent, LedgerPayload, ProposedRecord, RunControlBatch,
};
use musteragent_loop::{
    AgentLoopJournal, DurableLoopCheckpoint, LoopControlQueue, LoopPhase, ModelCompletionCommit,
    ModelCompletionKind, ModelEventBatch, ModelIteration, ModelRecoveryConstraints,
    PlannedToolCall, RunCompletionDisposition, ToolResultCommit,
};
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelRoute, PricingVersionRef, ProviderStatePolicy,
};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresArtifactStore, PostgresContentStore, PostgresDispatchStore,
    PostgresFencedArtifactToolCommitter, PostgresFencedToolInvocationJournal, PostgresLedgerStore,
    PostgresLoopControlQueue, migrate,
};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_resource::ResourceName;
use musteragent_run::{
    AttemptContext, InvocationContext, RunContext, RunControlKind, RunEvent, WorkerId,
};
use musteragent_runtime_artifact::{
    ArtifactCommitDisposition, ArtifactMutation, CommitPreparedArtifactTool,
    FencedArtifactToolCommitter,
};
use musteragent_runtime_capability::{
    CapabilityResult, FencedToolInvocationJournal, ToolExecutionStart, ToolStartMode,
    ToolTerminalOutcome,
};
use musteragent_runtime_content::{ArtifactContentAuthorization, ContentStore, PutContent};
use musteragent_runtime_contract::{ErrorCode, OpaqueExternalRef};
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    JsonValue, ModelReplayText, ModelRouteRef, ModelStreamEvent, ModelText, ModelUsage,
    ProviderRecoveryCapability, ProviderToolCallReplay, ReasoningPolicy, StopReason,
};
use musteragent_runtime_resource::{
    PreparedResourceFilePlacement, ResourceCommandContext, ResourceMutationIdentity,
};
use musteragent_scheduler::{ClaimOutcome, DispatchStore};
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{
    ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract, WireName,
};
use musteragent_wire::ProtobufMeteringPublicationEncoder;
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder};
use running_loop::{
    Fixture, database_now, keyring, run_context, seed_parallel_run, seed_running_loop,
};
use sqlx::Row;
use support::{
    assert_cross_tenant_run_source_rejected, assert_no_tool_audit, assert_succeeded_model_audits,
    assert_succeeded_tool_audit, audited_loop_journal, audited_tool_journal, completed_checkpoint,
    provider_ref, response_model_key, security_audit_pipeline,
};

#[tokio::test]
async fn journal_commits_model_frames_usage_tools_and_terminal_state_atomically() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let journal = audited_loop_journal(
        &pool,
        content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
        running_loop::recovery_keyring(),
    );
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"canonical request one".to_vec(),
        })
        .await
        .expect("store request");
    let mut iteration = model_iteration(fixture, attempt.clone(), request_ref, 1);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut iteration,
    )
    .await;
    let started = journal
        .begin_model(
            &attempt,
            &iteration,
            ProviderRecoveryCapability::NonResumable,
        )
        .await
        .expect("begin model");
    assert!(matches!(
        started.phase,
        LoopPhase::ModelInFlight { invocation_id }
            if invocation_id == iteration.invocation_id
    ));
    journal
        .mark_model_dispatched(&attempt, iteration.invocation_id)
        .await
        .expect("mark model dispatched");
    let call_id = ToolCallId::new("call-search").expect("call id");
    let tool_name = WireName::new("search").expect("wire name");
    let provider_replay = ProviderToolCallReplay::google_thought_signature(
        ModelRouteRef::from_key(VersionedKey::new("cloud.chat-primary@1").expect("model route")),
        ApiFormat::GoogleGenerateContent,
        "gemini-3-pro",
        "opaque-signature",
    )
    .expect("provider replay");
    let usage = ModelUsage {
        input_tokens_uncached: 20,
        output_tokens: 5,
        reasoning_tokens: 2,
        provider_cache_read_tokens: 0,
        provider_cache_write_tokens: 0,
        provider_cache_write_1h_tokens: 0,
        total_tokens: Some(25),
    };
    journal
        .append_model_events(
            &attempt,
            ModelEventBatch {
                invocation_id: iteration.invocation_id,
                iteration: 1,
                batch_index: 0,
                events: vec![
                    ModelStreamEvent::ResponseStarted {
                        response_ref: provider_ref("response_1"),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::ReasoningDelta {
                        output_index: 0,
                        delta: ModelText::new("private reasoning").expect("text"),
                    },
                    ModelStreamEvent::ToolCallStarted {
                        output_index: 1,
                        call_id: call_id.clone(),
                        name: tool_name.clone(),
                        provider_item_ref: None,
                        provider_replay: Some(provider_replay.clone()),
                    },
                    ModelStreamEvent::ToolCallCompleted {
                        output_index: 1,
                        call_id: call_id.clone(),
                        arguments: JsonValue::Object(BTreeMap::from([(
                            "query".into(),
                            JsonValue::String("Rust".into()),
                        )])),
                    },
                    ModelStreamEvent::Usage(usage),
                    ModelStreamEvent::ResponseCompleted {
                        stop_reason: StopReason::ToolCalls,
                    },
                ],
            },
        )
        .await
        .expect("append model batch");
    let pending = musteragent_conformance::fixtures::model_journal::finish_model(
        &journal,
        &attempt,
        ModelCompletionCommit {
            invocation_id: iteration.invocation_id,
            kind: ModelCompletionKind::ToolCalls,
            tool_calls: vec![PlannedToolCall {
                argument_diagnostic: None,
                call_id: call_id.clone(),
                name: tool_name.clone(),
                arguments: JsonValue::Object(BTreeMap::from([(
                    "query".into(),
                    JsonValue::String("Rust".into()),
                )])),
                provider_replay: Some(provider_replay),
                planning_failure: None,
            }],
            reasoning_replay: Some(
                ModelReplayText::new("private reasoning").expect("reasoning replay"),
            ),
            usage: Some(usage),
            provider_response_ref: provider_ref("response_1"),
            response_model_key: response_model_key(),
            continuation_ref: None,
        },
    )
    .await
    .expect("finish model");
    assert!(matches!(pending.phase, LoopPhase::ToolsPending { .. }));
    let replay_row = sqlx::query(
        "SELECT content.content_id, content.media_type,
                content.byte_length::text AS byte_length, content.content_hash
         FROM runtime.pending_tool_calls AS pending
         JOIN artifact.content_refs AS content
           ON content.tenant_id = $1
          AND content.content_id = pending.provider_replay_content_id
         WHERE pending.run_id = $2 AND pending.tool_call_id = $3",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(call_id.as_str())
    .fetch_one(&pool)
    .await
    .expect("provider replay reference");
    let replay_ref = ContentRef {
        content_id: ContentId::from_uuid(replay_row.get("content_id")).expect("content id"),
        media_type: MediaType::new(replay_row.get::<String, _>("media_type")).expect("media"),
        byte_length: replay_row
            .get::<String, _>("byte_length")
            .parse()
            .expect("byte length"),
        sha256: Sha256::from_bytes(replay_row.get::<Vec<u8>, _>("content_hash"))
            .expect("content hash"),
    };
    let replay_bytes = content
        .read(fixture.tenant_id, &replay_ref)
        .await
        .expect("read replay");
    let replay_json: serde_json::Value =
        serde_json::from_slice(&replay_bytes).expect("decode replay");
    assert_eq!(
        replay_json["schema"],
        "musteragent.legacy.runtime.provider-tool-replay.v2"
    );
    assert_eq!(replay_json["source_route_ref"], "cloud.chat-primary@1");
    assert_eq!(replay_json["source_api_format"], "google.generate-content");
    assert_eq!(replay_json["signature"], "opaque-signature");
    let recovered = journal
        .recover_checkpoint(
            &attempt,
            pending.clone(),
            ModelRecoveryConstraints {
                recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
            },
        )
        .await
        .expect("recover pending Tool");
    assert_eq!(recovered.checkpoint, pending);
    assert_eq!(recovered.pending_tool_calls.len(), 1);
    assert_eq!(recovered.pending_tool_calls[0].call_id, call_id);
    assert_eq!(
        recovered.pending_tool_calls[0]
            .provider_replay
            .as_ref()
            .and_then(ProviderToolCallReplay::google_signature),
        Some("opaque-signature"),
        "crash recovery must preserve provider-bound Tool-call replay state"
    );
    let ready = journal
        .commit_tool_result(
            &attempt,
            ToolResultCommit {
                call_id: call_id.clone(),
                message: CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(
                        ModelText::new("Rust is a systems language.").expect("result"),
                    )],
                    tool_calls: Vec::new(),
                    tool_call_id: Some(call_id),
                    tool_name: Some(tool_name),
                    reasoning: None,
                },
            },
        )
        .await
        .expect("commit Tool result");
    assert_eq!(ready.phase, LoopPhase::ReadyForModel);

    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"canonical request two".to_vec(),
        })
        .await
        .expect("store request");
    let mut iteration = model_iteration(fixture, attempt.clone(), request_ref, 2);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut iteration,
    )
    .await;
    journal
        .begin_model(
            &attempt,
            &iteration,
            ProviderRecoveryCapability::NonResumable,
        )
        .await
        .expect("begin second model");
    journal
        .mark_model_dispatched(&attempt, iteration.invocation_id)
        .await
        .expect("mark second model dispatched");
    journal
        .append_model_events(
            &attempt,
            ModelEventBatch {
                invocation_id: iteration.invocation_id,
                iteration: 2,
                batch_index: 0,
                events: vec![
                    ModelStreamEvent::ResponseStarted {
                        response_ref: provider_ref("response_2"),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::TextDelta {
                        output_index: 0,
                        delta: ModelText::new("Rust is a systems language.").expect("text"),
                    },
                    ModelStreamEvent::Usage(usage),
                    ModelStreamEvent::ResponseCompleted {
                        stop_reason: StopReason::Stop,
                    },
                ],
            },
        )
        .await
        .expect("append second batch");
    musteragent_conformance::fixtures::model_journal::finish_model(
        &journal,
        &attempt,
        ModelCompletionCommit {
            invocation_id: iteration.invocation_id,
            kind: ModelCompletionKind::AssistantResponse,
            tool_calls: Vec::new(),
            reasoning_replay: None,
            usage: Some(usage),
            provider_response_ref: provider_ref("response_2"),
            response_model_key: response_model_key(),
            continuation_ref: None,
        },
    )
    .await
    .expect("finish second model");
    let committed: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT event.encoded_event FROM event_store.domain_events event JOIN event_store.ledger_records record USING(record_id) WHERE record.run_id = $1",
    ).bind(fixture.run_id.into_uuid()).fetch_all(&pool).await.expect("durable events");
    assert!(committed.iter().any(|event| matches!(
        ProtobufLedgerPayloadCodec.decode(LedgerRecordKind::DomainEvent, event),
        Ok(LedgerPayload::DomainEvent(DomainEvent::Conversation(
            ConversationEvent::MessageCommitted {
                role: MessageRole::Assistant,
                ..
            }
        )))
    )));
    let completed = completed_checkpoint(journal.complete(&attempt).await.expect("complete Run"));
    assert_eq!(completed.phase, LoopPhase::Completed);

    let counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
             (SELECT count(*) FROM event_store.outbox WHERE topic = 'musteragent.model-usage.v1'),
             (SELECT count(*) FROM runtime.pending_tool_calls
                WHERE run_id = $1 AND completed_at_ms IS NOT NULL),
             (SELECT count(*) FROM event_store.output_frames),
             (SELECT count(*) FROM conversation.messages
                WHERE run_id = $1 AND role = 2 AND lifecycle = 1
                  AND participant_id = $2)",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.executor_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read durable facts");
    assert_eq!(counts.0, 2);
    assert_eq!(counts.1, 2);
    assert_eq!(counts.2, 1);
    assert_eq!(
        counts.3, 2,
        "a Tool-only intermediate turn emits no dormant Assistant frames; the final non-empty output emits one data and one terminal Frame"
    );
    assert_eq!(counts.4, 1);
    assert_succeeded_model_audits(&pool, fixture.tenant_id, fixture.run_id, 2).await;
    assert_cross_tenant_run_source_rejected(&pool, iteration.invocation_id).await;
    let statuses: (i16, i16, i16) = sqlx::query_as(
        "SELECT run.status, attempt.status, activity.status
         FROM runtime.runs AS run
         JOIN runtime.attempts AS attempt ON attempt.run_id = run.run_id
         JOIN activity.activities AS activity ON activity.activity_id = run.activity_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read terminal state");
    assert_eq!(statuses, (5, 3, 4));
}

#[tokio::test]
async fn terminal_run_failure_persists_failure_event() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let journal = audited_loop_journal(
        &pool,
        content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
        running_loop::recovery_keyring(),
    );
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let failed = journal
        .fail(
            &attempt,
            ErrorCode::CapabilityUnavailable,
            false,
            None,
            std::num::NonZeroU32::new(8).expect("limit").into(),
        )
        .await
        .expect("fail the Run");
    assert_eq!(failed.phase, LoopPhase::Failed);

    let events: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT event.encoded_event FROM event_store.domain_events event JOIN event_store.ledger_records record USING(record_id) WHERE record.run_id = $1",
    ).bind(fixture.run_id.into_uuid()).fetch_all(&pool).await.expect("failure events");
    assert_eq!(
        events
            .iter()
            .filter(|bytes| matches!(
                ProtobufLedgerPayloadCodec.decode(LedgerRecordKind::DomainEvent, bytes),
                Ok(LedgerPayload::DomainEvent(DomainEvent::Run(
                    RunEvent::Failed { .. }
                )))
            ))
            .count(),
        1,
        "a terminal failure commits exactly one failure event"
    );
}

#[tokio::test]
async fn empty_model_completion_is_private_durable_and_recoverable() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal =
        musteragent_conformance::fixtures::model_journal::supervise(PostgresAgentLoopJournal::new(
            pool.clone(),
            content.clone(),
            codec.clone(),
            codec,
            Arc::new(ProtobufMeteringPublicationEncoder),
            keys,
            running_loop::recovery_keyring(),
        ));
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"empty response request".to_vec(),
        })
        .await
        .expect("store request");
    let mut iteration = model_iteration(fixture, attempt.clone(), request_ref, 1);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut iteration,
    )
    .await;
    journal
        .begin_model(
            &attempt,
            &iteration,
            ProviderRecoveryCapability::NonResumable,
        )
        .await
        .expect("begin model");
    journal
        .mark_model_dispatched(&attempt, iteration.invocation_id)
        .await
        .expect("mark model dispatched");
    let usage = ModelUsage {
        input_tokens_uncached: 20,
        output_tokens: 0,
        total_tokens: Some(20),
        ..ModelUsage::default()
    };
    journal
        .append_model_events(
            &attempt,
            ModelEventBatch {
                invocation_id: iteration.invocation_id,
                iteration: 1,
                batch_index: 0,
                events: vec![
                    ModelStreamEvent::ResponseStarted {
                        response_ref: provider_ref("response_empty"),
                        response_model_key: response_model_key(),
                    },
                    ModelStreamEvent::Usage(usage),
                    ModelStreamEvent::ResponseCompleted {
                        stop_reason: StopReason::Stop,
                    },
                ],
            },
        )
        .await
        .expect("append empty model completion");
    let ready = musteragent_conformance::fixtures::model_journal::finish_model(
        &journal,
        &attempt,
        ModelCompletionCommit {
            invocation_id: iteration.invocation_id,
            kind: ModelCompletionKind::EmptyResponse,
            tool_calls: Vec::new(),
            reasoning_replay: None,
            usage: Some(usage),
            provider_response_ref: provider_ref("response_empty"),
            response_model_key: response_model_key(),
            continuation_ref: None,
        },
    )
    .await
    .expect("commit empty completion");

    assert_eq!(ready.phase, LoopPhase::ReadyForModel);
    assert_eq!(ready.consecutive_empty_responses, 1);
    assert_eq!(ready.total_tokens, 20);
    let recovered = journal
        .recover_checkpoint(
            &attempt,
            ready.clone(),
            ModelRecoveryConstraints {
                recovery_policy: std::num::NonZeroU32::new(8).expect("limit").into(),
            },
        )
        .await
        .expect("recover empty response checkpoint");
    assert_eq!(recovered.checkpoint, ready);
    assert!(recovered.pending_tool_calls.is_empty());

    let facts: (i64, i64, i64, i16) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id = $1),
             (SELECT count(*) FROM conversation.messages WHERE run_id = $1 AND role = 2),
             (SELECT count(*) FROM event_store.output_frames AS frame
                JOIN event_store.ledger_records AS record
                  ON record.record_id = frame.record_id
                WHERE record.run_id = $1),
             (SELECT status FROM runtime.model_invocations
                WHERE invocation_id = $2)",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(iteration.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read empty completion facts");
    assert_eq!(facts, (1, 0, 0, 2));
}

#[tokio::test]
async fn retry_backoff_stays_waiting_until_database_deadline_then_resumes_atomically() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = PostgresAgentLoopJournal::new(
        pool.clone(),
        content,
        codec.clone(),
        codec,
        Arc::new(ProtobufMeteringPublicationEncoder),
        keys,
        running_loop::recovery_keyring(),
    );
    let (fixture, attempt) = seed_running_loop(&pool).await;

    let checkpoint = journal
        .fail(
            &attempt,
            ErrorCode::DependencyTimeout,
            true,
            None,
            std::num::NonZeroU32::new(8).expect("limit").into(),
        )
        .await
        .expect("schedule durable retry");
    assert_eq!(checkpoint.phase, LoopPhase::ReadyForModel);
    let waiting: (i16, Option<i16>, i16, i64) = sqlx::query_as(
        "SELECT status, wait_reason, next_attempt_trigger,
                (SELECT count(*) FROM runtime.leases WHERE run_id = run.run_id)
         FROM runtime.runs AS run
         WHERE run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read waiting retry");
    assert_eq!(waiting, (3, Some(4), 2, 0));

    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let before_due = scheduler
        .claim_one(
            &WorkerId::new("retry-worker").expect("worker"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("query scheduler");
    assert!(matches!(before_due, ClaimOutcome::Idle { .. }));

    sqlx::query(
        "UPDATE runtime.runs
         SET not_before_ms =
           floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint - 1
         WHERE run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .execute(&pool)
    .await
    .expect("make retry due");
    let claimed = scheduler
        .claim_one(
            &WorkerId::new("retry-worker").expect("worker"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("claim due retry");
    let ClaimOutcome::Claimed(claimed) = claimed else {
        panic!("due retry must be claimed");
    };
    assert_eq!(claimed.fence.attempt_number.get(), 2);
    let resumed: (i16, Option<i16>, Option<i64>) = sqlx::query_as(
        "SELECT status, wait_reason, not_before_ms
         FROM runtime.runs WHERE run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read resumed retry");
    assert_eq!(resumed, (2, None, None));

    let retry_attempt = AttemptContext {
        attempt_id: claimed.fence.attempt_id,
        run_id: claimed.fence.run_id,
        attempt_number: claimed.fence.attempt_number,
        worker_id: claimed.fence.worker_id,
        lease_id: claimed.fence.lease_id,
        fencing_token: claimed.fence.fencing_token,
        lease_expires_at: claimed.lease_expires_at,
    };
    let second_retry = journal
        .fail(
            &retry_attempt,
            ErrorCode::DependencyUnavailable,
            true,
            None,
            std::num::NonZeroU32::new(8).expect("limit").into(),
        )
        .await
        .expect("schedule second durable retry with attempt-scoped record IDs");
    assert_eq!(second_retry.phase, LoopPhase::ReadyForModel);
    let second_waiting: (i16, Option<i16>, i32, i64) = sqlx::query_as(
        "SELECT run.status, run.wait_reason, run.next_attempt_number,
                (SELECT count(*) FROM runtime.attempts
                 WHERE run_id = run.run_id AND status = 4)
         FROM runtime.runs AS run
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read second retry");
    assert_eq!(second_waiting, (3, Some(4), 3, 2));
}

#[tokio::test]
async fn loop_controls_are_enqueued_consumed_and_ledger_audited() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = audited_loop_journal(
        &pool,
        content.clone(),
        codec.clone(),
        keys,
        running_loop::recovery_keyring(),
    );
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let content_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/plain;charset=utf-8").expect("media"),
            bytes: b"Change the active plan before the next model step.".to_vec(),
        })
        .await
        .expect("store steering content");
    let control_id = CommandId::generate();
    let message_id = MessageId::generate();
    let message_record_id = LedgerRecordId::generate();
    let control_record_id = LedgerRecordId::generate();
    let now = database_now(&pool).await;
    let message_payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Conversation(
        ConversationEvent::MessageAccepted {
            project_task_target: None,
            message_id,
            role: MessageRole::User,
            content_ref: content_ref.clone(),
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
        },
    )));
    let control_payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
        RunEvent::ControlQueued {
            kind: RunControlKind::Steering,
            message_id,
        },
    )));
    let append = AppendRequest {
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
        expected_offset: LedgerOffset::new(1),
        idempotency_key: IdempotencyKey::new("loop-control:enqueue:test").expect("idempotency"),
        command_fingerprint: Sha256::digest(b"loop-control:enqueue:test"),
        records: vec![
            ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
                record_id: message_record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(fixture.activity_id),
                run_id: Some(fixture.run_id),
                run_seq: Some(RunSeq::new(2).expect("run sequence")),
                attempt_id: None,
                participant_id: Some(fixture.requester_id),
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&message_payload),
                payload: message_payload,
            },
            ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
                record_id: control_record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(fixture.activity_id),
                run_id: Some(fixture.run_id),
                run_seq: Some(RunSeq::new(3).expect("run sequence")),
                attempt_id: None,
                participant_id: Some(fixture.requester_id),
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&control_payload),
                payload: control_payload,
            },
        ],
        outbox: Vec::new(),
    };
    PostgresLedgerStore::new(pool.clone())
        .enqueue_run_control(RunControlBatch {
            actor: Actor {
                tenant_id: fixture.tenant_id,
                principal_id: fixture.principal_id,
                principal_kind: PrincipalKind::User,
            },
            append,
            control_id,
            kind: RunControlKind::Steering,
            activity_id: fixture.activity_id,
            run_id: fixture.run_id,
            expected_last_run_sequence: RunSeq::new(1).expect("run sequence"),
            message_record_id,
            control_record_id,
            message: AdmissionMessage {
                project_task_target: None,
                message_id,
                participant_id: fixture.requester_id,
                role: MessageRole::User,
                source_run_id: fixture.run_id,
                content: content_ref,
            },
        })
        .await
        .expect("enqueue steering control");
    let pending: (i16, String, String, String) = sqlx::query_as(
        "SELECT control.status, control.source_conversation_seq::text,
                run.last_run_sequence::text, stream.last_sequence::text
         FROM runtime.loop_controls AS control
         JOIN runtime.runs AS run USING (run_id)
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = run.tenant_id
          AND stream.conversation_id = run.conversation_id
         WHERE control.control_id = $1",
    )
    .bind(control_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read pending control");
    assert_eq!(pending, (1, "2".into(), "3".into(), "3".into()));
    assert_eq!(
        journal.complete(&attempt).await.expect("defer completion"),
        RunCompletionDisposition::ControlsPending,
        "a control that wins the commit lane must prevent Run completion"
    );

    let queue = PostgresLoopControlQueue::new(pool.clone(), codec.clone());
    assert_eq!(
        queue
            .drain_steering(&attempt)
            .await
            .expect("consume steering"),
        1
    );
    let consumed: (i16, Option<uuid::Uuid>, String, String) = sqlx::query_as(
        "SELECT control.status, control.consumed_attempt_id,
                run.last_run_sequence::text, stream.last_sequence::text
         FROM runtime.loop_controls AS control
         JOIN runtime.runs AS run USING (run_id)
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = run.tenant_id
          AND stream.conversation_id = run.conversation_id
         WHERE control.control_id = $1",
    )
    .bind(control_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read consumed control");
    assert_eq!(consumed.0, 2);
    assert_eq!(consumed.1, Some(attempt.attempt_id.into_uuid()));
    assert_eq!(consumed.2, "4");
    assert_eq!(consumed.3, "4");
    let consumed_payload: Vec<u8> = sqlx::query_scalar(
        "SELECT event.encoded_event
         FROM event_store.ledger_records AS record
         JOIN event_store.domain_events AS event USING (record_id)
         WHERE record.run_id = $1 AND record.run_seq = 4",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read control consumption audit");
    assert!(matches!(
        codec.decode(LedgerRecordKind::DomainEvent, &consumed_payload),
        Ok(LedgerPayload::DomainEvent(DomainEvent::Run(
            RunEvent::ControlConsumed {
                kind: RunControlKind::Steering,
                message_ids,
            }
        ))) if message_ids == vec![message_id]
    ));
    assert_eq!(
        queue
            .drain_steering(&attempt)
            .await
            .expect("repeat empty drain"),
        0,
        "an already consumed control must never be reintroduced"
    );
}

#[tokio::test]
async fn loop_crash_mid_tool() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let security_audit = security_audit_pipeline(&pool);
    let journal = audited_tool_journal(
        &pool,
        Arc::new(ProtobufLedgerPayloadCodec),
        keys.clone(),
        Arc::clone(&security_audit),
    );
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: br#"{"query":"rust recovery"}"#.to_vec(),
        })
        .await
        .expect("store Tool request");
    let invocation = ToolInvocation {
        invocation_id: InvocationId::generate(),
        run_id: fixture.run_id,
        requested_by_attempt_id: attempt.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new("call-fenced-search").expect("Tool call ID"),
        tool_key: ToolKey::from_versioned_key(VersionedKey::new("web.search@1").expect("Tool key")),
        capability_provider_id: CapabilityProviderId::generate(),
        status: ToolInvocationStatus::Requested,
        effect: Effect::Read,
        approval: ApprovalRequirement::Never,
        idempotency_key: IdempotencyKey::new("tool:fenced:search").expect("idempotency"),
        request_ref,
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
    };
    let begun = journal
        .load_or_begin(&attempt, invocation.clone(), ToolRetryContract::ReadOnly)
        .await
        .expect("begin Tool");
    assert_eq!(begun.invocation.status, ToolInvocationStatus::Requested);
    assert_eq!(
        journal
            .load_or_begin(&attempt, invocation.clone(), ToolRetryContract::ReadOnly)
            .await
            .expect("duplicate begin"),
        begun
    );

    let started_at = database_now(&pool).await;
    let execution_start = ToolExecutionStart {
        started_at,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
    };
    let started = journal
        .start(
            &attempt,
            invocation.invocation_id,
            ToolStartMode::Initial,
            execution_start.clone(),
        )
        .await
        .expect("start Tool");
    assert_eq!(started.invocation.status, ToolInvocationStatus::Running);
    assert_eq!(
        journal
            .start(
                &attempt,
                invocation.invocation_id,
                ToolStartMode::Initial,
                execution_start.clone(),
            )
            .await
            .expect("duplicate start"),
        started
    );

    let result_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: br#"{"items":["restart-safe"]}"#.to_vec(),
        })
        .await
        .expect("store Tool result");
    let outcome = ToolTerminalOutcome::Succeeded(CapabilityResult {
        result_hash: result_ref.sha256,
        result_ref,
        result_schema: SchemaRef {
            schema_key: VersionedKey::new("web.search.output@1").expect("schema"),
            schema_hash: Sha256::digest(b"web.search.output.schema.v1"),
        },
        provider_request_ref: Some(
            OpaqueExternalRef::new("provider-secret-request-42").expect("provider ref"),
        ),
    });
    let finished_at = database_now(&pool).await;

    sqlx::raw_sql(
        "CREATE FUNCTION tool.reject_result_commit()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected Tool result failure'; END $$;
         CREATE TRIGGER reject_result_commit
         BEFORE INSERT ON tool.tool_results
         FOR EACH ROW EXECUTE FUNCTION tool.reject_result_commit();",
    )
    .execute(&pool)
    .await
    .expect("install failure trigger");
    assert!(
        journal
            .complete(
                &attempt,
                invocation.invocation_id,
                outcome.clone(),
                finished_at,
            )
            .await
            .is_err()
    );
    let rolled_back: (i16, String, String) = sqlx::query_as(
        "SELECT invocation.status, run.last_run_sequence::text,
                stream.last_sequence::text
         FROM tool.tool_invocations AS invocation
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = invocation.tenant_id
          AND stream.conversation_id = invocation.conversation_id
         WHERE invocation.invocation_id = $1",
    )
    .bind(invocation.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rolled-back Tool state");
    assert_eq!(rolled_back, (3, "3".into(), "3".into()));
    assert_no_tool_audit(&pool, fixture.tenant_id, invocation.invocation_id).await;
    sqlx::raw_sql(
        "DROP TRIGGER reject_result_commit ON tool.tool_results;
         DROP FUNCTION tool.reject_result_commit();",
    )
    .execute(&pool)
    .await
    .expect("remove failure trigger");

    drop(journal);
    let restarted = audited_tool_journal(
        &pool,
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
        security_audit,
    );
    let completed = restarted
        .complete(
            &attempt,
            invocation.invocation_id,
            outcome.clone(),
            finished_at,
        )
        .await
        .expect("complete after restart");
    assert_eq!(completed.invocation.status, ToolInvocationStatus::Succeeded);
    assert_eq!(completed.recorded_outcome, Some(outcome.clone()));
    assert_eq!(
        restarted
            .complete(&attempt, invocation.invocation_id, outcome, finished_at,)
            .await
            .expect("duplicate completion"),
        completed
    );

    let durable: (i16, String, String, Option<String>, Vec<u8>) = sqlx::query_as(
        "SELECT invocation.status, run.last_run_sequence::text,
                stream.last_sequence::text, result.provider_request_ref,
                result.provider_request_ref_protected
         FROM tool.tool_invocations AS invocation
         JOIN tool.tool_results AS result USING (invocation_id)
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = invocation.tenant_id
          AND stream.conversation_id = invocation.conversation_id
         WHERE invocation.invocation_id = $1",
    )
    .bind(invocation.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read committed Tool state");
    assert_eq!(durable.0, 4);
    assert_eq!(durable.1, "4");
    assert_eq!(durable.2, "4");
    assert_eq!(durable.3, None);
    assert!(
        !durable
            .4
            .windows("provider-secret-request-42".len())
            .any(|window| window == b"provider-secret-request-42")
    );
    assert_succeeded_tool_audit(&pool, fixture.tenant_id, invocation.invocation_id).await;

    let mut stale_attempt = attempt.clone();
    stale_attempt.fencing_token = FencingToken::new(2).expect("stale token");
    let stale_invocation = ToolInvocation {
        invocation_id: InvocationId::generate(),
        tool_call_id: ToolCallId::new("call-stale").expect("Tool call ID"),
        idempotency_key: IdempotencyKey::new("tool:fenced:stale").expect("idempotency"),
        ..invocation
    };
    let stale = restarted
        .load_or_begin(
            &stale_attempt,
            stale_invocation,
            ToolRetryContract::ReadOnly,
        )
        .await
        .expect_err("stale Worker must be rejected");
    assert_eq!(stale.code, ErrorCode::AttemptLeaseStale);
}

#[tokio::test]
async fn fenced_artifact_commit_is_atomic_restart_safe_and_idempotent() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        codec.clone(),
        codec.clone(),
        keys.clone(),
    );
    let committer = PostgresFencedArtifactToolCommitter::new(pool.clone(), codec, keys.clone());
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: br#"{"filename":"src/main.py","content":"print(1)"}"#.to_vec(),
        })
        .await
        .expect("store request");
    let invocation_id = InvocationId::generate();
    let invocation = ToolInvocation {
        invocation_id,
        run_id: fixture.run_id,
        requested_by_attempt_id: attempt.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new("call-artifact-file").expect("call ID"),
        tool_key: ToolKey::from_versioned_key(
            VersionedKey::new("artifact.file.create@1").expect("Tool key"),
        ),
        capability_provider_id: CapabilityProviderId::generate(),
        status: ToolInvocationStatus::Requested,
        effect: Effect::WriteReversible,
        approval: ApprovalRequirement::Never,
        idempotency_key: IdempotencyKey::new("tool:artifact:file").expect("idempotency"),
        request_ref,
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
    };
    journal
        .load_or_begin(
            &attempt,
            invocation.clone(),
            ToolRetryContract::IdempotentByKey,
        )
        .await
        .expect("begin Artifact Tool");
    journal
        .start(
            &attempt,
            invocation_id,
            ToolStartMode::Initial,
            ToolExecutionStart {
                started_at: database_now(&pool).await,
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
            },
        )
        .await
        .expect("start Artifact Tool");

    let artifact_content = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/x-python").expect("media type"),
            bytes: b"print(1)\n".to_vec(),
        })
        .await
        .expect("store Artifact content");
    let result_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: br#"{"status":"created"}"#.to_vec(),
        })
        .await
        .expect("store Tool result");
    let dependency_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("image/png").expect("media type"),
            bytes: b"\x89PNG\r\n\x1a\npostgres-artifact-preview".to_vec(),
        })
        .await
        .expect("store Artifact dependency");
    let committed_at = database_now(&pool).await;
    let artifact_id = ArtifactId::from_uuid(invocation_id.into_uuid()).expect("Artifact id");
    let artifact_version_id = ArtifactVersionId::generate();
    let command = CommitPreparedArtifactTool {
        resource_replacement: None,
        run_context: run_context(fixture),
        invocation_id,
        result: CapabilityResult {
            result_ref: result_ref.clone(),
            result_hash: result_ref.sha256,
            result_schema: SchemaRef {
                schema_key: VersionedKey::new("artifact.file.create.output@1")
                    .expect("result schema"),
                schema_hash: Sha256::digest(b"artifact.file.create.output.schema.v1"),
            },
            provider_request_ref: None,
        },
        authorized_content: Vec::new(),
        mutation: ArtifactMutation::Create {
            artifact: Artifact {
                artifact_id,
                tenant_id: fixture.tenant_id,
                source: ArtifactSource::Conversation(fixture.conversation_id),
                origin: ArtifactOrigin::Runtime {
                    activity_id: fixture.activity_id,
                    created_by_run_id: fixture.run_id,
                },
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("version"),
                title: "src/main.py".into(),
                created_at: committed_at,
                updated_at: committed_at,
            },
            version: ArtifactVersion {
                artifact_version_id,
                artifact_id,
                version: VersionNumber::new(1).expect("version"),
                content_hash: artifact_content.sha256,
                content_ref: artifact_content,
                dependency_refs: vec![dependency_ref.clone()],
                source_invocation_id: Some(invocation_id),
                committed_by: ArtifactVersionAuthor::Participant(fixture.executor_id),
                committed_at,
            },
        },
        resource_placement: Some(PreparedResourceFilePlacement {
            context: ResourceCommandContext {
                tenant_id: fixture.tenant_id,
                actor: Actor {
                    tenant_id: fixture.tenant_id,
                    principal_id: fixture.principal_id,
                    principal_kind: PrincipalKind::User,
                },
                source: musteragent_runtime_resource::ResourceCommandSource::Conversation {
                    conversation_id: fixture.conversation_id,
                    workspace_id: None,
                },
                invocation_id: Some(invocation_id),
                tool_key: Some(
                    VersionedKey::new("artifact.file.create@1").expect("Resource Tool key"),
                ),
                now: committed_at,
            },
            identity: ResourceMutationIdentity {
                change_set_id: ResourceChangeSetId::from_uuid(invocation_id.into_uuid())
                    .expect("change set id"),
                idempotency_key: IdempotencyKey::new("resource:artifact:file")
                    .expect("Resource idempotency"),
                command_fingerprint: Sha256::digest(b"src/main.py"),
            },
            expected_revision: None,
            node_id: ResourceNodeId::from_uuid(artifact_id.into_uuid()).expect("Resource node id"),
            parent_node_id: None,
            path: vec![
                ResourceName::new("src").expect("folder name"),
                ResourceName::new("main.py").expect("file name"),
            ],
            sort_key: "main.py".into(),
        }),
        committed_at,
    };

    sqlx::raw_sql(
        "CREATE FUNCTION artifact.reject_version_commit()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected Artifact version failure'; END $$;
         CREATE TRIGGER reject_version_commit
         BEFORE INSERT ON artifact.artifact_versions
         FOR EACH ROW EXECUTE FUNCTION artifact.reject_version_commit();",
    )
    .execute(&pool)
    .await
    .expect("install failure trigger");
    assert!(committer.commit(&attempt, command.clone()).await.is_err());
    let rolled_back: (i16, i64, i64, String, String) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM artifact.artifacts WHERE artifact_id = $2),
                (SELECT count(*) FROM resource.nodes WHERE file_artifact_id = $2),
                run.last_run_sequence::text, stream.last_sequence::text
         FROM tool.tool_invocations AS invocation
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = invocation.tenant_id
          AND stream.conversation_id = invocation.conversation_id
         WHERE invocation.invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rollback");
    assert_eq!(rolled_back, (3, 0, 0, "3".into(), "3".into()));
    sqlx::raw_sql(
        "DROP TRIGGER reject_version_commit ON artifact.artifact_versions;
         DROP FUNCTION artifact.reject_version_commit();",
    )
    .execute(&pool)
    .await
    .expect("remove failure trigger");

    drop(committer);
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let restarted =
        PostgresFencedArtifactToolCommitter::new(pool.clone(), codec, Arc::clone(&keys));
    let receipt = restarted
        .commit(&attempt, command.clone())
        .await
        .expect("commit after restart");
    assert_eq!(receipt.disposition, ArtifactCommitDisposition::Committed);
    assert_eq!(receipt.artifact_id, artifact_id);
    let duplicate = restarted
        .commit(&attempt, command)
        .await
        .expect("duplicate commit");
    assert_eq!(duplicate.disposition, ArtifactCommitDisposition::Duplicate);

    let authorization = PostgresArtifactStore::new(pool.clone(), keys);
    let actor = Actor {
        tenant_id: fixture.tenant_id,
        principal_id: fixture.principal_id,
        principal_kind: PrincipalKind::User,
    };
    assert!(
        authorization
            .is_current_content_reachable(actor, fixture.conversation_id, &dependency_ref)
            .await
            .expect("authorize exact current Artifact dependency")
    );
    assert_eq!(
        authorization
            .version_content_reachability(
                actor,
                fixture.conversation_id,
                artifact_id,
                artifact_version_id,
                std::slice::from_ref(&dependency_ref),
            )
            .await
            .expect("authorize exact immutable Artifact version dependency"),
        vec![true]
    );
    assert_eq!(
        authorization
            .resolve_current_content(actor, fixture.conversation_id, dependency_ref.content_id,)
            .await
            .expect("resolve exact current Artifact dependency"),
        Some(dependency_ref)
    );

    let durable: (i16, i64, i64, i64, String, String) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM artifact.artifacts WHERE artifact_id = $2),
                (SELECT count(*) FROM artifact.artifact_versions
                   WHERE artifact_version_id = $3),
                (SELECT count(*) FROM resource.nodes
                   WHERE tenant_id = invocation.tenant_id),
                run.last_run_sequence::text, stream.last_sequence::text
         FROM tool.tool_invocations AS invocation
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = invocation.tenant_id
          AND stream.conversation_id = invocation.conversation_id
         WHERE invocation.invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(artifact_version_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read durable Artifact commit");
    assert_eq!(durable, (4, 1, 1, 2, "6".into(), "6".into()));
}

#[tokio::test]
async fn loop_parallel_tool_transitions_share_one_durable_commit_lane() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal =
        PostgresFencedToolInvocationJournal::new(pool.clone(), codec.clone(), codec, keys);
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: br#"{"query":"parallel durability"}"#.to_vec(),
        })
        .await
        .expect("store Tool request");
    let make_invocation = |suffix: &str| {
        ToolInvocation {
        invocation_id: InvocationId::generate(),
        run_id: fixture.run_id,
        requested_by_attempt_id: attempt.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new(format!("parallel-{suffix}")).expect("Tool call ID"),
        tool_key: ToolKey::from_versioned_key(VersionedKey::new("web.search@1").expect("Tool key")),
        capability_provider_id: CapabilityProviderId::generate(),
        status: ToolInvocationStatus::Requested,
        effect: Effect::Read,
        approval: ApprovalRequirement::Never,
        idempotency_key: IdempotencyKey::new(format!("tool:parallel:{suffix}"))
            .expect("idempotency"),
        request_ref: request_ref.clone(),
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
    }
    };
    let invocations = [
        make_invocation("a"),
        make_invocation("b"),
        make_invocation("c"),
    ];
    let (first, second, third) = tokio::join!(
        journal.load_or_begin(
            &attempt,
            invocations[0].clone(),
            ToolRetryContract::ReadOnly
        ),
        journal.load_or_begin(
            &attempt,
            invocations[1].clone(),
            ToolRetryContract::ReadOnly
        ),
        journal.load_or_begin(
            &attempt,
            invocations[2].clone(),
            ToolRetryContract::ReadOnly
        ),
    );
    for begun in [first, second, third] {
        assert_eq!(
            begun.expect("parallel request commit").invocation.status,
            ToolInvocationStatus::Requested
        );
    }
    let now = database_now(&pool).await;
    let execution_start = ToolExecutionStart {
        started_at: now,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
    };
    let (first, second, third) = tokio::join!(
        journal.start(
            &attempt,
            invocations[0].invocation_id,
            ToolStartMode::Initial,
            execution_start.clone()
        ),
        journal.start(
            &attempt,
            invocations[1].invocation_id,
            ToolStartMode::Initial,
            execution_start.clone()
        ),
        journal.start(
            &attempt,
            invocations[2].invocation_id,
            ToolStartMode::Initial,
            execution_start.clone()
        ),
    );
    for started in [first, second, third] {
        assert_eq!(
            started.expect("parallel start commit").invocation.status,
            ToolInvocationStatus::Running
        );
    }
}

#[tokio::test]
async fn loop_parallel_runs_single_conversation_ledger() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal =
        musteragent_conformance::fixtures::model_journal::supervise(PostgresAgentLoopJournal::new(
            pool.clone(),
            content.clone(),
            codec.clone(),
            codec,
            Arc::new(ProtobufMeteringPublicationEncoder),
            keys,
            running_loop::recovery_keyring(),
        ));
    let (first_fixture, first_attempt) = seed_running_loop(&pool).await;
    let (second_fixture, second_attempt) = seed_parallel_run(&pool, first_fixture).await;
    let first_request = content
        .put(PutContent {
            tenant_id: first_fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"parallel first model request".to_vec(),
        })
        .await
        .expect("store first request");
    let second_request = content
        .put(PutContent {
            tenant_id: second_fixture.tenant_id,
            media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
            bytes: b"parallel second model request".to_vec(),
        })
        .await
        .expect("store second request");
    let mut first_iteration =
        model_iteration(first_fixture, first_attempt.clone(), first_request, 1);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut first_iteration,
    )
    .await;
    let mut second_iteration =
        model_iteration(second_fixture, second_attempt.clone(), second_request, 1);
    second_iteration
        .request
        .invocation
        .run_context
        .activity_kind = ActivityKind::Automation;
    second_iteration
        .request
        .invocation
        .run_context
        .parent_run_id = Some(first_fixture.run_id);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        content.as_ref(),
        &mut second_iteration,
    )
    .await;
    let (first, second) = tokio::join!(
        journal.begin_model(
            &first_attempt,
            &first_iteration,
            ProviderRecoveryCapability::NonResumable,
        ),
        journal.begin_model(
            &second_attempt,
            &second_iteration,
            ProviderRecoveryCapability::NonResumable,
        ),
    );
    assert!(matches!(
        first.expect("first parallel Run"),
        DurableLoopCheckpoint {
            phase: LoopPhase::ModelInFlight { .. },
            ..
        }
    ));
    assert!(matches!(
        second.expect("second parallel Run"),
        DurableLoopCheckpoint {
            phase: LoopPhase::ModelInFlight { .. },
            ..
        }
    ));
    let (first_dispatch, second_dispatch) = tokio::join!(
        journal.mark_model_dispatched(&first_attempt, first_iteration.invocation_id),
        journal.mark_model_dispatched(&second_attempt, second_iteration.invocation_id),
    );
    first_dispatch.expect("mark first parallel model dispatched");
    second_dispatch.expect("mark second parallel model dispatched");
    let sequences: Vec<String> = sqlx::query_scalar(
        "SELECT ledger.conversation_seq::text FROM event_store.ledger_records AS ledger
         WHERE ledger.conversation_id = $1 ORDER BY ledger.conversation_seq",
    )
    .bind(first_fixture.conversation_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read shared Ledger");
    // Each Run has Started, InvocationStarted and a durable liveness admission.
    assert_eq!(sequences, ["1", "2", "3", "4", "5", "6"]);

    let usage = ModelUsage {
        input_tokens_uncached: 12,
        output_tokens: 4,
        total_tokens: Some(16),
        ..ModelUsage::default()
    };
    let append_both = tokio::time::timeout(musteragent_testkit::integration_timeout(), async {
        tokio::join!(
            journal.append_model_events(
                &first_attempt,
                ModelEventBatch {
                    invocation_id: first_iteration.invocation_id,
                    iteration: 1,
                    batch_index: 0,
                    events: vec![
                        ModelStreamEvent::ResponseStarted {
                            response_ref: provider_ref("parallel-first-response"),
                            response_model_key: response_model_key(),
                        },
                        ModelStreamEvent::TextDelta {
                            output_index: 0,
                            delta: ModelText::new("first parallel Run complete").expect("text"),
                        },
                        ModelStreamEvent::Usage(usage),
                        ModelStreamEvent::ResponseCompleted {
                            stop_reason: StopReason::Stop,
                        },
                    ],
                },
            ),
            journal.append_model_events(
                &second_attempt,
                ModelEventBatch {
                    invocation_id: second_iteration.invocation_id,
                    iteration: 1,
                    batch_index: 0,
                    events: vec![
                        ModelStreamEvent::ResponseStarted {
                            response_ref: provider_ref("parallel-second-response"),
                            response_model_key: response_model_key(),
                        },
                        ModelStreamEvent::TextDelta {
                            output_index: 0,
                            delta: ModelText::new("second parallel Run complete").expect("text"),
                        },
                        ModelStreamEvent::Usage(usage),
                        ModelStreamEvent::ResponseCompleted {
                            stop_reason: StopReason::Stop,
                        },
                    ],
                },
            ),
        )
    })
    .await
    .expect("parallel Runs must not starve while journaling model streams");
    append_both.0.expect("append first parallel model batch");
    append_both.1.expect("append second parallel model batch");

    let finish_both = tokio::time::timeout(musteragent_testkit::integration_timeout(), async {
        tokio::join!(
            musteragent_conformance::fixtures::model_journal::finish_model(
                &journal,
                &first_attempt,
                ModelCompletionCommit {
                    invocation_id: first_iteration.invocation_id,
                    kind: ModelCompletionKind::AssistantResponse,
                    tool_calls: Vec::new(),
                    reasoning_replay: None,
                    usage: Some(usage),
                    provider_response_ref: provider_ref("parallel-first-response"),
                    response_model_key: response_model_key(),
                    continuation_ref: None,
                },
            ),
            musteragent_conformance::fixtures::model_journal::finish_model(
                &journal,
                &second_attempt,
                ModelCompletionCommit {
                    invocation_id: second_iteration.invocation_id,
                    kind: ModelCompletionKind::AssistantResponse,
                    tool_calls: Vec::new(),
                    reasoning_replay: None,
                    usage: Some(usage),
                    provider_response_ref: provider_ref("parallel-second-response"),
                    response_model_key: response_model_key(),
                    continuation_ref: None,
                },
            ),
        )
    })
    .await
    .expect("parallel Runs must not starve while committing model terminals");
    assert_eq!(
        finish_both.0.expect("finish first parallel model").phase,
        LoopPhase::ReadyForModel
    );
    assert_eq!(
        finish_both.1.expect("finish second parallel model").phase,
        LoopPhase::ReadyForModel
    );

    let complete_both = tokio::time::timeout(musteragent_testkit::integration_timeout(), async {
        tokio::join!(
            journal.complete(&first_attempt),
            journal.complete(&second_attempt),
        )
    })
    .await
    .expect("parallel Runs must not starve while committing Run terminals");
    assert_eq!(
        completed_checkpoint(complete_both.0.expect("complete first parallel Run")).phase,
        LoopPhase::Completed
    );
    assert_eq!(
        completed_checkpoint(complete_both.1.expect("complete second parallel Run")).phase,
        LoopPhase::Completed
    );

    let final_sequences = sqlx::query_scalar::<_, String>(
        "SELECT ledger.conversation_seq::text FROM event_store.ledger_records AS ledger
         WHERE ledger.conversation_id = $1 ORDER BY ledger.conversation_seq",
    )
    .bind(first_fixture.conversation_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read final shared Ledger")
    .into_iter()
    .map(|value| value.parse::<u64>().expect("Conversation sequence"))
    .collect::<Vec<_>>();
    assert_eq!(
        final_sequences,
        (1..=u64::try_from(final_sequences.len()).expect("sequence count")).collect::<Vec<_>>(),
        "all lifecycle facts from both Runs must share one gap-free Conversation Ledger"
    );
    let run_states: Vec<(uuid::Uuid, i16, String)> = sqlx::query_as(
        "SELECT run_id, status, last_run_sequence::text
         FROM runtime.runs
         WHERE run_id = ANY($1)
         ORDER BY run_id",
    )
    .bind(vec![
        first_fixture.run_id.into_uuid(),
        second_fixture.run_id.into_uuid(),
    ])
    .fetch_all(&pool)
    .await
    .expect("read both parallel Run terminals");
    assert_eq!(run_states.len(), 2);
    assert!(run_states.iter().all(|(_, status, last)| {
        *status == 5 && last.parse::<u64>().is_ok_and(|sequence| sequence >= 3)
    }));
}

#[path = "postgres_agent_loop/liveness_lifecycle.rs"]
mod liveness_lifecycle;
#[tokio::test]
async fn postgres_tool_lifecycle_is_atomic_and_resume_keeps_original_idle_budget() {
    liveness_lifecycle::postgres_tool_lifecycle_is_atomic_and_resume_keeps_original_idle_budget()
        .await;
}

#[tokio::test]
async fn postgres_effect_commit_closes_liveness_atomically_and_replays_original_receipt() {
    liveness_lifecycle::postgres_effect_commit_closes_liveness_atomically_and_replays_original_receipt().await;
}

#[path = "postgres_agent_loop/model_liveness.rs"]
mod model_liveness;
#[tokio::test]
async fn postgres_model_lifecycle_rolls_back_with_its_original_journal_and_replays_terminal() {
    model_liveness::run().await;
}

#[tokio::test]
async fn postgres_model_cancellation_requires_physical_stop_and_commits_with_original_terminal() {
    model_liveness::confirmed_cancellation().await;
}

#[path = "postgres_agent_loop/argument_diagnostics.rs"]
mod argument_diagnostics;

#[path = "postgres_agent_loop/generation_recovery.rs"]
mod generation_recovery;

#[path = "postgres_agent_loop/orphaned_model.rs"]
mod orphaned_model;
#[tokio::test]
async fn postgres_cancel_recovers_orphaned_dispatched_model() {
    orphaned_model::scenario(true, true).await;
}
#[tokio::test]
async fn postgres_cancel_recovers_orphaned_undispatched_model() {
    orphaned_model::scenario(false, true).await;
}
#[tokio::test]
async fn postgres_orphaned_model_recovery_without_cancellation() {
    orphaned_model::scenario(true, false).await;
}

#[tokio::test]
async fn postgres_ready_completion_survives_shutdown_before_link() {
    model_liveness::ready_shutdown().await;
}
#[tokio::test]
async fn postgres_ready_completion_does_not_override_user_stop() {
    model_liveness::ready_stop_denied().await;
}
#[tokio::test]
async fn postgres_ready_completion_requires_canonical_terminal() {
    model_liveness::ready_missing_terminal().await;
}

#[tokio::test]
async fn postgres_ready_completion_does_not_override_pending_user_stop() {
    model_liveness::ready_stopping_denied().await;
}

#[path = "postgres_agent_loop/model_rejection.rs"]
mod model_rejection;

#[path = "support/execution_liveness.rs"]
mod execution_liveness_support;
