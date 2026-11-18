use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use std::sync::Arc;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProposal,
    ArtifactProposalStatus, ArtifactVersion,
};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, Actor, ArtifactId, ArtifactProposalId, ArtifactVersionId, AttemptId,
    CapabilityProviderId, ConversationId, CorrelationId, DottedKey, HomeEpoch, IdempotencyKey,
    InvocationId, LedgerOffset, LedgerRecordId, ParticipantId, PrincipalId, PrincipalKind, RunId,
    RunSeq, TenantId, ToolCallId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_ledger::{AppendRequest, LedgerPayload, LedgerPayloadEncoder, ProposedRecord};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_postgres::{
    PostgresArtifactError, PostgresArtifactProposalStore, PostgresArtifactStore,
    PostgresContentStore, PostgresToolInvocationStore,
};
use musteragent_runtime_artifact::{
    ApplyArtifactProposal, ArtifactApplication, ArtifactCommitDisposition, ArtifactMutation,
    ArtifactProposalDisposition, ArtifactProposalStore, ArtifactReader, ArtifactSemanticRecords,
    ArtifactStore, CommitToolArtifact, GetArtifact, ListArtifacts, ListConversationArtifacts,
    ProposeArtifactEdit,
};
use musteragent_runtime_capability::{
    CapabilityResult, ToolInvocationStore, ToolPersistenceDisposition,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract};

struct Fixture {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    run_id: RunId,
    participant_id: ParticipantId,
    principal_id: PrincipalId,
    activity_id: ActivityId,
    keyring: Arc<OpaqueRefKeyring>,
}

#[derive(Debug)]
struct TestLedgerPayloadEncoder;

impl LedgerPayloadEncoder for TestLedgerPayloadEncoder {
    fn encode(&self, payload: LedgerPayload) -> Vec<u8> {
        format!("{payload:?}").into_bytes()
    }
}

fn fixture() -> Fixture {
    let key_id = DottedKey::new("runtime.artifact.test").expect("key id");
    Fixture {
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        run_id: RunId::generate(),
        participant_id: ParticipantId::generate(),
        principal_id: PrincipalId::generate(),
        activity_id: ActivityId::generate(),
        keyring: Arc::new(
            OpaqueRefKeyring::new(key_id.clone(), [(key_id, [41_u8; 32])])
                .expect("reference keyring"),
        ),
    }
}

async fn seed_public_viewer(pool: &sqlx::PgPool, fixture: &Fixture) {
    let execution_owner_id = ParticipantId::generate();
    let mut participant_transaction = pool.begin().await.expect("begin participant fixture");
    sqlx::query(
        "INSERT INTO conversation.conversations (
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms,
             execution_owner_participant_id
         ) VALUES ($1, $2, 1, NULL, 1, NULL, 1, 1, $3, 1, 1, NULL, $4)",
    )
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.principal_id.into_uuid())
    .bind(execution_owner_id.into_uuid())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed public Artifact Conversation");
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES
             ($1, $2, $3, 1, $4, NULL, NULL, 'Artifact Viewer', NULL, 1, NULL),
             ($5, $2, $3, 2, NULL,
              (SELECT agent_id FROM agent.agent_definitions WHERE agent_key = 'assistant'),
              NULL, 'Kernel', NULL, 1, NULL)",
    )
    .bind(fixture.participant_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.principal_id.into_uuid())
    .bind(execution_owner_id.into_uuid())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed public Artifact participant");
    participant_transaction
        .commit()
        .await
        .expect("commit participant fixture");
}

async fn stored_content(pool: &sqlx::PgPool, fixture: &Fixture, bytes: &[u8]) -> ContentRef {
    stored_content_with_media(pool, fixture, "application/json", bytes).await
}

async fn stored_content_with_media(
    pool: &sqlx::PgPool,
    fixture: &Fixture,
    media_type: &str,
    bytes: &[u8],
) -> ContentRef {
    PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        fixture.keyring.clone(),
    )
    .put(PutContent {
        tenant_id: fixture.tenant_id,
        media_type: MediaType::new(media_type).expect("media type"),
        bytes: bytes.to_vec(),
    })
    .await
    .expect("store Artifact content")
}

async fn running_invocation(
    pool: &sqlx::PgPool,
    fixture: &Fixture,
    invocation_id: InvocationId,
    key: &str,
) {
    let store = PostgresToolInvocationStore::new(pool.clone(), fixture.keyring.clone());
    let invocation = ToolInvocation {
        invocation_id,
        run_id: fixture.run_id,
        requested_by_attempt_id: AttemptId::generate(),
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new(format!("call-{invocation_id}")).expect("call ID"),
        tool_key: ToolKey::from_versioned_key(
            VersionedKey::new("artifact.note.create@1").expect("Tool key"),
        ),
        capability_provider_id: CapabilityProviderId::generate(),
        status: ToolInvocationStatus::Requested,
        effect: Effect::WriteReversible,
        approval: ApprovalRequirement::Never,
        idempotency_key: IdempotencyKey::new(key).expect("idempotency key"),
        request_ref: stored_content_with_media(pool, fixture, "application/json", b"{}").await,
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(10_000) },
    };
    assert_eq!(
        store
            .begin(
                fixture.tenant_id,
                fixture.conversation_id,
                invocation,
                ToolRetryContract::IdempotentByKey,
            )
            .await
            .expect("begin Tool"),
        ToolPersistenceDisposition::Applied
    );
    store
        .start(
            fixture.tenant_id,
            invocation_id,
            AttemptId::generate(),
            UtcTimestamp::from_unix_millis(2),
        )
        .await
        .expect("start Tool");
}

fn result(content: &ContentRef) -> CapabilityResult {
    CapabilityResult {
        result_ref: content.clone(),
        result_hash: content.sha256,
        result_schema: SchemaRef {
            schema_key: VersionedKey::new("artifact.note.output@1").expect("schema"),
            schema_hash: Sha256::digest(b"artifact-note-output-schema"),
        },
        provider_request_ref: None,
    }
}

fn records(ids: &[LedgerRecordId], run_id: RunId, first_run_sequence: u64) -> Vec<ProposedRecord> {
    ids.iter()
        .enumerate()
        .map(|(index, record_id)| {
            let payload = format!("semantic-event-{index}").into_bytes();
            ProposedRecord {
                schema_version: DottedKey::new("musteragent.legacy.event.v1").expect("schema"),
                record_id: *record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: None,
                run_id: Some(run_id),
                run_seq: Some(
                    RunSeq::new(
                        first_run_sequence
                            .checked_add(u64::try_from(index).expect("bounded sequence"))
                            .expect("run sequence bound"),
                    )
                    .expect("run sequence"),
                ),
                attempt_id: None,
                participant_id: None,
                producer: Producer::Worker,
                occurred_at: UtcTimestamp::from_unix_millis(3),
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&payload),
                payload,
            }
        })
        .collect()
}

fn append(
    fixture: &Fixture,
    offset: LedgerOffset,
    key: &str,
    ids: &[LedgerRecordId],
) -> AppendRequest {
    AppendRequest {
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
        expected_offset: offset,
        idempotency_key: IdempotencyKey::new(key).expect("idempotency"),
        command_fingerprint: Sha256::digest(format!("fingerprint:{key}")),
        records: records(
            ids,
            fixture.run_id,
            offset.get().checked_add(1).expect("run sequence bound"),
        ),
        outbox: vec![],
    }
}

#[tokio::test]
async fn create_update_replay_and_tool_terminal_fact_commit_atomically() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let fixture = fixture();
    seed_public_viewer(&pool, &fixture).await;
    let store = PostgresArtifactStore::new(pool.clone(), fixture.keyring.clone())
        .with_payload_encoder(Arc::new(TestLedgerPayloadEncoder));
    let artifact_id = ArtifactId::generate();

    let invocation_id = InvocationId::generate();
    running_invocation(&pool, &fixture, invocation_id, "tool:note:create").await;
    let content = stored_content(&pool, &fixture, br#"{"title":"One"}"#).await;
    let dependency = stored_content(&pool, &fixture, br#"{"child":"One"}"#).await;
    let output = stored_content(
        &pool,
        &fixture,
        br#"{"artifact":{"artifact_type":"note","status":"ready"}}"#,
    )
    .await;
    let tool_record = LedgerRecordId::generate();
    let created_record = LedgerRecordId::generate();
    let version_record = LedgerRecordId::generate();
    let command = CommitToolArtifact {
        tenant_id: fixture.tenant_id,
        invocation_id,
        result: result(&output),
        mutation: ArtifactMutation::Create {
            artifact: Artifact {
                artifact_id,
                tenant_id: fixture.tenant_id,
                source: ArtifactSource::Conversation(fixture.conversation_id),
                origin: ArtifactOrigin::Runtime {
                    activity_id: fixture.activity_id,
                    created_by_run_id: fixture.run_id,
                },
                kind: ArtifactKind::Note,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("version"),
                title: "One".into(),
                created_at: UtcTimestamp::from_unix_millis(3),
                updated_at: UtcTimestamp::from_unix_millis(3),
            },
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id,
                version: VersionNumber::new(1).expect("version"),
                content_ref: content.clone(),
                dependency_refs: vec![dependency.clone()],
                content_hash: content.sha256,
                source_invocation_id: Some(invocation_id),
                committed_by: ArtifactVersionAuthor::Participant(fixture.participant_id),
                committed_at: UtcTimestamp::from_unix_millis(3),
            },
        },
        append: append(
            &fixture,
            LedgerOffset::ZERO,
            "artifact:note:create",
            &[tool_record, created_record, version_record],
        ),
        semantic_records: ArtifactSemanticRecords {
            tool_succeeded_record_id: tool_record,
            artifact_created_record_id: Some(created_record),
            version_committed_record_id: version_record,
        },
    };
    let receipt = store
        .commit_tool_artifact(command.clone())
        .await
        .expect("commit create");
    assert_eq!(receipt.disposition, ArtifactCommitDisposition::Committed);
    assert_eq!(receipt.ledger.last_sequence.get(), 3);
    let duplicate = store
        .commit_tool_artifact(command)
        .await
        .expect("replay create");
    assert_eq!(duplicate.disposition, ArtifactCommitDisposition::Duplicate);

    let invocation_id = InvocationId::generate();
    running_invocation(&pool, &fixture, invocation_id, "tool:note:update").await;
    let content = stored_content(&pool, &fixture, br#"{"title":"Two"}"#).await;
    let output = stored_content(
        &pool,
        &fixture,
        br#"{"artifact":{"artifact_type":"note","status":"ready","version":2}}"#,
    )
    .await;
    let tool_record = LedgerRecordId::generate();
    let version_record = LedgerRecordId::generate();
    let update = CommitToolArtifact {
        tenant_id: fixture.tenant_id,
        invocation_id,
        result: result(&output),
        mutation: ArtifactMutation::Update {
            artifact_id,
            expected_version: VersionNumber::new(1).expect("version"),
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id,
                version: VersionNumber::new(2).expect("version"),
                content_ref: content.clone(),
                dependency_refs: Vec::new(),
                content_hash: content.sha256,
                source_invocation_id: Some(invocation_id),
                committed_by: ArtifactVersionAuthor::Participant(fixture.participant_id),
                committed_at: UtcTimestamp::from_unix_millis(4),
            },
            title: Some("Two".into()),
        },
        append: append(
            &fixture,
            LedgerOffset::new(3),
            "artifact:note:update",
            &[tool_record, version_record],
        ),
        semantic_records: ArtifactSemanticRecords {
            tool_succeeded_record_id: tool_record,
            artifact_created_record_id: None,
            version_committed_record_id: version_record,
        },
    };
    store
        .commit_tool_artifact(update)
        .await
        .expect("commit update");

    let view = store
        .load(fixture.tenant_id, artifact_id)
        .await
        .expect("load Artifact");
    assert_eq!(view.artifact.title, "Two");
    assert_eq!(view.artifact.current_version.get(), 2);
    assert_eq!(view.versions.len(), 2);
    assert_eq!(view.versions[0].version.get(), 1);
    assert_eq!(view.versions[1].version.get(), 2);
    assert_eq!(view.versions[0].dependency_refs, vec![dependency.clone()]);
    assert!(view.versions[1].dependency_refs.is_empty());
    assert_ne!(
        view.versions[0].content_ref.content_id,
        view.versions[1].content_ref.content_id
    );
    assert!(
        ArtifactReader::load_for_conversation(
            &store,
            fixture.tenant_id,
            ConversationId::generate(),
            artifact_id,
        )
        .await
        .expect("Conversation-scoped Artifact lookup")
        .is_none(),
        "an Artifact UUID from another Conversation is not read authority",
    );
    assert!(matches!(
        store.load(TenantId::generate(), artifact_id).await,
        Err(PostgresArtifactError::NotFound)
    ));
    assert_eq!(
        ArtifactStore::list(
            &store,
            ListArtifacts {
                tenant_id: fixture.tenant_id,
                conversation_id: fixture.conversation_id,
                limit: 20,
                before_artifact_id: None,
            },
        )
        .await
        .expect("list")
        .len(),
        1
    );
    let actor = Actor {
        tenant_id: fixture.tenant_id,
        principal_id: fixture.principal_id,
        principal_kind: PrincipalKind::User,
    };
    let public_view = store
        .get_artifact(GetArtifact { actor, artifact_id })
        .await
        .expect("get authorized Artifact");
    assert_eq!(public_view.versions.len(), 2);
    assert_eq!(public_view.versions[0].dependency_refs, vec![dependency]);
    let public_page = store
        .list_conversation_artifacts(ListConversationArtifacts {
            actor,
            conversation_id: fixture.conversation_id,
            kinds: Vec::new(),
            search_query: None,
            limit: 20,
            before_artifact_id: None,
        })
        .await
        .expect("list authorized Artifacts");
    assert_eq!(public_page.artifacts.len(), 1);
    assert_eq!(public_page.artifacts[0].current_version.version.get(), 2);
    assert!(!public_page.has_more);
    let unauthorized = store
        .get_artifact(GetArtifact {
            actor: Actor {
                tenant_id: fixture.tenant_id,
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            artifact_id,
        })
        .await
        .expect_err("hide Artifact from non-participant");
    assert_eq!(
        unauthorized.code,
        musteragent_runtime_contract::ErrorCode::ArtifactNotFound
    );

    let tool_states: Vec<i16> = sqlx::query_scalar(
        "SELECT status FROM tool.tool_invocations
         WHERE tenant_id = $1 ORDER BY finished_at_ms",
    )
    .bind(fixture.tenant_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read Tool states");
    assert_eq!(tool_states, vec![4, 4]);
    pool.close().await;
}

#[tokio::test]
async fn failure_after_artifact_insert_rolls_back_tool_artifact_and_ledger() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let fixture = fixture();
    let store = PostgresArtifactStore::new(pool.clone(), fixture.keyring.clone());
    let invocation_id = InvocationId::generate();
    running_invocation(&pool, &fixture, invocation_id, "tool:note:rollback").await;
    let content = stored_content(&pool, &fixture, br#"{"title":"Rollback"}"#).await;
    let output = stored_content(
        &pool,
        &fixture,
        br#"{"artifact":{"artifact_type":"note","status":"ready"}}"#,
    )
    .await;
    let artifact_id = ArtifactId::generate();
    let ids = [
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
    ];
    let command = CommitToolArtifact {
        tenant_id: fixture.tenant_id,
        invocation_id,
        result: result(&output),
        mutation: ArtifactMutation::Create {
            artifact: Artifact {
                artifact_id,
                tenant_id: fixture.tenant_id,
                source: ArtifactSource::Conversation(fixture.conversation_id),
                origin: ArtifactOrigin::Runtime {
                    activity_id: fixture.activity_id,
                    created_by_run_id: fixture.run_id,
                },
                kind: ArtifactKind::Note,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("version"),
                title: "Rollback".into(),
                created_at: UtcTimestamp::from_unix_millis(3),
                updated_at: UtcTimestamp::from_unix_millis(3),
            },
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id,
                version: VersionNumber::new(1).expect("version"),
                content_ref: content.clone(),
                dependency_refs: Vec::new(),
                content_hash: content.sha256,
                source_invocation_id: Some(invocation_id),
                committed_by: ArtifactVersionAuthor::Participant(fixture.participant_id),
                committed_at: UtcTimestamp::from_unix_millis(3),
            },
        },
        append: append(&fixture, LedgerOffset::ZERO, "artifact:note:rollback", &ids),
        semantic_records: ArtifactSemanticRecords {
            tool_succeeded_record_id: ids[0],
            artifact_created_record_id: Some(ids[1]),
            version_committed_record_id: ids[2],
        },
    };
    sqlx::raw_sql(
        "CREATE FUNCTION artifact.reject_tool_result()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected artifact commit failure'; END $$;
         CREATE TRIGGER reject_tool_result
         BEFORE INSERT ON tool.tool_results
         FOR EACH ROW EXECUTE FUNCTION artifact.reject_tool_result();",
    )
    .execute(&pool)
    .await
    .expect("install failure trigger");
    assert!(store.commit_tool_artifact(command).await.is_err());

    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
           (SELECT count(*) FROM artifact.artifacts WHERE artifact_id = $1),
           (SELECT count(*) FROM artifact.artifact_versions WHERE artifact_id = $1),
           (SELECT count(*) FROM event_store.ledger_records
              WHERE conversation_id = $2)",
    )
    .bind(artifact_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rollback state");
    assert_eq!(counts, (0, 0, 0));
    let tool_status: i16 =
        sqlx::query_scalar("SELECT status FROM tool.tool_invocations WHERE invocation_id = $1")
            .bind(invocation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("read Tool state");
    assert_eq!(tool_status, 3);
    pool.close().await;
}

#[tokio::test]
async fn proposal_stays_candidate_until_atomic_apply_and_replays_safely() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let fixture = fixture();
    let artifact_store = PostgresArtifactStore::new(pool.clone(), fixture.keyring.clone());
    let proposal_store = PostgresArtifactProposalStore::new(pool.clone());
    let invocation_id = InvocationId::generate();
    running_invocation(&pool, &fixture, invocation_id, "tool:note:create:proposal").await;
    let original = stored_content(&pool, &fixture, br#"{"title":"Original"}"#).await;
    let output = stored_content(
        &pool,
        &fixture,
        br#"{"artifact":{"artifact_type":"note","status":"ready"}}"#,
    )
    .await;
    let artifact_id = ArtifactId::generate();
    let create_records = [
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
    ];
    artifact_store
        .commit_tool_artifact(CommitToolArtifact {
            tenant_id: fixture.tenant_id,
            invocation_id,
            result: result(&output),
            mutation: ArtifactMutation::Create {
                artifact: Artifact {
                    artifact_id,
                    tenant_id: fixture.tenant_id,
                    source: ArtifactSource::Conversation(fixture.conversation_id),
                    origin: ArtifactOrigin::Runtime {
                        activity_id: fixture.activity_id,
                        created_by_run_id: fixture.run_id,
                    },
                    kind: ArtifactKind::Note,
                    lifecycle: ArtifactLifecycle::Draft,
                    current_version: VersionNumber::new(1).expect("version"),
                    title: "Original".into(),
                    created_at: UtcTimestamp::from_unix_millis(3),
                    updated_at: UtcTimestamp::from_unix_millis(3),
                },
                version: ArtifactVersion {
                    artifact_version_id: ArtifactVersionId::generate(),
                    artifact_id,
                    version: VersionNumber::new(1).expect("version"),
                    content_ref: original.clone(),
                    dependency_refs: Vec::new(),
                    content_hash: original.sha256,
                    source_invocation_id: Some(invocation_id),
                    committed_by: ArtifactVersionAuthor::Participant(fixture.participant_id),
                    committed_at: UtcTimestamp::from_unix_millis(3),
                },
            },
            append: append(
                &fixture,
                LedgerOffset::ZERO,
                "artifact:proposal:create-target",
                &create_records,
            ),
            semantic_records: ArtifactSemanticRecords {
                tool_succeeded_record_id: create_records[0],
                artifact_created_record_id: Some(create_records[1]),
                version_committed_record_id: create_records[2],
            },
        })
        .await
        .expect("create proposal target");

    let candidate = stored_content(&pool, &fixture, br#"{"title":"Accepted candidate"}"#).await;
    let proposal_id = ArtifactProposalId::generate();
    let proposed_record_id = LedgerRecordId::generate();
    let propose = ProposeArtifactEdit {
        proposal: ArtifactProposal {
            proposal_id,
            tenant_id: fixture.tenant_id,
            artifact_id,
            base_version: VersionNumber::new(1).expect("version"),
            proposal_ref: candidate.clone(),
            proposal_hash: candidate.sha256,
            source_invocation_id: None,
            proposed_by: fixture.participant_id,
            proposed_at: UtcTimestamp::from_unix_millis(4),
            status: ArtifactProposalStatus::Pending,
            resolved_by: None,
            resolved_at: None,
        },
        append: append(
            &fixture,
            LedgerOffset::new(3),
            "artifact:proposal:propose",
            &[proposed_record_id],
        ),
        edit_proposed_record_id: proposed_record_id,
    };
    let proposed = proposal_store
        .propose(propose.clone())
        .await
        .expect("propose edit");
    assert_eq!(proposed.disposition, ArtifactProposalDisposition::Committed);
    assert_eq!(proposed.status, ArtifactProposalStatus::Pending);
    let before_apply = artifact_store
        .load(fixture.tenant_id, artifact_id)
        .await
        .expect("load target before apply");
    assert_eq!(before_apply.artifact.current_version.get(), 1);
    assert_eq!(before_apply.versions[0].content_ref, original);

    let applied_record_id = LedgerRecordId::generate();
    let version_record_id = LedgerRecordId::generate();
    let applied_at = UtcTimestamp::from_unix_millis(5);
    let apply = ApplyArtifactProposal {
        tenant_id: fixture.tenant_id,
        proposal_id,
        artifact_id,
        expected_base_version: VersionNumber::new(1).expect("version"),
        version: ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            artifact_id,
            version: VersionNumber::new(2).expect("version"),
            content_ref: candidate.clone(),
            dependency_refs: Vec::new(),
            content_hash: candidate.sha256,
            source_invocation_id: None,
            committed_by: ArtifactVersionAuthor::Participant(fixture.participant_id),
            committed_at: applied_at,
        },
        title: Some("Accepted candidate".into()),
        applied_by: fixture.participant_id,
        applied_at,
        append: append(
            &fixture,
            LedgerOffset::new(4),
            "artifact:proposal:apply",
            &[applied_record_id, version_record_id],
        ),
        edit_applied_record_id: applied_record_id,
        version_committed_record_id: version_record_id,
    };
    let applied = proposal_store
        .apply(apply.clone())
        .await
        .expect("apply proposal");
    assert_eq!(applied.disposition, ArtifactProposalDisposition::Committed);
    assert_eq!(applied.status, ArtifactProposalStatus::Applied);
    let duplicate = proposal_store.apply(apply).await.expect("replay apply");
    assert_eq!(
        duplicate.disposition,
        ArtifactProposalDisposition::Duplicate
    );
    let after_apply = artifact_store
        .load(fixture.tenant_id, artifact_id)
        .await
        .expect("load target after apply");
    assert_eq!(after_apply.artifact.current_version.get(), 2);
    assert_eq!(after_apply.artifact.title, "Accepted candidate");
    assert_eq!(after_apply.versions[1].content_ref, candidate);
    let persisted = proposal_store
        .load_proposal(fixture.tenant_id, proposal_id)
        .await
        .expect("load applied proposal");
    assert_eq!(persisted.status, ArtifactProposalStatus::Applied);
    assert_eq!(persisted.resolved_by, Some(fixture.participant_id));
    assert!(
        proposal_store
            .load_proposal(TenantId::generate(), proposal_id)
            .await
            .is_err(),
        "proposal reads are tenant isolated"
    );
    pool.close().await;
}
