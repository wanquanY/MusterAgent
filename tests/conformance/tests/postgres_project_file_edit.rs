#![cfg(test)]

#[path = "support/file_events.rs"]
mod file_events;
#[path = "postgres_project_file_edit/workspace_assignment.rs"]
mod workspace_assignment;
#[path = "postgres_project_file_edit/workspace_principal.rs"]
mod workspace_principal;
use std::sync::Arc;

#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;

use resource_support::*;

use musteragent_content_ref::{MediaType, SchemaRef, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, IdempotencyKey, ResourceEditOperationId, ResourceEditSessionId,
    ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision, UtcTimestamp, VersionNumber,
    VersionedKey, WorkspaceId,
};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{
    PostgresContentStore, PostgresFencedFileEditToolCommitter, PostgresFencedToolInvocationJournal,
    PostgresResourceStore,
};
use musteragent_resource::{EditSessionState, TextEditOperation, TextRange};
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_resource::{
    AbortFileEdit, ApplyFileEdits, BeginFileEdit, CommitPreparedFileEditTool,
    FencedFileEditToolCommitter, FileEditApplication, FileEditError, FinalizeFileEdit,
    LinkResourceFile, PreparedFileEditMutation, PreparedFileEditToolResults, ResourceApplication,
};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufLedgerPayloadCodec;

#[tokio::test]
async fn file_edit_tool_commit_rolls_back_and_replays_as_one_fenced_unit() {
    file_edit_tool_commit_with_assignment_barrier(false).await;
}

#[tokio::test]
async fn accepted_tool_edit_drains_through_assignment_barrier_atomically() {
    file_edit_tool_commit_with_assignment_barrier(true).await;
}

async fn file_edit_tool_commit_with_assignment_barrier(barrier: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let workspace_id = WorkspaceId::generate();
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id = $1
          WHERE tenant_id = $2 AND conversation_id = $3",
    )
    .bind((!barrier).then(|| workspace_id.into_uuid()))
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("attach running Conversation to Project");

    let source_context = |mut context: musteragent_runtime_resource::ResourceCommandContext| {
        if barrier
            && let musteragent_runtime_resource::ResourceCommandSource::Conversation {
                workspace_id: target,
                ..
            } = &mut context.source
        {
            *target = None;
        }
        context
    };
    if barrier {
        workspace_assignment::prepare_for_fixture(&pool, fixture, workspace_id).await;
    }

    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        Arc::clone(&keys),
    ));
    let base = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/markdown;charset=utf-8").expect("Markdown media type"),
            bytes: b"# Durable\n".to_vec(),
        })
        .await
        .expect("persist base File bytes");
    let artifact_id = ArtifactId::generate();
    sqlx::query(
        "INSERT INTO artifact.artifacts (
             artifact_id, tenant_id, conversation_id, activity_id,
             created_by_run_id, kind, lifecycle, current_version, title,
             created_at_ms, updated_at_ms
         ) VALUES ($1, $2, $3, $4, $5, 6, 1, 1, 'README.md', 1, 1)",
    )
    .bind(artifact_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .execute(&pool)
    .await
    .expect("seed File Artifact");
    sqlx::query(
        "INSERT INTO artifact.artifact_versions (
             artifact_version_id, artifact_id, version, content_id,
             content_hash, source_invocation_id, committed_by, committed_at_ms
         ) VALUES ($1, $2, 1, $3, $4, NULL, $5, 1)",
    )
    .bind(ArtifactVersionId::generate().into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(base.content_id.into_uuid())
    .bind(base.sha256.into_bytes().to_vec())
    .bind(fixture.executor_id.into_uuid())
    .execute(&pool)
    .await
    .expect("seed File Artifact version");
    let file_node_id = ResourceNodeId::generate();
    ResourceApplication::new(PostgresResourceStore::new(pool.clone()))
        .link_file(LinkResourceFile {
            context: source_context(context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                1,
            )),
            identity: mutation("link-atomic-edit-file"),
            expected_revision: ResourceRevision::ZERO,
            node_id: file_node_id,
            parent_node_id: None,
            name: name("README.md"),
            sort_key: "README.md".to_owned(),
            file_artifact_id: artifact_id,
        })
        .await
        .expect("place File in Project tree");

    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        codec.clone(),
        codec.clone(),
        Arc::clone(&keys),
    );
    let invocation_id =
        start_file_edit_tool(&pool, &content, &journal, &attempt, fixture, file_node_id).await;
    let session_id = ResourceEditSessionId::from_uuid(invocation_id.into_uuid())
        .expect("derived edit Session ID");
    let committed_at = running_loop::database_now(&pool).await;
    let result_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: format!(
                r#"{{"session_id":"{session_id}","state":"open","session_revision":0,"next_sequence":1}}"#
            )
            .into_bytes(),
        })
        .await
        .expect("persist File Edit result");
    let command = CommitPreparedFileEditTool {
        run_context: running_loop::run_context(fixture),
        invocation_id,
        results: PreparedFileEditToolResults {
            succeeded: CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new("resource.file.edit.begin.output@1")
                        .expect("result schema"),
                    schema_hash: Sha256::digest(b"resource.file.edit.begin.output.v1"),
                },
                provider_request_ref: None,
            },
            conflicted: None,
        },
        mutation: PreparedFileEditMutation::Begin(BeginFileEdit {
            context: source_context(file_edit_tool_context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                invocation_id,
                committed_at,
            )),
            session_id,
            file_node_id,
            base_version: VersionNumber::new(1).expect("base version"),
            format_adapter: VersionedKey::new("text.utf8@1").expect("format adapter"),
            idempotency_key: IdempotencyKey::new("file-edit-tool:begin:atomic")
                .expect("File Edit idempotency"),
            command_fingerprint: Sha256::digest(b"file-edit-tool:begin:atomic"),
            expires_at: UtcTimestamp::from_unix_millis(committed_at.unix_millis() + 60_000),
        }),
        committed_at,
    };
    let committer =
        PostgresFencedFileEditToolCommitter::new(pool.clone(), codec, Arc::clone(&keys));

    sqlx::raw_sql(
        "CREATE FUNCTION tool.reject_file_edit_result_commit()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected File Edit result failure'; END $$;
         CREATE TRIGGER reject_file_edit_result_commit
         BEFORE INSERT ON tool.tool_results
         FOR EACH ROW EXECUTE FUNCTION tool.reject_file_edit_result_commit();",
    )
    .execute(&pool)
    .await
    .expect("install File Edit commit failure trigger");
    assert!(committer.commit(&attempt, command.clone()).await.is_err());
    let rolled_back: (i16, i64, String, String) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM resource.edit_sessions WHERE session_id = $2),
                run.last_run_sequence::text, stream.last_sequence::text
           FROM tool.tool_invocations AS invocation
           JOIN runtime.runs AS run ON run.run_id = invocation.run_id
           JOIN event_store.ledger_streams AS stream
             ON stream.tenant_id = invocation.tenant_id
            AND stream.conversation_id = invocation.conversation_id
          WHERE invocation.invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(session_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read atomically rolled-back File Edit state");
    assert_eq!(rolled_back.0, 3, "Tool must remain Running after rollback");
    assert_eq!(rolled_back.1, 0, "Edit Session must roll back");
    assert_eq!(
        rolled_back.2, rolled_back.3,
        "Ledger and Run must not advance"
    );

    sqlx::raw_sql(
        "DROP TRIGGER reject_file_edit_result_commit ON tool.tool_results;
         DROP FUNCTION tool.reject_file_edit_result_commit();",
    )
    .execute(&pool)
    .await
    .expect("remove File Edit commit failure trigger");
    drop(committer);
    let restarted = PostgresFencedFileEditToolCommitter::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
    );
    let committed = restarted
        .commit(&attempt, command.clone())
        .await
        .expect("commit File Edit after restart");
    assert!(matches!(
        committed,
        musteragent_runtime_resource::FileEditMutationReceipt::Begun(_)
    ));
    let replayed = restarted
        .commit(&attempt, command)
        .await
        .expect("replay committed File Edit Tool");
    assert!(matches!(
        replayed,
        musteragent_runtime_resource::FileEditMutationReceipt::Begun(_)
    ));
    let durable: (i16, i64, i64) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM resource.edit_sessions WHERE session_id = $2),
                (SELECT count(*) FROM event_store.command_idempotency
                  WHERE tenant_id = $3 AND idempotency_key = $4)
           FROM tool.tool_invocations AS invocation
          WHERE invocation.invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(session_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(format!("file-edit-tool:{invocation_id}"))
    .fetch_one(&pool)
    .await
    .expect("read durable replay state");
    assert_eq!(durable, (4, 1, 1));
    assert!(
        file_events::artifacts(&pool, fixture.run_id)
            .await
            .is_empty(),
        "private edit checkpoint is not a committed file"
    );
    let finalize_invocation = start_resource_tool_with_contract(
        &pool,
        &content,
        &journal,
        &attempt,
        fixture,
        "finalize",
        "resource.file.edit.finalize@1",
        b"{}".to_vec(),
    )
    .await;
    let now = running_loop::database_now(&pool).await;
    let result_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").unwrap(),
            bytes: b"{\"state\":\"committed\"}".to_vec(),
        })
        .await
        .unwrap();
    let terminal = FinalizeFileEdit {
        context: source_context(file_edit_tool_context(
            fixture.tenant_id,
            fixture.principal_id,
            fixture.conversation_id,
            workspace_id,
            finalize_invocation,
            now,
        )),
        command_id: ResourceEditTerminalCommandId::generate(),
        session_id,
        artifact_version_id: ArtifactVersionId::generate(),
        committed_by: musteragent_artifact::ArtifactVersionAuthor::Participant(fixture.executor_id),
        idempotency_key: IdempotencyKey::new("finalize-event-regression").unwrap(),
        expected_session_revision: ResourceRevision::ZERO,
    };
    let fingerprint = musteragent_runtime_resource::file_edit_terminal_fingerprint(
        "finalize",
        terminal.command_id,
        session_id,
        ResourceRevision::ZERO,
        Some(terminal.artifact_version_id),
        Some(terminal.committed_by),
    )
    .unwrap();
    let command = CommitPreparedFileEditTool {
        run_context: running_loop::run_context(fixture),
        invocation_id: finalize_invocation,
        results: PreparedFileEditToolResults {
            succeeded: CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new("resource.file.edit.finalize.output@1").unwrap(),
                    schema_hash: Sha256::digest(b"finalize output"),
                },
                provider_request_ref: None,
            },
            conflicted: None,
        },
        mutation: PreparedFileEditMutation::Finalize {
            command: terminal,
            command_fingerprint: fingerprint,
        },
        committed_at: now,
    };
    restarted
        .commit(&attempt, command.clone())
        .await
        .expect("publish version");
    restarted
        .commit(&attempt, command)
        .await
        .expect("replay publication");
    let events = file_events::artifacts(&pool, fixture.run_id).await;
    let mut legacy = file_events::legacy_snapshot(
        fixture,
        finalize_invocation,
        base.clone(),
        Some(artifact_id),
    );
    musteragent_postgres::PostgresArtifactStore::recover_resource_file_projection(
        &pool,
        &mut legacy,
    )
    .await
    .unwrap();
    assert_eq!(legacy.artifacts[0].current_version.get(), 2);
    let musteragent_artifact::ArtifactEvent::VersionCommitted { content_ref, .. } = &events[0]
    else {
        panic!("version fact");
    };
    assert_eq!(
        &legacy.artifacts[0].current_content_ref, content_ref,
        "recovery uses the immutable committed version, including a no-content-change commit"
    );
    let mut unrelated =
        file_events::legacy_snapshot(fixture, finalize_invocation, base.clone(), None);
    musteragent_postgres::PostgresArtifactStore::recover_resource_file_projection(
        &pool,
        &mut unrelated,
    )
    .await
    .unwrap();
    assert!(
        unrelated.artifacts.is_empty(),
        "editing an existing project file does not invent conversation membership"
    );

    assert_eq!(
        events.len(),
        1,
        "finalize publishes exactly one version fact"
    );
    assert!(
        matches!(&events[0], musteragent_artifact::ArtifactEvent::VersionCommitted {
        artifact_id: id, version, source_invocation_id: Some(invocation), ..
    } if *id == artifact_id && version.get() == 2 && *invocation == finalize_invocation)
    );
}

#[tokio::test]
async fn file_edit_session_is_durable_atomic_idempotent_and_conflict_aware() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let workspace_id = WorkspaceId::generate();
    sqlx::query(
        "UPDATE conversation.conversations
            SET workspace_id = $1
          WHERE tenant_id = $2 AND conversation_id = $3",
    )
    .bind(workspace_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("attach Conversation to Project");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        running_loop::keyring(),
    ));
    let base = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/x-python").expect("Python media type"),
            bytes: b"print('old')\n".to_vec(),
        })
        .await
        .expect("persist base File bytes");
    let artifact_id = ArtifactId::generate();
    sqlx::query(
        "INSERT INTO artifact.artifacts (
             artifact_id, tenant_id, conversation_id, activity_id,
             created_by_run_id, kind, lifecycle, current_version, title,
             created_at_ms, updated_at_ms
         ) VALUES ($1, $2, $3, $4, $5, 6, 1, 1, 'main.py', 1, 1)",
    )
    .bind(artifact_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .execute(&pool)
    .await
    .expect("seed File Artifact");
    sqlx::query(
        "INSERT INTO artifact.artifact_versions (
             artifact_version_id, artifact_id, version, content_id,
             content_hash, source_invocation_id, committed_by, committed_at_ms
         ) VALUES ($1, $2, 1, $3, $4, NULL, $5, 1)",
    )
    .bind(ArtifactVersionId::generate().into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(base.content_id.into_uuid())
    .bind(base.sha256.into_bytes().to_vec())
    .bind(fixture.executor_id.into_uuid())
    .execute(&pool)
    .await
    .expect("seed File Artifact version");

    let now = running_loop::database_now(&pool).await;
    let file_node_id = ResourceNodeId::generate();
    ResourceApplication::new(PostgresResourceStore::new(pool.clone()))
        .link_file(LinkResourceFile {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                now.unix_millis(),
            ),
            identity: mutation("link-edit-file"),
            expected_revision: ResourceRevision::ZERO,
            node_id: file_node_id,
            parent_node_id: None,
            name: name("main.py"),
            sort_key: "main.py".to_owned(),
            file_artifact_id: artifact_id,
        })
        .await
        .expect("place File in Project tree");

    let edits = FileEditApplication::new(
        PostgresResourceStore::new(pool.clone()),
        Arc::clone(&content),
    );
    let session_id = ResourceEditSessionId::generate();
    let begin = BeginFileEdit {
        context: context(
            fixture.tenant_id,
            fixture.principal_id,
            fixture.conversation_id,
            workspace_id,
            now.unix_millis(),
        ),
        session_id,
        file_node_id,
        base_version: VersionNumber::new(1).expect("base version"),
        format_adapter: VersionedKey::new("text.utf8@1").expect("format adapter"),
        idempotency_key: IdempotencyKey::new("edit:begin:1").expect("idempotency"),
        command_fingerprint: Sha256::digest(b"edit:begin:1"),
        expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 60_000),
    };
    let opened = edits
        .begin(begin.clone())
        .await
        .expect("begin durable edit");
    assert_eq!(opened.session.state, EditSessionState::Open);
    assert_eq!(opened.session.session_revision, ResourceRevision::ZERO);
    assert_eq!(opened.checkpoint_ref, base);
    let replayed_begin = edits.begin(begin.clone()).await.expect("replay begin");
    assert_eq!(replayed_begin.session.session_id, session_id);

    let apply = ApplyFileEdits {
        context: context(
            fixture.tenant_id,
            fixture.principal_id,
            fixture.conversation_id,
            workspace_id,
            now.unix_millis() + 1,
        ),
        session_id,
        operation_id: ResourceEditOperationId::generate(),
        idempotency_key: IdempotencyKey::new("edit:apply:1").expect("idempotency"),
        expected_session_revision: ResourceRevision::ZERO,
        expected_sequence: 1,
        operations: vec![TextEditOperation::Replace {
            range: TextRange {
                start_byte: 6,
                end_byte: 11,
            },
            expected_hash: Sha256::digest(b"'old'"),
            text: "'new'".to_owned(),
        }],
    };
    let checkpoint = edits.apply(apply.clone()).await.expect("apply exact edit");
    assert_eq!(checkpoint.session_revision, ResourceRevision::new(1));
    assert_eq!(checkpoint.next_sequence, 2);
    assert_eq!(
        content
            .read(fixture.tenant_id, &checkpoint.checkpoint_ref)
            .await
            .expect("read checkpoint"),
        b"print('new')\n"
    );
    let replayed_apply = edits
        .apply(apply.clone())
        .await
        .expect("replay lost apply ACK");
    assert!(replayed_apply.duplicate);
    assert_eq!(replayed_apply.checkpoint_ref, checkpoint.checkpoint_ref);

    // Existing edit FKs now reference historical ownership. They must not make
    // a handle issued in an earlier workspace a new-write capability.
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id = $1 WHERE conversation_id = $2",
    )
    .bind(WorkspaceId::generate().into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("scope change fixture");
    assert!(
        edits
            .apply(apply.clone())
            .await
            .expect("old checkpoint receipt")
            .duplicate
    );
    assert_eq!(
        edits
            .begin(begin.clone())
            .await
            .expect("old begin receipt")
            .session
            .session_id,
        session_id
    );
    let mut stale_begin = begin.clone();
    stale_begin.session_id = ResourceEditSessionId::generate();
    stale_begin.idempotency_key = IdempotencyKey::new("stale-context-begin").unwrap();
    stale_begin.command_fingerprint = Sha256::digest(b"stale-context-begin");
    assert_eq!(
        edits.begin(stale_begin).await.unwrap_err(),
        FileEditError::SessionConflict
    );
    let mut stale_apply = apply.clone();
    stale_apply.operation_id = ResourceEditOperationId::generate();
    stale_apply.idempotency_key = IdempotencyKey::new("stale-context-checkpoint").unwrap();
    stale_apply.expected_session_revision = ResourceRevision::new(1);
    stale_apply.expected_sequence = 2;
    stale_apply.operations = vec![TextEditOperation::Replace {
        range: TextRange {
            start_byte: 6,
            end_byte: 11,
        },
        expected_hash: Sha256::digest(b"'new'"),
        text: "'bad'".to_owned(),
    }];
    assert_eq!(
        edits.apply(stale_apply).await.unwrap_err(),
        FileEditError::SessionConflict
    );
    assert_eq!(
        edits
            .finalize(FinalizeFileEdit {
                context: apply.context.clone(),
                command_id: ResourceEditTerminalCommandId::generate(),
                session_id,
                artifact_version_id: ArtifactVersionId::generate(),
                committed_by: musteragent_artifact::ArtifactVersionAuthor::Participant(
                    fixture.executor_id
                ),
                idempotency_key: IdempotencyKey::new("stale-context-finalize").unwrap(),
                expected_session_revision: ResourceRevision::new(1),
            })
            .await
            .unwrap_err(),
        FileEditError::SessionConflict
    );
    assert_eq!(
        edits
            .abort(AbortFileEdit {
                context: apply.context.clone(),
                command_id: ResourceEditTerminalCommandId::generate(),
                session_id,
                idempotency_key: IdempotencyKey::new("stale-context-abort").unwrap(),
                expected_session_revision: ResourceRevision::new(1),
            })
            .await
            .unwrap_err(),
        FileEditError::SessionConflict
    );
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id = $1 WHERE conversation_id = $2",
    )
    .bind(workspace_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("restore fixture for version-conflict checks");

    let recovered = FileEditApplication::new(
        PostgresResourceStore::new(pool.clone()),
        Arc::clone(&content),
    )
    .resume(
        context(
            fixture.tenant_id,
            fixture.principal_id,
            fixture.conversation_id,
            workspace_id,
            now.unix_millis() + 2,
        ),
        session_id,
    )
    .await
    .expect("resume after application restart");
    assert_eq!(recovered.checkpoint_ref, checkpoint.checkpoint_ref);
    assert_eq!(recovered.session.next_sequence, 2);

    let finalize = FinalizeFileEdit {
        context: context(
            fixture.tenant_id,
            fixture.principal_id,
            fixture.conversation_id,
            workspace_id,
            now.unix_millis() + 3,
        ),
        command_id: ResourceEditTerminalCommandId::generate(),
        session_id,
        artifact_version_id: ArtifactVersionId::generate(),
        committed_by: musteragent_artifact::ArtifactVersionAuthor::Participant(fixture.executor_id),
        idempotency_key: IdempotencyKey::new("edit:finalize:1").expect("idempotency"),
        expected_session_revision: ResourceRevision::new(1),
    };
    let committed = edits
        .finalize(finalize.clone())
        .await
        .expect("atomically finalize File version");
    assert_eq!(committed.state, EditSessionState::Committed);
    assert_eq!(committed.file_version.map(VersionNumber::get), Some(2));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT current_version::text FROM artifact.artifacts WHERE artifact_id = $1",
        )
        .bind(artifact_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read committed version"),
        "2"
    );
    let replayed_finalize = edits
        .finalize(finalize)
        .await
        .expect("replay lost finalize ACK");
    assert!(replayed_finalize.duplicate);

    let stale_begin = edits
        .begin(BeginFileEdit {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                now.unix_millis() + 4,
            ),
            session_id: ResourceEditSessionId::generate(),
            file_node_id,
            base_version: VersionNumber::new(1).expect("stale base version"),
            format_adapter: VersionedKey::new("text.utf8@1").expect("format adapter"),
            idempotency_key: IdempotencyKey::new("edit:begin:stale").expect("idempotency"),
            command_fingerprint: Sha256::digest(b"edit:begin:stale"),
            expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 60_000),
        })
        .await
        .expect_err("reject stale File Edit baseline");
    assert_eq!(stale_begin, FileEditError::BaseVersionConflict);

    let conflict_session_id = ResourceEditSessionId::generate();
    edits
        .begin(BeginFileEdit {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                now.unix_millis() + 4,
            ),
            session_id: conflict_session_id,
            file_node_id,
            base_version: VersionNumber::new(2).expect("base version"),
            format_adapter: VersionedKey::new("text.utf8@1").expect("format adapter"),
            idempotency_key: IdempotencyKey::new("edit:begin:conflict").expect("idempotency"),
            command_fingerprint: Sha256::digest(b"edit:begin:conflict"),
            expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 60_000),
        })
        .await
        .expect("open concurrent-baseline edit");
    let external = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/x-python").expect("Python media type"),
            bytes: b"print('external')\n".to_vec(),
        })
        .await
        .expect("persist external version");
    sqlx::query(
        "INSERT INTO artifact.artifact_versions (
             artifact_version_id, artifact_id, version, content_id,
             content_hash, source_invocation_id, committed_by, committed_at_ms
         ) VALUES ($1, $2, 3, $3, $4, NULL, $5, $6)",
    )
    .bind(ArtifactVersionId::generate().into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(external.content_id.into_uuid())
    .bind(external.sha256.into_bytes().to_vec())
    .bind(fixture.executor_id.into_uuid())
    .bind(now.unix_millis() + 5)
    .execute(&pool)
    .await
    .expect("commit external immutable version");
    sqlx::query(
        "UPDATE artifact.artifacts SET current_version = 3, updated_at_ms = $1
          WHERE artifact_id = $2 AND current_version = 2",
    )
    .bind(now.unix_millis() + 5)
    .bind(artifact_id.into_uuid())
    .execute(&pool)
    .await
    .expect("advance external current version");
    let conflicted = edits
        .finalize(FinalizeFileEdit {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                now.unix_millis() + 6,
            ),
            command_id: ResourceEditTerminalCommandId::generate(),
            session_id: conflict_session_id,
            artifact_version_id: ArtifactVersionId::generate(),
            committed_by: musteragent_artifact::ArtifactVersionAuthor::Participant(
                fixture.executor_id,
            ),
            idempotency_key: IdempotencyKey::new("edit:finalize:conflict").expect("idempotency"),
            expected_session_revision: ResourceRevision::ZERO,
        })
        .await
        .expect("persist exact baseline conflict");
    assert_eq!(conflicted.state, EditSessionState::Conflicted);
    assert_eq!(conflicted.file_version, None);

    let abort_session_id = ResourceEditSessionId::generate();
    edits
        .begin(BeginFileEdit {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                now.unix_millis() + 7,
            ),
            session_id: abort_session_id,
            file_node_id,
            base_version: VersionNumber::new(3).expect("base version"),
            format_adapter: VersionedKey::new("text.utf8@1").expect("format adapter"),
            idempotency_key: IdempotencyKey::new("edit:begin:abort").expect("idempotency"),
            command_fingerprint: Sha256::digest(b"edit:begin:abort"),
            expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 60_000),
        })
        .await
        .expect("open abortable edit");
    let abort = AbortFileEdit {
        context: context(
            fixture.tenant_id,
            fixture.principal_id,
            fixture.conversation_id,
            workspace_id,
            now.unix_millis() + 8,
        ),
        command_id: ResourceEditTerminalCommandId::generate(),
        session_id: abort_session_id,
        idempotency_key: IdempotencyKey::new("edit:abort:1").expect("idempotency"),
        expected_session_revision: ResourceRevision::ZERO,
    };
    let aborted = edits
        .abort(abort.clone())
        .await
        .expect("abort durable edit");
    assert_eq!(aborted.state, EditSessionState::Aborted);
    assert!(edits.abort(abort).await.expect("replay abort").duplicate);
}
