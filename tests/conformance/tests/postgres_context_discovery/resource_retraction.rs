//! Original Cloud Ledger + Resource transaction qualification, disposable PG.
use super::*;
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ResourceChangeSetId, ResourceEditSessionId,
    ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision, VersionNumber, WorkspaceId,
};
use musteragent_postgres::PostgresResourceStore;
use musteragent_resource::ResourceName;
use musteragent_runtime_file_query::{
    FileQueryContext, FileQueryError, FileRetractionStatusSource,
};
use musteragent_runtime_resource::*;
use musteragent_runtime_session::file_retraction::ManagedResourceRetractionPlan;

fn workspace(f: &Fixture) -> WorkspaceId {
    WorkspaceId::from_uuid(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        f.conversation_id.to_string().as_bytes(),
    ))
    .expect("isolated workspace")
}
async fn assign_workspace(f: &Fixture) {
    sqlx::query("UPDATE conversation.conversations SET workspace_id=$1 WHERE conversation_id=$2")
        .bind(workspace(f).into_uuid())
        .bind(f.conversation_id.into_uuid())
        .execute(&f.pool)
        .await
        .expect("fixture project");
}
fn identity() -> ResourceMutationIdentity {
    let id = ResourceChangeSetId::generate();
    ResourceMutationIdentity {
        change_set_id: id,
        idempotency_key: IdempotencyKey::new(id.to_string()).expect("key"),
        command_fingerprint: Sha256::digest(id.to_string()),
    }
}
struct File {
    node: ResourceNodeId,
    artifact: ArtifactId,
    invocation: InvocationId,
    context: ResourceCommandContext,
}
async fn create_file(f: &Fixture, run: &SeededRun, revision: u64, name: &str) -> File {
    let invocation = InvocationId::generate();
    let now = database_now(&f.pool).await;
    let body = f
        .content
        .put(PutContent {
            tenant_id: f.tenant_id,
            media_type: MediaType::new("text/plain").expect("media"),
            bytes: b"original creation".to_vec(),
        })
        .await
        .expect("content");
    let provider = run.snapshot.tools[0].manifest.capability_provider_id;
    sqlx::query("INSERT INTO tool.tool_invocations(invocation_id,tenant_id,conversation_id,run_id,requested_by_attempt_id,execution_attempt_id,tool_call_id,tool_ref,capability_provider_id,status,effect,approval_policy,retry_contract,idempotency_key,request_content_id,request_hash,deadline_ms,started_at_ms) VALUES($1,$2,$3,$4,$5,$5,$6,'resource.file.create@1',$7,3,2,1,2,$6,$8,$9,$10,$11)")
        .bind(invocation.into_uuid()).bind(f.tenant_id.into_uuid()).bind(f.conversation_id.into_uuid()).bind(run.attempt.run_id.into_uuid()).bind(run.attempt.attempt_id.into_uuid()).bind(invocation.to_string()).bind(provider.into_uuid()).bind(body.content_id.into_uuid()).bind(body.sha256.into_bytes().to_vec()).bind(now.unix_millis()+60000).bind(now.unix_millis()).execute(&f.pool).await.expect("source invocation");
    let context = ResourceCommandContext {
        tenant_id: f.tenant_id,
        actor: f.actor,
        source: ResourceCommandSource::Conversation {
            conversation_id: f.conversation_id,
            workspace_id: Some(workspace(f)),
        },
        invocation_id: Some(invocation),
        tool_key: Some(VersionedKey::new("resource.file.create@1").expect("tool")),
        now,
    };
    let artifact = ArtifactId::generate();
    let node = ResourceNodeId::generate();
    ResourceApplication::new(PostgresResourceStore::new(f.pool.clone()))
        .create_file(CreateResourceFile {
            context: context.clone(),
            identity: identity(),
            expected_revision: ResourceRevision::new(revision),
            node_id: node,
            parent_node_id: None,
            name: ResourceName::new(name).expect("name"),
            sort_key: name.into(),
            artifact: Artifact {
                artifact_id: artifact,
                tenant_id: f.tenant_id,
                source: ArtifactSource::Conversation(f.conversation_id),
                origin: ArtifactOrigin::Runtime {
                    activity_id: run.activity_id,
                    created_by_run_id: run.attempt.run_id,
                },
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("version"),
                title: name.into(),
                created_at: now,
                updated_at: now,
            },
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id: artifact,
                version: VersionNumber::new(1).expect("version"),
                content_ref: body.clone(),
                content_hash: body.sha256,
                dependency_refs: vec![],
                source_invocation_id: Some(invocation),
                committed_by: ArtifactVersionAuthor::Participant(f.executor_id),
                committed_at: now,
            },
        })
        .await
        .expect("original atomic creation");
    File {
        node,
        artifact,
        invocation,
        context,
    }
}
fn begin(_f: &Fixture, file: &File, mut context: ResourceCommandContext) -> BeginFileEdit {
    context.invocation_id = None;
    context.tool_key = None;
    let key = ResourceEditSessionId::generate();
    BeginFileEdit {
        context,
        session_id: key,
        file_node_id: file.node,
        base_version: VersionNumber::new(1).expect("base"),
        format_adapter: VersionedKey::new("text.utf8@1").expect("format"),
        idempotency_key: IdempotencyKey::new(key.to_string()).expect("key"),
        command_fingerprint: Sha256::digest(key.to_string()),
        expires_at: UtcTimestamp::from_unix_millis(file.context.now.unix_millis() + 60000),
    }
}
async fn command(
    f: &Fixture,
    run: &SeededRun,
    plan: ManagedResourceRetractionPlan,
) -> RetractMessageCommit {
    let store =
        PostgresRuntimeApplicationStore::new(f.pool.clone(), Arc::new(ProtobufSnapshotCodec));
    let authority = store
        .authorize_retract_message(
            f.actor,
            f.conversation_id,
            run.message_id,
            CorrelationId::generate(),
        )
        .await
        .expect("authority");
    let now = database_now(&f.pool).await;
    let records = authority
        .targets
        .iter()
        .map(|target| {
            let payload = ProtobufLedgerPayloadCodec.encode(LedgerPayload::DomainEvent(
                DomainEvent::Conversation(ConversationEvent::MessageRetracted {
                    message_id: target.message_id,
                    reason: SafeDisplayText::new("withdraw creation").expect("reason"),
                    source_run_id: target.source_run_id,
                }),
            ));
            ProposedRecord {
                schema_version: DottedKey::new(musteragent_wire::RECORD_SCHEMA_VERSION)
                    .expect("schema"),
                record_id: LedgerRecordId::generate(),
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: None,
                run_id: None,
                run_seq: None,
                attempt_id: None,
                participant_id: Some(authority.requested_by_participant_id),
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&payload),
                payload,
            }
        })
        .collect();
    RetractMessageCommit {
        managed_resources: Some(plan),
        actor: f.actor,
        retracted_at: now,
        targets: authority.targets,
        append: AppendRequest {
            tenant_id: f.tenant_id,
            conversation_id: f.conversation_id,
            expected_home_epoch: authority.home_epoch,
            expected_offset: authority.current_offset,
            idempotency_key: IdempotencyKey::new(CommandId::generate().to_string()).expect("key"),
            command_fingerprint: Sha256::digest("original Resource retraction"),
            records,
            outbox: vec![],
        },
    }
}
fn query(f: &Fixture) -> FileQueryContext {
    FileQueryContext {
        dialogue_source: None,
        actor: f.actor,
        conversation_id: Some(f.conversation_id),
        project_workspace_id: Some(workspace(f)),
        now: UtcTimestamp::from_unix_millis(1),
        authority_revision: "current".into(),
        content_allowlist: None,
    }
}
#[tokio::test]
async fn original_resource_retraction_is_atomic_replayable_and_preserves_changed_or_referenced_files()
 {
    let f = Fixture::start().await;
    assign_workspace(&f).await;
    let run = f
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    let first = create_file(&f, &run, 0, "remove.txt").await;
    let renamed = create_file(&f, &run, 1, "rename.txt").await;
    let retained = create_file(&f, &run, 2, "referenced.txt").await;
    let changed = create_file(&f, &run, 3, "edited.txt").await;
    let editing = create_file(&f, &run, 4, "editing.txt").await;
    let resources = PostgresResourceStore::new(f.pool.clone());
    let edits = FileEditApplication::new(resources.clone(), f.content.clone());
    for file in [&changed, &editing] {
        let opened = begin(&f, file, file.context.clone());
        edits
            .begin(opened.clone())
            .await
            .expect("begin original user edit");
        if file.node == changed.node {
            let mut user = file.context.clone();
            user.invocation_id = None;
            user.tool_key = None;
            edits
                .finalize(FinalizeFileEdit {
                    context: user,
                    command_id: ResourceEditTerminalCommandId::generate(),
                    session_id: opened.session_id,
                    artifact_version_id: ArtifactVersionId::generate(),
                    committed_by: ArtifactVersionAuthor::Participant(f.requester_id),
                    idempotency_key: IdempotencyKey::new("user-finalize").expect("key"),
                    expected_session_revision: ResourceRevision::ZERO,
                })
                .await
                .expect("original version publish");
        }
    }
    let mut user = renamed.context.clone();
    user.invocation_id = None;
    user.tool_key = None;
    ResourceApplication::new(resources.clone())
        .rename_node(RenameResourceNode {
            context: user,
            identity: identity(),
            expected_revision: ResourceRevision::new(5),
            node_id: renamed.node,
            name: ResourceName::new("user-renamed.txt").expect("name"),
        })
        .await
        .expect("user rename");
    f.finish(&run).await;
    let commit = command(
        &f,
        &run,
        ManagedResourceRetractionPlan {
            invocation_ids: [
                first.invocation,
                renamed.invocation,
                retained.invocation,
                changed.invocation,
                editing.invocation,
            ]
            .into_iter()
            .collect(),
            retained_artifact_ids: [retained.artifact].into_iter().collect(),
        },
    )
    .await;
    let store =
        PostgresRuntimeApplicationStore::new(f.pool.clone(), Arc::new(ProtobufSnapshotCodec));
    // Failure after message mutation and Resource side effects must roll back all
    // of them. A PostgreSQL constraint trigger emulates an owner persistence fault.
    sqlx::raw_sql("CREATE FUNCTION resource.reject_retraction_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture owner failure'; END $$; CREATE TRIGGER reject_retraction_fixture BEFORE INSERT ON resource.file_retraction_outcomes FOR EACH ROW WHEN (NEW.disposition=1) EXECUTE FUNCTION resource.reject_retraction_fixture();").execute(&f.pool).await.expect("fixture failure trigger");
    assert!(
        store
            .retract_message(CorrelationId::generate(), commit.clone())
            .await
            .is_err()
    );
    let active: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resource.nodes WHERE tenant_id=$1 AND kind=2 AND lifecycle=1",
    )
    .bind(f.tenant_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .expect("active files");
    assert_eq!(active, 5);
    assert_eq!(
        current_offset(&f.pool, f.tenant_id, f.conversation_id).await,
        commit.append.expected_offset
    );
    sqlx::query("DROP TRIGGER reject_retraction_fixture ON resource.file_retraction_outcomes")
        .execute(&f.pool)
        .await
        .expect("remove fixture trigger");
    let receipt = store
        .retract_message(CorrelationId::generate(), commit.clone())
        .await
        .expect("atomic retract");
    assert_eq!(
        receipt.disposition,
        musteragent_ledger::AppendDisposition::Committed
    );
    let status = resources
        .retraction_status(&query(&f))
        .await
        .expect("status")
        .expect("managed source");
    assert_eq!(
        (status.applied, status.retained, status.conflict),
        (1, 1, 3)
    );
    let duplicate = store
        .retract_message(CorrelationId::generate(), commit)
        .await
        .expect("retry after lost ACK");
    assert_eq!(
        duplicate.disposition,
        musteragent_ledger::AppendDisposition::Duplicate
    );
    assert_eq!(
        resources
            .retraction_status(&query(&f))
            .await
            .expect("status"),
        Some(status)
    );
    let state: i16 = sqlx::query_scalar("SELECT lifecycle FROM resource.nodes WHERE node_id=$1")
        .bind(first.node.into_uuid())
        .fetch_one(&f.pool)
        .await
        .expect("original trash");
    assert_eq!(state, 2);
    let versions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifact.artifact_versions WHERE artifact_id=$1")
            .bind(first.artifact.into_uuid())
            .fetch_one(&f.pool)
            .await
            .expect("retained immutable version");
    assert_eq!(versions, 1);
    let mut denied = query(&f);
    denied.actor.principal_id = PrincipalId::generate();
    assert_eq!(
        resources.retraction_status(&denied).await,
        Err(FileQueryError::Denied)
    );
}

#[tokio::test]
async fn concurrent_editor_cannot_open_a_session_after_original_resource_retraction() {
    let f = Fixture::start().await;
    assign_workspace(&f).await;
    let run = f
        .seed_run(ApiFormat::OpenAiResponses, bootstrap_keys())
        .await;
    let file = create_file(&f, &run, 0, "race.txt").await;
    f.finish(&run).await;
    let commit = command(
        &f,
        &run,
        ManagedResourceRetractionPlan {
            invocation_ids: [file.invocation].into_iter().collect(),
            retained_artifact_ids: BTreeSet::new(),
        },
    )
    .await;
    // A second Conversation edits the same Project file. Its authority lock is
    // intentionally distinct, so only the original Resource node lock can fence
    // this race; a same-Conversation test would miss the defect.
    let other = ConversationId::generate();
    let owner = ParticipantId::generate();
    let mut tx = f.pool.begin().await.expect("other conversation");
    sqlx::query("INSERT INTO conversation.conversations(conversation_id,tenant_id,kind,title,control_home_kind,home_epoch,lifecycle,created_by,created_at_ms,updated_at_ms,workspace_id,execution_owner_participant_id) VALUES($1,$2,1,'Other editor',1,1,1,$3,1,1,$4,$5)")
        .bind(other.into_uuid()).bind(f.tenant_id.into_uuid()).bind(f.actor.principal_id.into_uuid()).bind(workspace(&f).into_uuid()).bind(owner.into_uuid()).execute(&mut *tx).await.expect("other root");
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,agent_ref,display_name,joined_at_ms) VALUES($1,$2,$3,2,(SELECT agent_id FROM agent.agent_definitions WHERE agent_key='assistant'),'Agent',1)")
        .bind(owner.into_uuid()).bind(f.tenant_id.into_uuid()).bind(other.into_uuid()).execute(&mut *tx).await.expect("other executor");
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,display_name,joined_at_ms) VALUES($1,$2,$3,1,$4,'User',1)")
        .bind(ParticipantId::generate().into_uuid()).bind(f.tenant_id.into_uuid()).bind(other.into_uuid()).bind(f.actor.principal_id.into_uuid()).execute(&mut *tx).await.expect("other user");
    tx.commit().await.expect("other authority");
    let mut editor_context = file.context.clone();
    editor_context.source = ResourceCommandSource::Conversation {
        conversation_id: other,
        workspace_id: Some(workspace(&f)),
    };
    let open = begin(&f, &file, editor_context);
    let mut barrier = f.pool.acquire().await.expect("barrier connection");
    sqlx::query("SELECT pg_advisory_lock(772096)")
        .execute(&mut *barrier)
        .await
        .expect("hold test barrier");
    sqlx::raw_sql("CREATE FUNCTION resource.pause_retraction_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(772096); RETURN NEW; END $$; CREATE TRIGGER pause_retraction_fixture BEFORE INSERT ON resource.file_retraction_outcomes FOR EACH ROW WHEN(NEW.disposition=1) EXECUTE FUNCTION resource.pause_retraction_fixture();").execute(&f.pool).await.expect("owner barrier");
    let store =
        PostgresRuntimeApplicationStore::new(f.pool.clone(), Arc::new(ProtobufSnapshotCodec));
    let retract = tokio::spawn(async move {
        store
            .retract_message(CorrelationId::generate(), commit)
            .await
    });
    let mut reached = false;
    for _ in 0..200 {
        reached=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=772096 AND NOT granted)").fetch_one(&f.pool).await.expect("owner checkpoint");
        if reached {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(reached, "original file owner reached its commit boundary");
    let edits = FileEditApplication::new(
        PostgresResourceStore::new(f.pool.clone()),
        f.content.clone(),
    );
    let mut edit = tokio::spawn(async move { edits.begin(open).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(75), &mut edit)
            .await
            .is_err(),
        "editor must wait for the original node decision"
    );
    sqlx::query("SELECT pg_advisory_unlock(772096)")
        .execute(&mut *barrier)
        .await
        .expect("release original owner");
    retract
        .await
        .expect("retraction task")
        .expect("commit original retraction");
    assert!(
        edit.await.expect("editor task").is_err(),
        "the original node is now in the trash"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resource.edit_sessions WHERE file_node_id=$1")
            .bind(file.node.into_uuid())
            .fetch_one(&f.pool)
            .await
            .expect("no orphan session");
    assert_eq!(count, 0);
}
