use super::*;
use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
};
use musteragent_identity::{CommandId, HomeEpoch};
use musteragent_postgres::{
    PostgresWorkspaceAssignmentPreparationStore, PostgresWorkspaceResourceRootAuthority,
};
use musteragent_runtime_resource::{
    CreateResourceFile, EnsureWorkspaceResourceRoot, WorkspaceResourceRootAuthority,
};
use musteragent_runtime_session::workspace_assignment::{
    PrepareWorkspaceAssignment, WorkspaceAssignmentPreparationStore,
};

#[tokio::test]
async fn assignment_blocks_new_editors_but_existing_editor_can_finish_and_replay() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let now = running_loop::database_now(&pool).await;
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        running_loop::keyring(),
    ));
    let base = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"original".to_vec(),
        })
        .await
        .unwrap();
    let target = WorkspaceId::generate();
    let mut source = context(
        fixture.tenant_id,
        fixture.principal_id,
        fixture.conversation_id,
        target,
        now.unix_millis(),
    );
    if let musteragent_runtime_resource::ResourceCommandSource::Conversation {
        workspace_id, ..
    } = &mut source.source
    {
        *workspace_id = None;
    }
    let artifact_id = ArtifactId::generate();
    let node = ResourceNodeId::generate();
    ResourceApplication::new(PostgresResourceStore::new(pool.clone()))
        .create_file(CreateResourceFile {
            context: source.clone(),
            identity: mutation("barrier-source"),
            expected_revision: ResourceRevision::ZERO,
            node_id: node,
            parent_node_id: None,
            name: name("source.txt"),
            sort_key: "source.txt".into(),
            artifact: Artifact {
                artifact_id,
                tenant_id: fixture.tenant_id,
                source: ArtifactSource::Conversation(fixture.conversation_id),
                origin: ArtifactOrigin::Product,
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).unwrap(),
                title: "source.txt".into(),
                created_at: now,
                updated_at: now,
            },
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id,
                version: VersionNumber::new(1).unwrap(),
                content_hash: base.sha256,
                content_ref: base,
                dependency_refs: vec![],
                source_invocation_id: None,
                committed_by: ArtifactVersionAuthor::Participant(fixture.requester_id),
                committed_at: now,
            },
        })
        .await
        .unwrap();
    let edits = FileEditApplication::new(PostgresResourceStore::new(pool.clone()), content);
    let begin = BeginFileEdit {
        context: source.clone(),
        session_id: ResourceEditSessionId::generate(),
        file_node_id: node,
        base_version: VersionNumber::new(1).unwrap(),
        format_adapter: VersionedKey::new("text.utf8@1").unwrap(),
        idempotency_key: IdempotencyKey::new("existing-edit").unwrap(),
        command_fingerprint: Sha256::digest(b"existing-edit"),
        expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 60_000),
    };
    let original = edits.begin(begin.clone()).await.unwrap();
    PostgresWorkspaceResourceRootAuthority::new(pool.clone())
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor: source.actor,
            workspace_id: target,
        })
        .await
        .unwrap();
    let request = PrepareWorkspaceAssignment {
        command_id: CommandId::generate(),
        actor: source.actor,
        conversation_id: fixture.conversation_id,
        target_workspace_id: target,
        expected_home_epoch: HomeEpoch::new(1).unwrap(),
    };
    let migration = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    migration.prepare(request).await.unwrap();
    assert_eq!(
        edits.begin(begin.clone()).await.unwrap().session,
        original.session
    );
    let mut new_begin = begin.clone();
    new_begin.session_id = ResourceEditSessionId::generate();
    new_begin.idempotency_key = IdempotencyKey::new("new-editor").unwrap();
    new_begin.command_fingerprint = Sha256::digest(b"new-editor");
    assert_eq!(
        edits.begin(new_begin.clone()).await.unwrap_err(),
        FileEditError::DependencyUnavailable
    );
    let mut spoofed = new_begin.clone();
    spoofed.context.invocation_id = Some(musteragent_identity::InvocationId::generate());
    spoofed.context.tool_key = Some(VersionedKey::new("resource.file.edit.begin@1").unwrap());
    assert_eq!(
        edits.begin(spoofed).await.unwrap_err(),
        FileEditError::DependencyUnavailable
    );
    let checkpoint = edits
        .apply(ApplyFileEdits {
            context: source.clone(),
            session_id: begin.session_id,
            operation_id: ResourceEditOperationId::generate(),
            idempotency_key: IdempotencyKey::new("drain-edit").unwrap(),
            expected_session_revision: ResourceRevision::ZERO,
            expected_sequence: 1,
            operations: vec![TextEditOperation::Replace {
                range: TextRange {
                    start_byte: 0,
                    end_byte: 8,
                },
                expected_hash: Sha256::digest(b"original"),
                text: "finished".into(),
            }],
        })
        .await
        .unwrap();
    let terminal = FinalizeFileEdit {
        artifact_version_id: ArtifactVersionId::generate(),
        committed_by: musteragent_artifact::ArtifactVersionAuthor::Participant(
            fixture.requester_id,
        ),
        context: source,
        session_id: begin.session_id,
        command_id: ResourceEditTerminalCommandId::generate(),
        idempotency_key: IdempotencyKey::new("drain-finalize").unwrap(),
        expected_session_revision: checkpoint.session_revision,
    };
    let finished = edits.finalize(terminal.clone()).await.unwrap();
    let replay = edits.finalize(terminal).await.unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.artifact_version_id, finished.artifact_version_id);
    migration.cancel(request).await.unwrap();
    new_begin.base_version = VersionNumber::new(2).unwrap();
    edits
        .begin(new_begin)
        .await
        .expect("new editor after explicit cancellation");
}

pub(super) async fn prepare_for_fixture(
    pool: &sqlx::PgPool,
    fixture: running_loop::Fixture,
    target: WorkspaceId,
) {
    let actor = context(
        fixture.tenant_id,
        fixture.principal_id,
        fixture.conversation_id,
        target,
        1,
    )
    .actor;
    PostgresWorkspaceResourceRootAuthority::new(pool.clone())
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: target,
        })
        .await
        .unwrap();
    PostgresWorkspaceAssignmentPreparationStore::new(pool.clone())
        .prepare(PrepareWorkspaceAssignment {
            command_id: CommandId::generate(),
            actor,
            conversation_id: fixture.conversation_id,
            target_workspace_id: target,
            expected_home_epoch: HomeEpoch::new(1).unwrap(),
        })
        .await
        .unwrap();
}
