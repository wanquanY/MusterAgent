#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_content_ref::MediaType;
use musteragent_identity::*;
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{
    PostgresArtifactStore, PostgresContentStore, PostgresResourceStore,
    PostgresWorkspaceResourceRootAuthority,
};
use musteragent_runtime_artifact::{
    ArtifactApplication, ArtifactStore, GetArtifact, GetWorkspaceFileArtifact,
    WorkspaceFileArtifactApplication,
};
use musteragent_runtime_content::{ArtifactContentAuthorization, ContentStore, PutContent};
use musteragent_runtime_resource::*;
use musteragent_testkit::IsolatedPostgres;
use resource_support::{mutation, name};
use std::sync::Arc;

#[tokio::test]
async fn native_file_create_roundtrips_without_conversation_and_rolls_back_conflicts() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let workspace = WorkspaceId::generate();
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    roots
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: workspace,
        })
        .await
        .unwrap();
    let keyring = running_loop::keyring();
    let content = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keyring.clone(),
    );
    let content_ref = content
        .put(PutContent {
            tenant_id: actor.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"workspace file".to_vec(),
        })
        .await
        .unwrap();
    let artifact_id = ArtifactId::generate();
    let now = UtcTimestamp::from_unix_millis(100);
    let command = CreateResourceFile {
        context: ResourceCommandContext {
            tenant_id: actor.tenant_id,
            actor,
            source: ResourceCommandSource::Workspace {
                workspace_id: workspace,
            },
            invocation_id: None,
            tool_key: None,
            now,
        },
        identity: mutation("workspace-file"),
        expected_revision: ResourceRevision::ZERO,
        node_id: ResourceNodeId::generate(),
        parent_node_id: None,
        name: name("file.txt"),
        sort_key: "file.txt".into(),
        artifact: Artifact {
            artifact_id,
            tenant_id: actor.tenant_id,
            source: ArtifactSource::Workspace(workspace),
            origin: ArtifactOrigin::Product,
            kind: ArtifactKind::File,
            lifecycle: ArtifactLifecycle::Draft,
            current_version: VersionNumber::new(1).unwrap(),
            title: "file.txt".into(),
            created_at: now,
            updated_at: now,
        },
        version: ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            artifact_id,
            version: VersionNumber::new(1).unwrap(),
            content_hash: content_ref.sha256,
            content_ref,
            dependency_refs: vec![],
            source_invocation_id: None,
            committed_by: ArtifactVersionAuthor::ProductPrincipal(actor.principal_id),
            committed_at: now,
        },
    };
    let application = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let receipt = application.create_file(command.clone()).await.unwrap();
    assert_eq!(receipt.node.provenance.conversation_id, None);
    let replay = application.create_file(command.clone()).await.unwrap();
    assert_eq!(receipt.node, replay.node);
    assert!(replay.duplicate);
    let artifacts = PostgresArtifactStore::new(pool.clone(), keyring);
    let view = artifacts.load(actor.tenant_id, artifact_id).await.unwrap();
    assert_eq!(view.artifact, command.artifact);
    assert_eq!(view.versions, vec![command.version.clone()]);

    let query = GetWorkspaceFileArtifact {
        actor,
        workspace_id: workspace,
        artifact_id,
    };
    assert_eq!(
        artifacts.get_workspace_file_artifact(query).await.unwrap(),
        view
    );
    assert!(
        artifacts
            .get_artifact(GetArtifact { actor, artifact_id })
            .await
            .is_err(),
        "legacy Conversation authorization must not silently widen"
    );
    for forbidden in [
        GetWorkspaceFileArtifact {
            workspace_id: WorkspaceId::generate(),
            ..query
        },
        GetWorkspaceFileArtifact {
            artifact_id: ArtifactId::generate(),
            ..query
        },
        GetWorkspaceFileArtifact {
            actor: Actor {
                principal_id: PrincipalId::generate(),
                ..actor
            },
            ..query
        },
        GetWorkspaceFileArtifact {
            actor: Actor {
                tenant_id: TenantId::generate(),
                ..actor
            },
            ..query
        },
        GetWorkspaceFileArtifact {
            actor: Actor {
                principal_kind: PrincipalKind::Service,
                ..actor
            },
            ..query
        },
    ] {
        assert!(
            artifacts
                .get_workspace_file_artifact(forbidden)
                .await
                .is_err()
        );
    }

    let primary = command.version.content_ref.clone();
    let unlinked = content
        .put(PutContent {
            tenant_id: actor.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"private staged content not linked to this file".to_vec(),
        })
        .await
        .unwrap();
    let mut wrong_hash = primary.clone();
    wrong_hash.sha256 = musteragent_content_ref::Sha256::digest(b"wrong");
    let mut wrong_length = primary.clone();
    wrong_length.byte_length += 1;
    let mut wrong_media = primary.clone();
    wrong_media.media_type = MediaType::new("text/html").unwrap();
    let references = vec![
        primary.clone(),
        unlinked,
        wrong_hash,
        wrong_length,
        wrong_media,
        primary.clone(),
    ];
    assert_eq!(
        artifacts
            .workspace_version_content_reachability(
                actor,
                workspace,
                artifact_id,
                command.version.artifact_version_id,
                &references,
            )
            .await
            .unwrap(),
        vec![true, false, false, false, false, true]
    );
    for forbidden in [
        GetWorkspaceFileArtifact {
            workspace_id: WorkspaceId::generate(),
            ..query
        },
        GetWorkspaceFileArtifact {
            artifact_id: ArtifactId::generate(),
            ..query
        },
        GetWorkspaceFileArtifact {
            actor: Actor {
                principal_id: PrincipalId::generate(),
                ..actor
            },
            ..query
        },
        GetWorkspaceFileArtifact {
            actor: Actor {
                tenant_id: TenantId::generate(),
                ..actor
            },
            ..query
        },
        GetWorkspaceFileArtifact {
            actor: Actor {
                principal_kind: PrincipalKind::Service,
                ..actor
            },
            ..query
        },
    ] {
        assert_eq!(
            artifacts
                .workspace_version_content_reachability(
                    forbidden.actor,
                    forbidden.workspace_id,
                    forbidden.artifact_id,
                    command.version.artifact_version_id,
                    &references,
                )
                .await
                .unwrap(),
            vec![false; references.len()]
        );
    }
    assert_eq!(
        artifacts
            .workspace_version_content_reachability(
                actor,
                workspace,
                artifact_id,
                ArtifactVersionId::generate(),
                &references,
            )
            .await
            .unwrap(),
        vec![false; references.len()]
    );

    let mut conflicting = command.clone();
    conflicting.identity = mutation("workspace-file-conflict");
    conflicting.expected_revision = receipt.revision;
    conflicting.node_id = ResourceNodeId::generate();
    conflicting.artifact.artifact_id = ArtifactId::generate();
    conflicting.version.artifact_id = conflicting.artifact.artifact_id;
    conflicting.version.artifact_version_id = ArtifactVersionId::generate();
    assert!(application.create_file(conflicting.clone()).await.is_err());
    let orphan: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.artifacts WHERE artifact_id=$1)")
            .bind(conflicting.artifact.artifact_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        !orphan,
        "node conflict must roll back File Artifact and version"
    );
    let mut wrong_actor = command.clone();
    wrong_actor.context.actor.principal_id = PrincipalId::generate();
    wrong_actor.version.committed_by =
        ArtifactVersionAuthor::ProductPrincipal(wrong_actor.context.actor.principal_id);
    assert!(
        application.create_file(wrong_actor).await.is_err(),
        "foreign owner cannot replay a receipt"
    );
    let mut wrong_source = command.clone();
    wrong_source.artifact.source = ArtifactSource::Workspace(WorkspaceId::generate());
    assert!(application.create_file(wrong_source).await.is_err());
    let mut forged_participant = command.clone();
    forged_participant.version.committed_by =
        ArtifactVersionAuthor::Participant(ParticipantId::generate());
    assert!(application.create_file(forged_participant).await.is_err());
    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM conversation.conversations),
        (SELECT count(*) FROM conversation.participants), (SELECT count(*) FROM artifact.artifacts),
        (SELECT count(*) FROM artifact.artifact_versions)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (0, 0, 1, 1));
    let version_id = command.version.artifact_version_id;
    let trashed = application
        .trash_node(TrashResourceNode {
            context: command.context.clone(),
            identity: mutation("trash-native-file"),
            expected_revision: receipt.revision,
            node_id: command.node_id,
        })
        .await
        .unwrap();
    assert!(
        artifacts.get_workspace_file_artifact(query).await.is_err(),
        "immutable Workspace source does not authorize a trashed resource"
    );
    assert_eq!(
        artifacts
            .workspace_version_content_reachability(
                actor,
                workspace,
                artifact_id,
                version_id,
                &references,
            )
            .await
            .unwrap(),
        vec![false; references.len()]
    );
    application
        .restore_node(RestoreResourceNode {
            context: command.context,
            identity: mutation("restore-native-file"),
            expected_revision: trashed.revision,
            node_id: command.node_id,
            target_parent_node_id: None,
            name: None,
        })
        .await
        .unwrap();
    assert_eq!(
        artifacts.get_workspace_file_artifact(query).await.unwrap(),
        view
    );
    assert_eq!(
        artifacts
            .workspace_version_content_reachability(
                actor,
                workspace,
                artifact_id,
                version_id,
                &[primary],
            )
            .await
            .unwrap(),
        vec![true]
    );
}

#[tokio::test]
async fn native_file_edits_commit_real_principal_and_replay_without_conversations() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let workspace = WorkspaceId::generate();
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    roots
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: workspace,
        })
        .await
        .unwrap();
    let keyring = running_loop::keyring();
    let content = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keyring.clone(),
    );
    let content_ref = content
        .put(PutContent {
            tenant_id: actor.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"workspace file".to_vec(),
        })
        .await
        .unwrap();
    let artifact_id = ArtifactId::generate();
    let now = UtcTimestamp::from_unix_millis(100);
    let command = CreateResourceFile {
        context: ResourceCommandContext {
            tenant_id: actor.tenant_id,
            actor,
            source: ResourceCommandSource::Workspace {
                workspace_id: workspace,
            },
            invocation_id: None,
            tool_key: None,
            now,
        },
        identity: mutation("workspace-file"),
        expected_revision: ResourceRevision::ZERO,
        node_id: ResourceNodeId::generate(),
        parent_node_id: None,
        name: name("file.txt"),
        sort_key: "file.txt".into(),
        artifact: Artifact {
            artifact_id,
            tenant_id: actor.tenant_id,
            source: ArtifactSource::Workspace(workspace),
            origin: ArtifactOrigin::Product,
            kind: ArtifactKind::File,
            lifecycle: ArtifactLifecycle::Draft,
            current_version: VersionNumber::new(1).unwrap(),
            title: "file.txt".into(),
            created_at: now,
            updated_at: now,
        },
        version: ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            artifact_id,
            version: VersionNumber::new(1).unwrap(),
            content_hash: content_ref.sha256,
            content_ref,
            dependency_refs: vec![],
            source_invocation_id: None,
            committed_by: ArtifactVersionAuthor::ProductPrincipal(actor.principal_id),
            committed_at: now,
        },
    };
    let application = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    application.create_file(command.clone()).await.unwrap();
    let edits = FileEditApplication::new(
        PostgresResourceStore::new(pool.clone()),
        Arc::new(content.clone()),
    );
    let session_id = ResourceEditSessionId::generate();
    let begin = BeginFileEdit {
        context: command.context.clone(),
        session_id,
        file_node_id: command.node_id,
        base_version: VersionNumber::new(1).unwrap(),
        format_adapter: VersionedKey::new("text.utf8@1").unwrap(),
        idempotency_key: IdempotencyKey::new("native-edit-begin").unwrap(),
        command_fingerprint: musteragent_content_ref::Sha256::digest(b"native-edit-begin"),
        expires_at: UtcTimestamp::from_unix_millis(100_000),
    };
    edits.begin(begin.clone()).await.unwrap();
    assert_eq!(
        edits.begin(begin).await.unwrap().session.session_id,
        session_id
    );
    let apply = ApplyFileEdits {
        context: command.context.clone(),
        session_id,
        operation_id: ResourceEditOperationId::generate(),
        idempotency_key: IdempotencyKey::new("native-edit-apply").unwrap(),
        expected_session_revision: ResourceRevision::ZERO,
        expected_sequence: 1,
        operations: vec![musteragent_resource::TextEditOperation::Replace {
            range: musteragent_resource::TextRange {
                start_byte: 0,
                end_byte: 14,
            },
            expected_hash: musteragent_content_ref::Sha256::digest(b"workspace file"),
            text: "edited workspace file".into(),
        }],
    };
    let checkpoint = edits.apply(apply.clone()).await.unwrap();
    assert!(edits.apply(apply).await.unwrap().duplicate);
    let finalize = FinalizeFileEdit {
        context: command.context.clone(),
        command_id: ResourceEditTerminalCommandId::generate(),
        session_id,
        artifact_version_id: ArtifactVersionId::generate(),
        committed_by: ArtifactVersionAuthor::ProductPrincipal(actor.principal_id),
        idempotency_key: IdempotencyKey::new("native-edit-finalize").unwrap(),
        expected_session_revision: checkpoint.session_revision,
    };
    let mut forged = finalize.clone();
    forged.committed_by = ArtifactVersionAuthor::ProductPrincipal(PrincipalId::generate());
    assert_eq!(
        edits.finalize(forged).await.unwrap_err(),
        FileEditError::IdentityMismatch
    );
    let mut forged = finalize.clone();
    forged.committed_by = ArtifactVersionAuthor::Participant(ParticipantId::generate());
    assert_eq!(
        edits.finalize(forged).await.unwrap_err(),
        FileEditError::IdentityMismatch
    );
    let receipt = edits.finalize(finalize.clone()).await.unwrap();
    assert_eq!(
        receipt.state,
        musteragent_resource::EditSessionState::Committed
    );
    assert!(edits.finalize(finalize.clone()).await.unwrap().duplicate);
    let artifacts = PostgresArtifactStore::new(pool.clone(), keyring);
    let view = artifacts
        .get_workspace_file_artifact(GetWorkspaceFileArtifact {
            actor,
            workspace_id: workspace,
            artifact_id,
        })
        .await
        .unwrap();
    let version = view
        .versions
        .iter()
        .find(|v| v.artifact_version_id == finalize.artifact_version_id)
        .unwrap();
    assert_eq!(
        version.committed_by,
        ArtifactVersionAuthor::ProductPrincipal(actor.principal_id)
    );
    assert_eq!(
        content
            .read(actor.tenant_id, &version.content_ref)
            .await
            .unwrap(),
        b"edited workspace file"
    );
    let conversations: i64 = sqlx::query_scalar("SELECT count(*) FROM conversation.conversations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(conversations, 0);
}

#[tokio::test]
async fn tool_file_workspace_reads_preserve_real_run_source_and_follow_current_placement() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let actor = Actor {
        tenant_id: fixture.tenant_id,
        principal_id: fixture.principal_id,
        principal_kind: PrincipalKind::User,
    };
    let workspace = WorkspaceId::generate();
    sqlx::query("UPDATE conversation.conversations SET workspace_id=$1 WHERE tenant_id=$2 AND conversation_id=$3")
        .bind(workspace.into_uuid()).bind(actor.tenant_id.into_uuid())
        .bind(fixture.conversation_id.into_uuid()).execute(&pool).await.unwrap();
    PostgresWorkspaceResourceRootAuthority::new(pool.clone())
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: workspace,
        })
        .await
        .unwrap();
    let keys = running_loop::keyring();
    let content = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    );
    let reference = content
        .put(PutContent {
            tenant_id: actor.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"original tool output".to_vec(),
        })
        .await
        .unwrap();
    let artifact_id = ArtifactId::generate();
    let version_id = ArtifactVersionId::generate();
    // Real running-loop provenance; no synthetic Conversation or principal author.
    sqlx::query("INSERT INTO artifact.artifacts
        (artifact_id,tenant_id,conversation_id,activity_id,created_by_run_id,origin,kind,lifecycle,current_version,title,created_at_ms,updated_at_ms)
        VALUES ($1,$2,$3,$4,$5,1,6,1,1,'tool.txt',1,1)")
        .bind(artifact_id.into_uuid()).bind(actor.tenant_id.into_uuid())
        .bind(fixture.conversation_id.into_uuid()).bind(fixture.activity_id.into_uuid())
        .bind(fixture.run_id.into_uuid()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO artifact.artifact_versions
        (artifact_version_id,artifact_id,version,content_id,content_hash,committed_by,committed_at_ms)
        VALUES ($1,$2,1,$3,$4,$5,1)")
        .bind(version_id.into_uuid()).bind(artifact_id.into_uuid()).bind(reference.content_id.into_uuid())
        .bind(reference.sha256.into_bytes().to_vec()).bind(fixture.executor_id.into_uuid())
        .execute(&pool).await.unwrap();
    let application = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let context = resource_support::context(
        actor.tenant_id,
        actor.principal_id,
        fixture.conversation_id,
        workspace,
        1,
    );
    let receipt = application
        .link_file(LinkResourceFile {
            context: context.clone(),
            identity: mutation("link-real-tool-file"),
            expected_revision: ResourceRevision::ZERO,
            node_id: ResourceNodeId::generate(),
            parent_node_id: None,
            name: name("tool.txt"),
            sort_key: "tool.txt".into(),
            file_artifact_id: artifact_id,
        })
        .await
        .unwrap();
    let artifacts = PostgresArtifactStore::new(pool.clone(), keys);
    let query = GetWorkspaceFileArtifact {
        actor,
        workspace_id: workspace,
        artifact_id,
    };
    let view = artifacts.get_workspace_file_artifact(query).await.unwrap();
    assert_eq!(
        view.artifact.origin,
        ArtifactOrigin::Runtime {
            activity_id: fixture.activity_id,
            created_by_run_id: fixture.run_id
        }
    );
    assert_eq!(
        view.artifact.source,
        ArtifactSource::Conversation(fixture.conversation_id)
    );
    assert_eq!(
        view.versions[0].committed_by,
        ArtifactVersionAuthor::Participant(fixture.executor_id)
    );
    assert_eq!(view.versions[0].artifact_version_id, version_id);
    let refs = std::slice::from_ref(&reference);
    assert_eq!(
        artifacts
            .workspace_version_content_reachability(actor, workspace, artifact_id, version_id, refs)
            .await
            .unwrap(),
        vec![true]
    );
    for (viewer, scope, version) in [
        (
            Actor {
                principal_id: PrincipalId::generate(),
                ..actor
            },
            workspace,
            version_id,
        ),
        (actor, WorkspaceId::generate(), version_id),
        (actor, workspace, ArtifactVersionId::generate()),
    ] {
        assert_eq!(
            artifacts
                .workspace_version_content_reachability(viewer, scope, artifact_id, version, refs)
                .await
                .unwrap(),
            vec![false]
        );
    }
    application
        .trash_node(TrashResourceNode {
            context,
            identity: mutation("trash-real-tool-file"),
            expected_revision: receipt.revision,
            node_id: receipt.node.node_id,
        })
        .await
        .unwrap();
    assert!(artifacts.get_workspace_file_artifact(query).await.is_err());
    assert_eq!(
        artifacts
            .workspace_version_content_reachability(actor, workspace, artifact_id, version_id, refs)
            .await
            .unwrap(),
        vec![false]
    );
    assert_eq!(
        artifacts.load(actor.tenant_id, artifact_id).await.unwrap(),
        view
    );
}
