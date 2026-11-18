use super::workspace_assignment_barrier::preparation;
use super::*;
use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use musteragent_postgres::PostgresWorkspaceAssignmentPreparationStore;
use musteragent_runtime_session::workspace_assignment::{
    WorkspaceAssignmentError, WorkspaceAssignmentPreparationStore,
};
use musteragent_runtime_session::workspace_assignment_commit::{
    WorkspaceAssignmentCommit, WorkspaceAssignmentCommitStore,
};
use musteragent_wire::ProtobufLedgerPayloadCodec;

fn command(
    request: musteragent_runtime_session::workspace_assignment::PrepareWorkspaceAssignment,
    offset: LedgerOffset,
) -> WorkspaceAssignmentCommit {
    WorkspaceAssignmentCommit::new(
        request,
        offset,
        UtcTimestamp::from_unix_millis(123),
        CorrelationId::generate(),
        &ProtobufLedgerPayloadCodec,
    )
    .unwrap()
}

#[tokio::test]
async fn empty_assignment_is_atomic_idempotent_and_does_not_create_a_fake_file() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let request = preparation(&pool, &fixture).await;
    let store = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    store.prepare(request).await.unwrap();
    let commit = command(request, LedgerOffset::ZERO);
    let receipt = store.commit_assignment(commit.clone()).await.unwrap();
    let row:(uuid::Uuid,i64,i16,i64)=sqlx::query_as("SELECT c.workspace_id,c.updated_at_ms,p.phase,
        (SELECT count(*) FROM resource.nodes WHERE tenant_id=c.tenant_id)
        FROM conversation.conversations c JOIN conversation.workspace_assignment_preparations p USING(tenant_id,conversation_id)
        WHERE c.conversation_id=$1").bind(fixture.conversation_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(row, (request.target_workspace_id.into_uuid(), 1, 3, 0));
    let catalog: (String, String, i64) = sqlx::query_as(
        "SELECT attention_revision::text,last_activity_position::text,last_user_visible_activity_at_ms
         FROM projection.conversation_catalog WHERE conversation_id=$1")
        .bind(fixture.conversation_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(catalog, ("0".into(), "0".into(), 1));
    let duplicate = store.commit_assignment(commit).await.unwrap();
    assert_eq!(duplicate.disposition, AppendDisposition::Duplicate);
    assert_eq!(duplicate.last_sequence, receipt.last_sequence);
    let changes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM projection.conversation_catalog_changes WHERE conversation_id=$1",
    )
    .bind(fixture.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(changes, 1, "assignment invalidation commits exactly once");
    let source_runs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE conversation_id=$1")
            .bind(fixture.conversation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(source_runs, 0);
}

#[tokio::test]
async fn active_execution_cannot_be_transferred_or_cancelled_by_assignment() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let batch = admission_batch(&pool, &fixture, "active-assignment").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    let admitted = admission_store(&pool).admit(batch).await.unwrap();
    let request = preparation(&pool, &fixture).await;
    let store = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    store.prepare(request).await.unwrap();
    assert_eq!(
        store
            .commit_assignment(command(
                request,
                LedgerOffset::from(admitted.ledger.last_sequence)
            ))
            .await
            .unwrap_err(),
        WorkspaceAssignmentError::WorkPending
    );
    let workspace: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT workspace_id FROM conversation.conversations WHERE conversation_id=$1",
    )
    .bind(fixture.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(workspace, None);
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resource.workspace_assignment_receipts")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(receipts, 0);
}

#[tokio::test]
async fn interrupted_run_history_does_not_block_workspace_assignment() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let batch = admission_batch(&pool, &fixture, "interrupted-assignment").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    let admitted = admission_store(&pool).admit(batch).await.unwrap();
    sqlx::query("UPDATE runtime.runs SET status=8 WHERE conversation_id=$1")
        .bind(fixture.conversation_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let request = preparation(&pool, &fixture).await;
    let store = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    store.prepare(request).await.unwrap();
    store
        .commit_assignment(command(
            request,
            LedgerOffset::from(admitted.ledger.last_sequence),
        ))
        .await
        .expect("immutable interrupted history can move");
    let status: i16 =
        sqlx::query_scalar("SELECT status FROM runtime.runs WHERE conversation_id=$1")
            .bind(fixture.conversation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        status, 8,
        "assignment must not rewrite historical execution"
    );
}

#[tokio::test]
async fn tree_transfer_preserves_same_names_trash_edges_and_rolls_back_on_ledger_failure() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let request = preparation(&pool, &fixture).await;
    let tenant = fixture.tenant_id.into_uuid();
    let source = fixture.conversation_id.into_uuid();
    sqlx::query("INSERT INTO resource.scope_heads VALUES($1,2,$2,4,1)")
        .bind(tenant)
        .bind(source)
        .execute(&pool)
        .await
        .unwrap();
    let ids = [
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
    ];
    for (i, (name, parent, original, state)) in [
        ("left", None, None, 1),
        ("right", None, None, 1),
        ("same", Some(ids[0]), None, 1),
        ("same", None, Some(ids[1]), 2),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query("INSERT INTO resource.nodes(node_id,tenant_id,scope_kind,scope_id,parent_node_id,original_parent_node_id,
            name,comparison_name,kind,lifecycle,sort_key,actor_principal_id,actor_principal_kind,source_conversation_id,
            created_at_ms,updated_at_ms,revision) VALUES($1,$2,2,$3,$4,$5,$6,$6,1,$7,$6,$8,1,$3,1,1,4)")
            .bind(ids[i]).bind(tenant).bind(source).bind(parent).bind(original).bind(name).bind(state as i16)
            .bind(fixture.principal_id.into_uuid()).execute(&pool).await.unwrap();
    }
    sqlx::query("UPDATE conversation.conversations SET title=$1 WHERE conversation_id=$2")
        .bind("研究/路径\\e\u{301} ".repeat(35))
        .bind(source)
        .execute(&pool)
        .await
        .unwrap();
    let user_folder = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO resource.nodes(node_id,tenant_id,scope_kind,scope_id,name,comparison_name,
        kind,lifecycle,sort_key,actor_principal_id,actor_principal_kind,created_at_ms,updated_at_ms,revision)
        VALUES($1,$2,1,$3,'历史会话资料','历史会话资料',1,1,'original',$4,1,1,1,1)")
        .bind(user_folder).bind(tenant).bind(request.target_workspace_id.into_uuid())
        .bind(fixture.principal_id.into_uuid()).execute(&pool).await.unwrap();
    let store = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    store.prepare(request).await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION event_store.reject_assignment_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$;
        CREATE TRIGGER reject_assignment_test BEFORE INSERT ON event_store.domain_events FOR EACH ROW EXECUTE FUNCTION event_store.reject_assignment_test();")
        .execute(&pool).await.unwrap();
    let commit = command(request, LedgerOffset::ZERO);
    assert!(store.commit_assignment(commit.clone()).await.is_err());
    let source_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resource.nodes WHERE scope_kind=2 AND scope_id=$1",
    )
    .bind(source)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(source_count, 4);
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resource.workspace_assignment_receipts")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(receipts, 0);
    sqlx::query("DROP TRIGGER reject_assignment_test ON event_store.domain_events")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_assignment_catalog BEFORE INSERT ON projection.conversation_catalog_changes FOR EACH ROW EXECUTE FUNCTION event_store.reject_assignment_test()")
        .execute(&pool).await.unwrap();
    assert!(store.commit_assignment(commit.clone()).await.is_err());
    let records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM event_store.ledger_records WHERE conversation_id=$1",
    )
    .bind(source)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        records, 0,
        "catalog failure must roll back the already-staged Ledger"
    );
    sqlx::query(
        "DROP TRIGGER reject_assignment_catalog ON projection.conversation_catalog_changes",
    )
    .execute(&pool)
    .await
    .unwrap();
    store.commit_assignment(commit.clone()).await.unwrap();
    let edges:Vec<(uuid::Uuid,Option<uuid::Uuid>,Option<uuid::Uuid>,i16)>=sqlx::query_as(
        "SELECT node_id,parent_node_id,original_parent_node_id,lifecycle FROM resource.nodes WHERE node_id=ANY($1) ORDER BY node_id")
        .bind(ids.to_vec()).fetch_all(&pool).await.unwrap();
    assert_eq!(edges[2], (ids[2], Some(ids[0]), None, 1));
    assert_eq!(edges[3], (ids[3], None, Some(ids[1]), 2));
    assert_eq!(edges[0].1, edges[1].1);
    assert!(edges[0].1.is_some());
    let proofs:i64=sqlx::query_scalar("SELECT count(*) FROM resource.scope_history WHERE assignment_command_id=$1 AND evidence_source=5")
        .bind(request.command_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(proofs, 4);
    store.commit_assignment(commit).await.unwrap();
    let nodes: i64 = sqlx::query_scalar("SELECT count(*) FROM resource.nodes WHERE tenant_id=$1")
        .bind(tenant)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(nodes, 7);
    let archive_name: String =
        sqlx::query_scalar("SELECT name FROM resource.nodes WHERE node_id=$1")
            .bind(edges[0].1)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(archive_name.ends_with(&source.to_string()[..8]));
    musteragent_resource::ResourceName::new(archive_name).unwrap();
    let original_name: String =
        sqlx::query_scalar("SELECT name FROM resource.nodes WHERE node_id=$1")
            .bind(user_folder)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(original_name, "历史会话资料");
}

#[tokio::test]
async fn file_identity_versions_and_terminal_edit_receipt_survive_assignment() {
    use musteragent_artifact::{
        Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
    };
    use musteragent_identity::{
        ArtifactId, ArtifactVersionId, ResourceChangeSetId, ResourceEditSessionId,
        ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision,
    };
    use musteragent_postgres::PostgresResourceStore;
    use musteragent_resource::ResourceName;
    use musteragent_runtime_content::PutContent;
    use musteragent_runtime_resource::{
        BeginFileEdit, CreateResourceFile, FileEditApplication, FinalizeFileEdit,
        ResourceApplication, ResourceCommandContext, ResourceMutationIdentity,
    };
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let request = preparation(&pool, &fixture).await;
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    let bytes = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"keep original content".to_vec(),
        })
        .await
        .unwrap();
    let now_ms: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();
    let now = UtcTimestamp::from_unix_millis(now_ms);
    let context = ResourceCommandContext {
        tenant_id: fixture.tenant_id,
        actor: request.actor,
        source: musteragent_runtime_resource::ResourceCommandSource::Conversation {
            conversation_id: fixture.conversation_id,
            workspace_id: None,
        },
        invocation_id: None,
        tool_key: None,
        now,
    };
    let artifact_id = ArtifactId::generate();
    let node_id = ResourceNodeId::generate();
    let create = CreateResourceFile {
        context: context.clone(),
        identity: ResourceMutationIdentity {
            change_set_id: ResourceChangeSetId::generate(),
            idempotency_key: IdempotencyKey::new("source-file").unwrap(),
            command_fingerprint: Sha256::digest(b"source-file"),
        },
        expected_revision: ResourceRevision::ZERO,
        node_id,
        parent_node_id: None,
        name: ResourceName::new("original.txt").unwrap(),
        sort_key: "original.txt".into(),
        artifact: Artifact {
            artifact_id,
            tenant_id: fixture.tenant_id,
            source: ArtifactSource::Conversation(fixture.conversation_id),
            origin: ArtifactOrigin::Product,
            kind: ArtifactKind::File,
            lifecycle: ArtifactLifecycle::Draft,
            current_version: VersionNumber::new(1).unwrap(),
            title: "original.txt".into(),
            created_at: now,
            updated_at: now,
        },
        version: ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            artifact_id,
            version: VersionNumber::new(1).unwrap(),
            content_hash: bytes.sha256,
            content_ref: bytes.clone(),
            dependency_refs: vec![],
            source_invocation_id: None,
            committed_by: ArtifactVersionAuthor::Participant(fixture.user_participant_id),
            committed_at: now,
        },
    };
    let resources = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let original_create = resources.create_file(create.clone()).await.unwrap();
    let edits = FileEditApplication::new(PostgresResourceStore::new(pool.clone()), content.clone());
    let begin = BeginFileEdit {
        context: context.clone(),
        session_id: ResourceEditSessionId::generate(),
        file_node_id: node_id,
        base_version: VersionNumber::new(1).unwrap(),
        format_adapter: VersionedKey::new("text.utf8@1").unwrap(),
        idempotency_key: IdempotencyKey::new("before-assignment-edit").unwrap(),
        command_fingerprint: Sha256::digest(b"before-assignment-edit"),
        expires_at: UtcTimestamp::from_unix_millis(now_ms + 60_000),
    };
    edits.begin(begin.clone()).await.unwrap();
    let store = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    store.prepare(request).await.unwrap();
    let commit = command(request, LedgerOffset::ZERO);
    assert_eq!(
        store.commit_assignment(commit.clone()).await.unwrap_err(),
        WorkspaceAssignmentError::WorkPending
    );
    let finalize = FinalizeFileEdit {
        context: context.clone(),
        command_id: ResourceEditTerminalCommandId::generate(),
        session_id: begin.session_id,
        artifact_version_id: ArtifactVersionId::generate(),
        committed_by: musteragent_artifact::ArtifactVersionAuthor::Participant(
            fixture.user_participant_id,
        ),
        idempotency_key: IdempotencyKey::new("finish-before-assignment").unwrap(),
        expected_session_revision: ResourceRevision::ZERO,
    };
    let terminal = edits.finalize(finalize.clone()).await.unwrap();
    store.commit_assignment(commit).await.unwrap();
    // After assignment the Conversation resolves into the target project; a
    // retried command carries that current context, not the pre-assignment one.
    let assigned = ResourceCommandContext {
        source: musteragent_runtime_resource::ResourceCommandSource::Conversation {
            conversation_id: fixture.conversation_id,
            workspace_id: Some(request.target_workspace_id),
        },
        ..context.clone()
    };
    let mut finalize = finalize;
    finalize.context = assigned.clone();
    let replay = edits.finalize(finalize).await.unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.artifact_version_id, terminal.artifact_version_id);
    let created_again = resources.create_file(create).await.unwrap();
    assert!(created_again.duplicate);
    assert_eq!(created_again.node, original_create.node);
    let current:(uuid::Uuid,uuid::Uuid,i64)=sqlx::query_as("SELECT file_artifact_id,scope_id,
        (SELECT count(*) FROM artifact.artifact_versions WHERE artifact_id=$2) FROM resource.nodes WHERE node_id=$1")
        .bind(node_id.into_uuid()).bind(artifact_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        current,
        (
            artifact_id.into_uuid(),
            request.target_workspace_id.into_uuid(),
            2
        )
    );
    assert_eq!(
        content.read(fixture.tenant_id, &bytes).await.unwrap(),
        b"keep original content"
    );
    assert_eq!(
        store.prepare(request).await.unwrap().phase,
        musteragent_runtime_session::workspace_assignment::WorkspaceAssignmentPhase::Committed
    );
    assert_eq!(
        store.cancel(request).await.unwrap_err(),
        WorkspaceAssignmentError::IdentityConflict
    );
}

#[tokio::test]
async fn standing_goal_and_assignment_preparation_cannot_starve_each_other() {
    use musteragent_goal::{GoalBudget, GoalCompletionContract, GoalOrigin, GoalRiskClass};
    use musteragent_identity::{GoalId, GoalPursuitId};
    use musteragent_postgres::PostgresStandingGoalProductStore;
    use musteragent_runtime_automation::{
        CreateStandingGoal, StandingGoalDefinition, StandingGoalProductApplication,
        StandingGoalProductError,
    };
    let (_guard, _postgres, pool) = runtime_pool().await;
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    let goals = StandingGoalProductApplication::new(
        Arc::new(PostgresStandingGoalProductStore::new(pool.clone())),
        content,
    );
    let store = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    for goal_first in [true, false] {
        let fixture = seed_fixture(&pool, None).await;
        let request = preparation(&pool, &fixture).await;
        let create = CreateStandingGoal {
            tenant_id: fixture.tenant_id,
            goal_id: GoalId::generate(),
            pursuit_id: GoalPursuitId::generate(),
            principal_id: fixture.principal_id,
            origin: GoalOrigin::User,
            creator_agent_id: None,
            definition: StandingGoalDefinition {
                mode: Default::default(),
                target_agent_id: fixture.agent_id,
                conversation_id: fixture.conversation_id,
                title: "Continue existing work".into(),
                objective: b"Preserve goal progress during migration".to_vec(),
                constraints: vec![],
                completion: GoalCompletionContract {
                    risk_class: GoalRiskClass::Standard,
                    success_criteria: vec!["Finish".into()],
                    evidence_requirements: vec!["Receipt".into()],
                    failure_conditions: vec![],
                    minimum_evidence_items: 1,
                },
                budget: GoalBudget {
                    max_runs: 10,
                    max_elapsed_millis: 600_000,
                    max_model_tokens: 10_000,
                    max_tool_invocations: 100,
                    max_consecutive_failures: 3,
                },
            },
            activate: false,
            initial_run_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(10),
        };
        if goal_first {
            goals.create(create).await.unwrap();
            assert_eq!(
                store.prepare(request).await.unwrap_err(),
                WorkspaceAssignmentError::WorkPending
            );
            let barrier:i64=sqlx::query_scalar("SELECT count(*) FROM conversation.workspace_assignment_preparations WHERE conversation_id=$1")
                .bind(fixture.conversation_id.into_uuid()).fetch_one(&pool).await.unwrap();
            assert_eq!(
                barrier, 0,
                "accepted Goal must remain free to make progress"
            );
        } else {
            store.prepare(request).await.unwrap();
            assert_eq!(
                goals.create(create).await.unwrap_err(),
                StandingGoalProductError::Unavailable
            );
            store.cancel(request).await.unwrap();
        }
    }
}
