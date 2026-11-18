use super::*;
use musteragent_identity::{MessageId, ParticipantId};
use musteragent_projection::CatalogFactKind;
use musteragent_testkit::IsolatedPostgres;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn conversation(
    pool: &PgPool,
    actor: Actor,
    workspace: WorkspaceId,
) -> TestResult<ConversationId> {
    let id = ConversationId::generate();
    let participant = ParticipantId::generate();
    let database = crate::PostgresDatabaseRuntime::from_pool(pool.clone());
    let mut tx = database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
        .await?;
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO conversation.conversations(conversation_id,tenant_id,kind,title,control_home_kind,
        home_epoch,lifecycle,created_by,created_at_ms,updated_at_ms,workspace_id,execution_owner_participant_id)
        VALUES($1,$2,1,'Activity fixture',1,1,1,$3,1000,1000,$4,$5)")
        .bind(id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(workspace.into_uuid()).bind(participant.into_uuid()).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,display_name,joined_at_ms)
        VALUES($1,$2,$3,1,$4,'User',1000)")
        .bind(participant.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(id.into_uuid())
        .bind(actor.principal_id.into_uuid()).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(id)
}
async fn output(
    store: &PostgresConversationCatalogStore,
    actor: Actor,
    id: ConversationId,
) -> TestResult {
    store
        .apply_fact(CatalogFactDraft {
            control_home: RuntimeHome::Cloud,
            tenant_id: actor.tenant_id,
            conversation_id: id,
            home_epoch: HomeEpoch::new(1)?,
            source_record_id: None,
            source_key: CatalogSourceKey::new(
                CatalogSourceKind::VisibleOutput,
                CatalogSourceIdentity::Message(MessageId::generate()),
                1,
            )?,
            audience: CatalogAttentionAudience::AllParticipants,
            occurred_at: UtcTimestamp::from_unix_millis(2000),
            committed_at: UtcTimestamp::from_unix_millis(2000),
            kind: CatalogFactKind::VisibleOutput,
        })
        .await?;
    Ok(())
}
#[tokio::test]
async fn workspace_activity_is_authorized_complete_and_read_only_without_catalog_paging()
-> TestResult {
    let pg = IsolatedPostgres::start_with_pgvector().await?;
    let pool = pg.pool().await?;
    crate::migrate(&pool).await?;
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let workspace = WorkspaceId::generate();
    let absent = WorkspaceId::generate();
    let store = PostgresConversationCatalogStore::new(pool.clone());
    let first = conversation(&pool, actor, workspace).await?;
    let pending = conversation(&pool, actor, workspace).await?;
    let stranger = Actor {
        principal_id: PrincipalId::generate(),
        ..actor
    };
    let second = conversation(&pool, stranger, workspace).await?;
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,display_name,joined_at_ms) VALUES($1,$2,$3,1,$4,'Member',1000)")
        .bind(ParticipantId::generate().into_uuid()).bind(actor.tenant_id.into_uuid())
        .bind(second.into_uuid()).bind(actor.principal_id.into_uuid()).execute(&pool).await?;
    let hidden = conversation(&pool, stranger, workspace).await?;
    for (owner, id) in [(actor, first), (actor, second), (stranger, hidden)] {
        output(&store, owner, id).await?;
    }
    sqlx::query("INSERT INTO projection.conversation_catalog_executions(tenant_id,conversation_id,execution_kind,execution_id,run_id,status)
        VALUES($1,$2,1,$3,$3,2)")
        .bind(actor.tenant_id.into_uuid()).bind(first.into_uuid()).bind(MessageId::generate().into_uuid()).execute(&pool).await?;
    sqlx::query("INSERT INTO projection.conversation_catalog_actions(tenant_id,conversation_id,action_identity_kind,action_identity,action_kind,opened_at_ms)
        VALUES($1,$2,1,$3,1,2000)")
        .bind(actor.tenant_id.into_uuid()).bind(first.into_uuid()).bind(MessageId::generate().into_uuid()).execute(&pool).await?;
    let query = ListWorkspaceCatalogActivity {
        excluded_conversation_ids: vec![],
        actor,
        workspace_ids: vec![absent, workspace],
    };
    let before = store
        .workspace_activity(query.clone())
        .await
        .expect("read authorized workspace activity");
    assert_eq!(
        before.workspaces[0],
        WorkspaceCatalogActivity {
            workspace_id: absent,
            active_executions: 0,
            action_required: 0,
            unread: 0,
            latest_unread_outcome: None,
            pending_projections: 0
        }
    );
    assert_eq!(
        before.workspaces[1],
        WorkspaceCatalogActivity {
            workspace_id: workspace,
            active_executions: 1,
            action_required: 1,
            unread: 2,
            latest_unread_outcome: None,
            pending_projections: 1
        }
    );
    assert_eq!(
        store
            .workspace_activity(query.clone())
            .await
            .expect("read authorized workspace activity"),
        before
    );
    let filtered = store
        .workspace_activity(ListWorkspaceCatalogActivity {
            excluded_conversation_ids: vec![first, pending],
            ..query.clone()
        })
        .await
        .expect("read authorized workspace activity");
    assert_eq!(filtered.workspaces[1].unread, 1);
    assert_eq!(filtered.workspaces[1].active_executions, 0);
    assert_eq!(filtered.workspaces[1].action_required, 0);
    assert_eq!(filtered.workspaces[1].pending_projections, 0);
    assert_eq!(
        store
            .workspace_activity(query.clone())
            .await
            .expect("read authorized workspace activity"),
        before
    );
    // An existing but bootstrap-fenced projection must not report stale activity.
    sqlx::query("INSERT INTO projection.conversation_catalog_bootstrap_receipts(tenant_id,conversation_id,principal_id) VALUES($1,$2,$3)")
        .bind(actor.tenant_id.into_uuid()).bind(first.into_uuid()).bind(actor.principal_id.into_uuid())
        .execute(&pool).await?;
    let fenced = store
        .workspace_activity(query.clone())
        .await
        .expect("read authorized workspace activity");
    assert_eq!(fenced.workspaces[1].pending_projections, 2);
    assert_eq!(fenced.workspaces[1].active_executions, 0);
    assert_eq!(fenced.workspaces[1].action_required, 0);
    assert_eq!(fenced.workspaces[1].unread, 1);
    sqlx::query("DELETE FROM projection.conversation_catalog_bootstrap_receipts WHERE tenant_id=$1 AND conversation_id=$2")
        .bind(actor.tenant_id.into_uuid()).bind(first.into_uuid()).execute(&pool).await?;
    assert_eq!(
        store
            .workspace_activity(query.clone())
            .await
            .expect("read authorized workspace activity"),
        before
    );
    assert_eq!(
        store
            .workspace_activity(ListWorkspaceCatalogActivity {
                actor: Actor {
                    principal_kind: PrincipalKind::Service,
                    ..actor
                },
                ..query.clone()
            })
            .await
            .expect_err("invalid activity queries must be rejected"),
        ConversationCatalogSourceError::InvalidQuery
    );
    let receipt_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM projection.conversation_read_receipts")
            .fetch_one(&pool)
            .await?;
    assert_eq!(receipt_count, 0);
    store
        .acknowledge_seen_through(
            actor.tenant_id,
            first,
            actor.principal_id,
            HomeEpoch::new(1)?,
            AttentionRevision::new(1),
            UtcTimestamp::from_unix_millis(3000),
        )
        .await?;
    sqlx::query("UPDATE conversation.participants SET left_at_ms=3000 WHERE conversation_id=$1 AND principal_ref=$2")
        .bind(second.into_uuid()).bind(actor.principal_id.into_uuid())
        .execute(&pool)
        .await
        ?;
    sqlx::query("UPDATE conversation.conversations SET lifecycle=2,archived_at_ms=3000 WHERE conversation_id=$1").bind(pending.into_uuid()).execute(&pool).await?;
    sqlx::query(
        "UPDATE projection.conversation_catalog_executions SET status=5 WHERE conversation_id=$1",
    )
    .bind(first.into_uuid())
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM projection.conversation_catalog_actions WHERE conversation_id=$1")
        .bind(first.into_uuid())
        .execute(&pool)
        .await?;
    let after = store
        .workspace_activity(query.clone())
        .await
        .expect("read authorized workspace activity");
    assert_eq!(after.workspaces[1].unread, 0);
    assert_eq!(after.workspaces[1].pending_projections, 0);
    assert_eq!(after.workspaces[1].active_executions, 0);
    assert_eq!(after.workspaces[1].action_required, 0);
    let foreign = store
        .workspace_activity(ListWorkspaceCatalogActivity {
            actor: Actor {
                tenant_id: TenantId::generate(),
                ..actor
            },
            ..query.clone()
        })
        .await
        .expect("read authorized workspace activity");
    assert!(
        foreign
            .workspaces
            .iter()
            .all(|v| v.unread == 0 && v.pending_projections == 0)
    );
    for ids in [
        vec![],
        vec![workspace; 2],
        (0..101).map(|_| WorkspaceId::generate()).collect(),
    ] {
        assert_eq!(
            store
                .workspace_activity(ListWorkspaceCatalogActivity {
                    actor,
                    excluded_conversation_ids: vec![],
                    workspace_ids: ids
                })
                .await
                .expect_err("invalid activity queries must be rejected"),
            ConversationCatalogSourceError::InvalidQuery
        );
    }
    Ok(())
}

#[tokio::test]
async fn workspace_outcome_follows_unread_delivery_order_and_receipts() -> TestResult {
    use musteragent_identity::RunId;
    let pg = IsolatedPostgres::start_with_pgvector().await?;
    let pool = pg.pool().await?;
    crate::migrate(&pool).await?;
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let workspace = WorkspaceId::generate();
    let first = conversation(&pool, actor, workspace).await?;
    let second = conversation(&pool, actor, workspace).await?;
    let store = PostgresConversationCatalogStore::new(pool.clone());
    for (id, kind) in [
        (first, ExecutionOutcomeKind::Completed),
        (second, ExecutionOutcomeKind::Failed),
    ] {
        store
            .apply_fact(CatalogFactDraft {
                control_home: RuntimeHome::Cloud,
                tenant_id: actor.tenant_id,
                conversation_id: id,
                home_epoch: HomeEpoch::new(1)?,
                source_record_id: None,
                source_key: CatalogSourceKey::new(
                    CatalogSourceKind::ExecutionOutcome,
                    CatalogSourceIdentity::Run(RunId::generate()),
                    1,
                )?,
                audience: CatalogAttentionAudience::AllParticipants,
                occurred_at: UtcTimestamp::from_unix_millis(2000),
                committed_at: UtcTimestamp::from_unix_millis(2000),
                kind: musteragent_projection::CatalogFactKind::VisibleExecutionOutcome {
                    outcome: ExecutionOutcome {
                        kind,
                        error_class: None,
                        recoverability: None,
                    },
                },
            })
            .await?;
    }
    output(&store, actor, first).await?;
    let query = ListWorkspaceCatalogActivity {
        excluded_conversation_ids: vec![],
        actor,
        workspace_ids: vec![workspace],
    };
    assert_eq!(
        store
            .workspace_activity(query.clone())
            .await
            .expect("read authorized workspace activity")
            .workspaces[0]
            .latest_unread_outcome,
        Some(ExecutionOutcomeKind::Failed)
    );
    let narrowed = store
        .workspace_activity(ListWorkspaceCatalogActivity {
            excluded_conversation_ids: vec![second],
            ..query.clone()
        })
        .await
        .expect("read authorized workspace activity");
    assert_eq!(narrowed.workspaces[0].unread, 2);
    assert_eq!(
        narrowed.workspaces[0].latest_unread_outcome,
        Some(ExecutionOutcomeKind::Completed)
    );
    store
        .acknowledge_seen_through(
            actor.tenant_id,
            second,
            actor.principal_id,
            HomeEpoch::new(1)?,
            AttentionRevision::new(1),
            UtcTimestamp::from_unix_millis(3000),
        )
        .await?;
    assert_eq!(
        store
            .workspace_activity(query.clone())
            .await
            .expect("read authorized workspace activity")
            .workspaces[0]
            .latest_unread_outcome,
        Some(ExecutionOutcomeKind::Completed)
    );
    store
        .acknowledge_seen_through(
            actor.tenant_id,
            first,
            actor.principal_id,
            HomeEpoch::new(1)?,
            AttentionRevision::new(2),
            UtcTimestamp::from_unix_millis(3000),
        )
        .await?;
    let seen = store
        .workspace_activity(query)
        .await
        .expect("read authorized workspace activity");
    assert_eq!(seen.workspaces[0].latest_unread_outcome, None);
    assert_eq!(seen.workspaces[0].unread, 0);
    Ok(())
}
