use std::time::Duration;

use musteragent_conversation::ConversationLifecycle;
use musteragent_identity::{Actor, MessageId, ParticipantId, PrincipalKind};
use musteragent_projection::{
    CatalogAttentionAudience, CatalogFactDraft, CatalogFactKind, CatalogSourceIdentity,
    CatalogSourceKey, CatalogSourceKind,
};
use musteragent_testkit::IsolatedPostgres;
use sqlx::postgres::PgListener;

use super::*;
use crate::migrate;
use crate::projection::CATALOG_INVALIDATION_CHANNEL;

#[tokio::test]
async fn committed_catalog_change_emits_one_tenant_scoped_hint() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let principal_id = PrincipalId::generate();
    let participant_id = ParticipantId::generate();
    let mut listener = PgListener::connect_with(&pool)
        .await
        .expect("connect Catalog listener");
    listener
        .listen(CATALOG_INVALIDATION_CHANNEL)
        .await
        .expect("listen for Catalog invalidations");
    let mut fixture = pool.begin().await.expect("begin fixture");
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut *fixture)
        .await
        .expect("defer fixture constraints");
    sqlx::query(
        "INSERT INTO conversation.conversations(
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms, workspace_id,
             execution_owner_participant_id
         ) VALUES($1, $2, 1, 'Notify fixture', 1, NULL, 1, 1, $3,
                  1000, 1000, NULL, NULL, $4)",
    )
    .bind(conversation_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(principal_id.into_uuid())
    .bind(participant_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("insert Conversation");
    sqlx::query(
        "INSERT INTO conversation.participants(
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES($1, $2, $3, 1, $4, NULL, NULL, 'User', NULL, 1000, NULL)",
    )
    .bind(participant_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(principal_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("insert participant");
    sqlx::query(
        "INSERT INTO projection.conversation_catalog_changes(
             tenant_id, activity_position, conversation_id, principal_id,
             change_kind, catalog_revision, committed_at_ms
         ) VALUES($1, 1, $2, NULL, 1, 1, 1000)",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("insert Catalog invalidation");
    fixture.commit().await.expect("commit Catalog invalidation");

    let notification = tokio::time::timeout(Duration::from_secs(1), listener.recv())
        .await
        .expect("Catalog hint timeout")
        .expect("receive Catalog hint");
    assert_eq!(notification.channel(), CATALOG_INVALIDATION_CHANNEL);
    assert_eq!(notification.payload(), tenant_id.to_string());
}

#[tokio::test]
async fn bootstrap_receipt_releases_complete_history_as_read() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let principal_id = musteragent_identity::PrincipalId::generate();
    let participant_id = ParticipantId::generate();
    let epoch = HomeEpoch::new(1).expect("home epoch");
    let mut fixture = pool.begin().await.expect("begin fixture");
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut *fixture)
        .await
        .expect("defer fixture constraints");
    sqlx::query(
        "INSERT INTO conversation.conversations(
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms, workspace_id,
             execution_owner_participant_id
         ) VALUES($1, $2, 1, 'Bootstrap fixture', 1, NULL, 1, 1, $3,
                  1000, 1000, NULL, NULL, $4)",
    )
    .bind(conversation_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(principal_id.into_uuid())
    .bind(participant_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("insert Conversation");
    sqlx::query(
        "INSERT INTO conversation.participants(
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES($1, $2, $3, 1, $4, NULL, NULL, 'User', NULL, 1000, NULL)",
    )
    .bind(participant_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(principal_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("insert participant");
    sqlx::query(
        "INSERT INTO event_store.ledger_streams(
             tenant_id, conversation_id, home_epoch, last_sequence,
             retained_from_sequence, created_at_ms, updated_at_ms
         ) VALUES($1, $2, 1, 1, 1, 1000, 1000)",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("insert Ledger stream");
    sqlx::query(
        "INSERT INTO projection.conversation_catalog_bootstrap_receipts(
             tenant_id, conversation_id, principal_id
         ) VALUES($1, $2, $3)",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(principal_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("queue read baseline");
    fixture.commit().await.expect("commit fixture");

    let store = PostgresConversationCatalogStore::new(pool.clone());
    for (source_key, audience, committed_at, kind) in [
        (
            CatalogSourceKey::new(
                CatalogSourceKind::ConversationCreated,
                CatalogSourceIdentity::Conversation(conversation_id),
                1,
            )
            .expect("created source"),
            CatalogAttentionAudience::AllExceptPrincipal(principal_id),
            2_000,
            CatalogFactKind::ConversationCreated,
        ),
        (
            CatalogSourceKey::new(
                CatalogSourceKind::VisibleOutput,
                CatalogSourceIdentity::Message(MessageId::generate()),
                1,
            )
            .expect("historical output source"),
            CatalogAttentionAudience::AllParticipants,
            3_000,
            CatalogFactKind::VisibleOutput,
        ),
    ] {
        store
            .apply_fact(CatalogFactDraft {
                control_home: RuntimeHome::Cloud,
                tenant_id,
                conversation_id,
                home_epoch: epoch,
                source_key,
                source_record_id: None,
                audience,
                occurred_at: UtcTimestamp::from_unix_millis(committed_at - 10),
                committed_at: UtcTimestamp::from_unix_millis(committed_at),
                kind,
            })
            .await
            .expect("project historical fact");
    }
    let query = ListConversationCatalogV2 {
        actor: Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        },
        conversation_id: None,
        workspace_id: None,
        lifecycle: ConversationLifecycle::Active,
        page_size: 20,
        after: None,
    };
    assert!(
        store
            .list_catalog(query)
            .await
            .expect("list fenced Catalog")
            .conversations
            .is_empty()
    );
    sqlx::query(
        "INSERT INTO projection.conversation_catalog_projector_checkpoints(
             tenant_id, conversation_id, home_epoch, through_conversation_seq,
             projected_at_ms
         ) VALUES($1, $2, 1, 1, 4000)",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("advance Catalog checkpoint");
    assert_eq!(
        musteragent_projection::ConversationCatalogProjectionPort::finalize_bootstrap_receipts(
            &store,
            20,
            UtcTimestamp::from_unix_millis(4_001),
        )
        .await
        .expect("finalize read baseline"),
        1
    );

    let page = store
        .list_catalog(query)
        .await
        .expect("list released Catalog");
    assert_eq!(page.conversations.len(), 1);
    assert_eq!(page.conversations[0].unread.count, 0);
    assert_eq!(
        page.conversations[0].unread.seen_through,
        AttentionRevision::new(2)
    );
    assert_eq!(
        page.conversations[0].unread.latest_visible_revision,
        AttentionRevision::new(2)
    );
}

#[tokio::test]
async fn cloud_claim_reduce_delivery_and_receipt_are_transactional() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let principal_id = PrincipalId::generate();
    let participant_id = ParticipantId::generate();
    let epoch = HomeEpoch::new(1).expect("home epoch");
    let mut fixture = pool.begin().await.expect("begin fixture");
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut *fixture)
        .await
        .expect("defer fixture constraints");
    sqlx::query(
        "INSERT INTO conversation.conversations(
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms, workspace_id,
             execution_owner_participant_id
         ) VALUES($1, $2, 1, 'Fixture', 1, NULL, 1, 1, $3,
                  1000, 1000, NULL, NULL, $4)",
    )
    .bind(conversation_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(principal_id.into_uuid())
    .bind(participant_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("insert Conversation");
    sqlx::query(
        "INSERT INTO conversation.participants(
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES($1, $2, $3, 1, $4, NULL, NULL, 'User', NULL, 1000, NULL)",
    )
    .bind(participant_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(principal_id.into_uuid())
    .execute(&mut *fixture)
    .await
    .expect("insert participant");
    fixture.commit().await.expect("commit fixture");

    let store = PostgresConversationCatalogStore::new(pool.clone());
    assert!(
        store
            .list_goal_projection_snapshots(20)
            .await
            .expect("query empty Goal projection work")
            .is_empty()
    );
    assert!(
        store
            .list_goal_evaluation_projection_snapshots(20)
            .await
            .expect("query empty Goal evaluation projection work")
            .is_empty()
    );
    let message_id = MessageId::generate();
    let draft = CatalogFactDraft {
        control_home: RuntimeHome::Cloud,
        tenant_id,
        conversation_id,
        home_epoch: epoch,
        source_record_id: None,
        source_key: CatalogSourceKey::new(
            CatalogSourceKind::VisibleOutput,
            CatalogSourceIdentity::Message(message_id),
            1,
        )
        .expect("source key"),
        audience: CatalogAttentionAudience::AllParticipants,
        occurred_at: UtcTimestamp::from_unix_millis(1_900),
        committed_at: UtcTimestamp::from_unix_millis(2_000),
        kind: CatalogFactKind::VisibleOutput,
    };
    assert_eq!(
        store.apply_fact(draft.clone()).await.expect("apply output"),
        CatalogProjectionApplyDisposition::Applied
    );
    assert_eq!(
        store.apply_fact(draft).await.expect("deduplicate output"),
        CatalogProjectionApplyDisposition::Duplicate
    );
    let page = store
        .list_catalog(ListConversationCatalogV2 {
            actor: Actor {
                tenant_id,
                principal_id,
                principal_kind: PrincipalKind::User,
            },
            conversation_id: None,
            workspace_id: None,
            lifecycle: ConversationLifecycle::Active,
            page_size: 20,
            after: None,
        })
        .await
        .expect("list catalog");
    assert_eq!(page.snapshot_position, CatalogActivityPosition::new(1));
    assert_eq!(page.conversations.len(), 1);
    assert_eq!(page.conversations[0].unread.count, 1);
    let mut plan_transaction = pool.begin().await.expect("begin query-plan assertion");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *plan_transaction)
        .await
        .expect("prefer catalog index in bounded fixture");
    let plan = sqlx::query(
        "EXPLAIN (FORMAT TEXT)
         SELECT conversation_id FROM projection.conversation_catalog
         WHERE tenant_id = $1 AND lifecycle = $2
         ORDER BY last_user_visible_activity_at_ms DESC, conversation_id DESC
         LIMIT 101",
    )
    .bind(tenant_id.into_uuid())
    .bind(encode_lifecycle(ConversationLifecycle::Active))
    .fetch_all(&mut *plan_transaction)
    .await
    .expect("catalog query plan");
    assert!(plan.iter().any(|row| {
        row.get::<String, _>("QUERY PLAN")
            .contains("conversation_catalog_list_idx")
    }));
    plan_transaction
        .rollback()
        .await
        .expect("rollback query-plan assertion");
    assert_eq!(
        page.conversations[0].unread.latest_unread_kind,
        Some(CatalogAttentionKind::Output)
    );
    let continuation = ConversationCatalogCursorV2 {
        snapshot_position: page.snapshot_position,
        after_activity_at: page.conversations[0].last_user_visible_activity_at,
        after_conversation_id: page.conversations[0].conversation_id,
    };
    assert!(
        store
            .acknowledge_seen_through(
                tenant_id,
                conversation_id,
                principal_id,
                epoch,
                AttentionRevision::new(1),
                UtcTimestamp::from_unix_millis(3_000),
            )
            .await
            .expect("acknowledge output")
    );
    let row = sqlx::query(
        "SELECT catalog.attention_revision::text AS attention_revision,
                catalog.last_user_visible_activity_at_ms,
                receipt.seen_through_revision::text AS seen_through_revision
         FROM projection.conversation_catalog AS catalog
         JOIN projection.conversation_read_receipts AS receipt
           USING(tenant_id, conversation_id)
         WHERE catalog.tenant_id = $1 AND catalog.conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("catalog and receipt");
    assert_eq!(row.get::<String, _>("attention_revision"), "1");
    assert_eq!(row.get::<i64, _>("last_user_visible_activity_at_ms"), 2_000);
    assert_eq!(row.get::<String, _>("seen_through_revision"), "1");
    let continued = store
        .list_catalog(ListConversationCatalogV2 {
            actor: Actor {
                tenant_id,
                principal_id,
                principal_kind: PrincipalKind::User,
            },
            conversation_id: None,
            workspace_id: None,
            lifecycle: ConversationLifecycle::Active,
            page_size: 20,
            after: Some(continuation),
        })
        .await
        .expect("continue pagination after receipt change");
    assert_eq!(continued.snapshot_position, CatalogActivityPosition::new(2));
    assert!(continued.conversations.is_empty());
    let stale_ack = ConversationCatalogV2Source::acknowledge(
        &store,
        AcknowledgeConversationSeenThrough {
            actor: Actor {
                tenant_id,
                principal_id,
                principal_kind: PrincipalKind::User,
            },
            conversation_id,
            expected_home_epoch: epoch,
            seen_through: AttentionRevision::ZERO,
            rendered_at: UtcTimestamp::from_unix_millis(4_000),
        },
    )
    .await
    .expect("stale receipt response");
    assert!(!stale_ack.changed);
    assert_eq!(stale_ack.seen_through, AttentionRevision::new(1));
}
