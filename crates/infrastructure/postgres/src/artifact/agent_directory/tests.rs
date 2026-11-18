//! Exercises the actual directory candidate SQL in disposable PostgreSQL.
//! This is a query-contract test, not a substitute for the publication/auth tests.
use super::*;
use musteragent_identity::{AgentId, PrincipalId, TenantId};
use musteragent_testkit::IsolatedPostgres;

#[tokio::test]
async fn query_filters_current_mime_origin_and_publication_before_paging() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    let mut db = pool.acquire().await.unwrap();
    sqlx::raw_sql(
        "CREATE SCHEMA agent; CREATE SCHEMA artifact; CREATE SCHEMA resource;
         CREATE TABLE agent.agent_dialogue_entries (
           entry_id int,seq bigint,artifact_id int,tenant_id uuid,owner_id uuid,
           dialogue_id uuid,author_agent_id uuid,epoch bigint,kind text);
         CREATE TABLE agent.dialogue_publications (
           entry_id int,tenant_id uuid,owner_id uuid,dialogue_id uuid,agent_id uuid);
         CREATE TABLE artifact.artifacts (
           artifact_id int,tenant_id uuid,origin smallint,kind smallint,lifecycle smallint,current_version bigint);
         CREATE TABLE artifact.artifact_versions (artifact_id int,version bigint,content_id int);
         CREATE TABLE artifact.content_refs (tenant_id uuid,content_id int,media_type text);
         -- Since 556eb6ebd7 a returned task result pins its delivered version.
         CREATE TABLE artifact.work_item_result_asset_returns (
           entry_id int,producer_id uuid,requester_id uuid,version bigint);
         CREATE FUNCTION resource.account_file_owned(uuid,uuid,uuid,uuid,int,int)
           RETURNS boolean LANGUAGE sql AS 'SELECT false';",
    ).execute(&mut *db).await.unwrap();
    let query = AgentArtifactDirectoryQuery {
        actor: Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        agent_id: AgentId::generate(),
        origin: Some(AgentArtifactOrigin::Agent),
        file_types: vec![],
        cursor: None,
        limit: 1,
    };
    let dialogue = AgentDialogueId::generate();
    let samples = [
        (1, "text/markdown", 1_i16),
        (2, "image/png", 1),
        (3, "video/mp4", 1),
        (4, "application/pdf", 3), // user-created, not Agent output
        (5, "audio/wav", 1),       // private, not published
        (
            6,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            1,
        ),
        (7, "text/tab-separated-values; charset=utf-8", 1),
    ];
    for (id, mime, origin) in samples {
        sqlx::query("INSERT INTO artifact.artifacts VALUES($1,$2,$3,2,1,1)")
            .bind(id)
            .bind(query.actor.tenant_id.into_uuid())
            .bind(origin)
            .execute(&mut *db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact.artifact_versions VALUES($1,1,$1)")
            .bind(id)
            .execute(&mut *db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact.content_refs VALUES($1,$2,$3)")
            .bind(query.actor.tenant_id.into_uuid())
            .bind(id)
            .bind(mime)
            .execute(&mut *db)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO agent.agent_dialogue_entries VALUES($1,$2,$1,$3,$4,$5,$6,1,'artifact')",
        )
        .bind(id)
        .bind(i64::from(id))
        .bind(query.actor.tenant_id.into_uuid())
        .bind(query.actor.principal_id.into_uuid())
        .bind(dialogue.into_uuid())
        .bind(query.agent_id.into_uuid())
        .execute(&mut *db)
        .await
        .unwrap();
        if id != 5 {
            sqlx::query("INSERT INTO agent.dialogue_publications VALUES($1,$2,$3,$4,$5)")
                .bind(id)
                .bind(query.actor.tenant_id.into_uuid())
                .bind(query.actor.principal_id.into_uuid())
                .bind(dialogue.into_uuid())
                .bind(query.agent_id.into_uuid())
                .execute(&mut *db)
                .await
                .unwrap();
        }
    }
    let all = candidates(
        &mut db,
        &query,
        dialogue,
        1,
        &Window {
            at: 7,
            before: None,
        },
        &[],
        25,
    )
    .await
    .unwrap();
    let seqs = |rows: &[sqlx::postgres::PgRow]| {
        rows.iter()
            .map(|row| row.get::<i64, _>("seq"))
            .collect::<Vec<_>>()
    };
    assert_eq!(seqs(&all), [7, 6, 3, 2, 1]);
    let types = vec!["image".into(), "video".into()];
    let first = candidates(
        &mut db,
        &query,
        dialogue,
        1,
        &Window {
            at: 7,
            before: None,
        },
        &types,
        1,
    )
    .await
    .unwrap();
    assert_eq!(seqs(&first), [3]);
    let second = candidates(
        &mut db,
        &query,
        dialogue,
        1,
        &Window {
            at: 7,
            before: Some(3),
        },
        &types,
        1,
    )
    .await
    .unwrap();
    assert_eq!(seqs(&second), [2]);
    let office = candidates(
        &mut db,
        &query,
        dialogue,
        1,
        &Window {
            at: 7,
            before: None,
        },
        &["office".into()],
        25,
    )
    .await
    .unwrap();
    assert_eq!(seqs(&office), [6]);
    let table = candidates(
        &mut db,
        &query,
        dialogue,
        1,
        &Window {
            at: 7,
            before: None,
        },
        &["table".into()],
        25,
    )
    .await
    .unwrap();
    assert_eq!(seqs(&table), [7]);
    // Current head MIME, not a stale delivered filename or extension.
    sqlx::query("UPDATE artifact.content_refs SET media_type='application/pdf' WHERE content_id=2")
        .execute(&mut *db)
        .await
        .unwrap();
    let changed = candidates(
        &mut db,
        &query,
        dialogue,
        1,
        &Window {
            at: 7,
            before: None,
        },
        &types,
        25,
    )
    .await
    .unwrap();
    assert_eq!(seqs(&changed), [3]);
    let owned = AgentArtifactDirectoryQuery {
        origin: Some(AgentArtifactOrigin::AccountOwner),
        ..query.clone()
    };
    assert_eq!(
        seqs(
            &candidates(
                &mut db,
                &owned,
                dialogue,
                1,
                &Window {
                    at: 7,
                    before: None
                },
                &[],
                25
            )
            .await
            .unwrap()
        ),
        [4]
    );
    let cursor_query = AgentArtifactDirectoryQuery {
        file_types: types,
        ..query
    };
    let cursor = continuation(&cursor_query, dialogue, 1, 7, 3);
    let continued = AgentArtifactDirectoryQuery {
        cursor: Some(cursor),
        ..cursor_query
    };
    assert!(window(&continued, dialogue, 1, 7).is_ok());
    assert!(
        window(
            &AgentArtifactDirectoryQuery {
                file_types: vec!["image".into()],
                ..continued
            },
            dialogue,
            1,
            7
        )
        .is_err()
    );
}
