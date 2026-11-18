use musteragent_extension::*;
use musteragent_postgres::PostgresExtensionJournal;
use musteragent_testkit::IsolatedPostgres;
use std::path::PathBuf;
#[path = "../../../tests/conformance/tests/support/running_loop.rs"]
mod running_loop;

#[tokio::test]
async fn extension_facts_are_idempotent_immutable_and_stop_at_the_original_lease_fence() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("extension fencing fixture");
    let pool = pg.pool().await.expect("extension fencing fixture");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("extension fencing fixture");
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let scope = ExtensionContext {
        run: running_loop::run_context(fixture),
        attempt,
        workspace_dir: PathBuf::from("/tmp/extension-fence"),
        session_key: "fixture".into(),
        channel: None,
        sandboxed: false,
        ingress: None,
    };
    let journal = PostgresExtensionJournal::new(pool.clone());
    let fact = ExtensionFact::Lifecycle {
        extension: "fixture".into(),
        hook: Hook::AgentStart,
        child_run_id: None,
    };
    journal
        .append(&scope, "start", fact.clone())
        .await
        .expect("extension fencing fixture");
    journal
        .append(&scope, "start", fact.clone())
        .await
        .expect("extension fencing fixture");
    assert_eq!(
        journal
            .read(&scope)
            .await
            .expect("extension fencing fixture")
            .len(),
        1
    );
    let outbox: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.outbox WHERE conversation_id=$1 AND topic='extension.fact'").bind(fixture.conversation_id.into_uuid()).fetch_one(&pool).await.expect("extension fencing fixture");
    assert_eq!(outbox, 1);
    assert!(
        journal
            .append(
                &scope,
                "start",
                ExtensionFact::Service {
                    service: "fixture".into(),
                    hook: Hook::ServiceStart
                }
            )
            .await
            .is_err()
    );
    for sql in [
        "DELETE FROM runtime.extension_facts WHERE run_id=$1",
        "UPDATE runtime.extension_facts SET fact_key='rewritten' WHERE run_id=$1",
    ] {
        assert!(
            sqlx::query(sql)
                .bind(fixture.run_id.into_uuid())
                .execute(&pool)
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE runtime.leases SET expires_at_ms=renewed_at_ms WHERE run_id=$1")
        .bind(fixture.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("extension fencing fixture");
    assert!(journal.append(&scope, "lost", fact).await.is_err());
    assert!(
        journal
            .append(
                &scope,
                "end",
                ExtensionFact::Lifecycle {
                    extension: "fixture".into(),
                    hook: Hook::AgentEnd,
                    child_run_id: None
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        journal
            .read(&scope)
            .await
            .expect("extension fencing fixture")
            .len(),
        1
    );
    let state: (i16,i16) = sqlx::query_as("SELECT r.status,a.status FROM runtime.runs r JOIN runtime.attempts a USING(run_id) WHERE r.run_id=$1").bind(fixture.run_id.into_uuid()).fetch_one(&pool).await.expect("extension fencing fixture");
    assert_eq!(state, (2, 1), "lease loss never seals the run or attempt");
    let mut foreign = scope;
    foreign.run.tenant_id = musteragent_identity::TenantId::generate();
    assert!(
        journal
            .read(&foreign)
            .await
            .expect("extension fencing fixture")
            .is_empty()
    );
}
