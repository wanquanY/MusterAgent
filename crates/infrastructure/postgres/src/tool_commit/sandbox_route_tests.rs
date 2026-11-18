use super::test_fixture::*;
use super::*;
use musteragent_identity::FencingToken;
use musteragent_runtime_capability::BrowserProviderRuntimeError;
use musteragent_sandbox::*;

#[tokio::test]
async fn route_round_trips_replays_and_conflicts_without_duplicate_facts() {
    for executor in [SandboxExecutor::Browser, SandboxExecutor::Server] {
        let f = Fixture::new().await;
        let commit = f.commit(executor);
        let stored = f
            .journal
            .commit_sandbox_route(&f.attempt, commit)
            .await
            .unwrap();
        assert_eq!(stored.sandbox_route, Some(commit.route));
        let reloaded = f
            .journal
            .load_invocation(f.actor.tenant_id, f.invocation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.sandbox_route, Some(commit.route));
        assert_eq!(
            f.journal
                .commit_sandbox_route(&f.attempt, commit)
                .await
                .unwrap()
                .sandbox_route,
            Some(commit.route)
        );
        assert!(matches!(
            f.journal
                .commit_sandbox_route(
                    &f.attempt,
                    f.commit(match executor {
                        SandboxExecutor::Browser => SandboxExecutor::Server,
                        SandboxExecutor::Server => SandboxExecutor::Browser,
                    })
                )
                .await,
            Err(BrowserProviderRuntimeError::RouteConflict)
        ));
        let expected = StorageCodec.encode(LedgerPayload::DomainEvent(DomainEvent::Tool(
            commit.event(),
        )));
        let events:Vec<Vec<u8>>=sqlx::query_scalar("SELECT encoded_event FROM event_store.domain_events e JOIN event_store.ledger_records r USING(record_id) WHERE r.run_id=$1 ORDER BY r.run_seq")
        .bind(f.attempt.run_id.into_uuid()).fetch_all(&f.pool).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[2], expected);
        let outbox: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM event_store.outbox WHERE topic='musteragent.tool.sandbox-routed'",
        )
        .fetch_all(&f.pool)
        .await
        .unwrap();
        assert_eq!(outbox, vec![expected]);
        let (sequence,): (i64,) =
            sqlx::query_as("SELECT last_run_sequence FROM runtime.runs WHERE run_id=$1")
                .bind(f.attempt.run_id.into_uuid())
                .fetch_one(&f.pool)
                .await
                .unwrap();
        assert_eq!(sequence, 3); // Admission, requested, then the single route fact.
        f.pool.close().await;
    }
}

#[tokio::test]
async fn stale_and_cancelled_attempts_cannot_record_routes() {
    let f = Fixture::new().await;
    let mut stale = f.attempt.clone();
    stale.fencing_token = FencingToken::new(2).unwrap();
    assert!(matches!(
        f.journal
            .commit_sandbox_route(&stale, f.commit(SandboxExecutor::Server))
            .await,
        Err(BrowserProviderRuntimeError::StaleLease)
    ));
    sqlx::query("UPDATE runtime.runs SET cancel_requested_at_ms=1 WHERE run_id=$1")
        .bind(f.attempt.run_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(matches!(
        f.journal
            .commit_sandbox_route(&f.attempt, f.commit(SandboxExecutor::Server))
            .await,
        Err(BrowserProviderRuntimeError::StaleLease)
    ));
    sqlx::query("UPDATE runtime.runs SET cancel_requested_at_ms=NULL WHERE run_id=$1")
        .bind(f.attempt.run_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runtime.leases SET expires_at_ms=1 WHERE run_id=$1")
        .bind(f.attempt.run_id.into_uuid())
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(matches!(
        f.journal
            .commit_sandbox_route(&f.attempt, f.commit(SandboxExecutor::Server))
            .await,
        Err(BrowserProviderRuntimeError::StaleLease)
    ));
    assert!(
        f.journal
            .load_invocation(f.actor.tenant_id, f.invocation)
            .await
            .unwrap()
            .unwrap()
            .sandbox_route
            .is_none()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM event_store.outbox")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    f.pool.close().await;
}
