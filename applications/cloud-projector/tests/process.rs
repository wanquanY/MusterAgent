use musteragent_testkit::IsolatedPostgres;
use std::time::Duration;
use tokio::process::Command;
#[path = "../../../tests/conformance/tests/support/running_loop.rs"]
mod running_loop;
#[tokio::test]
async fn projector_consumes_dispatch_and_stops_cleanly() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("projector process fixture");
    let pool = pg.pool().await.expect("projector process fixture");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("projector process fixture");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let message = uuid::Uuid::now_v7();
    let payload = format!("run:{}", fixture.run_id).into_bytes();
    sqlx::query("INSERT INTO event_store.outbox(message_id,tenant_id,conversation_id,topic,payload,payload_hash,committed_at_ms,available_at_ms) VALUES($1,$2,$3,'run.dispatch',$4,$5,1,1)")
        .bind(message).bind(fixture.tenant_id.into_uuid()).bind(fixture.conversation_id.into_uuid()).bind(&payload).bind(musteragent_content_ref::Sha256::digest(&payload).into_bytes().to_vec()).execute(&pool).await.expect("projector process fixture");
    let mut child = Command::new(env!("CARGO_BIN_EXE_musteragent-cloud-projector"))
        .env("MUSTERAGENT_DATABASE_URL", pg.connection_url())
        .env("MUSTERAGENT_PROJECTOR_ID", "p9-projector")
        .env("MUSTERAGENT_OTLP_MODE", "disabled")
        .kill_on_drop(true)
        .spawn()
        .expect("projector process fixture");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(
                child
                    .try_wait()
                    .expect("projector process fixture")
                    .is_none(),
                "projector must remain alive"
            );
            let published: bool = sqlx::query_scalar(
                "SELECT published_at_ms IS NOT NULL FROM event_store.outbox WHERE message_id=$1",
            )
            .bind(message)
            .fetch_one(&pool)
            .await
            .expect("projector process fixture");
            if published {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("projector process fixture");
    let id = child.id().expect("projector process fixture");
    assert!(
        Command::new("kill")
            .args(["-TERM", &id.to_string()])
            .status()
            .await
            .expect("projector process fixture")
            .success()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("projector process fixture")
            .expect("projector process fixture")
            .success()
    );
}
