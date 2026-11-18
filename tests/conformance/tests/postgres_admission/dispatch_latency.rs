use super::*;
use musteragent_postgres::{PostgresDispatchStore, PostgresWakeChannel, PostgresWakeSignal};
use musteragent_run::WorkerId;
use musteragent_scheduler::{DurableScheduler, ScheduleSignal, SchedulerPolicy};
use musteragent_wire::ProtobufSchedulerEventEncoder;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn warmed_cloud_admission_wakes_real_scheduler() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let store = admission_store(&pool);
    let signal = Arc::new(PostgresWakeSignal::new());
    let cancellation = CancellationToken::new();
    let watch = {
        let (signal, pool, token) = (signal.clone(), pool.clone(), cancellation.clone());
        tokio::spawn(async move {
            signal
                .run(&pool, PostgresWakeChannel::Dispatch, token)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(3), signal.wait())
        .await
        .expect("warmed cloud admission wakes real scheduler fixture");
    let dispatch = Arc::new(PostgresDispatchStore::new(
        pool.clone(),
        Arc::new(ProtobufSchedulerEventEncoder),
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let scheduler = DurableScheduler::new(
        SchedulerPolicy {
            claim_batch_size: 1,
            lease_duration: Duration::from_secs(300),
            idle_wakeup_ceiling: Duration::from_secs(1),
        },
        WorkerId::new("cloud-latency-acceptance")
            .expect("warmed cloud admission wakes real scheduler fixture"),
        dispatch,
        signal,
        tx,
    )
    .expect("warmed cloud admission wakes real scheduler fixture");
    let task = tokio::spawn(scheduler.run(cancellation.clone()));
    let mut admission = Vec::new();
    let mut upper_bound = Vec::new();
    let mut after_ack = Vec::new();
    for ordinal in 0..208 {
        let fixture = seed_fixture(&pool, None).await;
        let batch = admission_batch(&pool, &fixture, &format!("latency:{ordinal}")).await;
        seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
        let expected = batch.run.context.run_id;
        let started = Instant::now();
        store
            .admit(batch)
            .await
            .expect("warmed cloud admission wakes real scheduler fixture");
        let ack = Instant::now();
        let receipt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("warmed cloud admission wakes real scheduler fixture")
            .expect("warmed cloud admission wakes real scheduler fixture");
        let received = Instant::now();
        assert_eq!(receipt.fence.run_id, expected);
        if ordinal >= 8 {
            admission.push((ack - started).as_secs_f64() * 1000.0);
            upper_bound.push((received - started).as_secs_f64() * 1000.0);
            after_ack.push((received - ack).as_secs_f64() * 1000.0);
        }
    }
    cancellation.cancel();
    task.await
        .expect("warmed cloud admission wakes real scheduler fixture")
        .expect("warmed cloud admission wakes real scheduler fixture");
    watch
        .await
        .expect("warmed cloud admission wakes real scheduler fixture")
        .expect("warmed cloud admission wakes real scheduler fixture");
    println!(
        "LTC_DISPATCH_BENCH {}",
        serde_json::json!({"runtime":"cloud/postgres", "samples":200,
        "profile":if cfg!(debug_assertions) {"debug"} else {"optimized"},
        "fixture":"real PostgresAdmissionStore + separate LISTEN connection + running DurableScheduler; serial new conversations; no manual notify",
        "admission_ms":percentiles(admission),"commit_to_claim_upper_bound_ms":percentiles(upper_bound),
        "admission_return_to_claim_received_ms":percentiles(after_ack)})
    );
}
fn percentiles(mut v: Vec<f64>) -> serde_json::Value {
    v.sort_by(f64::total_cmp);
    serde_json::json!({"p50":v[99],"p95":v[189],"max":v[199]})
}
