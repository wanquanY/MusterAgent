//! The host's sandbox routing context is durable in the same transaction as
//! the Run it governs, and derived Runs (retries, delegated children) carry it.
use std::collections::BTreeSet;

use musteragent_identity::RunId;
use musteragent_sandbox::{
    SandboxClientSurface, SandboxRoutingContext, SandboxRoutingPolicy, SandboxTrigger,
};
use musteragent_scheduler::AdmissionSelection;
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;

async fn routing_row(pool: &PgPool, run: RunId) -> Option<(i16, i16, i16)> {
    sqlx::query_as(
        "SELECT surface,trigger,policy FROM runtime.sandbox_routing_contexts WHERE run_id=$1",
    )
    .bind(run.into_uuid())
    .fetch_optional(pool)
    .await
    .expect("read sandbox routing context")
}

#[tokio::test]
async fn submit_retry_and_delegation_bind_sandbox_routing_atomically() {
    let _suite_guard = super::POSTGRES_ADMISSION_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    let harness = super::input_message::fixture::InputMessageHarness::new(pool.clone()).await;
    let (conversation_id, participant) = harness.create_conversation("sandbox-routing").await;

    // Without a host-bound context, admission writes none (unchanged behavior).
    let (_, unbound_run) = harness
        .submit_user_message(conversation_id, participant, "routing-unbound", "plain")
        .await;
    assert_eq!(routing_row(&pool, unbound_run).await, None);
    harness
        .claim_and_fail_run(unbound_run, "worker-routing-unbound")
        .await;

    // A browser-only host with an attended Web page binds its context on submit.
    let browser_only = SandboxRoutingContext {
        surface: SandboxClientSurface::Web,
        trigger: SandboxTrigger::Interactive,
        policy: SandboxRoutingPolicy::BrowserOnly,
    };
    let (message_id, run_id) = harness
        .submit_user_message_with_selection(
            conversation_id,
            participant,
            "routing-browser-only",
            "run python in my page",
            AdmissionSelection::web(BTreeSet::new()).with_sandbox_routing(browser_only),
        )
        .await;
    let expected = Some((1, 1, 2));
    assert_eq!(
        routing_row(&pool, run_id).await,
        expected,
        "the route authority is visible as soon as the Run is"
    );

    // A delegated child serves the same attended turn and inherits the context.
    let attempt = harness.claim_run(run_id, "worker-routing-parent").await;
    let child = harness
        .spawn_delegated_child(&attempt, "delegated work in the same page")
        .await;
    assert_eq!(routing_row(&pool, child).await, expected);
    harness
        .claim_and_fail_run(child, "worker-routing-child")
        .await;
    harness.fail_claimed_attempt(&attempt).await;

    // A retry without a new context inherits the source Run's context.
    let retry = harness
        .retry_failed_run(
            conversation_id,
            participant,
            run_id,
            message_id,
            1,
            "routing-retry-inherit",
        )
        .await;
    assert_eq!(routing_row(&pool, retry).await, expected);
    harness
        .claim_and_fail_run(retry, "worker-routing-retry")
        .await;

    // A retry with an explicit context binds that context instead.
    let server_only = SandboxRoutingContext {
        surface: SandboxClientSurface::Web,
        trigger: SandboxTrigger::Interactive,
        policy: SandboxRoutingPolicy::ServerOnly,
    };
    let explicit = harness
        .retry_failed_run_with_selection(
            conversation_id,
            participant,
            retry,
            message_id,
            1,
            "routing-retry-explicit",
            AdmissionSelection::web(BTreeSet::new()).with_sandbox_routing(server_only),
        )
        .await;
    assert_eq!(routing_row(&pool, explicit).await, Some((1, 1, 3)));
}

#[tokio::test]
async fn delegated_child_of_an_unbound_parent_keeps_the_unattended_server_default() {
    let _suite_guard = super::POSTGRES_ADMISSION_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    let harness = super::input_message::fixture::InputMessageHarness::new(pool.clone()).await;
    let (conversation_id, participant) = harness.create_conversation("sandbox-default").await;
    let (_, run_id) = harness
        .submit_user_message(conversation_id, participant, "default-parent", "plain")
        .await;
    let attempt = harness.claim_run(run_id, "worker-default-parent").await;
    let child = harness
        .spawn_delegated_child(&attempt, "unattended delegated work")
        .await;
    assert_eq!(routing_row(&pool, child).await, Some((4, 2, 3)));
}
