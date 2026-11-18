//! Real handlers and fenced PostgreSQL owners on a disposable pgvector database.
use super::*;
use musteragent_capability::CapabilityLocation;
use musteragent_runtime_capability::{
    CapabilityInvocation, DurableToolInvocation, RuntimeClock, ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_tool_executor::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, builtin_capability_manifest_at_location,
};
use serde_json::{Value, json};
mod batch;
mod material;
mod media;
mod session;
mod snapshot;
mod work_item;

struct Clock(UtcTimestamp);
impl RuntimeClock for Clock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}
struct Call {
    attempt: AttemptContext,
    material: ToolExecutionMaterial,
    durable: DurableToolInvocation,
    invocation: CapabilityInvocation,
    manifest: musteragent_capability::CapabilityManifest,
}
fn location(d: &musteragent_tool::BuiltinToolDefinition) -> CapabilityLocation {
    if matches!(
        d.target,
        musteragent_tool::CapabilityExecutionTarget::BackendNamed(_)
    ) {
        CapabilityLocation::Backend
    } else {
        CapabilityLocation::Cloud
    }
}
async fn setup() -> Fixture {
    let mut f = super::super::fixture_with_reply_contract(true).await;
    f.content = Some(super::super::super::receipt::attachment_fixture::content_store(&f.pool));
    append_admission(&f, &f.command.fence).await;
    f
}
async fn call(f: &Fixture, key: &str, args: Value) -> Call {
    let (run, attempt) = context(f, &f.command.fence).await;
    let definition = musteragent_tool::BuiltinToolCatalog::load_cloud()
        .expect("valid durable execution fixture")
        .by_key(&ToolKey::from_versioned_key(
            VersionedKey::new(key).expect("valid durable execution fixture"),
        ))
        .expect("valid durable execution fixture")
        .clone();
    let provider = CapabilityProviderId::generate();
    let material = material::material(f, run, &definition, provider);
    let request = f
        .content
        .as_ref()
        .expect("valid durable execution fixture")
        .put_at(
            ContentId::generate(),
            PutContent {
                tenant_id: f.owner.tenant_id,
                media_type: MediaType::new("application/json")
                    .expect("valid durable execution fixture"),
                bytes: serde_json::to_vec(&args).expect("valid durable execution fixture"),
            },
        )
        .await
        .expect("valid durable execution fixture");
    let id = InvocationId::generate();
    let invocation = ToolInvocation {
        invocation_id: id,
        run_id: run.run_id,
        requested_by_attempt_id: attempt.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new(id.to_string()).expect("valid durable execution fixture"),
        tool_key: definition.key.clone(),
        capability_provider_id: provider,
        status: ToolInvocationStatus::Requested,
        effect: definition.effect,
        approval: definition.approval,
        idempotency_key: IdempotencyKey::new(id.to_string())
            .expect("valid durable execution fixture"),
        request_ref: request,
        result_ref: None,
        interaction_id: None,
        timing: InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(now(f).await.unix_millis() + 60_000),
        },
    };
    let store = PostgresToolInvocationStore::new(f.pool.clone(), keys());
    // Storage fixture uses the canonical invocation timing codec. Terminal commits
    // are tested below; no test depends on a synthetic provider success receipt.
    store
        .begin(
            f.owner.tenant_id,
            run.conversation_id,
            invocation.clone(),
            definition.idempotency.retry_contract(),
        )
        .await
        .expect("valid durable execution fixture");
    store
        .start(f.owner.tenant_id, id, attempt.attempt_id, now(f).await)
        .await
        .expect("valid durable execution fixture");
    let mut invocation = invocation;
    invocation.execution_attempt_id = Some(attempt.attempt_id);
    invocation.status = ToolInvocationStatus::Running;
    let capability = CapabilityInvocation {
        actor: f.owner,
        tenant_id: f.owner.tenant_id,
        conversation_id: run.conversation_id,
        run_id: run.run_id,
        requested_by_attempt_id: attempt.attempt_id,
        execution_attempt_id: attempt.attempt_id,
        execution_attempt_number: attempt.attempt_number,
        invocation_id: id,
        tool_call_id: invocation.tool_call_id.clone(),
        tool_key: definition.key.clone(),
        capability_key: musteragent_capability::CapabilityKey::from_versioned_key(
            definition.capability_key.clone(),
        ),
        capability_provider_id: provider,
        idempotency_key: invocation.idempotency_key.clone(),
        request_ref: invocation.request_ref.clone(),
        request_schema: material.tool.manifest.input_schema.clone(),
        result_schema: material.tool.manifest.output_schema.clone(),
        authorized_content: vec![],
        workspace_id: None,
        timing: invocation.timing.clone(),
        trace_context: musteragent_runtime_contract::TraceContext {
            traceparent: musteragent_runtime_contract::TraceParent::new(
                "00-00000000000000000000000000000001-0000000000000001-00",
            )
            .expect("valid durable execution fixture"),
            tracestate: None,
        },
        execution_grant_proof: None,
    };
    Call {
        attempt,
        material,
        manifest: builtin_capability_manifest_at_location(
            &definition,
            provider,
            location(&definition),
        )
        .expect("valid durable execution fixture"),
        durable: DurableToolInvocation {
            sandbox_route: None,
            request_record_id: LedgerRecordId::generate(),
            invocation,
            retry_contract: definition.idempotency.retry_contract(),
            prebound_reconciliation_ref: None,
            recorded_outcome: None,
            interaction_resolution: None,
        },
        invocation: capability,
    }
}
async fn result(f: &Fixture, outcome: DispatchedCapabilityOutcome) -> Value {
    let DispatchedCapabilityOutcome::Committed(result) = outcome else {
        panic!("expected committed result: {outcome:?}")
    };
    serde_json::from_slice(
        &f.content
            .as_ref()
            .expect("valid durable execution fixture")
            .read(f.owner.tenant_id, &result.result_ref)
            .await
            .expect("valid durable execution fixture"),
    )
    .expect("valid durable execution fixture")
}

// Advance the database's current authority, retaining the old AttemptContext
// so tests reject a genuinely stale writer rather than a fabricated future token.
async fn advance_fence(f: &Fixture, old: &AttemptContext) -> AttemptContext {
    let mut current = old.clone();
    current.fencing_token = FencingToken::new(old.fencing_token.get() + 1).expect("valid P7 fence");
    let mut tx = f.pool.begin().await.expect("begin fence transition");
    for statement in [
        "UPDATE runtime.attempts SET fencing_token=$1 WHERE attempt_id=$2 AND fencing_token=$3",
        "UPDATE runtime.leases SET fencing_token=$1 WHERE attempt_id=$2 AND fencing_token=$3",
    ] {
        let changed = sqlx::query(statement)
            .bind(current.fencing_token.get() as i64)
            .bind(old.attempt_id.into_uuid())
            .bind(old.fencing_token.get() as i64)
            .execute(&mut *tx)
            .await
            .expect("advance fixture authority");
        assert_eq!(changed.rows_affected(), 1);
    }
    tx.commit().await.expect("commit fence transition");
    current
}
