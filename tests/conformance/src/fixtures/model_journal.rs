//! Current model journal fixtures with canonical requests and explicit physical-stop proof.
use musteragent_identity::UtcTimestamp;
use musteragent_run::AttemptContext;
use sqlx::PgPool;

pub fn model_timing() -> musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming
{
    musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
        .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
        .expect("current model timing")
}

pub fn supervise(
    journal: musteragent_postgres::PostgresAgentLoopJournal,
) -> musteragent_postgres::PostgresAgentLoopJournal {
    journal.with_execution_liveness(
        std::sync::Arc::new(musteragent_wire::ProtobufSnapshotCodec),
        home_clock(),
    )
}

pub async fn freeze_model_request(
    content: &dyn musteragent_runtime_content::ContentStore,
    iteration: &mut musteragent_loop::ModelIteration,
) {
    let bytes = musteragent_context::ContextCodec::encode_model_request(
        &musteragent_wire::CanonicalJsonContextCodec,
        &iteration.request,
    )
    .expect("canonical request");
    iteration.request_ref = content
        .put(musteragent_runtime_content::PutContent {
            tenant_id: iteration.request.invocation.run_context.tenant_id,
            media_type: musteragent_content_ref::MediaType::new(
                musteragent_runtime_model::MODEL_REQUEST_MEDIA_TYPE,
            )
            .expect("request media type"),
            bytes,
        })
        .await
        .expect("freeze current model request");
}

fn home_clock() -> std::sync::Arc<musteragent_execution_liveness_io::SystemLivenessHomeClock> {
    static CLOCK: std::sync::OnceLock<
        std::sync::Arc<musteragent_execution_liveness_io::SystemLivenessHomeClock>,
    > = std::sync::OnceLock::new();
    CLOCK
        .get_or_init(|| {
            std::sync::Arc::new(
                musteragent_execution_liveness_io::SystemLivenessHomeClock::default(),
            )
        })
        .clone()
}

/// These journal fixtures dispatch no physical task or remote I/O. Their
/// in-process provider has returned, so the fixture owns positive stop proof.
pub async fn confirm_model_stop(
    pool: &PgPool,
    owner: &AttemptContext,
    invocation: musteragent_identity::InvocationId,
) {
    use musteragent_execution_liveness::{
        ControlStop, Event, FencedExecutionLivenessJournal, RecordMutation,
    };
    let codec = std::sync::Arc::new(musteragent_wire::ProtobufLedgerPayloadCodec);
    let journal = musteragent_postgres::PostgresFencedExecutionLivenessJournal::new(
        pool.clone(),
        std::sync::Arc::new(musteragent_wire::ProtobufSnapshotCodec),
        codec.clone(),
        codec,
        home_clock(),
    );
    let scope =
        musteragent_runtime_contract::execution_liveness::ExecutionScope::Invocation(invocation);
    let record = journal
        .load(owner, scope)
        .await
        .expect("original model scope")
        .expect("admitted model");
    if record.is_closed() {
        return;
    }
    let stopping = journal
        .mutate(
            owner,
            scope,
            record.revision(),
            RecordMutation::Event(Event::Stop(ControlStop::ExecutionFailed)),
        )
        .await
        .expect("stop fixture provider")
        .next;
    journal
        .mutate(
            owner,
            scope,
            stopping.revision(),
            RecordMutation::Event(Event::StopConfirmed),
        )
        .await
        .expect("fixture provider returned without remote work");
}

pub async fn finish_model(
    journal: &dyn musteragent_loop::AgentLoopJournal,
    owner: &AttemptContext,
    completion: musteragent_loop::ModelCompletionCommit,
) -> Result<musteragent_loop::DurableLoopCheckpoint, musteragent_loop::LoopPortError> {
    journal
        .stage_model_completion(owner, completion.clone())
        .await?;
    journal.finish_model(owner, completion).await
}

/// The controlled fixture provider has returned before logical failure closes.
pub async fn finish_failed_model(
    pool: &PgPool,
    journal: &dyn musteragent_loop::AgentLoopJournal,
    owner: &AttemptContext,
    failure: musteragent_loop::ModelFailureCommit,
) -> Result<musteragent_loop::DurableLoopCheckpoint, musteragent_loop::LoopPortError> {
    confirm_model_stop(pool, owner, failure.observation.invocation_id).await;
    journal.finish_failed_model(owner, failure).await
}

/// Complete current snapshot for journal fixtures that do not materialize context.
pub fn snapshot_bytes(run_id: musteragent_identity::RunId) -> Vec<u8> {
    use musteragent_runtime_snapshot::SnapshotCodec;
    let policy = super::agent::fixture_runtime_policy_content_ref().expect("fixture policy");
    let content = super::context_snapshot::snapshot_content(run_id, policy);
    musteragent_wire::ProtobufSnapshotCodec
        .encode_content(&content)
        .expect("complete current snapshot")
}
