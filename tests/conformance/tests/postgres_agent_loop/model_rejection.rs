use super::*;
use musteragent_execution_liveness::{
    Closure, ControlStop, Event, ExecutionRecord, FencedExecutionLivenessJournal, Phase,
    RecordMutation, StopReason as ExecutionStopReason,
};
use musteragent_loop::{ModelActionRejection, ModelActionRejectionCommit};
use musteragent_postgres::PostgresFencedExecutionLivenessJournal;
use musteragent_runtime_contract::execution_liveness::ExecutionScope;

pub(crate) struct ModelJournals<'a> {
    pub(crate) pool: &'a sqlx::PgPool,
    pub(crate) journal: &'a PostgresAgentLoopJournal,
    pub(crate) live: &'a PostgresFencedExecutionLivenessJournal,
}

fn rejection(
    observation: musteragent_loop::ModelInvocationObservation,
) -> ModelActionRejectionCommit {
    ModelActionRejectionCommit {
        observation,
        rejection: ModelActionRejection::ToolNotExposed {
            attempted_name: WireName::new("unexposed_action").expect("name"),
        },
    }
}
fn phase(record: ExecutionRecord) -> Phase {
    match record {
        ExecutionRecord::Checkpoint(c) => c.state().phase(),
        _ => panic!("original checkpoint"),
    }
}
pub(super) async fn terminal(
    ModelJournals {
        pool,
        journal,
        live,
    }: ModelJournals<'_>,
    owner: &AttemptContext,
    iteration: &ModelIteration,
    scope: ExecutionScope,
    usage: ModelUsage,
    cancel: bool,
) {
    let observed = journal
        .load_model_observation(owner, iteration.invocation_id)
        .await
        .expect("observation");
    assert_eq!(observed.usage, Some(usage));
    if cancel {
        // A persisted original stop whose notification has not reached the
        // reactor. The real Journal must arbitrate in its own commit lane.
        sqlx::query("UPDATE runtime.runs SET status=4,cancel_requested_at_ms=1 WHERE run_id=$1")
            .bind(owner.run_id.into_uuid())
            .execute(pool)
            .await
            .expect("durable stop before model close");
        let committed = journal
            .finish_rejected_model_action(owner, rejection(observed))
            .await
            .expect("stop wins");
        assert_eq!(committed.phase, LoopPhase::Cancelled);
        // Cancellation ends the original Attempt lease. Inspect its immutable
        // stored record using the original codec, never reacquire that stale lease.
        let row: (Vec<u8>, Vec<u8>) = sqlx::query_as("SELECT encoded_record,record_hash FROM runtime.execution_liveness WHERE run_id=$1 AND scope_kind=1 AND scope_invocation_id=$2")
            .bind(owner.run_id.into_uuid()).bind(iteration.invocation_id.into_uuid()).fetch_one(pool).await.expect("retained terminal");
        assert_eq!(
            Sha256::digest(&row.0).into_bytes().as_slice(),
            row.1.as_slice()
        );
        let retained = musteragent_wire::decode_execution_liveness_record(&row.0)
            .expect("canonical original record");
        assert_eq!(retained.binding().run_id, owner.run_id);
        assert_eq!(retained.binding().owner_attempt_id, owner.attempt_id);
        assert_eq!(retained.binding().scope, scope);
        assert_eq!(phase(retained), Phase::Closed(Closure::TerminalObserved));
        let rejected: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime.model_action_rejections WHERE invocation_id=$1",
        )
        .bind(iteration.invocation_id.into_uuid())
        .fetch_one(pool)
        .await
        .expect("no correction");
        assert_eq!(rejected, 0);
        return;
    }
    let before = live
        .load(owner, scope)
        .await
        .expect("live")
        .expect("original");
    sqlx::query("CREATE FUNCTION fail_rejection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected rejection insert failure'; END $$").execute(pool).await.expect("fault");
    sqlx::query("CREATE TRIGGER fail_rejection BEFORE INSERT ON runtime.model_action_rejections FOR EACH ROW EXECUTE FUNCTION fail_rejection()").execute(pool).await.expect("fault");
    assert!(
        journal
            .finish_rejected_model_action(owner, rejection(observed.clone()))
            .await
            .is_err()
    );
    assert_eq!(
        live.load(owner, scope).await.expect("rolled back"),
        Some(before)
    );
    sqlx::query("DROP TRIGGER fail_rejection ON runtime.model_action_rejections")
        .execute(pool)
        .await
        .expect("remove fault");
    let committed = journal
        .finish_rejected_model_action(owner, rejection(observed))
        .await
        .expect("atomic rejection");
    assert_eq!(committed.phase, LoopPhase::ReadyForModel);
    assert_eq!(
        phase(
            live.load(owner, scope)
                .await
                .expect("live")
                .expect("original")
        ),
        Phase::Closed(Closure::TerminalObserved)
    );
    let row = sqlx::query("SELECT m.status,r.error_code, m.terminal_stop_reason FROM runtime.model_invocations m JOIN runtime.model_action_rejections r USING(invocation_id) WHERE m.invocation_id=$1").bind(iteration.invocation_id.into_uuid()).fetch_one(pool).await.expect("failed semantic call");
    assert_eq!(row.get::<i16, _>("status"), 3);
    assert_eq!(row.get::<String, _>("error_code"), "tool_not_exposed");
    assert!(row.get::<Option<i16>, _>("terminal_stop_reason").is_some());
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.pending_tool_calls WHERE run_id=$1")
            .bind(owner.run_id.into_uuid())
            .fetch_one(pool)
            .await
            .expect("no Tool admission");
    assert_eq!(count, 0);
}
pub(super) async fn unknown(
    ModelJournals {
        pool,
        journal,
        live,
    }: ModelJournals<'_>,
    owner: &AttemptContext,
    iteration: &ModelIteration,
    scope: ExecutionScope,
    clock: &super::model_liveness::support::ManualHomeClock,
    initial: &ExecutionRecord,
) {
    let observed = journal
        .load_model_observation(owner, iteration.invocation_id)
        .await
        .expect("observation");
    assert!(
        journal
            .finish_rejected_model_action(owner, rejection(observed.clone()))
            .await
            .is_err(),
        "rejection alone is not stop evidence"
    );
    let original = live
        .load(owner, scope)
        .await
        .expect("live")
        .expect("original");
    let stopping = live
        .mutate(
            owner,
            scope,
            original.revision(),
            RecordMutation::Event(Event::Stop(ControlStop::ExecutionFailed)),
        )
        .await
        .expect("original stop")
        .next;
    clock.set(super::model_liveness::support::clock(initial, 150));
    let closed = live
        .mutate(
            owner,
            scope,
            stopping.revision(),
            RecordMutation::Event(Event::CheckDue),
        )
        .await
        .expect("unknown stop")
        .next;
    assert_eq!(
        phase(closed.clone()),
        Phase::Closed(Closure::StopUnconfirmed(
            ExecutionStopReason::ExecutionFailed
        ))
    );
    journal
        .finish_rejected_model_action(owner, rejection(observed))
        .await
        .expect("logical correction");
    assert_eq!(
        live.load(owner, scope).await.expect("retained"),
        Some(closed)
    );
    let terminal: Option<i16> = sqlx::query_scalar(
        "SELECT terminal_stop_reason FROM runtime.model_invocations WHERE invocation_id=$1",
    )
    .bind(iteration.invocation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("no provider terminal");
    assert_eq!(terminal, None);
}

#[tokio::test]
async fn rejected_model_action_closes_original_physical_terminal_atomically() {
    Box::pin(super::model_liveness::semantic_rejection_terminal()).await;
}
#[tokio::test]
async fn rejected_model_action_preserves_original_unknown_stop() {
    Box::pin(super::model_liveness::semantic_rejection_unknown()).await;
}

#[tokio::test]
async fn durable_stop_wins_before_semantic_rejection_commit() {
    Box::pin(super::model_liveness::semantic_rejection_cancelled()).await;
}
