#![cfg(test)]

use musteragent_cloud_worker::PersistedContextCompactor;
use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::Sha256;
use musteragent_context::{ContextCompactionRequest, ContextCompactionSlotV1, ContextCompactor};
use musteragent_execution_liveness::{
    Closure, ExecutionRecord, FencedExecutionLivenessJournal, Phase, StopReason,
};
use musteragent_execution_liveness_io::{HomeExecutionActivityFactory, SystemLivenessHomeClock};
use musteragent_identity::{
    ConversationSeq, CorrelationId, InvocationId, LedgerRecordId, UtcTimestamp, VersionedKey,
};
use musteragent_model::AuxiliaryModelRole;
use musteragent_postgres::{
    PostgresContentStore, PostgresContextCompactionSlotStore,
    PostgresFencedExecutionLivenessJournal, migrate,
};
use musteragent_run::InvocationContext;
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::{
    ErrorCode,
    execution_liveness::{
        ActivityExecutionTiming, ActivitySource, ExecutionLivenessPolicy, ExecutionScope,
        InvocationExecutionTiming,
    },
};
use musteragent_runtime_snapshot::{PinnedAuxiliaryModelRoute, RuntimeSnapshot};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::{ProtobufMeteringPublicationEncoder, ProtobufSnapshotCodec};
use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use musteragent_conformance::fixtures::liveness_admission as admission;
#[path = "support/auxiliary_provider_wire.rs"]
mod auxiliary_wire;
#[path = "support/continued_compaction_model.rs"]
mod continued;
#[path = "support/owned_progress_model.rs"]
mod owned;
#[path = "support/running_loop.rs"]
mod running_loop;
#[allow(dead_code)]
#[path = "support/tool_material.rs"]
mod tool_material;
struct FixedClock(UtcTimestamp);
impl RuntimeClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

#[tokio::test]
async fn compression_progress_outlives_idle_and_native_end_does_not_wait_for_eof() {
    scenario(false, false, false, false).await;
}
#[tokio::test]
async fn silent_compression_returns_timeout_retains_unknown_stop_and_never_replays() {
    scenario(true, false, false, false).await;
}

#[tokio::test]
async fn compression_preflight_stall_times_out_before_dispatch_and_terminal_is_reused() {
    scenario(true, true, false, false).await;
}

#[tokio::test]
async fn truncated_compression_is_durable_and_continues_in_a_distinct_paid_invocation() {
    scenario(false, false, true, false).await;
}

#[tokio::test]
async fn compression_reopens_original_failed_provider_and_successful_role_standby_without_repaying()
{
    scenario(false, false, false, true).await;
}

async fn scenario(stall: bool, preflight_stall: bool, continue_output: bool, with_alternate: bool) {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, owner) = running_loop::seed_running_loop(&pool).await;
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let context = running_loop::run_context(fixture);
    let (mut windows, progress_interval) = admission::database_activity_timing();
    if with_alternate {
        windows.idle *= 5;
    }
    let policy = Arc::new(
        ExecutionLivenessPolicy::new(
            VersionedKey::new("test.compaction-tool@1").unwrap(),
            windows,
            [ActivitySource::ExecutorWork],
        )
        .unwrap(),
    );
    let (mut snapshot, _, _) = admission::fixture(content.clone(), context, &owner, policy).await;
    let mut model = musteragent_runtime_snapshot::PinnedModelRoute {
        route: snapshot.content.model.route.clone(),
        credential_grant_ref: snapshot.content.model.credential_grant_ref.clone(),
    };
    model.route.route_ref = VersionedKey::new("test.compaction-model@1").unwrap();
    let alternate = if with_alternate {
        let [first, second] =
            auxiliary_wire::routes(model.clone(), AuxiliaryModelRole::Compression);
        model = first;
        Some(second)
    } else {
        None
    };
    let wire = if let Some(second) = &alternate {
        Some(
            auxiliary_wire::Providers::start(
                AuxiliaryModelRole::Compression,
                [model.clone(), second.clone()],
                content.clone(),
            )
            .await,
        )
    } else {
        None
    };
    snapshot
        .content
        .auxiliary_models
        .retain(|v| v.role != AuxiliaryModelRole::Compression);
    snapshot
        .content
        .auxiliary_models
        .push(PinnedAuxiliaryModelRoute {
            role: AuxiliaryModelRole::Compression,
            model: model.clone(),
        });
    if let Some(second) = &alternate {
        snapshot
            .content
            .auxiliary_models
            .push(PinnedAuxiliaryModelRoute {
                role: AuxiliaryModelRole::Compression,
                model: second.clone(),
            });
    }
    snapshot.content.auxiliary_models.sort_by_key(|v| v.role);
    let snapshot = RuntimeSnapshot::seal(
        snapshot.runtime_snapshot_id,
        snapshot.content,
        running_loop::database_now(&pool).await,
        &ProtobufSnapshotCodec,
    )
    .expect("seal");
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$1,content_hash=$2,encoded_snapshot=$3 WHERE runtime_snapshot_id=$4")
        .bind(snapshot.content.schema_version.as_str()).bind(snapshot.content_hash.into_bytes().to_vec()).bind(&snapshot.encoded_content).bind(fixture.runtime_snapshot_id.into_uuid()).execute(&pool).await.unwrap();
    let clock = Arc::new(SystemLivenessHomeClock::default());
    let journal = Arc::new(PostgresFencedExecutionLivenessJournal::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        clock.clone(),
    ));
    let sends = Arc::new(AtomicUsize::new(0));
    let store = PostgresContextCompactionSlotStore::new(pool.clone(), keys)
        .with_execution_liveness(journal.clone());
    let now = running_loop::database_now(&pool).await;
    let compose = || {
        let provider = owned::ScriptedProvider {
            stall,
            preflight_stall,
            sends: sends.clone(),
            progress_interval,
        };
        let provider: Arc<dyn musteragent_runtime_model::ModelProvider> = if let Some(wire) = &wire
        {
            wire.adapter.clone()
        } else if continue_output {
            Arc::new(continued::ContinuedProvider(provider))
        } else {
            Arc::new(provider)
        };
        PersistedContextCompactor::new(
            provider,
            store.clone(),
            content.clone(),
            Arc::new(ProtobufMeteringPublicationEncoder),
            Arc::new(FixedClock(now)),
        )
        .with_execution_supervision(musteragent_loop::ExecutionSupervision::new(
            journal.clone(),
            clock.clone(),
            Arc::new(HomeExecutionActivityFactory),
        ))
    };
    let runner = compose();
    let mut request = ContextCompactionRequest {
        capture_provider_failure: with_alternate,
        continuation: None,
        invocation: InvocationContext {
            invocation_id: InvocationId::generate(),
            run_context: context,
            attempt_context: owner.clone(),
            parent_invocation_id: None,
            causation_id: LedgerRecordId::generate(),
            correlation_id: CorrelationId::generate(),
            timing: InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
                policy: snapshot
                    .content
                    .execution_policy
                    .liveness
                    .unwrap()
                    .model_policy()
                    .reference()
                    .clone(),
                business_deadline: None,
            }),
        },
        slot: ContextCompactionSlotV1 {
            source: Default::default(),
            loop_iteration: 1,
            generation: NonZeroU32::MIN,
            active_branch_id: None,
            ledger_cutoff: ConversationSeq::new(10).unwrap(),
            source_seq_from: ConversationSeq::new(1).unwrap(),
            source_seq_to: ConversationSeq::new(5).unwrap(),
            retained_record_ids: vec![LedgerRecordId::generate()],
            previous_summary_ref: None,
            input_budget: 1000,
            input_tokens_before: 900,
            summary_budget: 100,
            compaction_policy_ref: VersionedKey::new(if continue_output {
                musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V11
            } else {
                musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V7
            })
            .unwrap(),
            plan_fingerprint: Sha256::digest(b"unsealed"),
        },
        model,
        stream_budget: snapshot.content.execution_policy.model_stream,
        // The summarizer consumes the semantic transcript document (718cf19f36).
        serialized_history: serde_json::to_vec(&serde_json::json!({
            "schema_version": "musteragent.context.compaction-input.v4",
            "messages": [{"role": "user", "text": ["Preserve the user task and unfinished work"]}],
        }))
        .unwrap(),
        previous_summary: None,
        summarized_record_ids: vec![LedgerRecordId::generate()],
        max_output_tokens: NonZeroU64::new(100).unwrap(),
        recovery_policy: snapshot.content.execution_policy.recovery_policy,
    };
    request.slot.plan_fingerprint = request.expected_plan_fingerprint().unwrap();
    if let Some(second) = alternate {
        let (first, retained) = runner
            .prepare_role_candidate(request.clone(), None)
            .await
            .unwrap();
        assert!(retained.is_none());
        let error = runner.compact_step(first.clone()).await.unwrap_err();
        assert!(error.permits_role_fallback(), "{error:?}");
        let mut standby = request.clone();
        standby.model = second;
        standby.slot.plan_fingerprint = standby.expected_plan_fingerprint().unwrap();
        let previous = Some(musteragent_context::CompactionCandidatePredecessor::from(
            &first,
        ));
        assert!(
            runner
                .prepare_role_candidate(standby.clone(), None)
                .await
                .is_err()
        );
        let (second, retained) = runner
            .prepare_role_candidate(standby.clone(), previous.clone())
            .await
            .unwrap();
        assert!(retained.is_none());
        assert_eq!(second.slot.generation.get(), 2);
        assert_ne!(
            second.invocation.invocation_id,
            first.invocation.invocation_id
        );
        assert_eq!(second.serialized_history, first.serialized_history);
        let completed = runner
            .compact_step(second.clone())
            .await
            .unwrap()
            .into_complete()
            .unwrap();
        let reopened = compose();
        let (original, retained) = reopened
            .prepare_role_candidate(request, None)
            .await
            .unwrap();
        assert_eq!(
            original.invocation.invocation_id,
            first.invocation.invocation_id
        );
        assert!(retained.unwrap().permits_role_fallback());
        let (restored, retained) = reopened
            .prepare_role_candidate(standby, previous)
            .await
            .unwrap();
        assert!(retained.is_none());
        assert_eq!(
            restored.invocation.invocation_id,
            second.invocation.invocation_id
        );
        assert_eq!(
            reopened
                .compact_step(restored)
                .await
                .unwrap()
                .into_complete()
                .unwrap(),
            completed
        );
        let body = wire.unwrap().assert_same_input().await;
        assert!(body["input"].to_string().contains("unfinished work"));
        let rows: Vec<(i16, Option<String>)> = sqlx::query_as("SELECT status,early_provider_cause FROM runtime.model_invocations WHERE invocation_id IN ($1,$2) ORDER BY status DESC")
            .bind(first.invocation.invocation_id.into_uuid()).bind(second.invocation.invocation_id.into_uuid())
            .fetch_all(&pool).await.unwrap();
        assert_eq!(
            rows,
            vec![(3, Some("provider_server".to_owned())), (2, None)]
        );
        return;
    }
    assert_request_transport(&request);
    let before = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        musteragent_testkit::integration_timeout(),
        runner.compact_step(request.clone()),
    )
    .await
    .expect("bounded call");
    let ExecutionRecord::Checkpoint(record) = journal
        .load(
            &owner,
            ExecutionScope::Invocation(request.invocation.invocation_id),
        )
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("checkpoint")
    };
    let original = store.latest_generation(&request).await.unwrap().unwrap();
    let mut replan = request.clone();
    replan.slot.input_tokens_before += 37;
    replan.slot.plan_fingerprint = replan.expected_plan_fingerprint().unwrap();
    let observed = store.latest_generation(&replan).await.unwrap().unwrap();
    assert_eq!(
        observed, original,
        "a fresh primary estimate keeps the exact sealed observation"
    );
    assert_eq!(
        observed.sealed_input_tokens_before,
        Some(request.slot.input_tokens_before)
    );
    assert_eq!(observed.generation_high_watermark, request.slot.generation);
    // Force the bounded metadata query as well as the ordinary exact-node path.
    // This is a read of a different plan, never authorization to dispatch it.
    replan
        .serialized_history
        .extend_from_slice(b"; a different exact source");
    replan.slot.plan_fingerprint = replan.expected_plan_fingerprint().unwrap();
    let unmatched = store.latest_generation(&replan).await.unwrap().unwrap();
    assert_eq!(unmatched, original);
    assert!(!replan.matches_sealed_plan(&unmatched).unwrap());
    if stall {
        assert_eq!(
            result.unwrap_err().code,
            ErrorCode::DependencyTimeout,
            "durable phase: {:?}",
            record.state().phase()
        );
        assert_eq!(
            record.state().phase(),
            if preflight_stall {
                Phase::Closed(Closure::StopConfirmed(StopReason::StartupExpired))
            } else {
                Phase::Closed(Closure::StopUnconfirmed(StopReason::IdleExpired))
            }
        );
        let observed = store.latest_generation(&request).await.unwrap().unwrap();
        assert_eq!(observed.timing, request.invocation.timing);
        if preflight_stall {
            assert!(matches!(
                observed.state,
                musteragent_context::CompactionGenerationState::Failed(_)
            ));
        } else {
            assert_eq!(
                observed.state,
                musteragent_context::CompactionGenerationState::DispatchedUnknown
            );
            assert!(
                runner
                    .prepare_invocation(request.clone())
                    .await
                    .unwrap_err()
                    .is_indeterminate()
            );
        }
        assert_eq!(
            runner.compact_step(request.clone()).await.unwrap_err().code,
            ErrorCode::DependencyTimeout
        );
    } else {
        let result = result.expect("checkpoint");
        assert!(before.elapsed() > windows.idle * 2);
        assert_eq!(
            record.state().phase(),
            Phase::Closed(Closure::TerminalObserved)
        );
        assert_eq!(runner.compact_step(request.clone()).await.unwrap(), result);
    }
    assert_eq!(
        sends.load(Ordering::SeqCst),
        if preflight_stall { 0 } else { 1 }
    );
    let row: (i16, i64, i64) = sqlx::query_as("SELECT status,(SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id=$1),(SELECT count(*) FROM security_audit.records WHERE source_model_invocation_id=$1) FROM runtime.model_invocations WHERE invocation_id=$1")
        .bind(request.invocation.invocation_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(row, if stall { (3, 0, 1) } else { (2, 1, 1) });
    if continue_output {
        use musteragent_context::{CompactionCheckpoint, CompactionContinuation};
        let reopened = compose();
        let partial = reopened.compact_step(request.clone()).await.unwrap();
        let CompactionCheckpoint::Incomplete(prefix) = partial.checkpoint else {
            panic!("partial, never authoritative")
        };
        assert_eq!(
            sends.load(Ordering::SeqCst),
            1,
            "reopened process reuses the retained output"
        );
        let mut next = request.clone();
        next.continuation = Some(CompactionContinuation {
            parent_invocation_id: partial.invocation_id,
            prefix: prefix.clone(),
        });
        next.slot.plan_fingerprint = next.expected_plan_fingerprint().unwrap();
        let next = reopened.prepare_invocation(next).await.unwrap();
        assert_request_transport(&next);
        assert_eq!(next.slot.generation.get(), 2);
        assert_ne!(
            next.invocation.invocation_id,
            request.invocation.invocation_id
        );
        let completed = reopened
            .compact_step(next.clone())
            .await
            .unwrap()
            .into_complete()
            .unwrap();
        assert!(completed.summary.as_str().contains(prefix.as_str()));
        assert!(
            completed
                .summary
                .as_str()
                .contains("continued checkpoint detail.")
        );
        assert_eq!(
            compose()
                .compact_step(next.clone())
                .await
                .unwrap()
                .into_complete()
                .unwrap(),
            completed
        );
        assert_eq!(sends.load(Ordering::SeqCst), 2);
        let usages: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id IN ($1,$2)",
        )
        .bind(request.invocation.invocation_id.into_uuid())
        .bind(next.invocation.invocation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            usages, 2,
            "each observed physical response is charged exactly once"
        );
    }
    let status: i16 = sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
        .bind(owner.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, 2, "call failure never terminates the Agent Run");
}

fn assert_request_transport(request: &ContextCompactionRequest) {
    use musteragent_loop::{AgentCompactionCommand, AgentContextCommand};
    use musteragent_wire::{decode_agent_context_command, encode_agent_context_command};
    let command = AgentContextCommand::CompactionModel(
        AgentCompactionCommand::LatestGenerationWithSealedEstimate(Box::new(request.clone())),
    );
    let maximum = std::num::NonZeroUsize::new(1024 * 1024).unwrap();
    let encoded = encode_agent_context_command(&command, maximum).unwrap();
    assert_eq!(
        decode_agent_context_command(&encoded, maximum).unwrap(),
        command
    );
}
