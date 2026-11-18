//! Publication storage fixtures use the original fenced Tool/Artifact commit.
//! Blob transport and model execution are deliberately outside this test layer.
use super::*;
use std::sync::Arc;

use musteragent_activity::ActivityKind;
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_content_ref::SchemaRef;
use musteragent_event_envelope::{ExecutionScope, LedgerRecordKind, Producer, RuntimeHome};
use musteragent_ledger::{
    AppendRequest, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_postgres::{
    PostgresFencedArtifactToolCommitter, PostgresLedgerStore, PostgresToolInvocationStore,
};
use musteragent_run::{AttemptContext, RunContext, RunEvent};
use musteragent_runtime_artifact::{
    ArtifactMutation, CommitPreparedArtifactTool, FencedArtifactToolCommitter,
};
use musteragent_runtime_capability::{
    CapabilityResult, ToolInvocationStore, ToolPersistenceDisposition,
};
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract};
use sqlx::Row;

// Same opaque test encoding as the Artifact storage conformance suite. These
// assertions cover atomic storage facts, not the production Ledger wire codec.
struct TestLedgerPayloadEncoder;
impl LedgerPayloadEncoder for TestLedgerPayloadEncoder {
    fn encode(&self, payload: LedgerPayload) -> Vec<u8> {
        format!("{payload:?}").into_bytes()
    }
}

fn keys() -> Arc<OpaqueRefKeyring> {
    let id = DottedKey::new("runtime.dialogue-publication.test").unwrap();
    Arc::new(OpaqueRefKeyring::new(id.clone(), [(id, [41; 32])]).unwrap())
}

async fn now(f: &Fixture) -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(
        sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
    )
}

async fn context(f: &Fixture, fence: &AttemptFence) -> (RunContext, AttemptContext) {
    let row = sqlx::query(
        "SELECT r.conversation_id,r.activity_id,r.activity_kind,r.executor_participant_id,
        r.requested_by_participant_id,r.runtime_snapshot_id,a.lease_expires_at_ms
        FROM runtime.runs r JOIN runtime.attempts a ON a.run_id=r.run_id AND a.attempt_id=$2
        WHERE r.run_id=$1 AND r.tenant_id=$3",
    )
    .bind(fence.run_id.into_uuid())
    .bind(fence.attempt_id.into_uuid())
    .bind(f.owner.tenant_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    (
        RunContext {
            tenant_id: f.owner.tenant_id,
            conversation_id: ConversationId::from_uuid(row.get("conversation_id")).unwrap(),
            activity_id: ActivityId::from_uuid(row.get("activity_id")).unwrap(),
            activity_kind: match row.get::<i16, _>("activity_kind") {
                1 => ActivityKind::Chat,
                2 => ActivityKind::Coordination,
                other => panic!("unexpected fixture activity kind {other}"),
            },
            run_id: fence.run_id,
            executor_participant_id: ParticipantId::from_uuid(row.get("executor_participant_id"))
                .unwrap(),
            requested_by_participant_id: ParticipantId::from_uuid(
                row.get("requested_by_participant_id"),
            )
            .unwrap(),
            parent_run_id: None,
            delegation_id: None,
            execution_scope: ExecutionScope::Tenant {
                tenant_id: f.owner.tenant_id,
            },
            control_home: RuntimeHome::Cloud,
            execution_home: RuntimeHome::Cloud,
            home_epoch: fence.home_epoch,
            runtime_snapshot_id: RuntimeSnapshotId::from_uuid(row.get("runtime_snapshot_id"))
                .unwrap(),
        },
        AttemptContext {
            attempt_id: fence.attempt_id,
            run_id: fence.run_id,
            attempt_number: fence.attempt_number,
            worker_id: fence.worker_id.clone(),
            lease_id: fence.lease_id,
            fencing_token: fence.fencing_token,
            lease_expires_at: UtcTimestamp::from_unix_millis(row.get("lease_expires_at_ms")),
        },
    )
}

pub(super) async fn append_admission(f: &Fixture, fence: &AttemptFence) {
    let (run, _) = context(f, fence).await;
    let offset: i64 = sqlx::query_scalar("SELECT last_sequence FROM event_store.ledger_streams WHERE tenant_id=$1 AND conversation_id=$2")
        .bind(run.tenant_id.into_uuid()).bind(run.conversation_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    let payload = TestLedgerPayloadEncoder.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
        RunEvent::Admitted {
            runtime_snapshot_id: run.runtime_snapshot_id,
            execution_home: run.execution_home,
            parent_run_id: None,
            delegation_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: None,
        },
    )));
    let receipt = PostgresLedgerStore::new(f.pool.clone())
        .append(AppendRequest {
            tenant_id: run.tenant_id,
            conversation_id: run.conversation_id,
            expected_home_epoch: run.home_epoch,
            expected_offset: LedgerOffset::new(offset.try_into().unwrap()),
            idempotency_key: IdempotencyKey::new(format!("publication-admission:{}", run.run_id))
                .unwrap(),
            command_fingerprint: Sha256::digest(&payload),
            records: vec![ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").unwrap(),
                record_id: LedgerRecordId::generate(),
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(run.activity_id),
                run_id: Some(run.run_id),
                run_seq: Some(RunSeq::new(1).unwrap()),
                attempt_id: None,
                participant_id: Some(run.requested_by_participant_id),
                producer: Producer::RuntimeApi,
                occurred_at: now(f).await,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&payload),
                payload,
            }],
            outbox: vec![],
        })
        .await
        .unwrap();
    let admitted: i64 =
        sqlx::query_scalar("SELECT admitted_sequence FROM runtime.runs WHERE run_id=$1")
            .bind(run.run_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(
        receipt.first_sequence.get(),
        u64::try_from(admitted).unwrap()
    );
}

pub(super) async fn current_command(f: &Fixture) -> PublicEntryAppend {
    let sequence: i64 =
        sqlx::query_scalar("SELECT last_run_sequence FROM runtime.runs WHERE run_id=$1")
            .bind(f.command.fence.run_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    let mut command = f.command.clone();
    command.delivery_id = CommandId::generate();
    command.expected_last_run_sequence = RunSeq::new(sequence.try_into().unwrap()).unwrap();
    command
}

pub(super) async fn requested_invocation(f: &Fixture, fence: &AttemptFence) -> InvocationId {
    let (run, _) = context(f, fence).await;
    let invocation = InvocationId::generate();
    let request = content("artifact tool request");
    register_content(&f.pool, f.owner, &request).await;
    let result = PostgresToolInvocationStore::new(f.pool.clone(), keys())
        .begin(
            run.tenant_id,
            run.conversation_id,
            ToolInvocation {
                invocation_id: invocation,
                run_id: run.run_id,
                requested_by_attempt_id: fence.attempt_id,
                execution_attempt_id: None,
                tool_call_id: ToolCallId::new(format!("call-{invocation}")).unwrap(),
                tool_key: ToolKey::from_versioned_key(
                    VersionedKey::new("artifact.note.create@1").unwrap(),
                ),
                capability_provider_id: CapabilityProviderId::generate(),
                status: ToolInvocationStatus::Requested,
                effect: Effect::WriteReversible,
                approval: ApprovalRequirement::Never,
                idempotency_key: IdempotencyKey::new(invocation.to_string()).unwrap(),
                request_ref: request,
                result_ref: None,
                interaction_id: None,
                // Preserve the existing fixture's deadline-based contract. This is not
                // an ActivityV1/model-loop liveness qualification.
                timing: InvocationExecutionTiming::LegacyFixed {
                    deadline: UtcTimestamp::from_unix_millis(now(f).await.unix_millis() + 60_000),
                },
            },
            ToolRetryContract::IdempotentByKey,
        )
        .await
        .unwrap();
    assert_eq!(result, ToolPersistenceDisposition::Applied);
    invocation
}

pub(super) async fn start_invocation(f: &Fixture, fence: &AttemptFence, invocation: InvocationId) {
    PostgresToolInvocationStore::new(f.pool.clone(), keys())
        .start(
            f.owner.tenant_id,
            invocation,
            fence.attempt_id,
            now(f).await,
        )
        .await
        .unwrap();
}

pub(super) async fn committed_artifact(
    f: &Fixture,
    fence: &AttemptFence,
) -> (ArtifactId, ArtifactVersionId, ContentRef) {
    let (run, attempt) = context(f, fence).await;
    let invocation = requested_invocation(f, fence).await;
    start_invocation(f, fence, invocation).await;
    let artifact_id = ArtifactId::generate();
    let version_id = ArtifactVersionId::generate();
    let bytes = content("actual artifact bytes");
    let result = content("actual artifact tool result");
    for reference in [&bytes, &result] {
        register_content(&f.pool, f.owner, reference).await;
    }
    let committed_at = now(f).await;
    PostgresFencedArtifactToolCommitter::new(
        f.pool.clone(),
        Arc::new(TestLedgerPayloadEncoder),
        keys(),
    )
    .commit(
        &attempt,
        CommitPreparedArtifactTool {
            run_context: run,
            invocation_id: invocation,
            result: CapabilityResult {
                result_hash: result.sha256,
                result_ref: result.clone(),
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new("artifact.note.output@1").unwrap(),
                    schema_hash: Sha256::digest("artifact-note-output-schema"),
                },
                provider_request_ref: None,
            },
            authorized_content: vec![],
            resource_placement: None,
            resource_replacement: None,
            committed_at,
            mutation: ArtifactMutation::Create {
                artifact: Artifact {
                    artifact_id,
                    tenant_id: run.tenant_id,
                    source: ArtifactSource::Conversation(run.conversation_id),
                    origin: ArtifactOrigin::Runtime {
                        activity_id: run.activity_id,
                        created_by_run_id: run.run_id,
                    },
                    kind: ArtifactKind::Note,
                    lifecycle: ArtifactLifecycle::Published,
                    current_version: VersionNumber::new(1).unwrap(),
                    title: "Report.md".into(),
                    created_at: committed_at,
                    updated_at: committed_at,
                },
                version: ArtifactVersion {
                    artifact_version_id: version_id,
                    artifact_id,
                    version: VersionNumber::new(1).unwrap(),
                    content_hash: bytes.sha256,
                    content_ref: bytes.clone(),
                    dependency_refs: vec![],
                    source_invocation_id: Some(invocation),
                    committed_by: ArtifactVersionAuthor::Participant(run.executor_participant_id),
                    committed_at,
                },
            },
        },
    )
    .await
    .unwrap();
    let success: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM artifact.artifacts a
        JOIN artifact.artifact_versions v USING(artifact_id)
        JOIN tool.tool_invocations i ON i.invocation_id=v.source_invocation_id
        JOIN tool.tool_results r ON r.invocation_id=i.invocation_id AND r.result_content_id=i.result_content_id
        JOIN artifact.content_refs c ON c.content_id=r.result_content_id AND c.tenant_id=i.tenant_id AND c.content_hash=r.result_hash AND c.state=2
        WHERE a.artifact_id=$1 AND v.artifact_version_id=$2 AND a.origin=1
        AND i.invocation_id=$3 AND i.run_id=$4 AND i.status=4 AND i.execution_attempt_id=$5
        AND a.tenant_id=$6 AND i.tenant_id=$6 AND i.conversation_id=$7
        AND r.result_content_id=$8 AND r.result_hash=$9)",
    )
    .bind(artifact_id.into_uuid())
    .bind(version_id.into_uuid())
    .bind(invocation.into_uuid())
    .bind(run.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .bind(run.tenant_id.into_uuid())
    .bind(run.conversation_id.into_uuid())
    .bind(result.content_id.into_uuid())
    .bind(result.sha256.into_bytes().to_vec())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert!(
        success,
        "original Artifact owner must commit a Runtime version and successful Tool receipt together"
    );
    (artifact_id, version_id, bytes)
}

// Malformed storage is intentional ONLY for refusal tests, never for success.
pub(super) async fn uncommitted_artifact(
    f: &Fixture,
    invocation: Option<InvocationId>,
) -> (ArtifactId, ArtifactVersionId) {
    let artifact = ArtifactId::generate();
    let version = ArtifactVersionId::generate();
    let bytes = content("uncommitted artifact");
    register_content(&f.pool, f.owner, &bytes).await;
    sqlx::query("INSERT INTO artifact.artifacts
        (artifact_id,tenant_id,conversation_id,activity_id,created_by_run_id,kind,lifecycle,current_version,title,created_at_ms,updated_at_ms)
        SELECT $1,tenant_id,conversation_id,activity_id,run_id,1,2,1,'Uncommitted.md',1,1 FROM runtime.runs WHERE run_id=$2")
        .bind(artifact.into_uuid()).bind(f.command.fence.run_id.into_uuid()).execute(&f.pool).await.unwrap();
    sqlx::query("INSERT INTO artifact.artifact_versions
        (artifact_version_id,artifact_id,version,content_id,content_hash,committed_by,source_invocation_id,committed_at_ms)
        SELECT $1,$2,1,$3,$4,executor_participant_id,$5,1 FROM runtime.runs WHERE run_id=$6")
        .bind(version.into_uuid()).bind(artifact.into_uuid()).bind(bytes.content_id.into_uuid())
        .bind(bytes.sha256.into_bytes().to_vec()).bind(invocation.map(InvocationId::into_uuid))
        .bind(f.command.fence.run_id.into_uuid()).execute(&f.pool).await.unwrap();
    (artifact, version)
}

pub(super) async fn another_run(f: &Fixture) -> AttemptFence {
    // A Conversation has one active foreground Chat Run. Independent
    // Coordination work may coexist; keep that real storage invariant and
    // use its exact stored kind in the fenced commit's RunContext.
    let fence = AttemptFence {
        run_id: RunId::generate(),
        attempt_id: AttemptId::generate(),
        lease_id: LeaseId::generate(),
        ..f.command.fence.clone()
    };
    let current = now(f).await.unix_millis();
    let mut tx = f.pool.begin().await.unwrap();
    sqlx::query("INSERT INTO runtime.runs
        (run_id,tenant_id,conversation_id,activity_id,activity_kind,executor_participant_id,requested_by_participant_id,
        execution_scope_kind,control_home_kind,execution_home_kind,home_epoch,runtime_snapshot_id,admitted_sequence,
        status,next_attempt_number,admitted_at_ms,started_at_ms,traceparent)
        SELECT $1,r.tenant_id,r.conversation_id,$2,2,r.executor_participant_id,r.requested_by_participant_id,
        r.execution_scope_kind,r.control_home_kind,r.execution_home_kind,r.home_epoch,r.runtime_snapshot_id,s.last_sequence+1,
        2,2,$3,$3,r.traceparent FROM runtime.runs r JOIN event_store.ledger_streams s
        ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id WHERE r.run_id=$4")
        .bind(fence.run_id.into_uuid()).bind(ActivityId::generate().into_uuid()).bind(current)
        .bind(f.command.fence.run_id.into_uuid()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO runtime.attempts
        (attempt_id,tenant_id,run_id,attempt_number,trigger,worker_id,lease_id,fencing_token,lease_expires_at_ms,status,claimed_at_ms)
        VALUES($1,$2,$3,1,1,$4,$5,1,$6,1,$7)")
        .bind(fence.attempt_id.into_uuid()).bind(f.owner.tenant_id.into_uuid()).bind(fence.run_id.into_uuid())
        .bind(fence.worker_id.as_str()).bind(fence.lease_id.into_uuid()).bind(current+60_000).bind(current).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO runtime.leases (run_id,tenant_id,attempt_id,worker_id,lease_id,fencing_token,expires_at_ms,renewed_at_ms)
        VALUES($1,$2,$3,$4,$5,1,$6,$7)")
        .bind(fence.run_id.into_uuid()).bind(f.owner.tenant_id.into_uuid()).bind(fence.attempt_id.into_uuid())
        .bind(fence.worker_id.as_str()).bind(fence.lease_id.into_uuid()).bind(current+60_000).bind(current).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    append_admission(f, &fence).await;
    fence
}

#[path = "durable_execution/mod.rs"]
mod durable_execution;
