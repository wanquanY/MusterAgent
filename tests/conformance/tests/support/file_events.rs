#![allow(
    dead_code,
    reason = "Integration test binaries use different subsets of this shared fixture."
)]

use musteragent_artifact::ArtifactEvent;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::RunId;
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_wire::ProtobufLedgerPayloadCodec;

pub async fn artifacts(pool: &sqlx::PgPool, run: RunId) -> Vec<ArtifactEvent> {
    let payloads: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT event.encoded_event FROM event_store.ledger_records AS record
         JOIN event_store.domain_events AS event USING (record_id)
         WHERE record.run_id = $1 ORDER BY record.run_seq",
    )
    .bind(run.into_uuid())
    .fetch_all(pool)
    .await
    .expect("read committed facts");
    payloads
        .iter()
        .filter_map(|bytes| {
            match ProtobufLedgerPayloadCodec
                .decode(LedgerRecordKind::DomainEvent, bytes)
                .expect("decode fact")
            {
                LedgerPayload::DomainEvent(DomainEvent::Artifact(event)) => Some(event),
                _ => None,
            }
        })
        .collect()
}

pub fn legacy_snapshot(
    fixture: super::running_loop::Fixture,
    invocation: musteragent_identity::InvocationId,
    reference: musteragent_content_ref::ContentRef,
    existing: Option<musteragent_identity::ArtifactId>,
) -> musteragent_projection::ConversationProjectionSnapshot {
    use musteragent_identity::*;
    use musteragent_projection::*;
    let at = UtcTimestamp::from_unix_millis(100);
    let mut snapshot = ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.into(),
        snapshot_id: ProjectionSnapshotId::generate(),
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        home_epoch: HomeEpoch::new(1).unwrap(),
        through_conversation_seq: LedgerOffset::ZERO,
        messages: vec![],
        tool_timeline: vec![ProjectedToolItem {
            invocation_id: invocation,
            message_id: None,
            run_id: fixture.run_id,
            tool_call_id: ToolCallId::new("legacy-resource").unwrap(),
            tool_ref: if existing.is_some() {
                "resource.file.edit.finalize@2"
            } else {
                "resource.change-set.commit@3"
            }
            .into(),
            status: ProjectedToolStatus::Completed,
            request_ref: reference.clone(),
            result_ref: None,
            requested_conversation_seq: ConversationSeq::new(1).unwrap(),
            requested_at: at,
            started_at: Some(at),
            finished_at: Some(at),
            output_artifact_ids: vec![],
        }],
        context_compactions: vec![],
        pending_interactions: vec![],
        activities: vec![],
        runs: vec![ProjectedRun {
            run_id: fixture.run_id,
            activity_id: fixture.activity_id,
            executor_participant_id: Some(fixture.executor_id),
            parent_run_id: None,
            delegation_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: None,
            terminal_failure: None,
            retry_failure: None,
            status: musteragent_run::RunStatus::Running,
            wait_reason: None,
            attempt_number: None,
            next_attempt_number: None,
            retry_not_before: None,
            admitted_at: at,
            started_at: Some(at),
            finished_at: None,
        }],
        active_runs: vec![],
        artifacts: vec![],
        stream_outputs: vec![],
        content_manifest: vec![],
        checksum: musteragent_content_ref::Sha256::digest([]),
        created_at: at,
    };
    if let Some(artifact_id) = existing {
        snapshot.artifacts.push(ProjectedArtifact {
            artifact_id,
            activity_id: Some(fixture.activity_id),
            created_by_run_id: Some(fixture.run_id),
            updated_by_run_id: Some(fixture.run_id),
            presentation_run_id: Some(fixture.run_id),
            kind: musteragent_artifact::ArtifactKind::File,
            lifecycle: musteragent_artifact::ArtifactLifecycle::Published,
            current_version: VersionNumber::new(1).unwrap(),
            title: "README.md".into(),
            current_content_ref: reference,
            current_dependency_refs: vec![],
            updated_at: at,
        });
    }
    snapshot
}
