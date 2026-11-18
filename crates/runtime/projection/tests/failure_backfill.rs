use std::sync::Mutex;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerMetadata, LedgerRecordKind, Producer};
use musteragent_identity::*;
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_projection::*;
use musteragent_run::{RunEvent, RunStatus};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, ErrorInfo, SafeDisplayText};

struct Source {
    records: Vec<RetainedProjectionRunEvent>,
    calls: Mutex<Vec<ProjectionRunFailureQuery>>,
    ignore_scope: bool,
}

impl ProjectionRunFailureSource for Source {
    fn load_run_events(
        &self,
        query: ProjectionRunFailureQuery,
    ) -> ApplicationFuture<'_, Result<Vec<RetainedProjectionRunEvent>, SubscriptionError>> {
        self.calls.lock().expect("record page query").push(query);
        Box::pin(async move {
            let mut records: Vec<_> = self
                .records
                .iter()
                .filter(|record| {
                    self.ignore_scope
                        || (record.metadata.run_id == Some(query.run_id)
                            && query.before_run_sequence.is_none_or(|before| {
                                record
                                    .metadata
                                    .run_seq
                                    .is_some_and(|sequence| sequence < before)
                            }))
                })
                .cloned()
                .collect();
            records.sort_by_key(|record| std::cmp::Reverse(record.metadata.run_seq));
            records.truncate(PROJECTION_FAILURE_BACKFILL_PAGE_SIZE);
            Ok(records)
        })
    }
}

struct Decoder;
impl LedgerPayloadDecoder for Decoder {
    fn decode(
        &self,
        kind: LedgerRecordKind,
        payload: &[u8],
    ) -> Result<LedgerPayload, SubscriptionError> {
        if kind != LedgerRecordKind::DomainEvent {
            return Err(SubscriptionError::CorruptRecord);
        }
        let event = match payload {
            b"quota" => RunEvent::Failed {
                error: error(ErrorCode::QuotaExceeded),
                last_attempt_id: None,
            },
            b"timeout" => RunEvent::Failed {
                error: error(ErrorCode::DependencyTimeout),
                last_attempt_id: None,
            },
            b"completed" => RunEvent::Completed {
                assistant_message_id: None,
                result_refs: vec![],
            },
            b"invalid-error" => {
                let mut error = error(ErrorCode::QuotaExceeded);
                error.category = ErrorCode::DependencyTimeout.category();
                RunEvent::Failed {
                    error,
                    last_attempt_id: None,
                }
            }
            b"progress" => RunEvent::AttemptSucceeded {
                attempt_number: AttemptNumber::new(1).expect("attempt"),
            },
            _ => return Err(SubscriptionError::CorruptRecord),
        };
        Ok(LedgerPayload::DomainEvent(DomainEvent::Run(event)))
    }
}

fn error(code: ErrorCode) -> ErrorInfo {
    ErrorInfo {
        code,
        category: code.category(),
        safe_message: SafeDisplayText::new("Original runtime failure").expect("safe failure"),
        retryable: false,
        dependency_ref: None,
        diagnostic_ref: None,
    }
}

fn snapshot() -> ConversationProjectionSnapshot {
    ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        snapshot_id: ProjectionSnapshotId::generate(),
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        home_epoch: HomeEpoch::new(1).expect("epoch"),
        through_conversation_seq: LedgerOffset::new(1_000),
        messages: vec![],
        tool_timeline: vec![],
        context_compactions: vec![],
        pending_interactions: vec![],
        activities: vec![],
        runs: vec![run(), run(), run()],
        active_runs: vec![],
        artifacts: vec![],
        stream_outputs: vec![],
        content_manifest: vec![],
        checksum: Sha256::digest(b"legacy"),
        created_at: UtcTimestamp::from_unix_millis(1_000),
    }
}

fn run() -> ProjectedRun {
    ProjectedRun {
        run_id: RunId::generate(),
        activity_id: ActivityId::generate(),
        executor_participant_id: None,
        parent_run_id: None,
        delegation_id: None,
        retry_source_message_id: None,
        retry_source_attempt_number: None,
        terminal_failure: None,
        retry_failure: None,
        status: RunStatus::Failed,
        wait_reason: None,
        attempt_number: None,
        next_attempt_number: None,
        retry_not_before: None,
        admitted_at: UtcTimestamp::from_unix_millis(1),
        started_at: None,
        finished_at: Some(UtcTimestamp::from_unix_millis(999)),
    }
}

fn record(
    snapshot: &ConversationProjectionSnapshot,
    run_index: usize,
    sequence: u64,
    payload: &[u8],
) -> RetainedProjectionRunEvent {
    let run = &snapshot.runs[run_index];
    RetainedProjectionRunEvent {
        metadata: LedgerMetadata {
            schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("record schema"),
            record_id: LedgerRecordId::generate(),
            record_kind: LedgerRecordKind::DomainEvent,
            tenant_id: snapshot.tenant_id,
            conversation_id: snapshot.conversation_id,
            conversation_seq: ConversationSeq::new(sequence).expect("conversation sequence"),
            activity_id: Some(run.activity_id),
            run_id: Some(run.run_id),
            run_seq: Some(RunSeq::new(sequence).expect("Run sequence")),
            attempt_id: None,
            participant_id: None,
            home_epoch: snapshot.home_epoch,
            producer: Producer::RuntimeApi,
            occurred_at: UtcTimestamp::from_unix_millis(100),
            committed_at: UtcTimestamp::from_unix_millis(101),
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            idempotency_key: IdempotencyKey::new(format!("fact-{sequence}")).expect("idempotency"),
            payload_hash: Sha256::digest(payload),
        },
        context_branch_id: None,
        payload: Some(payload.to_vec()),
    }
}

fn source(records: Vec<RetainedProjectionRunEvent>) -> Source {
    Source {
        records,
        calls: Mutex::new(vec![]),
        ignore_scope: false,
    }
}

#[tokio::test]
async fn restores_the_latest_exact_run_failure_and_preserves_other_run_states() {
    let mut snapshot = snapshot();
    snapshot.runs[2].status = RunStatus::Completed;
    let older = record(&snapshot, 0, 10, b"timeout");
    let parent = record(&snapshot, 0, 20, b"quota");
    let other = record(&snapshot, 1, 30, b"timeout");
    let source = source(vec![older, parent.clone(), other.clone()]);
    backfill_projection_run_failures(&mut snapshot, &source, &Decoder, |_, _| true)
        .await
        .expect("backfill retained facts");
    let parent_failure = snapshot.runs[0]
        .terminal_failure
        .as_ref()
        .expect("parent failure");
    assert_eq!(parent_failure.error.code, ErrorCode::QuotaExceeded);
    assert_eq!(parent_failure.ledger_record_id, parent.metadata.record_id);
    assert_eq!(
        parent_failure.correlation_id,
        parent.metadata.correlation_id
    );
    assert_eq!(
        parent_failure.conversation_sequence,
        parent.metadata.conversation_seq
    );
    assert_eq!(
        snapshot.runs[1]
            .terminal_failure
            .as_ref()
            .expect("other failure")
            .ledger_record_id,
        other.metadata.record_id
    );
    assert_eq!(snapshot.runs[2].terminal_failure, None);
    assert_eq!(source.calls.lock().expect("queries").len(), 2);
    backfill_projection_run_failures(&mut snapshot, &source, &Decoder, |_, _| true)
        .await
        .expect("preserve populated errors");
    assert_eq!(source.calls.lock().expect("no extra queries").len(), 2);
}

#[tokio::test]
async fn bounded_pages_advance_across_invisible_unavailable_branch_facts() {
    let mut snapshot = snapshot();
    snapshot.runs.truncate(1);
    let invisible = ContextBranchId::generate();
    let mut records: Vec<_> = (2..=66)
        .map(|sequence| {
            let mut record = record(&snapshot, 0, sequence, b"progress");
            record.context_branch_id = Some(invisible);
            record.payload = None;
            record
        })
        .collect();
    records.push(record(&snapshot, 0, 1, b"quota"));
    let source = source(records);
    backfill_projection_run_failures(&mut snapshot, &source, &Decoder, |branch, _| {
        branch != Some(invisible)
    })
    .await
    .expect("page past invisible facts");
    assert_eq!(
        snapshot.runs[0]
            .terminal_failure
            .as_ref()
            .expect("visible failure")
            .conversation_sequence
            .get(),
        1
    );
    let calls = source.calls.lock().expect("page queries");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].before_run_sequence.expect("next cursor").get(), 3);
}

#[tokio::test]
async fn unavailable_authority_stays_unknown_without_guessing_from_older_failures() {
    let mut snapshot = snapshot();
    snapshot.runs.truncate(1);
    let older = record(&snapshot, 0, 1, b"quota");
    let mut unavailable = record(&snapshot, 0, 2, b"timeout");
    unavailable.payload = None;
    backfill_projection_run_failures(
        &mut snapshot,
        &source(vec![older, unavailable]),
        &Decoder,
        |_, _| true,
    )
    .await
    .expect("respect unavailable bytes");
    assert_eq!(snapshot.runs[0].terminal_failure, None);
    backfill_projection_run_failures(&mut snapshot, &source(vec![]), &Decoder, |_, _| true)
        .await
        .expect("respect absent records");
    assert_eq!(snapshot.runs[0].terminal_failure, None);
}

#[tokio::test]
async fn rejects_mismatched_identity_hash_payload_and_terminal_state() {
    for corruption in 0..11 {
        let mut snapshot = snapshot();
        snapshot.runs.truncate(1);
        let mut fact = record(&snapshot, 0, 1, b"quota");
        match corruption {
            0 => fact.metadata.tenant_id = TenantId::generate(),
            1 => fact.metadata.conversation_id = ConversationId::generate(),
            2 => fact.metadata.run_id = Some(RunId::generate()),
            3 => fact.metadata.activity_id = Some(ActivityId::generate()),
            4 => fact.metadata.home_epoch = HomeEpoch::new(2).expect("wrong epoch"),
            5 => {
                fact.metadata.conversation_seq =
                    ConversationSeq::new(1_001).expect("future sequence")
            }
            6 => fact.metadata.payload_hash = Sha256::digest(b"wrong hash"),
            7 => {
                fact.payload = Some(b"invalid-error".to_vec());
                fact.metadata.payload_hash = Sha256::digest(b"invalid-error");
            }
            8 => {
                fact.payload = Some(b"completed".to_vec());
                fact.metadata.payload_hash = Sha256::digest(b"completed");
            }
            9 => fact.metadata.record_kind = LedgerRecordKind::OutputFrame,
            _ => {
                fact.payload = Some(b"invalid-wire".to_vec());
                fact.metadata.payload_hash = Sha256::digest(b"invalid-wire");
            }
        }
        let mut source = source(vec![fact]);
        source.ignore_scope = true;
        assert!(
            matches!(
                backfill_projection_run_failures(&mut snapshot, &source, &Decoder, |_, _| true)
                    .await,
                Err(SubscriptionError::CorruptRecord)
            ),
            "corruption {corruption}"
        );
        assert_eq!(snapshot.runs[0].terminal_failure, None);
    }
}
