use super::*;
use crate::wire;
use musteragent_identity::{CorrelationId, LedgerRecordId};
use musteragent_projection::ProjectedRunFailure;
use musteragent_runtime_contract::{ErrorCode, ErrorInfo, OpaqueDiagnosticRef, SafeDisplayText};

fn failed_snapshot() -> ConversationProjectionSnapshot {
    let mut value = snapshot(12);
    let activity_id = ActivityId::generate();
    value.activities.push(terminal_activity(
        activity_id,
        ActivityKind::Chat,
        ActivityStatus::Failed,
        1_000,
    ));
    let mut run = terminal_run(
        RunId::generate(),
        activity_id,
        None,
        None,
        RunStatus::Failed,
        1_000,
    );
    run.terminal_failure = Some(ProjectedRunFailure {
        error: ErrorInfo {
            code: ErrorCode::DependencyTimeout,
            category: ErrorCode::DependencyTimeout.category(),
            safe_message: SafeDisplayText::new("Provider recovery attempts exhausted")
                .expect("valid terminal failure fixture"),
            retryable: false,
            dependency_ref: Some(
                OpaqueDiagnosticRef::new("provider:test").expect("valid terminal failure fixture"),
            ),
            diagnostic_ref: Some(
                OpaqueDiagnosticRef::new("trace:recovery").expect("valid terminal failure fixture"),
            ),
        },
        ledger_record_id: LedgerRecordId::generate(),
        correlation_id: CorrelationId::generate(),
        conversation_sequence: ConversationSeq::new(20).expect("valid terminal failure fixture"),
    });
    value.runs.push(run);
    value.checksum = ProtobufConversationProjectionCodec.checksum(&value);
    value
}

fn reseal_wire(value: &mut wire::ConversationProjectionSnapshot) {
    value.checksum = None;
    value.checksum = Some(wire::Sha256 {
        value: Sha256::digest(value.encode_to_vec()).into_bytes().to_vec(),
    });
}

#[test]
fn snapshot_round_trip_preserves_authoritative_failure_and_original_answer() {
    let codec = ProtobufConversationProjectionCodec;
    let value = failed_snapshot();
    let encoded = codec
        .encode(&value)
        .expect("valid terminal failure fixture");
    assert_eq!(
        codec
            .decode(&encoded)
            .expect("valid terminal failure fixture"),
        value
    );
}

#[test]
fn snapshot_without_failure_details_remains_readable_without_inventing_a_reason() {
    let codec = ProtobufConversationProjectionCodec;
    let mut wire = conversation_projection_snapshot_message(&failed_snapshot());
    wire.runs[0].terminal_failure = None;
    reseal_wire(&mut wire);
    let decoded = codec
        .decode(&wire.encode_to_vec())
        .expect("valid terminal failure fixture");
    assert_eq!(decoded.runs[0].status, RunStatus::Failed);
    assert_eq!(decoded.runs[0].terminal_failure, None);
    assert_eq!(
        codec
            .decode(
                &codec
                    .encode(&decoded)
                    .expect("valid terminal failure fixture")
            )
            .expect("valid terminal failure fixture"),
        decoded
    );
}

#[test]
fn failure_details_require_valid_error_and_canonical_provenance() {
    let codec = ProtobufConversationProjectionCodec;
    for malformed in 0..6 {
        let mut wire = conversation_projection_snapshot_message(&failed_snapshot());
        let failure = wire.runs[0]
            .terminal_failure
            .as_mut()
            .expect("valid terminal failure fixture");
        match malformed {
            0 => failure.error = None,
            1 => {
                failure
                    .error
                    .as_mut()
                    .expect("valid terminal failure fixture")
                    .category = wire::ErrorCategory::Quota as i32
            }
            2 => failure.ledger_record_id = None,
            3 => failure.correlation_id = None,
            4 => failure.conversation_sequence = 0,
            5 => failure.conversation_sequence = 355_360,
            _ => unreachable!(),
        }
        reseal_wire(&mut wire);
        assert!(
            matches!(
                codec.decode(&wire.encode_to_vec()),
                Err(ProjectionSnapshotCodecError::InvalidContract)
            ),
            "case {malformed}"
        );
    }
}

#[test]
fn nonfailed_run_cannot_carry_terminal_failure() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = failed_snapshot();
    value.runs[0].status = RunStatus::Completed;
    value.activities[0].status = ActivityStatus::Completed;
    value.checksum = codec.checksum(&value);
    assert!(matches!(
        codec.encode(&value),
        Err(ProjectionSnapshotCodecError::InvalidContract)
    ));
}

#[test]
fn retry_cause_round_trips_without_turning_the_run_into_a_terminal_failure() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = failed_snapshot();
    let run = &mut value.runs[0];
    run.status = RunStatus::Waiting;
    run.wait_reason = Some(musteragent_run::RunWaitReason::RetryBackoff);
    run.finished_at = None;
    run.retry_failure = run.terminal_failure.take();
    let error = &mut run.retry_failure.as_mut().expect("retry cause").error;
    error.code = ErrorCode::CredentialRefreshRequired;
    error.category = error.code.category();
    error.retryable = true;
    value.active_runs = value.runs.clone();
    value.activities[0].status = ActivityStatus::Waiting;
    value.activities[0].finished_at = None;
    value.checksum = codec.checksum(&value);
    let decoded = codec
        .decode(&codec.encode(&value).expect("valid retry snapshot"))
        .expect("retry snapshot");
    assert_eq!(decoded, value);
    assert!(decoded.runs[0].terminal_failure.is_none());
    let mut legacy = conversation_projection_snapshot_message(&value);
    legacy.runs[0].retry_failure = None;
    legacy.active_runs[0].retry_failure = None;
    reseal_wire(&mut legacy);
    assert!(
        codec
            .decode(&legacy.encode_to_vec())
            .expect("legacy retry snapshot")
            .runs[0]
            .retry_failure
            .is_none()
    );
    value.runs[0].status = RunStatus::Running;
    value.runs[0].wait_reason = None;
    value.active_runs = value.runs.clone();
    value.checksum = codec.checksum(&value);
    assert!(
        codec.encode(&value).is_err(),
        "a stale retry cause cannot survive resumption"
    );
}
