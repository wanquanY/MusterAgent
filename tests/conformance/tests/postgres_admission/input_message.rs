#[path = "../support/input_message_fixture.rs"]
mod fixture;

use musteragent_conversation::ConversationEvent;
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::{AttemptNumber, HomeEpoch, LedgerOffset, RunSeq};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_postgres::PostgresLedgerStore;
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{GetRun, RunApplication, RunEvent};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufLedgerPayloadCodec;

#[tokio::test]
async fn admission_records_exact_input_message_across_identical_sends_retries_and_system_runs() {
    let _suite_guard = super::POSTGRES_ADMISSION_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    let harness = fixture::InputMessageHarness::new(pool.clone()).await;

    let (conversation_id, user_participant) =
        harness.create_conversation("create-admission-k3").await;
    let (target_conversation_id, _) = harness
        .create_conversation("create-admission-k3-target")
        .await;

    let identical_text = "repeat this exact admission prompt";
    let (first_message_id, first_run_id) = harness
        .submit_user_message(
            conversation_id,
            user_participant,
            "submit-k3-1",
            identical_text,
        )
        .await;
    harness
        .claim_and_fail_run(first_run_id, "worker-k3-1")
        .await;

    let (second_message_id, second_run_id) = harness
        .submit_user_message(
            conversation_id,
            user_participant,
            "submit-k3-2",
            identical_text,
        )
        .await;
    assert_ne!(first_message_id, second_message_id);
    // Enqueue a mid-run steering message before failing Run 2 so we also verify
    // `RunApplication::get_run` never confuses mid-run controls with admission input.
    let steering_message_id = harness
        .enqueue_steering_control(
            conversation_id,
            user_participant,
            second_run_id,
            "steer-k3-2",
            identical_text,
        )
        .await;
    assert_ne!(steering_message_id, second_message_id);
    let second_attempt = harness.claim_run(second_run_id, "worker-k3-2").await;
    let child_run_id = harness
        .spawn_delegated_child(&second_attempt, "inspect delegated child objective")
        .await;
    harness
        .claim_and_fail_run(child_run_id, "worker-k3-child")
        .await;
    harness.fail_claimed_attempt(&second_attempt).await;

    let retry_run_id = harness
        .retry_failed_run(
            conversation_id,
            user_participant,
            second_run_id,
            second_message_id,
            1,
            "retry-k3-2",
        )
        .await;

    let coord_run_id = harness
        .admit_coordination_run(
            conversation_id,
            target_conversation_id,
            "system coordination message without conversation user message",
        )
        .await;

    // Verify `conversation.messages.run_id` for the retried message was never overwritten.
    let stored_second_msg_run: uuid::Uuid = sqlx::query_scalar(
        "SELECT run_id FROM conversation.messages WHERE tenant_id = $1 AND message_id = $2",
    )
    .bind(harness.actor.tenant_id.into_uuid())
    .bind(second_message_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read second message run_id");
    assert_eq!(stored_second_msg_run, second_run_id.into_uuid());

    // Inspect committed Ledger records on the primary conversation.
    let ledger = PostgresLedgerStore::new(pool.clone());
    let page = ledger
        .load(
            LedgerCursor {
                conversation_id,
                home_epoch: HomeEpoch::new(1).expect("epoch"),
                after_conversation_seq: LedgerOffset::ZERO,
            },
            500,
        )
        .await
        .expect("load primary conversation ledger");
    let mut accepted_messages = Vec::new();
    let mut admitted_runs = Vec::new();
    for record in &page.records {
        let payload = ProtobufLedgerPayloadCodec
            .decode(record.metadata.record_kind, &record.payload)
            .expect("decode ledger record");
        match payload {
            LedgerPayload::DomainEvent(DomainEvent::Conversation(
                ConversationEvent::MessageAccepted { message_id, .. },
            )) => {
                accepted_messages.push((
                    message_id,
                    record.metadata.run_id,
                    record.metadata.run_seq,
                ));
            }
            LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Admitted {
                parent_run_id,
                delegation_id,
                input_message_id,
                retry_source_message_id,
                retry_source_attempt_number,
                ..
            })) => {
                admitted_runs.push((
                    record.metadata.run_id.expect("RunAdmitted must set run_id"),
                    record.metadata.run_seq,
                    parent_run_id,
                    delegation_id,
                    input_message_id,
                    retry_source_message_id,
                    retry_source_attempt_number,
                ));
            }
            _ => {}
        }
    }

    // Ordinary `MessageAccepted` still has no run_id and no run_seq; mid-run steering carries run_id.
    assert_eq!(
        accepted_messages
            .iter()
            .find(|entry| entry.0 == first_message_id)
            .copied(),
        Some((first_message_id, None, None))
    );
    assert_eq!(
        accepted_messages
            .iter()
            .find(|entry| entry.0 == second_message_id)
            .copied(),
        Some((second_message_id, None, None))
    );

    let seq_one = Some(RunSeq::new(1).expect("run_seq 1"));
    assert_eq!(
        admitted_runs
            .iter()
            .find(|entry| entry.0 == first_run_id)
            .copied(),
        Some((
            first_run_id,
            seq_one,
            None,
            None,
            Some(first_message_id),
            None,
            None,
        ))
    );
    assert_eq!(
        admitted_runs
            .iter()
            .find(|entry| entry.0 == second_run_id)
            .copied(),
        Some((
            second_run_id,
            seq_one,
            None,
            None,
            Some(second_message_id),
            None,
            None,
        ))
    );
    assert_eq!(
        admitted_runs
            .iter()
            .find(|entry| entry.0 == retry_run_id)
            .copied(),
        Some((
            retry_run_id,
            seq_one,
            Some(second_run_id),
            None,
            Some(second_message_id),
            Some(second_message_id),
            Some(AttemptNumber::new(1).expect("attempt 1")),
        ))
    );
    let child_admitted = admitted_runs
        .iter()
        .find(|entry| entry.0 == child_run_id)
        .copied()
        .expect("delegated child RunAdmitted");
    assert_eq!(child_admitted.1, seq_one);
    assert_eq!(child_admitted.2, Some(second_run_id));
    assert!(child_admitted.3.is_some());
    assert_eq!(child_admitted.4, None);
    assert_eq!(child_admitted.5, None);

    // Inspect committed Ledger records on the coordination target conversation.
    let target_page = ledger
        .load(
            LedgerCursor {
                conversation_id: target_conversation_id,
                home_epoch: HomeEpoch::new(1).expect("epoch"),
                after_conversation_seq: LedgerOffset::ZERO,
            },
            100,
        )
        .await
        .expect("load target conversation ledger");
    let coord_admitted = target_page
        .records
        .iter()
        .find_map(|record| {
            match ProtobufLedgerPayloadCodec
                .decode(record.metadata.record_kind, &record.payload)
                .expect("decode target record")
            {
                LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Admitted {
                    input_message_id,
                    retry_source_message_id,
                    ..
                })) => Some((
                    record.metadata.run_id,
                    record.metadata.run_seq,
                    input_message_id,
                    retry_source_message_id,
                )),
                _ => None,
            }
        })
        .expect("coordination RunAdmitted");
    assert_eq!(
        coord_admitted,
        (Some(coord_run_id), seq_one, None, None),
        "system coordination admissions must stay at run_seq 1 with empty input_message_id"
    );

    // Verify host-facing `RunApplication::get_run` (`RunView.input_message_id`) for all 5 runs.
    for (run_id, expected_input) in [
        (first_run_id, Some(first_message_id)),
        (second_run_id, Some(second_message_id)),
        (retry_run_id, Some(second_message_id)),
        (child_run_id, None),
        (coord_run_id, None),
    ] {
        let view = RunApplication::get_run(
            &harness.application,
            GetRun {
                actor: harness.actor,
                run_id,
            },
        )
        .await
        .expect("get_run authorized read");
        assert_eq!(
            view.input_message_id, expected_input,
            "RunView.input_message_id mismatch"
        );
    }

    pool.close().await;
}
