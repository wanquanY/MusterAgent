#[path = "../support/input_message_fixture.rs"]
mod fixture;

use std::collections::BTreeSet;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::{HomeEpoch, LedgerOffset, MessageId, ProjectionSnapshotId, RunId};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_postgres::{PostgresLedgerError, PostgresLedgerStore};
use musteragent_projection::{
    ConversationProjectionApplication, ConversationProjectionSnapshotCodec,
    GetConversationProjection, GetConversationProjectionWindow,
    LEGACY_PROJECTION_SNAPSHOT_ROOT_KIND, LedgerPayloadDecoder, PROJECTION_SNAPSHOT_ROOT_KIND,
};
use musteragent_run::{GetRun, RunApplication, RunEvent};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::{ProtobufConversationProjectionCodec, ProtobufLedgerPayloadCodec};

#[tokio::test]
async fn projection_pairs_identical_sends_retries_system_delegated_and_replays_after_retraction() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    let harness = fixture::InputMessageHarness::new(pool.clone()).await;

    let (conversation_id, user_participant) =
        harness.create_conversation("create-projection-k3").await;
    let (target_conversation_id, _) = harness
        .create_conversation("create-projection-k3-target")
        .await;

    let identical_text = "identical user prompt across turns";
    let identical_sha = Sha256::digest(identical_text);
    let (msg_1, run_1) = harness
        .submit_user_message(
            conversation_id,
            user_participant,
            "proj-submit-1",
            identical_text,
        )
        .await;
    harness.claim_and_fail_run(run_1, "proj-worker-1").await;

    let (msg_2, run_2) = harness
        .submit_user_message(
            conversation_id,
            user_participant,
            "proj-submit-2",
            identical_text,
        )
        .await;
    assert_ne!(msg_1, msg_2);
    // Mid-run steering message with identical text must never overwrite `run_2.input_message_id`.
    let steer_msg = harness
        .enqueue_steering_control(
            conversation_id,
            user_participant,
            run_2,
            "proj-steer-2",
            identical_text,
        )
        .await;
    let run_2_attempt = harness.claim_run(run_2, "proj-worker-2").await;
    let child_run = harness
        .spawn_delegated_child(&run_2_attempt, "inspect child objective")
        .await;
    harness
        .claim_and_fail_run(child_run, "proj-worker-child")
        .await;
    harness.fail_claimed_attempt(&run_2_attempt).await;

    let retry_run = harness
        .retry_failed_run(
            conversation_id,
            user_participant,
            run_2,
            msg_2,
            1,
            "proj-retry-2",
        )
        .await;

    let coord_run = harness
        .admit_coordination_run(
            conversation_id,
            target_conversation_id,
            "system coordination message without conversation user message",
        )
        .await;

    let snapshot = harness.read_projection(conversation_id).await;
    let find_msg = |id: MessageId| {
        snapshot
            .messages
            .iter()
            .find(|message| message.message_id == id)
            .expect("projected message")
    };
    let find_run = |id: RunId| {
        snapshot
            .runs
            .iter()
            .find(|run| run.run_id == id)
            .expect("projected run")
    };

    assert_eq!(find_msg(msg_1).content_ref.sha256, identical_sha);
    assert_eq!(find_msg(msg_2).content_ref.sha256, identical_sha);
    assert_eq!(find_msg(msg_1).source_run_id, Some(run_1));
    assert_eq!(
        find_msg(msg_2).source_run_id,
        Some(run_2),
        "a retry must never overwrite the original user message's source_run_id"
    );
    assert_eq!(find_msg(steer_msg).source_run_id, Some(run_2));

    assert_eq!(find_run(run_1).input_message_id, Some(msg_1));
    assert_eq!(find_run(run_2).input_message_id, Some(msg_2));
    assert_eq!(find_run(retry_run).input_message_id, Some(msg_2));
    assert_eq!(find_run(retry_run).retry_source_message_id, Some(msg_2));
    assert_eq!(find_run(child_run).input_message_id, None);

    // Bounded window read selecting the latest messages (`msg_2` + `steer_msg`) must also include `retry_run`.
    let window = harness
        .projection
        .get_product_read_projection_window(GetConversationProjectionWindow {
            actor: harness.actor,
            conversation_id,
            limit: 2,
            before: None,
        })
        .await
        .expect("bounded projection window");
    let window_run_ids = window
        .projection
        .runs
        .iter()
        .map(|run| run.run_id)
        .collect::<BTreeSet<_>>();
    assert!(window_run_ids.contains(&run_2));
    assert!(
        window_run_ids.contains(&retry_run),
        "bounded projection window must retain retry runs paired via input_message_id"
    );

    let target_snapshot = harness.read_projection(target_conversation_id).await;
    assert!(target_snapshot.messages.is_empty());
    let projected_coord = target_snapshot
        .runs
        .iter()
        .find(|run| run.run_id == coord_run)
        .expect("projected coordination run");
    assert_eq!(projected_coord.input_message_id, None);

    // Verify `RunApplication::get_run` matches projection across every run kind.
    for (run_id, expected_input) in [
        (run_1, Some(msg_1)),
        (run_2, Some(msg_2)),
        (retry_run, Some(msg_2)),
        (child_run, None),
        (coord_run, None),
    ] {
        let view = RunApplication::get_run(
            &harness.application,
            GetRun {
                actor: harness.actor,
                run_id,
            },
        )
        .await
        .expect("get_run");
        assert_eq!(view.input_message_id, expected_input);
    }

    // Cancel the queued retry Run, retract `msg_2`, submit a new turn `msg_3`,
    // and verify both incremental tail replay and full cold-start ledger replay after retraction.
    harness
        .cancel_queued_run(conversation_id, retry_run, "proj-cancel-retry-2")
        .await;
    harness
        .retract_message(conversation_id, msg_2, "proj-retract-2")
        .await;

    let (msg_3, run_3) = harness
        .submit_user_message(
            conversation_id,
            user_participant,
            "proj-submit-3",
            identical_text,
        )
        .await;

    let after_retract = harness
        .projection
        .get_head_projection(GetConversationProjection {
            actor: harness.actor,
            conversation_id,
            preferred_snapshot_id: None,
        })
        .await
        .expect("head projection after retraction");
    let visible_messages = after_retract
        .messages
        .iter()
        .map(|message| (message.message_id, message.source_run_id))
        .collect::<Vec<_>>();
    assert_eq!(
        visible_messages,
        vec![(msg_1, Some(run_1)), (msg_3, Some(run_3))],
        "retracted turn and steering control must be absent while surviving and subsequent turns keep exact source_run_id"
    );
    let visible_runs = after_retract
        .runs
        .iter()
        .map(|run| (run.run_id, run.input_message_id))
        .collect::<Vec<_>>();
    assert_eq!(
        visible_runs,
        vec![(run_1, Some(msg_1)), (run_3, Some(msg_3))],
        "retracting msg_2 must also retract run_2, retry_run, and child_run"
    );

    // Delete cached snapshots to force a complete cold-start replay from Ledger after retraction.
    sqlx::query("DELETE FROM event_store.snapshots WHERE conversation_id = $1")
        .bind(conversation_id.into_uuid())
        .execute(&pool)
        .await
        .expect("clear cached projection snapshots");
    let cold_replayed = harness.read_projection(conversation_id).await;
    assert_eq!(
        cold_replayed
            .messages
            .iter()
            .map(|message| (message.message_id, message.source_run_id))
            .collect::<Vec<_>>(),
        vec![(msg_1, Some(run_1)), (msg_3, Some(run_3))]
    );
    assert_eq!(
        cold_replayed
            .runs
            .iter()
            .map(|run| (run.run_id, run.input_message_id))
            .collect::<Vec<_>>(),
        vec![(run_1, Some(msg_1)), (run_3, Some(msg_3))]
    );
    assert_eq!(
        cold_replayed
            .activities
            .iter()
            .map(|activity| activity.activity_id)
            .collect::<BTreeSet<_>>(),
        after_retract
            .activities
            .iter()
            .map(|activity| activity.activity_id)
            .collect::<BTreeSet<_>>()
    );

    pool.close().await;
}

#[tokio::test]
async fn cold_start_from_legacy_events_and_compacted_v18_snapshots_backfills_visible_inputs() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    // Use `LegacyPreK3LedgerEncoder` so every `RunAdmitted` event persisted in `event_store.ledger_records`
    // omits field 7 (`input_message_id`), authentically reproducing pre-K3 historical events.
    let harness = fixture::InputMessageHarness::with_legacy_encoder(pool.clone()).await;

    let (conversation_id, user_participant) = harness.create_conversation("create-legacy-k3").await;
    let identical_text = "historical identical prompt";

    let (msg_1, run_1) = harness
        .submit_user_message(
            conversation_id,
            user_participant,
            "legacy-submit-1",
            identical_text,
        )
        .await;
    harness.claim_and_fail_run(run_1, "legacy-worker-1").await;

    let (msg_retracted, run_retracted) = harness
        .submit_user_message(
            conversation_id,
            user_participant,
            "legacy-submit-retracted",
            identical_text,
        )
        .await;
    harness
        .claim_and_fail_run(run_retracted, "legacy-worker-retracted")
        .await;
    harness
        .retract_message(conversation_id, msg_retracted, "legacy-retract-2")
        .await;

    let (msg_3, run_3) = harness
        .submit_user_message(
            conversation_id,
            user_participant,
            "legacy-submit-3",
            identical_text,
        )
        .await;
    harness.claim_and_fail_run(run_3, "legacy-worker-3").await;
    let retry_3 = harness
        .retry_failed_run(
            conversation_id,
            user_participant,
            run_3,
            msg_3,
            1,
            "legacy-retry-3",
        )
        .await;

    // Confirm persisted `RunAdmitted` records genuinely have `input_message_id == None`.
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
        .expect("load historical ledger");
    let mut legacy_admitted_count = 0usize;
    for record in &page.records {
        if let LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Admitted {
            input_message_id,
            ..
        })) = ProtobufLedgerPayloadCodec
            .decode(record.metadata.record_kind, &record.payload)
            .expect("decode historical record")
        {
            assert_eq!(
                input_message_id, None,
                "fixture must persist pre-K3 RunAdmitted records without field 7"
            );
            legacy_admitted_count += 1;
        }
    }
    assert_eq!(legacy_admitted_count, 4);

    // 1. Cold start from old pre-K3 events (no snapshots in `event_store.snapshots`).
    let from_old_events = harness.read_projection(conversation_id).await;
    assert_eq!(
        from_old_events
            .messages
            .iter()
            .map(|message| (message.message_id, message.source_run_id))
            .collect::<Vec<_>>(),
        vec![(msg_1, Some(run_1)), (msg_3, Some(run_3))]
    );
    assert_eq!(
        from_old_events
            .runs
            .iter()
            .map(|run| (run.run_id, run.input_message_id))
            .collect::<Vec<_>>(),
        vec![
            (run_1, Some(msg_1)),
            (run_3, Some(msg_3)),
            (retry_3, Some(msg_3)),
        ]
    );
    assert!(
        from_old_events
            .messages
            .iter()
            .all(|message| message.message_id != msg_retracted)
    );
    assert!(
        from_old_events
            .runs
            .iter()
            .all(|run| run.run_id != run_retracted)
    );

    // 2. Cold start from a compacted pre-K3 `v18` snapshot (`conversation_projection.v18.root`)
    // where visible user messages have `source_run_id = None` and visible runs have `input_message_id = None`,
    // while `conversation.messages` and `runtime.runs` still retain the retracted turn (`msg_retracted`, `run_retracted`).
    let retracted_rows_before: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT message_id FROM conversation.message_retractions
         WHERE tenant_id = $1 AND conversation_id = $2
         ORDER BY message_id",
    )
    .bind(harness.actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read message_retractions before upgrade");
    assert_eq!(retracted_rows_before, vec![msg_retracted.into_uuid()]);

    let codec = ProtobufConversationProjectionCodec;
    let mut legacy_v18 = from_old_events.clone();
    legacy_v18.snapshot_id = ProjectionSnapshotId::generate();
    for message in &mut legacy_v18.messages {
        message.source_run_id = None;
    }
    for run in &mut legacy_v18.runs {
        run.input_message_id = None;
    }
    for run in &mut legacy_v18.active_runs {
        run.input_message_id = None;
    }
    legacy_v18.checksum = codec.checksum(&legacy_v18);
    let legacy_v18_bytes = codec
        .encode(&legacy_v18)
        .expect("encode legacy v18 snapshot");
    let legacy_v18_content_hash = Sha256::digest(&legacy_v18_bytes);

    sqlx::query("DELETE FROM event_store.snapshots WHERE conversation_id = $1")
        .bind(conversation_id.into_uuid())
        .execute(&pool)
        .await
        .expect("remove v19 snapshot before seeding compacted v18 fixture");
    sqlx::query(
        "INSERT INTO event_store.snapshots (
             snapshot_id, tenant_id, conversation_id, home_epoch,
             source_sequence_from, source_sequence_to, snapshot_kind,
             encoded_snapshot, content_hash, created_at_ms
         ) VALUES (
             $1, $2, $3, $4::bigint, 1, $5::bigint, $6, $7, $8, $9
         )",
    )
    .bind(legacy_v18.snapshot_id.into_uuid())
    .bind(legacy_v18.tenant_id.into_uuid())
    .bind(legacy_v18.conversation_id.into_uuid())
    .bind(legacy_v18.home_epoch.get().to_string())
    .bind(legacy_v18.through_conversation_seq.get().to_string())
    .bind(LEGACY_PROJECTION_SNAPSHOT_ROOT_KIND)
    .bind(&legacy_v18_bytes)
    .bind(legacy_v18_content_hash.into_bytes().to_vec())
    .bind(legacy_v18.created_at.unix_millis())
    .execute(&pool)
    .await
    .expect("seed legacy v18 snapshot");

    // Advance `retained_from_sequence` past the compacted prefix (`last_sequence + 1`).
    let compacted_floor = legacy_v18.through_conversation_seq.get() + 1;
    sqlx::query(
        "UPDATE event_store.ledger_streams
         SET retained_from_sequence = $1::bigint
         WHERE tenant_id = $2 AND conversation_id = $3",
    )
    .bind(compacted_floor.to_string())
    .bind(harness.actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("advance ledger retention floor");
    assert!(matches!(
        ledger
            .load(
                LedgerCursor {
                    conversation_id,
                    home_epoch: legacy_v18.home_epoch,
                    after_conversation_seq: LedgerOffset::ZERO,
                },
                10,
            )
            .await,
        Err(PostgresLedgerError::CursorCompacted { .. })
    ));

    // Explicit `preferred_snapshot_id` read must return the untouched `v18` snapshot with its exact checksum, cursor, and retracted pruning.
    let pinned_v18 = harness
        .projection
        .get_conversation_projection(GetConversationProjection {
            actor: harness.actor,
            conversation_id,
            preferred_snapshot_id: Some(legacy_v18.snapshot_id),
        })
        .await
        .expect("read pinned legacy v18 snapshot");
    assert_eq!(pinned_v18.snapshot_id, legacy_v18.snapshot_id);
    assert_eq!(pinned_v18.checksum, legacy_v18.checksum);
    assert_eq!(pinned_v18.cursor(), legacy_v18.cursor());
    assert!(
        pinned_v18
            .messages
            .iter()
            .all(|message| message.message_id != msg_retracted)
    );
    assert!(
        pinned_v18
            .runs
            .iter()
            .all(|run| run.run_id != run_retracted)
    );

    // Ordinary head read (`preferred_snapshot_id: None`) must upgrade and backfill visible rows into a valid `v19` snapshot
    // while preserving cursors, retracted pruning/tables, and the stored `v18` row's checksum/bytes.
    let upgraded_v19 = harness.read_projection(conversation_id).await;
    assert_ne!(upgraded_v19.snapshot_id, legacy_v18.snapshot_id);
    assert_eq!(upgraded_v19.cursor(), legacy_v18.cursor());
    assert_eq!(
        upgraded_v19
            .activities
            .iter()
            .map(|activity| activity.activity_id)
            .collect::<BTreeSet<_>>(),
        legacy_v18
            .activities
            .iter()
            .map(|activity| activity.activity_id)
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(
        upgraded_v19
            .messages
            .iter()
            .map(|message| (message.message_id, message.source_run_id))
            .collect::<Vec<_>>(),
        vec![(msg_1, Some(run_1)), (msg_3, Some(run_3))],
        "compacted v18 backfill must pair visible user messages without resurrecting retracted msg_retracted or overwriting msg_3 with retry_3"
    );
    assert_eq!(
        upgraded_v19
            .runs
            .iter()
            .map(|run| (run.run_id, run.input_message_id))
            .collect::<Vec<_>>(),
        vec![
            (run_1, Some(msg_1)),
            (run_3, Some(msg_3)),
            (retry_3, Some(msg_3)),
        ]
    );
    assert_eq!(
        upgraded_v19
            .active_runs
            .iter()
            .map(|run| (run.run_id, run.input_message_id))
            .collect::<Vec<_>>(),
        vec![(retry_3, Some(msg_3))]
    );
    assert_eq!(upgraded_v19.checksum, codec.checksum(&upgraded_v19));

    let retracted_rows_after: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT message_id FROM conversation.message_retractions
         WHERE tenant_id = $1 AND conversation_id = $2
         ORDER BY message_id",
    )
    .bind(harness.actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read message_retractions after upgrade");
    assert_eq!(retracted_rows_after, retracted_rows_before);

    let v19_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM event_store.snapshots
         WHERE conversation_id = $1 AND snapshot_kind = $2",
    )
    .bind(conversation_id.into_uuid())
    .bind(PROJECTION_SNAPSHOT_ROOT_KIND)
    .fetch_one(&pool)
    .await
    .expect("count persisted v19 snapshots");
    assert_eq!(v19_count, 1);

    let preserved_v18_hash: Vec<u8> = sqlx::query_scalar(
        "SELECT content_hash FROM event_store.snapshots
         WHERE snapshot_id = $1 AND snapshot_kind = $2",
    )
    .bind(legacy_v18.snapshot_id.into_uuid())
    .bind(LEGACY_PROJECTION_SNAPSHOT_ROOT_KIND)
    .fetch_one(&pool)
    .await
    .expect("read preserved v18 content_hash");
    assert_eq!(
        preserved_v18_hash,
        legacy_v18_content_hash.into_bytes().to_vec(),
        "upgrading to v19 must preserve existing v18 snapshot rows and checksums untouched"
    );

    pool.close().await;
}
