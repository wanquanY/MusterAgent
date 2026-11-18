//! Discovery reads the existing queue without claiming it or issuing authority.
use super::device_target::{now, schema, target};
use super::*;
use musteragent_postgres::PostgresDialogueIntentHandoffStore;

#[tokio::test]
async fn discovery_pages_original_installation_and_recovers_live_and_expired_claims() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    schema(&pool).await;
    let owner = actor();
    let device = DeviceId::generate();
    let install = DeviceInstallationId::generate();
    let authority = target(owner, device, install, now(&pool).await);
    let active = attachment_fixture::activate(&pool, owner).await;
    let dialogue = PostgresAgentDialogueStore::new(pool.clone());
    let queue = PostgresDialogueIntentHandoffStore::new(pool.clone());
    let mut expected = Vec::new();
    let mut cursor = DialogueCursor { epoch: 1, seq: 0 };
    for _ in 0..35 {
        let mut command = input(owner, active.agent.dialogue_id);
        command.origin = DialogueInputOrigin {
            surface: DialogueInputSurface::Desktop,
            installation_id: Some(install),
        };
        command.accepted_at = UtcTimestamp::from_unix_millis(now(&pool).await);
        command.observed_cursor = cursor;
        register_content(&pool, owner, &command.content_ref).await;
        let accepted = dialogue
            .accept_on_target(
                command,
                DialogueIntentExecutionTarget::OriginDevice(authority.clone()),
            )
            .await
            .unwrap();
        cursor = accepted.intent.accepted_cursor;
        expected.push(accepted.intent.intent_id);
    }
    expected.sort();
    let first = queue
        .discover_device(owner, &authority, None)
        .await
        .unwrap();
    assert_eq!(first.intents, expected[..32]);
    assert_eq!(first.next_after, Some(expected[31]));
    assert_eq!(
        queue
            .discover_device(owner, &authority, None)
            .await
            .unwrap(),
        first
    );
    let second = queue
        .discover_device(owner, &authority, first.next_after)
        .await
        .unwrap();
    assert_eq!(second.intents, expected[32..]);
    assert_eq!(second.next_after, None);
    assert!(
        queue
            .discover_device(owner, &authority, expected.last().copied())
            .await
            .unwrap()
            .intents
            .is_empty()
    );
    let states: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT status FROM agent.dialogue_intent_admissions")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(states, ["pending"], "discovery cannot acquire execution");
    let claim = queue
        .claim_device(
            owner,
            authority.clone(),
            expected[0],
            CommandId::generate().into_uuid(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    // A live claim stays discoverable (its SQLite receipt may be unacknowledged)
    // but only an expired lease may be prepared again.
    let claimed = queue
        .discover_device(owner, &authority, None)
        .await
        .unwrap();
    assert_eq!(
        claimed.intents, first.intents,
        "live claims may have an unacknowledged SQLite receipt"
    );
    assert_eq!(claimed.next_after, first.next_after);
    assert!(
        !claimed.prepare_allowed[0],
        "a live lease cannot be prepared again"
    );
    assert_eq!(claimed.prepare_allowed[1..], first.prepare_allowed[1..]);
    assert!(
        queue
            .claim_device(
                owner,
                authority.clone(),
                expected[0],
                CommandId::generate().into_uuid(),
                Duration::from_secs(30)
            )
            .await
            .unwrap()
            .is_none(),
        "discovery does not steal the live lease"
    );
    // Clock/expiry injection only; reclaim still runs through the original owner.
    sqlx::query(
        "UPDATE agent.dialogue_intent_admissions SET lease_expires_at_ms=0 WHERE intent_id=$1",
    )
    .bind(expected[0].into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        queue
            .discover_device(owner, &authority, None)
            .await
            .unwrap(),
        first
    );
    let recovered = queue
        .claim_device(
            owner,
            authority.clone(),
            expected[0],
            CommandId::generate().into_uuid(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.claim.generation, claim.claim.generation + 1);
    for wrong in [
        target(owner, DeviceId::generate(), install, now(&pool).await),
        target(
            owner,
            device,
            DeviceInstallationId::generate(),
            now(&pool).await,
        ),
    ] {
        assert!(
            queue
                .discover_device(owner, &wrong, None)
                .await
                .unwrap()
                .intents
                .is_empty()
        );
    }
    assert!(
        queue
            .discover_device(actor(), &authority, None)
            .await
            .is_err()
    );
    let expired = target(owner, device, install, now(&pool).await - 400_000);
    assert!(queue.discover_device(owner, &expired, None).await.is_err());
    // A retired original public scope stays discoverable to its own exact
    // installation so the device can claim each queued input and settle it as
    // cancelled (agent_inactive) instead of leaving it pending (556eb6ebd7).
    sqlx::query("UPDATE agent.owned_agents SET status='archived' WHERE agent_id=$1")
        .bind(active.agent.agent_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        queue
            .discover_device(owner, &authority, None)
            .await
            .unwrap()
            .intents,
        first.intents
    );
}
