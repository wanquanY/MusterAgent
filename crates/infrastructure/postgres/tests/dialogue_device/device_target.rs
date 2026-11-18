//! Trusted Native transport and original PG queue/custody boundaries together.
//! Backend signature/live membership is covered by the original Python owner;
//! here the fixture signs its real canonical subject with an isolated test key.
use super::*;
use ed25519_dalek::Signer;
use musteragent_device_authority as membership;
use musteragent_postgres::PostgresDialogueIntentHandoffStore;

pub(super) async fn schema(pool: &PgPool) {
    // Run rejection cases under unqualified profiles too; a fixture must not
    // stop before the production acceptance/claim guards are exercised.
    migrate(pool).await.unwrap();
}

pub(super) fn require_resource_successor_release() {
    // The kernel publication is independently compiled. A legacy
    // process-wide profile cannot select the product tested by this fixture.
    let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
    let metadata = musteragent_tool::product_release_metadata(&release).unwrap();
    assert_eq!(
        (
            metadata.profile_id,
            metadata.cloud_head,
            metadata.device_head,
            metadata.skill_schema
        ),
        (
            "musteragent.kernel.v1",
            "cloud.k1",
            "device.k1",
            "musteragent.runtime.skill-release.v1"
        ),
        "Native Resource conformance must exercise the exact current kernel publication"
    );
    assert_eq!(
        musteragent_runtime_snapshot::dialogue_device_discovery_policy_for_release(&release),
        Some(musteragent_runtime_snapshot::DIALOGUE_DEVICE_DISCOVERY_POLICY),
        "The current kernel publication must qualify Native Dialogue execution"
    );
}
pub(super) fn target(
    owner: Actor,
    device: DeviceId,
    install: DeviceInstallationId,
    now: i64,
) -> OriginDeviceExecutionTarget {
    let mut envelope = crate::fixture::membership::envelope(
        owner,
        DeviceDestination {
            device_id: device,
            installation_id: install,
            membership_id: uuid::Uuid::new_v5(&device.into_uuid(), b"test-membership"),
            membership_epoch: std::num::NonZeroU64::MIN,
        },
        now,
    );
    let membership::BackendPayload::Authority(mut authority) = envelope.decode_payload().unwrap();
    authority.subject.tenant_id = owner.tenant_id.to_string().parse().unwrap();
    authority.subject.principal_id = owner.principal_id.to_string().parse().unwrap();
    authority.subject.device_id = device.to_string().parse().unwrap();
    authority.subject.client_installation_id =
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse().unwrap();
    assert_ne!(
        authority.subject.client_installation_id.as_str(),
        install.to_string()
    );
    authority.issued_at_unix_ms = (now as u64 - 1000).try_into().unwrap();
    authority.expires_at_unix_ms = (now as u64 + 299_000).try_into().unwrap();
    envelope.payload_base64 = membership::PayloadBytes::from_payload(authority.as_ref()).unwrap();
    let key = ed25519_dalek::SigningKey::from_bytes(&[72; 32]);
    envelope.signature_base64 =
        membership::Signature::new(key.sign(&envelope.signing_input().unwrap()).to_bytes());
    key.verifying_key()
        .verify_strict(
            &envelope.signing_input().unwrap(),
            &ed25519_dalek::Signature::from_bytes(envelope.signature()),
        )
        .unwrap();
    OriginDeviceExecutionTarget::from_trusted_native_transport(
        device,
        install,
        membership::canonical_bytes(&envelope).unwrap(),
    )
    .unwrap()
}
pub(super) async fn now(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
        .fetch_one(pool)
        .await
        .unwrap()
}
#[tokio::test]
async fn device_acceptance_is_fixed_and_original_queue_freezes_one_exact_source() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    schema(&pool).await;
    let owner = actor();
    let device = DeviceId::generate();
    let install = DeviceInstallationId::generate();
    let authority = target(owner, device, install, now(&pool).await);
    let active = attachment_fixture::activate(&pool, owner).await;
    let mut command = input(owner, active.agent.dialogue_id);
    command.origin = DialogueInputOrigin {
        surface: DialogueInputSurface::Desktop,
        installation_id: Some(install),
    };
    command.accepted_at = UtcTimestamp::from_unix_millis(now(&pool).await);
    register_content(&pool, owner, &command.content_ref).await;
    let dialogue = PostgresAgentDialogueStore::new(pool.clone());
    let destination = DialogueIntentExecutionTarget::OriginDevice(authority.clone());
    if musteragent_runtime_snapshot::dialogue_device_discovery_policy_for_current_release()
        .is_none()
    {
        assert_eq!(
            dialogue.accept_on_target(command, destination).await,
            Err(DialogueStoreError::Unavailable)
        );
        assert_eq!(count(&pool, "dialogue_user_intents").await, 0);
        assert_eq!(count(&pool, "dialogue_intent_admissions").await, 0);
        assert_eq!(
            PostgresDialogueIntentHandoffStore::new(pool.clone())
                .claim_device(
                    owner,
                    authority,
                    UserIntentId::generate(),
                    CommandId::generate().into_uuid(),
                    Duration::from_secs(30),
                )
                .await,
            Err(DialogueStoreError::Unavailable),
            "An unqualified compiled composition cannot claim Device custody either"
        );
        return;
    }
    let accepted = dialogue
        .accept_on_target(command.clone(), destination.clone())
        .await
        .unwrap();
    assert!(
        dialogue
            .accept_on_target(command.clone(), destination)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(
        dialogue.accept(command.clone()).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    // Fresh membership proof for the exact same destination must recover the
    // retained receipt, not mutate its source proof or create another intent.
    let renewed = target(owner, device, install, now(&pool).await + 100);
    assert_ne!(
        renewed.membership_authority(),
        authority.membership_authority()
    );
    let replay = dialogue
        .accept_on_target(
            command.clone(),
            DialogueIntentExecutionTarget::OriginDevice(renewed),
        )
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.intent, accepted.intent);
    let observed = dialogue
        .read_intent_receipt(owner, command.dialogue_id, command.command_id)
        .await
        .unwrap();
    assert_eq!(
        observed,
        DialogueIntentReceiptObservation::Accepted(accepted.intent.clone())
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 1);
    let queue = PostgresDialogueIntentHandoffStore::new(pool.clone());
    assert!(
        queue
            .claim(CommandId::generate().into_uuid(), Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        queue
            .claim_device(
                owner,
                target(owner, DeviceId::generate(), install, now(&pool).await),
                accepted.intent.intent_id,
                CommandId::generate().into_uuid(),
                Duration::from_secs(30)
            )
            .await
            .unwrap()
            .is_none()
    );
    let foreign = actor();
    assert!(
        queue
            .claim_device(
                foreign,
                authority.clone(),
                accepted.intent.intent_id,
                CommandId::generate().into_uuid(),
                Duration::from_secs(30)
            )
            .await
            .is_err()
    );
    let claim = queue
        .claim_device(
            owner,
            authority.clone(),
            accepted.intent.intent_id,
            CommandId::generate().into_uuid(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    let custody = queue.capture_device_source(claim).await.unwrap();
    assert_eq!(
        queue
            .claim_device(
                owner,
                authority.clone(),
                accepted.intent.intent_id,
                claim.claim.owner,
                Duration::from_secs(30)
            )
            .await
            .unwrap(),
        Some(claim)
    );
    assert!(
        queue
            .claim_device(
                owner,
                authority.clone(),
                accepted.intent.intent_id,
                CommandId::generate().into_uuid(),
                Duration::from_secs(30)
            )
            .await
            .unwrap()
            .is_none()
    );
    custody.validate().unwrap();
    assert_eq!(custody.source.scope.agent_id, active.agent.agent_id);
    assert_eq!(queue.capture_device_source(claim).await.unwrap(), custody);
    // Changing current presentation after custody cannot rewrite the source
    // selected before the original DevicePrivate signer runs.
    sqlx::query("UPDATE agent.owned_agent_configurations SET display_name='Later name',profile_revision=profile_revision+1,manifest_revision=manifest_revision+1 WHERE agent_id=$1")
        .bind(active.agent.agent_id.into_uuid()).execute(&pool).await.unwrap();
    sqlx::query(
        "UPDATE agent.dialogue_intent_admissions SET lease_expires_at_ms=0 WHERE intent_id=$1",
    )
    .bind(accepted.intent.intent_id.into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    assert!(queue.capture_device_source(claim).await.is_err());
    assert!(
        queue
            .claim(CommandId::generate().into_uuid(), Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    let next = queue
        .claim_device(
            owner,
            authority,
            accepted.intent.intent_id,
            CommandId::generate().into_uuid(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(next.claim.generation > claim.claim.generation);
    assert_eq!(queue.capture_device_source(next).await.unwrap(), custody);
    assert_eq!(count(&pool, "device_admissions").await, 1);
    assert!(sqlx::query("UPDATE agent.dialogue_intent_admissions SET execution_target='cloud',target_device_id=NULL,target_installation_id=NULL,native_subject_proof=NULL WHERE intent_id=$1")
        .bind(accepted.intent.intent_id.into_uuid()).execute(&pool).await.is_err());
    assert!(
        sqlx::query("UPDATE agent.device_admissions SET source_envelope=$2 WHERE intent_id=$1")
            .bind(accepted.intent.intent_id.into_uuid())
            .bind(vec![1u8])
            .execute(&pool)
            .await
            .is_err()
    );
    // Source custody is not a fabricated Run or Cloud context.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.runs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}
