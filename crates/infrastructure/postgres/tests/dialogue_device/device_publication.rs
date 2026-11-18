//! Real PG custody and publication transactions, using the original isolated
//! Native signature fixture. This tests the trusted transport boundary; it is
//! not evidence that an operating-system-qualified Native host was installed.
use super::device_grant::{grant, keys, sign};
use super::device_target::{now, schema, target};
use super::*;
use musteragent_postgres::{
    PostgresContentStore, PostgresDeviceToolPublication, PostgresDialogueIntentHandoffStore,
    PostgresTieredBlobStore,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_storage_protection::OpaqueRefKeyring;

struct Fixture {
    _pg: IsolatedPostgres,
    pool: PgPool,
    actor: Actor,
    target: OriginDeviceExecutionTarget,
    queue: PostgresDialogueIntentHandoffStore,
    content: Arc<PostgresContentStore>,
    publisher: PostgresDeviceToolPublication,
    claim: DeviceIntentClaim,
    custody: DeviceSourceCustody,
    binding: DeviceDeliveryBinding,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_prior_input(false).await
    }
    async fn with_prior_input(prior_input: bool) -> Self {
        Self::with_permission(prior_input, None).await
    }
    async fn with_permission(prior_input: bool, permission: Option<&str>) -> Self {
        super::device_target::require_resource_successor_release();
        let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
        let pool = pg.pool().await.unwrap();
        schema(&pool).await;
        let owner = actor();
        let device = DeviceId::generate();
        let install = DeviceInstallationId::generate();
        let authority = target(owner, device, install, now(&pool).await);
        let active = attachment_fixture::activate(&pool, owner).await;
        if let Some(permission) = permission {
            sqlx::query("UPDATE agent.owned_agent_configurations SET permission_mode=$2,permission_revision=permission_revision+1,manifest_revision=manifest_revision+1 WHERE agent_id=$1")
                .bind(active.agent.agent_id.into_uuid()).bind(permission).execute(&pool).await.unwrap();
        }
        let key = DottedKey::new("test.device.publication").unwrap();
        let content = Arc::new(PostgresContentStore::new(
            pool.clone(),
            Arc::new(PostgresTieredBlobStore::new(
                pool.clone(),
                Arc::new(crate::fixture::MemoryBlobs::default()),
            )),
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [48; 32])]).unwrap()),
        ));
        let mut command = input(owner, active.agent.dialogue_id);
        command.origin = DialogueInputOrigin {
            surface: DialogueInputSurface::Desktop,
            installation_id: Some(install),
        };
        command.accepted_at = UtcTimestamp::from_unix_millis(now(&pool).await);
        command.content_ref = content
            .put(PutContent {
                tenant_id: owner.tenant_id,
                media_type: MediaType::new("text/plain").unwrap(),
                bytes: b"Give me a concise answer on this device".to_vec(),
            })
            .await
            .unwrap();
        if prior_input {
            use musteragent_runtime_content::upload::{content_upload_key, idempotent_content_id};
            let upload_id = "f".repeat(64);
            let attachment = content
                .put_at(
                    idempotent_content_id(owner, &content_upload_key(&upload_id, None).unwrap())
                        .unwrap(),
                    PutContent {
                        tenant_id: owner.tenant_id,
                        media_type: MediaType::new("image/png").unwrap(),
                        bytes: vec![
                            137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0,
                            1, 0, 0, 0, 1, 8, 4, 0, 0, 0, 181, 28, 12, 2, 0, 0, 0, 11, 73, 68, 65,
                            84, 120, 218, 99, 252, 255, 31, 0, 3, 3, 2, 0, 239, 154, 63, 89, 0, 0,
                            0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
                        ],
                    },
                )
                .await
                .unwrap();
            let mut prior = command.clone();
            prior.command_id = CommandId::generate();
            prior.input_attachments = vec![DialogueInputAttachment {
                attachment: musteragent_runtime_contract::AttachmentRef {
                    content_ref: attachment,
                    display_name: musteragent_runtime_contract::DisplayText::new("earlier.png")
                        .unwrap(),
                },
                source: DialogueInputAttachmentSource::Uploaded { upload_id },
            }];
            command.observed_cursor = PostgresAgentDialogueStore::new(pool.clone())
                .accept_on_target(
                    prior,
                    DialogueIntentExecutionTarget::OriginDevice(authority.clone()),
                )
                .await
                .unwrap()
                .intent
                .accepted_cursor;
        }
        let intent = PostgresAgentDialogueStore::new(pool.clone())
            .accept_on_target(
                command,
                DialogueIntentExecutionTarget::OriginDevice(authority.clone()),
            )
            .await
            .unwrap()
            .intent
            .intent_id;
        let queue = PostgresDialogueIntentHandoffStore::new(pool.clone())
            .with_device_grant_authority(keys());
        let claim = queue
            .claim_device(
                owner,
                authority.clone(),
                intent,
                CommandId::generate().into_uuid(),
                Duration::from_secs(30),
            )
            .await
            .unwrap()
            .unwrap();
        let custody = queue.capture_device_source(claim).await.unwrap();
        let binding = DeviceDeliveryBinding {
            work_item: None,
            intent_id: intent,
            run_id: custody.source.identities.run_id().unwrap(),
            device_id: device,
            installation_id: install,
            source_digest: custody.source_digest,
            snapshot_hash: Sha256::digest(b"actual-receipt-field-under-test"),
            grant_proof_hash: Sha256::digest(b"not-yet-bound"),
        };
        let publisher = PostgresDeviceToolPublication::new(pool.clone(), content.clone());
        Self {
            _pg: pg,
            pool,
            actor: owner,
            target: authority,
            queue,
            content,
            publisher,
            claim,
            custody,
            binding,
        }
    }
    async fn bind(&mut self) {
        let proof = self
            .queue
            .bind_device_grant(
                self.claim,
                sign(grant(&self.custody, now(&self.pool).await)),
            )
            .await
            .unwrap();
        self.binding.grant_proof_hash = Sha256::digest(&proof.encoded_proof);
    }
    fn delivery(&self) -> DeviceToolDelivery {
        let bytes = br#"{"text":"The answer has been delivered.","purpose":"reply"}"#.to_vec();
        DeviceToolDelivery::new(
            self.binding.clone(),
            InvocationId::generate(),
            DIALOGUE_PUBLISH_TOOL.into(),
            ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("application/json").unwrap(),
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(&bytes),
            },
            bytes,
        )
        .unwrap()
    }
}

#[tokio::test]
async fn device_publication_admission_ack_requires_exact_bound_custody_and_is_immutable() {
    let mut f = Fixture::new().await;
    assert!(
        f.queue
            .acknowledge_device_admission(f.actor, &f.target, &f.binding)
            .await
            .is_err()
    );
    f.bind().await;
    for case in 0..6 {
        let mut wrong = f.binding.clone();
        match case {
            0 => wrong.run_id = RunId::generate(),
            1 => wrong.intent_id = UserIntentId::generate(),
            2 => wrong.device_id = DeviceId::generate(),
            3 => wrong.installation_id = DeviceInstallationId::generate(),
            4 => wrong.source_digest = Sha256::digest(b"wrong source"),
            _ => wrong.grant_proof_hash = Sha256::digest(b"wrong proof"),
        }
        assert!(
            f.queue
                .acknowledge_device_admission(f.actor, &f.target, &wrong)
                .await
                .is_err(),
            "case {case}"
        );
    }
    let foreign = actor();
    assert!(
        f.queue
            .acknowledge_device_admission(
                foreign,
                &target(
                    foreign,
                    f.binding.device_id,
                    f.binding.installation_id,
                    now(&f.pool).await
                ),
                &f.binding
            )
            .await
            .is_err()
    );
    // An observed admission may arrive after the old pickup HTTP response was
    // lost. It cannot renew that claim or select a second execution Home.
    sqlx::query(
        "UPDATE agent.dialogue_intent_admissions SET lease_expires_at_ms=0 WHERE intent_id=$1",
    )
    .bind(f.binding.intent_id.into_uuid())
    .execute(&f.pool)
    .await
    .unwrap();
    f.queue
        .acknowledge_device_admission(f.actor, &f.target, &f.binding)
        .await
        .unwrap();
    let first: (Vec<u8>, i64) = sqlx::query_as("SELECT admitted_snapshot_hash,admitted_at_ms FROM agent.device_admissions WHERE intent_id=$1")
        .bind(f.binding.intent_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    f.queue
        .acknowledge_device_admission(f.actor, &f.target, &f.binding)
        .await
        .unwrap();
    let second: (Vec<u8>, i64) = sqlx::query_as("SELECT admitted_snapshot_hash,admitted_at_ms FROM agent.device_admissions WHERE intent_id=$1")
        .bind(f.binding.intent_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(first, second);
    let mut wrong = f.binding.clone();
    wrong.snapshot_hash = Sha256::digest(b"another admitted snapshot");
    assert_eq!(
        f.queue
            .acknowledge_device_admission(f.actor, &f.target, &wrong)
            .await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.runs")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn device_publication_retries_commit_one_real_entry_without_cloud_run_or_tool() {
    let mut f = Fixture::new().await;
    f.bind().await;
    let delivery = f.delivery();
    assert_eq!(
        f.publisher
            .publish_device(f.actor, &f.target, &delivery)
            .await,
        Err(DialogueToolPublicationError::Forbidden)
    );
    f.queue
        .acknowledge_device_admission(f.actor, &f.target, &f.binding)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        f.publisher.publish_device(f.actor, &f.target, &delivery),
        f.publisher.publish_device(f.actor, &f.target, &delivery)
    );
    let a = a.unwrap();
    assert_eq!(a, b.unwrap());
    a.validate_for(&delivery).unwrap();
    assert_eq!(
        f.content
            .read(f.actor.tenant_id, &a.result_ref)
            .await
            .unwrap(),
        a.result_bytes
    );
    assert_eq!(count(&f.pool, "dialogue_publications").await, 1);
    assert_eq!(count(&f.pool, "dialogue_tool_publication_groups").await, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.runs")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tool.tool_invocations")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        0
    );
    let entry = PostgresAgentDialogueStore::new(f.pool.clone())
        .read_history(
            f.actor,
            f.custody.source.scope.dialogue_id,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 20,
            },
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|e| e.entry.entry_id == a.entry_ids[0])
        .unwrap();
    let AgentDialogueContent::AgentMessage { content_ref } = entry.entry.content else {
        panic!("expected visible message")
    };
    assert_eq!(
        f.content
            .read(f.actor.tenant_id, &content_ref)
            .await
            .unwrap(),
        b"The answer has been delivered."
    );
    // Foreign account and altered immutable invocation fail before staging.
    let foreign = actor();
    assert_eq!(
        f.publisher
            .publish_device(
                foreign,
                &target(
                    foreign,
                    f.binding.device_id,
                    f.binding.installation_id,
                    now(&f.pool).await
                ),
                &delivery
            )
            .await,
        Err(DialogueToolPublicationError::Forbidden)
    );
    let mut wrong = f.binding.clone();
    wrong.snapshot_hash = Sha256::digest(b"wrong admitted snapshot");
    let altered = DeviceToolDelivery::new(
        wrong,
        delivery.invocation_id(),
        delivery.tool_key().into(),
        delivery.request_ref().clone(),
        delivery.request_bytes().to_vec(),
    )
    .unwrap();
    assert_eq!(
        f.publisher
            .publish_device(f.actor, &f.target, &altered)
            .await,
        Err(DialogueToolPublicationError::Forbidden)
    );
    // Only a permission reduction fences admitted native work (2345e0acca).
    sqlx::query("UPDATE agent.owned_agent_configurations SET permission_revision=permission_revision+1,manifest_revision=manifest_revision+1,permission_mode='supervised' WHERE agent_id=$1")
        .bind(f.custody.source.scope.agent_id.into_uuid()).execute(&f.pool).await.unwrap();
    assert!(
        f.publisher
            .publish_device(f.actor, &f.target, &f.delivery())
            .await
            .is_err()
    );
    assert_eq!(count(&f.pool, "dialogue_publications").await, 1);
    // Read an already committed original receipt, never republish on retry.
    assert_eq!(
        f.publisher
            .publish_device(f.actor, &f.target, &delivery)
            .await
            .unwrap(),
        a
    );
    assert!(sqlx::query("UPDATE agent.dialogue_tool_publication_groups SET device_snapshot_hash=$2 WHERE invocation_id=$1")
        .bind(delivery.invocation_id().into_uuid()).bind(Sha256::digest(b"tamper").into_bytes().to_vec())
        .execute(&f.pool).await.is_err());
}

#[path = "device_artifacts.rs"]
mod artifacts;

#[path = "canonical_resources.rs"]
mod canonical_resources;
