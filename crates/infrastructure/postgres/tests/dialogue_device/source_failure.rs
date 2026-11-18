//! Native source corruption is a server-owned pre-Run failure, never a client
//! schema observation. Real signed destination, original queue and PG writers.
use super::device_target::{now, schema, target};
use super::*;
use musteragent_postgres::{
    PostgresContentStore, PostgresDialogueIntentHandoffStore, PostgresTieredBlobStore,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_storage_protection::OpaqueRefKeyring;
use prost::Message as _;

struct Fixture {
    pg: IsolatedPostgres,
    pool: PgPool,
    owner: Actor,
    active: OwnedAgentActivationReceipt,
    target: OriginDeviceExecutionTarget,
    queue: PostgresDialogueIntentHandoffStore,
    content: Arc<PostgresContentStore>,
    claim: DeviceIntentClaim,
}
impl Fixture {
    async fn new() -> Self {
        super::device_target::require_resource_successor_release();
        let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
        let pool = pg.pool().await.unwrap();
        schema(&pool).await;
        let owner = actor();
        let key = DottedKey::new("dialogue.source.failure.test").unwrap();
        let content = Arc::new(PostgresContentStore::new(
            pool.clone(),
            Arc::new(PostgresTieredBlobStore::new(
                pool.clone(),
                Arc::new(crate::fixture::MemoryBlobs::default()),
            )),
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [63; 32])]).unwrap()),
        ));
        let active = attachment_fixture::activate_with_content(&pool, owner, content.clone()).await;
        let device = DeviceId::generate();
        let install = DeviceInstallationId::generate();
        let target = target(owner, device, install, now(&pool).await);
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
                bytes: b"Continue this request on this device".to_vec(),
            })
            .await
            .unwrap();
        let accepted = PostgresAgentDialogueStore::new(pool.clone())
            .accept_on_target(
                command,
                DialogueIntentExecutionTarget::OriginDevice(target.clone()),
            )
            .await
            .unwrap();
        let queue = PostgresDialogueIntentHandoffStore::new(pool.clone())
            .with_rejection_authority(super::device_grant::keys(), content.clone());
        let claim = queue
            .claim_device(
                owner,
                target.clone(),
                accepted.intent.intent_id,
                uuid::Uuid::now_v7(),
                Duration::from_secs(120),
            )
            .await
            .unwrap()
            .unwrap();
        Self {
            pg,
            pool,
            owner,
            active,
            target,
            queue,
            content,
            claim,
        }
    }
}

#[tokio::test]
async fn native_canonical_source_failure_finishes_queue_and_public_progress_once() {
    Box::pin(native_failure_journey()).await;
}
async fn native_failure_journey() {
    let f = Fixture::new().await;
    sqlx::query("UPDATE agent.owned_agent_configurations SET instructions_content_ref=$2,instructions_revision=instructions_revision+1,manifest_revision=manifest_revision+1 WHERE agent_id=$1")
        .bind(f.active.agent.agent_id.into_uuid()).bind(vec![255_u8]).execute(&f.pool).await.unwrap();
    assert_failed_once(&f).await;
}

#[tokio::test]
async fn native_missing_capability_ancestor_without_stop_index_finishes_original_queue() {
    let f = Box::pin(Fixture::new()).await;
    let retained: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent.dialogue_execution_source_roots WHERE intent_id=$1)",
    )
    .bind(f.claim.claim.intent_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert!(
        !retained,
        "fresh capture has no previously verified stop index"
    );
    // Simulate a damaged retained Capability lineage after canonical Native
    // acceptance. No public request can supply these server-owned columns.
    // The original signed destination, claim, content and public entry remain;
    // this nonexistent completion cannot prove the claimed ancestor edge.
    sqlx::query("UPDATE agent.dialogue_user_intents SET origin_surface='capability',continuation_interaction_id=$2,reply_to_intent_id=intent_id WHERE intent_id=$1")
        .bind(f.claim.claim.intent_id.into_uuid()).bind(uuid::Uuid::now_v7())
        .execute(&f.pool).await.unwrap();
    assert_failed_once(&f).await;
    let retained: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent.dialogue_execution_source_roots WHERE intent_id=$1)",
    )
    .bind(f.claim.claim.intent_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert!(!retained, "failure must not fabricate a verified stop root");
}

async fn assert_failed_once(f: &Fixture) {
    let wrong = DeviceIntentClaim {
        device_id: DeviceId::generate(),
        ..f.claim
    };
    assert_eq!(
        f.queue.capture_device_source(wrong).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(count(&f.pool, "dialogue_admission_outcomes").await, 0);
    assert_eq!(
        f.queue.capture_device_source(f.claim).await,
        Err(DialogueStoreError::Inactive)
    );
    let outcome:(String,String,String,uuid::Uuid)=sqlx::query_as("SELECT q.status,o.outcome,o.internal_failure_code,o.entry_id FROM agent.dialogue_intent_admissions q JOIN agent.dialogue_admission_outcomes o USING(intent_id) WHERE q.intent_id=$1")
        .bind(f.claim.claim.intent_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(
        (&outcome.0, &outcome.1, &outcome.2),
        (
            &"failed".to_owned(),
            &"failed".to_owned(),
            &"invalid_preparation".to_owned()
        )
    );
    let progress:(i16,i16,bool,i64)=sqlx::query_as("SELECT stage,blocker,execution_finished,revision FROM agent.dialogue_admission_progress WHERE intent_id=$1")
        .bind(f.claim.claim.intent_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(
        (progress.0, progress.1, progress.2),
        (4, 0, false),
        "failure never invents an admitted Run"
    );
    let bytes:Vec<u8>=sqlx::query_scalar("SELECT content_ref FROM agent.agent_dialogue_entries WHERE entry_id=$1 AND kind='agent_message' AND source_intent_id=$2")
        .bind(outcome.3).bind(f.claim.claim.intent_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    let reference = musteragent_persistence_codec::decode_content_ref(
        Some(musteragent_persistence_codec::wire::ContentRef::decode(bytes.as_slice()).unwrap()),
        "source_failure.content_ref",
    )
    .unwrap();
    let message =
        String::from_utf8(f.content.read(f.owner.tenant_id, &reference).await.unwrap()).unwrap();
    assert!(message.contains("系统遇到了问题"));
    assert!(!message.contains("invalid_preparation"));
    assert_eq!(count(&f.pool, "device_admissions").await, 0);
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.runs")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(runs, 0);
    for _ in 0..2 {
        assert_eq!(
            f.queue.capture_device_source(f.claim).await,
            Err(DialogueStoreError::Inactive)
        );
    }
    let after:(i16,i16,bool,i64)=sqlx::query_as("SELECT stage,blocker,execution_finished,revision FROM agent.dialogue_admission_progress WHERE intent_id=$1")
        .bind(f.claim.claim.intent_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(after, progress);
    assert_eq!(count(&f.pool, "dialogue_admission_outcomes").await, 1);
    let published:i64=sqlx::query_scalar("SELECT count(*) FROM agent.agent_dialogue_entries WHERE source_intent_id=$1 AND kind='agent_message'")
        .bind(f.claim.claim.intent_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(published, 1);
    assert!(
        f.queue
            .claim_device(
                f.owner,
                f.target.clone(),
                f.claim.claim.intent_id,
                uuid::Uuid::now_v7(),
                Duration::from_secs(120)
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.queue
            .discover_device(f.owner, &f.target, None)
            .await
            .unwrap()
            .intents
            .is_empty()
    );
}

#[tokio::test]
async fn native_source_infrastructure_failure_does_not_become_a_public_terminal() {
    let f = Box::pin(Fixture::new()).await;
    f.pool.close().await;
    assert_eq!(
        f.queue.capture_device_source(f.claim).await,
        Err(DialogueStoreError::Infrastructure)
    );
    let fresh = f.pg.pool().await.unwrap();
    assert_eq!(count(&fresh, "dialogue_admission_outcomes").await, 0);
    let state: String = sqlx::query_scalar(
        "SELECT status FROM agent.dialogue_intent_admissions WHERE intent_id=$1",
    )
    .bind(f.claim.claim.intent_id.into_uuid())
    .fetch_one(&fresh)
    .await
    .unwrap();
    assert_eq!(state, "claimed");
}
