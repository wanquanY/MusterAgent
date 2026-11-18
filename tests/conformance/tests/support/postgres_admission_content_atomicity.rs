use std::sync::Arc;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{CommandId, ContentId, DottedKey, TenantId};
use musteragent_postgres::{PostgresContentStore, migrate};
use musteragent_runtime_content::{
    AdmissionContentPlan, AdmissionContentSlot, AdmissionContentSource, BlobIdentity, BlobStore,
    BlobStoreError, ContentStore, ContentStoreError, PutBlob, PutBlobReceipt, StorageLocator,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;

fn keys(
    active: &str,
    material: impl IntoIterator<Item = (&'static str, [u8; 32])>,
) -> Arc<OpaqueRefKeyring> {
    Arc::new(
        OpaqueRefKeyring::new(
            DottedKey::new(active).expect("active key"),
            material
                .into_iter()
                .map(|(id, key)| (DottedKey::new(id).expect("retained key"), key)),
        )
        .expect("reference keyring"),
    )
}

fn plan(tenant_id: TenantId) -> AdmissionContentPlan {
    AdmissionContentPlan {
        tenant_id,
        command_id: CommandId::generate(),
        command_fingerprint: Sha256::digest(b"protected-locator-restart-command"),
        slot: AdmissionContentSlot::SubmitMessage,
        proposed_content_id: ContentId::generate(),
        source: AdmissionContentSource::Inline {
            media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
            bytes: b"protected locator restart bytes".to_vec(),
        },
    }
}

async fn protected_bytes(pool: &sqlx::PgPool, plan: &AdmissionContentPlan) -> Vec<u8> {
    sqlx::query_scalar(
        "SELECT storage_ref_protected FROM artifact.admission_content_stages
         WHERE tenant_id = $1 AND command_id = $2 AND slot = $3",
    )
    .bind(plan.tenant_id.into_uuid())
    .bind(plan.command_id.into_uuid())
    .bind(i16::from(plan.slot.code()))
    .fetch_one(pool)
    .await
    .expect("protected stage locator")
}

fn context(identity: BlobIdentity) -> Vec<u8> {
    format!(
        "content:{}:{}:storage",
        identity.tenant_id, identity.content_id
    )
    .into_bytes()
}

pub async fn semantic_restart_and_rotation_are_verified() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let blobs = Arc::new(MemoryBlobs::in_memory());
    let old_keys = keys(
        "runtime.content.old",
        [("runtime.content.old", [17_u8; 32])],
    );
    let plan = plan(TenantId::generate());
    let first = PostgresContentStore::new(pool.clone(), blobs.clone(), old_keys.clone());
    let stage = first
        .prepare_admission(plan.clone())
        .await
        .expect("prepare stage");
    let first_ciphertext = protected_bytes(&pool, &plan).await;

    let reconstructed = PostgresContentStore::new(pool.clone(), blobs.clone(), old_keys.clone());
    assert_eq!(
        reconstructed
            .prepare_admission(plan.clone())
            .await
            .expect("restart reconcile"),
        stage
    );
    assert_eq!(
        protected_bytes(&pool, &plan).await,
        first_ciphertext,
        "semantic restart must retain the original randomized ciphertext"
    );

    let rotating_keys = keys(
        "runtime.content.active",
        [
            ("runtime.content.old", [17_u8; 32]),
            ("runtime.content.active", [29_u8; 32]),
        ],
    );
    let second_reconstruction =
        PostgresContentStore::new(pool.clone(), blobs.clone(), rotating_keys);
    second_reconstruction
        .prepare_admission(plan.clone())
        .await
        .expect("retained key restart");
    assert_eq!(protected_bytes(&pool, &plan).await, first_ciphertext);
    second_reconstruction
        .rewrap_admission_stage(&stage)
        .await
        .expect("explicit verified rewrap");
    let rotated_ciphertext = protected_bytes(&pool, &plan).await;
    assert_ne!(rotated_ciphertext, first_ciphertext);

    let active_only = PostgresContentStore::new(
        pool.clone(),
        blobs.clone(),
        keys(
            "runtime.content.active",
            [("runtime.content.active", [29_u8; 32])],
        ),
    );
    active_only
        .prepare_admission(plan.clone())
        .await
        .expect("active key restart");
    let old_only = PostgresContentStore::new(pool.clone(), blobs, old_keys);
    assert_eq!(
        old_only.prepare_admission(plan).await,
        Err(ContentStoreError::Unavailable),
        "rotation is explicit and old-only readers fail closed"
    );
    pool.close().await;
}

pub async fn hostile_protection_and_material_drift_fail_closed() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let blobs = Arc::new(MemoryBlobs::in_memory());
    let keyring = keys(
        "runtime.content.active",
        [("runtime.content.active", [41_u8; 32])],
    );
    let plan = plan(TenantId::generate());
    let store = PostgresContentStore::new(pool.clone(), blobs.clone(), keyring.clone());
    let stage = store
        .prepare_admission(plan.clone())
        .await
        .expect("prepare stage");
    let identity = BlobIdentity {
        tenant_id: stage.tenant_id,
        content_id: stage.content.content_id,
    };
    let locator = blobs.derive_locator(identity).expect("locator");
    let original = protected_bytes(&pool, &plan).await;

    let wrong_context = keyring
        .seal(b"wrong-protection-context", locator.as_str())
        .expect("seal wrong context");
    replace_protected(&pool, &plan, wrong_context.as_bytes()).await;
    assert!(store.prepare_admission(plan.clone()).await.is_err());

    let wrong_locator = keyring
        .seal(&context(identity), "wrong/semantic/locator")
        .expect("seal wrong locator");
    replace_protected(&pool, &plan, wrong_locator.as_bytes()).await;
    assert!(store.prepare_admission(plan.clone()).await.is_err());
    replace_protected(&pool, &plan, &original).await;

    let wrong_keys = PostgresContentStore::new(
        pool.clone(),
        blobs.clone(),
        keys(
            "runtime.content.wrong",
            [("runtime.content.wrong", [99_u8; 32])],
        ),
    );
    assert_eq!(
        wrong_keys.prepare_admission(plan.clone()).await,
        Err(ContentStoreError::Unavailable)
    );

    sqlx::query(
        "UPDATE artifact.admission_content_stages SET content_hash = $4
         WHERE tenant_id = $1 AND command_id = $2 AND slot = $3",
    )
    .bind(plan.tenant_id.into_uuid())
    .bind(plan.command_id.into_uuid())
    .bind(i16::from(plan.slot.code()))
    .bind(Sha256::digest(b"wrong hash").into_bytes().as_slice())
    .execute(&pool)
    .await
    .expect("corrupt hash");
    assert_eq!(
        store.prepare_admission(plan.clone()).await,
        Err(ContentStoreError::Invariant)
    );
    sqlx::query(
        "UPDATE artifact.admission_content_stages SET content_hash = $4,
             byte_length = $5::numeric
         WHERE tenant_id = $1 AND command_id = $2 AND slot = $3",
    )
    .bind(plan.tenant_id.into_uuid())
    .bind(plan.command_id.into_uuid())
    .bind(i16::from(plan.slot.code()))
    .bind(stage.content.sha256.into_bytes().as_slice())
    .bind(stage.content.byte_length.saturating_add(1).to_string())
    .execute(&pool)
    .await
    .expect("corrupt length");
    assert_eq!(
        store.prepare_admission(plan.clone()).await,
        Err(ContentStoreError::Invariant)
    );
    sqlx::query(
        "UPDATE artifact.admission_content_stages SET byte_length = $4::numeric
         WHERE tenant_id = $1 AND command_id = $2 AND slot = $3",
    )
    .bind(plan.tenant_id.into_uuid())
    .bind(plan.command_id.into_uuid())
    .bind(i16::from(plan.slot.code()))
    .bind(stage.content.byte_length.to_string())
    .execute(&pool)
    .await
    .expect("restore length");

    let corrupt_reader: Arc<dyn BlobStore> = Arc::new(CorruptReadBlobStore(blobs));
    let corrupt_store = PostgresContentStore::new(pool.clone(), corrupt_reader, keyring);
    assert_eq!(
        corrupt_store.prepare_admission(plan).await,
        Err(ContentStoreError::LengthMismatch)
    );
    pool.close().await;
}

async fn replace_protected(pool: &sqlx::PgPool, plan: &AdmissionContentPlan, protected: &[u8]) {
    sqlx::query(
        "UPDATE artifact.admission_content_stages SET storage_ref_protected = $4
         WHERE tenant_id = $1 AND command_id = $2 AND slot = $3",
    )
    .bind(plan.tenant_id.into_uuid())
    .bind(plan.command_id.into_uuid())
    .bind(i16::from(plan.slot.code()))
    .bind(protected)
    .execute(pool)
    .await
    .expect("replace protected locator");
}

struct CorruptReadBlobStore(Arc<MemoryBlobs>);

impl BlobStore for CorruptReadBlobStore {
    fn put_stream<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        source: &'a mut dyn musteragent_runtime_content::ContentSource,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<
            musteragent_runtime_content::PutBlobReceipt,
            musteragent_runtime_content::ContentStoreError,
        >,
    > {
        self.0.put_stream(identity, length, hash, source)
    }
    fn copy_to<'a>(
        &'a self,
        identity: musteragent_runtime_content::BlobIdentity,
        locator: &'a musteragent_runtime_content::StorageLocator,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        sink: &'a mut dyn musteragent_runtime_content::ContentSink,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<(), musteragent_runtime_content::ContentStoreError>,
    > {
        Box::pin(async move {
            let bytes = self.read(identity, locator).await?;
            musteragent_runtime_content::transfer_content(&mut bytes.as_slice(), sink, length, hash)
                .await
        })
    }

    fn put(
        &self,
        request: PutBlob,
    ) -> musteragent_runtime_content::ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>>
    {
        self.0.put(request)
    }

    fn read<'a>(
        &'a self,
        _identity: BlobIdentity,
        _locator: &'a StorageLocator,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<Vec<u8>, BlobStoreError>> {
        Box::pin(async { Ok(b"wrong blob".to_vec()) })
    }

    fn delete<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<(), BlobStoreError>> {
        self.0.delete(identity, locator)
    }

    fn derive_locator(&self, identity: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        self.0.derive_locator(identity)
    }
}
