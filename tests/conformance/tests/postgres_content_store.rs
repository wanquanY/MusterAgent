use std::sync::Arc;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::{ContentId, DottedKey, TenantId};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::PostgresContentStore;
use musteragent_runtime_content::{
    BlobIdentity, BlobStore, ContentStore, ContentStoreError, PutBlob, PutContent,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;

fn reference_keys() -> Arc<OpaqueRefKeyring> {
    Arc::new(
        OpaqueRefKeyring::new(
            DottedKey::new("runtime.content.active").expect("key id"),
            [(
                DottedKey::new("runtime.content.active").expect("key id"),
                [23_u8; 32],
            )],
        )
        .expect("reference keyring"),
    )
}

fn rotating_reference_keys(
    active: &str,
    keys: impl IntoIterator<Item = (&'static str, [u8; 32])>,
) -> Arc<OpaqueRefKeyring> {
    Arc::new(
        OpaqueRefKeyring::new(
            DottedKey::new(active).expect("active key id"),
            keys.into_iter()
                .map(|(key_id, key)| (DottedKey::new(key_id).expect("retained key id"), key)),
        )
        .expect("reference keyring"),
    )
}

#[tokio::test]
async fn content_bytes_are_deduplicated_verified_and_tenant_authorized() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let blobs = Arc::new(ObjectStoreBlobStore::in_memory());
    let store = PostgresContentStore::new(pool.clone(), blobs, reference_keys());
    let tenant_id = TenantId::generate();
    let bytes = b"canonical-content".to_vec();
    let request = PutContent {
        tenant_id,
        media_type: MediaType::new("text/plain").expect("media type"),
        bytes: bytes.clone(),
    };

    let first = store.put(request.clone()).await.expect("store content");
    let duplicate = store
        .put(request.clone())
        .await
        .expect("deduplicate content");
    assert_eq!(first, duplicate);
    let other_request = PutContent {
        tenant_id: TenantId::generate(),
        ..request.clone()
    };
    let other = store
        .put(other_request.clone())
        .await
        .expect("other tenant content");
    let public = store
        .put_runtime_release(request)
        .await
        .expect("public release content");
    let public_duplicate = store
        .put_runtime_release(other_request)
        .await
        .expect("deduplicate public release");
    assert_eq!(public, public_duplicate);
    assert_ne!(first.content_id, other.content_id);
    assert_ne!(public.content_id, first.content_id);
    assert_ne!(public.content_id, other.content_id);
    assert_eq!(
        store.read(tenant_id, &first).await.expect("read content"),
        bytes
    );
    assert_eq!(
        store.read(TenantId::generate(), &first).await,
        Err(ContentStoreError::Unauthorized)
    );

    let alternate_media = store
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/octet-stream").expect("media type"),
            bytes: b"canonical-content".to_vec(),
        })
        .await
        .expect("store distinct semantic content");
    assert_ne!(first.content_id, alternate_media.content_id);

    let metadata: (Option<String>, Option<Vec<u8>>, i16) = sqlx::query_as(
        "SELECT storage_ref, storage_ref_protected, state
         FROM artifact.content_refs
         WHERE tenant_id = $1 AND content_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(first.content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read content metadata");
    assert_eq!(metadata.0, None);
    assert!(metadata.1.is_some_and(|value| !value.is_empty()));
    assert_eq!(metadata.2, 2);
    pool.close().await;
}

#[tokio::test]
async fn predeclared_content_identity_is_authoritative_and_idempotent() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let blobs = Arc::new(ObjectStoreBlobStore::in_memory());
    let store = PostgresContentStore::new(pool.clone(), blobs, reference_keys());
    let tenant_id = TenantId::generate();
    let first_id = ContentId::generate();
    let second_id = ContentId::generate();
    let request = PutContent {
        tenant_id,
        media_type: MediaType::new("application/json").expect("media type"),
        bytes: br#"{"timezone":"Asia/Shanghai"}"#.to_vec(),
    };

    assert_eq!(store.find_prepared_at(tenant_id, first_id).await, Ok(None));
    let first = store
        .put_at(first_id, request.clone())
        .await
        .expect("first predeclared identity");
    let replay = store
        .put_at(first_id, request.clone())
        .await
        .expect("idempotent replay");
    let second = store
        .put_at(second_id, request.clone())
        .await
        .expect("distinct predeclared identity with identical bytes");

    assert_eq!(first, replay);
    assert_eq!(
        store.find_prepared_at(tenant_id, first_id).await,
        Ok(Some(first.clone()))
    );
    assert_eq!(
        store.find_prepared_at(TenantId::generate(), first_id).await,
        Ok(None)
    );
    assert_eq!(first.content_id, first_id);
    assert_eq!(second.content_id, second_id);
    assert_eq!(first.sha256, second.sha256);
    assert_eq!(
        store.read(tenant_id, &first).await.expect("read first"),
        request.bytes
    );
    assert_eq!(
        store.read(tenant_id, &second).await.expect("read second"),
        br#"{"timezone":"Asia/Shanghai"}"#.to_vec()
    );
    let reference_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact.content_refs
         WHERE tenant_id = $1 AND content_hash = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(first.sha256.into_bytes().as_slice())
    .fetch_one(&pool)
    .await
    .expect("count logical references");
    assert_eq!(reference_count, 2);

    let conflicting = store
        .put_at(
            first_id,
            PutContent {
                bytes: b"different immutable bytes".to_vec(),
                ..request
            },
        )
        .await;
    assert_eq!(conflicting, Err(ContentStoreError::IdentityConflict));
    store
        .delete(tenant_id, &first)
        .await
        .expect("tombstone producer content");
    assert_eq!(
        store.find_prepared_at(tenant_id, first_id).await,
        Err(ContentStoreError::NotFound)
    );
    assert!(
        store
            .put_at(
                first_id,
                PutContent {
                    tenant_id,
                    media_type: first.media_type.clone(),
                    bytes: br#"{"timezone":"Asia/Shanghai"}"#.to_vec()
                }
            )
            .await
            .is_err(),
        "tombstone cannot be recreated"
    );
    assert_eq!(
        store.find_prepared_at(tenant_id, first_id).await,
        Err(ContentStoreError::NotFound)
    );
    pool.close().await;
}

#[tokio::test]
async fn staged_object_is_reconciled_after_runtime_crash_boundary() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let blobs = Arc::new(ObjectStoreBlobStore::in_memory());
    let store = PostgresContentStore::new(pool.clone(), blobs.clone(), reference_keys());
    let tenant_id = TenantId::generate();
    let content_id = ContentId::generate();
    let bytes = b"crash-recovery".to_vec();
    let hash = Sha256::digest(&bytes);

    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id, tenant_id, media_type, byte_length, content_hash,
             storage_ref, storage_ref_protected, state, created_at_ms, updated_at_ms
         ) VALUES ($1, $2, 'application/octet-stream', $3::numeric, $4,
                   NULL, NULL, 1, 1, 1)",
    )
    .bind(content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(bytes.len().to_string())
    .bind(hash.into_bytes().as_slice())
    .execute(&pool)
    .await
    .expect("stage metadata");
    assert!(
        store.find_prepared_at(tenant_id, content_id).await.is_err(),
        "staged metadata without verified bytes is not an absent producer"
    );
    assert_eq!(
        store
            .find_prepared_at(TenantId::generate(), content_id)
            .await,
        Ok(None)
    );
    blobs
        .put(PutBlob {
            identity: BlobIdentity {
                tenant_id,
                content_id,
            },
            sha256: hash,
            bytes: bytes.clone(),
        })
        .await
        .expect("simulate object upload before crash");

    let reference = store
        .find_prepared_at(tenant_id, content_id)
        .await
        .expect("reconcile staged upload")
        .expect("original producer identity");
    assert_eq!(
        store.read(tenant_id, &reference).await.expect("read"),
        bytes
    );
    let state: i16 = sqlx::query_scalar(
        "SELECT state FROM artifact.content_refs
         WHERE tenant_id = $1 AND content_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(content_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read state");
    assert_eq!(state, 2);
    pool.close().await;
}

#[tokio::test]
async fn rolling_keyring_reads_old_content_and_seals_new_content_with_the_active_key() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let blobs = Arc::new(ObjectStoreBlobStore::in_memory());
    let tenant_id = TenantId::generate();
    let old_store = PostgresContentStore::new(
        pool.clone(),
        blobs.clone(),
        rotating_reference_keys(
            "runtime.content.old",
            [("runtime.content.old", [17_u8; 32])],
        ),
    );
    let old_bytes = b"written-before-key-rotation".to_vec();
    let old_reference = old_store
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/plain").expect("media type"),
            bytes: old_bytes.clone(),
        })
        .await
        .expect("write with old key");

    let rotating_store = PostgresContentStore::new(
        pool.clone(),
        blobs.clone(),
        rotating_reference_keys(
            "runtime.content.active",
            [
                ("runtime.content.old", [17_u8; 32]),
                ("runtime.content.active", [29_u8; 32]),
            ],
        ),
    );
    assert_eq!(
        rotating_store
            .read(tenant_id, &old_reference)
            .await
            .expect("read old content during rotation"),
        old_bytes
    );

    let new_bytes = b"written-after-key-rotation".to_vec();
    let new_reference = rotating_store
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/plain").expect("media type"),
            bytes: new_bytes.clone(),
        })
        .await
        .expect("write with active key");
    let new_only_store = PostgresContentStore::new(
        pool.clone(),
        blobs,
        rotating_reference_keys(
            "runtime.content.active",
            [("runtime.content.active", [29_u8; 32])],
        ),
    );
    assert_eq!(
        new_only_store
            .read(tenant_id, &new_reference)
            .await
            .expect("read newly sealed content"),
        new_bytes
    );
    assert_eq!(
        old_store.read(tenant_id, &new_reference).await,
        Err(ContentStoreError::Unavailable),
        "new content must be sealed by the new active key"
    );
    pool.close().await;
}
