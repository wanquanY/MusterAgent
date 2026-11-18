use std::sync::Mutex;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{ContentId, VersionedKey};
use musteragent_skill::{builtin_skill_release_entry, retained_builtin_skill_catalogs};

use super::*;

#[derive(Default)]
struct ImmutableContent {
    objects: Mutex<BTreeMap<ContentId, (ContentRef, Vec<u8>)>>,
    writes: Mutex<Vec<(TenantId, ContentId)>>,
}

impl ContentStore for ImmutableContent {
    fn put(
        &self,
        _request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { panic!("release material must use its exact published identity") })
    }

    fn put_runtime_release_at(
        &self,
        content_id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            let reference = ContentRef {
                content_id,
                media_type: request.media_type,
                byte_length: request.bytes.len() as u64,
                sha256: Sha256::digest(&request.bytes),
            };
            self.writes
                .lock()
                .expect("valid test fixture")
                .push((request.tenant_id, content_id));
            let mut objects = self.objects.lock().expect("fixture mutex is not poisoned");
            if let Some((stored, bytes)) = objects.get(&content_id) {
                if stored != &reference || bytes != &request.bytes {
                    return Err(ContentStoreError::IdentityConflict);
                }
            } else {
                objects.insert(content_id, (reference.clone(), request.bytes));
            }
            Ok(reference)
        })
    }

    fn read<'a>(
        &'a self,
        _tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            let objects = self.objects.lock().expect("fixture mutex is not poisoned");
            let (reference, bytes) = objects
                .get(&content.content_id)
                .ok_or(ContentStoreError::NotFound)?;
            if reference != content {
                return Err(ContentStoreError::HashMismatch);
            }
            // Deliberately return raw bytes so the application's verify boundary
            // is tested independently of a storage adapter's integrity checks.
            Ok(bytes.clone())
        })
    }
}

fn original(reference: &str) -> ContentRef {
    builtin_skill_release_entry(&VersionedKey::new(reference).expect("valid test fixture"))
        .expect("valid test fixture")
        .expect("valid test fixture")
        .definition
        .instructions_ref
}

fn seed(store: &ImmutableContent, reference: ContentRef) {
    let bytes = builtin_skill_instructions(&reference)
        .expect("valid test fixture")
        .as_bytes()
        .to_vec();
    store
        .objects
        .lock()
        .expect("valid test fixture")
        .insert(reference.content_id, (reference, bytes));
}

#[tokio::test]
async fn selected_release_material_reconciles_idempotently_without_changing_admission() {
    let active = builtin_skill_catalog_entries().expect("valid test fixture");
    let material = verified_official_catalog_entries().expect("valid test fixture");
    let store = Arc::new(ImmutableContent::default());
    let application = OfficialSkillCatalogMaterialApplication::new(store.clone());
    assert!(application.verify_global().await.is_err());
    assert!(
        store
            .writes
            .lock()
            .expect("fixture mutex is not poisoned")
            .is_empty()
    );
    for _ in 0..2 {
        assert_eq!(
            application
                .reconcile_global()
                .await
                .expect("valid test fixture"),
            material.len()
        );
        application
            .verify_global()
            .await
            .expect("valid test fixture");
    }
    assert_eq!(
        store
            .objects
            .lock()
            .expect("fixture mutex is not poisoned")
            .len(),
        material.len()
    );
    let writes = store.writes.lock().expect("fixture mutex is not poisoned");
    assert_eq!(writes.len(), material.len() * 2);
    assert!(
        writes.iter().all(|(tenant, _)| *tenant
            == official_catalog_scope_tenant_id().expect("valid test fixture"))
    );
    for entry in &material {
        assert_eq!(
            writes
                .iter()
                .filter(|(_, id)| *id == entry.definition.instructions_ref.content_id)
                .count(),
            2
        );
    }
    assert_eq!(
        active,
        builtin_skill_catalog_entries().expect("valid test fixture")
    );
}

#[tokio::test]
async fn clean_kernel_publication_contains_only_declared_material() {
    let store = Arc::new(ImmutableContent::default());
    let application = OfficialSkillCatalogMaterialApplication::new(store.clone());
    let active = builtin_skill_catalog_entries().expect("valid test fixture");
    assert_eq!(active.len(), 7);
    assert_eq!(
        application
            .reconcile_global()
            .await
            .expect("valid test fixture"),
        7
    );
    application
        .verify_global()
        .await
        .expect("valid test fixture");
    assert!(
        retained_builtin_skill_catalogs()
            .expect("valid test fixture")
            .is_empty()
    );
    let objects = store.objects.lock().expect("fixture mutex is not poisoned");
    assert_eq!(objects.len(), 7);
    for entry in active {
        let reference = entry.definition.instructions_ref;
        assert_eq!(
            objects[&reference.content_id].1,
            builtin_skill_instructions(&reference)
                .expect("valid test fixture")
                .as_bytes()
        );
    }
    assert!(
        builtin_skill_release_entry(
            &VersionedKey::new("assistant-base@2").expect("valid test fixture")
        )
        .expect("valid test fixture")
        .is_none()
    );
}

#[tokio::test]
async fn readiness_requires_complete_material_and_reconcile_repairs_only_missing_objects() {
    let store = Arc::new(ImmutableContent::default());
    for entry in builtin_skill_catalog_entries()
        .expect("valid test fixture")
        .into_iter()
        .skip(1)
    {
        seed(&store, entry.definition.instructions_ref);
    }
    let before = store
        .objects
        .lock()
        .expect("fixture mutex is not poisoned")
        .clone();
    let application = OfficialSkillCatalogMaterialApplication::new(store.clone());
    assert!(matches!(
        application.verify_global().await,
        Err(OfficialSkillCatalogMaterialError::Content {
            source: ContentStoreError::NotFound,
            ..
        })
    ));
    assert!(
        store
            .writes
            .lock()
            .expect("fixture mutex is not poisoned")
            .is_empty()
    );
    application
        .reconcile_global()
        .await
        .expect("valid test fixture");
    application
        .verify_global()
        .await
        .expect("valid test fixture");
    for (id, object) in before {
        assert_eq!(
            store
                .objects
                .lock()
                .expect("fixture mutex is not poisoned")
                .get(&id),
            Some(&object)
        );
    }
    let reference = original("project-files@1");
    store
        .objects
        .lock()
        .expect("fixture mutex is not poisoned")
        .remove(&reference.content_id);
    let writes = store
        .writes
        .lock()
        .expect("fixture mutex is not poisoned")
        .len();
    assert!(matches!(application.verify_global().await,
        Err(OfficialSkillCatalogMaterialError::Content { source: ContentStoreError::NotFound, content_id, .. })
            if content_id == reference.content_id.to_string()));
    assert_eq!(
        store
            .writes
            .lock()
            .expect("fixture mutex is not poisoned")
            .len(),
        writes
    );
}

#[tokio::test]
async fn published_identity_conflict_is_reported_without_overwriting_content() {
    let store = Arc::new(ImmutableContent::default());
    let reference = original("assistant-base@1");
    let corrupt = b"different immutable content".to_vec();
    store
        .objects
        .lock()
        .expect("valid test fixture")
        .insert(reference.content_id, (reference.clone(), corrupt.clone()));
    let application = OfficialSkillCatalogMaterialApplication::new(store.clone());
    assert!(matches!(application.reconcile_global().await,
        Err(OfficialSkillCatalogMaterialError::Content { source: ContentStoreError::IdentityConflict, content_id, .. })
            if content_id == reference.content_id.to_string()));
    assert_eq!(
        store
            .objects
            .lock()
            .expect("valid test fixture")
            .get(&reference.content_id)
            .expect("valid test fixture")
            .1,
        corrupt
    );
}

#[tokio::test]
async fn readiness_rejects_published_bytes_with_a_valid_declared_reference() {
    let store = Arc::new(ImmutableContent::default());
    let application = OfficialSkillCatalogMaterialApplication::new(store.clone());
    application
        .reconcile_global()
        .await
        .expect("valid test fixture");
    let reference = original("assistant-base@1");
    store
        .objects
        .lock()
        .expect("valid test fixture")
        .get_mut(&reference.content_id)
        .expect("valid test fixture")
        .1 = b"tampered".to_vec();
    let writes = store
        .writes
        .lock()
        .expect("fixture mutex is not poisoned")
        .len();
    assert!(matches!(application.verify_global().await,
        Err(OfficialSkillCatalogMaterialError::Conflict { expected_content_id, .. })
            if expected_content_id.as_ref() == reference.content_id.to_string()));
    assert_eq!(
        store
            .writes
            .lock()
            .expect("fixture mutex is not poisoned")
            .len(),
        writes
    );
}
