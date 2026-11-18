use super::*;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::ContentId;

fn reference(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

fn put(
    cache: &VerifiedContentCache,
    tenant: TenantId,
    reference: &ContentRef,
    bytes: &[u8],
) -> Arc<[u8]> {
    let read = cache.begin_read(tenant).expect("read generation");
    cache
        .publish_after_authorization(&read, reference, bytes.to_vec())
        .expect("verified publication")
}

#[test]
fn identical_content_is_shared_only_inside_the_exact_tenant_and_reference() {
    let cache = VerifiedContentCache::default();
    let first = TenantId::generate();
    let second = TenantId::generate();
    let bytes = "完整上下文\n\"tool_call_id\":\"call-1\"".as_bytes();
    let reference = reference(bytes);
    let original = put(&cache, first, &reference, bytes);
    let read = cache.begin_read(first).expect("read");
    let hit = cache
        .get_after_authorization(&read, &reference)
        .expect("lookup")
        .expect("hit");
    assert!(Arc::ptr_eq(&original, &hit));
    assert_eq!(hit.as_ref(), bytes);
    let other = cache.begin_read(second).expect("read");
    assert!(
        cache
            .get_after_authorization(&other, &reference)
            .expect("lookup")
            .is_none()
    );
    let mut changed = reference.clone();
    changed.media_type = MediaType::new("application/json").expect("media type");
    assert!(
        cache
            .get_after_authorization(&read, &changed)
            .expect("lookup")
            .is_none()
    );
    changed = reference.clone();
    changed.sha256 = Sha256::digest(b"different");
    assert!(
        cache
            .get_after_authorization(&read, &changed)
            .expect("lookup")
            .is_none()
    );
    changed = reference.clone();
    changed.content_id = ContentId::generate();
    assert!(
        cache
            .get_after_authorization(&read, &changed)
            .expect("lookup")
            .is_none()
    );
}

#[test]
fn deletion_fences_a_delayed_read_even_if_no_entry_existed_yet() {
    let cache = VerifiedContentCache::default();
    let tenant = TenantId::generate();
    let bytes = b"read started before deletion";
    let reference = reference(bytes);
    let pending = cache
        .begin_read(tenant)
        .expect("generation before metadata");
    cache.invalidate_tenant(tenant).expect("erasure commit");
    assert_eq!(
        cache.publish_after_authorization(&pending, &reference, bytes.to_vec()),
        Err(ContentCacheError::Invalidated)
    );
    assert_eq!(cache.stats().expect("stats").entries, 0);
}

#[test]
fn deletion_rejects_a_previous_hit_and_preserves_other_authorized_tenants() {
    let cache = VerifiedContentCache::default();
    let tenant = TenantId::generate();
    let other = TenantId::generate();
    let reference = reference(b"bytes");
    put(&cache, tenant, &reference, b"bytes");
    put(&cache, other, &reference, b"bytes");
    let pending = cache.begin_read(tenant).expect("old authorization view");
    cache.invalidate_tenant(tenant).expect("invalidate");
    assert_eq!(
        cache.get_after_authorization(&pending, &reference),
        Err(ContentCacheError::Invalidated)
    );
    let fresh = cache.begin_read(tenant).expect("new view");
    assert!(
        cache
            .get_after_authorization(&fresh, &reference)
            .expect("lookup")
            .is_none()
    );
    let fresh_other = cache.begin_read(other).expect("other new view");
    assert!(
        cache
            .get_after_authorization(&fresh_other, &reference)
            .expect("lookup")
            .is_some()
    );
    assert_eq!(cache.stats().expect("stats").entries, 1);
}

#[test]
fn a_read_generation_cannot_be_reused_in_another_cache() {
    let first = VerifiedContentCache::default();
    let second = VerifiedContentCache::default();
    let read = first.begin_read(TenantId::generate()).expect("read");
    assert_eq!(
        second.publish_after_authorization(&read, &reference(b"x"), b"x".to_vec()),
        Err(ContentCacheError::Invalidated)
    );
}

#[test]
fn corrupt_bytes_are_never_published() {
    let cache = VerifiedContentCache::default();
    let read = cache.begin_read(TenantId::generate()).expect("read");
    let reference = reference(b"good");
    assert_eq!(
        cache.publish_after_authorization(&read, &reference, b"evil".to_vec()),
        Err(ContentCacheError::Content(ContentStoreError::HashMismatch))
    );
    assert_eq!(
        cache.publish_after_authorization(&read, &reference, Vec::new()),
        Err(ContentCacheError::Content(
            ContentStoreError::LengthMismatch
        ))
    );
    assert_eq!(cache.stats().expect("stats").entries, 0);
}

#[test]
fn pressure_evicts_least_recently_used_without_changing_returned_bytes() {
    let cache = VerifiedContentCache::new(2 * (ENTRY_OVERHEAD_BYTES + 20 + 1), 100);
    let tenant = TenantId::generate();
    let one = reference(b"1");
    let two = reference(b"2");
    let three = reference(b"3");
    let retained_by_caller = put(&cache, tenant, &two, b"2");
    put(&cache, tenant, &one, b"1");
    let read = cache.begin_read(tenant).expect("read");
    assert!(
        cache
            .get_after_authorization(&read, &two)
            .expect("hit")
            .is_some()
    );
    put(&cache, tenant, &three, b"3");
    assert!(
        cache
            .get_after_authorization(&read, &one)
            .expect("lookup")
            .is_none()
    );
    assert_eq!(retained_by_caller.as_ref(), b"2");
    let stats = cache.stats().expect("stats");
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.evictions, 1);
    assert!(stats.retained_bytes <= cache.max_bytes);
}

#[test]
fn empty_objects_are_bounded_by_metadata_and_entry_limits() {
    let cache = VerifiedContentCache::new(10_000, 3);
    let tenant = TenantId::generate();
    for _ in 0..100 {
        put(&cache, tenant, &reference(b""), b"");
    }
    let stats = cache.stats().expect("stats");
    assert_eq!(stats.entries, 3);
    assert!(stats.retained_bytes > 0);
    assert_eq!(stats.evictions, 97);
}

#[test]
fn disabled_or_oversized_cache_reads_still_return_the_whole_object() {
    let tenant = TenantId::generate();
    let bytes = vec![b'x'; 65_537];
    let reference = reference(&bytes);
    for cache in [
        VerifiedContentCache::new(0, 10),
        VerifiedContentCache::new(1024, 0),
        VerifiedContentCache::new(1024, 10),
    ] {
        assert_eq!(put(&cache, tenant, &reference, &bytes).as_ref(), bytes);
        assert_eq!(cache.stats().expect("stats").entries, 0);
    }
}

#[test]
fn clear_fences_reads_and_diagnostics_never_print_bytes_or_identity() {
    let cache = VerifiedContentCache::default();
    let tenant = TenantId::generate();
    let reference = reference(b"secret-canary-content");
    put(&cache, tenant, &reference, b"secret-canary-content");
    let read = cache.begin_read(tenant).expect("read");
    let diagnostics = format!("{cache:?} {read:?}");
    assert!(!diagnostics.contains("secret-canary-content"));
    assert!(!diagnostics.contains(&tenant.to_string()));
    assert!(!diagnostics.contains(&reference.sha256.to_string()));
    cache.clear().expect("clear");
    assert_eq!(
        cache.get_after_authorization(&read, &reference),
        Err(ContentCacheError::Invalidated)
    );
    assert_eq!(cache.stats().expect("stats").retained_bytes, 0);
}

#[derive(Debug, Eq, PartialEq)]
struct Derived(Vec<u8>);
impl RetainedContentMaterial for Derived {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.0.capacity())
    }
}
#[derive(Debug, Eq, PartialEq)]
struct DifferentType(Vec<u8>);
impl RetainedContentMaterial for DifferentType {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.0.capacity())
    }
}

#[test]
fn raw_and_derived_material_share_one_budget_and_one_lru() {
    let cache = VerifiedContentCache::new(1400, 20);
    let tenant = TenantId::generate();
    let raw = reference(&[1; 100]);
    put(&cache, tenant, &raw, &[1; 100]);
    let read = cache.begin_read(tenant).expect("begin tenant cache read");
    let digest = Sha256::digest(b"all material dependencies");
    cache
        .publish_material_after_authorization(
            &read,
            "test.projection.v1",
            digest,
            Derived(vec![2; 400]),
        )
        .expect("publish authorized material");
    assert!(
        cache
            .get_after_authorization(&read, &raw)
            .expect("authorized content lookup")
            .is_none()
    );
    assert!(
        cache
            .material_after_authorization::<Derived>(&read, "test.projection.v1", digest)
            .expect("authorized material lookup")
            .is_some()
    );
    assert_eq!(cache.stats().expect("cache statistics").entries, 1);
    let next = reference(&[3; 300]);
    put(&cache, tenant, &next, &[3; 300]);
    assert!(
        cache
            .material_after_authorization::<Derived>(&read, "test.projection.v1", digest)
            .expect("authorized material lookup")
            .is_none()
    );
    assert!(cache.stats().expect("cache statistics").retained_bytes <= 1400);
}

#[test]
fn derived_keys_bind_tenant_type_purpose_and_all_dependencies() {
    let cache = VerifiedContentCache::default();
    let tenant = TenantId::generate();
    let read = cache.begin_read(tenant).expect("begin tenant cache read");
    let digest = Sha256::digest(b"recipe1");
    let published = cache
        .publish_material_after_authorization(&read, "test.memo.v1", digest, Derived(vec![42]))
        .expect("publish authorized material");
    let hit = cache
        .material_after_authorization::<Derived>(&read, "test.memo.v1", digest)
        .expect("authorized material lookup")
        .expect("published material is cached");
    assert!(Arc::ptr_eq(&published, &hit));
    assert!(
        cache
            .material_after_authorization::<DifferentType>(&read, "test.memo.v1", digest)
            .expect("authorized material lookup")
            .is_none()
    );
    assert!(
        cache
            .material_after_authorization::<Derived>(&read, "test.memo.v2", digest)
            .expect("authorized material lookup")
            .is_none()
    );
    assert!(
        cache
            .material_after_authorization::<Derived>(
                &read,
                "test.memo.v1",
                Sha256::digest(b"recipe2")
            )
            .expect("authorized material lookup")
            .is_none()
    );
    let other = cache
        .begin_read(TenantId::generate())
        .expect("begin tenant cache read");
    assert!(
        cache
            .material_after_authorization::<Derived>(&other, "test.memo.v1", digest)
            .expect("authorized material lookup")
            .is_none()
    );
}

#[test]
fn deletion_clears_decoded_material_and_fences_late_derived_publication() {
    let cache = VerifiedContentCache::default();
    let tenant = TenantId::generate();
    let read = cache.begin_read(tenant).expect("begin tenant cache read");
    let digest = Sha256::digest(b"recipe");
    cache
        .publish_material_after_authorization(&read, "test.memo.v1", digest, Derived(vec![42]))
        .expect("publish authorized material");
    cache
        .invalidate_tenant(tenant)
        .expect("invalidate tenant cache");
    assert_eq!(
        cache.material_after_authorization::<Derived>(&read, "test.memo.v1", digest),
        Err(ContentCacheError::Invalidated)
    );
    assert_eq!(
        cache.publish_material_after_authorization(
            &read,
            "test.memo.v1",
            digest,
            Derived(vec![42])
        ),
        Err(ContentCacheError::Invalidated)
    );
    let current = cache.begin_read(tenant).expect("begin tenant cache read");
    assert!(
        cache
            .material_after_authorization::<Derived>(&current, "test.memo.v1", digest)
            .expect("authorized material lookup")
            .is_none()
    );
    assert_eq!(cache.stats().expect("cache statistics").retained_bytes, 0);
}

#[test]
fn oversized_derived_material_is_returned_intact_without_retention() {
    let cache = VerifiedContentCache::new(600, 10);
    let read = cache
        .begin_read(TenantId::generate())
        .expect("begin tenant cache read");
    let digest = Sha256::digest(b"large");
    let value = cache
        .publish_material_after_authorization(&read, "test.memo.v1", digest, Derived(vec![9; 1024]))
        .expect("publish authorized material");
    assert_eq!(value.0, vec![9; 1024]);
    assert_eq!(cache.stats().expect("cache statistics").entries, 0);
}

#[test]
fn public_release_material_cannot_collide_with_private_tenant_material() {
    let cache = VerifiedContentCache::default();
    let tenant = TenantId::generate();
    let digest = Sha256::digest(b"exact-source-release-index-page");
    let read = cache.begin_read(tenant).expect("tenant read");
    cache
        .publish_material_after_authorization(&read, "test.same-purpose", digest, Derived(vec![1]))
        .expect("private");
    let generation = cache.begin_generation().expect("public generation");
    let public = cache
        .publish_public_release_material(&generation, "test.same-purpose", digest, Derived(vec![2]))
        .expect("public");
    assert_eq!(
        cache
            .material_after_authorization::<Derived>(&read, "test.same-purpose", digest)
            .expect("private lookup")
            .expect("private hit")
            .0,
        vec![1]
    );
    assert!(Arc::ptr_eq(
        &public,
        &cache
            .public_release_material::<Derived>("test.same-purpose", digest)
            .expect("public lookup")
            .expect("public hit")
    ));
    cache.invalidate_tenant(tenant).expect("tenant erasure");
    assert!(
        cache
            .material_after_authorization::<Derived>(&read, "test.same-purpose", digest)
            .is_err()
    );
    assert!(
        cache
            .public_release_material::<Derived>("test.same-purpose", digest)
            .expect("public is not user data")
            .is_some()
    );
    assert!(
        cache
            .publish_public_release_material(
                &generation,
                "test.same-purpose",
                Sha256::digest(b"late"),
                Derived(vec![3])
            )
            .is_err()
    );
}

#[test]
fn public_catalogues_share_the_original_cache_budget_and_exact_dependency_key() {
    let cache = VerifiedContentCache::new(1400, 10);
    let generation = cache.begin_generation().expect("generation");
    let first = Sha256::digest(b"source1/release1/index1/page1");
    cache
        .publish_public_release_material(&generation, "test.page", first, Derived(vec![1; 400]))
        .expect("first");
    assert!(
        cache
            .public_release_material::<Derived>(
                "test.page",
                Sha256::digest(b"source1/release2/index1/page1")
            )
            .expect("exact lookup")
            .is_none()
    );
    assert!(
        cache
            .public_release_material::<DifferentType>("test.page", first)
            .expect("typed lookup")
            .is_none()
    );
    cache
        .publish_public_release_material(
            &generation,
            "test.page",
            Sha256::digest(b"second"),
            Derived(vec![2; 400]),
        )
        .expect("second");
    assert!(
        cache
            .public_release_material::<Derived>("test.page", first)
            .expect("evicted")
            .is_none()
    );
    assert!(cache.stats().expect("stats").retained_bytes <= 1400);
    let oversized = cache
        .publish_public_release_material(
            &generation,
            "test.page",
            Sha256::digest(b"large"),
            Derived(vec![3; 2000]),
        )
        .expect("intact uncached");
    assert_eq!(oversized.0.len(), 2000);
    assert!(cache.stats().expect("stats").retained_bytes <= 1400);
}
