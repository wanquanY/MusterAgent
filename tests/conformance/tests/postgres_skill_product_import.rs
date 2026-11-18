#![cfg(test)]

//! Durable uploaded archive admission through the original Skill/Content owners on real PG.
use musteragent_content_ref::MediaType;
use musteragent_identity::{
    Actor, CommandId, DottedKey, PrincipalId, PrincipalKind, TenantId, UtcTimestamp,
};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{PostgresContentStore, PostgresSkillLifecycleStore};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_skill::SkillLifecycleStore;
use musteragent_runtime_skill::*;
use musteragent_skill::{SkillTrust, Surface};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use std::sync::Arc;

fn at(value: i64) -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(value)
}

struct Fixture {
    _pg: IsolatedPostgres,
    pool: sqlx::PgPool,
    store: Arc<PostgresSkillLifecycleStore>,
    content: Arc<PostgresContentStore>,
    app: Arc<SkillProductApplication>,
    request: ImportProductSkill,
}
impl Fixture {
    async fn new() -> Self {
        let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
        let pool = pg.pool().await.unwrap();
        musteragent_conformance::migrate(&pool).await.unwrap();
        let key = DottedKey::new("test.skill.import.command").unwrap();
        let content = Arc::new(PostgresContentStore::new(
            pool.clone(),
            Arc::new(ObjectStoreBlobStore::in_memory()),
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [31; 32])]).unwrap()),
        ));
        let store = Arc::new(PostgresSkillLifecycleStore::new(pool.clone()));
        let app = Arc::new(
            SkillProductApplication::new(store.clone(), store.clone(), content.clone())
                .with_import_store(store.clone()),
        );
        let actor = Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let archive = build_skill_source_archive(vec![SkillPackageSourceFile {
            path:"SKILL.md".into(), bytes:b"---\nname: fixture-notes\ndescription: Review supplied notes.\n---\n# Notes\nUse supplied evidence.\n".to_vec(),
        }]).unwrap();
        let archive = content
            .put(PutContent {
                tenant_id: actor.tenant_id,
                media_type: MediaType::new("application/zip").unwrap(),
                bytes: archive,
            })
            .await
            .unwrap();
        let request = ImportProductSkill {
            account_installation: None,
            actor,
            command_id: CommandId::generate(),
            origin: ProductSkillImportOrigin::UploadedArchive,
            surface: Surface::Web,
            disposition: ProductSkillImportDisposition::Install,
            archive,
        };
        Self {
            _pg: pg,
            pool,
            store,
            content,
            app,
            request,
        }
    }
    async fn seed(&self) -> ProductSkillImportRecord {
        self.app
            .import_product_skill(self.request.clone(), at(100))
            .await
            .unwrap()
    }
    async fn prepare(
        &self,
        request: ImportProductSkill,
        baseline: &ProductSkillImportRecord,
        time: i64,
    ) -> ProductSkillImportRecord {
        sqlx::raw_sql("CREATE FUNCTION skill.reject_import_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected post-admission crash'; END $$; CREATE TRIGGER reject_import_test BEFORE INSERT ON skill.skill_versions FOR EACH ROW EXECUTE FUNCTION skill.reject_import_test();").execute(&self.pool).await.unwrap();
        let result = self
            .app
            .import_product_skill(request.clone(), at(time))
            .await;
        sqlx::raw_sql("DROP TRIGGER reject_import_test ON skill.skill_versions; DROP FUNCTION skill.reject_import_test();").execute(&self.pool).await.unwrap();
        assert!(
            result.is_err(),
            "injected version failure must prevent completion"
        );
        let accepted = self
            .store
            .observe(request.actor, request.command_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(accepted.status, ProductSkillImportStatus::Accepted);
        assert_eq!(
            accepted.prepared.identity.skill_key,
            baseline.prepared.identity.skill_key
        );
        accepted
    }
    async fn revise(&self, request: &mut ImportProductSkill, body: &str) {
        let bytes = build_skill_source_archive(vec![SkillPackageSourceFile {
            path: "SKILL.md".into(),
            bytes: format!("---\nname: fixture-notes\ndescription: Review supplied notes.\n---\n# Notes\n{body}\n").into_bytes(),
        }]).unwrap();
        request.archive = self
            .content
            .put(PutContent {
                tenant_id: request.actor.tenant_id,
                media_type: MediaType::new("application/zip").unwrap(),
                bytes,
            })
            .await
            .unwrap();
    }
    fn query(&self, limit: u32) -> ProductSkillImportQuery {
        ProductSkillImportQuery {
            actor: self.request.actor,
            target: ProductSkillImportTarget::UploadedArchives,
            surface: Surface::Web,
            limit,
            cursor: None,
        }
    }
}

#[tokio::test]
async fn receipts_pin_real_audited_versions_and_replay_never_reenables_removed_skill() {
    let f = Fixture::new().await;
    let first = f.seed().await;
    assert_eq!(first.status, ProductSkillImportStatus::Completed);
    assert!(first.prepared.created_version);
    let package = f
        .store
        .load_version(f.request.actor.tenant_id, first.prepared.skill_version_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(package.identity.trust, SkillTrust::Local);
    assert!(!package.audit.signature_verified);
    assert!(package.audit.passed_for(SkillTrust::Local));
    let item = f
        .app
        .list_for_surface(f.request.actor.tenant_id, Surface::Web)
        .await
        .unwrap()
        .into_iter()
        .find(|x| x.skill_id == first.prepared.identity.skill_id)
        .unwrap();
    assert_eq!(item.skill_version_id, first.prepared.skill_version_id);
    let inspected = f
        .app
        .inspect(f.request.actor.tenant_id, item.skill_id, Surface::Mobile)
        .await
        .unwrap();
    assert_eq!(inspected.skill.skill_id, item.skill_id);
    assert!(item.marketplace_source.is_none());
    f.app
        .set_availability_for_surface(
            SetInstalledSkillAvailability {
                tenant_id: f.request.actor.tenant_id,
                principal_id: f.request.actor.principal_id,
                skill_id: first.prepared.identity.skill_id,
                expected_pointer_version: first.pointer_version.unwrap(),
                saved: false,
                installed: false,
                enabled: false,
                occurred_at: at(200),
            },
            Surface::Web,
        )
        .await
        .unwrap();
    assert_eq!(
        f.app
            .import_product_skill(f.request.clone(), at(300))
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        f.app
            .resume_product_import(f.request.actor, f.request.command_id, at(301))
            .await
            .unwrap(),
        first
    );
    let pointer = f
        .store
        .load_active(f.request.actor.tenant_id, first.prepared.identity.skill_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!pointer.installed && !pointer.enabled && !pointer.saved);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM skill.skill_versions WHERE skill_id=$1")
            .bind(first.prepared.identity.skill_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn accepted_command_survives_restart_and_concurrent_resume_commits_once() {
    let f = Fixture::new().await;
    let base = f.seed().await;
    let mut request = f.request.clone();
    request.command_id = CommandId::generate();
    f.revise(&mut request, "updated").await;
    let accepted = f.prepare(request.clone(), &base, 200).await;
    assert_eq!(accepted.status, ProductSkillImportStatus::Accepted);
    let restarted_store = Arc::new(PostgresSkillLifecycleStore::new(f.pool.clone()));
    let restarted = Arc::new(
        SkillProductApplication::new(
            restarted_store.clone(),
            restarted_store.clone(),
            f.content.clone(),
        )
        .with_import_store(restarted_store),
    );
    let (a, b) = tokio::join!(
        restarted.resume_product_import(request.actor, request.command_id, at(300)),
        f.app
            .resume_product_import(request.actor, request.command_id, at(300))
    );
    let a = a.unwrap();
    assert_eq!(a, b.unwrap());
    assert_eq!(a.status, ProductSkillImportStatus::Completed);
    assert_eq!(
        a.prepared.skill_version_id,
        accepted.prepared.skill_version_id
    );
    // A changed upload creates a distinct immutable version.
    assert_ne!(a.prepared.skill_version_id, base.prepared.skill_version_id);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM skill.skill_versions WHERE skill_id=$1")
            .bind(base.prepared.identity.skill_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn stale_admission_records_conflict_without_partial_version_or_pointer_writes() {
    let f = Fixture::new().await;
    let base = f.seed().await;
    let mut a = f.request.clone();
    a.command_id = CommandId::generate();
    f.revise(&mut a, "first update").await;
    let mut b = a.clone();
    b.command_id = CommandId::generate();
    f.revise(&mut b, "competing update").await;
    f.prepare(a.clone(), &base, 200).await;
    let losing = f.prepare(b.clone(), &base, 201).await;
    let won = f
        .app
        .resume_product_import(a.actor, a.command_id, at(300))
        .await
        .unwrap();
    let lost = f
        .app
        .resume_product_import(b.actor, b.command_id, at(301))
        .await
        .unwrap();
    assert_eq!(won.status, ProductSkillImportStatus::Completed);
    assert_eq!(lost.status, ProductSkillImportStatus::Conflict);
    assert!(
        f.store
            .load_version(b.actor.tenant_id, losing.prepared.skill_version_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.app
            .resume_product_import(b.actor, b.command_id, at(302))
            .await
            .unwrap(),
        lost
    );
    assert_eq!(
        f.store
            .load_active(a.actor.tenant_id, base.prepared.identity.skill_id)
            .await
            .unwrap()
            .unwrap()
            .skill_version_id,
        won.prepared.skill_version_id
    );
    let partial: i64 =
        sqlx::query_scalar("SELECT count(*) FROM skill.package_audits WHERE skill_version_id=$1")
            .bind(losing.prepared.skill_version_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(partial, 0);
}

#[tokio::test]
async fn recovery_is_bounded_actor_scoped_and_cursor_cannot_change_query_scope() {
    let f = Fixture::new().await;
    let base = f.seed().await;
    let mut second = f.request.clone();
    second.command_id = CommandId::generate();
    f.revise(&mut second, "second").await;
    f.prepare(second.clone(), &base, 200).await;
    let first = f.app.list_product_imports(f.query(1)).await.unwrap();
    assert_eq!(first.records.len(), 1);
    assert_eq!(
        first.records[0].prepared.request.command_id,
        second.command_id
    );
    let mut query = f.query(1);
    query.cursor = first.next_cursor;
    let page = f.app.list_product_imports(query.clone()).await.unwrap();
    assert_eq!(page.records, vec![base]);
    assert!(page.next_cursor.is_none());
    query.actor.principal_id = PrincipalId::generate();
    assert!(matches!(
        f.app.list_product_imports(query).await,
        Err(ProductSkillImportError::InvalidRequest)
    ));
    let mut query = f.query(50);
    query.actor.principal_id = PrincipalId::generate();
    assert!(
        f.app
            .list_product_imports(query.clone())
            .await
            .unwrap()
            .records
            .is_empty()
    );
    assert!(
        f.app
            .observe_product_import(query.actor, f.request.command_id)
            .await
            .unwrap()
            .is_none()
    );
    let mut query = f.query(50);
    query.actor.tenant_id = TenantId::generate();
    assert!(
        f.app
            .list_product_imports(query)
            .await
            .unwrap()
            .records
            .is_empty()
    );
    let mut changed = f.request.clone();
    f.revise(&mut changed, "conflicting command").await;
    assert!(matches!(
        f.app.import_product_skill(changed, at(300)).await,
        Err(ProductSkillImportError::CommandConflict)
    ));
}

#[tokio::test]
async fn invalid_archive_has_no_admission_and_conflicting_source_cannot_capture_identity() {
    let f = Fixture::new().await;
    let mut bad = f.request.clone();
    bad.archive = f
        .content
        .put(PutContent {
            tenant_id: bad.actor.tenant_id,
            media_type: MediaType::new("application/zip").unwrap(),
            bytes: b"not a zip".to_vec(),
        })
        .await
        .unwrap();
    assert!(f.app.import_product_skill(bad, at(100)).await.is_err());
    assert!(
        f.app
            .observe_product_import(f.request.actor, f.request.command_id)
            .await
            .unwrap()
            .is_none()
    );
    f.seed().await;
    let mut other = f.request.clone();
    other.command_id = CommandId::generate();
    other.actor.principal_id = PrincipalId::generate();
    assert!(matches!(
        f.app.import_product_skill(other.clone(), at(200)).await,
        Err(ProductSkillImportError::IdentityConflict)
    ));
    assert!(
        f.app
            .observe_product_import(other.actor, other.command_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn uploaded_archives_use_original_custom_identity_and_cannot_claim_market_provenance() {
    let mut f = Fixture::new().await;
    f.request.origin = ProductSkillImportOrigin::UploadedArchive;
    let result = f.seed().await;
    assert_eq!(
        result.prepared.identity.provenance,
        musteragent_skill::SkillProvenance::Custom {
            created_by: f.request.actor.principal_id
        }
    );
    assert_eq!(result.prepared.identity.trust, SkillTrust::Local);
    let item = f
        .app
        .inspect(
            f.request.actor.tenant_id,
            result.prepared.identity.skill_id,
            Surface::Web,
        )
        .await
        .unwrap();
    assert!(item.skill.marketplace_source.is_none());
    let wire = musteragent_wire::skill_product_import_codec::response(result.clone()).unwrap();
    let operation = wire.operation.unwrap();
    assert!(operation.uploaded_archive);
    assert_eq!(
        operation.uploaded_archive_sha256.unwrap().value,
        f.request.archive.sha256.into_bytes()
    );
    assert!(operation.market_skill_id.is_empty() && operation.market_publisher_id.is_empty());
    let mut attacker = f.request.clone();
    attacker.command_id = CommandId::generate();
    attacker.actor.principal_id = PrincipalId::generate();
    assert!(matches!(
        f.app.import_product_skill(attacker, at(200)).await,
        Err(ProductSkillImportError::IdentityConflict)
    ));
    let mut market = f.request.clone();
    market.command_id = CommandId::generate();
    market.origin = ProductSkillImportOrigin::Marketplace(ProductSkillMarketplaceSource {
        market_skill_id: uuid::Uuid::now_v7(),
        market_version_id: uuid::Uuid::now_v7(),
        market_publisher_id: uuid::Uuid::now_v7(),
    });
    assert!(matches!(
        f.app.import_product_skill(market, at(200)).await,
        Err(ProductSkillImportError::IdentityConflict)
    ));
    let query = ProductSkillImportQuery {
        actor: f.request.actor,
        target: ProductSkillImportTarget::UploadedArchives,
        surface: Surface::Web,
        limit: 10,
        cursor: None,
    };
    let page = f.app.list_product_imports(query.clone()).await.unwrap();
    assert_eq!(page.records, vec![result]);
    let mut wrong_actor = query;
    wrong_actor.actor.principal_id = PrincipalId::generate();
    assert!(
        f.app
            .list_product_imports(wrong_actor)
            .await
            .unwrap()
            .records
            .is_empty()
    );
}

#[tokio::test]
async fn uploaded_archive_reuses_identical_content_and_completed_replay_cannot_reenable() {
    let mut f = Fixture::new().await;
    f.request.origin = ProductSkillImportOrigin::UploadedArchive;
    let first = f.seed().await;
    let mut retry = f.request.clone();
    retry.command_id = CommandId::generate();
    let second = f
        .app
        .import_product_skill(retry.clone(), at(200))
        .await
        .unwrap();
    assert!(!second.prepared.created_version);
    assert_eq!(
        first.prepared.skill_version_id,
        second.prepared.skill_version_id
    );
    f.app
        .set_availability_for_surface(
            SetInstalledSkillAvailability {
                tenant_id: f.request.actor.tenant_id,
                principal_id: f.request.actor.principal_id,
                skill_id: first.prepared.identity.skill_id,
                expected_pointer_version: second.pointer_version.unwrap(),
                saved: false,
                installed: false,
                enabled: false,
                occurred_at: at(300),
            },
            Surface::Web,
        )
        .await
        .unwrap();
    assert_eq!(
        f.app.import_product_skill(retry, at(400)).await.unwrap(),
        second
    );
    let pointer = f
        .store
        .load_active(f.request.actor.tenant_id, first.prepared.identity.skill_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!pointer.saved && !pointer.installed && !pointer.enabled);
}

#[tokio::test]
async fn uploaded_archive_update_preserves_previous_immutable_version_and_import_receipt() {
    let mut f = Fixture::new().await;
    f.request.origin = ProductSkillImportOrigin::UploadedArchive;
    let first = f.seed().await;
    let archive = build_skill_source_archive(vec![SkillPackageSourceFile {
        path: "SKILL.md".into(), bytes: b"---\nname: fixture-notes\ndescription: Review supplied notes.\n---\n# Revised notes\nCite each source.\n".to_vec(),
    }, SkillPackageSourceFile { path: "references/guide.md".into(), bytes: b"Only use supplied sources.".to_vec() }]).unwrap();
    let mut update = f.request.clone();
    update.command_id = CommandId::generate();
    update.archive = f
        .content
        .put(PutContent {
            tenant_id: update.actor.tenant_id,
            media_type: MediaType::new("application/zip").unwrap(),
            bytes: archive,
        })
        .await
        .unwrap();
    let revised = f.app.import_product_skill(update, at(200)).await.unwrap();
    assert!(revised.prepared.created_version);
    assert_eq!(revised.prepared.version.get(), 2);
    assert_eq!(
        revised.prepared.parent_version_id,
        Some(first.prepared.skill_version_id)
    );
    assert_eq!(revised.prepared.identity, first.prepared.identity);
    assert_eq!(
        f.app
            .observe_product_import(f.request.actor, f.request.command_id)
            .await
            .unwrap(),
        Some(first.clone())
    );
    let current = f
        .app
        .inspect(
            f.request.actor.tenant_id,
            first.prepared.identity.skill_id,
            Surface::Web,
        )
        .await
        .unwrap();
    assert!(
        current
            .resources
            .iter()
            .any(|r| r.path.as_str() == "references/guide.md")
    );
    assert!(
        f.store
            .load_version(f.request.actor.tenant_id, first.prepared.skill_version_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn uploaded_import_wire_rejects_mixed_or_missing_provenance() {
    use musteragent_wire::{skill_product_import_codec as codec, wire};
    let mut f = Fixture::new().await;
    f.request.origin = ProductSkillImportOrigin::UploadedArchive;
    let valid = codec::encode_request(&f.request);
    assert!(codec::request(valid.clone()).is_ok());
    for field in ["skill", "version", "publisher"] {
        let mut mixed = valid.clone();
        let id = uuid::Uuid::now_v7().to_string();
        match field {
            "skill" => mixed.market_skill_id = id,
            "version" => mixed.market_version_id = id,
            _ => mixed.market_publisher_id = id,
        }
        assert!(codec::request(mixed).is_err());
    }
    let mut missing = valid;
    missing.uploaded_archive = false;
    assert!(codec::request(missing).is_err());
    let valid_query = wire::ListProductSkillImportsRequest {
        actor: codec::encode_request(&f.request).actor,
        uploaded_archives: true,
        surface: wire::SkillSurface::Web.into(),
        limit: 20,
        ..Default::default()
    };
    assert!(codec::query(valid_query.clone()).is_ok());
    let mut mixed = valid_query.clone();
    mixed.market_skill_id = uuid::Uuid::now_v7().to_string();
    assert!(codec::query(mixed).is_err());
    let mut mixed = valid_query.clone();
    mixed.market_version_id = Some(uuid::Uuid::now_v7().to_string());
    assert!(codec::query(mixed).is_err());
    let mut missing = valid_query;
    missing.uploaded_archives = false;
    assert!(codec::query(missing).is_err());
}
