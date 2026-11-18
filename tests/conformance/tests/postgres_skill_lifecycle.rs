use std::collections::BTreeSet;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ContentId, PrincipalId, SkillId, SkillVersionId, TenantId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_postgres::PostgresSkillLifecycleStore;
use musteragent_runtime_skill::{
    ActivateSkillVersion, ActiveSkillCatalogSource, ActiveSkillVersionGuard,
    CommitVerifiedSkillPackage, SkillLifecycleApplication, SkillLifecycleApplicationError,
    SkillLifecycleStoreError,
};
use musteragent_skill::{
    InstalledSkillSource, PackageAuditReport, QuarantineDisposition, SkillIdentity, SkillKey,
    SkillProvenance, SkillResourceDescriptor, SkillResourcePath, SkillResourceRole,
    SkillSourceFormat, SkillTrust, SkillVersion, Surface, VerifiedSkillPackage,
};
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;

#[tokio::test]
async fn real_unsigned_imports_preserve_original_trust_and_verified_audits_stay_signed() {
    use musteragent_conformance::MemoryBlobs;
    use musteragent_identity::DottedKey;
    use musteragent_postgres::PostgresContentStore;
    use musteragent_runtime_skill::SkillLifecycleStore;
    use musteragent_runtime_skill::{
        ImportSkillArchive, SkillImportLineage, SkillPackageSource, SkillPackageSourceFile,
        SkillProductApplication, build_skill_source_archive,
    };
    use musteragent_storage_protection::OpaqueRefKeyring;
    let pg = IsolatedPostgres::start_with_pgvector().await.expect("real unsigned imports preserve original trust and verified audits stay signed must succeed");
    let pool = pg.pool().await.expect("real unsigned imports preserve original trust and verified audits stay signed must succeed");
    musteragent_conformance::migrate(&pool).await.expect("real unsigned imports preserve original trust and verified audits stay signed must succeed");
    let tenant = TenantId::generate();
    let user = PrincipalId::generate();
    let key = DottedKey::new("test.skill.import").expect("real unsigned imports preserve original trust and verified audits stay signed must succeed");
    let content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [31; 32])]).expect("real unsigned imports preserve original trust and verified audits stay signed must succeed")),
    ));
    let store = Arc::new(PostgresSkillLifecycleStore::new(pool.clone()));
    let product = SkillProductApplication::new(store.clone(), store.clone(), content);
    for (name, source, trust) in [
        ("local-import", SkillPackageSource::Local, SkillTrust::Local),
        (
            "community-import",
            SkillPackageSource::Marketplace,
            SkillTrust::Community,
        ),
    ] {
        let archive = build_skill_source_archive(vec![SkillPackageSourceFile {
            path: "SKILL.md".into(), bytes: format!("---\nname: {name}\ndescription: Review supplied notes.\n---\n# Notes\nUse the supplied evidence.\n").into_bytes(),
        }]).expect("real unsigned imports preserve original trust and verified audits stay signed must succeed");
        let imported = product
            .import_archive_bytes(
                ImportSkillArchive {
                    tenant_id: tenant,
                    principal_id: user,
                    archive_path: "notes.zip".into(),
                    source,
                    source_ref: format!("market:{name}"),
                    publisher_ref: "test-publisher".into(),
                    suggested_name: None,
                    display_name: None,
                    expected_archive_hash: None,
                    occurred_at: UtcTimestamp::from_unix_millis(100),
                },
                &archive,
                SkillImportLineage::OwnedIdentity,
            )
            .await
            .expect("real unsigned imports preserve original trust and verified audits stay signed must succeed");
        let package = store
            .load_version(tenant, imported.skill_version_id)
            .await
            .expect("real unsigned imports preserve original trust and verified audits stay signed must succeed")
            .expect("real unsigned imports preserve original trust and verified audits stay signed must succeed");
        assert_eq!(package.identity.trust, trust);
        assert!(
            !package.audit.signature_verified,
            "do not manufacture an official attestation"
        );
        assert!(package.audit.passed_for(trust));
        let bad = sqlx::query(
            "UPDATE skill.package_audits SET static_audit_passed=false WHERE skill_version_id=$1",
        )
        .bind(imported.skill_version_id.into_uuid())
        .execute(&pool)
        .await
        .expect_err("invalid lifecycle operation must fail");
        assert_eq!(
            bad.as_database_error().expect("real unsigned imports preserve original trust and verified audits stay signed must succeed").constraint(),
            Some("skill_audit_activation_gate")
        );
        if trust == SkillTrust::Community {
            let bad = sqlx::query("UPDATE skill.skills SET trust_level=2 WHERE skill_id=$1")
                .bind(imported.skill.skill_id.into_uuid())
                .execute(&pool)
                .await
                .expect_err("invalid lifecycle operation must fail");
            assert_eq!(
                bad.as_database_error().expect("real unsigned imports preserve original trust and verified audits stay signed must succeed").constraint(),
                Some("skill_audit_signature_for_trust")
            );
        }
    }
    let verified = package(None);
    seed_package_content(&pool, tenant, &verified).await;
    let mut unsigned = verified.package.clone();
    unsigned.audit.signature_verified = false;
    assert!(unsigned.validate().is_err());
    SkillLifecycleApplication::new(store)
        .commit_verified_package(CommitVerifiedSkillPackage {
            tenant_id: tenant,
            package: verified.package.clone(),
        })
        .await
        .expect("real unsigned imports preserve original trust and verified audits stay signed must succeed");
    let bad = sqlx::query(
        "UPDATE skill.package_audits SET signature_verified=false WHERE skill_version_id=$1",
    )
    .bind(verified.package.version.skill_version_id.into_uuid())
    .execute(&pool)
    .await
    .expect_err("invalid lifecycle operation must fail");
    assert_eq!(
        bad.as_database_error().expect("real unsigned imports preserve original trust and verified audits stay signed must succeed").constraint(),
        Some("skill_audit_signature_for_trust")
    );
}

#[tokio::test]
async fn postgres_lifecycle_is_immutable_atomic_and_dual_guarded() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply lifecycle migrations");
    let tenant_id = TenantId::generate();
    let publisher = PrincipalId::generate();
    let store = Arc::new(PostgresSkillLifecycleStore::new(pool.clone()));
    let application = Arc::new(SkillLifecycleApplication::new(store.clone()));

    let root = package(None);
    seed_package_content(&pool, tenant_id, &root).await;
    application
        .commit_verified_package(CommitVerifiedSkillPackage {
            tenant_id,
            package: root.package.clone(),
        })
        .await
        .expect("commit immutable root");
    let first = application
        .activate(ActivateSkillVersion {
            tenant_id,
            skill_id: root.package.identity.skill_id,
            skill_version_id: root.package.version.skill_version_id,
            expected: None,
            activated_at: UtcTimestamp::from_unix_millis(30),
            activated_by: publisher,
        })
        .await
        .expect("activate root");

    let child = package(Some(&root.package));
    seed_package_content(&pool, tenant_id, &child).await;
    application
        .commit_verified_package(CommitVerifiedSkillPackage {
            tenant_id,
            package: child.package.clone(),
        })
        .await
        .expect("commit immutable child");

    let mut collision = child.package.clone();
    collision.version.title = "Attempted mutable overwrite".to_owned();
    assert!(matches!(
        application
            .commit_verified_package(CommitVerifiedSkillPackage {
                tenant_id,
                package: collision,
            })
            .await,
        Err(SkillLifecycleApplicationError::ImmutableVersionConflict)
    ));

    let command = ActivateSkillVersion {
        tenant_id,
        skill_id: root.package.identity.skill_id,
        skill_version_id: child.package.version.skill_version_id,
        expected: Some(ActiveSkillVersionGuard {
            skill_version_id: first.skill_version_id,
            pointer_version: first.pointer_version,
        }),
        activated_at: UtcTimestamp::from_unix_millis(40),
        activated_by: publisher,
    };
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let attempts = (0..2)
        .map(|_| {
            let application = application.clone();
            let barrier = barrier.clone();
            let command = command.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                application.activate(command).await
            })
        })
        .collect::<Vec<_>>();
    barrier.wait().await;
    let mut outcomes = Vec::new();
    for attempt in attempts {
        outcomes.push(attempt.await.expect("activation task"));
    }
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(SkillLifecycleApplicationError::CasConflict)))
            .count(),
        1
    );
    let active = store
        .load_active_entries(tenant_id)
        .await
        .expect("read active catalog");
    assert_eq!(active.len(), 1);
    assert_eq!(
        active[0].definition.skill_version_id,
        child.package.version.skill_version_id
    );
    let restarted_store = PostgresSkillLifecycleStore::new(pool.clone());
    let restarted_child = musteragent_runtime_skill::SkillProductStore::load_latest_package(
        &restarted_store,
        tenant_id,
        child.package.identity.skill_id,
    )
    .await
    .expect("reload sealed Skill package after adapter restart")
    .expect("sealed Skill package after adapter restart");
    assert_eq!(restarted_child, child.package.record());

    let bundled = bundled_package();
    assert!(matches!(
        application
            .commit_verified_package(CommitVerifiedSkillPackage {
                tenant_id,
                package: bundled.package,
            })
            .await,
        Err(SkillLifecycleApplicationError::Store(
            SkillLifecycleStoreError::Corrupt
        ))
    ));
    assert_eq!(
        store
            .load_active_entries(tenant_id)
            .await
            .expect("tenant-owned active catalog")
            .len(),
        1
    );

    let grandchild = package(Some(&child.package));
    seed_package_content(&pool, tenant_id, &grandchild).await;
    install_audit_failure(&pool).await;
    assert!(matches!(
        application
            .commit_verified_package(CommitVerifiedSkillPackage {
                tenant_id,
                package: grandchild.package.clone(),
            })
            .await,
        Err(SkillLifecycleApplicationError::Store(
            SkillLifecycleStoreError::Unavailable
        ))
    ));
    remove_audit_failure(&pool).await;
    let leaked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM skill.skill_versions WHERE skill_version_id = $1")
            .bind(grandchild.package.version.skill_version_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("count rolled-back version");
    assert_eq!(leaked, 0, "version and audit commit must be atomic");

    let missing_content = package(Some(&child.package));
    assert!(matches!(
        application
            .commit_verified_package(CommitVerifiedSkillPackage {
                tenant_id,
                package: missing_content.package,
            })
            .await,
        Err(SkillLifecycleApplicationError::Store(
            SkillLifecycleStoreError::Corrupt
        ))
    ));

    sqlx::query("UPDATE skill.package_audits SET audit_digest = $2 WHERE skill_version_id = $1")
        .bind(child.package.version.skill_version_id.into_uuid())
        .bind(Sha256::digest(b"tampered audit").into_bytes().to_vec())
        .execute(&pool)
        .await
        .expect("tamper audit projection");
    assert_eq!(
        store.load_active_entries(tenant_id).await,
        Err(SkillLifecycleStoreError::Corrupt),
        "audit/version disagreement must fail admission closed"
    );
}

struct PackageFixture {
    package: VerifiedSkillPackage,
}

fn package(parent: Option<&VerifiedSkillPackage>) -> PackageFixture {
    let identity = parent.map_or_else(identity, |value| value.identity.clone());
    let version_number = parent.map_or(1, |value| value.version.version.get() + 1);
    let manifest_bytes = format!("manifest-v{version_number}").into_bytes();
    let manifest_ref = content(&manifest_bytes, "application/json");
    let instructions = format!("instructions-v{version_number}").into_bytes();
    let instructions_ref = content(&instructions, "text/markdown;charset=utf-8");
    let package_bytes = format!("package-v{version_number}").into_bytes();
    let package_ref = content(
        &package_bytes,
        "application/vnd.musteragent.legacy.skill-package+zip",
    );
    let payload_hash = Sha256::digest(&instructions);
    let audit_digest = Sha256::digest(format!("audit-v{version_number}").as_bytes());
    let manifest_hash = manifest_ref.sha256;
    PackageFixture {
        package: VerifiedSkillPackage {
            identity: identity.clone(),
            version: SkillVersion {
                skill_version_id: SkillVersionId::generate(),
                skill_id: identity.skill_id,
                version: VersionNumber::new(version_number).expect("version"),
                skill_ref: VersionedKey::new(format!("verified-notes@{version_number}"))
                    .expect("Skill ref"),
                title: "Verified Notes".to_owned(),
                description: "Create governed notes.".to_owned(),
                when_to_use: "Use for a durable note.".to_owned(),
                declared_version: Some(version_number.to_string()),
                source_format: SkillSourceFormat::KernelSourceV1,
                instructions_hash: instructions_ref.sha256,
                instructions_ref: instructions_ref.clone(),
                package_hash: package_ref.sha256,
                package_ref,
                manifest_hash,
                manifest_ref,
                payload_hash,
                package_dependencies: BTreeSet::new(),
                required_tools: BTreeSet::from([
                    VersionedKey::new("artifact.note.create@1").expect("Tool")
                ]),
                optional_tools: BTreeSet::new(),
                compatible_agent_contracts: BTreeSet::from([VersionedKey::new(
                    "musteragent.legacy.runtime-contract@1",
                )
                .expect("contract")]),
                compatible_surfaces: BTreeSet::from([Surface::Web]),
                audit_digest,
                created_at: UtcTimestamp::from_unix_millis(
                    20 + i64::try_from(version_number).expect("version"),
                ),
                supersedes: parent.map(|value| value.version.skill_version_id),
            },
            package_bytes,
            manifest_bytes,
            resources: vec![SkillResourceDescriptor {
                path: SkillResourcePath::new("SKILL.md").expect("resource path"),
                media_type: instructions_ref.media_type,
                byte_length: instructions_ref.byte_length,
                sha256: instructions_ref.sha256,
                role: SkillResourceRole::Instructions,
            }],
            compatibility_findings: Vec::new(),
            audit: PackageAuditReport {
                manifest_hash,
                file_set_hash: payload_hash,
                signature_verified: true,
                manifest_verified: true,
                file_hashes_verified: true,
                static_audit_passed: true,
                scripts_executed: false,
                disposition: QuarantineDisposition::Cleared,
                audit_digest,
            },
        },
    }
}

fn bundled_package() -> PackageFixture {
    let mut fixture = package(None);
    let identity = SkillIdentity {
        skill_id: SkillId::generate(),
        skill_key: SkillKey::new("bundled-notes").expect("key"),
        provenance: SkillProvenance::Bundled {
            bundle_ref: VersionedKey::new("musteragent.legacy.skill-bundle@1").expect("bundle"),
        },
        trust: SkillTrust::Official,
        created_at: UtcTimestamp::from_unix_millis(10),
    };
    fixture.package.identity = identity.clone();
    fixture.package.version.skill_id = identity.skill_id;
    fixture.package.version.skill_version_id = SkillVersionId::generate();
    fixture.package.version.skill_ref = VersionedKey::new("bundled-notes@1").expect("Skill ref");
    fixture.package.version.title = "Bundled Notes".to_owned();
    fixture
}

fn identity() -> SkillIdentity {
    SkillIdentity {
        skill_id: SkillId::generate(),
        skill_key: SkillKey::new("verified-notes").expect("key"),
        provenance: SkillProvenance::Installed {
            source: InstalledSkillSource::Marketplace,
            source_ref: "market://verified-notes".to_owned(),
            publisher_ref: "publisher:test".to_owned(),
        },
        trust: SkillTrust::Verified,
        created_at: UtcTimestamp::from_unix_millis(10),
    }
}

fn content(bytes: &[u8], media_type: &str) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media_type).expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(bytes),
    }
}

async fn seed_package_content(pool: &PgPool, tenant_id: TenantId, fixture: &PackageFixture) {
    seed_content(pool, tenant_id, &fixture.package.version.manifest_ref).await;
    seed_content(pool, tenant_id, &fixture.package.version.instructions_ref).await;
    seed_content(pool, tenant_id, &fixture.package.version.package_ref).await;
}

async fn seed_content(pool: &PgPool, tenant_id: TenantId, reference: &ContentRef) {
    let storage_ref = format!("test://{}", reference.content_id);
    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id, tenant_id, media_type, byte_length, content_hash,
             storage_ref, created_at_ms, state
         ) VALUES ($1, $2, $3, $4::numeric, $5, $6, 1, 2)",
    )
    .bind(reference.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().to_vec())
    .bind(storage_ref)
    .execute(pool)
    .await
    .expect("seed ready ContentRef");
}

async fn install_audit_failure(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE FUNCTION skill.reject_package_audit()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             RAISE EXCEPTION 'injected package audit failure';
         END $$;
         CREATE TRIGGER reject_package_audit
         BEFORE INSERT ON skill.package_audits
         FOR EACH ROW EXECUTE FUNCTION skill.reject_package_audit();",
    )
    .execute(pool)
    .await
    .expect("install audit failure");
}

async fn remove_audit_failure(pool: &PgPool) {
    sqlx::raw_sql(
        "DROP TRIGGER reject_package_audit ON skill.package_audits;
         DROP FUNCTION skill.reject_package_audit();",
    )
    .execute(pool)
    .await
    .expect("remove audit failure");
}
