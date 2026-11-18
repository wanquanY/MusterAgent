use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Barrier, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ContentId, PrincipalId, SkillId, SkillVersionId, TenantId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_skill::{
    ActivateSkillVersion, ActivePointerCasOutcome, ActiveSkillVersionGuard,
    CommitVerifiedSkillPackage, SkillIdentityInsertOutcome, SkillLifecycleApplication,
    SkillLifecycleApplicationError, SkillLifecycleStore, SkillLifecycleStoreError,
    SkillVersionInsertOutcome,
};
use musteragent_skill::{
    ActiveSkillVersionPointer, InstalledSkillSource, PackageAuditReport, QuarantineDisposition,
    SkillIdentity, SkillKey, SkillPackageRecord, SkillProvenance, SkillResourceDescriptor,
    SkillResourcePath, SkillResourceRole, SkillSourceFormat, SkillTrust, SkillVersion, Surface,
    VerifiedSkillPackage,
};

#[derive(Default)]
struct InMemorySkillStore {
    state: Mutex<StoreState>,
}

#[derive(Default)]
struct StoreState {
    identities: BTreeMap<(TenantId, SkillId), SkillIdentity>,
    versions: BTreeMap<(TenantId, SkillVersionId), SkillPackageRecord>,
    active: BTreeMap<(TenantId, SkillId), ActiveSkillVersionPointer>,
}

impl SkillLifecycleStore for InMemorySkillStore {
    fn load_identity(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> ApplicationFuture<'_, Result<Option<SkillIdentity>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .map_err(|_| SkillLifecycleStoreError::Corrupt)?
                .identities
                .get(&(tenant_id, skill_id))
                .cloned())
        })
    }

    fn load_identity_by_key<'a>(
        &'a self,
        tenant_id: TenantId,
        skill_key: &'a SkillKey,
    ) -> ApplicationFuture<'a, Result<Option<SkillIdentity>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .map_err(|_| SkillLifecycleStoreError::Corrupt)?
                .identities
                .iter()
                .find(|((tenant, _), identity)| {
                    *tenant == tenant_id && identity.skill_key == *skill_key
                })
                .map(|(_, identity)| identity.clone()))
        })
    }

    fn insert_identity(
        &self,
        tenant_id: TenantId,
        identity: SkillIdentity,
    ) -> ApplicationFuture<'_, Result<SkillIdentityInsertOutcome, SkillLifecycleStoreError>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| SkillLifecycleStoreError::Corrupt)?;
            if let Some(existing) = state
                .identities
                .get(&(tenant_id, identity.skill_id))
                .cloned()
                .or_else(|| {
                    state
                        .identities
                        .iter()
                        .find(|((tenant, _), candidate)| {
                            *tenant == tenant_id && candidate.skill_key == identity.skill_key
                        })
                        .map(|(_, candidate)| candidate.clone())
                })
            {
                return Ok(SkillIdentityInsertOutcome::Existing(existing));
            }
            state
                .identities
                .insert((tenant_id, identity.skill_id), identity);
            Ok(SkillIdentityInsertOutcome::Inserted)
        })
    }

    fn load_version(
        &self,
        tenant_id: TenantId,
        skill_version_id: SkillVersionId,
    ) -> ApplicationFuture<'_, Result<Option<SkillPackageRecord>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .map_err(|_| SkillLifecycleStoreError::Corrupt)?
                .versions
                .get(&(tenant_id, skill_version_id))
                .cloned())
        })
    }

    fn insert_version(
        &self,
        tenant_id: TenantId,
        package: VerifiedSkillPackage,
    ) -> ApplicationFuture<'_, Result<SkillVersionInsertOutcome, SkillLifecycleStoreError>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| SkillLifecycleStoreError::Corrupt)?;
            if let Some(existing) = state
                .versions
                .get(&(tenant_id, package.version.skill_version_id))
                .cloned()
                .or_else(|| {
                    state
                        .versions
                        .iter()
                        .find(|((tenant, _), candidate)| {
                            *tenant == tenant_id
                                && candidate.version.skill_id == package.version.skill_id
                                && candidate.version.version == package.version.version
                        })
                        .map(|(_, candidate)| candidate.clone())
                })
            {
                return Ok(SkillVersionInsertOutcome::Existing(Box::new(existing)));
            }
            state.versions.insert(
                (tenant_id, package.version.skill_version_id),
                package.record(),
            );
            Ok(SkillVersionInsertOutcome::Inserted)
        })
    }

    fn load_active(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> ApplicationFuture<'_, Result<Option<ActiveSkillVersionPointer>, SkillLifecycleStoreError>>
    {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .map_err(|_| SkillLifecycleStoreError::Corrupt)?
                .active
                .get(&(tenant_id, skill_id))
                .copied())
        })
    }

    fn compare_and_swap_active(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        expected: Option<ActiveSkillVersionPointer>,
        replacement: ActiveSkillVersionPointer,
    ) -> ApplicationFuture<'_, Result<ActivePointerCasOutcome, SkillLifecycleStoreError>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| SkillLifecycleStoreError::Corrupt)?;
            let actual = state.active.get(&(tenant_id, skill_id)).copied();
            if actual != expected {
                return Ok(ActivePointerCasOutcome::Conflict(actual));
            }
            state.active.insert((tenant_id, skill_id), replacement);
            Ok(ActivePointerCasOutcome::Swapped(replacement))
        })
    }
}

#[test]
fn version_commit_is_immutable_and_concurrent_dual_guard_activation_has_one_winner() {
    let tenant_id = TenantId::generate();
    let publisher = PrincipalId::generate();
    let store = Arc::new(InMemorySkillStore::default());
    let application = Arc::new(SkillLifecycleApplication::new(store));
    let root = package(None);
    let skill_id = root.identity.skill_id;
    block_on(
        application.commit_verified_package(CommitVerifiedSkillPackage {
            tenant_id,
            package: root.clone(),
        }),
    )
    .expect("commit root");
    let first = block_on(application.activate(ActivateSkillVersion {
        tenant_id,
        skill_id,
        skill_version_id: root.version.skill_version_id,
        expected: None,
        activated_at: UtcTimestamp::from_unix_millis(30),
        activated_by: publisher,
    }))
    .expect("activate root");

    let child = package(Some(&root));
    block_on(
        application.commit_verified_package(CommitVerifiedSkillPackage {
            tenant_id,
            package: child.clone(),
        }),
    )
    .expect("commit child");
    let command = ActivateSkillVersion {
        tenant_id,
        skill_id,
        skill_version_id: child.version.skill_version_id,
        expected: Some(ActiveSkillVersionGuard {
            skill_version_id: first.skill_version_id,
            pointer_version: first.pointer_version,
        }),
        activated_at: UtcTimestamp::from_unix_millis(40),
        activated_by: publisher,
    };
    let barrier = Arc::new(Barrier::new(3));
    let handles = (0..2)
        .map(|_| {
            let application = Arc::clone(&application);
            let barrier = Arc::clone(&barrier);
            let command = command.clone();
            thread::spawn(move || {
                barrier.wait();
                block_on(application.activate(command))
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().expect("activation thread"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(SkillLifecycleApplicationError::CasConflict)))
            .count(),
        1
    );

    let mut mutated = child;
    mutated.version.title = "Mutated in place".to_owned();
    assert!(matches!(
        block_on(
            application.commit_verified_package(CommitVerifiedSkillPackage {
                tenant_id,
                package: mutated,
            })
        ),
        Err(SkillLifecycleApplicationError::ImmutableVersionConflict)
    ));
}

fn package(parent: Option<&VerifiedSkillPackage>) -> VerifiedSkillPackage {
    let identity = parent.map_or_else(identity, |value| value.identity.clone());
    let version_number = parent.map_or(1, |value| value.version.version.get() + 1);
    let manifest_bytes = format!("manifest-v{version_number}").into_bytes();
    let manifest_ref = content(&manifest_bytes, "application/json");
    let manifest_hash = manifest_ref.sha256;
    let instructions = format!("instructions-v{version_number}").into_bytes();
    let instructions_ref = content(&instructions, "text/markdown;charset=utf-8");
    let package_bytes = format!("package-v{version_number}").into_bytes();
    let package_ref = content(
        &package_bytes,
        "application/vnd.musteragent.legacy.skill-package+zip",
    );
    let payload_hash = Sha256::digest(&instructions);
    let audit_digest = Sha256::digest(format!("audit-v{version_number}").as_bytes());
    VerifiedSkillPackage {
        identity: identity.clone(),
        version: SkillVersion {
            skill_version_id: SkillVersionId::generate(),
            skill_id: identity.skill_id,
            version: VersionNumber::new(version_number).expect("version"),
            skill_ref: VersionedKey::new(format!("verified-notes@{version_number}")).expect("ref"),
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
    }
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

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::yield_now(),
        }
    }
}
