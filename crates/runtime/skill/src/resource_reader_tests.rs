use std::collections::BTreeMap;

use crate::authorized_skill_catalog;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_skill::builtin_skill_catalog_entries;

use super::resource_reader::{SkillResourceReadError, read_pinned_skill_resources};

#[derive(Default)]
struct MemoryContent(BTreeMap<ContentId, Vec<u8>>);
impl ContentStore for MemoryContent {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { panic!("Resource reads must not write or execute anything") })
    }
    fn read<'a>(
        &'a self,
        _: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async {
            self.0
                .get(&content.content_id)
                .cloned()
                .ok_or(ContentStoreError::NotFound)
        })
    }
}

fn descriptor() -> musteragent_discovery::AuthorizedSkillDescriptor {
    let entries = builtin_skill_catalog_entries().expect("catalog");
    // Exercise the exact package selected by this compiled release profile.
    // Pinning the fixture to @5 bypasses that owner and breaks when the
    // product correctly publishes @6 without retaining @5 in its active set.
    let mut packages = entries
        .iter()
        .filter(|entry| entry.definition.skill_ref.logical_key() == "project-files");
    let package = packages.next().expect("active system resource package");
    assert!(
        packages.next().is_none(),
        "one active system package revision"
    );
    let capabilities = entries
        .iter()
        .flat_map(|e| e.definition.required_capabilities.iter().cloned())
        .collect();
    authorized_skill_catalog(&entries, &capabilities)
        .expect("authorized catalog")
        .get(package.definition.skill_ref.as_str())
        .expect("the exact active resource package is authorized")
        .clone()
}

#[test]
fn bundled_resources_are_readable_as_exact_data_without_installation_or_execution() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build resource reader test runtime")
        .block_on(async {
            let content = MemoryContent::default();
            let tenant = TenantId::generate();
            let skill = descriptor();
            let index = read_pinned_skill_resources(&content, tenant, &skill, None)
                .await
                .expect("read pinned resource index");
            assert!(index.text.is_none());
            assert!(
                index
                    .resources
                    .iter()
                    .all(|r| !r.path.starts_with("scripts/"))
            );
            // Work and Dialogue select different immutable report packages.
            // Verify every file against the selected release's archive rather
            // than imposing Dialogue's HTML template on Work's CSS resources.
            let release =
                musteragent_skill_release::official_runtime_skill_release(skill.skill_version_id)
                    .expect("release registry")
                    .expect("selected immutable release");
            let mut archive =
                zip::ZipArchive::new(std::io::Cursor::new(&release.source_archive_bytes))
                    .expect("selected release archive");
            assert_eq!(index.resources.len(), archive.len());
            for position in 0..archive.len() {
                use std::io::Read;
                let mut packaged = archive.by_index(position).expect("archive resource");
                let name = packaged.name().expect("resource name").into_owned();
                let mut expected = Vec::new();
                packaged
                    .read_to_end(&mut expected)
                    .expect("immutable resource bytes");
                let file = index
                    .resources
                    .iter()
                    .find(|r| r.path == name)
                    .expect("packaged resource");
                let read = read_pinned_skill_resources(&content, tenant, &skill, Some(&name))
                    .await
                    .expect("read the exact packaged resource");
                let text = read.text.expect("readable packaged text");
                assert_eq!(text.as_bytes(), expected, "resource {name}");
                assert_eq!(text.len() as u64, file.byte_length);
                assert_eq!(Sha256::digest(text.as_bytes()).to_string(), file.sha256);
            }
            for path in ["../SKILL.md", "/etc/passwd", "references/missing.md"] {
                assert!(matches!(
                    read_pinned_skill_resources(&content, tenant, &skill, Some(path)).await,
                    Err(SkillResourceReadError::NotFound)
                ));
            }
            let mut forged = skill.clone();
            forged.instructions_hash = Sha256::digest(b"different version");
            assert!(matches!(
                read_pinned_skill_resources(&content, tenant, &forged, None).await,
                Err(SkillResourceReadError::Integrity)
            ));
        });
}

#[test]
fn installed_resources_use_pinned_content_and_reject_corruption() {
    tokio::runtime::Builder::new_current_thread().build().expect("build installed resource test runtime").block_on(async {
        let mut skill = descriptor();
        skill.origin = musteragent_discovery::SkillOrigin::Installed;
        let source = "print('read as data, never execute')\n";
        let instructions = musteragent_skill_release::official_runtime_skill_release(skill.skill_version_id)
            .expect("valid package fixture").expect("valid package fixture").instructions_bytes().to_vec();
        let binary = b"\x00\xff\x80original-font";
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema_version": "musteragent.skill-package.v2",
            "stable_skill_key": skill.skill_ref.logical_key(), "revision": skill.skill_ref.version(),
            "payload_sha256": Sha256::digest(b"normalized source file set").to_string(),
            "files": [
                {"canonical_path":"SKILL.md", "media_type":"text/markdown", "role":"instructions",
                 "byte_length":skill.instructions_ref.byte_length, "sha256":skill.instructions_hash.to_string()},
                {"canonical_path":"scripts/check.py", "media_type":"text/plain", "role":"script",
                 "byte_length":source.len(), "sha256":Sha256::digest(source.as_bytes()).to_string()},
                {"canonical_path":"assets/font.bin", "media_type":"application/octet-stream", "role":"asset",
                 "byte_length":binary.len(), "sha256":Sha256::digest(binary).to_string()}
            ]
        })).expect("serialize resource manifest");
        use std::io::Write;
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        writer.start_file("payload/SKILL.md", zip::write::SimpleFileOptions::default()).expect("valid package fixture");
        writer.write_all(&instructions).expect("valid package fixture");
        writer.start_file("payload/assets/font.bin", zip::write::SimpleFileOptions::default()).expect("valid package fixture");
        writer.write_all(binary).expect("valid package fixture");
        writer.start_file("payload/scripts/check.py", zip::write::SimpleFileOptions::default()).expect("start script resource");
        writer.write_all(source.as_bytes()).expect("write inert script bytes");
        let package = writer.finish().expect("finish immutable package").into_inner();
        let mut content = MemoryContent::default();
        skill.references.clear();
        use musteragent_discovery::{SkillCatalogReference, SkillCatalogReferenceKind as K};
        for (kind, bytes, mime) in [(K::Manifest, manifest, "application/json"), (K::Package, package, "application/zip")] {
            let reference = ContentRef {content_id:ContentId::generate(), media_type:MediaType::new(mime).expect("resource media type"),
                byte_length:bytes.len() as u64, sha256:Sha256::digest(&bytes)};
            if kind == K::Manifest { skill.spec_hash = reference.sha256; }
            content.0.insert(reference.content_id, bytes);
            skill.references.push(SkillCatalogReference {kind, content_ref:reference});
        }
        let result = read_pinned_skill_resources(&content, TenantId::generate(), &skill, Some("scripts/check.py")).await.expect("read pinned script as data");
        assert_eq!(result.text.as_deref(), Some(source));
        let pin = super::read_pinned_skill_identity(&content, TenantId::generate(), &skill).await.expect("verified version pin");
        assert_eq!(pin.payload_hash, Sha256::digest(b"normalized source file set"));
        assert_ne!(pin.payload_hash, skill.references.iter().find(|r| r.kind == K::Package).expect("valid package fixture").content_ref.sha256);
        let native_files = super::read_pinned_skill_files(&content, TenantId::generate(), &skill).await.expect("valid package fixture");
        assert_eq!(native_files.iter().find(|file| file.path == "assets/font.bin").expect("valid package fixture").bytes, binary);
        assert_eq!(native_files.iter().find(|file| file.path == "scripts/check.py").expect("valid package fixture").bytes, source.as_bytes());
        assert!(matches!(read_pinned_skill_resources(&content, TenantId::generate(), &skill, Some("assets/font.bin")).await,
            Err(SkillResourceReadError::NotText)));
        let mut forged = skill.clone();
        forged.spec_hash = Sha256::digest(b"another manifest");
        assert!(super::read_pinned_skill_files(&content, TenantId::generate(), &forged).await.is_err());
        let package_id = skill.references.iter().find(|r| r.kind == K::Package).expect("package reference").content_ref.content_id;
        content.0.get_mut(&package_id).expect("stored package bytes")[0] ^= 1;
        assert!(matches!(read_pinned_skill_resources(&content, TenantId::generate(), &skill, Some("scripts/check.py")).await,
            Err(SkillResourceReadError::Integrity)));
    });
}
