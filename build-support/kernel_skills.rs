//! Generic base Skills compiled into the single kernel release.
//!
//! `contracts/kernel-skills.json` declares every bundled Skill version, its
//! immutable package material and its automatic disclosures. Domain and
//! release build scripts share this loader so both compile exactly the same
//! validated catalog.
#![allow(dead_code)]
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

pub const PATH: &str = "contracts/kernel-skills.json";
pub const SCHEMA: &str = "musteragent.runtime.kernel-skills.v1";
pub const MATERIAL_HASH_DOMAIN: &str = "musteragent.runtime.skill-source-material.v1";
const SNAPSHOT_SCHEMA_PREFIX: &str = "musteragent.runtime.snapshot.v";
/// Stable slot namespaces reconstructed by `musteragent-skill`.
const SKILL_ID_PREFIX: &str = "01982f80-0000-7000-8000-000000001";
const SKILL_VERSION_ID_PREFIX: &str = "01982f80-0000-7000-8000-000000002";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema_version: String,
    pub bundle_ref: String,
    pub compatible_agent_contract: String,
    pub released_at_ms: i64,
    pub active_skill_version_ids: Vec<String>,
    pub skills: Vec<SkillRelease>,
    pub automatic_disclosures: Vec<AutomaticDisclosure>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillRelease {
    pub skill_id: String,
    pub skill_version_id: String,
    pub skill_key: String,
    pub version: u64,
    pub supersedes: Option<String>,
    pub identity_created_at_ms: i64,
    pub released_at_ms: i64,
    pub surfaces: Vec<String>,
    /// Code-execution sandbox compatibility: `browser-compatible`,
    /// `server-required`, or `none` when the Skill runs no code.
    pub sandbox: String,
    pub source: String,
    pub resources: Vec<String>,
    pub material_sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutomaticDisclosure {
    pub snapshot_schema_version: String,
    pub skill_key: String,
    pub required_route_capability: Option<String>,
    pub required_tool_capability: Option<String>,
}

impl SkillRelease {
    pub fn skill_ref(&self) -> String {
        format!("{}@{}", self.skill_key, self.version)
    }

    pub fn skill_slot(&self) -> usize {
        stable_slot(&self.skill_id, SKILL_ID_PREFIX)
    }

    pub fn material_slot(&self) -> usize {
        stable_slot(&self.skill_version_id, SKILL_VERSION_ID_PREFIX)
    }

    /// Only bundled `*-base` Skills hold the Base activation slot.
    pub fn activation(&self) -> &'static str {
        if self.skill_key.ends_with("-base") {
            "Base"
        } else {
            "ExplicitOrResolved"
        }
    }

    /// Rust expression for `Option<musteragent_sandbox::SandboxCompatibility>`.
    pub fn sandbox_expr(&self) -> &'static str {
        match self.sandbox.as_str() {
            "browser-compatible" => {
                "Some(musteragent_sandbox::SandboxCompatibility::BrowserCompatible)"
            }
            "server-required" => "Some(musteragent_sandbox::SandboxCompatibility::ServerRequired)",
            "none" => "None",
            _ => panic!("unknown Skill sandbox compatibility for {}", self.skill_key),
        }
    }

    /// Absolute SKILL.md path followed by `(archive path, absolute path)` resources.
    pub fn files(&self, root: &Path) -> (PathBuf, Vec<(String, PathBuf)>) {
        let source = checked_path(root, &self.source);
        assert!(
            source.file_name().is_some_and(|name| name == "SKILL.md"),
            "Skill source must be its package SKILL.md"
        );
        let parent = source.parent().expect("Skill source parent").to_owned();
        let resources = self
            .resources
            .iter()
            .map(|resource| {
                let path = checked_path(root, resource);
                let archive = path
                    .strip_prefix(&parent)
                    .expect("Skill resource must remain inside its package")
                    .to_string_lossy()
                    .replace('\\', "/");
                (archive, path)
            })
            .collect();
        (source, resources)
    }
}

pub fn load(root: &Path) -> Manifest {
    let path = root.join(PATH);
    println!("cargo:rerun-if-changed={}", path.display());
    let manifest: Manifest = serde_json::from_slice(&fs::read(path).expect("kernel Skills"))
        .expect("closed kernel Skill manifest");
    assert_eq!(manifest.schema_version, SCHEMA);
    assert!(
        manifest.bundle_ref.starts_with("musteragent.skill-bundle@"),
        "kernel Skills use the kernel bundle"
    );
    assert!(manifest.released_at_ms > 0);
    validate_lineage(&manifest);
    for skill in &manifest.skills {
        let (source, resources) = skill.files(root);
        println!("cargo:rerun-if-changed={}", source.display());
        for (_, path) in &resources {
            println!("cargo:rerun-if-changed={}", path.display());
        }
        assert_eq!(
            material_sha256(&source, &resources),
            skill.material_sha256,
            "published Skill material changed for {}; publish a successor version",
            skill.skill_ref()
        );
        skill.sandbox_expr();
        assert!(!skill.surfaces.is_empty(), "Skill requires a surface");
        for surface in &skill.surfaces {
            surface_variant(surface);
        }
    }
    validate_tool_dependencies(root, &manifest);
    validate_automatic_disclosures(&manifest);
    manifest
}

pub fn is_active(manifest: &Manifest, skill: &SkillRelease) -> bool {
    manifest
        .active_skill_version_ids
        .contains(&skill.skill_version_id)
}

fn validate_lineage(manifest: &Manifest) {
    let mut version_ids = BTreeSet::new();
    let mut refs = BTreeSet::new();
    let mut material_slots = BTreeSet::new();
    let mut identities = BTreeMap::<&str, &str>::new();
    let mut keys = BTreeMap::<&str, &str>::new();
    for skill in &manifest.skills {
        assert!(version_ids.insert(skill.skill_version_id.as_str()));
        assert!(refs.insert(skill.skill_ref()), "Skill refs must be unique");
        assert!(material_slots.insert(skill.material_slot()));
        skill.skill_slot();
        assert_eq!(
            *identities
                .entry(skill.skill_id.as_str())
                .or_insert(skill.skill_key.as_str()),
            skill.skill_key.as_str(),
            "Skill identity changes key within a lineage"
        );
        assert_eq!(
            *keys
                .entry(skill.skill_key.as_str())
                .or_insert(skill.skill_id.as_str()),
            skill.skill_id.as_str(),
            "Skill key changes identity within a lineage"
        );
        assert!(
            0 < skill.identity_created_at_ms
                && skill.identity_created_at_ms <= skill.released_at_ms
                && skill.released_at_ms <= manifest.released_at_ms
        );
        match (skill.version, skill.supersedes.as_deref()) {
            (1, None) => assert_eq!(skill.identity_created_at_ms, skill.released_at_ms),
            (version, Some(parent)) if version > 1 => {
                let parent = manifest
                    .skills
                    .iter()
                    .find(|candidate| candidate.skill_version_id == parent)
                    .expect("Skill successor parent must exist");
                assert!(version_ids.contains(parent.skill_version_id.as_str()));
                assert_eq!(parent.skill_id, skill.skill_id);
                assert_eq!(parent.version + 1, skill.version);
                assert!(parent.released_at_ms <= skill.released_at_ms);
            }
            _ => panic!("Skill release lineage is invalid"),
        }
    }
    let active = manifest
        .active_skill_version_ids
        .iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(active.len(), manifest.active_skill_version_ids.len());
    let superseded = manifest
        .skills
        .iter()
        .filter_map(|skill| skill.supersedes.as_ref())
        .collect::<BTreeSet<_>>();
    for id in active {
        assert!(version_ids.contains(id.as_str()), "unknown active Skill");
        assert!(
            !superseded.contains(id),
            "active Skill must be a lineage head"
        );
    }
}

/// Every Tool named by an active Skill must exist in the kernel Tool catalog.
/// Required Tools must exist on each deployment that serves the Skill's
/// surfaces (`desktop` runs on Device, every other surface on Cloud).
fn validate_tool_dependencies(root: &Path, manifest: &Manifest) {
    let path = root.join(super::kernel_release::KERNEL_TOOLS);
    println!("cargo:rerun-if-changed={}", path.display());
    let document: Value = serde_json::from_slice(&fs::read(&path).expect("kernel Tools"))
        .expect("kernel Tool manifest");
    let catalogs = ["cloud", "device"]
        .into_iter()
        .map(|deployment| {
            let keys = document["products"][super::kernel_release::PRODUCT][deployment]["tools"]
                .as_array()
                .expect("kernel Tools")
                .iter()
                .map(|tool| tool["tool_key"].as_str().expect("Tool key").to_owned())
                .collect::<BTreeSet<_>>();
            (deployment, keys)
        })
        .collect::<BTreeMap<_, _>>();
    for skill in manifest.skills.iter().filter(|s| is_active(manifest, s)) {
        let (source, _) = skill.files(root);
        let text = fs::read_to_string(source).expect("Skill instructions");
        let required = frontmatter_values(&text, &["required_tools", "required_capabilities"]);
        let optional = frontmatter_values(&text, &["optional_tools", "optional_capabilities"]);
        let deployments = skill
            .surfaces
            .iter()
            .map(|surface| {
                if surface == "desktop" {
                    "device"
                } else {
                    "cloud"
                }
            })
            .collect::<BTreeSet<_>>();
        for deployment in deployments {
            let missing = required
                .difference(&catalogs[deployment])
                .collect::<Vec<_>>();
            assert!(
                missing.is_empty(),
                "Skill {} requires unavailable {deployment} Tools: {missing:?}",
                skill.skill_ref()
            );
        }
        for tool in &optional {
            assert!(
                catalogs.values().any(|catalog| catalog.contains(tool)),
                "Skill {} names an unknown optional Tool {tool}",
                skill.skill_ref()
            );
        }
    }
    for disclosure in &manifest.automatic_disclosures {
        if let Some(tool) = &disclosure.required_tool_capability {
            assert!(
                catalogs.values().any(|catalog| catalog.contains(tool)),
                "automatic disclosure waits for an unknown Tool {tool}"
            );
        }
    }
}

fn validate_automatic_disclosures(manifest: &Manifest) {
    let active = manifest
        .skills
        .iter()
        .filter(|skill| is_active(manifest, skill))
        .map(|skill| skill.skill_key.as_str())
        .collect::<BTreeSet<_>>();
    let mut identities = BTreeSet::new();
    for disclosure in &manifest.automatic_disclosures {
        assert!(
            disclosure
                .snapshot_schema_version
                .strip_prefix(SNAPSHOT_SCHEMA_PREFIX)
                .is_some_and(|version| version.parse::<u32>().is_ok_and(|v| v > 0)),
            "automatic Skill disclosure has an invalid Snapshot schema"
        );
        assert!(
            active.contains(disclosure.skill_key.as_str()),
            "automatic Skill disclosure targets an inactive Skill"
        );
        match (
            disclosure.required_route_capability.as_deref(),
            disclosure.required_tool_capability.as_deref(),
        ) {
            (Some("vision"), None) => {}
            (None, Some(tool)) => assert!(
                tool.split_once('@')
                    .is_some_and(|(key, version)| !key.is_empty()
                        && version.parse::<u64>().is_ok_and(|v| v > 0)),
                "automatic Skill disclosure declares an invalid Tool capability"
            ),
            _ => panic!("automatic Skill disclosure must declare exactly one supported trigger"),
        }
        assert!(
            identities.insert((
                disclosure.snapshot_schema_version.as_str(),
                disclosure.skill_key.as_str()
            )),
            "automatic Skill disclosure is duplicated"
        );
    }
}

/// Domain-separated digest over the package archive entries in path order.
pub fn material_sha256(source: &Path, resources: &[(String, PathBuf)]) -> String {
    let mut entries = vec![(
        "SKILL.md".to_owned(),
        fs::read(source).expect("read Skill instructions"),
    )];
    for (archive, path) in resources {
        entries.push((
            archive.clone(),
            fs::read(path).expect("read Skill resource"),
        ));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        entries
            .iter()
            .map(|(path, _)| path)
            .collect::<BTreeSet<_>>()
            .len(),
        entries.len(),
        "Skill release contains duplicate archive paths"
    );
    let mut digest = Sha256::new();
    digest.update(MATERIAL_HASH_DOMAIN.as_bytes());
    digest.update(b"\0");
    for (path, bytes) in entries {
        digest.update((path.len() as u64).to_be_bytes());
        digest.update(path.as_bytes());
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Block-list values of selected top-level YAML frontmatter keys.
pub fn frontmatter_values(source: &str, accepted_keys: &[&str]) -> BTreeSet<String> {
    let mut lines = source.lines();
    assert_eq!(
        lines.next(),
        Some("---"),
        "Skill instructions require YAML frontmatter"
    );
    let mut selected = false;
    let mut values = BTreeSet::new();
    for line in lines {
        if line == "---" {
            break;
        }
        if !line.starts_with(' ') {
            selected = line
                .strip_suffix(':')
                .is_some_and(|key| accepted_keys.contains(&key));
            continue;
        }
        if selected && let Some(value) = line.trim().strip_prefix("- ") {
            values.insert(value.to_owned());
        }
    }
    values
}

pub fn surface_variant(value: &str) -> &'static str {
    match value {
        "web" => "Web",
        "studio" => "Studio",
        "api" => "Api",
        "desktop" => "Desktop",
        "channel" => "Channel",
        "mobile" => "Mobile",
        _ => panic!("unknown Skill surface"),
    }
}

fn stable_slot(value: &str, prefix: &str) -> usize {
    value
        .strip_prefix(prefix)
        .filter(|slot| slot.len() == 3 && slot.bytes().all(|byte| byte.is_ascii_digit()))
        .expect("kernel Skill identity must use the stable slot namespace")
        .parse()
        .expect("kernel Skill slot")
}

pub fn checked_path(root: &Path, value: &str) -> PathBuf {
    let relative = Path::new(value);
    assert!(
        !value.is_empty()
            && !relative.is_absolute()
            && !relative
                .components()
                .any(|item| matches!(item, Component::ParentDir)),
        "release path escapes the kernel root"
    );
    let path = root.join(relative);
    assert!(path.is_file(), "release dependency does not exist: {value}");
    path
}
