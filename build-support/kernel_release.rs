//! The single built-in kernel release.
//!
//! musteragent ships exactly one reference product release: its kernel Tool
//! catalog and its generic base Skills. Hosts that build products on top of the kernel publish their own
//! releases under their own product key at runtime; nothing product-specific
//! is compiled into the kernel.
#![allow(dead_code)]
use sha2::{Digest as _, Sha256};
use std::{fs, path::Path};

pub const PRODUCT: &str = "musteragent";
pub const RELEASE_ID: &str = "musteragent.kernel@1";
pub const PROFILE_ID: &str = "musteragent.kernel.v1";
pub const SKILL_RELEASE_SCHEMA: &str = "musteragent.runtime.skill-release.v1";
pub const KERNEL_TOOLS: &str = "contracts/kernel-tools.json";

pub const KERNEL_SKILLS: &str = "contracts/kernel-skills.json";
pub const SKILL_RESOURCES: &str = "resources/skills";

/// Digest of the release material: the kernel Tool catalog, the kernel Skill
/// manifest and every file of the bundled Skill packages (by relative path).
pub fn digest(root: &Path) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"musteragent.runtime.kernel-release.v2\0");
    for manifest in [KERNEL_TOOLS, KERNEL_SKILLS] {
        let path = root.join(manifest);
        println!("cargo:rerun-if-changed={}", path.display());
        let bytes = fs::read(path).expect("kernel release manifest");
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    let skills = root.join(SKILL_RESOURCES);
    println!("cargo:rerun-if-changed={}", skills.display());
    let mut files = Vec::new();
    collect_files(&skills, &mut files);
    files.sort();
    for path in files {
        println!("cargo:rerun-if-changed={}", path.display());
        let relative = path
            .strip_prefix(&skills)
            .expect("Skill resource path")
            .to_string_lossy()
            .replace('\\', "/");
        let bytes = fs::read(&path).expect("Skill resource");
        hasher.update((relative.len() as u64).to_be_bytes());
        hasher.update(relative.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    hasher.finalize().into()
}

fn collect_files(directory: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(directory).expect("read Skill resources") {
        let path = entry.expect("Skill resource entry").path();
        if path.is_dir() {
            println!("cargo:rerun-if-changed={}", path.display());
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}
