//! Versioned byte manifests for the credential-free execution boundary.
//! Resource remains the live directory owner; these describe one fixed snapshot
//! and an untrusted candidate, validated before planning an atomic change set.
use crate::ExecutionWorkspaceSnapshot;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const WORKSPACE_MANIFEST_VERSION: &str = "musteragent.execution-workspace/1";
pub const WORKSPACE_DELTA_VERSION: &str = "musteragent.execution-workspace-delta/1";
pub const WORKSPACE_DELTA_PATH: &str = "__workspace_delta__.json";
pub const MAX_WORKSPACE_MANIFEST_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceManifest {
    pub schema_version: String,
    pub revision: String,
    pub cwd: String,
    pub entries: Vec<WorkspaceManifestEntry>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceManifestEntry {
    pub path: String,
    pub identity: Option<String>,
    pub kind: WorkspaceEntryKind,
    pub read_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_length: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceEntryKind {
    Directory,
    File,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceExecutionOutcome {
    Succeeded,
    Failed,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum WorkspaceOperation {
    Rename { from: String, to: String },
    Remove { path: String },
    Replace { path: String },
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceDelta {
    pub schema_version: String,
    pub base_revision: String,
    pub execution: WorkspaceExecutionOutcome,
    pub entries: Vec<WorkspaceManifestEntry>,
    pub operations: Vec<WorkspaceOperation>,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("Execution workspace manifest is incomplete, inconsistent or outside the authorized tree")]
pub struct WorkspaceManifestError;
type Result<T> = std::result::Result<T, WorkspaceManifestError>;

impl WorkspaceManifestEntry {
    pub fn directory(path: String, identity: Option<String>, read_only: bool) -> Self {
        Self {
            path,
            identity,
            read_only,
            kind: WorkspaceEntryKind::Directory,
            sha256: None,
            byte_length: None,
            media_type: None,
        }
    }
    pub fn file(path: String, identity: String, read_only: bool, content: &ContentRef) -> Self {
        Self {
            path,
            identity: Some(identity),
            read_only,
            kind: WorkspaceEntryKind::File,
            sha256: Some(content.sha256.to_string()),
            byte_length: Some(content.byte_length),
            media_type: Some(content.media_type.as_str().into()),
        }
    }
    pub fn matches_content(&self, content: &ContentRef) -> bool {
        self.kind == WorkspaceEntryKind::File
            && self.sha256.as_deref() == Some(content.sha256.to_string().as_str())
            && self.byte_length == Some(content.byte_length)
            && self.media_type.as_deref() == Some(content.media_type.as_str())
    }
}
impl WorkspaceManifest {
    pub fn from_snapshot(snapshot: &ExecutionWorkspaceSnapshot, cwd: String) -> Result<Self> {
        let mut entries = vec![
            WorkspaceManifestEntry::directory("work".into(), Some("root:work".into()), false),
            WorkspaceManifestEntry::directory("inputs".into(), Some("root:inputs".into()), true),
        ];
        entries.extend(snapshot.entries.iter().map(|entry| match &entry.file {
            Some(file) => WorkspaceManifestEntry::file(
                format!("work/{}", entry.path),
                entry.node_id.to_string(),
                false,
                &file.content_ref,
            ),
            None => WorkspaceManifestEntry::directory(
                format!("work/{}", entry.path),
                Some(entry.node_id.to_string()),
                false,
            ),
        }));
        if let Some(project) = &snapshot.project {
            entries.push(WorkspaceManifestEntry::directory(
                "project".into(),
                Some("root:project".into()),
                !project.writable,
            ));
            entries.extend(project.entries.iter().map(|entry| match &entry.file {
                Some(file) => WorkspaceManifestEntry::file(
                    format!("project/{}", entry.path),
                    entry.node_id.to_string(),
                    !project.writable,
                    &file.content_ref,
                ),
                None => WorkspaceManifestEntry::directory(
                    format!("project/{}", entry.path),
                    Some(entry.node_id.to_string()),
                    !project.writable,
                ),
            }));
        }
        let manifest = Self {
            schema_version: WORKSPACE_MANIFEST_VERSION.into(),
            revision: snapshot.revision.get().to_string(),
            cwd,
            entries,
        };
        validate_entries(&manifest.entries)?;
        // Selected input directories are materialized by FileQuery afterwards.
        // The complete manifest must pass validate before dispatch.
        if !canonical_path(&manifest.cwd) {
            return Err(WorkspaceManifestError);
        }
        Ok(manifest)
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != WORKSPACE_MANIFEST_VERSION || !canonical_revision(&self.revision)
        {
            return Err(WorkspaceManifestError);
        }
        validate_entries(&self.entries)?;
        if !self
            .entries
            .iter()
            .any(|e| e.path == self.cwd && e.kind == WorkspaceEntryKind::Directory)
            || !self.entries.iter().any(|e| {
                e.path == "work" && e.kind == WorkspaceEntryKind::Directory && !e.read_only
            })
        {
            return Err(WorkspaceManifestError);
        }
        Ok(())
    }
    pub fn writable(&self, path: &str) -> bool {
        if !canonical_path(path) {
            return false;
        }
        let mut parent = path;
        loop {
            if let Some(entry) = self.entries.iter().find(|e| e.path == parent) {
                return !entry.read_only;
            }
            let Some((prefix, _)) = parent.rsplit_once('/') else {
                return false;
            };
            parent = prefix;
        }
    }
}
impl WorkspaceDelta {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_WORKSPACE_MANIFEST_BYTES {
            return Err(WorkspaceManifestError);
        }
        serde_json::from_slice(bytes).map_err(|_| WorkspaceManifestError)
    }
    /// The immutable baseline is the only authority for IDs, mounts and access.
    /// A complete manifest is required, even if no file body has changed.
    pub fn validate_against(
        &self,
        baseline: &WorkspaceManifest,
        outputs: &BTreeMap<String, ContentRef>,
    ) -> Result<()> {
        baseline.validate()?;
        if self.schema_version != WORKSPACE_DELTA_VERSION
            || self.base_revision != baseline.revision
            || self.operations.len() > 20_000
        {
            return Err(WorkspaceManifestError);
        }
        validate_entries(&self.entries)?;
        let mut identities: BTreeMap<String, String> = baseline
            .entries
            .iter()
            .filter_map(|e| e.identity.as_ref().map(|id| (e.path.clone(), id.clone())))
            .collect();
        for operation in &self.operations {
            let target = match operation {
                WorkspaceOperation::Rename { to, .. } => to,
                WorkspaceOperation::Remove { path } | WorkspaceOperation::Replace { path } => path,
            };
            if !target.contains('/') || !baseline.writable(target) {
                return Err(WorkspaceManifestError);
            }
            match operation {
                WorkspaceOperation::Replace { .. } => {}
                WorkspaceOperation::Remove { path } => {
                    identities.retain(|p, _| p != path && !p.starts_with(&format!("{path}/")))
                }
                WorkspaceOperation::Rename { from, to } => {
                    if !from.contains('/') || !baseline.writable(from) {
                        return Err(WorkspaceManifestError);
                    }
                    let moved: Vec<_> = identities
                        .iter()
                        .filter(|(p, _)| *p == from || p.starts_with(&format!("{from}/")))
                        .map(|(p, id)| (p.clone(), id.clone()))
                        .collect();
                    if !moved.is_empty() {
                        identities.retain(|p, _| p != to && !p.starts_with(&format!("{to}/")));
                        for (path, _) in &moved {
                            identities.remove(path);
                        }
                        for (path, id) in moved {
                            identities.insert(format!("{to}{}", &path[from.len()..]), id);
                        }
                    }
                }
            }
        }
        let original: BTreeMap<_, _> = baseline
            .entries
            .iter()
            .filter_map(|e| e.identity.as_ref().map(|id| (id, e)))
            .collect();
        let current: BTreeMap<_, _> = self.entries.iter().map(|e| (&e.path, e)).collect();
        for entry in &baseline.entries {
            if (entry.read_only || !entry.path.contains('/'))
                && current.get(&entry.path).copied() != Some(entry)
            {
                return Err(WorkspaceManifestError);
            }
        }
        let mut changed = BTreeSet::new();
        for entry in &self.entries {
            if entry.identity.as_ref() != identities.get(&entry.path)
                || entry.read_only == baseline.writable(&entry.path)
            {
                return Err(WorkspaceManifestError);
            }
            if entry.read_only && !baseline.entries.iter().any(|e| e == entry) {
                return Err(WorkspaceManifestError);
            }
            if entry
                .identity
                .as_ref()
                .and_then(|id| original.get(id))
                .is_some_and(|old| old.kind != entry.kind)
            {
                return Err(WorkspaceManifestError);
            }
            if entry.kind != WorkspaceEntryKind::File {
                continue;
            }
            let same = entry
                .identity
                .as_ref()
                .and_then(|id| original.get(id))
                .is_some_and(|old| {
                    old.kind == entry.kind
                        && old.sha256 == entry.sha256
                        && old.byte_length == entry.byte_length
                        && old.media_type == entry.media_type
                });
            if !same {
                if entry.read_only
                    || !outputs
                        .get(&entry.path)
                        .is_some_and(|content| entry.matches_content(content))
                {
                    return Err(WorkspaceManifestError);
                }
                changed.insert(entry.path.clone());
            }
        }
        if changed != outputs.keys().cloned().collect() {
            return Err(WorkspaceManifestError);
        }
        Ok(())
    }
}

fn canonical_revision(value: &str) -> bool {
    value.parse::<u64>().is_ok_and(|n| n.to_string() == value)
}
fn canonical_path(path: &str) -> bool {
    path.len() <= 512
        && matches!(path.split('/').next(), Some("work" | "inputs" | "project"))
        && path.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && !part.contains('\\')
                && !part.chars().any(char::is_control)
                && musteragent_resource::ResourceName::new(part)
                    .is_ok_and(|name| name.as_str() == part)
        })
}
fn validate_entries(entries: &[WorkspaceManifestEntry]) -> Result<()> {
    if entries.len() > 10000 {
        return Err(WorkspaceManifestError);
    }
    let mut paths = BTreeMap::new();
    let mut aliases = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for entry in entries {
        if !canonical_path(&entry.path)
            || !aliases.insert(entry.path.to_lowercase())
            || entry.identity.as_ref().is_some_and(|id| {
                id.is_empty()
                    || id.len() > 128
                    || !id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
                    || !ids.insert(id)
            })
            || ((entry.path == "inputs" || entry.path.starts_with("inputs/")) && !entry.read_only)
        {
            return Err(WorkspaceManifestError);
        }
        match entry.kind {
            WorkspaceEntryKind::Directory
                if entry.sha256.is_none()
                    && entry.byte_length.is_none()
                    && entry.media_type.is_none() => {}
            WorkspaceEntryKind::File
                if entry
                    .sha256
                    .as_ref()
                    .is_some_and(|s| Sha256::from_hex(s).is_ok() && *s == s.to_lowercase())
                    && entry
                        .byte_length
                        .is_some_and(|n| n <= 9_007_199_254_740_991)
                    && entry
                        .media_type
                        .as_ref()
                        .is_some_and(|s| MediaType::new(s).is_ok()) => {}
            _ => return Err(WorkspaceManifestError),
        }
        paths.insert(&entry.path, entry);
    }
    for entry in entries {
        if let Some((parent, _)) = entry.path.rsplit_once('/') {
            if !paths.get(&parent.to_owned()).is_some_and(|e| {
                e.kind == WorkspaceEntryKind::Directory && (!e.read_only || entry.read_only)
            }) {
                return Err(WorkspaceManifestError);
            }
        } else if entry.kind != WorkspaceEntryKind::Directory {
            return Err(WorkspaceManifestError);
        }
    }
    Ok(())
}
