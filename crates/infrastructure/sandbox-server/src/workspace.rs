//! Native filesystem projection of the shared browser workspace delta contract.
use crate::paths::{JobDirs, safe_relative_path};
use musteragent_capability::BrowserDeclaredOutput;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::ContentId;
use musteragent_runtime_resource::{
    WORKSPACE_DELTA_PATH, WORKSPACE_DELTA_VERSION, WorkspaceDelta, WorkspaceEntryKind,
    WorkspaceExecutionOutcome, WorkspaceManifest, WorkspaceManifestEntry, WorkspaceOperation,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

pub(crate) fn workspace_outputs(
    dirs: &JobDirs,
    baseline: &WorkspaceManifest,
    success: bool,
    limit: u64,
    max_files: u32,
) -> Result<Vec<(BrowserDeclaredOutput, Vec<u8>)>, String> {
    let origins: BTreeMap<String, String> = match fs::read(dirs.root.join("workspace-origins.json"))
    {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| e.to_string())?,
        Err(e) => return Err(format!("workspace provenance unavailable: {e}")),
    };
    let original: BTreeMap<_, _> = baseline
        .entries
        .iter()
        .map(|e| (e.path.clone(), e))
        .collect();
    let mut entries = baseline
        .entries
        .iter()
        .filter(|e| e.read_only || !e.path.contains('/'))
        .cloned()
        .collect::<Vec<_>>();
    let mut files = Vec::new();
    let mut total = 0;
    let mut used = BTreeSet::new();
    let mut moves = Vec::new();
    let mut replacements = Vec::new();
    let mut stack = vec!["work".to_owned()];
    if baseline
        .entries
        .iter()
        .any(|e| e.path == "project" && !e.read_only)
    {
        stack.push("project".into());
    }
    while let Some(parent) = stack.pop() {
        let mut children = fs::read_dir(dirs.work.join(&parent))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        children.sort_by_key(|e| e.file_name());
        for item in children {
            let path = format!(
                "{parent}/{}",
                item.file_name().to_str().ok_or("non-UTF8 output path")?
            );
            safe_relative_path(&path).map_err(|e| e.to_string())?;
            if !baseline.writable(&path) {
                continue;
            }
            let ty = item.file_type().map_err(|e| e.to_string())?;
            let kind = if ty.is_dir() {
                WorkspaceEntryKind::Directory
            } else if ty.is_file() {
                WorkspaceEntryKind::File
            } else {
                return Err("non-regular workspace output".into());
            };
            let prior = origins.get(&path).unwrap_or(&path);
            let old = original
                .get(prior)
                .copied()
                .filter(|e| !e.read_only && e.kind == kind);
            let mut entry = old
                .cloned()
                .unwrap_or_else(|| WorkspaceManifestEntry::directory(path.clone(), None, false));
            entry.path = path.clone();
            entry.kind = kind;
            if let Some(old) = old {
                if !used.insert(old.path.clone()) {
                    return Err("duplicate workspace provenance".into());
                }
                if old.path != path {
                    moves.push((old.path.clone(), path.clone()));
                }
            }
            match kind {
                WorkspaceEntryKind::Directory => stack.push(path.clone()),
                WorkspaceEntryKind::File => {
                    let allowance = old
                        .and_then(|e| e.byte_length)
                        .unwrap_or(0)
                        .max(limit.saturating_sub(total));
                    let data = dirs
                        .read_output(&path, allowance)
                        .map_err(|e| e.to_string())?
                        .ok_or("workspace file disappeared")?;
                    let hash = Sha256::digest(&data).to_string();
                    let changed = old.is_none_or(|e| e.sha256.as_ref() != Some(&hash));
                    entry.sha256 = Some(hash);
                    entry.byte_length = Some(data.len() as u64);
                    entry.media_type = Some(
                        old.and_then(|e| e.media_type.clone())
                            .unwrap_or_else(|| "application/octet-stream".into()),
                    );
                    if changed {
                        total += data.len() as u64;
                        if total > limit || files.len() >= max_files as usize {
                            return Err("workspace output quota".into());
                        }
                        if old.is_some() {
                            replacements.push(WorkspaceOperation::Replace { path: path.clone() });
                        }
                        files.push((
                            declaration(
                                &path,
                                entry.media_type.as_deref().ok_or("media missing")?,
                                data.len() as u64,
                            )?,
                            data,
                        ));
                    }
                }
            }
            entries.push(entry);
            if entries.len() > 10000 {
                return Err("workspace entry quota".into());
            }
        }
    }
    // Stage renames through unused paths. This preserves identities for swaps,
    // directory moves and overwritten targets, including subprocess renames.
    moves.sort_by(|a, b| {
        b.0.matches('/')
            .count()
            .cmp(&a.0.matches('/').count())
            .then(a.0.cmp(&b.0))
    });
    let occupied: BTreeSet<_> = baseline
        .entries
        .iter()
        .chain(&entries)
        .map(|e| e.path.as_str())
        .collect();
    let mut operations = Vec::new();
    let mut staged = Vec::new();
    let mut sequence = 0;
    for (from, to) in moves {
        let temporary = loop {
            let candidate = format!("work/.sandbox-rename-{sequence}");
            sequence += 1;
            if !occupied
                .iter()
                .any(|p| *p == candidate || p.starts_with(&format!("{candidate}/")))
            {
                break candidate;
            }
        };
        operations.push(WorkspaceOperation::Rename {
            from,
            to: temporary.clone(),
        });
        staged.push((temporary, to));
    }
    for old in &baseline.entries {
        if !old.read_only && old.path.contains('/') && !used.contains(&old.path) {
            operations.push(WorkspaceOperation::Remove {
                path: old.path.clone(),
            });
        }
    }
    staged.sort_by(|a, b| {
        a.1.matches('/')
            .count()
            .cmp(&b.1.matches('/').count())
            .then(a.1.cmp(&b.1))
    });
    operations.extend(
        staged
            .into_iter()
            .map(|(from, to)| WorkspaceOperation::Rename { from, to }),
    );
    operations.extend(replacements);
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    let delta = WorkspaceDelta {
        schema_version: WORKSPACE_DELTA_VERSION.into(),
        base_revision: baseline.revision.clone(),
        execution: if success {
            WorkspaceExecutionOutcome::Succeeded
        } else {
            WorkspaceExecutionOutcome::Failed
        },
        entries,
        operations,
    };
    let references = files
        .iter()
        .map(|(declared, bytes)| {
            (
                declared.path.clone(),
                ContentRef {
                    content_id: ContentId::generate(),
                    media_type: declared.media_type.clone(),
                    byte_length: bytes.len() as u64,
                    sha256: Sha256::digest(bytes),
                },
            )
        })
        .collect();
    delta
        .validate_against(baseline, &references)
        .map_err(|e| e.to_string())?;
    let body = serde_json::to_vec(&delta).map_err(|e| e.to_string())?;
    if total + body.len() as u64 > limit {
        return Err("workspace output quota".into());
    }
    files.push((
        declaration(WORKSPACE_DELTA_PATH, "application/json", body.len() as u64)?,
        body,
    ));
    Ok(files)
}
fn declaration(path: &str, media: &str, bytes: u64) -> Result<BrowserDeclaredOutput, String> {
    Ok(BrowserDeclaredOutput {
        path: path.into(),
        media_type: MediaType::new(media).map_err(|e| e.to_string())?,
        maximum_byte_length: std::num::NonZeroU64::new(bytes.max(1)).ok_or("length")?,
        required: false,
    })
}
