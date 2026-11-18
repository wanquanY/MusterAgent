//! Selected read-only sources use the same exact-version FileQuery authority as
//! the UI and file tools. References persist, permission does not.
use musteragent_capability::BrowserInput;
use musteragent_content_ref::Sha256;
use musteragent_loop::LoopPortError;
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_file_query::{FileQueryAccess, FileQueryService, FileReference};
use musteragent_runtime_resource::{WorkspaceManifest, WorkspaceManifestEntry};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkspaceInput {
    pub path: String,
    pub file_ref: FileReference,
}

pub(super) async fn mount(
    files: &FileQueryService,
    access: &FileQueryAccess,
    selected: &[WorkspaceInput],
    manifest: &mut WorkspaceManifest,
    inputs: &mut Vec<BrowserInput>,
) -> Result<(), LoopPortError> {
    if selected.len() > 512 {
        return Err(error(ErrorCode::QuotaExceeded));
    }
    let mut entries: BTreeMap<_, _> = manifest
        .entries
        .iter()
        .map(|e| (e.path.clone(), e.clone()))
        .collect();
    for selected in selected {
        let path = selected
            .path
            .strip_prefix("/workspace/inputs/")
            .or_else(|| selected.path.strip_prefix("inputs/"))
            .unwrap_or(&selected.path);
        if path.is_empty()
            || path.len() > 490
            || path
                .split('/')
                .any(|p| p.is_empty() || p == "." || p == ".." || p.contains('\\'))
        {
            return Err(error(ErrorCode::InvalidCommand));
        }
        let path = format!("inputs/{path}");
        if entries.contains_key(&path) {
            return Err(error(ErrorCode::InvalidCommand));
        }
        let file = files.open(access, &selected.file_ref).await.map_err(|e| {
            error(match e {
                musteragent_runtime_file_query::FileQueryError::Unavailable => {
                    ErrorCode::DependencyUnavailable
                }
                musteragent_runtime_file_query::FileQueryError::Integrity => {
                    ErrorCode::ContentIntegrityFailed
                }
                _ => ErrorCode::ResourceFileUnavailable,
            })
        })?;
        if file.entry.is_directory
            || (file.artifact_origin.is_some()
                && file
                    .entry
                    .semantic_kind
                    .as_deref()
                    .is_some_and(|k| k != "file"))
        {
            return Err(error(ErrorCode::InvalidCommand));
        }
        let mut parent = path.as_str();
        while let Some((prefix, _)) = parent.rsplit_once('/') {
            entries.entry(prefix.to_owned()).or_insert_with(|| {
                WorkspaceManifestEntry::directory(
                    prefix.into(),
                    Some(format!("input-dir:{}", Sha256::digest(prefix))),
                    true,
                )
            });
            parent = prefix;
        }
        let id = format!(
            "input:{}",
            Sha256::digest(format!(
                "{path}:{}",
                serde_json::to_string(&selected.file_ref)
                    .map_err(|_| error(ErrorCode::InternalInvariantViolated))?
            ))
        );
        entries.insert(
            path.clone(),
            WorkspaceManifestEntry::file(path.clone(), id, true, &file.primary),
        );
        inputs.push(BrowserInput {
            path,
            content_ref: file.primary,
        });
    }
    manifest.entries = entries.into_values().collect();
    manifest
        .validate()
        .map_err(|_| error(ErrorCode::InvalidCommand))?;
    Ok(())
}
fn error(code: ErrorCode) -> LoopPortError {
    LoopPortError {
        code,
        retryable: code == ErrorCode::DependencyUnavailable,
    }
}
