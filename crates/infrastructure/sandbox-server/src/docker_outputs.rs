//! Capture a stopped workload's tmpfs through attached exec. Never unpack an
//! untrusted tar: validate each entry and copy only regular files ourselves.
use crate::{paths::safe_relative_path, runtime::RuntimeError, spec::ContainerSpec};
use bollard::{
    Docker,
    container::LogOutput,
    exec::{CreateExecOptions, StartExecResults},
};
use futures_util::StreamExt;
use std::{io::Read, path::Path};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaptureStatus {
    Complete,
    Quota,
    Unsafe,
}

pub(crate) struct Capture {
    input: crate::docker_session::Input,
    output: crate::docker_session::Output,
    exec_id: String,
}
impl Capture {
    /// Reserve the collector before the workload can exhaust its PID limit.
    pub async fn start(docker: &Docker, name: &str, python: &str) -> Result<Self, RuntimeError> {
        let exec = docker
            .create_exec(
                name,
                CreateExecOptions::<String> {
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    cmd: Some(vec![
                        python.into(),
                        "-u".into(),
                        "-c".into(),
                        include_str!("scripts/capture.py").into(),
                    ]),
                    working_dir: Some("/tmp".into()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| RuntimeError::Start(format!("capture startup: {e}")))?;
        let StartExecResults::Attached { input, mut output } = docker
            .start_exec(&exec.id, None)
            .await
            .map_err(|e| RuntimeError::Start(format!("capture attach: {e}")))?
        else {
            return Err(RuntimeError::Start("capture did not attach".into()));
        };
        match tokio::time::timeout(std::time::Duration::from_secs(10), output.next()).await {
            Ok(Some(Ok(LogOutput::StdOut { message }))) if message.as_ref() == b"R" => Ok(Self {
                input,
                output,
                exec_id: exec.id,
            }),
            _ => Err(RuntimeError::Start(
                "output collector readiness failed".into(),
            )),
        }
    }
}

pub(crate) async fn collect(
    docker: &Docker,
    spec: &ContainerSpec,
    mut capture: Capture,
) -> Result<CaptureStatus, RuntimeError> {
    let Some(seed) = spec.mounts.iter().find(|m| m.container == "/sandbox/seed") else {
        return Ok(CaptureStatus::Complete);
    };
    capture.input.write_all(b"C").await.map_err(io_error)?;
    capture.input.shutdown().await.map_err(io_error)?;
    let cap = (spec.limits.memory_bytes as u64).saturating_add(32 * 1024 * 1024);
    let mut bytes = Vec::new();
    while let Some(chunk) = capture.output.next().await {
        match chunk.map_err(|e| RuntimeError::Indeterminate(format!("output stream: {e}")))? {
            LogOutput::StdOut { message } => {
                if (bytes.len() + message.len()) as u64 > cap {
                    return Ok(CaptureStatus::Quota);
                }
                bytes.extend_from_slice(&message);
            }
            LogOutput::StdErr { .. } => {
                return Err(RuntimeError::Indeterminate(
                    "output collector failed".into(),
                ));
            }
            _ => {}
        }
    }
    if docker
        .inspect_exec(&capture.exec_id)
        .await
        .map_err(|e| RuntimeError::Indeterminate(e.to_string()))?
        .exit_code
        != Some(0)
    {
        return Err(RuntimeError::Indeterminate(
            "output collector exited unsuccessfully".into(),
        ));
    }
    let baseline = baseline_hashes(Path::new(&seed.host))?;
    let mut archive = tar::Archive::new(bytes.as_slice());
    let mut written = 0_u64;
    let mut files = 0_u32;
    let mut status = CaptureStatus::Complete;
    let mut origins = std::collections::BTreeMap::<String, String>::new();
    let mut present = std::collections::BTreeSet::new();
    for item in archive.entries().map_err(io_error)? {
        let mut entry = item.map_err(io_error)?;
        let path = entry.path().map_err(io_error)?.into_owned();
        let Some(relative) = path.strip_prefix("work").ok().and_then(Path::to_str) else {
            status = CaptureStatus::Unsafe;
            continue;
        };
        if relative.is_empty() {
            continue;
        }
        let Ok(relative_path) = safe_relative_path(relative) else {
            status = CaptureStatus::Unsafe;
            continue;
        };
        present.insert(relative.to_owned());
        if let Some(extensions) = entry.pax_extensions().map_err(io_error)? {
            for extension in extensions {
                let extension = extension.map_err(io_error)?;
                if extension.key().ok() == Some("musteragent.original-path") {
                    let prior = extension
                        .value()
                        .map_err(|e| RuntimeError::Indeterminate(e.to_string()))?;
                    safe_relative_path(prior)
                        .map_err(|_| RuntimeError::Indeterminate("invalid original path".into()))?;
                    if baseline.files.contains_key(prior) || baseline.directories.contains(prior) {
                        origins.insert(relative.to_owned(), prior.to_owned());
                    }
                }
            }
        }
        let kind = entry.header().entry_type();
        let dest = Path::new(&seed.host).join(&relative_path);
        if kind.is_symlink() {
            // Only the immutable input projections created by the executor survive.
            let target = entry.link_name().map_err(io_error)?;
            let expected = Path::new("/sandbox/input").join(&relative_path);
            if target.as_deref() != Some(expected.as_path())
                || std::fs::read_link(&dest).ok().as_deref() != Some(expected.as_path())
            {
                status = CaptureStatus::Unsafe;
            }
            continue;
        }
        if !kind.is_dir() && !kind.is_file() {
            status = CaptureStatus::Unsafe;
            continue;
        }
        // Existing seed directories are ours; refuse aliases at every component.
        let mut parent = Path::new(&seed.host).to_path_buf();
        let parts: Vec<_> = relative_path.components().collect();
        let mut safe = true;
        for part in parts.iter().take(parts.len().saturating_sub(1)) {
            parent.push(part);
            match std::fs::symlink_metadata(&parent) {
                Ok(m) if m.is_dir() && !m.file_type().is_symlink() => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir(&parent).map_err(io_error)?
                }
                _ => {
                    safe = false;
                    break;
                }
            }
        }
        if !safe {
            status = CaptureStatus::Unsafe;
            continue;
        }
        if kind.is_dir() {
            if std::fs::symlink_metadata(&dest).is_ok_and(|m| m.file_type().is_symlink()) {
                status = CaptureStatus::Unsafe;
                continue;
            }
            if dest.is_file() {
                std::fs::remove_file(&dest).map_err(io_error)?;
            }
            if !dest.exists() {
                std::fs::create_dir(&dest).map_err(io_error)?;
            }
            continue;
        }
        if std::fs::symlink_metadata(&dest).is_ok_and(|m| m.file_type().is_symlink()) {
            // A workload replaced a read-only input projection. Never follow it.
            status = CaptureStatus::Unsafe;
            continue;
        }
        let size = entry.size();
        let old_path = origins.get(relative).map_or(relative, String::as_str);
        let old = baseline.files.get(old_path);
        if old.is_none_or(|(length, _)| *length != size)
            && (written.saturating_add(size) > spec.limits.output_bytes
                || files >= spec.limits.output_files)
        {
            status = CaptureStatus::Quota;
            continue;
        }
        let mut contents = Vec::new();
        entry
            .by_ref()
            .take(size + 1)
            .read_to_end(&mut contents)
            .map_err(io_error)?;
        if contents.len() as u64 != size {
            return Err(RuntimeError::Indeterminate(
                "truncated output archive".into(),
            ));
        }
        if old.is_none_or(|(length, hash)| {
            *length != size || *hash != musteragent_content_ref::Sha256::digest(&contents)
        }) {
            if written.saturating_add(size) > spec.limits.output_bytes
                || files >= spec.limits.output_files
            {
                status = CaptureStatus::Quota;
                continue;
            }
            written += size;
            files += 1;
        }
        if dest.is_dir() {
            std::fs::remove_dir_all(&dest).map_err(io_error)?;
        }
        std::fs::write(dest, contents).map_err(io_error)?;
    }
    // Apply deletions only inside this job's seed tree. Immutable input links
    // remain owned by the input projection and cannot be deleted by an archive.
    let mut stack = vec![std::path::PathBuf::from(&seed.host)];
    while let Some(dir) = stack.pop() {
        for item in std::fs::read_dir(&dir).map_err(io_error)? {
            let item = item.map_err(io_error)?;
            let ty = item.file_type().map_err(io_error)?;
            let path = item.path();
            let relative = path
                .strip_prefix(&seed.host)
                .map_err(|_| RuntimeError::Indeterminate("snapshot path".into()))?
                .to_string_lossy()
                .into_owned();
            if ty.is_symlink() {
                continue;
            }
            if !present.contains(&relative) {
                if ty.is_dir() {
                    std::fs::remove_dir_all(&path).map_err(io_error)?;
                } else {
                    std::fs::remove_file(&path).map_err(io_error)?;
                }
            } else if ty.is_dir() {
                stack.push(path);
            }
        }
    }
    let metadata = Path::new(&seed.host)
        .parent()
        .ok_or_else(|| RuntimeError::Indeterminate("seed parent missing".into()))?
        .join("workspace-origins.json");
    std::fs::write(
        metadata,
        serde_json::to_vec(&origins).map_err(|e| RuntimeError::Indeterminate(e.to_string()))?,
    )
    .map_err(io_error)?;
    Ok(status)
}
#[derive(Default)]
struct Baseline {
    files: std::collections::BTreeMap<String, (u64, musteragent_content_ref::Sha256)>,
    directories: std::collections::BTreeSet<String>,
}
fn baseline_hashes(root: &Path) -> Result<Baseline, RuntimeError> {
    let mut result = Baseline::default();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let ty = entry.file_type().map_err(io_error)?;
            if ty.is_dir() {
                let path = entry.path();
                let relative = path
                    .strip_prefix(root)
                    .ok()
                    .and_then(Path::to_str)
                    .ok_or_else(|| {
                        RuntimeError::Indeterminate("baseline directory invalid".into())
                    })?;
                result.directories.insert(relative.to_owned());
                stack.push(path);
            } else if ty.is_file() {
                let path = entry.path();
                let relative = path
                    .strip_prefix(root)
                    .ok()
                    .and_then(Path::to_str)
                    .ok_or_else(|| RuntimeError::Indeterminate("baseline path invalid".into()))?
                    .to_owned();
                let data = std::fs::read(path).map_err(io_error)?;
                result.files.insert(
                    relative,
                    (
                        data.len() as u64,
                        musteragent_content_ref::Sha256::digest(data),
                    ),
                );
            }
        }
    }
    Ok(result)
}
fn io_error(error: std::io::Error) -> RuntimeError {
    RuntimeError::Indeterminate(format!("output capture: {error}"))
}
