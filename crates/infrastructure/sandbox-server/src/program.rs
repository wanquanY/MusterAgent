//! Restore the browser input/output and workspace contracts for native Python.
use crate::{
    config::ServerSandboxConfig,
    paths::{JobDirs, safe_relative_path},
};
use musteragent_capability::{BrowserJobProgram, BrowserJobRequest};
use musteragent_runtime_resource::{WorkspaceEntryKind, WorkspaceManifest};
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt};

pub(crate) fn prepare(
    request: &BrowserJobRequest,
    dirs: &JobDirs,
    config: &ServerSandboxConfig,
    expected: Option<Value>,
) -> Result<Option<WorkspaceManifest>, String> {
    let bytes = |reference: &musteragent_content_ref::ContentRef| -> Result<Vec<u8>, String> {
        let input = request
            .inputs
            .iter()
            .find(|i| i.content_ref == *reference)
            .ok_or("program input missing")?;
        fs::read(dirs.input.join(&input.path)).map_err(|e| e.to_string())
    };
    let mut workspace = None;
    let (kind, entrypoint, cwd, env, installation, scope) = match &request.program {
        BrowserJobProgram::Python {
            entrypoint,
            environment,
            ..
        } => {
            let (env, id, scope) = environment_material(environment.as_ref(), expected.as_ref())?;
            (
                "python",
                format!("/sandbox/work/{entrypoint}"),
                "/sandbox/work".to_owned(),
                env,
                id,
                scope,
            )
        }
        BrowserJobProgram::PythonEnvironment {
            requirements,
            requires_python,
            scope_key,
        } => (
            "environment",
            String::new(),
            "/sandbox/work".into(),
            Some(json!({"requirements":requirements,"requiresPython":requires_python})),
            request.job_id.to_string(),
            scope_key.to_string(),
        ),
        BrowserJobProgram::PythonWorkspace {
            workspace_ref,
            source_ref,
            environment,
            ..
        } => {
            let manifest: WorkspaceManifest =
                serde_json::from_slice(&bytes(workspace_ref)?).map_err(|e| e.to_string())?;
            manifest.validate().map_err(|e| e.to_string())?;
            for entry in &manifest.entries {
                let path = safe_relative_path(&entry.path).map_err(|e| e.to_string())?;
                let target = dirs.work.join(&path);
                match entry.kind {
                    WorkspaceEntryKind::Directory => {
                        fs::create_dir_all(&target).map_err(|e| e.to_string())?
                    }
                    WorkspaceEntryKind::File => {
                        let input = request
                            .inputs
                            .iter()
                            .find(|i| i.path == entry.path && entry.matches_content(&i.content_ref))
                            .ok_or("workspace input mismatch")?;
                        if !entry.read_only {
                            fs::remove_file(&target).map_err(|e| e.to_string())?;
                            fs::copy(dirs.input.join(&input.path), &target)
                                .map_err(|e| e.to_string())?;
                            fs::set_permissions(&target, fs::Permissions::from_mode(0o644))
                                .map_err(|e| e.to_string())?;
                        }
                    }
                }
            }
            let source = request
                .inputs
                .iter()
                .find(|i| i.content_ref == *source_ref)
                .ok_or("source missing")?;
            let cwd = format!("/sandbox/work/{}", manifest.cwd);
            let (env, id, scope) = environment_material(environment.as_ref(), expected.as_ref())?;
            workspace = Some(manifest);
            (
                "workspace",
                format!("/sandbox/input/{}", source.path),
                cwd,
                env,
                id,
                scope,
            )
        }
        _ => return Err("unsupported program".into()),
    };
    let support = dirs.input.join("__kernel__");
    fs::create_dir(&support).map_err(|e| e.to_string())?;
    let job = json!({"kind":kind,"entrypoint":entrypoint,"cwd":cwd,"environment":env,"expected":expected,
        "installation":installation,"scope":scope,"runtime":config.image.reference,
        "inputs":request.inputs.iter().map(|i|&i.path).collect::<Vec<_>>(),
        "outputs":request.declared_outputs.iter().map(|o|&o.path).collect::<Vec<_>>()});
    for (name, body) in [
        ("job.json", job.to_string()),
        ("runner.py", include_str!("scripts/runner.py").into()),
        (
            "environment.py",
            include_str!("scripts/environment.py").into(),
        ),
        (
            "musteragent_io.py",
            include_str!("scripts/musteragent_io.py").into(),
        ),
    ] {
        let path = support.join(name);
        fs::write(&path, body).map_err(|e| e.to_string())?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o444)).map_err(|e| e.to_string())?;
    }
    Ok(workspace)
}
fn environment_material(
    binding: Option<&musteragent_capability::BrowserPythonEnvironmentBinding>,
    expected: Option<&Value>,
) -> Result<(Option<Value>, String, String), String> {
    match binding {
        None => Ok((None, String::new(), String::new())),
        Some(binding) => {
            let expected = expected
                .ok_or("python_environment_unavailable: prepared receipt is unavailable")?;
            if expected["schema"] != "musteragent.python-environment/1"
                || expected["installationId"] != binding.installation_id.to_string()
                || expected["scopeKey"] != binding.scope_key.to_string()
                || expected["revisionHash"] != binding.revision_hash.to_string()
            {
                return Err("python_environment_unavailable: binding mismatch".into());
            }
            Ok((
                Some(expected["requirements"].clone()),
                binding.installation_id.to_string(),
                binding.scope_key.to_string(),
            ))
        }
    }
}

pub(crate) use crate::workspace::workspace_outputs;
