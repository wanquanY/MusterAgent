//! Real native jobs exercise the same ContentRef, @4 delta and receipt codecs
//! used by browser execution. Docker/image prerequisites are mandatory.
mod support;
use musteragent_capability::*;
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_runtime_resource::*;
use musteragent_sandbox_server::{DockerRuntime, JobExecutor, job::Prepared};
use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

struct Harness {
    root: tempfile::TempDir,
    content: Arc<support::FakeContentStore>,
    fixture: support::JobFixture,
    executor: JobExecutor,
}
impl Harness {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temp");
        let content = Arc::new(support::FakeContentStore::default());
        let fixture = support::JobFixture::new();
        let executor = Self::executor(&root, &content);
        Self {
            root,
            content,
            fixture,
            executor,
        }
    }
    fn executor(root: &tempfile::TempDir, content: &Arc<support::FakeContentStore>) -> JobExecutor {
        JobExecutor::new(
            Arc::new(support::config(root.path(), "musteragent-sandbox-mcp:p6")),
            Arc::clone(content) as _,
            Arc::new(DockerRuntime::connect_local().expect("Docker")),
        )
    }
    async fn run(
        &self,
        request: &BrowserJobRequest,
        cancel_ready: Option<&str>,
    ) -> BrowserJobResult {
        request.validate().expect("request contract");
        let job = match self.executor.prepare(self.fixture.tenant_id, request).await {
            Prepared::Ready(j) => j,
            Prepared::Terminal(r) => return r,
        };
        let name = job.spec().name.clone();
        let token = CancellationToken::new();
        let cancellation = cancel_ready.map(|path| {
            let t = token.clone();
            let container = name.clone();
            let path = path.to_owned();
            tokio::spawn(async move {
                // Cancel only after the workload, not merely Docker startup,
                // has produced the partial file and flushed both streams.
                tokio::time::timeout(Duration::from_secs(20), async {
                    loop {
                        let probe = tokio::process::Command::new("docker")
                            .args(["exec", &container, "test", "-f", &path])
                            .kill_on_drop(true)
                            .output()
                            .await
                            .expect("probe this test's container");
                        if probe.status.success() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .expect("workload must become ready before cancellation");
                t.cancel();
            })
        });
        let outcome = self.executor.run(&job, token).await;
        if let Some(cancellation) = cancellation {
            cancellation.await.expect("cancellation task");
        }
        let result = self
            .executor
            .finish(self.fixture.tenant_id, request, job, outcome, None)
            .await;
        request
            .validate_result(&result)
            .expect("shared receipt contract");
        let wire = musteragent_persistence_codec::encode_browser_job_result(&result)
            .expect("encode shared wire");
        let restored = musteragent_persistence_codec::decode_browser_job_result(wire)
            .expect("decode shared wire");
        assert_eq!(result, restored);
        assert_eq!(result.result_hash, result.canonical_hash());
        assert!(
            !std::process::Command::new("docker")
                .args(["inspect", &name])
                .output()
                .expect("inspect")
                .status
                .success()
        );
        result
    }
    fn bytes(&self, result: &BrowserJobResult, path: &str) -> Vec<u8> {
        self.content
            .bytes(
                self.fixture.tenant_id,
                &result
                    .outputs
                    .iter()
                    .find(|o| o.path == path)
                    .unwrap_or_else(|| panic!("missing {path}: {result:?}"))
                    .content_ref,
            )
            .expect("stored content")
    }
    fn workspace(&self, source: &str) -> (BrowserJobRequest, WorkspaceManifest) {
        let data = b"old contents";
        let mut req = self.fixture.python_job(
            &self.content,
            source,
            &[
                ("work/old.txt", data),
                ("work/deleted.txt", b"remove"),
                ("inputs/read-only.txt", b"immutable"),
            ],
            &[],
            15000,
        );
        let get = |p: &str| {
            &req.inputs
                .iter()
                .find(|i| i.path == p)
                .expect("input")
                .content_ref
        };
        let manifest = WorkspaceManifest {
            schema_version: WORKSPACE_MANIFEST_VERSION.into(),
            revision: "7".into(),
            cwd: "work".into(),
            entries: vec![
                WorkspaceManifestEntry::directory("work".into(), Some("root:work".into()), false),
                WorkspaceManifestEntry::directory(
                    "inputs".into(),
                    Some("root:inputs".into()),
                    true,
                ),
                WorkspaceManifestEntry::file(
                    "work/old.txt".into(),
                    "file:old".into(),
                    false,
                    get("work/old.txt"),
                ),
                WorkspaceManifestEntry::file(
                    "work/deleted.txt".into(),
                    "file:deleted".into(),
                    false,
                    get("work/deleted.txt"),
                ),
                WorkspaceManifestEntry::file(
                    "inputs/read-only.txt".into(),
                    "file:input".into(),
                    true,
                    get("inputs/read-only.txt"),
                ),
            ],
        };
        manifest.validate().expect("manifest");
        let manifest_ref = self.content.insert(
            self.fixture.tenant_id,
            "application/json",
            &serde_json::to_vec(&manifest).expect("json"),
        );
        req.program = BrowserJobProgram::PythonWorkspace {
            execution_workspace_id: ExecutionWorkspaceId::generate(),
            source_ref: req.inputs[0].content_ref.clone(),
            workspace_ref: manifest_ref.clone(),
            environment_manifest_ref: None,
            entrypoint: "main.py".into(),
            environment: None,
        };
        req.inputs.push(BrowserInput {
            path: "workspace.json".into(),
            content_ref: manifest_ref,
        });
        req.limits.maximum_input_bytes =
            NonZeroU64::new(req.inputs.iter().map(|i| i.content_ref.byte_length).sum())
                .expect("input");
        req.limits.maximum_output_bytes = NonZeroU64::new(65536).expect("output");
        req.limits.maximum_files = NonZeroU32::new(20).expect("files");
        (req, manifest)
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_failure_timeout_and_cancellation_preserve_partial_files_and_bounded_tails() {
    let h = Harness::new();
    for (tail, cancel, status, code) in [
        (
            "raise RuntimeError('boom')",
            None,
            BrowserJobStatus::Failed,
            "python_exit_nonzero",
        ),
        (
            "while True: pass",
            None,
            BrowserJobStatus::ResourceExceeded,
            "wall_time_exceeded",
        ),
        (
            "while True: pass",
            Some("/tmp/partial-ready"),
            BrowserJobStatus::Cancelled,
            "cancelled",
        ),
    ] {
        let source = format!(
            "from musteragent_io import input_path,output_path,list_inputs\nimport sys\nassert input_path('input.txt').read_text()=='hello'\nassert list_inputs()==('input.txt','main.py')\noutput_path('partial.txt').write_text('partial bytes')\nprint('stdout-before',flush=True)\nprint('stderr-before',file=sys.stderr,flush=True)\nopen('/tmp/partial-ready','w').close()\n{tail}\n"
        );
        let req = h.fixture.python_job(
            &h.content,
            &source,
            &[("input.txt", b"hello")],
            &[
                ("partial.txt", "text/plain", 100, true),
                ("missing.txt", "text/plain", 100, true),
            ],
            if cancel.is_some() { 10000 } else { 1800 },
        );
        let result = h.run(&req, cancel).await;
        assert_eq!(
            result.status, status,
            "{:?} {}",
            result.diagnostics, result.stderr_tail
        );
        assert!(result.diagnostics.iter().any(|d| d.code == code));
        assert_eq!(h.bytes(&result, "partial.txt"), b"partial bytes");
        assert!(result.stdout_tail.contains("stdout-before"));
        assert!(result.stderr_tail.contains("stderr-before"));
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_tmpfs_output_and_stream_quotas_are_typed_and_keep_valid_partial_output() {
    let h = Harness::new();
    for (body, expected) in [
        (
            "open('/tmp/large','wb').write(b'x'*20000000)",
            "disk_quota_exceeded",
        ),
        (
            "open('large.txt','wb').write(b'x'*20000000)",
            "disk_quota_exceeded",
        ),
        ("open('large.txt','wb').write(b'x'*150)", "output_too_large"),
        ("print('Z'*100000,flush=True)", "stdout_limit_exceeded"),
    ] {
        let source =
            format!("open('a.txt','w').write('valid')\nprint('before',flush=True)\n{body}");
        let mut req = h.fixture.python_job(
            &h.content,
            &source,
            &[],
            &[
                ("a.txt", "text/plain", 100, true),
                ("large.txt", "text/plain", 100, false),
            ],
            15000,
        );
        req.limits.maximum_files = NonZeroU32::new(4).expect("files");
        let result = h.run(&req, None).await;
        assert_eq!(
            result.status,
            BrowserJobStatus::ResourceExceeded,
            "{:?} {}",
            result.diagnostics,
            result.stderr_tail
        );
        assert!(
            result.diagnostics.iter().any(|d| d.code == expected),
            "{:?}",
            result.diagnostics
        );
        assert_eq!(h.bytes(&result, "a.txt"), b"valid");
        assert!(result.stdout_tail.len() <= MAX_BROWSER_JOB_LOG_TAIL_BYTES);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_workspace_delta_preserves_renamed_identity_and_partial_changes() {
    let h = Harness::new();
    for fail in [false, true] {
        let source = format!(
            "import os,subprocess\nassert os.getcwd()=='/sandbox/work/work'\nassert open('../inputs/read-only.txt').read()=='immutable'\nsubprocess.run(['mv','old.txt','renamed.txt'],check=True)\nopen('renamed.txt','w').write('changed')\nos.unlink('deleted.txt')\nopen('new.txt','w').write('new')\n{}",
            if fail {
                "raise RuntimeError('partial')"
            } else {
                "print('workspace complete')"
            }
        );
        let (req, baseline) = h.workspace(&source);
        let result = h.run(&req, None).await;
        assert_eq!(
            result.status,
            if fail {
                BrowserJobStatus::Failed
            } else {
                BrowserJobStatus::Completed
            },
            "{:?} {}",
            result.diagnostics,
            result.stderr_tail
        );
        let delta =
            WorkspaceDelta::parse(&h.bytes(&result, WORKSPACE_DELTA_PATH)).expect("shared delta");
        let outputs: BTreeMap<_, _> = result
            .outputs
            .iter()
            .filter(|o| o.path != WORKSPACE_DELTA_PATH)
            .map(|o| (o.path.clone(), o.content_ref.clone()))
            .collect();
        delta
            .validate_against(&baseline, &outputs)
            .expect("resource owner acceptance");
        assert_eq!(
            delta.execution,
            if fail {
                WorkspaceExecutionOutcome::Failed
            } else {
                WorkspaceExecutionOutcome::Succeeded
            }
        );
        assert_eq!(
            delta
                .entries
                .iter()
                .find(|e| e.path == "work/renamed.txt")
                .expect("renamed")
                .identity
                .as_deref(),
            Some("file:old")
        );
        assert!(delta.entries.iter().all(|e| e.path != "work/deleted.txt"));
        assert_eq!(h.bytes(&result, "work/renamed.txt"), b"changed");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_environment_receipt_restores_from_contentref_and_refuses_unavailable_packages() {
    let mut h = Harness::new();
    let mut req = h.fixture.python_job(
        &h.content,
        "pass",
        &[],
        &[("environment-lock.json", "application/json", 65536, true)],
        15000,
    );
    req.engine = BrowserJobEngine::PythonEnvironment;
    req.inputs.clear();
    let scope = Sha256::digest(b"scope");
    req.program = BrowserJobProgram::PythonEnvironment {
        requirements: vec!["uv>=0".into()],
        requires_python: Some(">=3.12,<3.13".into()),
        scope_key: scope,
    };
    let result = h.run(&req, None).await;
    assert_eq!(
        result.status,
        BrowserJobStatus::Completed,
        "{:?} {}",
        result.diagnostics,
        result.stderr_tail
    );
    let receipt: serde_json::Value =
        serde_json::from_slice(&h.bytes(&result, "environment-lock.json")).expect("receipt");
    assert_eq!(receipt["schema"], "musteragent.python-environment/1");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&result.stdout_tail).expect("stdout receipt"),
        receipt
    );
    let binding = BrowserPythonEnvironmentBinding {
        installation_id: req.job_id,
        revision_hash: Sha256::from_hex(receipt["revisionHash"].as_str().expect("revision"))
            .expect("sha"),
        scope_key: scope,
    };
    // New executor has no in-process cache: @4's ContentRef restores authority.
    h.executor = Harness::executor(&h.root, &h.content);
    let (mut follow, _) = h.workspace("open('prepared.txt','w').write('verified')");
    let reference = result.outputs[0].content_ref.clone();
    if let BrowserJobProgram::PythonWorkspace {
        environment,
        environment_manifest_ref,
        ..
    } = &mut follow.program
    {
        *environment = Some(binding);
        *environment_manifest_ref = Some(reference.clone());
    }
    follow.inputs.push(BrowserInput {
        path: "environment.json".into(),
        content_ref: reference,
    });
    follow.limits.maximum_input_bytes = NonZeroU64::new(
        follow
            .inputs
            .iter()
            .map(|i| i.content_ref.byte_length)
            .sum(),
    )
    .expect("input");
    let prepared = h.run(&follow, None).await;
    assert_eq!(
        prepared.status,
        BrowserJobStatus::Completed,
        "{:?} {}",
        prepared.diagnostics,
        prepared.stderr_tail
    );
    assert_eq!(h.bytes(&prepared, "work/prepared.txt"), b"verified");
    req.job_id = BrowserJobId::generate();
    if let BrowserJobProgram::PythonEnvironment { requirements, .. } = &mut req.program {
        *requirements = vec!["not-installed-package-for-sandbox-test==0".into()];
    }
    let missing = h.run(&req, None).await;
    assert_eq!(missing.status, BrowserJobStatus::Failed);
    assert!(
        missing
            .diagnostics
            .iter()
            .any(|d| d.code == "server_sandbox_environment_unavailable")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_missing_image_is_typed_without_host_or_browser_fallback() {
    let mut h = Harness::new();
    let mut config = support::config(h.root.path(), "musteragent-missing-image:does-not-exist-p6");
    config.image.reference = format!("musteragent-missing-image:{}", BrowserJobId::generate());
    h.executor = JobExecutor::new(
        Arc::new(config),
        Arc::clone(&h.content) as _,
        Arc::new(DockerRuntime::connect_local().expect("Docker")),
    );
    let request = h
        .fixture
        .python_job(&h.content, "print('unexpected execution')", &[], &[], 5000);
    let result = h.run(&request, None).await;
    assert_eq!(result.status, BrowserJobStatus::Failed);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.code == "server_sandbox_image_missing")
    );
    assert!(result.stdout_tail.is_empty());
    assert!(result.outputs.is_empty());
}
