//! Provider lease/claim/seal behaviour against in-memory ports and a
//! scripted container runtime (no Docker needed).

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use musteragent_capability::{
    BrowserDiagnosticSeverity, BrowserJobProgram, BrowserJobStatus, BrowserProviderContract,
    BrowserPythonEnvironmentBinding,
};
use musteragent_content_ref::Sha256;
use musteragent_identity::BrowserJobId;
use musteragent_runtime_capability::{BrowserGatewayStore, BrowserJobTerminalReceipt};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_sandbox_server::job::STAGING_RECEIPT_PREFIX;
use musteragent_sandbox_server::runtime::CapturedStream;
use musteragent_sandbox_server::{
    ContainerExit, ContainerOutcome, ContainerRuntime, ContainerSpec, ProviderBinding,
    RuntimeError, ServerSandboxProvider,
};
use support::{FakeContentStore, FakeGatewayStore, JobFixture};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
enum Script {
    Exit {
        code: i64,
        stdout: &'static str,
        stderr: &'static str,
        files: Vec<(&'static str, Vec<u8>)>,
        symlinks: Vec<(&'static str, &'static str)>,
    },
    BlockUntilCancelled,
}

struct ScriptedRuntime {
    script: Script,
    specs: Mutex<Vec<ContainerSpec>>,
    cancelled: Mutex<usize>,
}

impl ScriptedRuntime {
    fn new(script: Script) -> Arc<Self> {
        Arc::new(Self {
            script,
            specs: Mutex::new(Vec::new()),
            cancelled: Mutex::new(0),
        })
    }

    fn runs(&self) -> usize {
        self.specs.lock().expect("lock").len()
    }

    fn cancelled(&self) -> usize {
        *self.cancelled.lock().expect("lock")
    }
}

fn outcome(exit: ContainerExit, stdout: &str, stderr: &str) -> ContainerOutcome {
    let mut out = CapturedStream::new(1 << 20, 50 * 1024);
    out.push(stdout.as_bytes());
    let mut err = CapturedStream::new(1 << 20, 50 * 1024);
    err.push(stderr.as_bytes());
    ContainerOutcome {
        egress: Vec::new(),
        runtime_error: None,
        exit,
        oom_killed: false,
        stdout: out,
        stderr: err,
        elapsed: Duration::from_millis(7),
    }
}

impl ContainerRuntime for ScriptedRuntime {
    fn run<'a>(
        &'a self,
        spec: &'a ContainerSpec,
        cancel: CancellationToken,
    ) -> ApplicationFuture<'a, Result<ContainerOutcome, RuntimeError>> {
        Box::pin(async move {
            self.specs.lock().expect("lock").push(spec.clone());
            let work = std::path::PathBuf::from(&spec.mounts[1].host);
            match &self.script {
                Script::Exit {
                    code,
                    stdout,
                    stderr,
                    files,
                    symlinks,
                } => {
                    for (path, bytes) in files {
                        let target = work.join(path);
                        std::fs::create_dir_all(target.parent().expect("parent")).expect("mkdir");
                        std::fs::write(target, bytes).expect("write");
                    }
                    for (path, target) in symlinks {
                        std::os::unix::fs::symlink(target, work.join(path)).expect("symlink");
                    }
                    Ok(outcome(ContainerExit::Exited(*code), stdout, stderr))
                }
                Script::BlockUntilCancelled => {
                    cancel.cancelled().await;
                    *self.cancelled.lock().expect("lock") += 1;
                    Ok(outcome(ContainerExit::Cancelled, "", ""))
                }
            }
        })
    }
}

struct Harness {
    store: Arc<FakeGatewayStore>,
    content: Arc<FakeContentStore>,
    runtime: Arc<ScriptedRuntime>,
    fixture: JobFixture,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _work_root: tempfile::TempDir,
}

impl Harness {
    async fn start(script: Script) -> Self {
        let work_root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(FakeGatewayStore::default());
        let content = Arc::new(FakeContentStore::default());
        let runtime = ScriptedRuntime::new(script);
        let fixture = JobFixture::new();
        let provider = ServerSandboxProvider::new(
            support::config(work_root.path(), "python:3.12-slim"),
            ProviderBinding {
                tenant_id: fixture.tenant_id,
                principal_id: fixture.principal_id,
            },
            Arc::clone(&store) as Arc<dyn BrowserGatewayStore>,
            Arc::clone(&content) as _,
            Arc::clone(&runtime) as _,
        )
        .expect("provider");
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let task = tokio::spawn(async move {
            provider.run(token).await.expect("provider run");
        });
        let harness = Self {
            store,
            content,
            runtime,
            fixture,
            shutdown,
            task,
            _work_root: work_root,
        };
        harness.wait_for(|h| h.store.lease_count() >= 1).await;
        harness
    }

    async fn wait_for(&self, condition: impl Fn(&Self) -> bool) {
        for _ in 0..400 {
            if condition(self) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition was not reached in time");
    }

    fn enqueue(&self, request: musteragent_capability::BrowserJobRequest) -> BrowserJobId {
        let job_id = request.job_id;
        self.store
            .enqueue(
                self.fixture.tenant_id,
                self.fixture.principal_id,
                BrowserProviderContract::PythonExecute,
                request,
            )
            .expect("enqueue");
        job_id
    }

    async fn receipt(&self, job_id: BrowserJobId) -> BrowserJobTerminalReceipt {
        self.wait_for(|h| h.store.receipt(job_id).is_some()).await;
        self.store.receipt(job_id).expect("receipt")
    }

    async fn stop(self) -> Arc<FakeGatewayStore> {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("provider stops")
            .expect("provider task");
        self.store
    }
}

fn codes(receipt: &BrowserJobTerminalReceipt) -> Vec<String> {
    receipt
        .result
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code.clone())
        .collect()
}

#[tokio::test]
async fn completed_job_stages_declared_outputs_and_seals_a_valid_receipt() {
    let harness = Harness::start(Script::Exit {
        code: 0,
        stdout: "hello\n",
        stderr: "",
        files: vec![
            ("out/result.txt", b"42".to_vec()),
            ("scratch.bin", b"junk".to_vec()),
        ],
        symlinks: vec![],
    })
    .await;
    let request = harness.fixture.python_job(
        &harness.content,
        "print('hello')",
        &[("data/in.csv", b"a,b\n")],
        &[("out/result.txt", "text/plain", 1_000, true)],
        5_000,
    );
    let validated = request.clone();
    let job_id = harness.enqueue(request);
    let receipt = harness.receipt(job_id).await;

    assert_eq!(receipt.result.status, BrowserJobStatus::Completed);
    validated
        .validate_result(&receipt.result)
        .expect("protocol-valid result");
    assert_eq!(receipt.result.stdout_tail, "hello\n");
    assert_eq!(receipt.result.outputs.len(), 1);
    let output = &receipt.result.outputs[0];
    assert_eq!(output.path, "out/result.txt");
    assert_eq!(output.content_ref.sha256, Sha256::digest(b"42"));
    assert!(
        output
            .staging_receipt_ref
            .starts_with(STAGING_RECEIPT_PREFIX)
    );
    assert_eq!(
        harness
            .content
            .bytes(harness.fixture.tenant_id, &output.content_ref)
            .as_deref(),
        Some(&b"42"[..])
    );
    assert_eq!(receipt.result.metrics.output_bytes, 2);
    assert_eq!(receipt.result.metrics.input_bytes, 14 + 4);
    let warning = receipt
        .result
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "undeclared_output_ignored")
        .expect("undeclared warning");
    assert_eq!(warning.severity, BrowserDiagnosticSeverity::Warning);
    assert_eq!(
        harness.store.progress_phases(job_id),
        vec!["initializing", "running", "collecting"]
    );

    let spec = harness.runtime.specs.lock().expect("lock")[0].clone();
    assert_eq!(
        spec.cmd,
        vec!["python3", "-s", "/sandbox/input/__kernel__/runner.py"]
    );
    assert!(spec.read_only_rootfs && spec.cap_drop_all && spec.no_new_privileges);
    assert!(
        !std::path::Path::new(&spec.mounts[1].host).exists(),
        "job directory is removed after sealing"
    );
    let store = harness.stop().await;
    assert_eq!(store.release_count(), 1, "shutdown releases the lease");
}

#[tokio::test]
async fn nonzero_exit_retains_stderr_tail_and_partial_outputs() {
    let harness = Harness::start(Script::Exit {
        code: 1,
        stdout: "",
        stderr: "Traceback: boom\n",
        files: vec![("out.txt", b"partial".to_vec())],
        symlinks: vec![],
    })
    .await;
    let request = harness.fixture.python_job(
        &harness.content,
        "raise SystemExit(1)",
        &[],
        &[("out.txt", "text/plain", 100, true)],
        5_000,
    );
    let job_id = harness.enqueue(request);
    let receipt = harness.receipt(job_id).await;
    assert_eq!(receipt.result.status, BrowserJobStatus::Failed);
    assert_eq!(receipt.result.outputs.len(), 1);
    assert_eq!(
        harness
            .content
            .bytes(
                harness.fixture.tenant_id,
                &receipt.result.outputs[0].content_ref
            )
            .expect("partial output content"),
        b"partial"
    );
    assert_eq!(receipt.result.stderr_tail, "Traceback: boom\n");
    assert_eq!(codes(&receipt), vec!["python_exit_nonzero"]);
    harness.stop().await;
}

#[tokio::test]
async fn missing_required_output_and_symlinked_output_fail() {
    let harness = Harness::start(Script::Exit {
        code: 0,
        stdout: "",
        stderr: "",
        files: vec![],
        symlinks: vec![("leak.txt", "/etc/passwd")],
    })
    .await;
    let missing = harness.fixture.python_job(
        &harness.content,
        "pass",
        &[],
        &[("out.txt", "text/plain", 100, true)],
        5_000,
    );
    let missing_id = harness.enqueue(missing);
    let receipt = harness.receipt(missing_id).await;
    assert_eq!(receipt.result.status, BrowserJobStatus::Failed);
    assert_eq!(
        codes(&receipt),
        vec!["required_output_missing", "undeclared_output_ignored"]
    );

    let leaked = harness.fixture.python_job(
        &harness.content,
        "pass",
        &[],
        &[("leak.txt", "text/plain", 10_000, true)],
        5_000,
    );
    let leaked_id = harness.enqueue(leaked);
    let receipt = harness.receipt(leaked_id).await;
    assert_eq!(receipt.result.status, BrowserJobStatus::Failed);
    assert_eq!(codes(&receipt), vec!["output_invalid"]);
    harness.stop().await;
}

#[tokio::test]
async fn corrupt_input_fails_before_any_container_runs() {
    let harness = Harness::start(Script::Exit {
        code: 0,
        stdout: "",
        stderr: "",
        files: vec![],
        symlinks: vec![],
    })
    .await;
    let source =
        harness
            .content
            .insert_corrupt(harness.fixture.tenant_id, "text/x-python", b"print(1)");
    let request = harness.fixture.request(
        source.clone(),
        vec![musteragent_capability::BrowserInput {
            path: "main.py".to_owned(),
            content_ref: source,
        }],
        &[],
        5_000,
    );
    let job_id = harness.enqueue(request);
    let receipt = harness.receipt(job_id).await;
    assert_eq!(receipt.result.status, BrowserJobStatus::Failed);
    assert_eq!(codes(&receipt), vec!["content_integrity_failed"]);
    assert_eq!(harness.runtime.runs(), 0);
    harness.stop().await;
}

#[tokio::test]
async fn required_prepared_environment_is_refused_explicitly() {
    let harness = Harness::start(Script::Exit {
        code: 0,
        stdout: "",
        stderr: "",
        files: vec![],
        symlinks: vec![],
    })
    .await;
    let mut request = harness
        .fixture
        .python_job(&harness.content, "pass", &[], &[], 5_000);
    if let BrowserJobProgram::Python {
        environment,
        require_prepared_environment,
        ..
    } = &mut request.program
    {
        *environment = Some(BrowserPythonEnvironmentBinding {
            installation_id: BrowserJobId::generate(),
            revision_hash: Sha256::digest(b"rev"),
            scope_key: Sha256::digest(b"scope"),
        });
        *require_prepared_environment = true;
    }
    // Prepared contracts are offered, but an unknown immutable receipt is refused.
    let queued = harness.store.enqueue(
        harness.fixture.tenant_id,
        harness.fixture.principal_id,
        BrowserProviderContract::PythonExecutePrepared,
        request.clone(),
    );
    let job_id = queued
        .expect("prepared contract is offered")
        .delivery
        .request
        .job_id;
    let receipt = harness.receipt(job_id).await;
    assert_eq!(
        codes(&receipt),
        vec!["server_sandbox_environment_unavailable"]
    );
    assert_eq!(harness.runtime.runs(), 0);
    harness.stop().await;
}

#[tokio::test]
async fn cancellation_kills_the_container_and_seals_cancelled() {
    let harness = Harness::start(Script::BlockUntilCancelled).await;
    let request =
        harness
            .fixture
            .python_job(&harness.content, "while True: pass", &[], &[], 60_000);
    let job_id = harness.enqueue(request);
    harness.wait_for(|h| h.runtime.runs() == 1).await;
    harness.store.request_cancel(job_id).await.expect("cancel");
    let receipt = harness.receipt(job_id).await;
    assert_eq!(receipt.result.status, BrowserJobStatus::Cancelled);
    assert_eq!(codes(&receipt), vec!["cancelled"]);
    assert_eq!(harness.runtime.cancelled(), 1);
    harness.stop().await;
}

#[tokio::test]
async fn lease_loss_kills_running_jobs_without_sealing_and_reregisters() {
    let harness = Harness::start(Script::BlockUntilCancelled).await;
    let request =
        harness
            .fixture
            .python_job(&harness.content, "while True: pass", &[], &[], 60_000);
    let job_id = harness.enqueue(request);
    harness.wait_for(|h| h.runtime.runs() == 1).await;
    harness.store.revoke_current_lease();
    harness.wait_for(|h| h.runtime.cancelled() == 1).await;
    harness.wait_for(|h| h.store.lease_count() >= 2).await;
    assert!(
        harness.store.receipt(job_id).is_none(),
        "a lost lease never seals"
    );
    let second = harness.store.current_lease().expect("lease");
    assert!(second.epoch.get() >= 2, "re-registration bumps the epoch");
    harness.stop().await;
}

#[tokio::test]
async fn shutdown_cancels_running_jobs_and_seals_them() {
    let harness = Harness::start(Script::BlockUntilCancelled).await;
    let request =
        harness
            .fixture
            .python_job(&harness.content, "while True: pass", &[], &[], 60_000);
    let job_id = harness.enqueue(request);
    harness.wait_for(|h| h.runtime.runs() == 1).await;
    let store = harness.stop().await;
    let receipt = store.receipt(job_id).expect("sealed on shutdown");
    assert_eq!(receipt.result.status, BrowserJobStatus::Cancelled);
    assert_eq!(codes(&receipt), vec!["server_sandbox_shutdown"]);
    assert_eq!(store.release_count(), 1);
}
