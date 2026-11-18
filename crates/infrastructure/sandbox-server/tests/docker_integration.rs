//! Real Docker runs require the daemon and public `python:3.12-slim` image.
//! Missing prerequisites are failures. Containers are local,
//! disposable and labelled `purpose=musteragent-sandbox-test`,
//! `disposable=true`; a drop guard removes anything left behind.

mod support;

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use musteragent_capability::{BrowserJobStatus, BrowserProviderContract};
use musteragent_identity::BrowserJobId;
use musteragent_runtime_capability::{BrowserGatewayStore, BrowserJobTerminalReceipt};
use musteragent_sandbox_server::{DockerRuntime, ProviderBinding, ServerSandboxProvider};
use support::{FakeContentStore, FakeGatewayStore, JobFixture};
use tokio_util::sync::CancellationToken;

const TEST_IMAGE: &str = "python:3.12-slim";
const RUN_LABEL: &str = "musteragent.test.run";

async fn docker_required() -> DockerRuntime {
    let runtime = DockerRuntime::connect_local().expect("connect disposable Docker test runtime");
    runtime
        .ping()
        .await
        .expect("Docker daemon must be reachable");
    assert!(
        runtime
            .image_present(TEST_IMAGE)
            .await
            .expect("inspect test image"),
        "required local test image {TEST_IMAGE} is missing"
    );
    runtime
}

/// Removes every container of this test run, even on panic.
struct ContainerSweep {
    run_id: String,
}

impl Drop for ContainerSweep {
    fn drop(&mut self) {
        let filter = format!("label={RUN_LABEL}={}", self.run_id);
        let Ok(listed) = Command::new("docker")
            .args([
                "ps",
                "-aq",
                "--filter",
                &filter,
                "--filter",
                "label=purpose=musteragent-sandbox-test",
            ])
            .output()
        else {
            return;
        };
        for id in String::from_utf8_lossy(&listed.stdout).split_whitespace() {
            let _ = Command::new("docker").args(["rm", "-f", "-v", id]).output();
        }
    }
}

struct DockerHarness {
    store: Arc<FakeGatewayStore>,
    content: Arc<FakeContentStore>,
    fixture: JobFixture,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    sweep: ContainerSweep,
    _work_root: tempfile::TempDir,
}

impl DockerHarness {
    async fn start(runtime: DockerRuntime) -> Self {
        let work_root = tempfile::tempdir().expect("tempdir");
        let run_id = BrowserJobId::generate().into_uuid().to_string();
        let mut config = support::config(work_root.path(), TEST_IMAGE);
        config.labels.insert(RUN_LABEL.to_owned(), run_id.clone());
        let store = Arc::new(FakeGatewayStore::default());
        let content = Arc::new(FakeContentStore::default());
        let fixture = JobFixture::new();
        let provider = ServerSandboxProvider::new(
            config,
            ProviderBinding {
                tenant_id: fixture.tenant_id,
                principal_id: fixture.principal_id,
            },
            Arc::clone(&store) as Arc<dyn BrowserGatewayStore>,
            Arc::clone(&content) as _,
            Arc::new(runtime),
        )
        .expect("provider");
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let task = tokio::spawn(async move { provider.run(token).await.expect("run") });
        for _ in 0..200 {
            if store.lease_count() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Self {
            store,
            content,
            fixture,
            shutdown,
            task,
            sweep: ContainerSweep { run_id },
            _work_root: work_root,
        }
    }

    async fn run(
        &self,
        request: musteragent_capability::BrowserJobRequest,
    ) -> BrowserJobTerminalReceipt {
        let job_id = request.job_id;
        self.store
            .enqueue(
                self.fixture.tenant_id,
                self.fixture.principal_id,
                BrowserProviderContract::PythonExecute,
                request,
            )
            .expect("enqueue");
        for _ in 0..1_200 {
            if let Some(receipt) = self.store.receipt(job_id) {
                return receipt;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("job did not finish");
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(30), self.task).await;
        let leftover = Command::new("docker")
            .args([
                "ps",
                "-aq",
                "--filter",
                &format!("label={RUN_LABEL}={}", self.sweep.run_id),
            ])
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
            .unwrap_or_default();
        assert_eq!(leftover, "", "job containers are removed after each job");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_job_reads_inputs_and_writes_declared_outputs() {
    let runtime = docker_required().await;
    let harness = DockerHarness::start(runtime).await;
    let source = "\
import csv\n\
rows = list(csv.reader(open('data/in.csv')))\n\
open('out/summary.txt', 'w').write(str(len(rows)))\n\
open('undeclared.txt', 'w').write('x')\n\
print('rows', len(rows))\n";
    let request = harness.fixture.python_job(
        &harness.content,
        source,
        &[("data/in.csv", b"a,b\n1,2\n3,4\n")],
        &[("out/summary.txt", "text/plain", 100, true)],
        30_000,
    );
    let validated = request.clone();
    let receipt = harness.run(request).await;
    assert_eq!(
        receipt.result.status,
        BrowserJobStatus::Completed,
        "{:?} {}",
        receipt.result.diagnostics,
        receipt.result.stderr_tail
    );
    validated
        .validate_result(&receipt.result)
        .expect("valid result");
    assert_eq!(receipt.result.stdout_tail, "rows 3\n");
    let output = &receipt.result.outputs[0];
    assert_eq!(
        harness
            .content
            .bytes(harness.fixture.tenant_id, &output.content_ref)
            .as_deref(),
        Some(&b"3"[..])
    );
    assert!(
        receipt
            .result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "undeclared_output_ignored")
    );
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn container_is_isolated() {
    let runtime = docker_required().await;
    let harness = DockerHarness::start(runtime).await;
    let source = r#"
import json, os, socket
def attempt(fn):
    try:
        fn()
        return "ok"
    except Exception as error:
        return type(error).__name__
status = dict(line.split(":\t", 1) for line in open("/proc/self/status").read().splitlines() if ":\t" in line)
report = {
    "uid": os.getuid(),
    "cap_eff": status["CapEff"].strip(),
    "no_new_privs": status["NoNewPrivs"].strip(),
    "root_write": attempt(lambda: open("/escape.txt", "w").write("x")),
    "input_write": attempt(lambda: open("/sandbox/input/main.py", "a").write("x")),
    "input_link_write": attempt(lambda: open("main.py", "a").write("x")),
    "tmp_write": attempt(lambda: open("/tmp/ok.txt", "w").write("x")),
    "network": attempt(lambda: socket.create_connection(("1.1.1.1", 53), timeout=2)),
    "ipv4_routes": len(open("/proc/net/route").read().splitlines()[1:]),
}
open("report.json", "w").write(json.dumps(report))
"#;
    let request = harness.fixture.python_job(
        &harness.content,
        source,
        &[],
        &[("report.json", "application/json", 10_000, true)],
        30_000,
    );
    let receipt = harness.run(request).await;
    assert_eq!(
        receipt.result.status,
        BrowserJobStatus::Completed,
        "{:?} {}",
        receipt.result.diagnostics,
        receipt.result.stderr_tail
    );
    let bytes = harness
        .content
        .bytes(
            harness.fixture.tenant_id,
            &receipt.result.outputs[0].content_ref,
        )
        .expect("report");
    let report: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(report["uid"], 65534, "{report}");
    assert_eq!(report["cap_eff"], "0000000000000000", "{report}");
    assert_eq!(report["no_new_privs"], "1", "{report}");
    assert_eq!(report["root_write"], "OSError", "{report}");
    assert_ne!(report["input_write"], "ok", "{report}");
    assert_ne!(report["input_link_write"], "ok", "{report}");
    assert_eq!(report["tmp_write"], "ok", "{report}");
    assert_ne!(report["network"], "ok", "{report}");
    assert_eq!(report["ipv4_routes"], 0, "{report}");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wall_time_and_pid_limits_are_enforced() {
    let runtime = docker_required().await;
    let harness = DockerHarness::start(runtime).await;
    let spin =
        harness
            .fixture
            .python_job(&harness.content, "while True:\n    pass\n", &[], &[], 1_500);
    let receipt = harness.run(spin).await;
    assert_eq!(receipt.result.status, BrowserJobStatus::ResourceExceeded);
    assert!(
        receipt
            .result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "wall_time_exceeded")
    );

    let fork = r#"
import os, sys
count = 0
try:
    for _ in range(500):
        if os.fork() == 0:
            import time; time.sleep(5); os._exit(0)
        count += 1
except OSError:
    print("fork limited at", count)
    sys.exit(3)
print("forked", count)
"#;
    let request = harness
        .fixture
        .python_job(&harness.content, fork, &[], &[], 20_000);
    let receipt = harness.run(request).await;
    assert_eq!(
        receipt.result.status,
        BrowserJobStatus::Failed,
        "{}",
        receipt.result.stdout_tail
    );
    assert!(
        receipt.result.stdout_tail.starts_with("fork limited at"),
        "{}",
        receipt.result.stdout_tail
    );
    harness.stop().await;
}
