//! Opt-in Docker and public HTTPS egress acceptance; selected runs require every prerequisite.
mod support;
use musteragent_identity::TenantId;
use musteragent_mcp_client::{
    McpClientOptions, McpProcessSpec, McpSession, ProcessLauncher, SecretEnvironment,
    StdioTransport,
};
use musteragent_sandbox_server::egress::{
    EgressPolicy, EgressProxy, ResolutionMode, UpstreamProxy,
};
use musteragent_sandbox_server::{DockerRuntime, McpSandboxLimits, SandboxProcessLauncher};
use std::{io::Write as _, sync::Arc, time::Duration};
use tokio::io::AsyncReadExt;

/// Public-network acceptance is deliberately opt-in on developer and CI machines.
fn acceptance_enabled() -> bool {
    if std::env::var("MUSTERAGENT_TEST_MCP_SANDBOX").as_deref() == Ok("1") {
        return true;
    }
    // Write directly so the default libtest capture still displays why this
    // external acceptance path was not selected.
    let _ = writeln!(
        std::io::stderr().lock(),
        "SKIP MCP sandbox acceptance: set MUSTERAGENT_TEST_MCP_SANDBOX=1; requires Docker, the documented MCP image, HTTPS_PROXY or HTTP_PROXY, and public HTTPS egress"
    );
    false
}

fn upstream() -> UpstreamProxy {
    let raw=std::env::var("HTTPS_PROXY").or_else(|_|std::env::var("HTTP_PROXY")).expect("Docker MCP acceptance requires HTTPS_PROXY or HTTP_PROXY and a working public HTTPS egress path");
    let uri: hyper::Uri = raw.parse().expect("HTTP proxy URI");
    assert_eq!(
        uri.scheme_str(),
        Some("http"),
        "acceptance upstream must be HTTP"
    );
    assert!(
        !uri.authority().expect("authority").as_str().contains('@'),
        "proxy credentials are unsupported in acceptance environment"
    );
    let host = uri.host().expect("proxy host");
    let ip = if host == "localhost" {
        "127.0.0.1"
    } else {
        host
    };
    UpstreamProxy {
        address: std::net::SocketAddr::new(
            ip.parse().expect("upstream needs an IP-literal address"),
            uri.port_u16().unwrap_or(80),
        ),
        upstream_resolves: true,
    }
}
fn request(tenant_id: TenantId) -> McpProcessSpec {
    McpProcessSpec {
        tenant_id,
        server_name: musteragent_mcp::McpServerName::new("node-sandbox").expect("name"),
        program: "node".into(),
        arguments: vec!["/opt/sandbox/stdio-mcp.mjs".into()],
        environment: SecretEnvironment::default(),
        working_directory: None,
    }
}
fn launcher(
    tenant: TenantId,
    root: &std::path::Path,
    limits: McpSandboxLimits,
) -> SandboxProcessLauncher {
    let config = support::config(root, "musteragent-sandbox-mcp:p6");
    let proxy = EgressProxy::new(EgressPolicy::default(), Duration::from_secs(15))
        .with_upstream(upstream());
    SandboxProcessLauncher::new(
        tenant,
        config,
        DockerRuntime::connect_local()
            .expect("Docker")
            .with_proxy(proxy),
        limits,
    )
    .expect("launcher")
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_mcp_lists_calls_pure_and_https_and_records_delegation() {
    if !acceptance_enabled() {
        return;
    }
    let root = tempfile::tempdir().expect("temp");
    let tenant = TenantId::generate();
    let launcher = launcher(tenant, root.path(), McpSandboxLimits::default());
    let transport = Arc::new(
        StdioTransport::launch(
            &launcher,
            request(tenant),
            1024 * 1024,
            Duration::from_secs(1),
        )
        .await
        .expect("Docker MCP launch"),
    );
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(30);
    let session = McpSession::connect(transport, McpClientOptions::default(), deadline())
        .await
        .expect("initialize");
    let tools = session.list_tools(deadline()).await.expect("tools/list");
    assert!(tools.contains_key("add") && tools.contains_key("fetch"));
    let pure = session
        .call_tool("add", serde_json::json!({"a":19,"b":23}), deadline(), None)
        .await
        .expect("pure call");
    assert_eq!(pure.content[0]["text"], "42");
    let remote = session
        .call_tool(
            "fetch",
            serde_json::json!({"url":"https://pypi.org/"}),
            deadline(),
            None,
        )
        .await
        .unwrap_or_else(|e| {
            if let musteragent_mcp_client::McpClientError::Remote(r) = &e {
                panic!("HTTPS remote error: {}", r.message.as_untrusted_str());
            }
            panic!("HTTPS error: {e:?}")
        });
    let result: serde_json::Value =
        serde_json::from_str(remote.content[0]["text"].as_str().expect("text")).expect("response");
    assert_eq!(result["status"], 200);
    assert!(result["bytes"].as_u64().expect("bytes") > 0);
    session.shutdown().await.expect("shutdown");
    let receipts = launcher.take_results();
    assert_eq!(receipts.len(), 1);
    assert!(
        receipts[0]
            .outcome
            .egress
            .iter()
            .any(|e| e.host == "pypi.org"
                && e.mode == ResolutionMode::UpstreamResolves
                && e.denial.is_none())
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_mcp_kill_closes_stdout_removes_container_and_rejects_other_tenants() {
    if !acceptance_enabled() {
        return;
    }
    let root = tempfile::tempdir().expect("temp");
    let tenant = TenantId::generate();
    let launcher = launcher(tenant, root.path(), McpSandboxLimits::default());
    assert!(
        launcher
            .launch(request(TenantId::generate()))
            .await
            .is_err()
    );
    let mut process = launcher.launch(request(tenant)).await.expect("launch");
    process.control.kill().await.expect("kill");
    process.control.kill().await.expect("idempotent kill");
    let mut output = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(2),
        process.stdout.read_to_end(&mut output),
    )
    .await
    .expect("stdout EOF")
    .expect("read");
    let receipts = launcher.take_results();
    assert_eq!(receipts.len(), 1);
    let name = format!("musteragent-mcp-{}", receipts[0].session_id);
    let check = std::process::Command::new("docker")
        .args(["inspect", &name])
        .output()
        .expect("inspect");
    assert!(
        !check.status.success(),
        "container must be removed before kill returns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_mcp_idle_wall_and_stdio_limits_terminate_with_typed_receipts() {
    if !acceptance_enabled() {
        return;
    }
    use musteragent_sandbox_server::ContainerExit;
    use tokio::io::AsyncWriteExt;
    let root = tempfile::tempdir().expect("temp");
    let tenant = TenantId::generate();
    for (limits, body, expected, idle) in [
        (
            McpSandboxLimits {
                idle_timeout: Duration::from_millis(150),
                ..Default::default()
            },
            "setInterval(()=>{},1000)",
            ContainerExit::TimedOut,
            true,
        ),
        (
            McpSandboxLimits {
                wall_time: Duration::from_millis(150),
                ..Default::default()
            },
            "setInterval(()=>{},1000)",
            ContainerExit::TimedOut,
            false,
        ),
        (
            McpSandboxLimits {
                stdout_bytes: 64,
                ..Default::default()
            },
            "process.stdout.write('X'.repeat(1024));setInterval(()=>{},1000)",
            ContainerExit::OutputLimitExceeded,
            false,
        ),
        (
            McpSandboxLimits {
                stdin_bytes: 64,
                ..Default::default()
            },
            "process.stdin.resume();setInterval(()=>{},1000)",
            ContainerExit::InputLimitExceeded,
            false,
        ),
    ] {
        let launcher = launcher(tenant, root.path(), limits);
        let mut spec = request(tenant);
        spec.arguments = vec!["-e".into(), body.into()];
        let mut process = launcher.launch(spec).await.expect("launch");
        if expected == ContainerExit::InputLimitExceeded {
            process.stdin.write_all(&[b'x'; 128]).await.expect("stdin");
        }
        tokio::time::timeout(Duration::from_secs(5), process.control.wait())
            .await
            .expect("bounded termination")
            .expect("wait");
        let mut bytes = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(2),
            process.stdout.read_to_end(&mut bytes),
        )
        .await
        .expect("EOF")
        .expect("read");
        let receipts = launcher.take_results();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].outcome.exit, expected);
        assert_eq!(receipts[0].idle_timeout, idle);
        assert!(receipts[0].outcome.runtime_error.is_none());
    }
}
