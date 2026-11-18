//! Mandatory real Docker network isolation and fail-closed policy acceptance.
mod support;
use musteragent_capability::{BrowserJobStatus, BrowserSecurityProfile};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_sandbox_server::egress::{
    EgressPolicy, EgressProxy, ResolveError, Resolver, UpstreamProxy,
};
use musteragent_sandbox_server::{DockerRuntime, EgressNetworkConfig, JobExecutor};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
#[derive(Debug)]
struct PrivateResolver;
impl Resolver for PrivateResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        _port: u16,
    ) -> ApplicationFuture<'a, Result<Vec<std::net::IpAddr>, ResolveError>> {
        Box::pin(async move {
            if host == "private.example" {
                Ok(vec!["10.0.0.1".parse().expect("IP")])
            } else {
                Err(ResolveError)
            }
        })
    }
}
async fn run(
    source: &str,
    profile: BrowserSecurityProfile,
    proxy: EgressProxy,
) -> musteragent_capability::BrowserJobResult {
    let root = tempfile::tempdir().expect("temp");
    let mut config = support::config(root.path(), "musteragent-sandbox-mcp:p6");
    config.egress = Some(EgressNetworkConfig {
        proxy: serde_json::from_value(serde_json::json!({"listen":"127.0.0.1:0"})).expect("config"),
    });
    let content = Arc::new(support::FakeContentStore::default());
    let fixture = support::JobFixture::new();
    let mut request = fixture.python_job(&content, source, &[], &[], 30000);
    request.security_profile = profile;
    let runtime = DockerRuntime::connect_local()
        .expect("Docker")
        .with_proxy(proxy);
    let executor = JobExecutor::new(Arc::new(config), content, Arc::new(runtime));
    let job = match executor.prepare(fixture.tenant_id, &request).await {
        musteragent_sandbox_server::job::Prepared::Ready(j) => j,
        other => panic!("prepare: {other:?}"),
    };
    let outcome = executor.run(&job, CancellationToken::new()).await;
    let result = executor
        .finish(fixture.tenant_id, &request, job, outcome, None)
        .await;
    request.validate_result(&result).expect("canonical result");
    result
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_proxy_denials_never_connect_upstream_and_direct_network_is_unreachable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let address = listener.local_addr().expect("addr");
    let count = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&count);
    let task = tokio::spawn(async move {
        while let Ok((_stream, _)) = listener.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
        }
    });
    let proxy = EgressProxy::new(EgressPolicy::default(), Duration::from_secs(2))
        .with_resolver(Arc::new(PrivateResolver))
        .with_upstream(UpstreamProxy {
            address,
            upstream_resolves: false,
        });
    let result=run(r#"
import socket, http.client
for target, reason in [('169.254.169.254','cloud_metadata'),('100.100.100.200','cloud_metadata'),('10.0.0.1','private'),('127.0.0.1','loopback'),('[::1]','loopback'),('[fd00::1]','unique_local'),('private.example','private'),('unresolved.example','resolve_failed')]+[('10.0.0.1','private')]*20:
    connection=http.client.HTTPConnection('127.0.0.1',18080,timeout=4)
    connection.request('CONNECT',target+':443')
    response=connection.getresponse()
    assert response.status==403,(target,response.status)
    assert response.getheader('x-sandbox-denial')==reason,(target,response.getheaders())
    connection.close()
    print(target,reason,flush=True)
try:
    socket.create_connection(('1.1.1.1',443),timeout=2)
    raise AssertionError('direct egress escaped')
except OSError:
    print('direct TCP denied',flush=True)
"#,BrowserSecurityProfile::ProxyEgress,proxy).await;
    task.abort();
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "denied targets must never reach the upstream listener"
    );
    assert_eq!(
        result.status,
        BrowserJobStatus::Completed,
        "{:?} {}",
        result.diagnostics,
        result.stderr_tail
    );
    for reason in [
        "cloud_metadata",
        "private",
        "loopback",
        "unique_local",
        "resolve_failed",
    ] {
        assert!(
            result
                .diagnostics
                .iter()
                .any(|d| d.code == format!("egress_denied_{reason}")),
            "{reason}: {:?}",
            result.diagnostics
        );
    }
    assert!(result.stdout_tail.contains("direct TCP denied"));
    println!("{}", result.stdout_tail);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn docker_offline_cannot_reach_proxy_public_ip_or_host() {
    let result = run(
        r#"
import socket
for host,port in [('127.0.0.1',18080),('1.1.1.1',443),('host.docker.internal',80)]:
    try:
        socket.create_connection((host,port),timeout=2)
        raise AssertionError('offline connection escaped: '+host)
    except OSError:
        print(host,'unreachable',flush=True)
assert len(open('/proc/net/route').read().splitlines())==1
"#,
        BrowserSecurityProfile::SafeOffline,
        EgressProxy::new(EgressPolicy::default(), Duration::from_secs(1)),
    )
    .await;
    assert_eq!(
        result.status,
        BrowserJobStatus::Completed,
        "{:?} {}",
        result.diagnostics,
        result.stderr_tail
    );
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !d.code.starts_with("egress_"))
    );
    println!("{}", result.stdout_tail);
}
