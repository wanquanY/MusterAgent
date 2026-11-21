//! Real process takeover with a host-owned credential source in both generations.
use super::*;
use musteragent_cloud_worker::{CloudWorkerSettings, WorkerOverrides, run_worker};
use musteragent_model::{CredentialGrantRef, ModelRoute};
use musteragent_model_providers::{
    CredentialMaterial, CredentialResolver, CredentialResolverError, ModelRequestHeaderHook,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{ModelInvocationRouteRole, ModelRequestAuthority, ProviderError};
use reqwest::header::HeaderMap;
use std::{fs::OpenOptions, io::Write, sync::atomic::AtomicU32, time::SystemTime};

struct Recorder {
    log: PathBuf,
    credentials: AtomicU32,
    signatures: AtomicU32,
}
impl Recorder {
    fn record(
        &self,
        event: &str,
        grant: &CredentialGrantRef,
        route: &ModelRoute,
        role: ModelInvocationRouteRole,
        invocation: InvocationId,
    ) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .expect("callback log");
        writeln!(
            file,
            "{}",
            serde_json::json!([
                event,
                grant.as_str(),
                route.route_ref.as_str(),
                route.model_provider_id.to_string(),
                route.model_entry_id.to_string(),
                role.canonical_code(),
                invocation.to_string()
            ])
        )
        .expect("append callback");
    }
}
impl CredentialResolver for Recorder {
    fn resolve<'a>(
        &'a self,
        grant: &'a CredentialGrantRef,
        route: &'a ModelRoute,
        role: ModelInvocationRouteRole,
        invocation: InvocationId,
    ) -> ApplicationFuture<'a, Result<CredentialMaterial, CredentialResolverError>> {
        Box::pin(async move {
            self.record("resolve", grant, route, role, invocation);
            let n = self.credentials.fetch_add(1, Ordering::SeqCst) + 1;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock");
            // The first preparation falls inside the adapter's one-second renewal window.
            let ttl = if n == 1 { 800 } else { 60_000 };
            Ok(CredentialMaterial::ScopedBearer {
                secret: format!("fixture-generation-{n}").into(),
                expires_at: UtcTimestamp::from_unix_millis(now.as_millis() as i64 + ttl),
            })
        })
    }
}
impl ModelRequestHeaderHook for Recorder {
    fn headers<'a>(
        &'a self,
        authority: &'a ModelRequestAuthority,
    ) -> ApplicationFuture<'a, Result<HeaderMap, ProviderError>> {
        Box::pin(async move {
            self.record(
                "headers",
                &authority.credential_grant_ref,
                &authority.route,
                authority.route_role,
                authority.invocation.invocation_id,
            );
            let n = self.signatures.fetch_add(1, Ordering::SeqCst) + 1;
            let mut headers = HeaderMap::new();
            if n == 1 {
                headers.insert(
                    "x-fixture-preflight",
                    "first-lease".parse().expect("fixture header"),
                );
            }
            headers.insert("x-fixture-signature", n.into());
            Ok(headers)
        })
    }
}

async fn recording_host(config: PathBuf) -> Result<(), BuildError> {
    let mut signal = musteragent_cloud_lifecycle::ShutdownSignal::install()?;
    let settings = CloudWorkerSettings::load(&config)?;
    let host = Arc::new(Recorder {
        log: config.with_extension("calls"),
        credentials: AtomicU32::new(0),
        signatures: AtomicU32::new(0),
    });
    run_worker(
        settings,
        WorkerOverrides {
            credentials: Some(host.clone()),
            model_headers: Some(host),
            capability_dispatchers: Default::default(),
        },
        signal.recv(),
    )
    .await
}

#[tokio::test]
async fn frozen_authority_survives_renewal_sigkill_and_pending_lookups() -> Result<(), BuildError> {
    const CHILD_CONFIG: &str = "K2_TEST_RECORDING_WORKER_CONFIG";
    if let Some(config) = std::env::var_os(CHILD_CONFIG) {
        return recording_host(config.into()).await;
    }
    let smoke = Smoke::start_for("k2").await?;
    let route = model()?;
    let gateway = Gateway::start(true, false, 2, 200).await?;
    let fixture = smoke
        .seed_tools(&route, false, &[], b"Recover the original response.")
        .await;
    std::fs::write(
        smoke.dir.join("empty.json"),
        br#"{"schema_version":"musteragent.workspace-tool-releases.v1","releases":[]}"#,
    )?;
    let spawn = |name: &str| -> Result<Child, BuildError> {
        let config =
            smoke.worker_config(name, &route, &gateway.endpoint, None, &gateway.config(true))?;
        let text =
            std::fs::read_to_string(&config)?.replace("credential_env = \"P9_MODEL_KEY\"\n", "");
        std::fs::write(
            &config,
            format!("workspace_releases_file = \"empty.json\"\n{text}"),
        )?;
        let mut cmd = Command::new(std::env::current_exe()?);
        cmd.args(["--exact", "model_recovery::host_authority::frozen_authority_survives_renewal_sigkill_and_pending_lookups"])
            .env(CHILD_CONFIG, config).env_remove("P9_MODEL_KEY");
        smoke.spawn_worker(name, cmd)
    };
    let mut first = spawn("first")?;
    tokio::time::timeout(Duration::from_secs(30), async {
        while gateway.calls.posts.load(Ordering::SeqCst) == 0 {
            assert!(
                first.try_wait().expect("child status").is_none(),
                "{}",
                smoke.dir.display()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    first.kill().await?;
    let status = first.wait().await?;
    #[cfg(unix)]
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(9)
    );
    let mut replacement = spawn("replacement")?;
    smoke.wait_completed(fixture, &mut replacement).await;
    terminate(&mut replacement).await;

    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE run_id=$1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&smoke.pool)
    .await?;
    let snapshot = musteragent_wire::ProtobufSnapshotCodec.decode_content(&encoded)?;
    assert!(
        snapshot
            .tools
            .iter()
            .all(|t| t.manifest.application.is_none())
    );
    let invocations: Vec<uuid::Uuid> =
        sqlx::query_scalar("SELECT invocation_id FROM runtime.model_invocations WHERE run_id=$1")
            .bind(fixture.run_id.into_uuid())
            .fetch_all(&smoke.pool)
            .await?;
    assert_eq!(invocations.len(), 1);
    let invocation = invocations[0];
    for (name, events) in [
        ("first", vec!["resolve", "headers", "resolve", "headers"]),
        (
            "replacement",
            vec![
                "resolve", "headers", "resolve", "headers", "resolve", "headers", "resolve",
                "headers", "resolve", "headers",
            ],
        ),
    ] {
        let log = std::fs::read_to_string(smoke.dir.join(format!("{name}.calls")))?;
        let calls = log
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        let expected: Vec<_> = events
            .iter()
            .map(|event| {
                serde_json::json!([
                    event,
                    snapshot.model.credential_grant_ref.as_str(),
                    snapshot.model.route.route_ref.as_str(),
                    snapshot.model.route.model_provider_id.to_string(),
                    snapshot.model.route.model_entry_id.to_string(),
                    ModelInvocationRouteRole::Chat.canonical_code(),
                    invocation.to_string()
                ])
            })
            .collect();
        assert_eq!(
            calls, expected,
            "{name} must authorize the frozen invocation"
        );
    }
    let requests = gateway.calls.requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1], "takeover retries the exact POST");
    assert_eq!(requests[0].0, format!("model-{invocation}"));
    assert_eq!(gateway.calls.dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(
        *gateway.calls.authorization.lock().expect("calls"),
        [
            ("POST".into(), 2, 2),
            ("POST".into(), 2, 2),
            ("GET".into(), 3, 3),
            ("GET".into(), 4, 4),
            ("GET".into(), 5, 5)
        ]
    );
    Ok(())
}
