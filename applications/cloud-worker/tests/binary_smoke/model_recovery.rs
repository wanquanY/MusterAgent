//! P13: the gateway and its original result survive a real worker SIGKILL.
use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
const TEXT: &str = "Recovered original completion.";

#[derive(Default)]
struct Calls {
    posts: AtomicUsize,
    lookups: AtomicUsize,
    dispatches: AtomicUsize,
    requests: Mutex<Vec<(String, Vec<u8>)>>,
}
struct Gateway {
    endpoint: String,
    calls: Arc<Calls>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Gateway {
    async fn start(
        hold: bool,
        prefix: bool,
        pending: usize,
        status: u16,
    ) -> Result<Self, BuildError> {
        Self::start_with_text(hold, prefix, pending, status, Arc::from(TEXT)).await
    }
    async fn start_with_text(
        hold: bool,
        prefix: bool,
        pending: usize,
        status: u16,
        text: Arc<str>,
    ) -> Result<Self, BuildError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/chat/completions", listener.local_addr()?);
        let calls = Arc::new(Calls::default());
        let state = calls.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { return; };
                        let state = state.clone();
                        let text = text.clone();
                        connections.spawn(async move {
                            let _ = serve(socket, state, hold, prefix, pending, status, text).await;
                        });
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(error)) = result { panic!("fake gateway task: {error}"); }
                    }
                }
            }
        });
        Ok(Self {
            endpoint,
            calls,
            task,
        })
    }
    fn config(&self, resolver: bool) -> String {
        let result = if resolver {
            format!(
                "result_url = {:?}\n",
                self.endpoint.replace("/chat/completions", "/result")
            )
        } else {
            String::new()
        };
        format!(
            r#"
[models.existing_invocation]
{result}[models.existing_invocation.already_accepted]
status = 409
code_pointer = "/error/code"
code = "fixture.accepted"
[models.existing_invocation.policy]
timeout_ms = 800
max_lookups = 8
initial_backoff_ms = 10
max_backoff_ms = 40
"#
        )
    }
}
async fn serve(
    mut socket: TcpStream,
    calls: Arc<Calls>,
    hold: bool,
    prefix: bool,
    pending: usize,
    status: u16,
    text: Arc<str>,
) -> Result<(), BuildError> {
    let mut bytes = Vec::new();
    let end = loop {
        let mut chunk = [0; 4096];
        let size = socket.read(&mut chunk).await?;
        if size == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&chunk[..size]);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break end + 4;
        }
        assert!(bytes.len() < 65536, "bounded request header");
    };
    let headers = String::from_utf8(bytes[..end].to_vec())?;
    let header = |name: &str| {
        headers.lines().find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.trim().to_owned())
        })
    };
    assert_eq!(
        header("authorization").as_deref(),
        Some("Bearer test-only-model-key")
    );
    if headers.starts_with("POST ") {
        let length = header("content-length")
            .expect("request length")
            .parse::<usize>()?;
        while bytes.len() - end < length {
            let mut chunk = [0; 4096];
            let size = socket.read(&mut chunk).await?;
            if size == 0 {
                return Ok(());
            }
            bytes.extend_from_slice(&chunk[..size]);
        }
        let key = header("idempotency-key").expect("stable invocation key");
        let number = calls.posts.fetch_add(1, Ordering::SeqCst);
        calls
            .requests
            .lock()
            .expect("fixture lock")
            .push((key, bytes[end..].to_vec()));
        if number == 0 {
            calls.dispatches.fetch_add(1, Ordering::SeqCst);
            if hold {
                if prefix {
                    let first = "data: {\"id\":\"original-response\",\"model\":\"fixture-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Recovered \"},\"finish_reason\":null}]}\n\n";
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n", first.len(), first).as_bytes()).await?;
                }
                // The gateway retained a billed completion before sending its
                // terminal. Killing only the worker cannot remove this result.
                std::future::pending::<()>().await;
            }
        }
        reply(
            &mut socket,
            409,
            "application/json",
            r#"{"error":{"code":"fixture.accepted"}}"#,
        )
        .await?;
    } else {
        let line = headers.lines().next().expect("request line");
        let path = line.split_whitespace().nth(1).expect("request target");
        let url = url::Url::parse(&format!("http://fixture{path}"))?;
        let key = url
            .query_pairs()
            .find(|(k, _)| k == "key")
            .expect("lookup key")
            .1
            .into_owned();
        assert_eq!(key, calls.requests.lock().expect("fixture lock")[0].0);
        let lookup = calls.lookups.fetch_add(1, Ordering::SeqCst);
        if lookup < pending {
            reply(&mut socket, 202, "application/json", "{}").await?;
        } else if status != 200 {
            reply(&mut socket, status, "application/json", "{}").await?;
        } else {
            // Changed chunk boundaries exercise durable prefix reconciliation.
            let mut body = String::new();
            for chunk in text.as_bytes().chunks(16 * 1024) {
                let text = std::str::from_utf8(chunk)?;
                body.push_str(&format!("data: {}\n\n", serde_json::json!({"id":"original-response", "model":"fixture-model", "choices":[{"index":0,"delta":{"content":text},"finish_reason":null}]})));
            }
            body.push_str("data: {\"id\":\"original-response\",\"model\":\"fixture-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\ndata: [DONE]\n\n");
            reply(&mut socket, 200, "text/event-stream", &body).await?;
        }
    }
    Ok(())
}
async fn reply(
    socket: &mut TcpStream,
    status: u16,
    media: &str,
    body: &str,
) -> Result<(), BuildError> {
    socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: {media}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
    Ok(())
}
fn model() -> Result<musteragent_model::ModelRoute, BuildError> {
    let mut route = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    route.api_format = musteragent_model::ApiFormat::OpenAiChatCompletions;
    route.adapter_version = VersionedKey::new("openai.chat-adapter@1")?;
    route.protocol_version = VersionedKey::new("openai.chat@1")?;
    Ok(route)
}

async fn killed_recovery(prefix: bool, large: bool) -> Result<(), BuildError> {
    let smoke = Smoke::start_for("p13").await?;
    let model = model()?;
    let expected: Arc<str> = if large {
        Arc::from(TEXT.repeat(100_000))
    } else {
        Arc::from(TEXT)
    };
    let gateway = Gateway::start_with_text(true, prefix, 2, 200, expected.clone()).await?;
    let fixture = smoke
        .seed_tools(&model, false, &[], b"Return the retained answer.")
        .await;
    let config = gateway.config(true);
    let mut doomed =
        smoke.worker_with_model_recovery("p13-doomed", &model, &gateway.endpoint, None, &config)?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if gateway.calls.posts.load(Ordering::SeqCst) == 1 {
                if !prefix { break; }
                let offset: i64 = sqlx::query_scalar("SELECT next_assistant_byte_offset::bigint FROM runtime.loop_checkpoints WHERE run_id=$1")
                    .bind(fixture.run_id.into_uuid()).fetch_one(&smoke.pool).await.expect("durable prefix");
                if offset > 0 { break; }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await?;
    doomed.kill().await?;
    doomed.wait().await?;
    let mut replacement = smoke.worker_with_model_recovery(
        "p13-replacement",
        &model,
        &gateway.endpoint,
        None,
        &config,
    )?;
    smoke.wait_completed(fixture, &mut replacement).await;
    terminate(&mut replacement).await;
    let counts: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM runtime.model_invocations WHERE run_id=$1), (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id=$1)")
        .bind(fixture.run_id.into_uuid()).fetch_one(&smoke.pool).await?;
    assert_eq!(counts, (1, 1));
    let usage: (i64, i64, Option<i64>, Option<uuid::Uuid>) = sqlx::query_as(
        "SELECT input_tokens_uncached,output_tokens,total_tokens,outbox_message_id FROM runtime.model_usage_facts WHERE run_id=$1"
    ).bind(fixture.run_id.into_uuid()).fetch_one(&smoke.pool).await?;
    assert_eq!(
        usage,
        (5, 2, Some(7), None),
        "raw usage once; no kernel billing publication"
    );

    let rows: Vec<(uuid::Uuid, String, i64, Vec<u8>)> = sqlx::query_as("SELECT c.content_id,c.media_type,c.byte_length::bigint,c.content_hash FROM runtime.model_invocations i JOIN artifact.content_refs c ON c.content_id=i.response_content_id WHERE i.run_id=$1")
        .bind(fixture.run_id.into_uuid()).fetch_all(&smoke.pool).await?;
    let (id, media, len, hash) = &rows[0];
    let reference = musteragent_content_ref::ContentRef {
        content_id: ContentId::from_uuid(*id)?,
        media_type: musteragent_content_ref::MediaType::new(media)?,
        byte_length: u64::try_from(*len)?,
        sha256: musteragent_content_ref::Sha256::from_bytes(hash.clone())?,
    };
    assert_eq!(
        smoke.content.read(fixture.tenant_id, &reference).await?,
        expected.as_bytes()
    );
    assert_eq!(gateway.calls.posts.load(Ordering::SeqCst), 2);
    assert_eq!(gateway.calls.dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(gateway.calls.lookups.load(Ordering::SeqCst), 3);
    let requests = gateway.calls.requests.lock().expect("fixture lock");
    assert_eq!(
        requests[0], requests[1],
        "same key and native payload across workers"
    );
    eprintln!("P13 SIGKILL evidence: {}", smoke.dir.display());
    Ok(())
}
#[tokio::test]
async fn sigkill_recovers_original_completion_once() -> Result<(), BuildError> {
    killed_recovery(false, false).await
}
#[tokio::test]
async fn sigkill_recovers_after_durable_text_prefix_once() -> Result<(), BuildError> {
    killed_recovery(true, false).await
}

async fn rejected_lookup(
    kill_first: bool,
    resolver: bool,
    pending: usize,
    status: u16,
    code: musteragent_runtime_model::ProviderErrorCode,
) -> Result<(), BuildError> {
    let smoke = Smoke::start_for("p13").await?;
    let model = model()?;
    let gateway = Gateway::start(kill_first, false, pending, status).await?;
    let fixture = smoke
        .seed_tools(&model, false, &[], b"Read the original result.")
        .await;
    if kill_first {
        let mut doomed = smoke.worker_with_model_recovery(
            "p13-killed-before-failure",
            &model,
            &gateway.endpoint,
            None,
            &gateway.config(resolver),
        )?;
        tokio::time::timeout(Duration::from_secs(30), async {
            while gateway.calls.posts.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        doomed.kill().await?;
        doomed.wait().await?;
    }
    let mut worker = smoke.worker_with_model_recovery(
        "p13-lookup-error",
        &model,
        &gateway.endpoint,
        None,
        &gateway.config(resolver),
    )?;
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let state: i16 = sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
                .bind(fixture.run_id.into_uuid())
                .fetch_one(&smoke.pool)
                .await
                .expect("run status");
            if [5, 6, 7, 8].contains(&state) {
                assert_eq!(state, 6, "typed failure terminates the run");
                break;
            }
            assert!(worker.try_wait().expect("worker status").is_none());
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await?;
    terminate(&mut worker).await;
    let failures: Vec<Vec<u8>> =
        sqlx::query_scalar("SELECT terminal_error FROM runtime.model_invocations WHERE run_id=$1")
            .bind(fixture.run_id.into_uuid())
            .fetch_all(&smoke.pool)
            .await?;
    assert_eq!(
        failures,
        vec![code.runtime_code().as_str().as_bytes().to_vec()]
    );
    assert_eq!(
        gateway.calls.posts.load(Ordering::SeqCst),
        if kill_first { 2 } else { 1 }
    );
    assert_eq!(gateway.calls.dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(
        gateway.calls.lookups.load(Ordering::SeqCst),
        if !resolver {
            0
        } else if pending == usize::MAX {
            8
        } else {
            1
        }
    );
    let usage: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.model_usage_facts WHERE run_id=$1")
            .bind(fixture.run_id.into_uuid())
            .fetch_one(&smoke.pool)
            .await?;
    assert_eq!(usage, 0, "unknown usage cannot be invented");
    Ok(())
}
#[tokio::test]
async fn pending_forever_is_a_typed_run_failure() -> Result<(), BuildError> {
    rejected_lookup(
        false,
        true,
        usize::MAX,
        200,
        musteragent_runtime_model::ProviderErrorCode::ExistingInvocationTimeout,
    )
    .await
}
#[tokio::test]
async fn failed_original_result_is_a_typed_run_failure() -> Result<(), BuildError> {
    rejected_lookup(
        false,
        true,
        0,
        410,
        musteragent_runtime_model::ProviderErrorCode::ExistingInvocationFailed,
    )
    .await
}
#[tokio::test]
async fn missing_resolver_is_a_typed_run_failure_without_redispatch() -> Result<(), BuildError> {
    rejected_lookup(
        false,
        false,
        0,
        200,
        musteragent_runtime_model::ProviderErrorCode::ExistingInvocationResolverUnavailable,
    )
    .await
}

#[tokio::test]
async fn sigkill_recovers_large_completion_with_bounded_ledger_append() -> Result<(), BuildError> {
    killed_recovery(false, true).await
}

#[tokio::test]
async fn sigkill_lookup_failure_is_surfaced_without_another_invocation() -> Result<(), BuildError> {
    rejected_lookup(
        true,
        true,
        0,
        410,
        musteragent_runtime_model::ProviderErrorCode::ExistingInvocationFailed,
    )
    .await
}
