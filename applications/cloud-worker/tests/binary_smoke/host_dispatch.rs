//! End-to-end worker test for dynamically admitted host-dispatched backend capabilities:
//! release-bound snapshot admission, catalog search & activation, host execution via
//! `WorkerOverrides.capability_dispatchers` (`run_worker`) and `CloudWorkerComposition`,
//! explicit `CapabilityUnavailable` on unknown provider IDs, and derived delegation runs.
use super::*;
use musteragent_cloud_worker::{
    CapabilityDispatcher, CloudWorkerSettings, DispatchedCapabilityOutcome,
    DispatchedReconciliationOutcome, ProviderCapabilityDispatcher, ProviderDispatcherError,
    ToolPhysicalExecution, ToolServices, WorkerOverrides, kernel_dispatcher,
    opaque_dispatcher_execution, run_worker,
};
use musteragent_content_ref::MediaType;
use musteragent_policy::{ApprovalRequirement, Effect, PermissionMode};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    DurableToolInvocation, ToolExecutionMaterial,
};
use musteragent_runtime_content::PutContent;
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, OpaqueExternalRef, execution_liveness::ExecutionBinding,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

const CHILD_WORKER_CONFIG_ENV: &str = "K1_TEST_HOST_DISPATCH_WORKER_CONFIG";
const CHILD_MINIO_ENDPOINT_ENV: &str = "K1_TEST_MINIO_ENDPOINT";
const CHILD_MINIO_BUCKET_ENV: &str = "K1_TEST_MINIO_BUCKET";

fn inventory_provider_id() -> CapabilityProviderId {
    host_capability_provider_id(
        &uuid::Uuid::NAMESPACE_OID,
        "example.test/capability-provider/inventory.v1",
    )
    .expect("deterministic host provider id")
}

fn unregistered_provider_id() -> CapabilityProviderId {
    host_capability_provider_id(
        &uuid::Uuid::NAMESPACE_OID,
        "example.test/capability-provider/unregistered.v1",
    )
    .expect("deterministic unregistered provider id")
}

fn quote_input_schema_json() -> Vec<u8> {
    br#"{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"],"additionalProperties":false}"#.to_vec()
}

fn quote_output_schema_json() -> Vec<u8> {
    br#"{"type":"object","properties":{"sku":{"type":"string"},"unit_price":{"type":"integer"},"method":{"type":"string"}},"required":["sku","unit_price","method"],"additionalProperties":false}"#.to_vec()
}

struct InventoryHostDispatcher {
    provider_id: CapabilityProviderId,
    content: Arc<dyn ContentStore>,
    log_path: PathBuf,
}

impl CapabilityDispatcher for InventoryHostDispatcher {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, musteragent_loop::LoopPortError> {
        if invocation.capability_provider_id != self.provider_id {
            return Err(musteragent_loop::LoopPortError {
                code: ErrorCode::CapabilityUnavailable,
                retryable: false,
            });
        }
        opaque_dispatcher_execution(self, attempt, material, durable, invocation, binding)
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, musteragent_loop::LoopPortError>>
    {
        if invocation.capability_provider_id != self.provider_id {
            return Box::pin(async {
                Err(musteragent_loop::LoopPortError {
                    code: ErrorCode::CapabilityUnavailable,
                    retryable: false,
                })
            });
        }
        let Some(op) = material.tool.execution_target.backend_operation() else {
            return Box::pin(async {
                Err(musteragent_loop::LoopPortError {
                    code: ErrorCode::RuntimeSnapshotInvalid,
                    retryable: false,
                })
            });
        };
        let method = op.as_str().to_owned();
        let content = self.content.clone();
        let log_path = self.log_path.clone();
        Box::pin(async move {
            let req_bytes = content
                .read(invocation.tenant_id, &invocation.request_ref)
                .await
                .map_err(|_| musteragent_loop::LoopPortError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                })?;
            let args: Value = serde_json::from_slice(&req_bytes).expect("valid JSON args");
            let sku = args["sku"].as_str().unwrap_or("unknown").to_owned();
            {
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_path)
                    .expect("host dispatch log");
                writeln!(
                    file,
                    "{}",
                    json!({
                        "run_id": invocation.run_id.to_string(),
                        "tool_key": invocation.tool_key.as_str(),
                        "provider_id": invocation.capability_provider_id.to_string(),
                        "has_grant_proof": invocation.execution_grant_proof.is_some(),
                        "sku": sku,
                    })
                )
                .expect("write host dispatch log");
            }
            let price = if sku == "rack-unit-7" { 420 } else { 510 };
            let payload = serde_json::to_vec(&json!({
                "sku": sku,
                "unit_price": price,
                "method": method,
            }))
            .expect("serialize result");
            let result_ref = content
                .put(PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json").expect("media type"),
                    bytes: payload,
                })
                .await
                .map_err(|_| musteragent_loop::LoopPortError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                })?;
            Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(CapabilityResult {
                    result_hash: result_ref.sha256,
                    result_ref,
                    result_schema: invocation.result_schema,
                    provider_request_ref: Some(
                        OpaqueExternalRef::new(format!("inv-quote-{}", invocation.invocation_id))
                            .expect("provider request ref"),
                    ),
                }),
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<
        'a,
        Result<DispatchedReconciliationOutcome, musteragent_loop::LoopPortError>,
    > {
        Box::pin(async {
            Err(musteragent_loop::LoopPortError {
                code: ErrorCode::CapabilityUnavailable,
                retryable: false,
            })
        })
    }
}

async fn run_host_worker_process(config: PathBuf) -> Result<(), BuildError> {
    let mut signal = musteragent_cloud_lifecycle::ShutdownSignal::install()?;
    let settings = CloudWorkerSettings::load(&config)?;
    let minio_endpoint = std::env::var(CHILD_MINIO_ENDPOINT_ENV)?;
    let minio_bucket = std::env::var(CHILD_MINIO_BUCKET_ENV)?;
    let pool = sqlx::PgPool::connect(&settings.database_url).await?;
    let blobs = musteragent_object_store::ObjectStoreBlobStore::from_s3(
        musteragent_object_store::S3ObjectStoreConfig {
            endpoint: minio_endpoint,
            bucket: minio_bucket,
            region: "us-east-1".into(),
            prefix: "smoke".into(),
            access_key_id: MINIO_TEST_ACCESS_KEY.into(),
            secret_access_key: MINIO_TEST_SECRET_KEY.into(),
            allow_http: true,
            virtual_hosted_style: false,
            write_once: Default::default(),
        },
    )?;
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(blobs),
        running_loop::keyring(),
    ));
    let provider_id = inventory_provider_id();
    let dispatcher: Arc<dyn CapabilityDispatcher> = Arc::new(InventoryHostDispatcher {
        provider_id,
        content,
        log_path: config.with_extension("host-dispatches.jsonl"),
    });
    let result = run_worker(
        settings,
        WorkerOverrides {
            capability_dispatchers: BTreeMap::from([(provider_id, dispatcher)]),
            ..WorkerOverrides::default()
        },
        signal.recv(),
    )
    .await;
    pool.close().await;
    result
}

struct ScriptedModelServer {
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ScriptedModelServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ScriptedModelServer {
    async fn start(
        search_wire: String,
        activate_wire: String,
        spawn_wire: String,
    ) -> Result<Self, BuildError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1/chat/completions", listener.local_addr()?);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { return; };
                        let search = search_wire.clone();
                        let activate = activate_wire.clone();
                        let spawn = spawn_wire.clone();
                        connections.spawn(async move {
                            let _ = serve_model_request(socket, &search, &activate, &spawn).await;
                        });
                    }
                    Some(res) = connections.join_next() => {
                        if let Err(err) = res {
                            panic!("scripted model server task panicked: {err}");
                        }
                    }
                }
            }
        });
        Ok(Self { endpoint, task })
    }
}

fn flatten_content(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|p| p.get("text").and_then(Value::as_str).or_else(|| p.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn offered_tool(request: &Value, name: &str) -> bool {
    request["tools"]
        .as_array()
        .is_some_and(|tools| tools.iter().any(|t| t["function"]["name"] == name))
}

fn sse_tool_call(turn_tag: usize, name: &str, args: Value) -> String {
    let frames = [
        json!({
            "id": format!("chatcmpl_k1_{turn_tag}"),
            "choices": [{
                "index": 0,
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": format!("call_k1_{turn_tag}"),
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": args.to_string()
                        }
                    }]
                },
                "finish_reason": null
            }]
        })
        .to_string(),
        json!({
            "id": format!("chatcmpl_k1_{turn_tag}"),
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 24, "completion_tokens": 10, "total_tokens": 34}
        })
        .to_string(),
        "[DONE]".to_owned(),
    ];
    frames
        .into_iter()
        .map(|f| format!("data: {f}\n\n"))
        .collect()
}

fn sse_text(turn_tag: usize, text: &str) -> String {
    let frames = [
        json!({
            "id": format!("chatcmpl_k1_txt_{turn_tag}"),
            "choices": [{
                "index": 0,
                "delta": {"content": text},
                "finish_reason": null
            }]
        })
        .to_string(),
        json!({
            "id": format!("chatcmpl_k1_txt_{turn_tag}"),
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 24, "completion_tokens": 8, "total_tokens": 32}
        })
        .to_string(),
        "[DONE]".to_owned(),
    ];
    frames
        .into_iter()
        .map(|f| format!("data: {f}\n\n"))
        .collect()
}

async fn serve_model_request(
    mut socket: TcpStream,
    search_wire: &str,
    activate_wire: &str,
    spawn_wire: &str,
) -> Result<(), BuildError> {
    let mut bytes = Vec::new();
    let end = loop {
        let mut chunk = [0_u8; 4096];
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec())?;
    let content_len: usize = headers
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, v)| v.trim().parse().ok())
        })
        .unwrap_or(0);
    while bytes.len() - end < content_len {
        let mut chunk = [0_u8; 4096];
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    let request: Value = serde_json::from_slice(&bytes[end..end + content_len])?;
    let messages = request["messages"].as_array().expect("messages array");
    let turn_tag = messages.len();
    let users = messages
        .iter()
        .filter(|m| m["role"] == "user")
        .map(|m| flatten_content(&m["content"]))
        .collect::<Vec<_>>()
        .join("\n");
    let tool_outputs = messages
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| flatten_content(&m["content"]))
        .collect::<Vec<_>>()
        .join("\n");
    let called = |name: &str| {
        messages
            .iter()
            .flat_map(|m| m["tool_calls"].as_array().into_iter().flatten())
            .filter(|c| c["function"]["name"] == name)
            .count()
    };

    let is_k3_helper = users.contains("[e2e:k3-helper]");
    let is_child = users.contains("[e2e:k1-child]");
    let body = if is_k3_helper {
        if called(search_wire) == 0 {
            assert!(
                offered_tool(&request, "inventory_status_ping"),
                "initially exposed host tool must be offered before activation"
            );
            assert!(
                !offered_tool(&request, "inventory_quote_lookup"),
                "deferred host tool must not be offered before activation"
            );
            sse_tool_call(
                turn_tag,
                search_wire,
                json!({"query": "wholesale_tariff", "locations": ["backend"]}),
            )
        } else if called(activate_wire) == 0 {
            assert!(
                tool_outputs.contains("inventory_quote_lookup"),
                "catalog search must find deferred host tool via projected tag: {tool_outputs}"
            );
            sse_tool_call(
                turn_tag,
                activate_wire,
                json!({
                    "tool_names": ["inventory_quote_lookup"],
                    "reason": "Activate deferred wholesale quote tool"
                }),
            )
        } else if called("inventory_quote_lookup") == 0 {
            assert!(
                offered_tool(&request, "inventory_quote_lookup"),
                "activated deferred host tool must be offered to model"
            );
            sse_tool_call(
                turn_tag,
                "inventory_quote_lookup",
                json!({"sku": "rack-unit-7"}),
            )
        } else {
            assert!(
                tool_outputs.contains("\"unit_price\":420")
                    && tool_outputs.contains("\"method\":\"InventoryService.LookupQuote\""),
                "host dispatcher must execute activated deferred tool: {tool_outputs}"
            );
            sse_text(turn_tag, "k3_helper_deferred_dispatch_complete:420")
        }
    } else if !is_child {
        if called(search_wire) == 0 {
            assert!(
                !offered_tool(&request, "inventory_quote_lookup"),
                "dynamic tool must start unexposed before activation"
            );
            sse_tool_call(
                turn_tag,
                search_wire,
                json!({"query": "inventory", "locations": ["backend"]}),
            )
        } else if called(activate_wire) == 0 {
            assert!(
                tool_outputs.contains("inventory_quote_lookup")
                    && tool_outputs.contains("inventory_audit_ping"),
                "catalog search must discover both dynamic backend tools: {tool_outputs}"
            );
            sse_tool_call(
                turn_tag,
                activate_wire,
                json!({
                    "tool_names": ["inventory_quote_lookup", "inventory_audit_ping"],
                    "reason": "Activate host inventory quote and unregistered audit probe"
                }),
            )
        } else if called("inventory_audit_ping") == 0 {
            assert!(
                offered_tool(&request, "inventory_audit_ping"),
                "activated tool must be offered to model"
            );
            sse_tool_call(turn_tag, "inventory_audit_ping", json!({"sku": "probe"}))
        } else if called("inventory_quote_lookup") == 0 {
            assert!(
                tool_outputs.contains("capability_unavailable"),
                "unregistered host provider must fail with capability_unavailable: {tool_outputs}"
            );
            sse_tool_call(
                turn_tag,
                "inventory_quote_lookup",
                json!({"sku": "rack-unit-7"}),
            )
        } else if called(spawn_wire) == 0 {
            assert!(
                tool_outputs.contains("\"unit_price\":420"),
                "root run must receive host quote result: {tool_outputs}"
            );
            sse_tool_call(
                turn_tag,
                spawn_wire,
                json!({"task": "[e2e:k1-child] Discover, activate, and look up sku rack-unit-9."}),
            )
        } else if users.contains("child_quote_complete:rack-unit-9:510") {
            sse_text(turn_tag, "root_and_child_dynamic_dispatch_complete:420:510")
        } else {
            sse_text(turn_tag, "Waiting for delegated child quote.")
        }
    } else if called(search_wire) == 0 {
        assert!(
            !offered_tool(&request, "inventory_quote_lookup"),
            "derived child must also start with dynamic tool unexposed"
        );
        sse_tool_call(
            turn_tag,
            search_wire,
            json!({"query": "inventory quote", "locations": ["backend"]}),
        )
    } else if called(activate_wire) == 0 {
        assert!(
            tool_outputs.contains("inventory_quote_lookup"),
            "derived child search must discover dynamic backend tool: {tool_outputs}"
        );
        sse_tool_call(
            turn_tag,
            activate_wire,
            json!({
                "tool_names": ["inventory_quote_lookup"],
                "reason": "Child activates host inventory quote lookup"
            }),
        )
    } else if called("inventory_quote_lookup") == 0 {
        assert!(
            offered_tool(&request, "inventory_quote_lookup"),
            "derived child activated tool must be offered"
        );
        sse_tool_call(
            turn_tag,
            "inventory_quote_lookup",
            json!({"sku": "rack-unit-9"}),
        )
    } else {
        assert!(
            tool_outputs.contains("\"unit_price\":510"),
            "derived child must receive host quote result: {tool_outputs}"
        );
        sse_text(turn_tag, "child_quote_complete:rack-unit-9:510")
    };

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn seed_dynamic_orchestration_run(
    smoke: &Smoke,
    route: &musteragent_model::ModelRoute,
    input: &[u8],
) -> running_loop::Fixture {
    let (fixture, _) =
        running_loop::seed_running_loop_with_input(&smoke.pool, smoke.content.as_ref(), input)
            .await;
    let partitions = HostCatalogSnapshotBuilder::new(inventory_provider_id())
        .with_builtin_tools(["delegation.spawn@5"])
        .with_host_tools([
            HostToolDescriptor::new(
                "inventory.quote.lookup@1",
                "Look up live inventory pricing by SKU.",
                quote_input_schema_json(),
                quote_output_schema_json(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.LookupQuote",
            )
            .with_title("Lookup Inventory Quote")
            .with_scopes(["inventory.quote.read"]),
            HostToolDescriptor::new(
                "inventory.audit.ping@1",
                "Probe an inventory audit endpoint.",
                quote_input_schema_json(),
                quote_output_schema_json(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.PingAudit",
            )
            .with_title("Ping Inventory Audit")
            .with_scopes(["inventory.quote.read"])
            .with_provider_id(unregistered_provider_id()),
        ])
        .with_initial_exposed_tools(["delegation.spawn@5"])
        .build()
        .expect("host catalog partitions");
    install_partitions_on_fixture(smoke, route, fixture, partitions, input).await
}

async fn install_partitions_on_fixture(
    smoke: &Smoke,
    route: &musteragent_model::ModelRoute,
    fixture: running_loop::Fixture,
    partitions: HostCatalogSnapshotPartitions,
    input: &[u8],
) -> running_loop::Fixture {
    let snapshot = source_selection_snapshot::isolated_snapshot(
        smoke.content.clone(),
        running_loop::run_context(fixture),
        route.clone(),
    )
    .await;
    let mut snapshot = liveness_admission::seal(
        snapshot,
        Arc::new(
            RunExecutionPolicySnapshot::production_defaults()
                .liveness
                .expect("liveness")
                .model_policy(),
        ),
    );
    snapshot.content.policy.permission_mode = PermissionMode::ServiceManaged;
    production_context::install_production_context(
        &smoke.pool,
        smoke.content.clone(),
        fixture,
        Arc::new(ProtobufLedgerPayloadCodec),
        snapshot.content.clone(),
        input,
    )
    .await;

    partitions.apply_to(&mut snapshot.content);
    let sealed = RuntimeSnapshot::seal(
        fixture.runtime_snapshot_id,
        snapshot.content,
        running_loop::database_now(&smoke.pool).await,
        &musteragent_wire::ProtobufSnapshotCodec,
    )
    .expect("release-bound snapshot built from HostCatalogSnapshotBuilder seals");

    sqlx::query(
        "UPDATE capability.runtime_snapshots
         SET schema_version = $2, encoded_snapshot = $3, content_hash = $4
         WHERE runtime_snapshot_id = $1",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(sealed.content.schema_version.as_str())
    .bind(&sealed.encoded_content)
    .bind(sealed.content_hash.into_bytes().to_vec())
    .execute(&smoke.pool)
    .await
    .expect("persist sealed helper-built RuntimeSnapshot");
    sqlx::query("UPDATE runtime.leases SET expires_at_ms=renewed_at_ms WHERE run_id=$1")
        .bind(fixture.run_id.into_uuid())
        .execute(&smoke.pool)
        .await
        .expect("expire predecessor lease");
    fixture
}

#[tokio::test]
async fn host_dispatcher_executes_discovered_dynamic_tool_in_root_and_derived_runs()
-> Result<(), BuildError> {
    if let Some(config) = std::env::var_os(CHILD_WORKER_CONFIG_ENV) {
        return run_host_worker_process(config.into()).await;
    }

    let smoke = Smoke::start_for("k1").await?;
    let mut route = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    route.api_format = musteragent_model::ApiFormat::OpenAiChatCompletions;
    route.adapter_version = VersionedKey::new("openai.chat-adapter@1")?;
    route.protocol_version = VersionedKey::new("openai.chat@1")?;

    let kernel_release = compiled_product_release(ProductId::KERNEL);
    let catalog =
        BuiltinToolCatalog::for_product_release(&kernel_release, ProductExecutionTarget::Cloud)?;
    let wire_for = |logical: &str| {
        catalog
            .definitions()
            .iter()
            .find(|d| d.key.logical_key() == logical)
            .expect("builtin tool")
            .wire_name
            .as_str()
            .to_owned()
    };
    let server = ScriptedModelServer::start(
        wire_for("tool.catalog.search"),
        wire_for("tool.catalog.activate"),
        wire_for("delegation.spawn"),
    )
    .await?;

    // Verify `ProviderCapabilityDispatcher` also composes cleanly with `kernel_dispatcher`
    // (for `CloudWorkerComposition` callers) and rejects builtin provider IDs.
    {
        let dummy_model = Arc::new(
            musteragent_model_providers::DirectModelProvider::new(
                Arc::new(musteragent_model_providers::StaticEndpointRegistry::new(
                    Vec::new(),
                    true,
                )?),
                musteragent_cloud_worker::EnvironmentCredentials::new(&[])?,
                Arc::new(musteragent_cloud_worker::RuntimeModelContentResolver::new(
                    smoke.content.clone(),
                )),
                musteragent_model_providers::ProviderAdapterConfig::default(),
            )
            .map_err(|e| format!("dummy model provider: {:?}", e.safe_code))?,
        );
        struct NoopMetering;
        impl musteragent_runtime_model::MeteringPublicationEncoder for NoopMetering {
            fn encode_billable_usage(
                &self,
                fact: &musteragent_runtime_model::AttributedModelUsageFact,
                _id: OutboxMessageId,
            ) -> Result<
                Option<musteragent_runtime_model::MeteringPublication>,
                musteragent_runtime_model::ModelContractError,
            > {
                fact.validate()?;
                Ok(None)
            }
            fn encode(
                &self,
                fact: &musteragent_runtime_model::AttributedModelUsageFact,
                id: OutboxMessageId,
            ) -> Result<
                musteragent_runtime_model::MeteringPublication,
                musteragent_runtime_model::ModelContractError,
            > {
                musteragent_wire::ProtobufMeteringPublicationEncoder.encode(fact, id)
            }
        }
        let db = PostgresDatabaseRuntime::from_pool(smoke.pool.clone());
        let base_dispatcher = kernel_dispatcher(ToolServices {
            database: db.clone(),
            content: smoke.content.clone(),
            keys: running_loop::keyring(),
            clock: Arc::new(musteragent_execution_liveness_io::SystemLivenessHomeClock::default()),
            providers: vec![],
            handlers: vec![],
            media: vec![],
            embeddings: None,
            model: dummy_model.clone(),
            metering: Arc::new(NoopMetering),
            releases: vec![],
            browser_unavailable_notice: Default::default(),
        })?;
        let rust_id: CapabilityProviderId = BUILTIN_RUST_PROVIDER_ID.parse()?;
        let host_disp: Arc<dyn CapabilityDispatcher> = Arc::new(InventoryHostDispatcher {
            provider_id: inventory_provider_id(),
            content: smoke.content.clone(),
            log_path: smoke.dir.join("unused.jsonl"),
        });
        assert_eq!(
            ProviderCapabilityDispatcher::new(
                base_dispatcher.clone(),
                [(rust_id, host_disp.clone())]
            )
            .err(),
            Some(ProviderDispatcherError::BuiltinProvider(rust_id))
        );
        let composed = Arc::new(ProviderCapabilityDispatcher::new(
            base_dispatcher,
            BTreeMap::from([(inventory_provider_id(), host_disp)]),
        )?);
        let _worker_composition = musteragent_cloud_worker::CloudWorkerComposition::build(
            musteragent_cloud_worker::WorkerHost {
                database: db.clone(),
                content: smoke.content.clone(),
                model: dummy_model,
                metering: Arc::new(NoopMetering),
                reference_keys: running_loop::keyring(),
                recovery_keys: running_loop::recovery_keyring(),
                dispatcher: composed,
                extensions: musteragent_cloud_worker::WorkerExtensionConfig {
                    registry: musteragent_extension::ExtensionRegistry::default(),
                    state_root: smoke.dir.join("comp-state"),
                    workspace_root: smoke.dir.join("comp-workspaces"),
                    subagent_agents: vec![],
                    loop_detection: musteragent_extension::LoopDetectionConfig::default(),
                },
                observer: None,
                liveness_clock: Arc::new(
                    musteragent_execution_liveness_io::SystemLivenessHomeClock::default(),
                ),
            },
            musteragent_cloud_worker::WorkerPolicy {
                worker_id: musteragent_run::WorkerId::new("k1-comp")?,
                concurrency: std::num::NonZeroUsize::MIN,
                lease: Duration::from_secs(5),
                heartbeat: Duration::from_millis(500),
                drain: Duration::from_secs(5),
            },
        )
        .await?;
    }

    let parent = seed_dynamic_orchestration_run(
        &smoke,
        &route,
        b"[e2e:k1-root] Discover dynamic inventory tools, verify unknown provider rejection, look up rack-unit-7, and delegate rack-unit-9.",
    )
    .await;

    let config = smoke.worker_config("k1-host-worker", &route, &server.endpoint, None, "")?;
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.args([
        "--exact",
        "host_dispatch::host_dispatcher_executes_discovered_dynamic_tool_in_root_and_derived_runs",
    ])
    .env(CHILD_WORKER_CONFIG_ENV, &config)
    .env(CHILD_MINIO_ENDPOINT_ENV, &smoke.minio.endpoint)
    .env(CHILD_MINIO_BUCKET_ENV, smoke.minio.bucket)
    .env("P9_MODEL_KEY", "test-only-model-key");
    let mut worker = smoke.spawn_worker("k1-host-worker", cmd)?;

    smoke.wait_completed(parent, &mut worker).await;
    terminate(&mut worker).await;

    // Verify delegation child completed and derived authority + re-derived grant were persisted.
    let delegations: Vec<(uuid::Uuid, i16)> = sqlx::query_as(
        "SELECT child_run_id, status FROM runtime.delegations WHERE parent_run_id=$1",
    )
    .bind(parent.run_id.into_uuid())
    .fetch_all(&smoke.pool)
    .await?;
    assert_eq!(delegations.len(), 1);
    assert_eq!(delegations[0].1, 2, "delegation must complete successfully");
    let child_uuid = delegations[0].0;
    let child_run_id = RunId::from_uuid(child_uuid)?;

    let child_run_status: i16 =
        sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
            .bind(child_uuid)
            .fetch_one(&smoke.pool)
            .await?;
    assert_eq!(child_run_status, 5, "derived child run must complete");

    let derived_authorities: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime.derived_run_authorities WHERE run_id=$1 AND source_run_id=$2",
    )
    .bind(child_uuid)
    .bind(parent.run_id.into_uuid())
    .fetch_one(&smoke.pool)
    .await?;
    assert_eq!(derived_authorities, 1);

    let child_snap_bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT s.encoded_snapshot FROM runtime.runs r JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE r.run_id=$1",
    )
    .bind(child_uuid)
    .fetch_one(&smoke.pool)
    .await?;
    let child_snap = musteragent_wire::ProtobufSnapshotCodec.decode_content(&child_snap_bytes)?;
    assert_eq!(
        child_snap.product_release,
        Some(compiled_product_release(ProductId::KERNEL))
    );
    assert_eq!(
        child_snap.model.credential_grant_ref.as_str(),
        format!("grant_derived_{child_run_id}")
    );
    assert!(
        child_snap
            .tools
            .iter()
            .any(|t| t.manifest.key.as_str() == "inventory.quote.lookup@1")
    );

    // Verify tool invocation statuses in PostgreSQL for both root and derived runs.
    let root_succeeded: Vec<String> = sqlx::query_scalar(
        "SELECT tool_ref FROM tool.tool_invocations WHERE run_id=$1 AND status=4 ORDER BY tool_ref",
    )
    .bind(parent.run_id.into_uuid())
    .fetch_all(&smoke.pool)
    .await?;
    assert!(
        root_succeeded
            .iter()
            .any(|k| k.starts_with("tool.catalog.search@"))
    );
    assert!(
        root_succeeded
            .iter()
            .any(|k| k.starts_with("tool.catalog.activate@"))
    );
    assert!(root_succeeded.contains(&"inventory.quote.lookup@1".to_owned()));
    assert!(
        root_succeeded
            .iter()
            .any(|k| k.starts_with("delegation.spawn@"))
    );

    let root_failed: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT tool_ref, terminal_error FROM tool.tool_invocations WHERE run_id=$1 AND status=5",
    )
    .bind(parent.run_id.into_uuid())
    .fetch_all(&smoke.pool)
    .await?;
    assert_eq!(root_failed.len(), 1);
    assert_eq!(root_failed[0].0, "inventory.audit.ping@1");
    assert!(
        String::from_utf8_lossy(&root_failed[0].1).contains("capability_unavailable"),
        "unexpected terminal_error: {:?}",
        String::from_utf8_lossy(&root_failed[0].1)
    );

    let child_succeeded: Vec<String> = sqlx::query_scalar(
        "SELECT tool_ref FROM tool.tool_invocations WHERE run_id=$1 AND status=4 ORDER BY tool_ref",
    )
    .bind(child_uuid)
    .fetch_all(&smoke.pool)
    .await?;
    assert!(
        child_succeeded
            .iter()
            .any(|k| k.starts_with("tool.catalog.search@"))
    );
    assert!(
        child_succeeded
            .iter()
            .any(|k| k.starts_with("tool.catalog.activate@"))
    );
    assert!(child_succeeded.contains(&"inventory.quote.lookup@1".to_owned()));

    // Verify host dispatcher observed both root and derived child invocations.
    let dispatch_log = std::fs::read_to_string(config.with_extension("host-dispatches.jsonl"))?;
    let records: Vec<Value> = dispatch_log
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        records.len(),
        2,
        "expected 1 root + 1 derived child host call"
    );
    assert_eq!(records[0]["run_id"], parent.run_id.to_string());
    assert_eq!(records[0]["tool_key"], "inventory.quote.lookup@1");
    assert_eq!(
        records[0]["provider_id"],
        inventory_provider_id().to_string()
    );
    assert_eq!(records[0]["sku"], "rack-unit-7");

    assert_eq!(records[1]["run_id"], child_run_id.to_string());
    assert_eq!(records[1]["tool_key"], "inventory.quote.lookup@1");
    assert_eq!(
        records[1]["provider_id"],
        inventory_provider_id().to_string()
    );
    assert_eq!(records[1]["has_grant_proof"], false);
    assert_eq!(records[1]["sku"], "rack-unit-9");

    Ok(())
}

#[tokio::test]
async fn helper_built_snapshot_admits_searches_activates_and_executes_deferred_tool()
-> Result<(), BuildError> {
    if let Some(config) = std::env::var_os(CHILD_WORKER_CONFIG_ENV) {
        return run_host_worker_process(config.into()).await;
    }

    let smoke = Smoke::start_for("k3").await?;
    let mut route = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    route.api_format = musteragent_model::ApiFormat::OpenAiChatCompletions;
    route.adapter_version = VersionedKey::new("openai.chat-adapter@1")?;
    route.protocol_version = VersionedKey::new("openai.chat@1")?;

    let kernel_release = compiled_product_release(ProductId::KERNEL);
    let catalog =
        BuiltinToolCatalog::for_product_release(&kernel_release, ProductExecutionTarget::Cloud)?;
    let wire_for = |logical: &str| {
        catalog
            .definitions()
            .iter()
            .find(|d| d.key.logical_key() == logical)
            .expect("builtin tool")
            .wire_name
            .as_str()
            .to_owned()
    };
    let server = ScriptedModelServer::start(
        wire_for("tool.catalog.search"),
        wire_for("tool.catalog.activate"),
        wire_for("delegation.spawn"),
    )
    .await?;

    let input =
        b"[e2e:k3-helper] Search for the wholesale_tariff tool, activate it, and quote rack-unit-7.";
    let (fixture, _) =
        running_loop::seed_running_loop_with_input(&smoke.pool, smoke.content.as_ref(), input)
            .await;
    let tenant_id = fixture.tenant_id;
    let instructions_ref = smoke
        .content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/markdown;charset=utf-8")?,
            bytes: b"# Inventory Operations\nUse wholesale quote tools when requested.".to_vec(),
        })
        .await?;
    let manifest_ref = smoke
        .content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/json")?,
            bytes: br#"{"schema_version":"musteragent.skill-package.v2","stable_skill_key":"inventory-ops-guide","revision":1}"#.to_vec(),
        })
        .await?;
    let package_ref = smoke
        .content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/zip")?,
            bytes: b"PK-inventory-ops-guide-v1".to_vec(),
        })
        .await?;

    let partitions = HostCatalogSnapshotBuilder::new(inventory_provider_id())
        .with_host_tools([
            HostToolDescriptor::new(
                "inventory.status.ping@1",
                "Check live status of the inventory service.",
                quote_input_schema_json(),
                quote_output_schema_json(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.CheckStatus",
            ),
            HostToolDescriptor::new(
                "inventory.quote.lookup@1",
                "Look up live inventory pricing by SKU.",
                quote_input_schema_json(),
                quote_output_schema_json(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.LookupQuote",
            )
            .with_when_to_use("Use when computing wholesale hardware quotes.")
            .with_tags(["wholesale_tariff", "sku_quote"]),
        ])
        .with_host_skills([HostSkillDescriptor::new(
            SkillId::generate(),
            SkillVersionId::generate(),
            "inventory-ops-guide@1",
            "Inventory Operations Guide",
            "Guidance for wholesale inventory pricing workflows.",
            "Use when handling SKU pricing requests.",
            tenant_id,
            HostSkillProvenance::hub(
                "https://hub.example.test/skills/inventory-ops-guide@1",
                "example.test",
            ),
            instructions_ref,
            manifest_ref,
            package_ref,
        )
        .with_required_capabilities(["inventory.quote.lookup@1"])])
        .with_initial_exposed_tools(["inventory.status.ping@1"])
        .build()
        .map_err(|err| err.to_string())?;

    let run = install_partitions_on_fixture(&smoke, &route, fixture, partitions, input).await;

    let config = smoke.worker_config("k3-host-worker", &route, &server.endpoint, None, "")?;
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.args([
        "--exact",
        "host_dispatch::helper_built_snapshot_admits_searches_activates_and_executes_deferred_tool",
    ])
    .env(CHILD_WORKER_CONFIG_ENV, &config)
    .env(CHILD_MINIO_ENDPOINT_ENV, &smoke.minio.endpoint)
    .env(CHILD_MINIO_BUCKET_ENV, smoke.minio.bucket)
    .env("P9_MODEL_KEY", "test-only-model-key");
    let mut worker = smoke.spawn_worker("k3-host-worker", cmd)?;

    smoke.wait_completed(run, &mut worker).await;
    terminate(&mut worker).await;

    let succeeded: Vec<String> = sqlx::query_scalar(
        "SELECT tool_ref FROM tool.tool_invocations WHERE run_id=$1 AND status=4 ORDER BY tool_ref",
    )
    .bind(run.run_id.into_uuid())
    .fetch_all(&smoke.pool)
    .await?;
    assert!(
        succeeded
            .iter()
            .any(|k| k.starts_with("tool.catalog.search@"))
    );
    assert!(
        succeeded
            .iter()
            .any(|k| k.starts_with("tool.catalog.activate@"))
    );
    assert!(succeeded.contains(&"inventory.quote.lookup@1".to_owned()));

    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT s.encoded_snapshot FROM runtime.runs r JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE r.run_id=$1",
    )
    .bind(run.run_id.into_uuid())
    .fetch_one(&smoke.pool)
    .await?;
    let persisted = musteragent_wire::ProtobufSnapshotCodec.decode_content(&encoded)?;
    assert!(
        persisted
            .skills
            .iter()
            .any(|s| s.skill_ref.as_str() == "inventory-ops-guide@1")
    );
    let discovery = persisted.discovery.expect("persisted discovery");
    assert!(
        discovery
            .initial_exposed_tool_keys
            .iter()
            .any(|k| k.as_str() == "inventory.status.ping@1")
    );
    assert!(
        !discovery
            .initial_exposed_tool_keys
            .iter()
            .any(|k| k.as_str() == "inventory.quote.lookup@1")
    );

    let dispatch_log = std::fs::read_to_string(config.with_extension("host-dispatches.jsonl"))?;
    let records: Vec<Value> = dispatch_log
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["run_id"], run.run_id.to_string());
    assert_eq!(records[0]["tool_key"], "inventory.quote.lookup@1");
    assert_eq!(
        records[0]["provider_id"],
        inventory_provider_id().to_string()
    );
    assert_eq!(records[0]["sku"], "rack-unit-7");

    Ok(())
}
