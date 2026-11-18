# PostgreSQL cloud execution

The worker, projector and migrator are separate processes. A host admits a run
through the kernel admission ports and supplies a complete immutable snapshot.
The worker does not expose an admission HTTP API. Scheduled occurrences are
dispatched by the worker. Cloud delegation supports child admission, messages,
completion delivery, cascading stop and restart recovery; see
[orchestration integration](cloud-orchestration.md).

## Process startup

Build with `cargo build --offline -p musteragent-cloud-worker -p
musteragent-cloud-projector -p musteragent-database-migrator`.

1. Set `MUSTERAGENT_DATABASE_URL` for a kernel-owned PostgreSQL database with
   pgvector, then run `musteragent-database-migrator apply`.
2. Run `musteragent-cloud-projector` with `MUSTERAGENT_PROJECTOR_ID` and the same
   database URL. It consumes dispatch and evolution publications; it never bills.
3. Run `musteragent-cloud-worker /absolute/path/worker.toml`.

Services verify the complete migration chain before claiming work. Only the
migrator applies the kernel baseline and incremental migrations. It also repairs
content retention references from the original immutable ledger bytes. It does
not rewrite facts or accept a product database lineage.

Example worker configuration (all identities and paths belong to the host):

```toml
database_url = "postgres://worker@localhost/kernel"
worker_id = "cloud-worker-1"
key_id = "host.content.v1"
keyring_file = "/run/secrets/kernel-content-keyring.json"
extension_state_root = "/var/lib/kernel/extensions"
extension_workspace_root = "/var/lib/kernel/workspaces"
concurrency = 8
lease_ms = 30000
heartbeat_ms = 5000
drain_ms = 30000
tool_releases = []

[object_store]
endpoint = "https://s3.example.test"
region = "us-east-1"
bucket = "kernel-content"
prefix = "runtime"
access_key_env = "HOST_S3_ACCESS_KEY"
secret_key_env = "HOST_S3_SECRET_KEY"
allow_http = false
virtual_hosted_style = false

[[models]]
provider_id = "01982f80-0000-7000-8000-000000000411"
entry_id = "01982f80-0000-7000-8000-000000000412"
protocol = "openai-chat"
base_url = "https://models.example.test/v1"
credential_env = "HOST_MODEL_CREDENTIAL"
allow_loopback_http = false
[models.headers]
x-host-deployment = "worker"
```

`MUSTERAGENT_DATABASE_URL` and `MUSTERAGENT_WORKER_ID` override the file values.
Unknown configuration fields fail validation. Model protocols are `openai-chat`,
`openai-responses`, and `anthropic-messages`; their respective endpoint suffixes
are `chat/completions`, `responses`, and `messages`. HTTP model endpoints require
an explicit loopback opt-in. S3 supports a host endpoint, signing region, bucket,
prefix and either addressing style; plain HTTP requires an explicit opt-in.
The worker and reference API use the same `[object_store]` settings. Omit
`write_once` for standard S3 or set it explicitly in that table.

```toml
write_once = { strategy = "standard" }
```

For a bucket that requires a forbid-overwrite header, configure its documented
name in both services. The adapter accepts dedicated `x-` extension headers
and reserves the `x-amz-` namespace for the S3 protocol. This example selects
the OSS header.

```toml
write_once = { strategy = "forbid-overwrite-header", header = "x-oss-forbid-overwrite" }
```

Use `x-cos-forbid-overwrite` for a bucket that implements the COS header. The
adapter sends the selected header with value `true` on PutObject and on the
destination CompleteMultipartUpload. Streaming writes stage and verify the
whole input, then use UploadPartCopy and conditional completion for each
destination segment. A collision triggers a reread. Identical bytes return
`AlreadyPresent`. Different bytes fail and the original survives.

**Keep bucket versioning disabled for the entire lifetime of a deployment
using a forbid-overwrite header.** Enabled or suspended versioning can allow
an overwrite despite the header. Apply the same policy across the content
prefix and its private staging keys. [OSS PutObject](https://www.alibabacloud.com/help/en/oss/developer-reference/putobject) · [COS Complete Multipart Upload](https://www.tencentcloud.com/document/product/436/7742)

Before the first use of each write path, the adapter tests a collision on
fresh private keys under `.staging/` and verifies the original bytes. A
successful check is shared by clones of that adapter. A rejected or ignored
precondition fails the write. Permit read, write, multipart copy, abort and
delete under the configured prefix. Configure lifecycle cleanup for abandoned
multipart uploads and private staging objects after a crash.

Run acceptance against the real bucket before enabling writers and after any
change to versioning, access policy or gateways. With fresh test identities,
exercise buffered and streaming create, replay identical bytes, attempt
different bytes with the same identity, and read back the original. Restart
the clients after configuration changes to repeat the capability checks.

The content keyring uses `musteragent.opaque-keyring.v2` and an array of
`{key_id, key_base64}` records. Each key is 32 bytes. Retain old decryption keys
while referenced content or completion recovery material exists. The executable
reads S3/model credentials from the explicitly named environment variables.

OTLP uses the optional `musteragent-telemetry-otlp` environment configuration.
Local structured logs work without an OTLP collector. Refer to that crate's
`TelemetryConfig` for endpoint, TLS, signal enablement and exporter health rules.

## Host ports

`CloudWorkerComposition::build(WorkerHost, WorkerPolicy)` accepts the content
store, model provider, metering encoder, dispatcher, extension registry, keyrings
and liveness clock. `kernel_dispatcher(ToolServices)` composes the native tools,
including interaction, discovery, plans, files, resources, memory, skills,
WorkItems, schedules, standing goals, history, publication, vision and sandbox jobs.
External capabilities and media providers are explicit registrations. Missing
host services produce typed unavailable errors. No product RPC adapter is linked.

`CapabilityProvider` in `musteragent-runtime-capability` is the host invocation
and reconciliation port. `rpc_capability_provider.proto` describes the slim
content-reference contract. A transport implementation must preserve the actor,
attempt fence, idempotency identity and reconciliation result; no billing,
account-application, channel or office RPC is part of it.

For direct model HTTP, compose `DirectModelProvider` with `HostRequestHeaders`.
Static headers and `ModelRequestHeaderHook` are scoped to the pinned route and
invocation. The hook can sign the invocation authority. Reserved authentication,
framing and idempotency headers cannot be replaced. The `idempotency-key` is
`model-{invocation_id}` by default; rebuilding a provider process for the same
persisted invocation preserves it. The extension API also provides an idempotency annotation
port: a host using that override must derive the key from the persisted invocation
and reuse it across retry/resume; arbitrary static/dynamic metadata cannot replace
the selected key. Credential refresh uses `CredentialResolver`; scoped
credentials are resolved again when their lease expires before dispatch.

For an idempotent gateway, configure `[models.existing_invocation]` to recover
an accepted response after worker loss with the original invocation and key.
See [accepted model invocation lookup](existing-model-invocations.md) for the
host classifier, resolver port, GET contract, bounded lookup, typed errors and
fenced completion-intent recovery. The standalone worker supports this contract.

`MeteringPublicationEncoder::encode_billable_usage` may return `None`. Raw model
usage still commits with the invocation. The kernel neither decides a price nor
charges an account. The standalone worker intentionally emits no billing outbox.

Optional embeddings use `[embeddings]` with `endpoint`, `model_key`, `model_revision`,
`upstream_model`, `credential_env`, `max_input_tokens`, and
`allow_loopback_http`. These settings must agree with the host's registered
embedding space. The worker wires the provider into memory retrieval and the
leased resource indexing worker.

Extensions wrap the original loop, tool dispatcher and invocation policy.
Their append-only PostgreSQL facts and outbox observations share the conversation
commit lane and exact attempt fence. State roots are host-owned absolute paths.
Background and resumed work have no synthetic command ingress. The library host
can register extensions; the worker supplies the real restricted subagent port
using `subagent_agents`. The standalone extension registry is empty. Maintenance runs use the Dream policy; evaluation runs use the standing
goal evaluation policy. Neither path is coerced into chat.

## Immutable host tool publications

Use `workspace_releases_file` for workspace connector bundles. Its schema, publication and admission settings are described in [host-published workspace tools](workspace-releases.md).

`ProductId::parse` accepts a host key. Install trusted `HostProductRelease` values
once, before constructing any worker. The executable accepts their JSON file
paths in `tool_releases`. A publication has this shape:

```text
schema_version: "musteragent.host-tool-release.v1"
product: host ProductId
release_id: versioned immutable release key
profile_id, cloud_head, device_head: bounded nonempty publication metadata
skill_schema: exact inherited Skill schema
skill_release: {product, release_id, digest: [32 bytes]}
cloud, device: arrays of complete released Tool contracts
```

Tool document fields and canonical schema digests use the same contract as
`contracts/kernel-tools.json`. The publication digest hashes the exact JSON
bytes. Admission and every worker generation must receive identical bytes.
`skill_release` explicitly pins a compiled kernel Skill publication; there is no
implicit active release or fallback. Host-specific installed Skills still enter
through the existing admitted Skill catalog. Unknown or altered publication
identities fail closed. Duplicate identical installation is idempotent; changing
the installed publication set requires a new process generation.

### Named backend methods

Declare the backend method in each complete tool document that the host releases.
For example, an ordinary read tool can include these fields.

```json
{
  "target": { "owner": "backend_named_capability", "binding": "Records.Lookup" },
  "effect": "read",
  "idempotency": "native",
  "egress_policy": "provider_bound"
}
```

The binding is case sensitive and limited to 128 ASCII bytes. The service and
method each match `[A-Za-z][A-Za-z0-9_]*`, separated by one period. The generic
method `execute_tool` is reserved, including case and underscore variants.
Declare the tool key, capability key, schemas, scopes, approval and surfaces in
the same release. Choose the egress policy for that specific operation.

In the embedding worker, register a `CapabilityProvider` in `ToolServices.providers`.
Its manifest names the capability key, provider ID and backend location. Its
`backend_operation()` returns the exact `BackendOperation::new("Records.Lookup", None)`
binding. The adapter calls the corresponding concrete host method. Operation
selection comes from the release and the adapter. Model arguments supply that
method's schema-checked input. A missing adapter or mismatched method fails
before invocation or reconciliation.

For a durable image or video job, declare `metadata.artifact_type` as `"image"`
or `"video"`, `effect` as `"write_external"` and `idempotency` as
`"provider_task_record"`. Keep the existing sandbox and wire-name metadata in
that object. Register a `BackendMediaCapabilityProvider` in `ToolServices.media`.
Its `operation()` returns the exact method paired with `Some(MediaArtifactKind::Image)`
or `Some(MediaArtifactKind::Video)`. The kernel uses this declaration to select
the materializer, check the source proof, and permit admission-time projection of
the input schema. The output schema stays pinned. [Media composition](work-items-and-media.md)

The snapshot seals the method in `ToolManifestSnapshot.execution_target` and the
optional media kind in `backend_artifact_type`. Both participate in release
contract comparison. Ship the same release bytes to admission and execution.
A method rename creates a different release digest.

## Sandbox and process lifecycle

Host admission calls `PostgresSandboxRoutingStore::bind_run_context` with the
original actor, surface, trigger and policy. This immutable context is never
inferred from model arguments. Tenant Skill sandbox declarations are published
through `publish_skill_compatibility` after host manifest validation.

The worker's Python/environment handlers call the shared router through the
PostgreSQL gateway. A route is recorded on the original tool invocation and as a
ledger event before enqueue. Server-required Skills force a server route.
`BrowserOnly` without an online browser returns a typed error and creates no
server job.

For an in-process server provider, add `[[sandboxes]]` entries containing
`tenant_id`, `principal_id`, `provider_id` and a `config` conforming to
`musteragent_sandbox_server::ServerSandboxConfig`. Image, interpreter, resource
limits, egress policy, non-root user and workspace root are host configuration.
The worker verifies that the image exists locally. It does not pull images or
substitute a runtime. The binding authorizes one original actor; hosts needing a
larger fleet can run the same provider separately.

SIGTERM/SIGINT stop claims, cancel physical operations and join owned work.
Lease renewal runs alongside execution. A lost lease cannot seal a terminal
outcome. After an ungraceful process death, the scheduler claims a new fenced
attempt; recovery reads the original ledger, model completion intent and tool
receipts. No in-memory cache is recovery authority.
