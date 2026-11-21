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
# Optional: localized text for the retryable `browser_execution_unavailable`
# tool failure (no open Web page runs the browser sandbox). English default.
# browser_unavailable_message = "No browser execution environment is available. Keep the Web page that runs the browser sandbox open, then retry."

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

## Per-invocation model credentials

A host executable can call `run_worker(settings, overrides, shutdown)` from
`musteragent-cloud-worker`. It starts the same PostgreSQL worker, dispatcher,
extensions, background tasks and recovery paths as the CLI. The shutdown
future returns `std::io::Result<()>`. The library drains its tasks before it
returns. Install the process signal handler before loading settings.

This example adapts a host's existing grant client. The host supplies
`GrantClient::connect` and `issue`; the latter authenticates the grant binding
and returns a secret bearer and its UTC expiry.

```rust
use std::{path::Path, sync::Arc};
use musteragent_cloud_worker::{BuildError, CloudWorkerSettings, WorkerOverrides, run_worker};
use musteragent_identity::InvocationId;
use musteragent_model::{CredentialGrantRef, ModelRoute};
use musteragent_model_providers::{CredentialMaterial, CredentialResolver, CredentialResolverError};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::ModelInvocationRouteRole;

struct HostCredentials(GrantClient);

impl CredentialResolver for HostCredentials {
    fn resolve<'a>(&'a self, grant: &'a CredentialGrantRef, route: &'a ModelRoute,
        role: ModelInvocationRouteRole, invocation: InvocationId,
    ) -> ApplicationFuture<'a, Result<CredentialMaterial, CredentialResolverError>> {
        Box::pin(async move {
            let (secret, expires_at) = self.0.issue(grant, route, role, invocation).await?;
            Ok(CredentialMaterial::ScopedBearer { secret, expires_at })
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), BuildError> {
    let mut signal = musteragent_cloud_lifecycle::ShutdownSignal::install()?;
    let settings = CloudWorkerSettings::load(Path::new("worker.toml"))?;
    let overrides = WorkerOverrides {
        credentials: Some(Arc::new(HostCredentials(GrantClient::connect().await?))),
        ..WorkerOverrides::default()
    };
    run_worker(settings, overrides, signal.recv()).await
}
```

The resolver receives the grant reference frozen at admission, the full route,
the route role and the original model invocation ID. A scoped bearer near
expiry at dispatch is renewed for the prepared request, and the worker signs
its host headers again with the same authority. An open response
stream uses its existing HTTP connection. A retry of that invocation or a
replacement worker recovers its authority from the frozen request. A newly
admitted model invocation has its own ID.

Set `WorkerOverrides::model_headers` to an implementation of
`ModelRequestHeaderHook` to sign invocation metadata. Each override replaces
its own default. The environment resolver requires `models[].credential_env`;
a host resolver may omit it. The default header hook reads `[models.headers]`.

With either override, the worker uses the same credential resolver and header
hook for every same-origin result GET, including polls after a pending
response. Each GET carries the original idempotency key, fresh authentication
and fresh host metadata. HTTP redirects and transparent retries are disabled
for these model requests. A failed lookup authorization ends recovery through
the bounded lookup error path. With `WorkerOverrides::default()`, the CLI
continues to use the named environment credential and configured headers, and
lookup forwards the dispatched headers.

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

A host with no workspace connectors can mount
`{"schema_version":"musteragent.workspace-tool-releases.v1","releases":[]}`
at `workspace_releases_file`. It publishes zero releases. A configured grant
must still resolve its exact release, and existing runs replay their sealed
tool selections.

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

### Dynamically admitted host capabilities

When backend capability keys are only known at run admission (for example
published at runtime without restarting workers), a host can admit those
`BackendNamed` tools on a release-bound snapshot and supply a
`CapabilityDispatcher` per host `CapabilityProviderId`.

Snapshot validation under `product_release: Some(_)` admits a backend-operation
tool absent from the release's compiled `BuiltinToolCatalog` if and only if:

- `capability_provider_id` is neither `BUILTIN_RUST_PROVIDER_ID` nor
  `BUILTIN_BACKEND_PROVIDER_ID`.
- `ToolManifest.key` collides with no compiled product release catalog (`Cloud`
  or `Device`). Any tool whose key is present in a compiled catalog must still
  match its compiled execution contract and schemas byte-for-byte.

Contract rules for dynamically admitted backend capabilities:

- **Deterministic provider ID**: `CapabilityProviderId` must be a deterministic,
  non-nil UUID (for example `Uuid::new_v5` over a host namespace and stable
  adapter identifier), never nil and never a builtin provider ID.
- **`BackendOperation` naming**: `BackendOperation::new("<Service>.<Method>", None)`
  requires two ASCII identifiers matching `[A-Za-z][A-Za-z0-9_]*` separated by
  one `.` within 128 ASCII bytes. Generic dispatcher method names (`ExecuteTool`,
  `execute_tool`, and case/underscore variants) are forbidden.
- **Approval floor for non-read effects**: Tools whose `Effect` is not
  `Effect::Read` (`WriteReversible`, `WriteExternal`, `Destructive`) must
  declare `ApprovalRequirement::ByPolicy` or `ApprovalRequirement::Always`;
  `ApprovalRequirement::Never` fails contract validation.
- **Catalog discovery and activation**: `tool.catalog.search` and
  `tool.catalog.activate` operate over the snapshot's `authorized_tools`, so
  dynamically admitted `CapabilityLocation::Backend` tools omitted from
  `initial_exposed_tool_keys` can be searched and activated on demand.
- **Derived runs**: Cloud delegation sub-agents and scheduled runs retain
  non-device backend capabilities through re-derived `grant_derived_<run_id>`
  grants with `execution_grant_proof: None`. Host dispatchers must authorize
  from the kernel-verified `CapabilityInvocation` (`tenant_id`, `actor`,
  `run_id`, `capability_key`, `capability_provider_id`) rather than requiring a
  root grant proof.

Register host dispatchers on `WorkerOverrides::capability_dispatchers` when
calling `run_worker`, or wrap `kernel_dispatcher` / `product_dispatchers` with
`ProviderCapabilityDispatcher::new` when composing `CloudWorkerComposition`:

```rust
use std::{collections::BTreeMap, path::Path, sync::Arc};
use musteragent_cloud_worker::{
    BuildError, CapabilityDispatcher, CloudWorkerSettings, WorkerOverrides, run_worker,
};
use musteragent_identity::CapabilityProviderId;
use uuid::Uuid;

fn inventory_provider_id() -> CapabilityProviderId {
    CapabilityProviderId::from_uuid(Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        b"example.test/capability-provider/inventory.v1",
    ))
    .expect("UUIDv5 is non-nil")
}

async fn start(dispatcher: Arc<dyn CapabilityDispatcher>) -> Result<(), BuildError> {
    let mut signal = musteragent_cloud_lifecycle::ShutdownSignal::install()?;
    let settings = CloudWorkerSettings::load(Path::new("worker.toml"))?;
    let overrides = WorkerOverrides {
        capability_dispatchers: BTreeMap::from([(inventory_provider_id(), dispatcher)]),
        ..WorkerOverrides::default()
    };
    run_worker(settings, overrides, signal.recv()).await
}
```

`ProviderCapabilityDispatcher::new` rejects builtin provider IDs and duplicate
registrations at construction. Calls matching a registered `CapabilityProviderId`
route to that host dispatcher; other calls fall through to the inner product
dispatcher, and an unknown provider ID for a host-shaped call fails explicitly
with `ErrorCode::CapabilityUnavailable`.

#### Building host catalog snapshot partitions

Instead of hand-assembling capability grants, `ToolDefinitionSnapshot` values,
bundled and tenant skill projections, and `DiscoverySnapshot` hashes at run
admission, use the pure `HostCatalogSnapshotBuilder` in
`musteragent-runtime-snapshot`:

```rust
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_runtime_snapshot::{
    HostCatalogBuildError, HostCatalogSnapshotBuilder, HostCatalogSnapshotPartitions,
    HostSkillDescriptor, HostSkillProvenance, HostToolDescriptor, host_capability_provider_id,
};
use uuid::Uuid;

fn build_host_partitions(
    skills: Vec<HostSkillDescriptor>,
) -> Result<HostCatalogSnapshotPartitions, HostCatalogBuildError> {
    let provider_id = host_capability_provider_id(
        &Uuid::NAMESPACE_OID,
        "example.test/capability-provider/inventory.v1",
    )?;
    HostCatalogSnapshotBuilder::new(provider_id)
        .with_builtin_tools(["skills.resource.read@2"])
        .with_host_tools([
            HostToolDescriptor::new(
                "inventory.status.ping@1",
                "Check live status of the inventory service.",
                br#"{"type":"object","additionalProperties":false}"#.to_vec(),
                br#"{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}"#.to_vec(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.CheckStatus",
            ),
            HostToolDescriptor::new(
                "inventory.quote.lookup@1",
                "Look up live inventory pricing by SKU.",
                br#"{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"],"additionalProperties":false}"#.to_vec(),
                br#"{"type":"object","properties":{"unit_price":{"type":"integer"}},"required":["unit_price"],"additionalProperties":false}"#.to_vec(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.LookupQuote",
            )
            .with_when_to_use("Use when computing wholesale SKU quotes.")
            .with_tags(["pricing", "warehouse"]),
        ])
        .with_host_skills(skills)
        .with_initial_exposed_tools(["inventory.status.ping@1"])
        .build()
}
```

Behavior and validation guarantees:

- **Authorized vs. initially exposed tools**: Every declared host tool receives a
  `CapabilityLocation::Backend` grant and enters `tools`, while
  `discovery.initial_exposed_tool_keys` contains only the 8 mandatory
  `CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1` bootstrap tools followed by the keys
  listed in `with_initial_exposed_tools`. Tools omitted from
  `with_initial_exposed_tools` remain deferred and are discoverable and
  activatable via `tool.catalog.search` and `tool.catalog.activate`. Optional
  `when_to_use` and `tags` on `HostToolDescriptor` (and `tags` on
  `HostSkillDescriptor`) populate the snapshot's native discovery metadata
  fields (`ToolManifestSnapshot.tags` / `when_to_use` and
  `AuthorizedSkillDescriptorSnapshot.tags`) for weighted catalog search without
  mutating `description`.
- **Fail-closed typed validation**: `build()` never silently drops entries. It
  returns a typed `HostCatalogBuildError` on builtin or empty provider IDs,
  malformed or duplicate tool/skill/exposure keys, keys colliding with compiled
  `Cloud` or `Device` catalogs, disallowed kernel namespaces, `ExecuteTool` or
  malformed `<Service>.<Method>` operations, non-`Effect::Read` tools paired with
  `ApprovalRequirement::Never`, non-object JSON schemas, or ungranted skill
  capability requirements.
- **Allowed host skill provenance (`source != "marketplace"`)**: Host skill
  descriptors (`HostSkillDescriptor` / `HostSkillProvenance`, including
  `HostSkillDescriptor::from_catalog_entry`) must use installed sources `"hub"`
  (`HostSkillProvenance::hub`), `"url"` (`HostSkillProvenance::url`), or
  `"github"` (`HostSkillProvenance::github`), or tenant-local custom provenance
  (`HostSkillProvenance::custom`). `source = "marketplace"` is rejected with
  `HostCatalogBuildError::MarketplaceSkillSourceUnsupported` because
  `cloud-worker` leaves `CurrentMarketSkillSource` unwired and requires a root
  `execution_grant_proof` that derived delegation and scheduled runs do not
  carry.
- **Snapshot and grant projection**: Call `partitions.apply_to(&mut content)` to
  populate `product_release`, `capabilities`, `tools`, `skills`, and `discovery`
  before `RuntimeSnapshot::seal`, and `partitions.capability_entitlements()` when
  minting the matching `ExecutionGrant`.

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
