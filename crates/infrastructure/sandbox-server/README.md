# Server sandbox and stdio MCP

The host supplies a Python 3.12 image with uv, pip's vendored packaging parser,
and its dependencies. Add Node when hosting Node MCP servers. Pin the image by
digest in production. `docker/Dockerfile.example` is a public starting point;
`docker/Dockerfile.mcp-test` adds Node and a dependency-free acceptance example.
There is no Docker pull, host execution, or browser execution fallback in the
runtime: missing Docker, images, environment material and proxy policy are
explicit errors.

## Network authority

`BrowserSecurityProfile::default()` is `SafeOffline`. `ProxyEgress` explicitly
requests the host egress policy. **Both profiles use Docker `network=none`**,
read-only rootfs, a non-root numeric user, all capabilities dropped,
no-new-privileges, PID/CPU/memory limits and bounded tmpfs. There is no bridge,
host network, published port, host socket or Docker socket inside a job.

For `ProxyEgress`, an attached Python exec serves a loopback HTTP proxy inside
the container. A bounded multiplexed stdin/stdout transport connects it to
`egress::EgressProxy` on the host. This avoids trusting bridge gateways, host
routes or container firewall privileges. The only external communication
available to the workload is the HTTP/CONNECT proxy protocol. Offline jobs do
not start this listener. Each job gets an independent proxy audit.

Configure `ServerSandboxConfig.egress.proxy`, construct
`EgressProxy::from_config`, and pass that policy to `DockerRuntime::with_proxy`.
The policy configuration's `listen` is used only when a host explicitly calls
`egress::bind` for a standalone proxy. The sandbox adapter uses attached streams
and does not expose that listener. Hosts may inject their DNS authority through
`Resolver` / `EgressProxy::with_resolver`.

The default decision is resolve once, check **every** returned address, then
connect to the selected checked IP. The proxy refuses loopback, private, shared
address space, link-local, metadata, IPv6 ULA and unsafe embedded IPv4 addresses,
plus host deny CIDRs/domains and disallowed ports. Required denials cannot be
lifted by allow-list entries. DNS failure is `resolve_failed`, never a reason to
forward an unchecked name. CONNECT does not perform a second DNS lookup.

`UpstreamProxy { address, upstream_resolves: false }` optionally chains an HTTP
corporate/cloud proxy. Its address is host configuration, not read implicitly
from process environment. Checked targets are sent to it as **IP CONNECT**.
Only explicit `upstream_resolves: true` sends a hostname to that upstream;
private-address enforcement for that hostname is then delegated to the upstream.
This mode requires an upstream by construction and is never selected after a
DNS failure. Literal IPs still undergo all local checks in either mode.

The structured audit records destination host/port, `ResolutionMode` and typed
`DenyReason`; no URL paths, bodies or proxy credentials are logged. Job receipts
include `egress_denied_*`, `egress_checked_address` or `egress_upstream_resolves`
diagnostics. MCP receipts carry the same structured events. At 127 decisions a
final `audit_limit` fact closes further egress. Relay limits are 16 simultaneous
connections, 32 KiB frames, bounded queues and 64 MiB in each direction per
session. Policy/relay failures terminate explicitly; they never open another
network path.

## Providers and job contracts

`ServerSandboxProvider::new` creates a tenant registration.
`new_registered` accepts a stable provider ID with `BrowserProviderScope::Tenant`
or `Global`. The host activates this registration for each authorized
`ProviderBinding` before dispatch. Execution leases remain bound to the exact
tenant/principal and carry the existing P4 fencing proof; global registration
does not grant global content access. There is no cross-tenant job or container
reuse. Registration scope and P4 `SandboxExecutor` are immutable, even after
release, expiry or a gateway restart. PostgreSQL baseline migration 0001 persists this
identity separately from ephemeral leases. Browser registrations remain actor
scoped; proxy jobs are refused on browser leases.

The provider serves Python @1/@2/@3, workspace @4 and environment preparation
using the existing `BrowserJobRequest`, `ContentRef`, `BrowserJobResult` and
canonical result digest/Protobuf codec. Capability owns and re-exports the path,
log-tail and diagnostic limits. Partial outputs are now valid for every terminal
status; request output authorization, byte limits and digest validation still
apply. Existing successful result bytes and hash fixtures are unchanged.

Inputs and generated runner/helpers are read-only mounts. Work starts as a copy
inside bounded tmpfs. `musteragent_io` provides `input_path`, `list_inputs`,
`output_path`, `INPUT_DIR`, `OUTPUT_DIR`, `WORK_DIR` and `environment`.
Declared output helpers reject undeclared paths; collection independently rejects
symlinks and special files. Workspace jobs use the shared resource manifest and
complete delta with unchanged read-only entries, changed ContentRefs and explicit
rename/remove/replace operations. A pre-start collector records inode plus Linux
`statx` birth time, preserving identities across subprocess renames without
mistaking inode reuse for a rename. The filesystem must supply birth times;
unavailable provenance fails explicitly. Deltas pass the resource owner's
`validate_against` before staging.

Environment preparation verifies requirements, Python version, dependency
markers/extras, installed versions and installed bytes in the supplied image.
It produces the shared `musteragent.python-environment/1` receipt on stdout and,
when declared, `environment-lock.json`. Native packages come from the immutable
host image; missing/version-incompatible packages are
`server_sandbox_environment_unavailable`. This does not install or silently
substitute packages. Native and browser environments have different runtime
hashes; their binding/receipt and job wire contracts are shared, not their ABI.
Prepared @3 jobs use a bounded tenant-scoped receipt cache (128 entries). @4
carries `environment_manifest_ref` and verifies it against the current image,
so it survives executor restart without cache authority. A stale/missing @3
binding fails explicitly and must be prepared again.

Work tmpfs is bounded by input bytes plus output budget plus bounded filesystem
block/metadata allowance, capped by host memory. `/tmp` has its own host cap.
Collection enforces exact logical output bytes/file counts and individual
output declarations; overflow is a typed resource failure. A collector is
reserved before the workload starts, so PID exhaustion cannot prevent recovery.
It stops all workload processes before streaming the tmpfs (Docker's archive API
omits tmpfs). Collection has a ten-second deadline, after which force-removal
kills the container. The parser never unpacks an untrusted tar onto the host:
it validates paths/types and copies regular files into this job's seed only.
Timeout, cancellation, nonzero exit and quota failures retain valid partial
files and bounded stdout/stderr tails. Capture/runtime failure is indeterminate,
with retained evidence, never reported as successful completion.

## MCP session lifecycle

`SandboxProcessLauncher` implements the existing `mcp-client::ProcessLauncher`;
`mcp-client` and the extension crate need no adapter-specific changes. Construct
it for exactly one tenant with an explicitly configured `DockerRuntime` proxy,
then use `StdioTransport::launch` and `McpSession` normally. Process paths and
working directories are paths **inside the image**. No host working directory
is mounted for MCP. The launcher always requests proxy egress.

One MCP session owns one fresh container. Reuse is limited to calls in that
session; no reuse across sessions or tenants. Defaults: 60 seconds idle,
300 seconds total (also capped by host limits), 8 MiB stdin, 8 MiB stdout,
1 MiB stderr. Stdin/stdout activity resets idle time; stderr does not. Streams,
relay queues and backpressure are bounded. Host concurrency limits apply.
`kill()` cancels I/O, stops the relay, force-removes the container, closes stdout
and stderr, then returns; repeated kill is idempotent. Dropping control requests
cleanup. Hosts must await shutdown before stopping their Tokio runtime and
recover crash leftovers by their recorded resource labels.

Drain `take_results()` and persist the terminal records with the host's MCP run.
Receipts include tenant/session IDs, termination cause, idle-timeout flag,
bounded stream tails, typed infrastructure errors and egress decisions. The
launcher reserves space for 128 active/uncollected receipts and refuses new
launches until hosts drain them; receipts are not silently dropped.

## Mandatory real Docker acceptance

Run from the repository root. A working HTTP upstream proxy in `HTTPS_PROXY` or
`HTTP_PROXY` is required for the public HTTPS test; absence fails with an explicit
message. Tests never skip missing Docker/images/egress. Proxy URLs are read only
from the environment; URL credentials are unsupported by the acceptance builder.
The build script translates a loopback upstream host to `host.docker.internal`
and passes Docker's predefined HTTP_PROXY/HTTPS_PROXY build arguments. No
machine-specific upstream address is stored in source or image ENV.

```sh
python3 crates/infrastructure/sandbox-server/docker/build-test-image.py
MUSTERAGENT_TEST_OWNER=codex-p6 MUSTERAGENT_TEST_RUN_ID=p6-t9 \
  RUST_TEST_THREADS=1 cargo test --offline -p musteragent-sandbox-server
```

Tests cover real MCP initialize/tools/list/tools/call, pure and public HTTPS
operations, all required literal/private-resolver denials with a zero-connection
host listener, DNS rebinding, offline/host reachability, direct-IP blocking,
container removal/EOF, all job types, partial results, quotas and lifecycle
limits. Containers have owner, run-id, purpose, disposable and expires-at labels
and removal guards. Built images are similarly labelled; the builder records
IDs, labels and logs under `.codex-work/p6`. Retain them for review or remove
those exact IDs after checking no active test references them. Do not prune
unrelated Docker resources.

## Public-network MCP acceptance

The `mcp_sandbox` integration tests print a skip reason unless
`MUSTERAGENT_TEST_MCP_SANDBOX=1` is set. Opting in requires Docker, the local
`musteragent-sandbox-mcp:p6` image (build from `docker/Dockerfile.mcp-test`),
`HTTPS_PROXY` or `HTTP_PROXY`, and working public HTTPS egress. Missing
prerequisites after opt-in fail the test. Run:

```sh
MUSTERAGENT_TEST_MCP_SANDBOX=1 cargo test --offline -p musteragent-sandbox-server --test mcp_sandbox -- --nocapture
```
