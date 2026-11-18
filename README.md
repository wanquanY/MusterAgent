# musteragent

A durable execution kernel for tool-using AI agents, written in Rust.

musteragent runs agent work as **durable, fenced, replayable executions**: every
run, attempt and tool invocation is recorded in an append-only ledger, leased to
exactly one worker at a time, and projected into read models. A run can start
in the cloud or on a user's device, and the same contracts govern both.

> **Status: experimental.** APIs, storage schemas and crate boundaries may
> change without notice. See [Support](#support).

## What is in the kernel

| Area | Crates | Responsibility |
|---|---|---|
| Foundation | `musteragent-identity`, `-content-ref`, `-event-envelope`, `-policy`, `-runtime-contract`, `-telemetry` | Typed IDs, content addressing, ledger envelopes, policy and error contracts, telemetry |
| Domains | `musteragent-activity`, `-definition`, `-conversation`, `-approval`, `-artifact`, `-capability`, `-discovery`, `-mcp`, `-memory`, `-model`, `-resource`, `-skill`, `-tool`, `-execution-context`, `-placement`, `-device-interaction` | Pure domain contracts with no I/O |
| Runtime | `musteragent-run`, `-ledger`, `-scheduler`, `-projection`, `-loop`, `-context`, `-tool-executor`, `-runtime-snapshot`, `-runtime-model`, `-runtime-capability`, … | Run/Attempt/Invocation lifecycle, ledger and outbox, leases and fencing, scheduling, projections, the durable agent loop, context planning and compaction, tool execution |
| Infrastructure | `musteragent-postgres` | PostgreSQL store for the ledger, scheduler, projections and runtime state |
| Integrations | `musteragent-model-providers` | Model protocol adapters behind the model port |
| Test support | `musteragent-testkit` | Disposable PostgreSQL containers, deterministic providers |

## Architecture in one page

```
            ┌──────────── host product (not in this repo) ────────────┐
            │ UI, personas, prompts, skills, model routing, billing     │
            └───────────────▲─────────────────────────▲─────────────────┘
                            │ ports (traits)          │ commands / queries
┌───────────────────────────┴─────────────────────────┴──────────────────┐
│ runtime: admission → snapshot → scheduler → agent loop → tool executor │
│          ledger (append-only, fenced)  ──outbox──▶  projection         │
├────────────────────────────────────────────────────────────────────────┤
│ domains: run, conversation, tool, capability, skill, memory, artifact… │
├────────────────────────────────────────────────────────────────────────┤
│ foundation: identity, content refs, envelopes, policy, contracts       │
└────────────────────────────────────────────────────────────────────────┘
                 storage: PostgreSQL (cloud)   [SQLite (device): planned]
```

* **Run / Attempt / Invocation.** A run is the durable unit of agent work. Each
  execution try is an attempt holding a lease; each model or tool call is an
  invocation with an idempotency key.
* **Ledger and outbox.** All state changes are appended to a per-run ledger
  under a fencing token. Projections and external effects consume the outbox,
  so a crashed worker never leaves half-applied state.
* **Leases and fencing.** Workers claim attempts with time-bounded leases. A
  stale worker's writes are rejected by its outdated fencing token.
* **Runtime snapshot.** Admission freezes everything a run may use — agent
  version, tool catalog, capability grants, model route, skills — into an
  immutable snapshot. Execution never consults mutable configuration.
* **Agent loop.** A durable loop of model rounds and tool rounds with
  checkpointing, compaction and bounded retries.

## Multi-device execution model

A work item starts in exactly one place, chosen by
`musteragent_placement::WorkItemStartPlacement`:

| Placement | Meaning |
|---|---|
| `Cloud` | A cloud worker claims the attempt from the PostgreSQL scheduler |
| `CurrentDevice` | The device that submitted the work runs it locally |
| `SelectedDevice(DeviceSelector)` | A specific paired device (membership + epoch) runs it |

Placement selects a *class* of target; it never carries host proof. Hosts
authenticate the chosen target separately before accepting an assignment. The
tool catalog has a cloud and a device half (`contracts/kernel-tools.json`);
device tools are only exposed to runs placed on a device. The cloud worker executes cloud placements. See the [device execution roadmap](docs/next-milestones.md) for the remaining device work.

## Quick start

Install Rust through rustup. The repository pins Rust 1.92.0, rustfmt and
Clippy. Fetch the locked dependencies once before working offline.

```sh
cargo fetch --locked
cargo build --offline --workspace --bins
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
```

The full suite requires Python 3 and a running Docker daemon. The test harness
starts disposable PostgreSQL, pgvector and MinIO containers. Prepare the local
sandbox images before running the suite. Use one Rust test thread on a shared
machine to bound the number of containers running at once.

```sh
docker pull python:3.12-slim
python3 crates/infrastructure/sandbox-server/docker/build-test-image.py
export MUSTERAGENT_DOCUMENT_PARSER_EXECUTABLE="$PWD/target/debug/musteragent-document-parser"
RUST_TEST_THREADS=1 cargo test --offline --workspace --no-fail-fast
```

Set `MUSTERAGENT_TEST_MCP_SANDBOX=1` for the public-network MCP tests.
`MUSTERAGENT_PUBLIC_DELIVERY_OUTPUT_AUDIT` selects an explicitly supplied
read-only audit fixture for the ignored delivery replay. See
[CONTRIBUTING.md](CONTRIBUTING.md#integration-test-configuration) for run
labels, timing budgets and individual canary and performance tests.

## Integrating a host

The host authenticates the caller, publishes an agent definition and provides
model routing, credentials and storage. It submits work through the admission
ports and consumes projections or outbox events. The reference API demonstrates
submission, cancellation and resumable events. The worker executes the admitted
snapshot and records raw usage. Billing, product UI, account management and
product-specific personas, prompts and skills belong to the host.

Start with the [reference API](applications/reference-api/README.md), then
use the [host integration guides](docs/integration/) for worker configuration,
wire contracts, scheduled execution and delegation.

## Support

There is **no support commitment**. Issues and pull requests may not be
answered, releases are not scheduled, and compatibility between versions is not
guaranteed. Use it if it helps you; fork it if you need different guarantees.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE). Third-party
licenses are listed in [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
Contributions are accepted under the same license; see [CONTRIBUTING.md](CONTRIBUTING.md).
