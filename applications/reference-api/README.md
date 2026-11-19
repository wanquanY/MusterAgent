# Reference cloud API

A small, loopback-only development host for the kernel. It publishes explicitly
configured Agent definitions, authenticates static development bearer tokens,
seals cloud admissions and adapts the existing PostgreSQL owners to HTTP. It
contains no charging logic. Production hosts replace `HostIdentity` and their
configuration/publication policy; never expose the static-token executable as a
production authentication service.

## Run

The PostgreSQL instance must have pgvector. Use a dedicated development database
and a private S3-compatible bucket. The worker and API must share the content
keyring, bucket/prefix, Agent publications and model provider/entry IDs.

```sh
cargo build --offline -p musteragent-reference-api -p musteragent-cloud-worker \
  -p musteragent-database-migrator --bins
MUSTERAGENT_DATABASE_URL=... target/debug/musteragent-database-migrator apply
# Populate the named environment variables in api.example.toml, then:
target/debug/musteragent-reference-api /absolute/path/api.toml
target/debug/musteragent-cloud-worker /absolute/path/worker.toml
```

See [api.example.toml](api.example.toml) and the existing
[worker integration](../../docs/integration/cloud-worker.md). The API verifies
migrations and does not apply them. Initial host Agent publications use the
existing immutable registry ports. Restarting with identical material is
idempotent; conflicting IDs/material or published pointers fail startup. This
example publishes initial immutable versions only; changed material needs a new
Agent key/ID/version ID. Use the registry lifecycle ports for version management.
Base skill materials are reconciled through the existing release publisher
before the HTTP listener is served. This
minimal host requires a separate configured Agent identity for each tenant.
The model route example uses OpenAI Chat Completions. Other protocols remain
available through the worker's model-provider port but are not configured by
this reference admission builder.

For per-user, per-Agent workspace connector grants, see [host-published workspace tools](../../docs/integration/workspace-releases.md).

Set `publish_raw_usage = true` in the worker's top-level configuration. Every
completed model usage fact is then committed with a `musteragent.model-usage.v1`
outbox publication, encoded by `ProtobufMeteringPublicationEncoder`. The host
consumes and deduplicates `message_id`/the metering dedup key. The reference host
supplies the explicit `catalog:host.unpriced@1` metadata label; it supplies no
price or monetary amount. Omitting the flag preserves the worker's existing
local-facts-only behavior. External outbox delivery is at least once.

## HTTP contract

Every endpoint requires `Authorization: Bearer <token>`. Tenant, principal,
permission ceiling, provider IDs and execution authority come from the host,
never request JSON. List endpoints return at most 100 entries and a `next` UUID;
pass it as `?after=...`. UUID keyset order is stable.

| Method/path | Request / response |
| --- | --- |
| `POST /v1/runs` | `{command_id, conversation_id?, message, agent, model_route, placement:"cloud"}` → `{conversation_id, run_id, duplicate}` |
| `GET /v1/conversations` | Authorized conversation IDs/titles |
| `GET /v1/conversations/{id}/runs` | Run IDs, parent IDs, input message IDs, current statuses |
| `GET /v1/runs/{id}` | Current status, input message ID, pending interaction IDs, conversation cursor |
| `POST /v1/runs/{id}/stop` | `{command_id}` → durable cancellation request; all live descendants cascade |
| `GET /v1/conversations/{id}/projection` | Current kernel-reduced messages (with `source_run_id`), runs (with `input_message_id`), tools, pending interactions and cursor |
| `GET /v1/conversations/{id}/events` | Resumable SSE projection input, described below |
| `POST /v1/conversations/{id}/interactions/{interaction}/response` | `{command_id, answer:<schema-conforming JSON>}` or `{command_id, approval:true/false}` |

A submit without `conversation_id` creates a deterministic conversation for
that command; an existing conversation retains its configured Agent identity.
Creation and Run admission are individually atomic kernel commands. A failed
admission may leave an empty conversation; retry the **same command ID and
body**. A committed retry returns the original Run, even after completion or
API restart. Reusing the command with different message/route/actor bytes is a
conflict. Distinct concurrent admissions respect the kernel foreground-Run and
optimistic conversation fences. Only cloud placement is supported; unsupported
placements produce an explicit 422 response.

Message content, immutable Snapshot, Run, sandbox routing context and dispatch
outbox commit in one admission transaction. The worker can never observe a
partially admitted Run. The routing context is API/Interactive/PreferBrowser;
the shared router selects the server and records its reason. No executor or
model fallback is inferred by this host.

Interaction responses use `InteractionResponseApplication`, including schema
validation, pending-state CAS, durable replay and worker wakeup. Host approval
is one-shot and restricted by the response owner to cloud control/execution
homes. It cannot mint device-presence, session approval or secure credential
authority. All foreground admissions use the existing `Supervised` permission
mode and the actual `api` grant surface. An Agent's optional `approval_tools`
list pins additional capability approval floors. It must be a subset of `tools`;
tools sharing that capability share the approval floor. Native tool policy still
applies when the list is empty. Skill selection and sandbox routing also use the
API surface; no device authority is granted. For `single_select` questions, the answer follows the frozen tool
schema (for example `{"value":"pass"}`).

Schedules and child runs remain available through the frozen kernel tools.
Include the desired `schedules.*`/`delegation.*` logical keys in the host Agent's
`tools` allow-list. The snapshot pins the cloud orchestration discovery policy.
No separate schedule or delegation HTTP state machine is introduced. Hosts that
need direct schedule management can use `PostgresScheduleService` as documented
in [scheduled execution](../../docs/integration/scheduled-execution.md).

## Projection SSE and reconnect

Each SSE `event: projection` is one committed, canonical kernel projection input
record, including the complete ledger metadata required by the reducer. Its `id` is `conversation_uuid:home_epoch:conversation_sequence`. The
JSON `data` schema is `musteragent.reference.projection-event.v1`:

```json
{
  "schema": "musteragent.reference.projection-event.v1",
  "cursor": "<conversation>:1:17",
  "record_id": "<uuid>",
  "record_schema": "musteragent.runtime.record.v1",
  "tenant_id": "<uuid>",
  "producer": "RuntimeApi",
  "committed_at_ms": 1790000000001,
  "causation_id": null,
  "correlation_id": "<uuid>",
  "idempotency_key": "<kernel command key>",
  "payload_sha256": "<sha256 hex>",
  "kind": "DomainEvent",
  "conversation_id": "<uuid>",
  "sequence": 17,
  "home_epoch": 1,
  "activity_id": "<uuid or null>",
  "run_id": "<uuid or null>",
  "run_sequence": 8,
  "attempt_id": "<uuid or null>",
  "participant_id": "<uuid or null>",
  "occurred_at_ms": 1790000000000,
  "payload_base64": "<canonical protobuf bytes>"
}
```

For `DomainEvent`, decode `musteragent.v1.DomainEvent`; for `OutputFrame`, decode
the canonical `musteragent.v1.OutputFrame`. The generated contracts are owned by
`musteragent-wire`/`musteragent-persistence-codec`; see
[wire contracts](../../docs/integration/wire-contracts.md). Clients can apply
these records through the kernel `ProductProjectionReducer`, or read the
materialized `/projection` endpoint. The stream is an exact projection change
feed, rather than repeated snapshots of the latest mutable state; it includes
checkpoint-only facts so every sequence is represented.

Persist the cursor **after** applying an event. Reconnect with `Last-Event-ID`
(or `?cursor=`). The next event is strictly after it. Polling reads durable,
authorized PostgreSQL pages; an API restart loses no cursor authority. There is
no process-local broadcast buffer. Reading a page is bounded to 64 records and
4 MiB; yielding the HTTP stream supplies backpressure and disconnect drops the
producer. Ten-second SSE comments are transport keepalives, without IDs.

A snapshot's cursor can start the subsequent tail without a race. Cursors for a
different conversation, malformed cursors or conflicting header/query values
are rejected. Future cursors and changed epochs are 409; retained-history
compaction is 410. These cases require fetching a new authorized projection,
not silently skipping missing facts. Stream dependency failures emit an `error`
event **without an ID** and close; resume from the last applied projection ID.
No claim of infinite retention is made. Replayed event data and ordering are
byte-equivalent while the retained ledger is unchanged. HTTP delivery across a
client crash is not a substitute for atomically persisting the client's applied
cursor.

## Acceptance

The real-process test starts its own PostgreSQL + MinIO containers, the migrator,
API, worker and a Python OpenAI SSE fixture. It requires the P6
`musteragent-sandbox-mcp:p6` image to be available locally; it never pulls or
substitutes an executor. Build all three binaries before a package-only test:

```sh
cargo build --offline -p musteragent-reference-api -p musteragent-cloud-worker \
  -p musteragent-database-migrator --bins
MUSTERAGENT_TEST_OWNER=codex-p11 MUSTERAGENT_TEST_RUN_ID=p11-acceptance \
  cargo test --offline -p musteragent-reference-api --test resilience -- --nocapture
```

All Run creation/control and client reads go through HTTP. SQL only independently
reads receipts. The fixture reads actual tool output and child reports before
answering. It uses the worker's default lease/heartbeat settings, kills/replaces a worker
at a known live child boundary and also
kills/restarts the API between client subscriptions. The test compares complete
reassembled event arrays, asserts one child and one of each tool/job, observes
both cancelled descendants, approval allow/deny, question response replay and
original raw-usage outbox facts, and checks
cross-tenant denial. Evidence is written beneath `.codex-work/p11/acceptance-*`.
