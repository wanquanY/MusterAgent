# Kernel extension composition

`musteragent-extension` composes host features at the existing durable loop,
context planner, model provider and tool executor boundaries. It owns no network
transport, model routing, billing prices, sandbox executor or database connection.
It cannot grant authority beyond the pinned kernel policy.

## Integration

1. Register extension instances and typed JSON configuration with
   `ExtensionRegistry`. Register tools, commands, services and gateway methods
   with their registered owner. Freeze the registry with `ExtensionHost::new`.
2. Supply an `ExtensionJournal`, `RuntimePorts`, and `LoopDetectionConfig`.
   `ExtensionContext` carries admitted run/attempt identity, an absolute workspace,
   session key, channel and sandbox status. These come from the host, never model
   arguments. Bind one composition to an attempt.
3. Put `RegisteredToolDispatcher` around the host capability dispatcher and
   `ExtensionInvocationPolicy` around its invocation policy evaluator when
   constructing the **original `DurableToolExecutor`**. Publish the registered
   tool definitions through the existing snapshot/material owner. Registration
   alone cannot expose an unpinned tool or skip schema validation, grants,
   approval, execution supervision, result storage or cancellation.
4. Pass the resulting `DefaultLoopPolicy`, original model provider, fenced loop
   journal, clock and execution supervisor to `ExtensionHost::compose_loop`.
   Execute the returned `ExtendedAgentLoop`; it guarantees `agent_end` after
   ordinary completion, failure, wait and cancellation exits. As with the base
   loop, the host must keep execution polled through cooperative cancellation.
   Dropping a future or terminating a process cannot synchronously run hooks.
5. The host owns session/service lifetime: call `start_services`, then the loop,
   and in its shutdown path `session_end` or `session_shutdown` via `lifecycle`
   and `stop_services`. Services must join their owned work before stop succeeds.
   A successful start is remembered once per tenant/session; a failed stop keeps
   ownership so it can be retried. Start failure rolls back prior starts.
6. Feed committed outbox items to `consume_outbox` from the host's existing durable
   subscription. The host owns cursor/acknowledgement. Delivery is at least once;
   observers must be idempotent by message ID. Extension-fact outbox entries are
   excluded to prevent a recursive observer-error stream.

No new dependencies outside the existing workspace dependency set are required.

## Composition and failures

All hooks and registrations stack in registration order. The immutable registry
freezes each extension's identity once. Duplicate names return `Duplicate` without
replacing the original. Memory and context engine each occupy **one exclusive
slot**; memory middleware can stack around that one owner.

Prompt construction runs on each new model-context plan, before request encoding,
content persistence and snapshot sealing. Context projection transforms a copied
view containing only messages and system instructions. Result projections run
first, then context hooks, then prompt hooks. Prepends from A and B produce
`A, B, base`; appends and named sections follow in A/B order. Projection cannot
change request authority, credentials, routing or the tool surface. The original
context owner validates the final request and budget. Projection disables opaque
provider continuation before source selection, so the full projected history is
sent. A context engine without the pre-seal projection port fails explicitly.
Oversized contributions fail the existing context quota check; they are not
silently dropped. Hosts should keep projections deterministic for a plan identity.

Before-tool hooks receive the previous hook's arguments. Every rewrite is
persisted separately **before dispatch**; recovery reuses recorded rewrites.
Every denying extension is recorded; a later allow or rewrite cannot revoke a
deny. The underlying invocation policy also uses strictest-decision composition.
Observers borrow immutable input/output. `after_tool_call` observes the original
committed outcome, while `tool_result_persist` changes only the later context view.
It cannot change the tool role, call identity or name. Ledger records, durable
invocation outcomes and content-store bytes remain unchanged.

| Hook/boundary | Failure policy |
| --- | --- |
| Registration | Typed error; registry remains unchanged |
| `agent_start`, command, prompt, context/result projection, before-tool, provider annotations | Record typed failure and abort before the dependent dispatch/model call |
| Invocation policy | Original tool executor records typed evaluator failure as a denied invocation; no physical dispatch |
| `after_tool_call`, `llm_input`, `llm_output`, `agent_end`, session/child lifecycle, outbox | Record failure, continue subsequent observers; return failures through lifecycle/outbox results and `ExtensionRunReport` |
| Service start | Record failure, roll back already started services, return error |
| Service stop | Record failure, continue later stops, retain failed ownership for retry |
| Registered tool | Record constructor/poll panic as a typed hook failure; original executor owns the failed outcome and physical stop control |
| Gateway, subagent port, memory provider/middleware | Record and return typed error; never synthesize success. Memory after-hook failure returns an error even though the owner already ran |
| Audit failure | Fail closed. Never dispatch an unrecorded rewrite/denial. A post-commit audit failure stops the attempt while preserving the committed result |

Synchronous panics and asynchronous construction/poll panics are caught and
converted to `HookFailureKind::Panic`. Panic payloads are not persisted. This
requires Rust unwinding; workspace release/mobile profiles inherit `panic =
"unwind"`. Process aborts and misbehaving physical stop controls remain outside
in-process recovery. Hook authors must preserve their own invariants on unwind.

## Models, commands and runtime API

`ExtendedModelProvider` retains the original physical execution binding and stop
owner. `llm_input` receives the final context view; `llm_output` sees each event,
including physical usage and error/terminal events. Usage settlement belongs to
the host. `before_provider_request` adds validated transport annotations through
`ModelExecution::annotate`. The native provider adapter applies them before HTTP
preflight. Credentials, host/framing headers and existing native headers cannot
be overridden. Header names, never values, are audited. An unsupported owned or
legacy provider fails explicitly when annotations are requested.

`ExtensionContext.ingress` identifies the **new host-admitted input** with a stable
ID. The loop parses a leading slash command before context planning and before any
model call, records its receipt, and substitutes that receipt for the matching
current input in the model view. Unknown current commands fail. Historical slash
text never executes. Recovery/background runs without new input use `None`.
Command handlers must be idempotent by `CommandRequest.input_id`; recording the
receipt cannot atomically commit an arbitrary external side effect. Reusing a
receipt identity for different input is an explicit error.

`GatewayMethod` has typed request/response associated types and a JSON host
boundary. There is no HTTP/WebSocket listener. The host must authenticate and
authorize callers before providing the verified context.

`RuntimeApi` provides typed config, a per-tenant/session/extension state directory,
ordered memory middleware and a `SubagentSpawner` port. State paths hash session
keys and reject symlink/non-directory components; the state root is host-owned,
not a substitute for an OS sandbox against hostile concurrent filesystem writes.
Missing memory or delegation ports return explicit errors. Memory rewrites are
recorded before the memory owner runs. Subagent admission emits `subagent_spawned`;
the host delivers `subagent_ended` from its child completion stream. Real cloud
delegation orchestration remains M2-13; P5 exercises the port with a test double.

## Append-only facts and recovery

`ExtensionJournal` requires append-only, ordered, conflict-detecting writes under
the supplied authority/fence. Production hosts implement it in their existing
fenced transaction owner. `LedgerExtensionJournal` is a reference adapter for a
host-owned conversation append lane: atomic ledger + outbox, epoch/offset checks,
bounded optimistic retries, idempotency and scoped reads. **`LedgerStore` alone
is not a worker lease fence**; this adapter is not a multi-worker production
adapter. It uses conversation-level extension facts with run/attempt identities
inside the payload, leaving the run owner's sequence untouched.

Facts are schema-versioned and append-only. A tool call that no extension changes
produces **zero extension facts and zero extension outbox messages**. There are no
admission/result shadow facts. Lifecycle/service notifications, projections,
annotations, failures, denials, rewrites and warning-or-higher loop decisions
remain observable.

Argument and memory rewrite facts contain canonical JSON SHA-256 digests and byte
lengths, plus one replacement content ID. `RuntimePorts.content` is the existing
tenant content owner: `put_reclaimable_at` stores the replacement before publishing its fact,
and replay reads and verifies that immutable reference. Original payloads are
never copied into audit facts. PostgreSQL retains each published replacement through
a tenant-scoped foreign key on the same fact row; the existing content GC can
collect interrupted unpublished prewrites after its grace period. Deterministic, tenant/run/attempt/key/digest-scoped
content identities reuse a prewrite after interruption. Content loss, erasure,
integrity failure or identity conflicts fail explicitly, without rerunning a
recorded rewrite. Once the original tool executor has recorded a request,
recovery reuses that admission and the recorded rewrites without rerunning hooks.
An interrupted preparation with no original request resumes the unrecorded hooks;
there is intentionally no extension receipt for a non-intervention.

Per-call and semantic-key reads use `FactQuery`; whole-attempt reads are reserved
for the final observer-failure report. The reference ledger adapter rebuilds its
in-memory key/call/decision indexes once, then consumes only the new ledger suffix.
PostgreSQL uses exact-key, sparse call and sparse loop-state indexes in migration
0004. Detector rewrites are fetched in one bounded call-ID batch. Extension
observers and services can be redelivered after loss and must be idempotent.

## Tool loop detection

`ExtensionLoopPolicy` detects:

- `GenericRepeat`: same tool plus canonical structural JSON arguments within the
  recent history window; object key order is irrelevant.
- `KnownPollNoProgress`: explicitly configured polling tools, same tool/arguments,
  identical original result streak. Changed results or missing earlier results
  break the streak. Out-of-order completions are reevaluated when facts arrive.
- `PingPong`: two distinct tool/argument identities alternating A/B/A/B without a
  third identity; both distinct arguments and distinct tool names can form A/B.

Defaults are history **30**, warning **10**, critical **20**, circuit breaker **30**
for each pattern. Poll tool names must be explicitly classified. Thresholds are
validated against bounded history (maximum 10,000). `None` explicitly disables a
level; with default critical enabled, a fresh attempt stops at 20 and cannot reach
30. The breaker still protects recovered histories; hosts can disable critical
to select a warning + breaker policy. Tests exercise each pattern at all three
levels with small thresholds.

Warnings append typed facts and inject a context note into subsequent requests.
Critical and breaker append pattern/level/count/call identity and terminate the
attempt with `PolicyDenied`, without automatic retry. Generic/alternation stops
occur before physical dispatch. Poll detection occurs after the original result
commit, preserving completed effects. `RuntimePorts.history` reads the original
kernel invocation/result projection in durable order, bounded by the configured
window and isolated by attempt. PostgreSQL reuses `pending_tool_calls` and its
model invocation provenance. Only explicitly configured polling tools need result
bytes from the content owner; argument-only detection performs no result reads.
Only executor-requested or completed calls and the current candidate participate;
future calls in a parallel model batch cannot trigger premature detection.
Recorded rewrites form an effective-argument view over those original records.
Fresh hosts reconstruct all three patterns without extension admission facts.
Preparation and executor entry share a lane for the same tenant/attempt/call ID.
The lane remains held until the original executor returns, closing the gap before
its request record exists without adding a shadow admission receipt. Different
calls overlap, including their asynchronous history and content reads.

## Samples and evidence

`samples` contains plan-mode with `/plan on|off` and scoped persisted state,
user-profile prompt ingress, physical-usage billing observation, and agent-end
memory extraction using the exclusive memory slot. These are small host-neutral
examples; billing storage, identity, memory storage and delegation stay host-owned.

`tests/composition.rs` drives the real `DurableAgentLoop`, `DefaultLoopPolicy`,
`DurableToolExecutor`, execution supervision and `InMemoryLedger`. Provider,
context material/plan storage and external capability ports are deterministic
fixtures. Tests inspect original invocation/content records, sealed model requests,
append-only extension facts, terminal failure receipts and ordered observations.
The native model adapter additionally verifies annotations against a local HTTP
server. No existing database or remote model provider is used by these tests.
