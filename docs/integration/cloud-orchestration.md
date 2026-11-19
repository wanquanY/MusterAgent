# Schedules and child runs

The cloud worker reconciles schedules, child results and extension lifecycle
deliveries alongside the dispatch loop. The kernel database stores schedule occurrences and child-run relationships. No reference HTTP API is required.

## Schedule service and tools

Hosts call `PostgresScheduleService::change(ScheduleCommand)`, `list`, `history`
or `history_page`. A scope identifies the authenticated actor, owning
conversation and an existing source Run. Tool scopes also carry their current
Attempt fence. `ScheduleAction` provides Create, Update, Pause, Resume, Delete,
RunNow, BeginHandover and CompleteHandover. Mutations require a unique command ID
and the expected revision; exact retries retain the original receipt.

The cloud catalog exposes `schedules.create@2`, `schedules.update@2`,
`schedules.control@2`, `schedules.list@2` and `schedules.history@1`. Select
`musteragent.cloud-schedule-discovery@1`, or the orchestration policy below, when
sealing the source snapshot. Historical discovery policies remain unchanged.
The control tool carries the remaining ScheduleAction variants. History pages
have a stable cursor, including for deleted schedules; deletion retains facts.

Definitions contain cron/interval/one-shot timing, validated IANA timezone,
concurrency, lateness and retry policies. Cron skips nonexistent DST wall times
and chooses the first occurrence of an ambiguous wall time. Interval arithmetic
is UTC and checked for overflow. Every admitted occurrence has its own
conversation, session, Run and immutable source/billing link. All scheduled
sandbox routing uses the server.

Each trigger has exactly one Cloud or named Device owner and a fencing epoch.
BeginHandover pauses and revokes the old epoch; CompleteHandover explicitly
activates the new owner. A stale owner cannot fire. A device cannot acquire a
cloud execution implicitly. Dispatch creates the occurrence, original Run and
dispatch outbox in one transaction; a replacement worker resumes the same Run.

Subscribe to `schedule.occurrence.ended` in `event_store.outbox` for delivery to
`owner_conversation_id`. It contains the separate `session_id`, Run ID, terminal
status/error and every assistant result ContentRef. Hosts read content through
their content-store port and deduplicate by outbox message ID. Outbox transport
is at least once; occurrence admission is exactly once. Scheduled Runs and their
descendants receive `ScheduledContextDenied` when managing schedules.

Goal-authorized sources retain their exact frozen Goal authority and current
Agent permission fence. The original Goal/Agent stores check revocation at
derived admission and execution. Schedules do not create pursuits or replace
the standing-goal judge. Product application catalogs are deliberately outside
this integration: creation returns `ApplicationCatalogUnsupported`, exposed to
tools as `schedule.application_catalog_source_unsupported`. Revoked Goal sources
return `SourceRevoked`. Dispatch failures remain explicit durable facts.

## Delegation

Select `musteragent.cloud-orchestration-discovery@1` when sealing the parent
snapshot. Its additive cloud catalog includes:

| Tool | Operation |
| --- | --- |
| `delegation.spawn@5` | Task, optional named Agent, optional tool deny-list and placement |
| `delegation.send@3` | Send to a direct child handle, or `parent` from a child |
| `delegation.receive@1` | Read the caller's durable inbox with a cursor |
| `delegation.list@1` | Page direct children and their statuses |
| `delegation.inspect@2` | Read the authorized transcript projection and exact content pages |
| `delegation.join@4` | Observe selected child statuses; final parent completion waits for reports |
| `delegation.cancel@2` | Request stop of a child and its descendants |
| `delegation.retry@2` | Start a fresh child from a terminal child's original specification |

`musteragent.cloud-orchestration-discovery@2` is an additive frozen policy for
cloud web hosts. Its ceiling is the `@1` ceiling plus conversation delivery
(`dialogue.publish@1`, `artifacts.deliver@2`) and the agent work-item tools
(`tasks.amend@2`, `tasks.control@1`, `tasks.executions.list@2`,
`tasks.feedback@1`, `tasks.followup@1`, `tasks.handoff@1`, `tasks.inspect@1`,
`tasks.list@1`, `tasks.start@3`, `tasks.submit_result@1`, `tasks.verify@3`),
all granted at the cloud location. Its bootstrap is the same as `@1`. `@1`
stays frozen and rejects these keys with `InvalidDiscovery`; Goal and WorkItem
subject ceilings still apply under `@2`. `cloud_orchestration_discovery_policy_for`
returns `@1` for every catalog `@1` admits, so existing hosts seal unchanged
snapshots, and `@2` only when the catalog includes one of these tools.

Children have private Activities and execution contexts. The parent receives a
typed completion event, then retains the acknowledged report in its context
history with the delivery ContextPlan as provenance. Child transcripts and
reasoning are not copied into the parent conversation. A waiting parent releases
its worker slot, so a single-slot worker can run its children. A child failure
has an explicit terminal report; it cannot silently complete the parent.

Child capability grants, model routes, permission mode and execution policy are
bounded by the frozen parent. Device capabilities are removed. Task arguments
cannot supply authority. `max_delegation_depth` and
`max_active_delegated_children` in `RunExecutionPolicySnapshot` are host-configured
ceilings and return `delegation.depth_limit` / `delegation.concurrency_limit` to
the model. Explicit unsupported Device placement returns
`delegation.placement_unavailable`. No device-to-cloud fallback occurs.

The original cancellation owner freezes and rechecks the complete descendant
set under the conversation lane. Root and descendant cancellation facts commit
atomically. Unleased descendants are queued for ordinary terminal processing;
active Attempts observe the durable cancellation request. `stop_run` on the
cloud delegation service constructs this same authenticated cancellation
command for hosts. New spawns check every ancestor's cancellation authority.

Usage envelopes expose the child Run ID, optional parent Run ID and inherited
billing subject. Raw PostgreSQL usage facts and the protobuf metering encoder
retain this attribution. Historical envelopes may have no attribution; the
worker supplies it on new main, auxiliary, recovery and compaction usage.

## Named Agents and extensions

Use the existing AgentDefinition/version store. Before publishing a version,
call `configure_subagent_version` with its model entry ID, sorted tool deny-list
and hidden flag. `subagent_version_settings` exposes that immutable metadata to
hosts, which omit hidden definitions from their end-user chooser. The version's
persona/system prompt is used by the child. Its execution policy reference/hash
must equal the parent's: an unproven policy replacement is denied. Its model must be one of the parent's
authorized routes. Unavailable definitions or broader model selections fail
explicitly. A Goal-bound child cannot switch its immutable Agent subject.

`WorkerExtensionConfig::subagent_agents` (deployment TOML
`extension_subagent_agents`) is an allow-list for `spawn_subagent`; an empty
list denies spawning. The worker installs the real restricted orchestration
port. Idempotency is scoped by extension and parent Run and persists across
worker death. Extension requests share the same admission, limits and permission
checks as tools, without fabricating a Tool invocation.

`subagent_spawned` and `subagent_ended` are immutable lifecycle facts with outbox
events. A recoverable claim delivers them through the extension lifecycle hooks.
Failed hooks remain pending. Hosts must make external hook effects idempotent;
the event ID is available in the extension session key. Empty extension
registries do not acknowledge lifecycle deliveries.
