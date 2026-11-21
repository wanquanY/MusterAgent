# Scheduled cloud execution

`musteragent-schedule` owns the deterministic schedule calculations and policies.
The goal domain reexports its contracts for compatibility; standing pursuit and
judge execution remain independent. The PostgreSQL baseline includes the schedule tables.

## Host service and tools

Use `PostgresScheduleService::{change,list,history_page}`. `ScheduleCommand`
contains an authenticated `ScheduleScope`, stable command ID, schedule ID,
expected revision, action and timestamp. A host supplies its authenticated actor
and an owned source Run. Tool calls additionally bind the current Attempt fence.
Never construct this scope from model arguments.

Actions are `Create`, `Update`, `Pause`, `Resume`, `Delete`, `RunNow`,
`BeginHandover` and `CompleteHandover`. Reusing an identical command returns its
original response; a different fingerprint or stale revision produces a typed
conflict. Deleted definitions and command/occurrence history remain persisted.
The owning conversation can list its schedules and page through immutable history
with `ScheduleHistoryCursor`. Limits are 1–100; `next: null` ends traversal.

The cloud catalog exposes `schedules.create@2`, `schedules.update@2`,
`schedules.control@2`, `schedules.list@2`, and `schedules.history@1`. Management
effects and original Tool success receipts commit together. The earlier device
contracts retain their versions. `ScheduleView` exposes `pause_reason: Option<SafeDisplayText>`
to host callers (`PostgresScheduleService::{list,change}`) and in `schedule.changed`
payloads; the frozen `@2` tool schemas (`additionalProperties: false`) remain
unchanged and omit `pause_reason` at the tool boundary. `Resume` and `Update`
clear `pause_reason`. A scheduled Run or its descendant receives
`ScheduledContextDenied` from the service when attempting schedule management;
the Tool boundary reports `PolicyDenied`.

New cloud snapshots offering these versions must pin
`CLOUD_SCHEDULE_DISCOVERY_POLICY_V1` (`musteragent.cloud-schedule-discovery@1`).
It retains the @9 cloud bootstrap and adds only the five schedule contracts.
Historical discovery policies keep their original ceilings; installing a newer
catalog does not expand already admitted Runs.

Definitions include instructions, cron/interval/once timing, IANA timezone,
concurrency, lateness window and retry specification. Invalid timezones are
rejected for every timing kind. Cron evaluation skips nonexistent local times;
an ambiguous local time fires once at its earlier instant. Interval arithmetic
uses the anchored UTC millisecond grid. There is no polling-time jitter.

## Ownership and reliability

The cloud worker polls bounded candidates every 250 ms. A locked schedule row,
deterministic occurrence ID and one transaction create a new private conversation,
original Activity/Run/Snapshot/Ledger admission, dispatch outbox and occurrence
link. Two dispatchers cannot admit two Runs for one occurrence. The source
Snapshot is the permission ceiling; device capabilities are excluded. Every
scheduled sandbox context has the Scheduled trigger and ServerOnly policy.

`Skip` skips overlapping occurrences, `Allow` admits them, and `Replace` durably
requests cancellation of active predecessors before admitting a pending
replacement. Lateness decisions precede concurrency decisions. Manual runs bypass
lateness but respect concurrency. Changes that cancel pending occurrences emit
terminal history/outbox events rather than silently discarding them.

RetrySpec applies to attempts of the **same** admitted Run, with a sealed capped
exponential backoff and attempt budget. It does not create replacement Runs with
fresh effect identities. Leases and fencing allow another worker to resume after a crash. Dependency errors
before admission preserve the due cursor, emit `schedule.dispatch.deferred`, and
defer that schedule by 30 seconds so it cannot starve other candidates.

## Pre-trigger host admission hook

Hosts whose capability entitlements, skills or prompts can change after schedule
creation configure `PostgresScheduleDispatcher::with_admission_hook(Arc<dyn ScheduleAdmissionHook>)`.
When no hook is configured, dispatch uses the frozen source Snapshot directly.
Delegated child sub-agent Runs never invoke `ScheduleAdmissionHook`.

Before admitting a due occurrence, the dispatcher releases database row locks and
calls `ScheduleAdmissionHook::before_trigger(ScheduleTriggerContext)`. The hook
returns `Result<PreTriggerDecision, ScheduleAdmissionHookError>`:

- `PreTriggerDecision::UseFrozenSource` derives the scheduled Run from the frozen
  source Snapshot (excluding device capabilities).
- `PreTriggerDecision::Refresh(Box<RefreshedAutomationPartition>)` replaces the
  `{ capabilities, tools, skills, discovery, prompt }` partition while keeping the
  frozen Agent identity, model routes, policy mode, memory/instinct selections and
  Goal authority. The refreshed Snapshot passes full admission sealing
  (`RuntimeSnapshot::seal`), derived-grant projection checks (`grant_derived_<run>`),
  content authorization and `runtime.derived_run_authorities` lineage recording
  with no validation bypass; physical device capabilities are rejected.
- `PreTriggerDecision::Pause { reason }` transitions `agent.owner_schedules` to
  `paused` with `pause_reason = reason` (`1..=1024` UTF-8 bytes, non-null only
  when `status = 'paused'`), creates no Run, records the occurrence as `skipped`
  with that reason (`schedule.occurrence.ended`), and emits `schedule.changed`.
  `ScheduleAction::Resume` and `ScheduleAction::Update` clear `pause_reason`.
- `Err(ScheduleAdmissionHookError { retryable: true, .. })` never falls back to
  the stale Snapshot: no Run is created, `tick` defers the schedule by 30 seconds
  and emits `schedule.dispatch.deferred` with the hook reason so the same
  occurrence retries later.
- `Err(ScheduleAdmissionHookError { retryable: false, .. })` never falls back to
  the stale Snapshot: no Run is created, the occurrence is recorded as
  `dead_lettered` with `schedule.occurrence.ended` carrying the failure reason,
  and the occurrence is not retried.

Placement has exactly one owner: `Cloud` or `Device(name)`. `BeginHandover`
pauses and increments the epoch in one transaction. `CompleteHandover` requires
that epoch and restores the prior active/paused/completed state and due instant.
An already completed one-shot is not reactivated. Stale cloud fires are rejected.
Device execution is not implemented here; its placement identity is retained for
the later device adapter.

## Result delivery and limits

Hosts subscribe to the original durable outbox topics `schedule.changed`,
`schedule.occurrence.started`, `schedule.occurrence.ended`, and
`schedule.dispatch.deferred`. Delivery is at least once; deduplicate `message_id`.
Occurrence dispatch itself is exactly once. Terminal payloads include schedule,
occurrence, Run, private session, owning conversation, terminal status/error and
all assistant message content references (ID/hash/media type/length). Full bytes
remain in the Content Store. The owner can inspect the separate conversation.

The current derived-admission adapter accepts cloud tenant/workspace source Runs.
Goal-authorized source snapshots are supported. Derived tasks preserve their
frozen Goal/Agent permission ceiling, and the original Goal and Agent owners
check current revocation at admission and execution. Their private context does
not claim the original pursuit's public-history window; no new Goal or pursuit
is created. Product application catalogs return the typed
`ApplicationCatalogUnsupported` rejection at creation, exposed to tools as
`schedule.application_catalog_source_unsupported`. See the
[orchestration integration](cloud-orchestration.md) for source authority and
child-run details.
