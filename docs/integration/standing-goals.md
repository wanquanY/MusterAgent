# Standing-Goal supervision

A standing Goal keeps working after the Run that created it ends. The
`StandingGoalSupervisor` (crate `musteragent-runtime-automation`) owns claims,
evaluation, retries, waits and settlement. It admits every bounded Run, either the
next continuation or an independent judge, through ports that this kernel
implements on the original `RuntimeApplication` admission pipeline. The host
supplies only authority: a signed execution grant for the exact target Run.

## What the kernel provides

| Port (automation) | Implementation | Crate |
|---|---|---|
| `StandingGoalAdmission` | `RuntimeStandingGoalAdmission::new(Arc<RuntimeApplication>)` | `musteragent-runtime-application` |
| `OwnerGoalContinuationAdmission` (optional) | `OwnerGoalAdmissionAdapter::new(source, grants, runtime, clock)` | `musteragent-runtime-application` |
| `StandingGoalProductStore` | `PostgresStandingGoalProductStore::new(database)` | `musteragent-postgres` |
| `StandingGoalJudgeRunSource` | `PostgresStandingGoalJudgeRunSource::new(pool, content)` | `musteragent-postgres` |

The `RuntimeApplication` must be built with
`with_execution_grant_proof_encoder(...)` (for example
`ProtobufExecutionGrantProofEncoder`): the supervisor redeems a continuation from
the source Run's frozen grant proof.

Each admission:

1. re-reads the Conversation, the active user participant and the terminal source
   Run (`prepare_standing_goal_continuation` / `prepare_standing_goal_judge`);
2. derives the target command, Run, Activity and record identities from the
   supervisor's deterministic command ID, so a retry names the same Run;
3. asks the host to sign a grant for exactly that Run;
4. resolves the Snapshot through the host `RuntimeSnapshotResolver`. A
   continuation must keep the source Run's frozen model route, model class,
   reasoning summary and permission mode, or admission fails closed;
5. commits the Run, Activity, private Goal context record and dispatch outbox in
   the same transaction that rechecks the Goal revision, Pursuit revision and
   claim token. A lost claim returns `StandingGoalAdmissionOutcome::Superseded`
   and commits nothing; a replay returns the original receipt as a duplicate.

Continuations are `ActivityKind::Automation` and judges are
`ActivityKind::Evaluation`; both name the terminal source as their parent Run.
Neither creates a Conversation message.

## What the host supplies

- `StandingGoalContinuationAuthority`: signs the execution grant for
  `StandingGoalContinuationAuthorityRequest::target_run_id`. It may also return
  opaque durable continuation authority, which is persisted with the Run. When
  the host permanently refuses continuation (for example, a required external
  capability was revoked), return `Err(StandingGoalContinuationAuthorityError::denied(reason))`
  (or set `reason: Option<SafeDisplayText>` on `StandingGoalContinuationAuthorityError`
  / `RecordStandingGoalAdmissionFailure`). The supervisor settles the Pursuit into
  `GoalPursuitStatus::Waiting` (`StandingGoalProgressPhase::Waiting`) with an
  `ExternalDependency` `GoalWaitBarrier` whose `reason_ref` stores the host's
  `SafeDisplayText` (falling back to the default denial guidance when `reason` is
  `None`), keeping continuation halted until explicit user resume (`resume_wait`
  or status recovery). Transient failures (`DependencyUnavailable` with
  `retryable: true`) release the claim and schedule bounded backoff instead of
  blocking.
- `RuntimeSnapshotResolver::resolve`: seals the Snapshot for the continuation
  (`run_context.activity_kind == Automation`) or judge (`Evaluation`). Apply the
  same unattended-run tool rules you apply to scheduled Runs here: no device
  tools without a device present, and no browser-executed tools when no browser
  page will be open.
- For owner-scoped Goals only: an `OwnerGoalExecutionGrantPort` and
  `RuntimeSnapshotResolver::resolve_owner_goal`. Hosts without owner-scoped Goals
  leave the default, which fails explicitly.
- A `RuntimeClock` and a worker ID.

## Composition

```rust
let runtime = Arc::new(
    RuntimeApplication::new(agent_key, ports)
        .with_execution_grant_proof_encoder(Arc::new(ProtobufExecutionGrantProofEncoder)),
);
let store = PostgresStandingGoalProductStore::new(database.clone());
let goals = Arc::new(StandingGoalProductApplication::new(Arc::new(store.clone()), content.clone()));
let admission = Arc::new(RuntimeStandingGoalAdmission::new(runtime.clone()));
let evaluator = IndependentStandingGoalTerminalEvaluator::new(
    admission.clone(),
    authority.clone(),
    Arc::new(PostgresStandingGoalJudgeRunSource::new(pool.clone(), content.clone())),
    content.clone(),
);
let supervisor = StandingGoalSupervisor::new(worker_id, goals, admission, authority, clock)
    .with_terminal_evaluator(Arc::new(evaluator));
// Owner-scoped Goals: .with_dialogue_admission(Arc::new(OwnerGoalAdmissionAdapter::new(
//     Arc::new(store.clone()), owner_goal_grants, runtime.clone(), application_clock)))
// on both the supervisor and the evaluator.

loop {
    for actor in store.supervisor_actors(clock.now(), 256).await? {
        supervisor.run_pass(actor).await?; // tenant-scoped and lease-fenced
    }
    // wait for ledger changes or a short idle interval
}
```

`supervisor_actors` discovers only tenants with actionable Goal work. Every claim
and mutation after discovery stays tenant-scoped. `StandingGoalSupervisor::run`
loops one actor until cancelled.
