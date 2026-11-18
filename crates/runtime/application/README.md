# Runtime application facade

`RuntimeApplication` is the host-facing command service. It implements the
Conversation, Admission, Run and Interaction application ports and uses the
existing session storage contracts for durable transactions. A transport does
not construct ledger rows or choose snapshot identities.

Compose it with an explicit `AgentKey` and `RuntimeApplicationPorts`: application
and interaction stores, content store, ledger encoder, clock, grant verifier and
snapshot resolver. The Agent must own the conversation's execution participant.
Cloud and single-device ownership are fixed by `RuntimeHomePolicy`; a request
cannot change the service's home or bypass the authenticated admission port.

The host resolves and authenticates model routes, credentials, capability
providers and immutable Agent publications. `KernelSnapshotBuilder` provides
material for the compiled kernel Tool/Skill release, using the exact grant and
published Agent references. It verifies prompt bytes, records discovery
carry-forward decisions and binds private execution to its frozen source. It
selects no provider or model implicitly. Hosts needing their own prompts,
installed catalogs, memory selection or extensions implement
`RuntimeSnapshotResolver`. Unsupported private resolution or exact retry sealing
fails with a typed error. Retained component catalogs require a host owner.

`create_conversation` commits its conversation, participants, ledger and outbox
atomically. Ordinary submission stages input before one atomic admission commit
(run, activity, message, immutable snapshot, ledger and outbox). They are separate
commands, as in the source. For a private accepted input,
`prepare_dialogue_admission` produces the commit consumed by
`DialogueExecutionStore::admit`; that store rechecks the frozen source and creates
the conversation and run in the same transaction. Preparing a commit is not a
durable admission receipt.

Replay uses the original command fingerprint and immutable receipt, checks
current conversation access and does not re-resolve the original snapshot.
Changed command content conflicts. Branch/edit/retract, retry, standing-goal
start and coordination use their existing transactional owners. Cancellation and
interaction responses reuse `InteractionResponseApplication` from the session
crate, preserving the single authority for those state transitions. Grants
requiring durable proof lineage use `with_execution_grant_proof_encoder`.

The facade contains no product prompts, marketplace, billing or channel routing.
The reference API demonstrates host composition; PostgreSQL conformance exercises
real storage transactions and failures. Test grant authenticators accept fixture
claims only and are not production cryptographic verifiers.
