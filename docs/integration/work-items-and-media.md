# WorkItem and generated media composition

The kernel supplies the WorkItem business implementation. A single-owner worker
constructs `DefaultWorkItemTools::new(owner, content, store, clock)`, where `store`
is `PostgresWorkItemStartStore::new(database, ledger_encoder, opaque_keys)` with
`with_query_content(content)`. The owner must be a user actor. Register a
`WorkItemCapabilityHandler` for each supported manifest in the cloud catalogue,
sharing that default implementation. `CompositeCapabilityDispatcher::register_atomic`
registers these handlers in the dispatch path used by `DurableToolExecutor`.
A host may supply its own `Arc<dyn WorkItemTools>` through the same handler.
No business callbacks are required by the default implementation.

The implementation uses existing session contracts and PostgreSQL owners for
start, handoff, amendment, dependency validation, cancellation, result submission,
verification, feedback and followup. Task revisions and receipts are appended;
only current projections advance. Every new tool transaction checks its live
execution fence and immutable source identity. Exact retries return their
original receipts. Another owner or a task selected by an untrusted argument
cannot create authority.

The active catalogue uses `tasks.submit_result@1` and `tasks.verify@3`. A result
submission is private. Verification checks the original report, manifest, item
references and goal criteria, then publishes through the existing result owner.
`tasks.feedback@1` can reserve another execution for the same owner and original
executor. It does not bypass verification. `tasks.followup@1` requires an accepted
user reply to an exact published result and creates a new task linked to that
result's current action head. It never reopens the completed parent.

For image/video generation, implement `BackendMediaCapabilityProvider` and
register `MediaArtifactCapabilityHandler` with:

- the provider's named operation, declared artifact type, manifest and stable reconciliation identity;
- `PostgresMediaGenerationJournal`;
- `BuiltinArtifactTools` using the same content store;
- `PostgresFencedArtifactToolCommitter` and the worker clock.

The provider owns transport, authentication, idempotent job submission, status
queries and result fetching. The kernel contains no provider names, credentials
or implicit alternative provider. `Active` is a known processing job; `Pending`
is an unknown outcome. Both remain nonterminal until reconciliation supplies
proof. A provider-declared failure is distinct from a runtime materialization
failure. Status queries never submit a new job. The adapter must preserve its
invocation-to-job mapping across restarts, including ambiguous submissions.

On completion the journal commits the provider result and its asset references
or immutable result URLs before fetching bytes. A restored worker loads this result without
submitting or polling the provider again. `materialize_media` fetches/verifies
exactly those assets through the content owner. Existing references and metadata
remain unchanged. URL-only results use `media_asset_content_id(invocation, ordinal)`
for stable Content identities; repeated fetches cannot replace earlier bytes. The artifact owner verifies stored bytes and
commits the Artifact version, tool result and ledger records in one fenced
transaction. Deterministic content/version identities and the frozen observation
time make retries identical, including after a lost commit response.

Database baseline `0001_kernel_baseline.sql` includes the bounded append-only
completion journal. Missing journal/provider/content services produce explicit
errors or an unknown outcome requiring reconciliation. They never imply that
external generation succeeded or failed.

The PostgreSQL tests use disposable pgvector containers and the production
dispatch/handler/storage owners. Admitted execution identities and completed
model batches are fixture inputs. They do not claim live-model, cloud-worker
process or full admission acceptance; those remain separate runtime journeys.
