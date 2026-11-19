# Kernel wire and persistence contracts

`musteragent-persistence-codec` is the single generated owner of shared values.
Its schemas use `musteragent.v1`, the same package as the RPC contracts.
Storage codecs and PostgreSQL depend on this transport-free crate.

`musteragent-wire` depends on that owner. Its `proto/rpc_*.proto` files contain
only additional kernel messages and service declarations. `build.rs` maps each
shared message/enum to its existing Rust type with `extern_path`; generated RPC
signatures therefore accept the very same types used by storage. Do not copy a
shared message into this crate. Add shared durable fields at the persistence
owner, and add RPC-only envelopes/services at the wire owner. Both descriptor
sets are generated from checked-in sources, using the cached vendored protoc.
The wire descriptor includes the transitive shared definitions for non-Rust
client generation. Rust clients use `musteragent_wire::wire`.

The RPC surface includes execution and model/tool journals, context-plan
lineage, conversation record/projection subscriptions, grants, WorkItems,
goals (including standing goals), evolution, delegation admission and sandbox
jobs. The usage projection service transports raw usage facts; billing
admission and commercial policy are excluded. Reserved field numbers, including
DomainEvent field 77, must remain reserved. Multi-device home identity remains a
kernel contract.

The ledger codec and scheduler encoder live in `musteragent-wire`. They share
one domain-event encoder, so a scheduler fact has exactly the same bytes as
its ledger representation. Decode failures use `SubscriptionError`; scheduler
failures use the scheduler port's error type. The sandbox routing fact is
encoded as DomainEvent field 81, including executor, reason, compatibility and
decision time. Decoding validates the closed values and route consistency.
Content-reference discovery uses the same shared descriptor, including that
new event, so routing events have complete empty reference coverage.

Hosts can compose generated service traits, the ledger codec, the scheduler
encoder and PostgreSQL ports. This package does not implement a server,
cloud worker, projector process, database migrator binary or reference API.
RPC declarations are contracts, not claims of deployed endpoints. Durable
worker restart and client reconnect acceptance remains with those packages.

See the [shared messages](../../crates/infrastructure/persistence-codec/proto/) and
[RPC declarations](../../crates/infrastructure/wire/proto/) for the complete protocol inventory.
PostgreSQL conformance creates fresh pgvector containers through the existing
testkit. Test hosts explicitly register a synthetic Agent definition; the
kernel does not restore a product's implicit built-in identity. Controlled
model/compactor and in-memory blob fixtures exercise real PostgreSQL owners,
without connecting to an existing database or external model provider.

## Conversation catalog queries and unread attribution

`ListConversationCatalogV2` in `musteragent-projection` filters conversations by
workspace scope with `CatalogWorkspaceFilter`:

- `All` — include both workspace-assigned and unassigned conversations;
- `Unassigned` — include only conversations with `workspace_id IS NULL`;
- `Assigned(WorkspaceId)` — include only conversations in that workspace.

`ListConversationCatalogV2::from_workspace_id` and `CatalogWorkspaceFilter::from_workspace_id`
preserve the `Option<WorkspaceId>` mapping (`None` → `All`, `Some(id)` → `Assigned(id)`).
`title_query` accepts an optional substring filter that is trimmed on validation,
bounded to 1–100 Unicode characters without control characters, and matched
case-insensitively with SQL `LIKE` wildcards (`%`, `_`, `\`) escaped as literals.
Empty or whitespace-only `title_query` values fail validation with `InvalidTitleQuery`
(`ConversationCatalogSourceError::InvalidQuery`). Both predicates execute inside
the keyset page query before `(last_user_visible_activity_at_ms DESC, conversation_id DESC)`
ordering and `LIMIT`, keeping `ConversationCatalogCursorV2` stable across pages.
On the gRPC surface, `ListConversationCatalogV2Request` adds additive
`workspace_filter_mode` (`CatalogWorkspaceFilterMode`) and `title_query` fields
alongside `workspace_id`.

`ConversationCatalogLedgerProjector` attributes unread attention deliveries per
participant:

- `ConversationEvent::Created` excludes the creator from `conversation_catalog_attention_deliveries`
  (using the record's `participant_id`, or falling back to the authority actor
  principal when the creation record precedes `ParticipantJoined` in the same
  transaction). Other participants still receive `ConversationCreated` unread
  attention.
- `RunEvent::Cancelled` with `CancelReason::UserRequested` or
  `CancelReason::ConversationArchived` updates the catalog row's `latest_outcome`
  to `Cancelled` and closes active execution and pending actions, while excluding
  the participant who requested cancellation from unread delivery. A
  `ConversationEvent::MessagePartiallyCommitted` with `PartialMessageReason::Cancelled`
  likewise excludes that cancelling participant from new `VisibleOutput` unread
  delivery. Other participants in the conversation, non-actor cancellation
  reasons (`ParentCancelled`, `PolicyRevoked`, `SystemShutdown`,
  `CoordinationInterrupted`), and non-cancelled outcomes keep broadcast unread
  behavior.
