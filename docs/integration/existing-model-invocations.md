# Looking up an accepted model invocation

The model adapter and durable agent loop recover an accepted invocation through
its original idempotency key. Configure the gateway contract before dispatch.
The route is then pinned as `IdempotentRequest`; an unconfigured route retains
its existing recovery policy.

After worker loss, completion-intent recovery runs first. If no terminal intent
exists, the journal loads and verifies the original frozen request, retains its
invocation and producer identity, and issues a bounded, fenced result-recovery
ticket. The replacement re-sends the frozen request with the same key. A matching
accepted response selects lookup, without creating a new model invocation.

Durable worker takeover uses the current `ActivityV1` snapshot and the journal's
original Home liveness authority. Embedded PostgreSQL hosts configure
`PostgresAgentLoopJournal::with_execution_liveness` and forward the model recovery
port through their decorators. Legacy fixed-deadline records do not have this
recovery authority and remain fail-closed. The reference worker supplies this
wiring.

## Adapter composition

`musteragent_model_providers::ExistingInvocationResolver` observes an operation
already accepted by the gateway. Install an `ExistingInvocationRecovery` using
`DirectModelProvider::with_existing_invocation_recovery`, keyed by the exact
`(ModelProviderId, ModelEntryId)` route pair. Configuration includes:

| Field | Meaning |
| --- | --- |
| `already_accepted.status` | Exact HTTP error status (400–599), commonly 409 |
| `already_accepted.code_pointer` | JSON pointer into the response, such as `/error/code` |
| `already_accepted.code` | Exact host-defined string value at that pointer |
| `resolver` | Host lookup port; `None` is an explicit unavailable error |
| `policy` | Lookup deadline, maximum count, initial backoff and maximum backoff |

A matching status without a matching body field is an ordinary HTTP error. No
product-specific error code is compiled into the classifier. Classification reads
at most 64 KiB of error data within two seconds; remote error bodies are not logged.

The resolver receives the pinned `ModelRequestAuthority`, invocation endpoint,
HTTP headers, and **the actual key selected before dispatch**. The default key is
`model-{invocation_id}`; an explicit admitted key annotation is retained exactly.
Scoped credential renewal before dispatch also updates the lookup headers.
An optional first-party transport must preserve the selected idempotency key.

The resolver returns:

- `Completed(response)`: the original native SSE or AWS event body, streamed
  or supplied as a whole retained body, including
  response identity, terminal event and original raw usage. The adapter feeds
  this into its ordinary normalizer, stream budgets and canonical validation.
- `Pending`: retry lookup with capped exponential backoff. This is not model
  progress and does not renew the fixed lookup deadline.
- `Failed`: verified terminal failure of the original inference.
- `Err(ProviderError)`: lookup failed; it does not authorize another inference.

The default policy allows 10 lookups within 30 seconds, starting at 100 ms and
capping backoff at two seconds. Deadline and count both bound the operation;
even a hung resolver future is cancelled at the deadline. The configured timeout
cannot exceed five minutes. Model activity supervision can impose a tighter
bound. Dropping the owned model stream also drops an in-progress lookup or delay.

## Standard HTTP result contract

`HttpExistingInvocationResolver::new(result_url)` implements this optional wire
contract. Hosts with another response shape implement the port instead.

```text
POST <invocation endpoint>
idempotency-key: model-<invocation UUID>

409
{"error":{"code":"<host-configured accepted code>"}}

GET <configured result URL>?key=model-<same invocation UUID>

200  original native event stream, with original usage and terminal event
202  original operation is pending
410  original operation failed
```

The result URL must have the same origin as the validated invocation URL. It
cannot contain credentials, query parameters or a fragment. The client disables
redirects and automatic retries. It copies the invocation's credentials and host
metadata, removes request-body framing, and URL-encodes the key. It never follows
a result URL supplied in a remote error body. A host needing method-specific
signatures or credential renewal during lookup implements its own resolver.

The gateway must authenticate the caller and scope key ownership to the original
tenant, actor and route. Its deduplication record and result must survive worker
loss; an expired or unknown key must not silently dispatch another upstream
request. Its retention period must cover the host's recovery horizon. The result
must be the original completion, not a newly generated equivalent answer.

## Failures and persistence boundary

| Provider error | Meaning | Runtime classification |
| --- | --- | --- |
| `ExistingInvocationResolverUnavailable` | Accepted response, no resolver | `ModelRouteUnavailable` |
| `ExistingInvocationTimeout` | Pending or lookup I/O exceeded budget | `DependencyTimeout` |
| `ExistingInvocationFailed` | Verified original-operation failure | `ModelRequestRejected` |
| `ExistingInvocationLookupFailed` | Lookup transport/protocol failure, including an incomplete recovered stream | `DependencyUnavailable` |

All four are nonretryable and excluded from alternate-provider and dependency
recovery admission. In particular a truncated recovered stream must not become
an ordinary retryable transport error. Missing resolution or pending timeout is
not proof that remote execution stopped; physical stop supervision retains that
uncertainty. Only a verified terminal can confirm remote completion.

The adapter does not write ledger entries, usage facts, billing records or
completion intents. It emits the same canonical stream as the native response.
The existing agent-loop completion-intent and fenced journal owners remain the
only persistence path. Kernel usage is a raw token fact; pricing and charging
belong to the host.

The replacement validates the whole recovered canonical response under the
original stream budgets before publishing replayed output. Pending lookups do
not renew execution activity. Text and reasoning-summary prefixes are compared
byte for byte, independently of old HTTP chunks or output batches; Runtime
preview output is also reconciled. Only missing suffixes are appended. Complete
recovered output uses ordinary ContentRef frames so a large response does not
exceed a ledger append's record limit.

The original output batch and verified terminal proof commit together under the
successor fence. The existing protected completion intent and `finish_model`
transaction then record the original completion, one raw usage fact, and the
next loop phase. Original producer Attempt identity remains immutable. Stale
owners cannot append output or finish this invocation. Ordinary completion-intent
recovery still has priority after a lost finish acknowledgement.

The Home recovery challenge bounds the entire query, parsing, and terminal-proof
acceptance, and may be shorter than `policy.timeout_ms`. A second owner cannot
renew an already issued challenge. Interrupted/expired challenges close through
the existing unconfirmed-stop protocol and fail closed; an HTTP disconnect does
not prove the upstream operation stopped. This is bounded result recovery, not
an unlimited background polling service.

Host annotations and credential hooks must select the same key for the frozen
invocation across worker replacement. Never derive a key from the current worker
or successor Attempt. Content resolution must also reproduce the original native
payload if the gateway checks request bytes. A gateway must retain ownership/results long enough for
that recovery horizon; it must reject expired or unknown prior keys explicitly.

## Reference cloud worker

Add the following to a configured `[[models]]` entry:

```toml
[models.existing_invocation]
result_url = "https://gateway.example/model-result"

[models.existing_invocation.already_accepted]
status = 409
code_pointer = "/error/code"
code = "<your gateway's accepted-operation code>"

[models.existing_invocation.policy]
timeout_ms = 5000
max_lookups = 10
initial_backoff_ms = 100
max_backoff_ms = 1000
```

Omit `result_url` to retain classification with an explicit
`ExistingInvocationResolverUnavailable` error. Other wire contracts use the
resolver trait in embedded composition. The executable uses the standard GET
contract above. Extension wrappers forward the same fenced recovery port and
retain their normal request annotations and completion observations.

Acceptance tests run real worker binaries against disposable PostgreSQL, S3 and
a retained fake gateway. They SIGKILL the first worker before any response and
after a durable text prefix, assert equal request bodies/keys, then verify one
invocation, one raw usage fact and exact original text. Separate binary cases
exercise typed timeout, failed result and missing resolver outcomes.
