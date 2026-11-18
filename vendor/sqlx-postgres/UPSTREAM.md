# SQLx PostgreSQL driver

This is the published `sqlx-postgres` 0.8.6 crate, with its original MIT and
Apache licenses, plus the transaction-cancellation fix from upstream:

- https://github.com/transact-rs/sqlx/pull/4394
- Commit `f1e94ec34dd5f11ad6488a3a8ac7988439a3f757` (2026-09-09).

The transaction correction is confined to `src/transaction.rs`: transaction depth
is claimed before awaiting BEGIN and unwound if BEGIN did not take. This lets
the driver's existing drop guard queue ROLLBACK when BEGIN is cancelled. Without
it, later borrowers can receive successful writes that are never committed.

The upstream fix targets the newer API line. This backport keeps the current
0.8.6 API and all existing connection, authorization, TLS and query semantics.
The workspace regression `postgres_begin_cancellation` observes the backend
inside BEGIN before cancellation, then verifies the next autocommit write from
an independent connection. Remove this override when upgrading to an upstream
release containing #4394, retaining the regression test.

Local maintenance for the current futures-channel API also replaces deprecated
`UnboundedReceiver::try_next` with `try_recv` in `src/listener.rs`, preserving
nonblocking notification behavior for available, empty and closed channels.
The unreferenced private Flush message module and format variant are removed;
no connection path emitted this message. These changes remove compiler warnings
without suppressing diagnostics or changing the transaction-cancellation fix.
