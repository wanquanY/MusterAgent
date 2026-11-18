# Security Policy

## Reporting a vulnerability

Please do **not** open a public issue for security problems. Report them
privately through [GitHub private vulnerability reporting](https://github.com/wanquanY/MusterAgent/security/advisories/new).

Include a description, affected versions or commits, reproduction steps and
the impact you expect. You will receive an acknowledgement if and when the
report is triaged; there is no guaranteed response time (see README.md,
"Support").

## Scope

The kernel crates in this repository: run/attempt execution, ledger and
outbox, leases and fencing, scheduling, projection, the agent loop, context
assembly, tool execution and capability grants, the PostgreSQL store, and the
placement and device-interaction contracts.

Out of scope: products built on top of musteragent, their hosting, and
third-party model providers.

## Handling secrets

The repository must never contain credentials. Provider credentials are
resolved by the host at runtime through the capability/credential ports.
