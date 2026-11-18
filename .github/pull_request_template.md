## What changes and why?

Describe the problem and the resulting behavior. Link the relevant issue.

## Validation

List the checks you ran and their results (fmt, Clippy, cargo deny, the
Docker-backed test suite or the targeted crates). State any checks you could
not run.

## Review checklist

- [ ] The change stays inside the kernel boundary (no billing, product UI or host-specific integration).
- [ ] Tests cover the changed behavior, including failures where relevant.
- [ ] Storage, wire and known-answer changes are intentional and documented.
- [ ] Added code and data have known redistribution rights.
- [ ] No credentials, private data or machine-specific paths are included.
