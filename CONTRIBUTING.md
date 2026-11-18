# Contributing to musteragent

Thank you for your interest. Please read this document before opening an issue
or a pull request.

## Support expectations

musteragent is published as-is. There is no commitment to respond to issues,
review pull requests, publish releases on a schedule, or keep APIs stable
between versions. See the "Support" section of README.md.

## Licensing of contributions

musteragent is licensed under [Apache-2.0](LICENSE). Contributions are
submitted under that license. There is no separate CLA or mandatory DCO
sign-off. Retain third-party notices and verify that you have the right to
redistribute any code or data you add.

## Pull request workflow

1. Discuss substantial architecture, storage or dependency changes in an issue or
   [Discussions](https://github.com/wanquanY/MusterAgent/discussions) first.
2. Fork the repository (or use a feature branch if you have write access) and
   open a pull request against `main` using the template.
3. All changes to `main`, including the maintainer's, land through a pull request
   and are merged with squash. Direct pushes, force pushes and deleting `main`
   are blocked, and history stays linear.
4. The required checks `CI required` and `Security required` must pass on an
   up-to-date branch, review conversations must be resolved, and a code owner
   approves external changes. Version tags (`v*`) are created by the maintainer
   and cannot be moved or deleted.

The exact repository rules are kept in [`.github/rulesets/`](.github/rulesets).

## Ground rules

- Keep changes focused; one concern per pull request.
- Run the [quick start](README.md#quick-start) checks, including Clippy with
  warnings denied and the full Docker-backed test suite. Check dependencies
  with `cargo deny check`.
- Do not commit secrets, credentials, real user data or production endpoints.
  Test fixtures use reserved names (`example.test`, RFC 5737 addresses).
- Product features (personas, prompts, skill content, UI, billing) do not
  belong in this repository; the kernel exposes ports for them instead.

## Third-party licenses

After updating dependencies, run `cargo fetch --locked`, then
`python3 scripts/third_party_licenses.py`. The script uses locked, offline
Cargo metadata for all targets, including build and test dependencies. Review
the regenerated `THIRD_PARTY_LICENSES.md` alongside `Cargo.lock`.

## Integration test configuration

Tests use disposable PostgreSQL/S3 containers. Set `MUSTERAGENT_TEST_OWNER`,
`MUSTERAGENT_TEST_RUN_ID` and `MUSTERAGENT_TEST_EXPIRES_AT` to label a run.
`MUSTERAGENT_TEST_INTEGRATION_TIMEOUT_MS` defaults to 120000 ms for durable
completion and cancellation waits. `MUSTERAGENT_TEST_LEDGER_P95_MS` defaults
to 250 ms on shared machines; set it to 20 for dedicated performance acceptance.
These budgets do not alter the runtime's leases or the asserted terminal facts.
Pure compaction idle-reset tests use virtual time. PostgreSQL compaction and
vision activity tests use `MUSTERAGENT_TEST_ACTIVITY_TICK_MS` (default 1000 ms)
to scale their synthetic activity windows and stream progress together. They
still assert that progress outlives multiple idle windows and silence expires;
database observation and cleanup no longer depend on subsecond scheduling.

Set `MUSTERAGENT_TEST_MCP_SANDBOX=1` to run the public-network MCP tests. See the
[sandbox README](crates/infrastructure/sandbox-server/README.md#public-network-mcp-acceptance).
Live provider canaries remain ignored because they require a controlled account
and paid requests. The adapter's 5% TTFT benchmark remains an explicitly selected
performance test. The delivery audit replay requires the original read-only
fixture via `MUSTERAGENT_PUBLIC_DELIVERY_OUTPUT_AUDIT`; it cannot run from repository
data. Run these ignored tests individually with `--ignored --exact --nocapture`.
