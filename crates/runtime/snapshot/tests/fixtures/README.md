# Current discovery regression matrix

`current-discovery.json` records execution and delegated-authority decisions for
the current kernel tool catalog, including unknown-version probes, before the
historical policy implementations were pruned. It contains no Skill content.

`source-key-discovery.json` additionally pins every capability boundary found by
compiling the pre-cleanup source at `2f155a0` and comparing both public policy
functions with the current implementation. The probe scans all 2,374 Rust/JSON
files, expands every logical key through its highest observed version, and adds
future-version and synthetic unknown keys. The fixture keeps all boundary
families, retired renderers and unknown keys; it does not depend on catalog
membership. Generic discovery @9/@10 deliberately fail closed after T11; all
retained roles preserve the baseline decisions. The full comparison and source
provenance are recorded in `.codex-work/c1/policy-differential.json` and
`policy-key-inventory.json`.
