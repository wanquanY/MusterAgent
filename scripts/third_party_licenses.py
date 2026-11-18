#!/usr/bin/env python3
"""Regenerate the workspace's third-party license inventory from Cargo.lock."""

from collections import defaultdict
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def main():
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--offline", "--format-version", "1"],
        cwd=ROOT,
    ))
    members = set(metadata["workspace_members"])
    packages = [p for p in metadata["packages"] if p["id"] not in members]
    groups = defaultdict(list)
    vendored = []
    for package in packages:
        license_expr = package["license"] or f"see {package['license_file'] or 'package'}"
        label = f"{package['name']} {package['version']}"
        if package["source"] is None:
            directory = Path(package["manifest_path"]).parent.relative_to(ROOT)
            description = label
            if directory.as_posix() == "vendor/sqlx-postgres":
                description = f"SQLx PostgreSQL driver {package['version']} with a backported cancellation fix"
            note = f"- `{directory.as_posix()}` — {description} ({license_expr})."
            if (ROOT / directory / "UPSTREAM.md").is_file():
                note += f" See `{directory.as_posix()}/UPSTREAM.md`."
            vendored.append(note)
            label += " (vendored)"
        groups[license_expr].append(label)

    lines = [
        "# Third-party licenses", "",
        "musteragent depends on the third-party Rust crates below (the complete",
        "dependency graph of the workspace, including build and test dependencies,",
        "as resolved in `Cargo.lock`). Each crate is distributed under the license",
        "expression shown; full license texts ship with each crate source.", "",
        "Generated from `cargo metadata`; regenerate after dependency changes.", "",
        f"Total: {len(packages)} crates.", "",
    ]
    for license_expr, labels in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        lines.extend([f"## {license_expr} ({len(labels)})", "", ", ".join(sorted(labels)), ""])
    if vendored:
        lines.extend(["## Vendored sources", "", *sorted(vendored), ""])
    (ROOT / "THIRD_PARTY_LICENSES.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
