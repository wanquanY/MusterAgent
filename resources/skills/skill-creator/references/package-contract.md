# Portable Skill Package contract

## Required root and metadata

A Package is a directory or ZIP with exactly one authoritative root `SKILL.md`. An importer may remove one harmless wrapping directory. `SKILL.md` must begin with YAML frontmatter containing non-empty `name` and `description`, followed by non-empty Markdown instructions.

- `name`: lowercase letters, digits, and hyphens; preferably a short action-oriented identity.
- `description`: states what the Skill handles, when it should trigger, and the nearest requests that should not trigger it.
- Compatible extra frontmatter may be preserved. Capability declarations such as `required_tools` and `optional_tools` are requirements, never grants.

## Resource roles

Supporting paths are not a closed allowlist:

- `agents/*.yaml`: optional display metadata;
- `scripts/`: deterministic helpers invoked only through ordinary authorized Tool execution;
- `references/`: detailed knowledge and schemas read progressively;
- `assets/`: templates, images, fonts, fixtures, or boilerplate used in output;
- `templates/`, `examples/`, `tasks/`, `workflows/`, and `prompts/`: ordinary resources when they materially support the workflow;
- licenses, attribution, and compatible ecosystem metadata.

The Package must not include redundant README, installation, changelog, or process-history files. `SKILL.md` must point directly to optional references that the Agent should load.

## Import and validation

The Runtime preserves safe unknown files, compatible metadata, bytes, case, and relative paths. It rejects absolute or traversal paths, links, device files, native executable payloads, duplicate normalized paths, ambiguous roots, and archive/file/count/expanded-size limit violations.

Validation checks structure, frontmatter, references, hashes, capability declarations, portability, and static safety. Creation, import, validation, inspection, saving, installation, listing, and deletion never execute scripts.

## Private lifecycle

The Runtime stores the validated archive as one owner-scoped, revisioned Package Preview. Corrections replace the complete file set through compare-and-swap while preserving the Preview ID. The host can then offer the user explicit actions to save the private Package, install it, uninstall it, or delete a user-owned saved Package; see `private-lifecycle.md`.

Official built-in Packages are Runtime-owned, loaded through the same Package mechanism, and non-removable. Ordinary users never publish Packages to a public catalog; operators use a separate controlled release path.
