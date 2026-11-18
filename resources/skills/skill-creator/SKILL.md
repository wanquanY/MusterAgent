---
name: skill-creator
title: Skill Creator
description: Create or revise a reusable private Skill Package when the user asks to turn a recurring workflow, domain procedure, reference set, template, or deterministic helper into a Skill. Do not use for a one-off task or for publishing a public catalog Skill.
when_to_use: Use only when the user explicitly asks to create, package, import, or improve a reusable Skill for their own account.
required_tools:
  - skill.package.preview.create@1
  - skill.package.preview.update@1
optional_tools:
  - workspace.file.read@1
  - workspace.file.list@1
---

# Skill Creator

Create a complete, portable Skill Package and return it as one reviewable, revision-preserving package preview. The Package belongs only to the current user unless an operator separately publishes an approved public package. Never invent a user-facing publish or evaluation flow.

## Understand the reusable job

1. Identify the recurring task, realistic trigger phrases, nearest non-triggers, required inputs, expected outputs, failure conditions, and repeatable verification evidence.
2. Decide whether a Skill is the right abstraction. Complete one-off requests directly; do not wrap a Tool, permission, Agent identity, or single prompt and call it a Skill.
3. Inspect representative files, prompts, references, and an existing Package when they are available. Ask only for information that materially changes the Package.

## Design the Package

Every Package is a self-contained directory rooted at `SKILL.md`:

- `SKILL.md` is required. Its YAML frontmatter contains a lowercase hyphenated `name` and a concrete `description` that states both what the Skill does and when it should trigger. Its Markdown body contains the operational workflow.
- `agents/*.yaml` may contain optional display metadata.
- `scripts/` contains deterministic helpers that are useful enough to avoid rewriting. Scripts never gain authority by being in a Skill and execute only through normal Runtime Tool policy, approval, sandbox, and evidence paths.
- `references/` contains detailed knowledge loaded only when relevant.
- `assets/` contains templates, images, fonts, or boilerplate copied into outputs rather than injected into model context.
- Other safe relative files are allowed when instructions explain their purpose. Do not add process-history clutter such as a README, changelog, or installation guide.

Read `references/package-contract.md` before building an unfamiliar or imported Package, and `references/private-lifecycle.md` for what happens after the preview is created. Start from `templates/SKILL.md` when no better source exists.

## Write for progressive disclosure

- Make the frontmatter description concise and discriminating because discovery sees it before the body.
- Keep the body focused on decisions, sequence, capability use, failure rules, and completion checks.
- Move detailed schemas and domain facts into directly referenced files; do not duplicate them in `SKILL.md`.
- Match freedom to risk: use exact deterministic steps for fragile work and decision criteria for judgment-heavy work.
- Use relative paths inside the Package and preserve safe unknown resources during compatible import.

## Create the reviewable Package

1. Build the complete file set, including non-Markdown resources and scripts when needed.
2. Call `skill.package.preview.create@1` exactly once with the full file set. Retain the returned `preview_id`, `revision`, validation status, and resource inventory as the authoritative checkout state.
3. If validation fails or the user requests a correction during the same authoring flow, fix the complete file set and call `skill.package.preview.update@1` with that same `preview_id` and the latest `expected_revision`. Never create a second Preview to revise the first one.
4. After every update, replace the retained checkout state with the returned revision. On a revision conflict, inspect the current state before retrying; never guess a revision or change the Package identity.
5. Never hide incompatibility or claim success from an invalid archive. Finish only from the latest canonical revision.
6. Saving or installing the Package is a separate explicit user action offered by the host. Do not save or install on the user's behalf merely because the Package was created.

## Safety and ownership

- Never embed secrets, host-specific absolute paths, traversal paths, links, native executable payloads, hidden authority, or approval-bypass instructions.
- Never execute Package scripts during creation, import, validation, inspection, saving, installation, listing, or deletion.
- A Package can declare required Tools but cannot grant capabilities or broaden tenant, workspace, network, credential, or filesystem authority.
- User-created Packages are private. Built-in official Skills are Runtime-owned and cannot be overwritten, uninstalled, or deleted by the user.

## Completion

Creation is complete when the latest revision of the one owner-scoped Skill Package Preview is valid and the Runtime returns its canonical identity key, revision, validation result, and complete resource list. Saving or installing is a later explicit user action; public publication is outside this Skill.
