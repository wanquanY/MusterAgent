---
name: workspace-reading
title: Workspace Reading
description: Use when a task depends on authorized workspace files or attachments that must be inspected, compared, summarized, or cited without escaping the selected workspace boundary.
when_to_use: Use when the task depends on files or attachments already authorized to the workspace.
required_tools:
  - conversation.attachment.inspect@1
  - conversation.attachment.search@1
  - conversation.attachment.read@2
  - workspace.file.list@1
  - workspace.file.read@1
---

# Workspace Reading

## Goal and scope

Read the minimum authorized file material needed for the task and preserve source identity in every conclusion. This Skill covers discovery, bounded reading, comparison, and citation; it does not grant write access or shell execution.

## Inputs and prerequisites

- Confirm the active workspace and any attachment references supplied by the conversation.
- Use only Runtime-provided workspace and attachment capabilities; never derive host paths from guesses or model-visible text.
- Identify the requested file types, search terms, comparison criteria, and desired output before broad enumeration.

## Workflow

1. List only the relevant directory or attachment scope.
2. Inspect an attachment manifest before reading it so parser provenance, page or slide coverage, parse issues, and available sections are explicit.
3. Search large attachments first, then read only returned chunk IDs or exact page, slide, or section locators. Continue with the opaque cursor only when more material is required.
4. Narrow workspace candidates by name, type, and task relevance before reading contents.
5. Keep each observation associated with its canonical source reference and source locator.
6. Distinguish direct content, calculated conclusions, and inference.
7. When several files conflict, report the conflict and the evidence supporting each side.

## Failure and stop rules

- Stop if a path resolves outside the authorized root or if the Runtime rejects the content reference.
- Do not treat unreadable, missing, truncated, encrypted, or unsupported content as empty content.
- If a required parser or capability is unavailable, state which source could not be inspected and avoid conclusions that depend on it.

## Completion and verification

Verify that every material claim can be traced to a successfully read file or attachment and that no source outside the authorized workspace was used. A file listing alone is not proof that its content was read.
