---
name: replace-with-skill-key
description: State what recurring task this Skill handles, the concrete situations that should trigger it, and the nearest tasks that should not trigger it.
---

# Purpose

Describe the repeatable outcome and the boundary of this Skill. Do not describe a Tool, permission, Agent identity, or one-off task.

## Inputs and prerequisites

- List the required inputs and authorized resources.
- State assumptions that must be verified before work begins.
- Name required and optional capabilities without implying that the Skill grants them.

## Workflow

1. Inspect the inputs and choose the applicable path.
2. Read only the supporting references needed for that path.
3. Perform each effect through an authorized Capability invocation.
4. Validate the result against the completion criteria before reporting success.

## Supporting resources

- Link each optional reference directly from the relevant workflow step.
- Put repeated deterministic logic in `scripts/` and explain its inputs, outputs, and failure behavior.
- Put output templates or reusable binary assets in `assets/`; do not load them into context unless required.

## Decision and failure rules

- Explain the important branches and when to stop or ask for input.
- State how to behave when a capability, credential, network, or source is unavailable.
- Never hide incompatibility, bypass approval, or claim an unverified result.

## Completion and verification

Define the evidence that proves completion, including checks that another Agent or reviewer can repeat.
