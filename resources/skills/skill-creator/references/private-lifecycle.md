# Private validation and lifecycle

Hosts decide on which surfaces Skill authoring is exposed. A surface that does not offer the Skill Package Preview Tools cannot create previews, and this Skill must not simulate them.

## Preview validation

Before a preview is presented, the Runtime validates the exact Package bytes and records evidence tied to the preview revision and source payload hash. The minimum checks cover:

- one valid root `SKILL.md` with usable trigger metadata and actionable instructions;
- safe, unique, bounded relative paths and a complete resource inventory;
- static script and payload safety without executing any Package code;
- valid capability and dependency references without treating them as authority;
- portability across declared surfaces and explicit reporting of incompatibilities;
- cryptographic hashes for the source archive and each inspectable resource.

An invalid preview may be displayed only with clear findings and no save/install actions. Updating files advances the same Preview to a new revision; it must not mutate evidence for an earlier revision or create a second checkout for the same correction flow.

## User actions

Creation itself has no side effect on the user's saved Skills. The host presents the preview so every file can be reviewed.

- **Save** persists the private Package and leaves it uninstalled.
- **Install** performs idempotent save plus install.
- **Uninstall** retains the saved Package.
- **Delete** is available only for user-owned Packages and removes them from the user's saved Skills.

There is no ordinary-user evaluation, approval, publish, or public-release step. Public catalog publication is a separate operator boundary. Built-in official Skills are never exposed as removable user-owned entries.
