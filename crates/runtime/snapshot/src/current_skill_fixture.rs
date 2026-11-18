//! Current kernel release material used by domain validation tests.
use musteragent_skill::{SkillCatalog, SkillDefinition};
pub(crate) fn skill(reference: &str) -> SkillDefinition {
    musteragent_skill::builtin_skill_catalog_entries()
        .expect("kernel skills")
        .into_iter()
        .find(|entry| entry.definition.skill_ref.as_str() == reference)
        .expect("current Skill")
        .definition
}
pub(crate) fn work_catalog() -> SkillCatalog {
    SkillCatalog::new(
        musteragent_skill::builtin_skill_catalog_entries()
            .expect("kernel skills")
            .into_iter()
            .map(|entry| entry.definition)
            .collect(),
    )
    .expect("catalog")
}
