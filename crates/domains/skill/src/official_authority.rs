//! Exact immutable official identities across independently published products.
use crate::{
    ActivationPolicy, Surface, all_builtin_skill_specs, builtin_skill_specs, skill_from_spec,
};
use musteragent_identity::{SkillId, SkillVersionId, VersionedKey};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

#[derive(Debug, PartialEq, Eq)]
pub struct OfficialSkillAuthority {
    pub skill_id: SkillId,
    pub skill_version_id: SkillVersionId,
    pub bundle_ref: VersionedKey,
    pub compatible_agent_contracts: BTreeSet<VersionedKey>,
    pub compatible_surfaces: BTreeSet<Surface>,
    pub activation_policy: ActivationPolicy,
}

fn authorities() -> &'static BTreeMap<(VersionedKey, SkillVersionId), OfficialSkillAuthority> {
    static INDEX: OnceLock<BTreeMap<(VersionedKey, SkillVersionId), OfficialSkillAuthority>> =
        OnceLock::new();
    INDEX.get_or_init(|| {
        all_builtin_skill_specs()
            .iter()
            .map(|spec| {
                let definition = skill_from_spec(spec).expect("verified official Skill");
                (
                    (definition.skill_ref, definition.skill_version_id),
                    OfficialSkillAuthority {
                        skill_id: definition.skill_id,
                        skill_version_id: definition.skill_version_id,
                        bundle_ref: VersionedKey::new(spec.bundle_ref).expect("compiled bundle"),
                        compatible_agent_contracts: definition.compatible_agent_contracts,
                        compatible_surfaces: definition.compatible_surfaces,
                        activation_policy: definition.activation_policy,
                    },
                )
            })
            .collect()
    })
}

/// Historical single-profile consumer. New admission/recovery must provide the
/// original SkillVersionId; a name alone cannot select between product lineages.
pub fn official_skill_authority(
    reference: &VersionedKey,
) -> Option<&'static OfficialSkillAuthority> {
    let spec = builtin_skill_specs()
        .into_iter()
        .find(|s| s.key == reference.as_str())?;
    let version = skill_from_spec(&spec).ok()?.skill_version_id;
    official_skill_authority_for_version(reference, version)
}

pub fn official_skill_authority_for_version(
    reference: &VersionedKey,
    version: SkillVersionId,
) -> Option<&'static OfficialSkillAuthority> {
    authorities().get(&(reference.clone(), version))
}
