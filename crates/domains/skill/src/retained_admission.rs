use musteragent_identity::VersionedKey;

use crate::{
    BuiltinSkillCatalogError, SkillCatalogEntry, builtin_entry, generated_retained_catalog,
    generated_retained_catalog_base_refs,
};

/// A complete catalog explicitly retained by the selected immutable release.
/// Its entries carry their original release authority, not individually revived
/// current-release entries. This selection grants no tools or tenant resources.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RetainedBuiltinSkillCatalog {
    pub release_schema: &'static str,
    pub entries: Vec<SkillCatalogEntry>,
}

/// Complete catalogs explicitly declared by the selected release for material
/// publication and readiness verification. This does not select an admission
/// catalog or change the active status of any historical Skill.
pub fn retained_builtin_skill_catalogs()
-> Result<Vec<RetainedBuiltinSkillCatalog>, BuiltinSkillCatalogError> {
    generated_retained_catalog_base_refs()
        .iter()
        .map(|base| {
            retained_builtin_skill_catalog(&[VersionedKey::new(*base)?])?
                .ok_or(BuiltinSkillCatalogError::AmbiguousRetainedAdmission)
        })
        .collect()
}

/// Admission must supply Base pins from the verified Agent manifest. Historical
/// lookup by itself is not admission authority: only profiles declared by the
/// selected release are eligible, and mixed Base profiles are rejected.
pub fn retained_builtin_skill_catalog(
    base_refs: &[VersionedKey],
) -> Result<Option<RetainedBuiltinSkillCatalog>, BuiltinSkillCatalogError> {
    let retained = base_refs
        .iter()
        .find_map(|base| generated_retained_catalog(base.as_str()));
    from_specs(base_refs, retained)
}

pub(crate) fn from_specs(
    base_refs: &[VersionedKey],
    retained: Option<(&'static str, Vec<crate::BuiltinSkillSpec>)>,
) -> Result<Option<RetainedBuiltinSkillCatalog>, BuiltinSkillCatalogError> {
    let Some((release_schema, specs)) = retained else {
        return Ok(None);
    };
    if base_refs.len() != 1 {
        return Err(BuiltinSkillCatalogError::AmbiguousRetainedAdmission);
    }
    let entries = specs
        .iter()
        .map(builtin_entry)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(RetainedBuiltinSkillCatalog {
        release_schema,
        entries,
    }))
}
