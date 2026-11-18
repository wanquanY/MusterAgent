//! A model name selects one exact Skill from the complete Run catalog. Names
//! are a derived interface, never a publication pointer or authorization fact.
use std::collections::{BTreeMap, BTreeSet};

use musteragent_identity::VersionedKey;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModelSkillNames {
    by_ref: BTreeMap<VersionedKey, String>,
    by_name: BTreeMap<String, VersionedKey>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum SkillNameError {
    #[error("duplicate canonical Skill identity")]
    DuplicateIdentity,
    #[error("model Skill name exceeds the interface budget")]
    NameTooLong,
}

impl ModelSkillNames {
    /// Call with the full sealed set, including unavailable entries. Rebuilding
    /// from a search page or exposed subset could reinterpret collision names.
    pub fn new<'a>(
        references: impl IntoIterator<Item = &'a VersionedKey>,
    ) -> Result<Self, SkillNameError> {
        let mut exact = BTreeSet::new();
        for reference in references {
            if !exact.insert(reference.clone()) {
                return Err(SkillNameError::DuplicateIdentity);
            }
        }
        let reserved: BTreeSet<_> = exact.iter().map(VersionedKey::logical_key).collect();
        let mut by_ref = BTreeMap::new();
        let mut by_name = BTreeMap::new();
        for reference in &exact {
            let base = reference.logical_key();
            let mut name = base.to_owned();
            let mut ordinal = 2_u32;
            while by_name.contains_key(&name) {
                loop {
                    name = format!("{base}_{ordinal}");
                    ordinal = ordinal.checked_add(1).ok_or(SkillNameError::NameTooLong)?;
                    if name.len() > 200 {
                        return Err(SkillNameError::NameTooLong);
                    }
                    if !reserved.contains(name.as_str()) {
                        break;
                    }
                }
            }
            by_ref.insert(reference.clone(), name.clone());
            by_name.insert(name, reference.clone());
        }
        Ok(Self { by_ref, by_name })
    }

    pub fn name(&self, reference: &VersionedKey) -> Option<&str> {
        self.by_ref.get(reference).map(String::as_str)
    }

    pub fn resolve(&self, name: &str) -> Option<&VersionedKey> {
        self.by_name.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: &str) -> VersionedKey {
        VersionedKey::new(value).unwrap()
    }

    #[test]
    fn logical_names_are_stable_across_a_single_release_upgrade() {
        let old = key("research@1");
        let new = key("research@17");
        let old_names = ModelSkillNames::new([&old]).unwrap();
        let new_names = ModelSkillNames::new([&new]).unwrap();
        assert_eq!(old_names.name(&old), Some("research"));
        assert_eq!(new_names.name(&new), Some("research"));
        assert_eq!(old_names.resolve("research"), Some(&old));
        assert_eq!(new_names.resolve("research"), Some(&new));
        assert_eq!(new_names.resolve("research@17"), None);
        assert_eq!(new_names.resolve("Research"), None);
    }

    #[test]
    fn parallel_versions_are_exact_and_natural_suffixes_cannot_be_captured() {
        let refs = [key("research@1"), key("research@2"), key("research_2@1")];
        let names = ModelSkillNames::new(refs.iter()).unwrap();
        assert_eq!(names, ModelSkillNames::new(refs.iter().rev()).unwrap());
        assert_eq!(names.name(&refs[0]), Some("research"));
        assert_eq!(names.name(&refs[1]), Some("research_3"));
        assert_eq!(names.name(&refs[2]), Some("research_2"));
        for reference in &refs {
            assert_eq!(
                names.resolve(names.name(reference).unwrap()),
                Some(reference)
            );
        }
        assert_eq!(names.resolve("unknown"), None);
        assert_eq!(
            ModelSkillNames::new([&refs[0], &refs[0]]),
            Err(SkillNameError::DuplicateIdentity)
        );
    }
}
