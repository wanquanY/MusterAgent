//! Validate the policy frozen at admission, never the current market default.
use super::*;

pub(super) fn valid(
    schema: &str,
    discovery: &DiscoverySnapshot,
    skills: &[SkillDefinition],
) -> bool {
    let defaults = &discovery.default_disclosed_skill_refs;
    (defaults.is_empty() || matches!(schema, SNAPSHOT_SCHEMA_VERSION))
        && skills_are_authorized(skills, defaults)
        && defaults.windows(2).all(|pair| pair[0] < pair[1])
        && defaults
            .iter()
            .all(|reference| discovery.initial_disclosed_skill_refs.contains(reference))
}

pub(super) fn append(discovery: &DiscoverySnapshot, expected: &mut Vec<VersionedKey>) {
    for reference in &discovery.default_disclosed_skill_refs {
        if !expected.contains(reference) {
            expected.push(reference.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;
    fn key(value: &str) -> VersionedKey {
        VersionedKey::new(value).unwrap()
    }
    fn fixture() -> (Vec<SkillDefinition>, DiscoverySnapshot) {
        let skills: Vec<_> = [
            "assistant-base@1",
            "project-files@1",
            "web-research@1",
            "data-analysis@1",
        ]
        .into_iter()
        .map(crate::current_skill_fixture::skill)
        .collect();
        let initial =
            dialogue_initial_skill_disclosures_for_policy(DIALOGUE_DISCOVERY_POLICY, &skills, &[])
                .unwrap();
        let discovery = DiscoverySnapshot {
            authorized_skills: vec![],
            authorized_skill_catalog_hash: Sha256::digest(b"skills"),
            authorized_tool_catalog_hash: Sha256::digest(b"tools"),
            bootstrap_tool_keys: vec![],
            initial_exposed_tool_keys: vec![],
            initial_disclosed_skill_refs: initial,
            default_disclosed_skill_refs: vec![],
            explicitly_selected_skill_refs: vec![],
            carry_forward_source_run_id: None,
            carry_forward_decision_ref: None,
            discovery_policy_ref: key(DIALOGUE_DISCOVERY_POLICY),
            max_exposed_tools: NonZeroU32::new(10).unwrap(),
            max_search_results: NonZeroU32::new(10).unwrap(),
        };
        (skills, discovery)
    }
    #[test]
    fn frozen_defaults_are_part_of_initial_disclosure_but_not_user_selection() {
        let (skills, mut discovery) = fixture();
        discovery.default_disclosed_skill_refs = vec![key("data-analysis@1")];
        let mut initial = discovery.initial_disclosed_skill_refs.clone();
        append(&discovery, &mut initial);
        discovery.initial_disclosed_skill_refs = initial;
        assert!(valid(SNAPSHOT_SCHEMA_VERSION, &discovery, &skills));
        let tools = AuthorizedToolCatalog::new(vec![]).unwrap();
        assert!(initial_skill_disclosures_are_valid(
            &skills,
            &discovery,
            SNAPSHOT_SCHEMA_VERSION,
            &BTreeSet::new(),
            &tools,
            None
        ));
        assert!(discovery.explicitly_selected_skill_refs.is_empty());
        let frozen = discovery.clone();
        // Changing an admission policy cannot silently reinterpret sealed instructions.
        discovery.default_disclosed_skill_refs.clear();
        assert!(!initial_skill_disclosures_are_valid(
            &skills,
            &discovery,
            SNAPSHOT_SCHEMA_VERSION,
            &BTreeSet::new(),
            &tools,
            None
        ));
        assert!(initial_skill_disclosures_are_valid(
            &skills,
            &frozen,
            SNAPSHOT_SCHEMA_VERSION,
            &BTreeSet::new(),
            &tools,
            None
        ));
    }
    #[test]
    fn defaults_cannot_add_unavailable_versions_or_duplicate_disclosures() {
        let (skills, mut discovery) = fixture();
        let selected = key("data-analysis@1");
        discovery.default_disclosed_skill_refs = vec![selected.clone()];
        assert!(!valid(SNAPSHOT_SCHEMA_VERSION, &discovery, &skills));
        discovery
            .initial_disclosed_skill_refs
            .push(selected.clone());
        assert!(valid(SNAPSHOT_SCHEMA_VERSION, &discovery, &skills));
        discovery.default_disclosed_skill_refs.push(selected);
        assert!(!valid(SNAPSHOT_SCHEMA_VERSION, &discovery, &skills));
        discovery.default_disclosed_skill_refs = vec![key("data-analysis@999")];
        discovery
            .initial_disclosed_skill_refs
            .push(key("data-analysis@999"));
        assert!(!valid(SNAPSHOT_SCHEMA_VERSION, &discovery, &skills));
    }
}
