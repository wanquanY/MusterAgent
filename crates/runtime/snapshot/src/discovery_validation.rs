//! Exact discovery invariants, shared by admission and sealed recovery.
use super::*;

impl RuntimeSnapshotContent {
    pub(super) fn validate_discovery(
        &self,
        discovery: &DiscoverySnapshot,
        catalog: &AuthorizedToolCatalog,
        frozen_hash: Sha256,
        conformance: Conformance,
    ) -> Result<(), SnapshotContractError> {
        let admission = conformance == Conformance::Admission;
        if admission {
            self.validate_discovery_policy(discovery)?;
        }
        if self.is_private_standing_goal_evaluation(discovery) {
            return self.validate_private_standing_goal_evaluation(discovery, catalog);
        }
        if frozen_hash != discovery.authorized_tool_catalog_hash
            || !self.frozen_skill_catalog_matches(discovery)
        {
            return Err(SnapshotContractError::DiscoveryInvariant(
                "discovery_catalog_mismatch",
            ));
        }
        if discovery.bootstrap_tool_keys.is_empty()
            || discovery.initial_exposed_tool_keys.len()
                > usize::try_from(discovery.max_exposed_tools.get()).unwrap_or(usize::MAX)
            || !discovery
                .initial_exposed_tool_keys
                .starts_with(&discovery.bootstrap_tool_keys)
            || has_duplicates(&discovery.bootstrap_tool_keys)
            || has_duplicates(&discovery.initial_exposed_tool_keys)
            || ((!self.application_catalogs.is_empty()
                && !catalog.application_catalogs().is_empty())
                || self.application_catalogs.is_empty())
                && discovery
                    .initial_exposed_tool_keys
                    .iter()
                    .any(|key| catalog.get_by_key(key.as_str()).is_none())
            || discovery
                .bootstrap_tool_keys
                .iter()
                .any(|key| catalog.get_by_key(key.as_str()).is_none())
        {
            return Err(SnapshotContractError::DiscoveryInvariant(
                "discovery_bootstrap_mismatch",
            ));
        }
        if admission
            && !bootstrap_tool_keys_are_valid(
                discovery.discovery_policy_ref.as_str(),
                &discovery.bootstrap_tool_keys,
                // A cloud dialogue can dispatch signed tools to its sending
                // computer while retaining the cloud bootstrap contract.
                catalog.definitions().iter().any(|tool| {
                    tool.location == CapabilityLocation::Device
                        && !self.origin_tool_in_cloud(tool.tool_ref.as_str())
                }),
                catalog
                    .definitions()
                    .iter()
                    .any(|tool| tool.capability_ref.as_str().starts_with("delegation.")),
            )
        {
            return Err(SnapshotContractError::DiscoveryInvariant(
                "discovery_bootstrap_policy_mismatch",
            ));
        }
        if !skills_are_authorized(&self.skills, &discovery.initial_disclosed_skill_refs)
            || !skills_are_authorized(&self.skills, &discovery.explicitly_selected_skill_refs)
            || !skill_loading::valid(self.schema_version.as_str(), discovery, &self.skills)
        {
            return Err(SnapshotContractError::DiscoveryInvariant(
                "discovery_skill_catalog_mismatch",
            ));
        }
        if admission
            && !initial_skill_disclosures_are_valid(
                &self.skills,
                discovery,
                self.schema_version.as_str(),
                &self.model.route.capabilities,
                catalog,
                self.product_release.as_ref(),
            )
        {
            return Err(SnapshotContractError::DiscoveryInvariant(
                "discovery_skill_disclosure_mismatch",
            ));
        }
        if discovery.carry_forward_source_run_id.is_some()
            != discovery.carry_forward_decision_ref.is_some()
        {
            return Err(SnapshotContractError::DiscoveryInvariant(
                "discovery_carry_forward_mismatch",
            ));
        }
        Ok(())
    }
}
