//! Frozen product namespace identity checks for Runtime snapshots.
use super::*;

impl RuntimeSnapshotContent {
    pub(super) fn validate_product_namespace(&self) -> Result<(), SnapshotContractError> {
        let prompt_section_keys = self
            .prompt
            .sections
            .iter()
            .map(|section| section.section_key.as_str())
            .collect::<BTreeSet<_>>();
        let discovery_policy = self
            .discovery
            .as_ref()
            .map(|discovery| discovery.discovery_policy_ref.as_str());
        let expected_discovery_policy = if self.model.requested_model_class.as_str()
            == STANDING_GOAL_JUDGE_MODEL_CLASS
            && discovery_policy == Some(PRIVATE_EVALUATION_DISCOVERY_POLICY_V2)
        {
            PRIVATE_EVALUATION_DISCOVERY_POLICY_V2
        } else if discovery_policy.is_some_and(is_hosted_discovery_policy) {
            self.validate_hosted_discovery_boundary()?;
            discovery_policy.ok_or(SnapshotContractError::ProductNamespaceMismatch)?
        } else if discovery_policy.is_some_and(|policy| {
            matches!(
                policy,
                DISCOVERY_POLICY_V11
                    | CLOUD_SCHEDULE_DISCOVERY_POLICY_V1
                    | CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1
            ) || is_dialogue_family_discovery_policy(policy)
        }) {
            discovery_policy.ok_or(SnapshotContractError::ProductNamespaceMismatch)?
        } else {
            return Err(SnapshotContractError::ProductNamespaceMismatch);
        };
        if self.protocol_versions.model_protocol.as_str() != MODEL_PROTOCOL_V2
            || !matches!(
                self.context_policy.compaction_policy_ref.as_str(),
                CONTEXT_COMPACTION_POLICY_V5
                    | CONTEXT_COMPACTION_POLICY_V6
                    | CONTEXT_COMPACTION_POLICY_V7
                    | CONTEXT_COMPACTION_POLICY_V8
                    | CONTEXT_COMPACTION_POLICY_V9
                    | CONTEXT_COMPACTION_POLICY_V10
                    | CONTEXT_COMPACTION_POLICY_V11
                    | CONTEXT_COMPACTION_POLICY_V12
            )
            || !prompt_section_keys.contains(MUSTERAGENT_ASSISTANT_PROMPT_SECTION)
            || prompt_section_keys.contains(LEGACY_PERSONA_PROMPT_SECTION)
            || discovery_policy != Some(expected_discovery_policy)
        {
            return Err(SnapshotContractError::ProductNamespaceMismatch);
        }
        Ok(())
    }
}
