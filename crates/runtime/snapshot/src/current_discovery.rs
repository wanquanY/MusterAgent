//! One current discovery contract per execution role. The signed catalog and
//! live admission owner supply authority; a role only narrows that authority.
use super::*;
#[path = "current_discovery_rules.rs"]
mod rules;
#[path = "current_video_policy.rs"]
mod video;
pub use video::video_tool_retired_for_release;

pub const DIALOGUE_DISCOVERY_POLICY: &str = "musteragent.dialogue-discovery@34";
pub const DIALOGUE_DEVICE_DISCOVERY_POLICY: &str = "musteragent.dialogue-device-discovery@25";
pub const DIALOGUE_REPORT_DISCOVERY_POLICY: &str = "musteragent.dialogue-report-discovery@24";
pub const DIALOGUE_TASK_RESULT_DISCOVERY_POLICY: &str =
    "musteragent.dialogue-task-result-discovery@5";
pub const DIALOGUE_DEVICE_TASK_RESULT_DISCOVERY_POLICY: &str =
    "musteragent.dialogue-device-task-result-discovery@6";
pub const DIALOGUE_PUBLICATION_TOOL_REFS: [&str; 2] = ["dialogue.publish@1", "artifacts.deliver@2"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role {
    Dialogue,
    Device,
    Report,
    Task,
    DeviceTask,
    Generic,
    Evaluation,
}
fn role(policy: &str) -> Option<Role> {
    Some(match policy {
        DIALOGUE_DISCOVERY_POLICY => Role::Dialogue,
        DIALOGUE_DEVICE_DISCOVERY_POLICY => Role::Device,
        DIALOGUE_REPORT_DISCOVERY_POLICY => Role::Report,
        DIALOGUE_TASK_RESULT_DISCOVERY_POLICY => Role::Task,
        DIALOGUE_DEVICE_TASK_RESULT_DISCOVERY_POLICY => Role::DeviceTask,
        DISCOVERY_POLICY_V11 => Role::Generic,
        PRIVATE_EVALUATION_DISCOVERY_POLICY_V2 => Role::Evaluation,
        _ if is_hosted_discovery_policy(policy) => Role::Generic,
        _ => return None,
    })
}
pub fn is_dialogue_family_discovery_policy(policy: &str) -> bool {
    matches!(
        role(policy),
        Some(Role::Dialogue | Role::Device | Role::Report | Role::Task | Role::DeviceTask)
    )
}
pub fn is_dialogue_device_discovery_policy(policy: &str) -> bool {
    matches!(role(policy), Some(Role::Device | Role::DeviceTask))
}
pub fn is_dialogue_report_discovery_policy(policy: &str) -> bool {
    role(policy) == Some(Role::Report)
}
pub fn dialogue_result_role(policy: &str) -> bool {
    matches!(
        role(policy),
        Some(Role::Report | Role::Task | Role::DeviceTask)
    )
}
pub fn private_delivery_tool(policy: &str) -> Option<&'static str> {
    matches!(role(policy), Some(Role::Task | Role::DeviceTask)).then_some("tasks.submit_result@1")
}
pub fn dialogue_policy_uses_native_publication(policy: &str) -> bool {
    is_dialogue_family_discovery_policy(policy) && private_delivery_tool(policy).is_none()
}
pub fn dialogue_publication_tool_refs(_policy: &str) -> [&'static str; 2] {
    DIALOGUE_PUBLICATION_TOOL_REFS
}
pub fn dialogue_foundation_policy(policy: &str) -> &str {
    match role(policy) {
        Some(Role::DeviceTask) => DIALOGUE_DEVICE_DISCOVERY_POLICY,
        Some(Role::Task | Role::Report) => DIALOGUE_DISCOVERY_POLICY,
        _ => policy,
    }
}
pub fn dialogue_task_result_discovery_policy(base: &str) -> Option<&'static str> {
    match role(base) {
        Some(Role::Dialogue) => Some(DIALOGUE_TASK_RESULT_DISCOVERY_POLICY),
        Some(Role::Device) => Some(DIALOGUE_DEVICE_TASK_RESULT_DISCOVERY_POLICY),
        _ => None,
    }
}
pub fn dialogue_device_discovery_policy_for_release(
    reference: &musteragent_identity::ProductReleaseRef,
) -> Option<&'static str> {
    if reference.product != musteragent_identity::ProductId::KERNEL {
        return None;
    }
    let metadata = musteragent_tool::product_release_metadata(reference).ok()?;
    (metadata.profile_id == "musteragent.kernel.v1"
        && metadata.cloud_head == "cloud.k1"
        && metadata.device_head == "device.k1"
        && metadata.skill_schema == "musteragent.runtime.skill-release.v1")
        .then_some(DIALOGUE_DEVICE_DISCOVERY_POLICY)
}
pub fn dialogue_device_discovery_policy_for_current_release() -> Option<&'static str> {
    dialogue_device_discovery_policy_for_release(&musteragent_tool::compiled_product_release(
        musteragent_identity::ProductId::KERNEL,
    ))
}
pub fn dialogue_report_discovery_policy_for_release(
    reference: &musteragent_identity::ProductReleaseRef,
    base: &str,
) -> Option<&'static str> {
    dialogue_device_discovery_policy_for_release(reference)?;
    (base == DIALOGUE_DISCOVERY_POLICY
        && musteragent_tool::verified_task_results_for_release(reference).ok()?)
    .then_some(DIALOGUE_REPORT_DISCOVERY_POLICY)
}
pub fn dialogue_report_discovery_policy(base: &str) -> Option<&'static str> {
    dialogue_report_discovery_policy_for_release(
        &musteragent_tool::compiled_product_release(musteragent_identity::ProductId::KERNEL),
        base,
    )
}
pub fn capability_available_for_dialogue_policy(policy: &str, key: &str) -> bool {
    if let Some(allowed) = cloud_delegation_discovery::allowed(policy, key)
        .or_else(|| cloud_schedule_discovery::allowed(policy, key))
    {
        return allowed;
    }
    role(policy).is_some_and(|role| rules::allows(role, key, false))
}
pub fn dialogue_capability_ceiling_for_policy(policy: &str, key: &str) -> bool {
    if matches!(
        policy,
        CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1
            | CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2
            | CLOUD_SCHEDULE_DISCOVERY_POLICY_V1
    ) {
        return capability_available_for_dialogue_policy(policy, key);
    }
    role(policy).is_some_and(|role| rules::allows(role, key, true))
}
pub fn dialogue_report_tool_available_for_policy(policy: &str, key: &str) -> bool {
    is_dialogue_report_discovery_policy(policy)
        && capability_available_for_dialogue_policy(policy, key)
}
pub fn dialogue_report_tool_available(key: &str) -> bool {
    dialogue_report_tool_available_for_policy(DIALOGUE_REPORT_DISCOVERY_POLICY, key)
}
pub fn dialogue_execution_directory_tool_available(policy: &str, key: &str) -> bool {
    key.starts_with("tasks.executions.") && capability_available_for_dialogue_policy(policy, key)
}
pub fn dialogue_bootstrap_logical_keys(files: bool) -> Vec<&'static str> {
    let mut keys = vec![
        "tool.catalog.search",
        "tool.catalog.activate",
        "skills.search",
        "skills.list",
        "skills.view",
        "plan.get",
        "plan.update",
        "context.read",
        "delegation.inspect",
    ];
    if files {
        keys.extend(["files.list", "files.inspect", "files.read"]);
    }
    keys
}
pub fn dialogue_bootstrap_logical_keys_for_policy(policy: &str, files: bool) -> Vec<&'static str> {
    if !is_dialogue_family_discovery_policy(policy) {
        return Vec::new();
    }
    let mut keys = dialogue_bootstrap_logical_keys(files);
    if is_dialogue_report_discovery_policy(policy) {
        keys.retain(|key| *key != "delegation.inspect");
        keys.push("tasks.feedback");
    }
    if private_delivery_tool(policy).is_some() {
        keys.push("tasks.submit_result");
    } else {
        keys.extend(["dialogue.publish", "artifacts.deliver"]);
    }
    keys
}
pub fn dialogue_initial_skill_disclosures_for_policy(
    policy: &str,
    skills: &[SkillDefinition],
    selected: &[VersionedKey],
) -> Result<Vec<VersionedKey>, SnapshotContractError> {
    if !is_dialogue_family_discovery_policy(policy) || !skills_are_authorized(skills, selected) {
        return Err(SnapshotContractError::InvalidDiscovery);
    }
    // Skill contents and required capabilities come from the sealed host catalog.
    // The kernel no longer selects a product's archived conversation persona.
    let mut initial = canonical_initial_skill_disclosures(skills, selected)
        .ok_or(SnapshotContractError::InvalidDiscovery)?;
    initial.dedup();
    Ok(initial)
}
impl RuntimeSnapshotContent {
    /// Signed device placement is required even when the conversation runs in cloud.
    pub fn origin_tool_in_cloud(&self, key: &str) -> bool {
        self.product_release
            .as_ref()
            .is_some_and(|r| r.product == musteragent_identity::ProductId::KERNEL)
            && musteragent_tool::origin_device_tool(key)
            && self.owner_goal_authority.is_none()
            && self
                .capabilities
                .iter()
                .any(|c| c.key.as_str() == key && c.location == CapabilityLocation::Device)
            && self.capabilities.iter().any(|c| {
                c.key.as_str() == "dialogue.publish@1" && c.location == CapabilityLocation::Cloud
            })
    }
    pub(super) fn validate_dialogue_discovery_boundary(
        &self,
        discovery: &DiscoverySnapshot,
    ) -> Result<(), SnapshotContractError> {
        let policy = discovery.discovery_policy_ref.as_str();
        let invalid = || SnapshotContractError::InvalidDiscovery;
        let release = self.product_release.as_ref().ok_or_else(invalid)?;
        if !is_dialogue_family_discovery_policy(policy)
            || !musteragent_tool::verified_task_results_for_release(release)
                .map_err(|_| invalid())?
            || (self.owner_goal_authority.is_some() && private_delivery_tool(policy).is_some())
        {
            return Err(invalid());
        }
        let location = if is_dialogue_device_discovery_policy(policy) {
            CapabilityLocation::Device
        } else {
            CapabilityLocation::Cloud
        };
        if self.capabilities.iter().any(|c| {
            !self.origin_tool_in_cloud(c.key.as_str())
                && (c.location != location
                    || !dialogue_capability_ceiling_for_policy(policy, c.key.as_str()))
        }) {
            return Err(invalid());
        }
        let files = self
            .tools
            .iter()
            .any(|t| t.manifest.key.logical_key() == "files.list");
        if !discovery
            .bootstrap_tool_keys
            .iter()
            .map(ToolKey::logical_key)
            .eq(dialogue_bootstrap_logical_keys_for_policy(policy, files))
        {
            return Err(invalid());
        }
        let mandatory = if private_delivery_tool(policy).is_some() {
            &["tasks.submit_result@1"][..]
        } else {
            &DIALOGUE_PUBLICATION_TOOL_REFS[..]
        };
        for key in mandatory {
            if !self.tools.iter().any(|t| t.manifest.key.as_str() == *key)
                || !self
                    .capabilities
                    .iter()
                    .any(|c| c.key.as_str() == *key && c.location == location)
                || !discovery
                    .bootstrap_tool_keys
                    .iter()
                    .any(|k| k.as_str() == *key)
                || !discovery
                    .initial_exposed_tool_keys
                    .iter()
                    .any(|k| k.as_str() == *key)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
