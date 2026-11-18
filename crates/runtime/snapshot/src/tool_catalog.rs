use super::*;

pub fn authorized_tool_catalog(
    tools: &[ToolDefinitionSnapshot],
    grants: &[CapabilityGrantSnapshot],
) -> Result<AuthorizedToolCatalog, SnapshotContractError> {
    let descriptors = tools
        .iter()
        .map(|tool| {
            tool.validate()?;
            let mut matching = grants
                .iter()
                .filter(|grant| grant.key.as_str() == tool.manifest.capability_key.as_str());
            let grant = matching
                .next()
                .ok_or(SnapshotContractError::ToolCapabilityNotGranted)?;
            if matching.next().is_some() {
                return Err(SnapshotContractError::DuplicateCapability);
            }
            if grant.policy_decision == musteragent_policy::Decision::Deny
                || grant.allowed_scopes.is_empty()
            {
                return Err(SnapshotContractError::CapabilitiesNotCanonical);
            }
            if grant.provider_id != tool.manifest.capability_provider_id {
                return Err(SnapshotContractError::ToolCapabilityNotGranted);
            }
            if grant.effect != tool.manifest.effect {
                return Err(SnapshotContractError::ToolCapabilityEffectMismatch);
            }
            if !execution_target_accepts_location(&tool.execution_target, grant.location) {
                return Err(SnapshotContractError::ToolCapabilityLocationMismatch);
            }
            if !tool
                .manifest
                .required_scopes
                .is_subset(&grant.allowed_scopes)
            {
                return Err(SnapshotContractError::ToolCapabilityScopeMismatch);
            }
            Ok(AuthorizedToolDescriptor {
                provided_capability: tool
                    .manifest
                    .application
                    .as_ref()
                    .and_then(|value| value.provided_capability()),
                tool_ref: tool.manifest.key.clone(),
                capability_ref: tool.manifest.capability_key.clone(),
                wire_name: tool.manifest.wire_name.clone(),
                title: tool.title.clone().into(),
                description: tool.description.clone().into(),
                schema_hash: tool.manifest.input_schema.schema_hash,
                schema: tool.input_schema_json.clone().into(),
                provider_id: grant.provider_id,
                location: grant.location,
                required_contexts: tool.manifest.required_contexts.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    AuthorizedToolCatalog::new(descriptors).map_err(|_| SnapshotContractError::InvalidDiscovery)
}
