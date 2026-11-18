use super::*;
pub(in crate::agent_context_codec) fn decode(
    v: wire::AgentContextMaterial,
) -> Result<ContextMaterial> {
    let snapshot = required(v.runtime_snapshot)?.encode_to_vec();
    let material = ContextMaterial {
        derived_source_run_id: v
            .derived_source_run_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        superseding_control_id: v
            .superseding_control_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        resolved_tools: v
            .resolved_tools
            .into_iter()
            .map(|v| {
                Ok(ResolvedContextTool {
                    tool: checked(crate::snapshot_codec::decode_tool(required(v.tool)?))?,
                    model_name: checked(WireName::new(v.model_name))?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        desktop_source_host: v
            .desktop_source_host
            .map(|host| {
                let membership_id: uuid::Uuid = checked(host.membership_id.parse())?;
                if membership_id.is_nil() || host.membership_epoch > i64::MAX as u64 {
                    return Err(AgentContextCodecError::Invalid);
                }
                Ok(musteragent_context::DesktopSourceHost {
                    membership_id,
                    membership_epoch: positive64(host.membership_epoch)?,
                })
            })
            .transpose()?,
        dialogue_execution_context: checked(
            v.dialogue_execution_context
                .map(crate::dialogue_execution_context_codec::decode)
                .transpose(),
        )?,
        dialogue_approval_context: checked(
            v.dialogue_approval_context
                .map(crate::dialogue_approval_context_codec::decode)
                .transpose(),
        )?,
        agent_instructions: v
            .agent_instructions
            .map(|value| {
                Ok(AgentInstructionMaterial {
                    scene_members: checked(
                        value
                            .scene_members
                            .into_iter()
                            .map(crate::dialogue_context_codec::decode_scene_member)
                            .collect::<std::result::Result<_, _>>(),
                    )?,
                    agent_id: id(value.agent_id.map(|id| id.value))?,
                    revision: positive64(value.revision)?,
                    content: value.content.map(decode_content).transpose()?,
                })
            })
            .transpose()?,
        plan_selection: checked(
            v.plan_selection
                .map(crate::context_plan_lifecycle_codec::context_plan_selection_from_message)
                .transpose(),
        )?,
        model_recovery: v.model_recovery.map(|v| id(Some(v.value))).transpose()?,
        context_capacity_exceeded: v.context_capacity_exceeded,
        rejected_request_bytes: v.rejected_request_bytes,
        consumed_stimulus_cuts: v
            .consumed_stimulus_cuts
            .into_iter()
            .map(runtime_fields::decode_cut)
            .collect::<Result<_>>()?,
        measured_usage: v
            .measured_usage
            .map(|v| {
                Ok(MeasuredContextUsage {
                    recovery_origin: v.recovery_origin.map(decode_origin).transpose()?,
                    source_sequence: sequence(v.source_sequence)?,
                    input_tokens: v.input_tokens,
                })
            })
            .transpose()?,
        retry_stimulus_record_id: v
            .retry_stimulus_record_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        source_content_refs: v
            .source_content_refs
            .into_iter()
            .map(content)
            .collect::<Result<_>>()?,
        run_context: checked(metadata::decode_run(&v.canonical_run_metadata))?,
        admitted_sequence: sequence(v.admitted_sequence)?,
        runtime_snapshot: checked(crate::ProtobufSnapshotCodec.decode_content(&snapshot))?,
        tool_policy: match v.tool_policy {
            1 => ContextToolPolicy::SnapshotDiscovery,
            2 => ContextToolPolicy::DenyAll,
            _ => return Err(AgentContextCodecError::Invalid),
        },
        exposed_tool_keys: v
            .exposed_tool_keys
            .into_iter()
            .map(|v| checked(VersionedKey::new(v)).map(ToolKey::from_versioned_key))
            .collect::<Result<_>>()?,
        prompt_sections: v
            .prompt_sections
            .into_iter()
            .map(decode_content)
            .collect::<Result<_>>()?,
        disclosed_skills: v
            .disclosed_skills
            .into_iter()
            .map(|v| {
                Ok(MaterializedSkill {
                    skill_ref: checked(VersionedKey::new(v.skill_ref))?,
                    skill_version_id: id(Some(v.skill_version_id))?,
                    instructions_hash: hash(required(v.instructions_hash)?)?,
                    disclosure_ordinal: positive(v.disclosure_ordinal)?,
                    instructions: decode_content(required(v.instructions)?)?,
                })
            })
            .collect::<Result<_>>()?,
        memories: v
            .memories
            .into_iter()
            .map(|v| {
                Ok(MaterializedMemory {
                    memory_version_id: id(Some(v.memory_version_id))?,
                    content: decode_content(required(v.content)?)?,
                })
            })
            .collect::<Result<_>>()?,
        instincts: v
            .instincts
            .into_iter()
            .map(|v| {
                Ok(MaterializedInstinct {
                    instinct_id: id(Some(v.instinct_id))?,
                    revision: checked(VersionNumber::new(v.revision))?,
                    payload: decode_content(required(v.payload)?)?,
                })
            })
            .collect::<Result<_>>()?,
        execution_plan: v
            .execution_plan
            .map(|v| {
                Ok(MaterializedExecutionPlan {
                    revision: positive64(v.revision)?,
                    content: decode_content(required(v.content)?)?,
                })
            })
            .transpose()?,
        delegation_completions: v
            .delegation_completions
            .into_iter()
            .map(|v| {
                Ok(MaterializedDelegationCompletion {
                    delivery_id: id(Some(v.delivery_id))?,
                    delegation_id: id(Some(v.delegation_id))?,
                    child_activity_id: id(v.child_activity_id.map(|v| v.value))?,
                    child_run_id: id(v.child_run_id.map(|v| v.value))?,
                    report: decode_content(required(v.report)?)?,
                })
            })
            .collect::<Result<_>>()?,
        model_action_rejection: v.model_action_rejection.map(decode_rejection).transpose()?,
        entries: v
            .entries
            .into_iter()
            .map(decode_entry)
            .collect::<Result<_>>()?,
        next_assistant_response_ordinal: positive(v.next_assistant_response_ordinal)?,
        usage_anchor: v
            .usage_anchor
            .map(|v| {
                Ok(ContextUsageAnchor {
                    source_sequence: sequence(v.source_sequence)?,
                    total_tokens: v.total_tokens,
                })
            })
            .transpose()?,
        previous_context: v
            .previous_context
            .map(|v| {
                Ok(MaterializedContextBaseline {
                    snapshot_id: id(v.snapshot_id.map(|v| v.value))?,
                    source_seq_to: sequence(v.source_seq_to)?,
                    retained_record_ids: v
                        .retained_record_ids
                        .into_iter()
                        .map(|v| id(Some(v.value)))
                        .collect::<Result<_>>()?,
                    summary: decode_content(required(v.summary)?)?,
                })
            })
            .transpose()?,
        active_branch_id: if v.active_branch_id.is_empty() {
            None
        } else {
            Some(id(Some(v.active_branch_id))?)
        },
        ledger_cutoff: sequence(v.ledger_cutoff)?,
        source_seq_from: sequence(v.source_seq_from)?,
        source_seq_to: sequence(v.source_seq_to)?,
    };
    if material.source_seq_from > material.source_seq_to
        || material.source_seq_to > material.ledger_cutoff
        || material.admitted_sequence > material.ledger_cutoff
    {
        return Err(AgentContextCodecError::Invalid);
    }
    checked(material.runtime_compaction_source())?;
    Ok(material)
}
