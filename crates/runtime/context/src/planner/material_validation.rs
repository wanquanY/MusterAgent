use std::collections::BTreeSet;

use musteragent_runtime_content::verify_content;

use crate::{
    ContextMaterial, ContextPlanError, ContextPlanInput, ContextToolPolicy, MaterializedContent,
};

pub(super) fn validate_material(
    input: &ContextPlanInput,
    material: &ContextMaterial,
) -> Result<(), ContextPlanError> {
    material.runtime_compaction_source()?;
    crate::resolved_tools::validate(material)?;
    material
        .runtime_snapshot
        .validate_sealed()
        .map_err(|_| ContextPlanError::invariant())?;
    material
        .runtime_snapshot
        .validate_model_invocation_timing(&input.invocation_timing)
        .map_err(|_| ContextPlanError::invariant())?;
    if material.run_context.run_id != input.attempt.run_id
        || material.source_seq_from > material.source_seq_to
        || material.source_seq_to > material.ledger_cutoff
        || material.admitted_sequence > material.ledger_cutoff
        || material.entries.is_empty()
        || material.prompt_sections.len() != material.runtime_snapshot.prompt.sections.len()
        || material.memories.len() != material.runtime_snapshot.memory.selected.len()
        || material.instincts.len() != material.runtime_snapshot.instincts.len()
        || (material.tool_policy == ContextToolPolicy::DenyAll
            && !material.exposed_tool_keys.is_empty())
    {
        return Err(ContextPlanError::invariant());
    }
    if let Some(record_id) = material.retry_stimulus_record_id
        && (material.run_context.activity_kind != musteragent_activity::ActivityKind::Chat
            || material.run_context.parent_run_id.is_none()
            || material
                .entries
                .iter()
                .filter(|entry| {
                    entry.source_record_ids.contains(&record_id)
                        && entry.source_sequence <= material.admitted_sequence
                        && entry.message.role == musteragent_runtime_model::CanonicalRole::User
                })
                .count()
                != 1)
    {
        return Err(ContextPlanError::invariant());
    }
    if let Some(measured) = &material.measured_usage
        && let Some(origin) = &measured.recovery_origin
        && (material.model_recovery != Some(origin.invocation_id)
            || measured.source_sequence <= material.admitted_sequence
            || measured.source_sequence > material.ledger_cutoff)
    {
        return Err(ContextPlanError::invariant());
    }
    for (resolved, pinned) in material
        .prompt_sections
        .iter()
        .zip(&material.runtime_snapshot.prompt.sections)
    {
        validate_content(resolved, &pinned.content_ref)?;
    }
    for (resolved, pinned) in material
        .instincts
        .iter()
        .zip(&material.runtime_snapshot.instincts)
    {
        if resolved.instinct_id != pinned.instinct_id || resolved.revision != pinned.revision {
            return Err(ContextPlanError::invariant());
        }
        validate_content(&resolved.payload, &pinned.payload_ref)?;
    }
    if let Some(instructions) = &material.agent_instructions {
        if !musteragent_execution_context::valid_scene_members(
            instructions.agent_id,
            &instructions.scene_members,
        ) {
            return Err(ContextPlanError::invariant());
        }
        if !material
            .runtime_snapshot
            .prompt
            .sections
            .iter()
            .any(|section| section.section_key.as_str() == "dialogue.output-contract")
        {
            return Err(ContextPlanError::invariant());
        }
        if let Some(content) = &instructions.content {
            validate_content(content, &content.content_ref)?;
        }
    }
    validate_disclosed_skills(material)?;
    for (resolved, pinned) in material
        .memories
        .iter()
        .zip(&material.runtime_snapshot.memory.selected)
    {
        if resolved.memory_version_id != pinned.memory_version_id {
            return Err(ContextPlanError::invariant());
        }
        verify_content(&resolved.content.content_ref, &resolved.content.bytes)
            .map_err(|_| ContextPlanError::invariant())?;
    }
    if let Some(plan) = &material.execution_plan {
        verify_content(&plan.content.content_ref, &plan.content.bytes)
            .map_err(|_| ContextPlanError::invariant())?;
    }
    let mut records = BTreeSet::new();
    for entry in &material.entries {
        entry.validate_messages()?;
        if entry.source_sequence < material.source_seq_from
            || entry.source_sequence > material.source_seq_to
            || entry.source_record_ids.is_empty()
            || entry
                .source_record_ids
                .iter()
                .any(|record_id| !records.insert(*record_id))
        {
            return Err(ContextPlanError::invariant());
        }
    }
    Ok(())
}

fn validate_content(
    resolved: &MaterializedContent,
    pinned: &musteragent_content_ref::ContentRef,
) -> Result<(), ContextPlanError> {
    if &resolved.content_ref != pinned {
        return Err(ContextPlanError::invariant());
    }
    verify_content(&resolved.content_ref, &resolved.bytes)
        .map_err(|_| ContextPlanError::invariant())
}

fn validate_disclosed_skills(material: &ContextMaterial) -> Result<(), ContextPlanError> {
    let mut seen = BTreeSet::new();
    for (index, resolved) in material.disclosed_skills.iter().enumerate() {
        let pinned = material
            .runtime_snapshot
            .skills
            .iter()
            .find(|skill| skill.skill_ref == resolved.skill_ref)
            .ok_or_else(ContextPlanError::invariant)?;
        let expected_ordinal = u32::try_from(index.saturating_add(1))
            .ok()
            .and_then(std::num::NonZeroU32::new)
            .ok_or_else(ContextPlanError::invariant)?;
        if resolved.skill_version_id != pinned.skill_version_id
            || resolved.instructions_hash != pinned.instructions_hash
            || resolved.disclosure_ordinal != expected_ordinal
            || !seen.insert(resolved.skill_ref.clone())
        {
            return Err(ContextPlanError::invariant());
        }
        validate_content(&resolved.instructions, &pinned.instructions_ref)?;
    }
    if let Some(discovery) = &material.runtime_snapshot.discovery {
        let prefix = material
            .disclosed_skills
            .iter()
            .take(discovery.initial_disclosed_skill_refs.len())
            .map(|skill| &skill.skill_ref)
            .collect::<Vec<_>>();
        if prefix
            != discovery
                .initial_disclosed_skill_refs
                .iter()
                .collect::<Vec<_>>()
        {
            return Err(ContextPlanError::invariant());
        }
    } else if material.disclosed_skills.len() != material.runtime_snapshot.skills.len()
        || material
            .disclosed_skills
            .iter()
            .zip(&material.runtime_snapshot.skills)
            .any(|(resolved, pinned)| resolved.skill_ref != pinned.skill_ref)
    {
        return Err(ContextPlanError::invariant());
    }
    Ok(())
}
