//! Neutral source qualification for persistence admission. Prompts are host-owned;
//! frozen source commitments and configured instruction references are kernel facts.
use crate::{
    AgentAdmissionIdentity, DialogueExecutionPreparation, OwnerGoalAdmissionPhase,
    OwnerGoalExecutionPreparation, WorkItemExecutionPreparation,
};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_definition::owned_agent::OwnedAgentConfiguration;
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_snapshot::{AgentVersionSnapshot, RuntimeSnapshotContent};

/// Section containing the 32 raw bytes of the frozen execution-source digest.
/// Hosts store these bytes as Content and include the exact reference in the
/// sealed snapshot. This is a commitment, never a natural-language authority.
pub const EXECUTION_SOURCE_SECTION: &str = "kernel.execution-source";

fn identity(
    snapshot: &RuntimeSnapshotContent,
    source: &AgentAdmissionIdentity,
    configuration: &OwnedAgentConfiguration,
) -> Result<(), ErrorCode> {
    let invalid = ErrorCode::RuntimeSnapshotInvalid;
    snapshot.validate_sealed().map_err(|_| invalid)?;
    let agent = AgentVersionSnapshot::from_resolved(&source.definition, &source.version)
        .map_err(|_| invalid)?;
    if snapshot.agent != agent {
        return Err(invalid);
    }
    let configured = snapshot
        .prompt
        .sections
        .iter()
        .filter(|s| s.section_key.as_str() == "agent.instructions")
        .map(|s| &s.content_ref)
        .collect::<Vec<_>>();
    if configured.as_slice()
        != configuration
            .instructions_ref
            .as_ref()
            .into_iter()
            .collect::<Vec<&ContentRef>>()
            .as_slice()
    {
        return Err(invalid);
    }
    if snapshot
        .tools
        .iter()
        .any(|t| t.manifest.key.logical_key() == "agents.create")
    {
        return Err(invalid);
    }
    Ok(())
}
fn commitment(snapshot: &RuntimeSnapshotContent, hash: Sha256) -> Result<(), ErrorCode> {
    let pins = snapshot
        .prompt
        .sections
        .iter()
        .filter(|s| s.section_key.as_str() == EXECUTION_SOURCE_SECTION)
        .collect::<Vec<_>>();
    let [pin] = pins.as_slice() else {
        return Err(ErrorCode::RuntimeSnapshotInvalid);
    };
    if pin.content_ref.byte_length != 32
        || pin.content_ref.sha256 != Sha256::digest(hash.into_bytes())
    {
        return Err(ErrorCode::RuntimeSnapshotInvalid);
    }
    Ok(())
}
/// Compare the exact published Agent, source commitment and configured instructions.
pub fn validate_dialogue_snapshot_source(
    snapshot: &RuntimeSnapshotContent,
    source: &DialogueExecutionPreparation,
) -> Result<(), ErrorCode> {
    source
        .validate()
        .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
    identity(snapshot, &source.assistant_identity, &source.configuration)?;
    if snapshot.owner_goal_authority.is_some()
        // Ordinary inputs have no frozen publication; their authenticated Grant
        // selects it at admission. Scheduled inputs pin a publication explicitly.
        || source.product_release.as_ref().is_some_and(|expected| {
            snapshot.product_release.as_ref() != Some(expected)
        })
    {
        return Err(ErrorCode::RuntimeSnapshotInvalid);
    }
    commitment(snapshot, crate::dialogue_execution_source_hash(source)?)
}
/// Task revisions, input custody and delegation are bound by the source digest.
pub fn validate_work_item_snapshot_source(
    snapshot: &RuntimeSnapshotContent,
    source: &WorkItemExecutionPreparation,
) -> Result<(), ErrorCode> {
    source
        .validate()
        .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
    identity(snapshot, &source.assistant_identity, &source.configuration)?;
    if snapshot.owner_goal_authority.is_some()
        || snapshot.product_release != source.delegation.product_release
        || snapshot
            .tools
            .iter()
            .any(|t| t.manifest.key.logical_key() == "tasks.handoff")
    {
        return Err(ErrorCode::RuntimeSnapshotInvalid);
    }
    commitment(
        snapshot,
        source
            .source_sha256()
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?,
    )
}
/// A Goal has a typed sealed authority, independently checked against current state.
pub fn validate_owner_goal_snapshot_source(
    snapshot: &RuntimeSnapshotContent,
    source: &OwnerGoalExecutionPreparation,
) -> Result<(), ErrorCode> {
    source
        .validate()
        .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
    identity(snapshot, &source.assistant_identity, &source.configuration)?;
    if snapshot.owner_goal_authority.as_ref() != Some(&source.snapshot_authority())
        || snapshot.product_release.as_ref() != Some(&source.product_release)
        || snapshot
            .tools
            .iter()
            .any(|t| t.manifest.key.logical_key() == "tasks.handoff")
    {
        return Err(ErrorCode::RuntimeSnapshotInvalid);
    }
    if matches!(source.phase, OwnerGoalAdmissionPhase::Evaluation { .. })
        && !snapshot.uses_closed_evaluation_profile()
    {
        return Err(ErrorCode::RuntimeSnapshotInvalid);
    }
    Ok(())
}
