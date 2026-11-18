//! Exact assignment and lifecycle facts remain outside lossy narrative history.
//! The source Owner already selected the authorized Ledger cut. This projection
//! does not infer a new assignment or approval from public text or a summary.
use super::*;
pub(super) mod approval;
pub(super) mod execution;
pub(super) mod reply;
use musteragent_runtime_model::{CanonicalRole, CanonicalRuntimeEvent};

pub(super) fn protected(material: &ContextMaterial, entry: &ContextEntry) -> bool {
    entry
        .runtime_events
        .iter()
        .any(|event| matches!(event, CanonicalRuntimeEvent::ToolApprovalResolved(_)))
        || assigned_input(material, entry)
}

fn assigned_input(material: &ContextMaterial, entry: &ContextEntry) -> bool {
    assigned(
        entry,
        material.run_context.run_id,
        material.admitted_sequence,
        material.retry_stimulus_record_id,
        &material.consumed_stimulus_cuts,
    )
}

fn assigned(
    entry: &ContextEntry,
    run: musteragent_identity::RunId,
    admitted: musteragent_identity::ConversationSeq,
    retry: Option<musteragent_identity::LedgerRecordId>,
    controls: &[crate::ConsumedStimulusCut],
) -> bool {
    // A Dialogue history page has a User sizing view, but is not an accepted
    // input. Only real source messages with the original admission/control
    // binding can define what this Run is responsible for.
    if entry.history_page.is_some()
        || !entry.runtime_events.is_empty()
        || entry.message.role != CanonicalRole::User
    {
        return false;
    }
    ((entry.source_run_id == Some(run)
        || retry.is_some_and(|id| entry.source_record_ids.contains(&id)))
        && entry.source_sequence <= admitted)
        || controls
            .iter()
            .any(|cut| entry.source_record_ids.contains(&cut.stimulus_record_id))
}

/// Bind an explicit selection to the original accepted input. A later steering
/// message must not inherit a reply target it did not select.
pub(super) fn bind_selected_reply(material: &mut ContextMaterial) -> Result<(), ContextPlanError> {
    let Some(binding) = material
        .dialogue_execution_context
        .as_ref()
        .and_then(|v| v.reply_binding.as_ref())
    else {
        return Ok(());
    };
    let mut roots = material.entries.iter_mut().filter(|entry| {
        assigned(
            entry,
            material.run_context.run_id,
            material.admitted_sequence,
            material.retry_stimulus_record_id,
            &[],
        )
    });
    let root = roots.next().ok_or_else(ContextPlanError::invariant)?;
    if roots.next().is_some() {
        return Err(ContextPlanError::invariant());
    }
    root.message
        .content
        .insert(0, CanonicalContentPart::Text(reply::material(binding)?));
    Ok(())
}

pub(super) fn retain<'a>(
    material: &ContextMaterial,
    logical: &[&'a ContextEntry],
    retained: Vec<&'a ContextEntry>,
    compacted: Vec<&'a ContextEntry>,
) -> Result<(Vec<&'a ContextEntry>, Vec<&'a ContextEntry>), ContextPlanError> {
    retain_units(logical, retained, compacted, |entry| {
        protected(material, entry)
    })
}

fn retain_units<'a>(
    logical: &[&'a ContextEntry],
    retained: Vec<&'a ContextEntry>,
    compacted: Vec<&'a ContextEntry>,
    protected: impl Fn(&ContextEntry) -> bool,
) -> Result<(Vec<&'a ContextEntry>, Vec<&'a ContextEntry>), ContextPlanError> {
    if compacted.is_empty() {
        return Ok((retained, compacted));
    }
    let mut keep = retained
        .iter()
        .map(|entry| entry.source_sequence)
        .collect::<BTreeSet<_>>();
    for unit in atomic_context_units(logical, false)? {
        if logical[unit.start..unit.end]
            .iter()
            .any(|entry| protected(entry))
        {
            keep.extend(
                logical[unit.start..unit.end]
                    .iter()
                    .map(|e| e.source_sequence),
            );
        }
    }
    Ok(logical
        .iter()
        .copied()
        .partition(|entry| keep.contains(&entry.source_sequence)))
}

#[cfg(test)]
mod tests;
