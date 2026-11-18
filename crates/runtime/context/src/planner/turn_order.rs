//! Presentation order for serialized root Chat turns and consumed Run-control
//! stimuli.

use std::collections::BTreeMap;

use musteragent_activity::ActivityKind;
use musteragent_identity::{ConversationSeq, LedgerRecordId, RunId};
use musteragent_runtime_model::CanonicalRole;

use crate::{ConsumedStimulusCut, ContextEntry, ContextMaterial};

/// Restores execution-turn order without rewriting immutable Ledger order.
///
/// Root Chat Runs are serialized, but several user turns may be durably
/// admitted before the predecessor Run commits its answer. The target Run's
/// admission stimulus must therefore be presented after predecessor suffixes
/// and before its own model/tool suffix. Other activity kinds retain exact
/// Ledger presentation order for their admission.
///
/// Independently of the admission case, a typed Run-control stimulus
/// (follow-up or steering) may be durably admitted while an older model
/// invocation is still in flight and then consumed only after that invocation
/// committed its reply. The immutable Ledger therefore orders the stimulus
/// *before* the old reply, while causally it belongs to the next logical turn.
/// The durable `ControlConsumed` cut anchors the stimulus' presentation to its
/// real consumption boundary so the next model request does not end in
/// Assistant history. Consumption is confirmed only by a successful
/// submission; an unconsumed control message never reaches the material, and a
/// consumed one survives retries and replanning because the cut is reloaded
/// from the Ledger every iteration.
pub(super) fn order_entries_for_run_turn(material: &ContextMaterial) -> Vec<&ContextEntry> {
    let ordered = if material.run_context.activity_kind != ActivityKind::Chat
        || (material.run_context.parent_run_id.is_some()
            && material.retry_stimulus_record_id.is_none())
    {
        material.entries.iter().collect()
    } else {
        order_serial_chat_turn(
            &material.entries,
            material.run_context.run_id,
            material.admitted_sequence,
            material.retry_stimulus_record_id,
        )
    };
    let mut ordered = order_consumed_stimuli(ordered, &material.consumed_stimulus_cuts);
    // Importing an earlier dialogue page appends a ledger record to the current Run.
    // Select, compact and present that page at its original dialogue position.
    ordered.sort_by_key(|entry| {
        entry
            .history_page
            .as_ref()
            .map_or((1, 0), |page| (0, page.from_seq))
    });
    ordered
}

/// Presents each consumed control stimulus at its durable consumption cut.
///
/// Only the Run's own consumed follow-up/steering messages carry a cut, so
/// sibling Runs and other sources keep their exact positions. Relative order
/// between multiple stimuli follows their consumption cuts (and stable Ledger
/// order within one cut); everything durable before a cut, including the reply
/// of the invocation the stimulus interrupted, stays ahead of it.
pub(super) fn order_consumed_stimuli<'entry>(
    entries: Vec<&'entry ContextEntry>,
    cuts: &[ConsumedStimulusCut],
) -> Vec<&'entry ContextEntry> {
    if cuts.is_empty() {
        return entries;
    }
    let cut_by_record = cuts
        .iter()
        .map(|cut| (cut.stimulus_record_id, cut.consumed_sequence))
        .collect::<BTreeMap<_, _>>();
    let mut positioned = Vec::with_capacity(entries.len());
    let mut deferred = Vec::new();
    for entry in entries {
        let cut = (entry.message.role == CanonicalRole::User)
            .then(|| {
                entry
                    .source_record_ids
                    .iter()
                    .find_map(|record_id| cut_by_record.get(record_id).copied())
            })
            .flatten();
        match cut {
            // The stimulus is anchored at its consumption boundary, not at its
            // earlier durable admission position.
            Some(cut) => deferred.push((cut, entry)),
            None => positioned.push((entry.source_sequence, entry)),
        }
    }
    for (cut, entry) in deferred {
        // Insert after the last entry anchored at or before the cut. No kept
        // entry can share the cut's exact sequence (the consumption record is
        // not a message entry), so `<=` only ties with stimuli consumed by the
        // same fence, preserving their stable Ledger order.
        let insertion = positioned
            .iter()
            .rposition(|(anchor, _)| *anchor <= cut)
            .map_or(0, |index| index + 1);
        positioned.insert(insertion, (cut, entry));
    }
    positioned.into_iter().map(|(_, entry)| entry).collect()
}

pub(super) fn order_serial_chat_turn(
    entries: &[ContextEntry],
    target_run_id: RunId,
    admitted_sequence: ConversationSeq,
    retry_stimulus_record_id: Option<LedgerRecordId>,
) -> Vec<&ContextEntry> {
    let mut stimuli = Vec::new();
    let mut ordered = Vec::with_capacity(entries.len());
    for entry in entries {
        if (entry.source_run_id == Some(target_run_id)
            || retry_stimulus_record_id.is_some_and(|id| entry.source_record_ids.contains(&id)))
            && entry.source_sequence <= admitted_sequence
            && entry.message.role == CanonicalRole::User
        {
            stimuli.push(entry);
        } else {
            ordered.push(entry);
        }
    }
    if stimuli.is_empty() {
        return ordered;
    }
    let insertion = ordered
        .iter()
        .position(|entry| {
            entry.source_run_id == Some(target_run_id) && entry.source_sequence > admitted_sequence
        })
        .unwrap_or(ordered.len());
    ordered.splice(insertion..insertion, stimuli);
    ordered
}
