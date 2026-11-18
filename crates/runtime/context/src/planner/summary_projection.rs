//! Provenance-preserving structured summary projection.
use super::*;
use musteragent_identity::ConversationSeq;

pub(super) fn build_summary(
    material: &ContextMaterial,
    compacted: &[&ContextEntry],
    previous: Option<&ContextSummaryDocument>,
) -> Result<ContextSummaryDocument, ContextPlanError> {
    if compacted.is_empty() {
        return previous.cloned().map_or_else(
            || {
                Ok(ContextSummaryDocument {
                    schema_version: source_baseline::summary_schema(material),
                    compacted: false,
                    source_seq_from: material.source_seq_from,
                    source_seq_to: material.source_seq_to,
                    summary: None,
                    generator_invocation_id: None,
                    previous_summary_ref: None,
                    summarized_record_ids: Vec::new(),
                    file_operations: Default::default(),
                    items: Vec::new(),
                    omitted_item_count: 0,
                })
            },
            Ok,
        );
    }
    let (source_seq_from, source_seq_to) = coverage_range(compacted, previous)?;
    let delta_items = compacted
        .iter()
        .map(|entry| ContextSummaryItem {
            source_sequence: entry.source_sequence,
            source_record_ids: entry.source_record_ids.clone(),
            role: if entry.runtime_events.is_empty() {
                entry.message.role.into()
            } else {
                crate::ContextSummaryRole::Runtime
            },
            excerpt: message_excerpt(&entry.message),
        })
        .collect::<Vec<_>>();
    let mut items = previous
        .map(|summary| summary.items.clone())
        .unwrap_or_default();
    let available_items = MAX_SUMMARY_ITEMS.saturating_sub(items.len());
    items.extend(delta_items.iter().take(available_items).cloned());
    let mut summarized_record_ids = previous
        .map(|summary| summary.summarized_record_ids.clone())
        .unwrap_or_default();
    summarized_record_ids.extend(
        compacted
            .iter()
            .flat_map(|entry| entry.source_record_ids.iter().copied()),
    );
    if summarized_record_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
        != summarized_record_ids.len()
    {
        return Err(ContextPlanError::invariant());
    }
    Ok(ContextSummaryDocument {
        schema_version: source_baseline::summary_schema(material),
        compacted: true,
        source_seq_from,
        source_seq_to,
        summary: None,
        generator_invocation_id: None,
        previous_summary_ref: None,
        summarized_record_ids,
        file_operations: collect_file_operations(material, compacted, previous)?,
        items,
        omitted_item_count: previous
            .map_or(0, |summary| summary.omitted_item_count)
            .saturating_add(
                u64::try_from(delta_items.len().saturating_sub(available_items))
                    .unwrap_or(u64::MAX),
            ),
    })
}

/// Source sequence is a durable ledger coordinate. Dialogue imports are
/// presented before the current turn even if their ledger write happened later.
pub(super) fn coverage_range(
    compacted: &[&ContextEntry],
    previous: Option<&ContextSummaryDocument>,
) -> Result<(ConversationSeq, ConversationSeq), ContextPlanError> {
    let sequences = compacted.iter().map(|entry| entry.source_sequence);
    let low = sequences
        .clone()
        .chain(previous.map(|s| s.source_seq_from))
        .min()
        .ok_or_else(ContextPlanError::invariant)?;
    let high = sequences
        .chain(previous.map(|s| s.source_seq_to))
        .max()
        .ok_or_else(ContextPlanError::invariant)?;
    Ok((low, high))
}
