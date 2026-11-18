//! Summary provenance is part of context isolation. Cache reuse is permitted
//! only after the source selector and current branch have selected its inputs.
use std::collections::BTreeSet;

use musteragent_runtime_snapshot::ContextSourceSelectionPolicy;

use crate::{
    CONTEXT_SUMMARY_SCHEMA, ContextMaterial, ContextSummaryDocument,
    ISOLATED_CONTEXT_SUMMARY_SCHEMA,
};

pub(super) fn summary_schema(material: &ContextMaterial) -> &'static str {
    if musteragent_runtime_snapshot::uses_native_history(
        material
            .runtime_snapshot
            .context_policy
            .compaction_policy_ref
            .as_str(),
    ) {
        return crate::NATIVE_CONTEXT_SUMMARY_SCHEMA;
    }
    match material
        .runtime_snapshot
        .context_policy
        .effective_source_selection()
    {
        ContextSourceSelectionPolicy::LegacyAdmissionV1 => CONTEXT_SUMMARY_SCHEMA,
        ContextSourceSelectionPolicy::ExecutionIsolatedV1 => ISOLATED_CONTEXT_SUMMARY_SCHEMA,
    }
}

pub(super) fn can_reuse(
    material: &ContextMaterial,
    summary: Option<&ContextSummaryDocument>,
) -> bool {
    // An older summary may have absorbed the original assignment or a real
    // approval observation. Rebuild from authorized source instead of treating
    // that model-generated text as an equivalent lifecycle fact.
    if let Some(summary) = summary {
        let summarized = summary
            .summarized_record_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if material.entries.iter().any(|entry| {
            super::authority::protected(material, entry)
                && entry
                    .source_record_ids
                    .iter()
                    .any(|id| summarized.contains(id))
        }) {
            return false;
        }
    }
    if summary_schema(material) == CONTEXT_SUMMARY_SCHEMA {
        return true;
    }
    let Some(summary) = summary else { return true };
    if summary.schema_version != summary_schema(material) {
        return false;
    }
    let visible = material
        .entries
        .iter()
        .flat_map(|entry| entry.source_record_ids.iter().copied())
        .collect::<BTreeSet<_>>();
    // Check the complete provenance, not the bounded display excerpts. This
    // rejects a sibling/child checkpoint and an ancestor invalidated by edits.
    material.previous_context.as_ref().is_some_and(|baseline| {
        provenance_visible(
            &visible,
            &summary.summarized_record_ids,
            &baseline.retained_record_ids,
        )
    })
}

fn provenance_visible(
    visible: &BTreeSet<musteragent_identity::LedgerRecordId>,
    summarized: &[musteragent_identity::LedgerRecordId],
    retained: &[musteragent_identity::LedgerRecordId],
) -> bool {
    !summarized.is_empty()
        && !retained.is_empty()
        && summarized
            .iter()
            .chain(retained)
            .all(|id| visible.contains(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::LedgerRecordId;

    #[test]
    fn checkpoint_reuse_requires_every_source_not_only_recent_excerpts() {
        let old_root = LedgerRecordId::generate();
        let recent_root = LedgerRecordId::generate();
        let child = LedgerRecordId::generate();
        let selected = BTreeSet::from([old_root, recent_root]);
        assert!(provenance_visible(&selected, &[old_root], &[recent_root]));
        assert!(!provenance_visible(
            &selected,
            &[old_root, child],
            &[recent_root]
        ));
        assert!(!provenance_visible(&selected, &[old_root], &[child]));
        // A branch edit/revocation invalidates the earlier checkpoint even
        // when its retained suffix remains visible.
        assert!(!provenance_visible(
            &BTreeSet::from([recent_root]),
            &[old_root],
            &[recent_root]
        ));
        assert!(!provenance_visible(&selected, &[], &[recent_root]));
    }
}
