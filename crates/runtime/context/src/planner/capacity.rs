//! Recovery selection is structural. Local token estimates never authorize a
//! capacity retry under @7; @8 also admits measured-input threshold compaction.
use super::*;
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_model::CanonicalRole;

pub(super) fn select<'a>(
    entries: &[&'a ContextEntry],
) -> Result<(Vec<&'a ContextEntry>, Vec<&'a ContextEntry>), ContextPlanError> {
    let units = atomic_context_units(entries, false)?;
    // Keep the most recent complete exchange and progressively summarize the
    // older half. No call/result pair can straddle this boundary.
    let split = if units.len() > 1 {
        units[units.len().div_ceil(2)].start
    } else {
        0
    };
    Ok((entries[split..].to_vec(), entries[..split].to_vec()))
}

/// A soft trigger may compact closed history. Only hard capacity pressure
/// permits summarizing completed exchanges within the still-active turn.
pub(super) fn select_native<'a>(
    protected: impl Fn(&ContextEntry) -> bool,
    entries: &[&'a ContextEntry],
    recover: bool,
    required_savings: u64,
) -> Result<(Vec<&'a ContextEntry>, Vec<&'a ContextEntry>), ContextPlanError> {
    // Dialogue pages and host events have separate owners. Their placement
    // before the assignment cannot turn them into an old, compactable turn.
    let all = entries;
    let ledger: Vec<_> = all
        .iter()
        .copied()
        .filter(|entry| entry.history_page.is_none() && entry.runtime_events.is_empty())
        .collect();
    let entries = ledger.as_slice();
    let units = atomic_context_units(entries, false)?;
    let partition = |split: usize| {
        let compacted: BTreeSet<_> = entries[..split].iter().map(|e| e.source_sequence).collect();
        all.iter()
            .copied()
            .partition(|entry| !compacted.contains(&entry.source_sequence))
    };
    let eligible = |entry: &ContextEntry| !protected(entry);
    let active = units.iter().position(|unit| {
        entries[unit.start..unit.end]
            .iter()
            .any(|entry| !eligible(entry))
    });
    if let Some(start) = active.map(|index| units[index].start)
        && entries[..start].iter().any(|entry| eligible(entry))
    {
        let mut last_for_run = std::collections::BTreeMap::new();
        for (index, entry) in entries[..start].iter().enumerate() {
            if let Some(run) = entry.source_run_id {
                last_for_run.insert(run, index + 1);
            }
        }
        let mut source_tokens = 0u64;
        let mut complete_through = 0;
        let mut split = start;
        for unit in units.iter().take_while(|unit| unit.end <= start) {
            let group = &entries[unit.start..unit.end];
            source_tokens = source_tokens.saturating_add(estimate_entries(group));
            for entry in group {
                if let Some(end) = entry.source_run_id.and_then(|run| last_for_run.get(&run)) {
                    complete_through = complete_through.max(*end);
                }
            }
            let next_turn =
                unit.end == start || entries[unit.end].message.role == CanonicalRole::User;
            if next_turn
                && unit.end >= complete_through
                && enough_summary_savings(source_tokens, required_savings)
            {
                split = unit.end;
                break;
            }
        }
        return Ok(partition(split));
    }
    if !recover {
        return Ok(partition(0));
    }
    let Some(newest) = units.last() else {
        return Ok(partition(0));
    };
    let mut saved = 0;
    let mut split = newest.start;
    for unit in units.iter().take_while(|unit| unit.end <= newest.start) {
        let group = &entries[unit.start..unit.end];
        if group.iter().any(|entry| protected(entry)) {
            continue;
        }
        saved = u64::saturating_add(saved, estimate_entries(group));
        if enough_summary_savings(saved, required_savings) {
            split = unit.end;
            break;
        }
    }
    Ok(partition(split))
}

// The model-capacity policy targets a quarter of the selected source for its summary.
pub(super) fn enough_summary_savings(source_tokens: u64, required_savings: u64) -> bool {
    source_tokens.saturating_sub((source_tokens / 4).max(1)) >= required_savings.max(1)
}

pub(super) fn bounded_tool_entries(
    entries: &[&ContextEntry],
    can_read: bool,
) -> Result<Vec<ContextEntry>, ContextPlanError> {
    let mut output = entries
        .iter()
        .map(|entry| (*entry).clone())
        .collect::<Vec<_>>();
    for entry in &mut output {
        if entry.message.role != CanonicalRole::Tool || !entry.runtime_events.is_empty() {
            continue;
        }
        let mut bytes = 0usize;
        for part in &entry.message.content {
            if let CanonicalContentPart::Text(text) = part {
                bytes = bytes.saturating_add(text.as_str().len());
            }
        }
        if bytes <= 4096 {
            continue;
        }
        if !can_read {
            return Err(exceeded());
        }
        let record_id = entry
            .source_record_ids
            .last()
            .ok_or_else(ContextPlanError::invariant)?;
        let message=serde_json::to_string(&serde_json::json!({
            "schema_version":"musteragent.context.tool-result-reference.v1",
            "omitted_from_inline_context":true,
            "reason":"Provider context capacity recovery; the original tool outcome remains stored.",
            "record_id":record_id.to_string(),
            "instruction":"Use context_read with this record_id, offset 0 and max_bytes to inspect the original result. Follow next_offset for more. This reference is not a new execution or a success claim.",
        })).map_err(|_|ContextPlanError::invariant())?;
        let mut reference = Some(CanonicalContentPart::Text(
            ModelText::new(message).map_err(|_| ContextPlanError::invariant())?,
        ));
        // Offload only text. Image/file parts retain their original identity and
        // order; a text-size recovery must not silently discard other evidence.
        entry.message.content = std::mem::take(&mut entry.message.content)
            .into_iter()
            .filter_map(|part| match part {
                CanonicalContentPart::Text(_) => reference.take(),
                other => Some(other),
            })
            .collect();
        entry.continuation_delta_for = None;
    }
    Ok(output)
}

pub(super) fn exceeded() -> ContextPlanError {
    ContextPlanError {
        early_provider_cause: None,
        code: ErrorCode::ModelContextExceeded,
        retryable: false,
        retry_after_ms: None,
    }
}

/// No local delta, output tokens, or cumulative billing count enters this gate.
pub(super) fn measured_trigger(
    entries: &[&ContextEntry],
    measured: Option<&crate::MeasuredContextUsage>,
    route: &musteragent_model::ModelRoute,
    threshold: u64,
) -> Result<bool, ContextPlanError> {
    // Aggregated physical request costs cannot authorize context compaction.
    if route
        .capabilities
        .iter()
        .any(|flag| flag.as_str() == "usage_aggregates_requests")
    {
        return Ok(false);
    }
    let Some(measured) = measured else {
        return Ok(false);
    };
    if measured.input_tokens < threshold {
        return Ok(false);
    }
    if let Some(origin) = &measured.recovery_origin {
        let hash = musteragent_model::SignedModelRouteHash::digest(route)
            .map_err(|_| ContextPlanError::invariant())?;
        return Ok(origin.model_route_ref == route.route_ref
            && origin.route_identity_hash == hash
            && origin.requested_model_key.as_str() == route.resolved_model_key
            && origin.response_model_key.as_str() == route.resolved_model_key);
    }
    target_route_owns_usage_anchor(entries, measured.source_sequence, route)
}

#[cfg(test)]
mod tests {
    use super::super::tests::{assistant_tool_entry, content_ref, user_entry};
    use super::*;
    #[test]
    fn closed_tail_replays_free_assistant_output_once_after_a_proven_publication() {
        let mut result = user_entry(3, "published");
        result.message.role = CanonicalRole::Tool;
        let receipt = result.source_record_ids[0];
        let mut private = user_entry(4, "after the visible bubble");
        private.message.role = CanonicalRole::Assistant;
        let entries = vec![result.clone(), private.clone()];
        assert_eq!(
            crate::conversation_history_segment(&entries, None, Some(receipt), false),
            Some(vec![result.clone()])
        );
        assert_eq!(
            crate::conversation_history_segment(&entries, None, Some(receipt), true),
            Some(entries.clone())
        );
        assert_eq!(
            crate::conversation_history_segment(
                &entries,
                Some(result.source_sequence),
                Some(receipt),
                true
            ),
            Some(vec![private])
        );
        assert_eq!(
            crate::conversation_history_segment(&entries, None, None, true),
            Some(entries.clone())
        );
        assert!(crate::conversation_history_segment(&entries, None, None, false).is_none());
        assert!(
            crate::conversation_history_segment(
                &entries,
                None,
                Some(musteragent_identity::LedgerRecordId::generate()),
                true
            )
            .is_none()
        );
    }

    #[test]
    fn structural_selection_preserves_atomic_calls_and_durable_large_results() {
        let user = user_entry(1, &"constraint ".repeat(5000));
        let call = assistant_tool_entry(2, &["call-a"]);
        let mut result = user_entry(3, &"🦀 oversized result ".repeat(5000));
        result.message.role = CanonicalRole::Tool;
        result.message.tool_call_id = Some(call.message.tool_calls[0].call_id.clone());
        let image = CanonicalContentPart::Image(content_ref(b"original image"));
        result.message.content.push(image.clone());
        let refs = vec![&user, &call, &result];
        let (tail, prefix) = select(&refs).expect("valid context capacity fixture");
        assert_eq!(prefix.len(), 1);
        assert_eq!(tail.len(), 2);
        let bounded = bounded_tool_entries(&tail, true).expect("valid context capacity fixture");
        assert_eq!(bounded[0], call);
        assert_eq!(bounded[1].message.tool_call_id, result.message.tool_call_id);
        assert_eq!(bounded[1].source_record_ids, result.source_record_ids);
        assert_eq!(bounded[1].message.content[1], image);
        let CanonicalContentPart::Text(text) = &bounded[1].message.content[0] else {
            panic!()
        };
        let reference: serde_json::Value =
            serde_json::from_str(text.as_str()).expect("valid context capacity fixture");
        assert_eq!(
            reference["record_id"],
            result
                .source_record_ids
                .last()
                .expect("valid context capacity fixture")
                .to_string()
        );
        assert_eq!(
            bounded_tool_entries(&tail, false)
                .expect_err("expected fixture rejection")
                .code,
            ErrorCode::ModelContextExceeded
        );
        assert_eq!(user.message.content[0], refs[0].message.content[0]);
        assert!(
            matches!(&result.message.content[0],CanonicalContentPart::Text(text) if text.as_str().len()>4096)
        );
    }
}
