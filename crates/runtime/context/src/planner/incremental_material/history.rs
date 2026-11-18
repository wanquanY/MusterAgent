//! Disposable projection segments selected from THIS read's authorized cut.
//! We never retain a Conversation's list of visible entries. Every rebuild
//! obtains that list, baseline and pairing from the original semantic owners.
//! A segment spans an entire Assistant call and its contiguous Tool results,
//! so an append or removal cannot reuse half of an atomic Tool exchange.
use super::*;
use crate::provider_context::{HandoffProjection, compile_provider_context};
use musteragent_identity::TenantId;
use musteragent_model::SignedModelRouteHash;
use musteragent_runtime_model::CanonicalRole;

#[derive(Debug, Clone, PartialEq)]
pub(in crate::planner) struct HistoryProjection {
    pub messages: Vec<CanonicalMessage>,
    pub projection: HandoffProjection,
    pub exact_assistant_entries: usize,
    pub portable_assistant_entries: usize,
}
impl HistoryProjection {
    pub(in crate::planner) fn has_portable_history(&self) -> bool {
        self.portable_assistant_entries > 0
    }
    fn set_projection(&mut self, compacted: bool) {
        self.projection = if compacted {
            HandoffProjection::CompactedHandoff
        } else if self.has_portable_history() {
            HandoffProjection::PortableProjection
        } else {
            HandoffProjection::ExactReplay
        };
    }
}
impl RetainedContentMaterial for HistoryProjection {
    fn retained_bytes(&self) -> usize {
        self.messages.iter().fold(
            std::mem::size_of::<Self>().saturating_add(
                self.messages
                    .capacity()
                    .saturating_mul(std::mem::size_of::<CanonicalMessage>()),
            ),
            |n, message| n.saturating_add(message_identity::message_allocations(message)),
        )
    }
}

impl MaterialReuse {
    pub(in crate::planner) fn history(
        &self,
        material: &ContextMaterial,
        entries: &[&ContextEntry],
        target: &ModelRoute,
        compacted: bool,
    ) -> Result<HistoryProjection, ContextPlanError> {
        let mut scope = Fingerprint::new(b"musteragent.context.history-scope.v1\0");
        scope.field(material.run_context.conversation_id.into_uuid().as_bytes());
        scope.number(material.run_context.home_epoch.get());
        scope.optional(material.active_branch_id, |key, id| {
            key.field(id.into_uuid().as_bytes())
        });
        scope.optional(material.previous_context.as_ref(), |key, previous| {
            key.field(previous.snapshot_id.into_uuid().as_bytes());
            key.content(&previous.summary.content_ref);
        });
        scope.field(
            material
                .runtime_snapshot
                .context_policy
                .compaction_policy_ref
                .as_str()
                .as_bytes(),
        );
        // The moving cut is checked, not cached as an append-only watermark.
        // Equal authorized segments below different cuts can safely share.
        if entries
            .iter()
            .any(|entry| entry.source_sequence > material.ledger_cutoff)
        {
            return Err(ContextPlanError::invariant());
        }
        let messages = entries
            .iter()
            .copied()
            .filter(|entry| entry.runtime_events.is_empty() && entry.history_page.is_none())
            .collect::<Vec<_>>();
        let mut result = self.history_for_scope(
            material.run_context.tenant_id,
            scope.finish(),
            &messages,
            target,
            compacted,
        )?;
        let mut prefix = Vec::new();
        let mut pending = Vec::new();
        // Material arrives in ledger order. Source-history pages precede the
        // current Run even when their import was committed after its admission.
        let mut pages: Vec<_> = entries
            .iter()
            .filter_map(|entry| entry.history_page.as_ref())
            .collect();
        pages.sort_by_key(|page| page.from_seq);
        for page in pages {
            crate::append_conversation_history(&page.history.items, &mut prefix, &mut pending);
        }
        // Consecutive user turns at a public cut can have different execution
        // owners. Keep the older turns addressable without presenting them as a
        // bundle of pending instructions for this worker.
        if prefix
            .last()
            .is_some_and(|(message, _)| message.role == CanonicalRole::User)
        {
            let earlier = CanonicalContentPart::Text(
                ModelText::new(
                    "Historical request assigned to another execution. Use as reference only.",
                )
                .map_err(|_| ContextPlanError::invariant())?,
            );
            for (message, _) in prefix
                .iter_mut()
                .rev()
                .take_while(|(message, _)| message.role == CanonicalRole::User)
            {
                message.content.insert(0, earlier.clone());
            }
            if let Some(root) = result
                .messages
                .iter_mut()
                .find(|message| message.role == CanonicalRole::User)
            {
                root.content.insert(
                    0,
                    CanonicalContentPart::Text(
                        ModelText::new(
                            "Current assignment. Respond to the following user message only.",
                        )
                        .map_err(|_| ContextPlanError::invariant())?,
                    ),
                );
            }
        }
        if !pending.is_empty() {
            if let Some(root) = result
                .messages
                .iter_mut()
                .find(|m| m.role == CanonicalRole::User)
            {
                pending.append(&mut root.content);
                root.content = pending;
            } else {
                prefix.push((
                    CanonicalMessage {
                        role: CanonicalRole::User,
                        content: pending,
                        tool_calls: vec![],
                        tool_call_id: None,
                        tool_name: None,
                        reasoning: None,
                    },
                    None,
                ));
            }
        }
        if !prefix.is_empty() {
            let mut history = crate::provider_context::compile_messages(
                prefix
                    .iter()
                    .map(|(message, origin)| (message, origin.as_ref())),
                target,
                compacted,
            )?;
            result.portable_assistant_entries += history.portable_assistant_entries;
            result.exact_assistant_entries += history.exact_assistant_entries;
            history.messages.append(&mut result.messages);
            result.messages = history.messages;
            result.set_projection(compacted);
        }
        Ok(result)
    }

    fn history_for_scope(
        &self,
        tenant_id: TenantId,
        scope: Sha256,
        entries: &[&ContextEntry],
        target: &ModelRoute,
        compacted: bool,
    ) -> Result<HistoryProjection, ContextPlanError> {
        if self.0.is_none() {
            let mut projected = project(entries, target)?;
            projected.set_projection(compacted);
            return Ok(projected);
        }
        let mut dependencies = Fingerprint::new(b"musteragent.context.history-projection.v2\0");
        dependencies.field(&scope.into_bytes());
        dependencies.field(
            SignedModelRouteHash::digest(target)
                .map_err(|_| ContextPlanError::invariant())?
                .as_bytes(),
        );
        let dependencies = dependencies.finish();
        let mut result = HistoryProjection {
            messages: Vec::with_capacity(entries.len()),
            projection: HandoffProjection::ExactReplay,
            exact_assistant_entries: 0,
            portable_assistant_entries: 0,
        };
        let mut start = 0;
        while start < entries.len() {
            let mut end = start + 1;
            let message = &entries[start].message;
            if message.role == CanonicalRole::Assistant && !message.tool_calls.is_empty() {
                while end < entries.len() && entries[end].message.role == CanonicalRole::Tool {
                    end += 1;
                }
            }
            let segment = &entries[start..end];
            let projected = self.memo(
                tenant_id,
                "context.history.v1",
                || {
                    let mut key = Fingerprint::new(b"musteragent.context.history-segment.v1\0");
                    key.field(&dependencies.into_bytes());
                    key.number(segment.len() as u64);
                    for entry in segment {
                        key.entry(entry);
                    }
                    Ok(key.finish())
                },
                || project(segment, target),
            )?;
            result.messages.extend(projected.messages.iter().cloned());
            result.exact_assistant_entries = result
                .exact_assistant_entries
                .saturating_add(projected.exact_assistant_entries);
            result.portable_assistant_entries = result
                .portable_assistant_entries
                .saturating_add(projected.portable_assistant_entries);
            start = end;
        }
        result.set_projection(compacted);
        Ok(result)
    }
}

fn project(
    entries: &[&ContextEntry],
    target: &ModelRoute,
) -> Result<HistoryProjection, ContextPlanError> {
    let provider = compile_provider_context(entries, target, false)?;
    Ok(HistoryProjection {
        messages: provider.messages,
        projection: provider.projection,
        exact_assistant_entries: provider.exact_assistant_entries,
        portable_assistant_entries: provider.portable_assistant_entries,
    })
}

#[cfg(test)]
mod tests;
