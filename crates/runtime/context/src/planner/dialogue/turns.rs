//! Translate a token target to the earliest closed conversation prefix.
//! A shared storage page can contain several turns or interleaved executions.
use super::*;
use crate::DialogueHistoryMaterial;
use std::collections::BTreeSet;

pub(super) struct Selection {
    pub(super) coverage: DialogueContextCut,
    fragments: Option<(ContextEntry, ContextEntry)>,
}

impl Selection {
    pub(super) fn entries<'a>(
        &'a self,
        material: &'a ContextMaterial,
    ) -> Result<(Vec<&'a ContextEntry>, Vec<&'a ContextEntry>), ContextPlanError> {
        let mut compacted = Vec::new();
        let mut retained = Vec::new();
        for entry in &material.entries {
            match &entry.history_page {
                Some(page) if page.through_seq <= self.coverage.through.seq => {
                    compacted.push(entry)
                }
                Some(page) if page.from_seq <= self.coverage.through.seq => {
                    let (prefix, suffix) = self
                        .fragments
                        .as_ref()
                        .ok_or_else(ContextPlanError::invariant)?;
                    compacted.push(prefix);
                    retained.push(suffix);
                }
                _ => retained.push(entry),
            }
        }
        compacted.sort_by_key(|e| e.history_page.as_ref().map(|page| page.from_seq));
        Ok((compacted, retained))
    }
}

pub(super) fn select(
    material: &ContextMaterial,
    window: &DialogueContextWindow,
    required: Option<u64>,
) -> Result<Option<Selection>, ContextPlanError> {
    let mut full = window.frozen_cut.clone();
    full.through.seq = window.bound_through_seq;
    full.entry_count = window.bound_through_seq;
    full.source_chain_sha256 = window.bound_source_chain_sha256;
    let full_selection = || Selection {
        coverage: full.clone(),
        fragments: None,
    };
    if !musteragent_runtime_snapshot::uses_native_history(
        material
            .runtime_snapshot
            .context_policy
            .compaction_policy_ref
            .as_str(),
    ) {
        return Ok(Some(full_selection()));
    }
    let sealed = window.started_coverage.as_ref();
    let selected = boundary(super::ordered_history(material), sealed, required)?;
    let Some((entry, boundary)) = selected else {
        if sealed == Some(&full) {
            return Ok(Some(full_selection()));
        }
        if sealed.is_some() {
            return Err(ContextPlanError::invariant());
        }
        return Ok(None);
    };
    let mut coverage = full;
    coverage.through.seq = boundary.seq;
    coverage.entry_count = boundary.seq;
    coverage.source_chain_sha256 = boundary.chain;
    let page = entry
        .history_page
        .as_ref()
        .ok_or_else(ContextPlanError::invariant)?;
    let fragments = if boundary.seq < page.through_seq {
        let mut prefix = entry.clone();
        let mut suffix = entry.clone();
        let left = prefix
            .history_page
            .as_mut()
            .ok_or_else(ContextPlanError::invariant)?;
        let right = suffix
            .history_page
            .as_mut()
            .ok_or_else(ContextPlanError::invariant)?;
        left.through_seq = boundary.seq;
        left.history.items.truncate(boundary.item_end);
        right.from_seq = boundary.seq + 1;
        right.history.items.drain(..boundary.item_end);
        // These ephemeral slices feed only the summary model and budget estimator.
        left.history.boundaries.clear();
        right.history.boundaries.clear();
        Some((prefix, suffix))
    } else {
        None
    };
    Ok(Some(Selection {
        coverage,
        fragments,
    }))
}

fn boundary<'a>(
    entries: impl IntoIterator<Item = &'a ContextEntry>,
    sealed: Option<&DialogueContextCut>,
    required: Option<u64>,
) -> Result<Option<(&'a ContextEntry, &'a crate::DialogueHistoryBoundary)>, ContextPlanError> {
    let mut open = BTreeSet::new();
    let mut tokens = 0u64;
    let mut selected = None;
    'pages: for entry in entries {
        let Some(page) = &entry.history_page else {
            continue;
        };
        if page.source.media_type == ContextPlanContentKind::OwnerSourceManifest.media_type() {
            continue;
        }
        let mut start = 0;
        for boundary in &page.history.boundaries {
            let slice = page
                .history
                .items
                .get(start..boundary.item_end)
                .ok_or_else(ContextPlanError::invariant)?;
            tokens = tokens.saturating_add(DialogueHistoryMaterial::estimate(slice));
            start = boundary.item_end;
            if boundary.begins {
                open.insert(boundary.intent);
            }
            if boundary.closes {
                open.remove(&boundary.intent);
                if open.is_empty()
                    && sealed.is_none_or(|c| {
                        c.through.seq == boundary.seq && c.source_chain_sha256 == boundary.chain
                    })
                {
                    selected = Some((entry, boundary));
                    if sealed.is_some()
                        || super::super::capacity::enough_summary_savings(
                            tokens,
                            required.unwrap_or(u64::MAX),
                        )
                    {
                        break 'pages;
                    }
                }
            }
        }
        tokens = tokens.saturating_add(DialogueHistoryMaterial::estimate(
            &page.history.items[start..],
        ));
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::tests::{content_ref, user_entry};
    use crate::{BoundConversationHistory, ConversationHistoryItem, DialogueHistoryBoundary};
    use musteragent_content_ref::Sha256;
    use musteragent_execution_context::DialogueCursor;
    use musteragent_identity::{AgentDialogueId, AgentId, PrincipalId, TenantId, UserIntentId};
    use musteragent_runtime_model::CanonicalRole;

    #[test]
    fn welcome_only_history_does_not_start_a_summary_during_the_first_tool_exchange() {
        let mut welcome = user_entry(1, "host record");
        welcome.history_page = Some(BoundConversationHistory {
            cut: DialogueContextCut {
                tenant_id: TenantId::generate(),
                owner_id: PrincipalId::generate(),
                agent_id: AgentId::generate(),
                dialogue_id: AgentDialogueId::generate(),
                through: DialogueCursor { epoch: 1, seq: 1 },
                entry_count: 1,
                source_chain_sha256: Sha256::digest(b"welcome"),
            },
            from_seq: 1,
            through_seq: 1,
            source: content_ref(b"welcome"),
            history: DialogueHistoryMaterial::default(),
        });
        let user = user_entry(2, "original assignment");
        let call = crate::planner::tests::assistant_tool_entry(3, &["list"]);
        let mut result = user_entry(4, "directory");
        result.message.role = CanonicalRole::Tool;
        result.message.tool_call_id = Some(call.message.tool_calls[0].call_id.clone());
        let entries = [&welcome, &user, &call, &result];
        assert!(boundary([&welcome], None, Some(1)).unwrap().is_none());
        for recover in [false, true] {
            let (retained, compacted) = crate::planner::capacity::select_native(
                |entry| entry.source_sequence == user.source_sequence,
                &entries,
                recover,
                1,
            )
            .unwrap();
            // Restoring the exact assignment is handled by the same authority
            // as in planning. No host page can ever enter the summary source.
            assert!(retained.contains(&&welcome));
            assert!(!compacted.contains(&&welcome));
            assert!(retained.contains(&&call) && retained.contains(&&result));
        }
    }

    #[test]
    fn only_the_required_closed_turns_are_compacted_even_inside_one_storage_page() {
        let hash = Sha256::digest(b"page");
        let cut = DialogueContextCut {
            tenant_id: TenantId::generate(),
            owner_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            through: DialogueCursor { epoch: 1, seq: 6 },
            entry_count: 6,
            source_chain_sha256: hash,
        };
        let mut entry = user_entry(9, "current");
        let text = "原始工作消息".repeat(90);
        let mut items = Vec::new();
        let mut boundaries = Vec::new();
        for turn in 0..3 {
            let intent = UserIntentId::generate();
            for i in 0..2 {
                let seq = turn * 2 + i + 1;
                items.push(ConversationHistoryItem::Message(
                    user_entry(seq, &text).message,
                    None,
                ));
                boundaries.push(DialogueHistoryBoundary {
                    seq,
                    chain: hash,
                    item_end: items.len(),
                    intent,
                    begins: i == 0,
                    closes: i == 1,
                });
            }
        }
        let required = DialogueHistoryMaterial::estimate(&items[..2]); // One turn minus its summary cannot provide this much space.
        entry.history_page = Some(BoundConversationHistory {
            cut,
            from_seq: 1,
            through_seq: 6,
            source: content_ref(b"page"),
            history: DialogueHistoryMaterial { items, boundaries },
        });
        assert_eq!(
            boundary([&entry], None, Some(required))
                .unwrap()
                .unwrap()
                .1
                .seq,
            4
        );
        let markers = &mut entry.history_page.as_mut().unwrap().history.boundaries;
        // A and B overlap. B returns first and A returns second.
        let b = markers[2].intent;
        let a = markers[0].intent;
        markers[1].begins = true;
        markers[1].closes = false;
        markers[1].intent = b;
        markers[2].begins = false;
        markers[2].closes = true;
        markers[3].intent = a;
        assert_eq!(boundary([&entry], None, Some(1)).unwrap().unwrap().1.seq, 4);
    }
}
