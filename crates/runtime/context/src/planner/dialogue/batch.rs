//! Read-ahead changes storage scheduling, never the model capacity boundary.
use super::*;
use crate::{DIALOGUE_SOURCE_BATCH_PAGES, DialogueSourcePreview};
use futures_util::{StreamExt, TryStreamExt, stream};
use std::num::NonZeroU32;

impl DurableContextPlanner {
    pub(super) async fn bind_dialogue_batch(
        &self,
        input: &ContextPlanInput,
        window: &DialogueContextWindow,
        capacity: &Option<capacity::CapacityState>,
    ) -> Result<Option<bool>, ContextPlanError> {
        let Some(remaining) = capacity.as_ref().and_then(|state| state.remaining(window)) else {
            return Ok(None);
        };
        let Some(previews) = self.source.dialogue_source_preview(input, window).await? else {
            return Ok(None);
        };
        if previews.is_empty() || previews.len() > DIALOGUE_SOURCE_BATCH_PAGES {
            return Err(ContextPlanError::invariant());
        }
        let mut next = window.bound_through_seq + 1;
        let mut chain = window.bound_source_chain_sha256;
        let mut tokens = 0_u64;
        let mut commits = Vec::new();
        {
            // Storage read-ahead does not move the model-capacity boundary.
            // Ordered buffering overlaps at most four immutable writes; only
            // the consumed prefix participates in the original atomic binding.
            let mut writes = Box::pin(
                stream::iter(previews.into_iter().map(
                    |DialogueSourcePreview { page, material }| async move {
                        page.validate().map_err(|_| ContextPlanError::invariant())?;
                        if page.frozen_cut != window.frozen_cut || page.checkpoint.is_some() {
                            return Err(ContextPlanError::invariant());
                        }
                        let reference = self
                            .content
                            .put(
                                &input.attempt,
                                window.frozen_cut.tenant_id,
                                ContextPlanContentKind::DialogueSourcePage,
                                self.codec.encode_dialogue_page(&page)?,
                            )
                            .await?;
                        Ok((page, material, reference))
                    },
                ))
                .buffered(4),
            );
            while let Some((page, material, reference)) = writes.try_next().await? {
                if page.from_seq != next || page.prefix_chain_sha256 != chain {
                    return Err(ContextPlanError::invariant());
                }
                tokens = tokens.saturating_add(material.estimated_tokens());
                let ordinal = window
                    .next_page_ordinal
                    .get()
                    .checked_add(
                        u32::try_from(commits.len()).map_err(|_| ContextPlanError::invariant())?,
                    )
                    .and_then(NonZeroU32::new)
                    .ok_or_else(ContextPlanError::invariant)?;
                commits.push(DialogueContextBindCommit {
                    attempt: input.attempt.clone(),
                    binding: DialogueContextBound {
                        frozen_cut: window.frozen_cut.clone(),
                        page_ordinal: ordinal,
                        from_seq: page.from_seq,
                        through_seq: page.through_seq,
                        source_page_ref: reference,
                        checkpoint_id: None,
                        completes_cut: page.through_seq == window.frozen_cut.through.seq,
                    },
                });
                next = page.through_seq + 1;
                chain = page.through_chain_sha256;
                // Exactly the ordinary loop's boundary: after adding a page, size
                // the prefix before admitting any further page. A fresh full load
                // still precedes compaction and final primary planning.
                if tokens >= remaining {
                    break;
                }
            }
            // Drop every unread write before entering the commit lane. This
            // releases pending connections even with a one-connection pool.
            // Unbound immutable bytes follow the existing Content lifecycle.
        }
        let complete = commits
            .last()
            .ok_or_else(ContextPlanError::invariant)?
            .binding
            .completes_cut;
        observe_context_plan_future(
            input.attempt.run_id,
            "dialogue_bind",
            self.plans.bind_owner_sources(commits),
        )
        .await?;
        Ok(Some(complete))
    }
}
