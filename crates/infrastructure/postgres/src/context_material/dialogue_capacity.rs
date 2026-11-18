//! Size only the new tail after proving the retained sizing basis unchanged.
use super::*;
use musteragent_context::{DialogueCapacityDelta, OwnerCapacityBasis};

impl PostgresContextMaterialSource {
    pub(super) async fn dialogue_capacity_delta_inner(
        &self,
        input: &ContextPlanInput,
        basis: &OwnerCapacityBasis,
        expected: &DialogueContextWindow,
    ) -> Result<Option<DialogueCapacityDelta>, PostgresContextMaterialError> {
        // Both readers retain their own fresh authorization. Neither holds a
        // connection while awaiting the other, including on a one-slot pool.
        let ((source, window), current) = tokio::try_join!(
            self.load_run_source(input),
            self.current_dialogue_context(input),
        )?;
        let Some(window) = window else {
            return Ok(None);
        };
        if &window != expected || basis.frozen_cut != window.frozen_cut {
            return Err(PostgresContextMaterialError::CorruptStateAt(
                "dialogue_window_cut",
            ));
        }
        let pages = window
            .next_page_ordinal
            .get()
            .checked_sub(basis.next_page_ordinal.get());
        if pages.is_none_or(|count| count == 0)
            || basis.through_seq >= window.bound_through_seq
            || basis.checkpoint_id != window.checkpoint.as_ref().map(|c| c.checkpoint_id)
            || source.ledger_cutoff != source.observed_ledger_head
            || basis
                .ledger_cutoff
                .get()
                .checked_add(u64::from(pages.unwrap_or(0)))
                != Some(source.ledger_cutoff)
        {
            // Controls, discovery, plans and every other Ledger change require
            // a complete new basis, never a guessed update of the old one.
            return Ok(None);
        }
        if current.instructions != basis.instructions
            || current.approvals != basis.approvals
            || current.executions != basis.executions
        {
            return Ok(None);
        }
        let entries = self
            .materialize_dialogue_history_after(input, Some(&window), Some(basis.through_seq))
            .await?;
        Ok(Some(DialogueCapacityDelta {
            ledger_cutoff: ConversationSeq::new(source.ledger_cutoff)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            entries,
        }))
    }
}
