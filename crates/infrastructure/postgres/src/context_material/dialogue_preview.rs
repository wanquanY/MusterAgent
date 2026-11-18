//! Bounded source read-ahead. No admission, capacity or dispatch outcome is cached.
use super::*;
use musteragent_context::{
    DIALOGUE_SOURCE_BATCH_PAGES, DialogueContextWindow, DialogueSourcePreview,
};
use musteragent_identity::{Actor, PrincipalKind};

impl PostgresContextMaterialSource {
    pub(super) async fn dialogue_source_preview_inner(
        &self,
        input: &ContextPlanInput,
        expected: &DialogueContextWindow,
    ) -> Result<Option<Vec<DialogueSourcePreview>>, PostgresContextMaterialError> {
        let Some(first) = &expected.next_page else {
            return Ok(None);
        };
        if expected.started_coverage.is_some()
            || first.checkpoint.is_some()
            || first.through_seq == expected.frozen_cut.through.seq
        {
            return Ok(None);
        }
        let (_, current) = self.load_run_source(input).await?;
        if current.as_ref() != Some(expected) {
            return Err(PostgresContextMaterialError::CorruptStateAt(
                "dialogue_window_cut",
            ));
        }
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Context,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        let cut = crate::dialogue_context::run::run_cut(&mut tx, input.attempt.run_id, false)
            .await
            .map_err(|e| PostgresContextMaterialError::dialogue_authority("dialogue_run_cut", e))?
            .ok_or(PostgresContextMaterialError::CorruptState)?;
        if cut != expected.frozen_cut {
            return Err(PostgresContextMaterialError::CorruptState);
        }
        let actor = Actor {
            tenant_id: cut.tenant_id,
            principal_id: cut.owner_id,
            principal_kind: PrincipalKind::User,
        };
        let source_pages = crate::dialogue_context::load_pages(
            &mut tx,
            actor,
            &cut,
            first.from_seq,
            DIALOGUE_SOURCE_BATCH_PAGES,
        )
        .await
        .map_err(|e| PostgresContextMaterialError::dialogue_source("dialogue_page", e))?;
        let mut pages = Vec::new();
        let mut references = Vec::new();
        let mut bytes = 0_u64;
        for page in source_pages {
            let refs = musteragent_context::dialogue_history_text_refs(&page);
            let size = refs
                .iter()
                .fold(0_u64, |n, r| n.saturating_add(r.byte_length));
            // Large individual pages retain the original one-page path. Do not
            // multiply its maximum resident content by the read-ahead factor.
            if bytes.saturating_add(size) > musteragent_context::DIALOGUE_SOURCE_BATCH_TEXT_BYTES {
                break;
            }
            bytes = bytes.saturating_add(size);
            references.extend(refs);
            pages.push(page);
        }
        tx.commit().await?;
        if pages.len() < 2 {
            return Ok(None);
        }
        let bodies = ContentBatch::load(self.content.as_ref(), cut.tenant_id, &references).await?;
        let mut previews = Vec::with_capacity(pages.len());
        for page in pages {
            let material = self
                .dialogue_history(input.attempt.run_id, &page, &bodies)
                .await?;
            previews.push(DialogueSourcePreview { page, material });
        }
        self.require_dialogue_window_current(input, expected)
            .await?;
        Ok(Some(previews))
    }
}
