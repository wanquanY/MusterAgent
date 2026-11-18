//! Public source pages become private typed observations with real Ledger IDs.
use super::*;
use musteragent_context::DialogueContextWindow;
use musteragent_execution_context::DialogueContextCut;
use musteragent_persistence_codec::dialogue_context_codec;
use prost::Message;

impl PostgresContextMaterialSource {
    pub(super) async fn materialize_dialogue_history(
        &self,
        input: &ContextPlanInput,
        window: Option<&DialogueContextWindow>,
    ) -> Result<Vec<ContextEntry>, PostgresContextMaterialError> {
        self.materialize_dialogue_history_after(input, window, None)
            .await
    }

    pub(super) async fn materialize_dialogue_history_after(
        &self,
        input: &ContextPlanInput,
        window: Option<&DialogueContextWindow>,
        after_seq: Option<u64>,
    ) -> Result<Vec<ContextEntry>, PostgresContextMaterialError> {
        let Some(window) = window else {
            return Ok(vec![]);
        };
        self.require_dialogue_window_current(input, window).await?;
        let cut = &window.frozen_cut;
        let run = input.attempt.run_id;
        let erased: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.dialogue_context_bindings b WHERE b.run_id=$1 AND (to_jsonb(b)->>'erasure_job_id') IS NOT NULL)")
            .bind(run.into_uuid()).fetch_one(&self.pool).await?;
        if erased {
            // The original erasure owner revoked this exact retained source.
            // This is an expected authority loss, not damaged source bytes.
            return Err(PostgresContextMaterialError::dialogue_authority(
                "dialogue_source_erased",
                musteragent_runtime_session::DialogueStoreError::Unavailable,
            ));
        }
        let mut entries = Vec::new();
        let checkpoint_through = after_seq.unwrap_or_else(|| {
            window
                .checkpoint
                .as_ref()
                .map_or(0, |c| c.coverage.through.seq)
        });
        // Resolve ledger provenance with the bindings instead of one query per
        // page. LEFT JOIN preserves missing records so corruption fails closed.
        let rows = sqlx::query("SELECT b.ledger_record_id,b.from_seq,b.through_seq,b.source_page_ref,b.source_wire_version,l.conversation_seq::text AS conversation_seq FROM runtime.dialogue_context_bindings b LEFT JOIN event_store.ledger_records l ON l.record_id=b.ledger_record_id AND l.tenant_id=b.tenant_id AND l.run_id=b.run_id WHERE b.run_id=$1 AND b.tenant_id=$2 AND b.through_seq>$3 ORDER BY b.page_ordinal")
            .bind(run.into_uuid()).bind(cut.tenant_id.into_uuid()).bind(i64::try_from(checkpoint_through)?).fetch_all(&self.pool).await?;
        let pages = rows
            .into_iter()
            .map(BoundPage::from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let mut references = pages
            .iter()
            .map(|p| p.reference.clone())
            .collect::<Vec<_>>();
        if let Some(checkpoint) = window.checkpoint.as_ref().filter(|_| after_seq.is_none()) {
            references.extend([
                checkpoint.source_manifest_ref.clone(),
                checkpoint.summary_ref.clone(),
            ]);
        }
        // Fresh full-reference authorization, integrity checks, revocation
        // fencing and provenance recording remain owned by ContentStore.
        let source_bytes =
            ContentBatch::load(self.content.as_ref(), cut.tenant_id, &references).await?;
        if let Some(checkpoint) = window.checkpoint.as_ref().filter(|_| after_seq.is_none()) {
            let imported: Option<uuid::Uuid> = sqlx::query_scalar(
                "SELECT ledger_record_id FROM runtime.dialogue_context_bindings WHERE run_id=$1 AND checkpoint_id=$2",
            ).bind(run.into_uuid()).bind(checkpoint.checkpoint_id.into_uuid()).fetch_optional(&self.pool).await?;
            let record = imported.unwrap_or_else(|| {
                uuid::Uuid::new_v5(&checkpoint.generator_invocation_id.into_uuid(), b"resolved")
            });
            let manifest = source_bytes.bytes(&checkpoint.source_manifest_ref)?;
            if manifest != dialogue_context_codec::cut(&checkpoint.coverage).encode_to_vec() {
                return Err(PostgresContextMaterialError::CorruptStateAt(
                    "dialogue_checkpoint_manifest",
                ));
            }
            let bytes = source_bytes.bytes(&checkpoint.summary_ref)?;
            let text = std::str::from_utf8(bytes)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let sequence: String = sqlx::query_scalar("SELECT conversation_seq::text FROM event_store.ledger_records WHERE record_id=$1 AND tenant_id=$2 AND run_id=$3")
                .bind(record).bind(cut.tenant_id.into_uuid()).bind(run.into_uuid()).fetch_one(&self.pool).await?;
            entries.push(dialogue_entry(
                input,
                cut,
                record,
                parse_sequence(sequence)?,
                1,
                checkpoint_through,
                checkpoint.source_manifest_ref.clone(),
                musteragent_context::DialogueHistoryMaterial::summary(text)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            )?);
        }
        let mut next = checkpoint_through + 1;
        let mut decoded = Vec::with_capacity(pages.len());
        let mut body_refs = Vec::new();
        for binding in pages {
            let bytes = source_bytes.bytes(&binding.reference)?;
            let page = match binding.wire_version {
                1 => crate::dialogue_context::PostgresDialogueContextSourceStore::new(
                    self.database.clone(),
                )
                .legacy_bound_page(run, cut, binding.from_seq, bytes)
                .await
                .map_err(|error| {
                    PostgresContextMaterialError::dialogue_source(
                        "dialogue_legacy_page_source",
                        error,
                    )
                })?,
                version @ (2..=4) => dialogue_context_codec::page_from_bytes_for_version(
                    bytes, version,
                )
                .map_err(|_| PostgresContextMaterialError::CorruptStateAt("dialogue_page_codec"))?,
                _ => {
                    return Err(PostgresContextMaterialError::CorruptStateAt(
                        "dialogue_page_format",
                    ));
                }
            };
            if page.frozen_cut != *cut
                || page.checkpoint.is_some()
                || page.from_seq != binding.from_seq
                || page.through_seq != binding.through_seq
            {
                return Err(PostgresContextMaterialError::CorruptStateAt(
                    "dialogue_page_coverage",
                ));
            }
            let page = match window.checkpoint.as_ref() {
                Some(c) => dialogue_context_codec::after_checkpoint(page, c).ok_or(
                    PostgresContextMaterialError::CorruptStateAt("dialogue_suffix_chain"),
                )?,
                None => page,
            };
            if page.from_seq != next {
                return Err(PostgresContextMaterialError::CorruptStateAt(
                    "dialogue_suffix_gap",
                ));
            }
            body_refs.extend(musteragent_context::dialogue_history_text_refs(&page));
            next = page.through_seq + 1;
            decoded.push((binding, page));
        }
        if next != window.bound_through_seq + 1 {
            return Err(PostgresContextMaterialError::CorruptStateAt(
                "dialogue_bound_coverage",
            ));
        }
        let bodies = ContentBatch::load(self.content.as_ref(), cut.tenant_id, &body_refs).await?;
        for (binding, page) in decoded {
            let history = self
                .dialogue_history(input.attempt.run_id, &page, &bodies)
                .await?;
            entries.push(dialogue_entry(
                input,
                cut,
                binding.record,
                binding.sequence,
                page.from_seq,
                page.through_seq,
                binding.reference,
                history,
            )?);
        }
        Ok(entries)
    }
}

struct BoundPage {
    record: uuid::Uuid,
    sequence: ConversationSeq,
    from_seq: u64,
    through_seq: u64,
    reference: ContentRef,
    wire_version: i16,
}
impl BoundPage {
    fn from_row(row: sqlx::postgres::PgRow) -> Result<Self, PostgresContextMaterialError> {
        Ok(Self {
            record: row.try_get("ledger_record_id")?,
            sequence: parse_sequence(
                row.try_get::<Option<String>, _>("conversation_seq")?
                    .ok_or(PostgresContextMaterialError::CorruptStateAt(
                        "dialogue_page_ledger",
                    ))?,
            )?,
            from_seq: u64::try_from(row.try_get::<i64, _>("from_seq")?)?,
            through_seq: u64::try_from(row.try_get::<i64, _>("through_seq")?)?,
            reference: crate::dialogue_codec::content_from_bytes(row.try_get("source_page_ref")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            wire_version: row.try_get("source_wire_version")?,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn dialogue_entry(
    input: &ContextPlanInput,
    cut: &DialogueContextCut,
    record: uuid::Uuid,
    sequence: ConversationSeq,
    from_seq: u64,
    through_seq: u64,
    reference: ContentRef,
    history: musteragent_context::DialogueHistoryMaterial,
) -> Result<ContextEntry, PostgresContextMaterialError> {
    let record = LedgerRecordId::from_uuid(record)
        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    Ok(musteragent_context::BoundConversationHistory {
        cut: cut.clone(),
        from_seq,
        through_seq,
        source: reference,
        history,
    }
    .entry(sequence, record, input.attempt.run_id))
}

fn parse_sequence(value: String) -> Result<ConversationSeq, PostgresContextMaterialError> {
    ConversationSeq::new(
        value
            .parse()
            .map_err(|_| PostgresContextMaterialError::CorruptState)?,
    )
    .map_err(|_| PostgresContextMaterialError::CorruptState)
}
