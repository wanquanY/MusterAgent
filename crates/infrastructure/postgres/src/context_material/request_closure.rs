//! A request closes on its last visible entry in the cut once all its cloud runs have settled.
use super::*;
use musteragent_execution_context::DialogueContextCut;
use musteragent_identity::DialogueEntryId;

pub(super) async fn settled(
    pool: &sqlx::PgPool,
    cut: &DialogueContextCut,
    entry_ids: &[uuid::Uuid],
    admitted_at_ms: i64,
) -> Result<BTreeSet<DialogueEntryId>, PostgresContextMaterialError> {
    if entry_ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let rows: Vec<uuid::Uuid> = sqlx::query_scalar(
        r#"
        SELECT e.entry_id FROM agent.agent_dialogue_entries e
        WHERE e.entry_id=ANY($4) AND e.tenant_id=$1 AND e.owner_id=$2 AND e.dialogue_id=$3
          AND e.epoch=$5 AND e.seq<=$6 AND e.erasure_job_id IS NULL
          AND e.source_intent_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM agent.agent_dialogue_entries later
             WHERE later.tenant_id=e.tenant_id AND later.dialogue_id=e.dialogue_id
               AND later.epoch=e.epoch AND later.source_intent_id=e.source_intent_id
               AND later.seq>e.seq AND later.seq<=$6 AND later.erasure_job_id IS NULL)
          AND NOT EXISTS (SELECT 1 FROM agent.dialogue_sessions t
             JOIN runtime.runs r ON r.conversation_id=t.conversation_id AND r.tenant_id=t.tenant_id
             WHERE t.tenant_id=e.tenant_id AND t.dialogue_id=e.dialogue_id
               AND t.source_intent_id=e.source_intent_id AND r.admitted_at_ms<=$7
               AND (r.finished_at_ms IS NULL OR r.finished_at_ms>$7))
        "#,
    )
    .bind(cut.tenant_id.into_uuid())
    .bind(cut.owner_id.into_uuid())
    .bind(cut.dialogue_id.into_uuid())
    .bind(entry_ids)
    .bind(i64::try_from(cut.through.epoch)?)
    .bind(i64::try_from(cut.through.seq)?)
    .bind(admitted_at_ms)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|entry| {
            DialogueEntryId::from_uuid(entry)
                .map_err(|_| PostgresContextMaterialError::CorruptState)
        })
        .collect()
}

#[cfg(test)]
#[path = "request_closure_tests.rs"]
mod tests;
