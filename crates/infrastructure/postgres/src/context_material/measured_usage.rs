//! Same-Run physical input usage; an installed summary invalidates older facts.
use super::*;
use musteragent_context::MeasuredContextUsage;
impl PostgresContextMaterialSource {
    pub(super) async fn load_measured_usage(
        &self,
        context: &RunContext,
        entries: &[ContextEntry],
        baseline: Option<&MaterializedContextBaseline>,
        recovery: Option<&AssistantOrigin>,
    ) -> Result<Option<MeasuredContextUsage>, PostgresContextMaterialError> {
        let entry = entries
            .iter()
            .filter(|entry| {
                entry.source_run_id == Some(context.run_id)
                    && entry.message.role == CanonicalRole::Assistant
                    && entry.assistant_origin.is_some()
            })
            .max_by_key(|entry| entry.source_sequence);
        let Some(origin) =
            recovery.or_else(|| entry.and_then(|entry| entry.assistant_origin.as_ref()))
        else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT (usage.input_tokens_uncached::numeric + usage.provider_cache_read_tokens + usage.provider_cache_write_tokens)::text AS input_tokens,
                    invocation.started_conversation_seq::text AS source_sequence
             FROM runtime.model_invocations invocation JOIN runtime.model_usage_facts usage ON usage.invocation_id=invocation.invocation_id AND usage.tenant_id=invocation.tenant_id
             WHERE invocation.tenant_id=$1 AND invocation.conversation_id=$2 AND invocation.run_id=$3
               AND invocation.invocation_id=$4 AND invocation.purpose=1
               AND (invocation.status=2 OR (invocation.status=3 AND invocation.terminal_stop_reason IN (3,7)))")
            .bind(context.tenant_id.into_uuid()).bind(context.conversation_id.into_uuid()).bind(context.run_id.into_uuid())
            .bind(origin.invocation_id.into_uuid()).fetch_optional(&self.pool).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let source_sequence = if recovery.is_some() {
            ConversationSeq::new(parse_u64(row.try_get("source_sequence")?)?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?
        } else {
            entry
                .ok_or(PostgresContextMaterialError::CorruptState)?
                .source_sequence
        };
        if let Some(baseline) = baseline {
            let checkpoint: String = sqlx::query_scalar("SELECT committed_sequence::text FROM runtime.context_plans WHERE tenant_id=$1 AND conversation_id=$2 AND snapshot_id=$3")
                .bind(context.tenant_id.into_uuid()).bind(context.conversation_id.into_uuid()).bind(baseline.snapshot_id.into_uuid())
                .fetch_one(&self.pool).await?;
            if parse_u64(checkpoint)? >= source_sequence.get() {
                return Ok(None);
            }
        }
        Ok(Some(MeasuredContextUsage {
            source_sequence,
            input_tokens: parse_u64(row.try_get("input_tokens")?)?,
            recovery_origin: recovery.cloned(),
        }))
    }
}
