//! Durable PostgreSQL compaction baseline lookup.

use super::*;

pub(crate) struct PlanPredecessor {
    pub(crate) next_iteration: u32,
    pub(crate) source_seq_to: ConversationSeq,
    pub(crate) same_iteration_predecessor: Option<ContextSnapshotId>,
}

impl PostgresContextMaterialSource {
    pub(super) async fn load_previous_plan(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        run_id: RunId,
        PlanPredecessor {
            next_iteration,
            source_seq_to,
            same_iteration_predecessor,
        }: PlanPredecessor,
        branch_facts: &LedgerFacts,
    ) -> Result<Option<PreviousContextPlan>, PostgresContextMaterialError> {
        let rows = sqlx::query(
            "SELECT plan.snapshot_id,
                    plan.source_sequence_to::text AS source_sequence_to,
                    plan.retained_record_ids,
                    plan.summary_content_id,
                    plan.summary_content_hash,
                    plan.summary_content_bytes::text AS summary_content_bytes,
                    checkpoint.context_branch_id AS checkpoint_branch_id,
                    checkpoint.conversation_seq::text AS checkpoint_sequence
             FROM runtime.context_plans AS plan
             JOIN artifact.content_refs AS content
               ON content.tenant_id = $1
              AND content.content_id = plan.summary_content_id
              AND content.content_hash = plan.summary_content_hash
              AND content.byte_length = plan.summary_content_bytes
              AND content.state = 2
             JOIN runtime.context_compaction_slots AS slot
               ON slot.tenant_id = plan.tenant_id
              AND slot.run_id = plan.run_id
              AND slot.loop_iteration = plan.iteration
             JOIN runtime.model_invocations AS invocation
               ON invocation.invocation_id = slot.invocation_id
              AND invocation.status = 2
             JOIN event_store.ledger_records AS checkpoint
               ON checkpoint.record_id = plan.ledger_record_id
              AND checkpoint.tenant_id = plan.tenant_id
              AND checkpoint.conversation_id = plan.conversation_id
             WHERE plan.tenant_id = $1
               AND plan.conversation_id = $2
               AND plan.source_sequence_to <= $5::bigint
               AND plan.input_tokens_after < plan.input_tokens_before
               AND (
                    (plan.run_id = $3 AND plan.iteration < $4)
                    OR (plan.run_id = $3 AND plan.iteration = $4 AND
                        COALESCE(plan.lineage_generation,1) <= (
                            SELECT COALESCE(parent.lineage_generation,1)
                            FROM runtime.context_plans parent
                            WHERE parent.tenant_id=$1 AND parent.run_id=$3
                              AND parent.iteration=$4 AND parent.snapshot_id=$6
                        ))
                    OR plan.run_id <> $3
               )
             ORDER BY plan.source_sequence_to DESC,
                      plan.created_at_ms DESC,
                      plan.snapshot_id DESC",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(i32::try_from(next_iteration)?)
        .bind(source_seq_to.get().to_string())
        .bind(same_iteration_predecessor.map(|id| id.into_uuid()))
        .fetch_all(&self.pool)
        .await?;
        let mut selected = None;
        for row in rows {
            let checkpoint_branch_id = row
                .try_get::<Option<uuid::Uuid>, _>("checkpoint_branch_id")?
                .map(ContextBranchId::from_uuid)
                .transpose()
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let checkpoint_sequence =
                ConversationSeq::new(parse_u64(row.try_get("checkpoint_sequence")?)?)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            if branch_facts.active_branch_includes(checkpoint_branch_id, checkpoint_sequence)? {
                selected = Some(row);
                break;
            }
        }
        let Some(row) = selected else {
            return Ok(None);
        };
        let summary_ref = ContentRef {
            content_id: id(row.try_get("summary_content_id")?)?,
            media_type: MediaType::new("application/vnd.musteragent.legacy.context-summary+json")
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            byte_length: parse_u64(row.try_get("summary_content_bytes")?)?,
            sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("summary_content_hash")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
        };
        let bytes = self.content.read(tenant_id, &summary_ref).await?;
        verify_content(&summary_ref, &bytes)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        Ok(Some(PreviousContextPlan {
            snapshot_id: ContextSnapshotId::from_uuid(row.try_get("snapshot_id")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            source_seq_to: ConversationSeq::new(parse_u64(row.try_get("source_sequence_to")?)?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            retained_record_ids: row
                .try_get::<Vec<uuid::Uuid>, _>("retained_record_ids")?
                .into_iter()
                .map(|value| {
                    LedgerRecordId::from_uuid(value)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)
                })
                .collect::<Result<Vec<_>, _>>()?,
            summary: MaterializedContent {
                content_ref: summary_ref,
                bytes,
            },
        }))
    }
}

pub(super) struct PreviousContextPlan {
    pub(super) snapshot_id: ContextSnapshotId,
    pub(super) source_seq_to: ConversationSeq,
    pub(super) retained_record_ids: Vec<LedgerRecordId>,
    pub(super) summary: MaterializedContent,
}

impl PostgresContextMaterialSource {
    pub(super) async fn rejected_request_bytes(
        &self,
        context: &musteragent_run::RunContext,
        rejected: bool,
        invocation_id: Option<InvocationId>,
    ) -> Result<Option<u64>, PostgresContextMaterialError> {
        if !rejected {
            return Ok(None);
        }
        let invocation_id = invocation_id.ok_or(PostgresContextMaterialError::CorruptState)?;
        // Never infer rejection from the most recently prepared ContextPlan.
        let bytes: Option<String> = sqlx::query_scalar(
            "SELECT content.byte_length::text
             FROM runtime.model_invocations AS invocation
             JOIN artifact.content_refs AS content
               ON content.tenant_id=invocation.tenant_id
              AND content.content_id=invocation.request_content_id
              AND content.content_hash=invocation.request_hash
             WHERE invocation.tenant_id=$1 AND invocation.conversation_id=$2
               AND invocation.run_id=$3 AND invocation.invocation_id=$4",
        )
        .bind(context.tenant_id.into_uuid())
        .bind(context.conversation_id.into_uuid())
        .bind(context.run_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        Ok(Some(parse_u64(
            bytes.ok_or(PostgresContextMaterialError::CorruptState)?,
        )?))
    }
}
