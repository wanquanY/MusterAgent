//! Private child stimulus and durable parent completion inbox, restored from
//! the original context owner. Child transcript remains in its own Activity.
use super::*;
pub(super) struct DelegationInput {
    pub sequence: ConversationSeq,
    pub record_id: LedgerRecordId,
    pub run_id: RunId,
    pub objective_ref: ContentRef,
}
impl PostgresContextMaterialSource {
    pub(super) async fn materialize_delegation_input(
        &self,
        tenant: TenantId,
        input: DelegationInput,
    ) -> Result<ContextEntry, PostgresContextMaterialError> {
        let bytes = self.content.read(tenant, &input.objective_ref).await?;
        verify_content(&input.objective_ref, &bytes)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let text =
            std::str::from_utf8(&bytes).map_err(|_| PostgresContextMaterialError::CorruptState)?;
        Ok(ContextEntry {
            history_page: None,
            runtime_events: Vec::new(),
            source_sequence: input.sequence,
            presentation_order: ContextPresentationOrder::ledger(input.sequence),
            source_record_ids: vec![input.record_id],
            source_run_id: Some(input.run_id),
            assistant_origin: None,
            message: CanonicalMessage {
                role: CanonicalRole::User,
                content: ModelText::chunked(format!("Delegated task:\n{text}"))
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?
                    .into_iter()
                    .map(CanonicalContentPart::Text)
                    .collect(),
                tool_calls: Vec::new(),
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
            continuation_delta_for: None,
        })
    }
    pub(super) async fn delegation_completions(
        &self,
        run: &RunContext,
    ) -> Result<
        Vec<musteragent_context::MaterializedDelegationCompletion>,
        PostgresContextMaterialError,
    > {
        let rows=sqlx::query("SELECT i.delivery_id,d.delegation_id,d.child_activity_id,d.child_run_id,c.content_id,c.media_type,c.byte_length::bigint AS byte_length,c.content_hash FROM runtime.delegation_deliveries i JOIN runtime.delegations d ON d.delegation_id=i.delegation_id JOIN artifact.content_refs c ON c.content_id=d.report_content_id WHERE i.tenant_id=$1 AND i.parent_run_id=$2 AND i.state IN (1,2) ORDER BY i.created_at_ms,i.delivery_id")
            .bind(run.tenant_id.into_uuid()).bind(run.run_id.into_uuid()).fetch_all(&self.pool).await?;
        let mut result = Vec::new();
        for row in rows {
            let reference = crate::cloud_delegation::controls::reference(&row)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let bytes = self.content.read(run.tenant_id, &reference).await?;
            verify_content(&reference, &bytes)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            result.push(musteragent_context::MaterializedDelegationCompletion {
                delivery_id: musteragent_identity::DelegationDeliveryId::from_uuid(
                    row.try_get("delivery_id")?,
                )
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                delegation_id: DelegationId::from_uuid(row.try_get("delegation_id")?)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                child_activity_id: ActivityId::from_uuid(row.try_get("child_activity_id")?)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                child_run_id: RunId::from_uuid(row.try_get("child_run_id")?)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                report: MaterializedContent {
                    content_ref: reference,
                    bytes,
                },
            });
        }
        Ok(result)
    }
}

impl PostgresContextMaterialSource {
    /// Acknowledged reports remain parent history, with the exact delivery
    /// ContextPlan as provenance. They never expose a sibling child transcript.
    pub(super) async fn delegation_report_history(
        &self,
        run: &RunContext,
        cutoff: u64,
        facts: &LedgerFacts,
    ) -> Result<Vec<ContextEntry>, PostgresContextMaterialError> {
        let rows=sqlx::query("SELECT i.delivery_id,d.delegation_id,d.child_activity_id,d.child_run_id,c.content_id,c.media_type,c.byte_length::bigint AS byte_length,c.content_hash,p.ledger_record_id,p.committed_sequence::text AS sequence FROM runtime.delegation_deliveries i JOIN runtime.delegations d ON d.delegation_id=i.delegation_id JOIN artifact.content_refs c ON c.content_id=d.report_content_id JOIN runtime.context_plans p ON p.snapshot_id=i.snapshot_id AND p.run_id=i.parent_run_id WHERE i.tenant_id=$1 AND i.parent_run_id=$2 AND i.state=3 AND p.committed_sequence<=$3::bigint ORDER BY p.committed_sequence,i.delivery_id")
            .bind(run.tenant_id.into_uuid()).bind(run.run_id.into_uuid()).bind(cutoff.to_string()).fetch_all(&self.pool).await?;
        let mut entries: Vec<ContextEntry> = Vec::new();
        for row in rows {
            let record = LedgerRecordId::from_uuid(row.try_get("ledger_record_id")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            if !facts.record_reachable_from(record, facts.active_branch_id)? {
                continue;
            }
            let sequence = ConversationSeq::new(
                row.try_get::<String, _>("sequence")?
                    .parse()
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            )
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let reference = crate::cloud_delegation::controls::reference(&row)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let bytes = self.content.read(run.tenant_id, &reference).await?;
            verify_content(&reference, &bytes)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let report = ModelText::new(
                std::str::from_utf8(&bytes)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?
                    .to_owned(),
            )
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let event = musteragent_runtime_model::CanonicalRuntimeEvent::DelegationCompletion {
                delivery_id: musteragent_identity::DelegationDeliveryId::from_uuid(
                    row.try_get("delivery_id")?,
                )
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                delegation_id: DelegationId::from_uuid(row.try_get("delegation_id")?)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                child_activity_id: ActivityId::from_uuid(row.try_get("child_activity_id")?)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                child_run_id: RunId::from_uuid(row.try_get("child_run_id")?)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                report: report.clone(),
            };
            if let Some(entry) = entries
                .last_mut()
                .filter(|entry| entry.source_record_ids == [record])
            {
                entry.runtime_events.push(event);
                entry
                    .message
                    .content
                    .push(CanonicalContentPart::Text(report));
            } else {
                entries.push(ContextEntry {
                    history_page: None,
                    runtime_events: vec![event],
                    source_sequence: sequence,
                    presentation_order: ContextPresentationOrder::ledger(sequence),
                    source_record_ids: vec![record],
                    source_run_id: Some(run.run_id),
                    assistant_origin: None,
                    message: CanonicalMessage {
                        role: CanonicalRole::Assistant,
                        content: vec![CanonicalContentPart::Text(report)],
                        tool_calls: Vec::new(),
                        tool_call_id: None,
                        tool_name: None,
                        reasoning: None,
                    },
                    continuation_delta_for: None,
                });
            }
        }
        Ok(entries)
    }
}

impl PostgresContextMaterialSource {
    pub(super) async fn derived_source(
        &self,
        run: &RunContext,
    ) -> Result<Option<RunId>, PostgresContextMaterialError> {
        let row = sqlx::query("SELECT d.source_run_id,d.derived_snapshot_hash,s.content_hash FROM runtime.derived_run_authorities d JOIN runtime.runs r ON r.run_id=d.run_id JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE r.run_id=$1 AND r.tenant_id=$2")
            .bind(run.run_id.into_uuid()).bind(run.tenant_id.into_uuid()).fetch_optional(&self.pool).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row.try_get::<Vec<u8>, _>("derived_snapshot_hash")?
            != row.try_get::<Vec<u8>, _>("content_hash")?
        {
            return Err(PostgresContextMaterialError::CorruptState);
        }
        let source = RunId::from_uuid(row.try_get("source_run_id")?)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        if source == run.run_id {
            return Err(PostgresContextMaterialError::CorruptState);
        }
        Ok(Some(source))
    }
}
