//! Import only verified, completed legacy protocol receipts into the tool journal.
use super::*;
use musteragent_context::ContextCodec;
use musteragent_loop::PlannedToolCall;
use musteragent_persistence_codec::CanonicalJsonContextCodec;
use musteragent_runtime_automation::EvaluationProtocolReceipt;

impl PostgresStandingGoalJudgeRunSource {
    pub(super) async fn load_protocol_receipts(
        &self,
        run_id: RunId,
    ) -> Result<Vec<EvaluationProtocolReceipt>, LoopPortError> {
        let rows = sqlx::query("SELECT run.tenant_id, p.tool_call_id, p.wire_name, p.arguments_json,
            c.content_id, c.media_type, c.byte_length::text AS byte_length, c.content_hash,
            p.result_content_hash, p.result_content_bytes::text AS result_content_bytes
            FROM runtime.runs run JOIN runtime.pending_tool_calls p ON p.run_id = run.run_id
            JOIN artifact.content_refs c ON c.tenant_id = run.tenant_id AND c.content_id = p.result_content_id AND c.state = 2
            WHERE run.run_id = $1 AND run.activity_kind = 6 AND p.completed_at_ms IS NOT NULL
            AND p.wire_name IN ('evaluation_read_evidence', 'evaluation_decide')
            AND NOT EXISTS (SELECT 1 FROM tool.tool_invocations t WHERE t.tenant_id = run.tenant_id AND t.run_id = run.run_id AND t.tool_call_id = p.tool_call_id AND t.status IN (4, 5, 7))
            ORDER BY p.completed_at_ms, p.ordinal")
            .bind(run_id.into_uuid()).fetch_all(&self.pool).await.map_err(loop_unavailable)?;
        let mut receipts = Vec::new();
        for row in rows {
            let tenant_id =
                TenantId::from_uuid(row.try_get("tenant_id").map_err(loop_unavailable)?)
                    .map_err(|_| loop_corrupt())?;
            let reference = ContentRef {
                content_id: ContentId::from_uuid(
                    row.try_get("content_id").map_err(loop_unavailable)?,
                )
                .map_err(|_| loop_corrupt())?,
                media_type: MediaType::new(
                    row.try_get::<String, _>("media_type")
                        .map_err(loop_unavailable)?,
                )
                .map_err(|_| loop_corrupt())?,
                byte_length: row
                    .try_get::<String, _>("byte_length")
                    .map_err(loop_unavailable)?
                    .parse()
                    .map_err(|_| loop_corrupt())?,
                sha256: Sha256::from_bytes(
                    row.try_get::<Vec<u8>, _>("content_hash")
                        .map_err(loop_unavailable)?,
                )
                .map_err(|_| loop_corrupt())?,
            };
            if row
                .try_get::<Vec<u8>, _>("result_content_hash")
                .map_err(loop_unavailable)?
                != reference.sha256.into_bytes()
                || row
                    .try_get::<String, _>("result_content_bytes")
                    .map_err(loop_unavailable)?
                    .parse::<u64>()
                    .map_err(|_| loop_corrupt())?
                    != reference.byte_length
            {
                return Err(loop_corrupt());
            }
            let bytes = self
                .content
                .read(tenant_id, &reference)
                .await
                .map_err(loop_content)?;
            verify_content(&reference, &bytes).map_err(|_| loop_corrupt())?;
            let call_id = musteragent_identity::ToolCallId::new(
                row.try_get::<String, _>("tool_call_id")
                    .map_err(loop_unavailable)?,
            )
            .map_err(|_| loop_corrupt())?;
            let name = WireName::new(
                row.try_get::<String, _>("wire_name")
                    .map_err(loop_unavailable)?,
            )
            .map_err(|_| loop_corrupt())?;
            let message = crate::context_material::decode_tool_message(&bytes, &call_id, &name)
                .map_err(|_| loop_corrupt())?;
            let args: serde_json::Value =
                row.try_get("arguments_json").map_err(loop_unavailable)?;
            let arguments = CanonicalJsonContextCodec
                .decode_json(&serde_json::to_vec(&args).map_err(|_| loop_corrupt())?)
                .map_err(|_| loop_corrupt())?;
            receipts.push(EvaluationProtocolReceipt {
                tenant_id,
                call: PlannedToolCall {
                    argument_diagnostic: None,
                    call_id,
                    name,
                    arguments,
                    provider_replay: None,
                    planning_failure: None,
                },
                message,
            });
        }
        Ok(receipts)
    }
}
