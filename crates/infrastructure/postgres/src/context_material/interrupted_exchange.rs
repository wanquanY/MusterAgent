//! Portable recovery of interrupted Tool batches from durable facts.
//! Never redispatches a Tool or manufactures provider replay tokens. A missing
//! result remains explicitly unknown/not executed, while verified successes survive.
use super::*;

impl PostgresContextMaterialSource {
    pub(super) async fn materialize_interrupted_exchanges(
        &self,
        tenant_id: TenantId,
        facts: &LedgerFacts,
    ) -> Result<MaterializedToolExchanges, PostgresContextMaterialError> {
        let ids = facts
            .model_completions
            .iter()
            .filter(|(_, c)| {
                c.run_id
                    .is_none_or(|id| !facts.retracted_runs.contains(&id))
            })
            .map(|(id, _)| id.into_uuid())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(MaterializedToolExchanges::default());
        }
        let rows = sqlx::query(
            "SELECT model.invocation_id AS model_id, pending.tool_call_id,
                    pending.wire_name, pending.arguments_json, pending.ordinal,
                    tool.invocation_id AS tool_id, tool.status, tool.execution_attempt_id,
                    result.result_content_id, result.result_hash
             FROM runtime.model_invocations AS model
             JOIN runtime.runs AS run
               ON run.tenant_id = model.tenant_id AND run.run_id = model.run_id
             JOIN runtime.pending_tool_calls AS pending
               ON pending.run_id = model.run_id AND pending.invocation_id = model.invocation_id
             LEFT JOIN tool.tool_invocations AS tool
               ON tool.tenant_id = model.tenant_id AND tool.run_id = model.run_id
              AND tool.tool_call_id = pending.tool_call_id
             LEFT JOIN tool.tool_results AS result ON result.invocation_id = tool.invocation_id
             WHERE model.tenant_id = $1 AND model.invocation_id = ANY($2)
               AND model.status = 2 AND model.terminal_stop_reason = 2
               AND (to_jsonb(pending)->>'erasure_job_id') IS NULL
               AND run.status = ANY($3)
               AND EXISTS (
                   SELECT 1 FROM runtime.pending_tool_calls AS incomplete
                   WHERE incomplete.run_id = model.run_id
                     AND incomplete.invocation_id = model.invocation_id
                     AND incomplete.completed_at_ms IS NULL
               )
             ORDER BY model.started_conversation_seq, pending.ordinal",
        )
        .bind(tenant_id.into_uuid())
        .bind(ids)
        .bind(RECOVERABLE_INCOMPLETE_EXCHANGE_RUN_STATUSES)
        .fetch_all(&self.pool)
        .await?;
        let mut groups: BTreeMap<InvocationId, (Vec<CanonicalToolCall>, Vec<ContextEntry>)> =
            BTreeMap::new();
        for row in rows {
            let model_id: InvocationId = id(row.try_get("model_id")?)?;
            let completion = facts
                .model_completions
                .get(&model_id)
                .ok_or(PostgresContextMaterialError::CorruptState)?;
            let call_id = ToolCallId::new(row.try_get::<String, _>("tool_call_id")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            // Runtime progress narration is public dialogue, never model history.
            if musteragent_execution_context::is_runtime_progress_call(call_id.as_str()) {
                continue;
            }
            let name = WireName::new(row.try_get::<String, _>("wire_name")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let ordinal = u32::try_from(row.try_get::<i32, _>("ordinal")?)?;
            let tool_id = row
                .try_get::<Option<uuid::Uuid>, _>("tool_id")?
                .map(id::<InvocationId>)
                .transpose()?;
            let terminal = tool_id.and_then(|id| facts.tool_terminals.get(&id));
            let status = row.try_get::<Option<i16>, _>("status")?;
            let dispatched = row
                .try_get::<Option<uuid::Uuid>, _>("execution_attempt_id")?
                .is_some();
            let (text, sequence, record_id) = if let Some(terminal) = terminal {
                let reference = match &terminal.outcome {
                    ToolTerminalSourceKind::Succeeded { result_ref } => Some(result_ref),
                    ToolTerminalSourceKind::Failed {
                        diagnostic_result_ref,
                    } => diagnostic_result_ref.as_ref(),
                    ToolTerminalSourceKind::Indeterminate {
                        diagnostic_result_ref,
                        ..
                    } => diagnostic_result_ref.as_ref(),
                    ToolTerminalSourceKind::Cancelled => None,
                };
                let result_id: Option<uuid::Uuid> = row.try_get("result_content_id")?;
                let hash = row
                    .try_get::<Option<Vec<u8>>, _>("result_hash")?
                    .map(Sha256::from_bytes)
                    .transpose()
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                if result_id != reference.map(|r| r.content_id.into_uuid())
                    || !terminal_result_matches(
                        &terminal.outcome,
                        status.unwrap_or_default(),
                        reference,
                        hash,
                    )
                {
                    return Err(PostgresContextMaterialError::CorruptStateAt(
                        "tool_terminal_result_contract",
                    ));
                }
                let text = if let ToolTerminalSourceKind::Indeterminate {
                    reason,
                    observed_error,
                    ..
                } = &terminal.outcome
                {
                    serde_json::json!({"ok": false, "outcome": "unknown", "retryable": false,
                        "uncertainty_reason": reason.as_str(),
                        "error": observed_error.as_ref().map(|error| serde_json::json!({"code": error.code.as_str(), "message": error.safe_message.as_str()})),
                        "diagnostic_result": reference.map(|reference| reference.content_id.to_string())}).to_string()
                } else if let Some(reference) = reference {
                    self.recovered_result_text(tenant_id, reference).await?
                } else {
                    interrupted_result(status, dispatched)
                };
                (text, terminal.sequence, terminal.record_id)
            } else {
                if status.is_some_and(|s| matches!(s, 4 | 5 | 7)) {
                    // A terminal database row without its reachable Ledger fact
                    // cannot be relabeled as an unknown execution.
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                (
                    interrupted_result(status, dispatched),
                    completion.sequence,
                    completion.record_id,
                )
            };
            let group = groups.entry(model_id).or_default();
            group.0.push(CanonicalToolCall {
                call_id: call_id.clone(),
                name: name.clone(),
                arguments: serde_to_json(row.try_get("arguments_json")?)?,
                provider_item_ref: None,
                provider_replay_ref: None,
            });
            group.1.push(ContextEntry {
                history_page: None,
                runtime_events: Vec::new(),
                source_sequence: sequence,
                presentation_order: ContextPresentationOrder::tool_result(
                    completion.sequence,
                    ordinal,
                ),
                source_record_ids: vec![record_id],
                source_run_id: completion.run_id,
                assistant_origin: None,
                continuation_delta_for: Some(model_id),
                message: CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: ModelText::chunked(text)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?
                        .into_iter()
                        .map(CanonicalContentPart::Text)
                        .collect(),
                    tool_calls: Vec::new(),
                    tool_call_id: Some(call_id),
                    tool_name: Some(name),
                    reasoning: None,
                },
            });
        }
        let mut recovered = MaterializedToolExchanges::default();
        for (model_id, (calls, results)) in groups {
            let completion = &facts.model_completions[&model_id];
            recovered
                .assistant_content_ids
                .insert(completion.response_ref.content_id);
            let response = self
                .content
                .read(tenant_id, &completion.response_ref)
                .await?;
            verify_content(&completion.response_ref, &response)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let text = std::str::from_utf8(&response)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let text = format!(
                "[Historical Tool batch from an interrupted Run. Results below reflect durable execution facts, not a fresh execution.]\n{text}"
            );
            recovered.entries.push(ContextEntry {
                history_page: None,
                runtime_events: Vec::new(),
                source_sequence: completion.sequence,
                presentation_order: ContextPresentationOrder::ledger(completion.sequence),
                source_record_ids: vec![completion.record_id],
                source_run_id: completion.run_id,
                // Reconstructed histories are portable, never exact provider replay.
                assistant_origin: None,
                continuation_delta_for: None,
                message: CanonicalMessage {
                    role: CanonicalRole::Assistant,
                    content: ModelText::chunked(text)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?
                        .into_iter()
                        .map(CanonicalContentPart::Text)
                        .collect(),
                    tool_calls: calls,
                    tool_call_id: None,
                    tool_name: None,
                    reasoning: None,
                },
            });
            recovered.entries.extend(results);
        }
        Ok(recovered)
    }

    async fn recovered_result_text(
        &self,
        tenant_id: TenantId,
        reference: &ContentRef,
    ) -> Result<String, PostgresContextMaterialError> {
        if reference.byte_length <= 1_000_000 {
            let bytes = self.content.read(tenant_id, reference).await?;
            verify_content(reference, &bytes)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            if let Ok(text) = String::from_utf8(bytes) {
                return Ok(text);
            }
        }
        Ok(serde_json::json!({"result_ref": {
            "content_id": reference.content_id.to_string(),
            "media_type": reference.media_type.as_str(),
            "byte_length": reference.byte_length
        }})
        .to_string())
    }
}

fn interrupted_result(status: Option<i16>, dispatched: bool) -> String {
    let (state, message) = match status {
        Some(5) => (
            "failed",
            "Tool failed before its result was delivered to the Agent.",
        ),
        Some(7) => ("cancelled", "Tool was cancelled."),
        _ if !dispatched => ("not_executed", "Run ended before this Tool was dispatched."),
        _ => (
            "outcome_unknown",
            "Run ended without a confirmed Tool result. Do not assume success or repeat a side effect without reconciliation.",
        ),
    };
    serde_json::json!({"ok": false, "execution_state": state, "error": {
        "code": "previous_run_interrupted", "message": message, "retryable": false
    }})
    .to_string()
}
