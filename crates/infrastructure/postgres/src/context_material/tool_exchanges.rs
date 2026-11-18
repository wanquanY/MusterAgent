//! Reconstruction of complete, branch-authorized Tool exchanges.

use super::*;

impl PostgresContextMaterialSource {
    pub(super) async fn materialize_tool_exchanges(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        facts: &LedgerFacts,
        assistant_origins: &BTreeMap<InvocationId, AssistantOrigin>,
        pending_native_reply: Option<InvocationId>,
    ) -> Result<MaterializedToolExchanges, PostgresContextMaterialError> {
        let completed_invocation_ids = facts
            .model_completions
            .iter()
            .filter(|(_, completion)| {
                completion
                    .run_id
                    .is_none_or(|run_id| !facts.retracted_runs.contains(&run_id))
            })
            .map(|(invocation_id, _)| invocation_id.into_uuid())
            .collect::<Vec<_>>();
        if completed_invocation_ids.is_empty() {
            return Ok(MaterializedToolExchanges::default());
        }
        let rows = sqlx::query(
            "SELECT invocation.invocation_id, invocation.terminal_stop_reason,
                    (to_jsonb(invocation)->>'context_erasure_job_id') IS NOT NULL AS context_erased,
                    invocation.model_route_ref,
                    invocation.response_content_id,
                    response.media_type AS response_media_type,
                    response.byte_length::text AS response_byte_length,
                    response.content_hash AS response_content_hash,
                    reasoning_replay.content_id AS reasoning_replay_content_id,
                    reasoning_replay.media_type AS reasoning_replay_media_type,
                    reasoning_replay.byte_length::text AS reasoning_replay_byte_length,
                    reasoning_replay.content_hash AS reasoning_replay_content_hash,
                    invocation.reasoning_replay_content_hash
                        AS reasoning_replay_persisted_hash,
                    invocation.reasoning_replay_content_bytes::text
                        AS reasoning_replay_persisted_bytes,
                    pending.tool_call_id, pending.wire_name,
                    pending.arguments_json, pending.ordinal, pending.planning_failure,
                    provider_replay.content_id AS provider_replay_content_id,
                    provider_replay.media_type AS provider_replay_media_type,
                    provider_replay.byte_length::text AS provider_replay_byte_length,
                    provider_replay.content_hash AS provider_replay_content_hash,
                    pending.provider_replay_content_hash AS provider_replay_persisted_hash,
                    pending.provider_replay_content_bytes::text AS provider_replay_persisted_bytes,
                    pending.completed_at_ms, pending.result_content_id,
                    result.media_type AS result_media_type,
                    result.byte_length::text AS result_byte_length,
                    result.content_hash AS result_content_hash,
                    tool_invocation.invocation_id AS tool_invocation_id,
                    tool_invocation.status AS tool_invocation_status, tool_invocation.tool_ref AS executed_tool_key,
                    capability_result.content_id AS capability_result_content_id,
                    capability_result.media_type AS capability_result_media_type,
                    capability_result.byte_length::text AS capability_result_byte_length,
                    capability_result.content_hash AS capability_result_content_hash,
                    tool_result.result_hash AS capability_result_persisted_hash
             FROM runtime.model_invocations AS invocation
             JOIN runtime.runs AS run
               ON run.tenant_id = invocation.tenant_id
              AND run.run_id = invocation.run_id
             JOIN runtime.pending_tool_calls AS pending
               ON pending.run_id = invocation.run_id
              AND pending.invocation_id = invocation.invocation_id
             JOIN artifact.content_refs AS response
               ON response.content_id = invocation.response_content_id
             LEFT JOIN artifact.content_refs AS provider_replay
               ON provider_replay.tenant_id = invocation.tenant_id
              AND provider_replay.content_id = pending.provider_replay_content_id
             LEFT JOIN artifact.content_refs AS reasoning_replay
               ON reasoning_replay.tenant_id = invocation.tenant_id
              AND reasoning_replay.content_id = invocation.reasoning_replay_content_id
             LEFT JOIN artifact.content_refs AS result
               ON result.content_id = pending.result_content_id
             LEFT JOIN tool.tool_invocations AS tool_invocation
               ON tool_invocation.tenant_id = invocation.tenant_id
              AND tool_invocation.run_id = invocation.run_id
              AND tool_invocation.tool_call_id = pending.tool_call_id
             LEFT JOIN tool.tool_results AS tool_result
               ON tool_result.invocation_id = tool_invocation.invocation_id
             LEFT JOIN artifact.content_refs AS capability_result
               ON capability_result.tenant_id = invocation.tenant_id
              AND capability_result.content_id = tool_result.result_content_id
             WHERE invocation.tenant_id = $1
               AND run.conversation_id = $2
               AND invocation.invocation_id = ANY($3)
               AND (to_jsonb(pending)->>'erasure_job_id') IS NULL
               AND invocation.status = 2
               AND NOT (
                   invocation.terminal_stop_reason IN (2, 3, 7)
                   AND run.status = ANY($4)
                   AND EXISTS (
                       SELECT 1
                       FROM runtime.pending_tool_calls AS incomplete
                       WHERE incomplete.run_id = invocation.run_id
                         AND incomplete.invocation_id = invocation.invocation_id
                         AND incomplete.completed_at_ms IS NULL
                   )
               )
             ORDER BY invocation.started_conversation_seq, pending.ordinal",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(&completed_invocation_ids)
        .bind(RECOVERABLE_INCOMPLETE_EXCHANGE_RUN_STATUSES)
        .fetch_all(&self.pool)
        .await?;
        let mut grouped = BTreeMap::<InvocationId, ToolExchangeSource>::new();
        for row in rows {
            let stop = row.try_get::<Option<i16>, _>("terminal_stop_reason")?;
            if stop != Some(2)
                && !(matches!(stop, Some(3 | 7))
                    && row.try_get::<Option<i16>, _>("planning_failure")? == Some(1))
            {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            let invocation_id: InvocationId = id(row.try_get("invocation_id")?)?;
            let completion = facts
                .model_completions
                .get(&invocation_id)
                .ok_or(PostgresContextMaterialError::CorruptState)?;
            let response_ref = content_ref_from_columns(
                row.try_get("response_content_id")?,
                row.try_get("response_media_type")?,
                row.try_get("response_byte_length")?,
                row.try_get("response_content_hash")?,
            )?;
            if response_ref != completion.response_ref {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            let reasoning = continuation_reasoning(&row)?;
            let call_id = ToolCallId::new(row.try_get::<String, _>("tool_call_id")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let source_ordinal = u32::try_from(row.try_get::<i32, _>("ordinal")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let provider_replay_ref = content_ref_from_optional_columns(
                row.try_get("provider_replay_content_id")?,
                row.try_get("provider_replay_media_type")?,
                row.try_get("provider_replay_byte_length")?,
                row.try_get("provider_replay_content_hash")?,
            )?;
            match (
                &provider_replay_ref,
                row.try_get::<Option<Vec<u8>>, _>("provider_replay_persisted_hash")?,
                row.try_get::<Option<String>, _>("provider_replay_persisted_bytes")?,
            ) {
                (None, None, None) => {}
                (Some(reference), Some(hash), Some(bytes)) => {
                    let persisted_hash = Sha256::from_bytes(hash)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                    if reference.sha256 != persisted_hash
                        || reference.byte_length != parse_u64(bytes)?
                    {
                        return Err(PostgresContextMaterialError::CorruptState);
                    }
                }
                _ => return Err(PostgresContextMaterialError::CorruptState),
            }
            let call = CanonicalToolCall {
                call_id: call_id.clone(),
                name: WireName::new(row.try_get::<String, _>("wire_name")?)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                arguments: serde_to_json(row.try_get("arguments_json")?)?,
                provider_item_ref: None,
                provider_replay_ref,
            };
            // The query already removes an entire incomplete exchange when its
            // owning Run ended unsuccessfully. Any incomplete row that remains
            // belongs to a completed or non-terminal Run and is therefore a
            // real invariant violation rather than recoverable audit residue.
            row.try_get::<Option<i64>, _>("completed_at_ms")?
                .ok_or(PostgresContextMaterialError::CorruptState)?;
            let result_ref = content_ref_from_optional_columns(
                row.try_get("result_content_id")?,
                row.try_get("result_media_type")?,
                row.try_get("result_byte_length")?,
                row.try_get("result_content_hash")?,
            )?
            .ok_or(PostgresContextMaterialError::CorruptState)?;
            let tool_invocation_id = row
                .try_get::<Option<uuid::Uuid>, _>("tool_invocation_id")?
                .map(id)
                .transpose()?
                .ok_or(PostgresContextMaterialError::CorruptState)?;
            let capability_result_ref = content_ref_from_optional_columns(
                row.try_get("capability_result_content_id")?,
                row.try_get("capability_result_media_type")?,
                row.try_get("capability_result_byte_length")?,
                row.try_get("capability_result_content_hash")?,
            )?;
            let capability_result_hash = row
                .try_get::<Option<Vec<u8>>, _>("capability_result_persisted_hash")?
                .map(Sha256::from_bytes)
                .transpose()
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let terminal = facts.tool_terminals.get(&tool_invocation_id).ok_or(
                PostgresContextMaterialError::CorruptStateAt("tool_terminal_missing"),
            )?;
            let persisted_status = row
                .try_get::<Option<i16>, _>("tool_invocation_status")?
                .ok_or(PostgresContextMaterialError::CorruptState)?;
            if !terminal_result_matches(
                &terminal.outcome,
                persisted_status,
                capability_result_ref.as_ref(),
                capability_result_hash,
            ) {
                return Err(PostgresContextMaterialError::CorruptStateAt(
                    "tool_terminal_result_contract",
                ));
            }
            let result = ToolResultSource {
                tool_key: row.try_get("executed_tool_key")?,
                content_ref: result_ref,
                sequence: terminal.sequence,
                record_id: terminal.record_id,
                source_ordinal,
                expected_call_id: call_id,
                expected_name: call.name.clone(),
            };
            let exchange = grouped
                .entry(invocation_id)
                .or_insert_with(|| ToolExchangeSource {
                    response_ref,
                    completion: completion.clone(),
                    reasoning: reasoning.clone(),
                    calls: Vec::new(),
                    results: Vec::new(),
                });
            if exchange.reasoning != reasoning {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            exchange.calls.push(call);
            exchange.results.push(result);
        }
        let mut entries = Vec::new();
        let mut assistant_content_ids = BTreeSet::new();
        let continuation_rows = sqlx::query(
            "SELECT invocation.invocation_id, invocation.terminal_stop_reason,
                    (to_jsonb(invocation)->>'context_erasure_job_id') IS NOT NULL AS context_erased, invocation.response_content_id,
                    invocation.model_route_ref,
                    reasoning_replay.content_id AS reasoning_replay_content_id,
                    reasoning_replay.media_type AS reasoning_replay_media_type,
                    reasoning_replay.byte_length::text AS reasoning_replay_byte_length,
                    reasoning_replay.content_hash AS reasoning_replay_content_hash,
                    invocation.reasoning_replay_content_hash AS reasoning_replay_persisted_hash,
                    invocation.reasoning_replay_content_bytes::text AS reasoning_replay_persisted_bytes,
                    response.media_type AS response_media_type,
                    response.byte_length::text AS response_byte_length,
                    response.content_hash AS response_content_hash
             FROM runtime.model_invocations AS invocation
             JOIN artifact.content_refs AS response
               ON response.tenant_id = invocation.tenant_id
              AND response.content_id = invocation.response_content_id
             LEFT JOIN artifact.content_refs AS reasoning_replay
               ON reasoning_replay.tenant_id = invocation.tenant_id
              AND reasoning_replay.content_id = invocation.reasoning_replay_content_id
              AND reasoning_replay.state = 2
             WHERE invocation.tenant_id = $1
               AND invocation.invocation_id = ANY($2)
               AND invocation.status = 2
               AND (invocation.terminal_stop_reason IN (3, 7)
                    OR (invocation.terminal_stop_reason = 1 AND (
                        EXISTS (SELECT 1 FROM runtime.runs AS owner WHERE owner.run_id = invocation.run_id AND owner.activity_kind = 6)
                        OR (invocation.invocation_id = $3 AND invocation.purpose = 1
                            AND invocation.public_output_observed AND response.byte_length > 0
                            AND (to_jsonb(invocation)->>'erasure_job_id') IS NULL
                            AND (to_jsonb(invocation)->>'context_erasure_job_id') IS NULL)
                    )))
               AND NOT EXISTS (
                   SELECT 1 FROM runtime.pending_tool_calls AS pending
                   WHERE pending.run_id = invocation.run_id
                     AND pending.invocation_id = invocation.invocation_id
               )
               AND response.state = 2
             ORDER BY invocation.started_conversation_seq, invocation.invocation_id",
        )
        .bind(tenant_id.into_uuid())
        .bind(&completed_invocation_ids)
        .bind(pending_native_reply.map(|id| id.into_uuid()))
        .fetch_all(&self.pool)
        .await?;
        let mut continuations = Vec::with_capacity(continuation_rows.len());
        for row in &continuation_rows {
            let invocation_id: InvocationId = id(row.try_get("invocation_id")?)?;
            let completion = facts
                .model_completions
                .get(&invocation_id)
                .ok_or(PostgresContextMaterialError::CorruptState)?;
            let response_ref = content_ref_from_columns(
                row.try_get("response_content_id")?,
                row.try_get("response_media_type")?,
                row.try_get("response_byte_length")?,
                row.try_get("response_content_hash")?,
            )?;
            if response_ref != completion.response_ref {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            let reasoning = continuation_reasoning(row)?;
            continuations.push((row, invocation_id, completion, response_ref, reasoning));
        }
        // One verified owner read for every continuation response and replay
        // instead of a store round trip per row.
        let continuation_references = continuations
            .iter()
            .flat_map(|(_, _, _, response_ref, reasoning)| {
                std::iter::once(response_ref)
                    .chain(reasoning.iter().map(|replay| &replay.content_ref))
            })
            .cloned()
            .collect::<Vec<_>>();
        let continuation_batch =
            ContentBatch::load(self.content.as_ref(), tenant_id, &continuation_references).await?;
        for (row, invocation_id, completion, response_ref, reasoning) in continuations {
            assistant_content_ids.insert(response_ref.content_id);
            let response_bytes = continuation_batch
                .bytes(&response_ref)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let text = std::str::from_utf8(response_bytes)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            if text.is_empty() && reasoning.is_none() {
                if row.try_get::<bool, _>("context_erased")? {
                    continue;
                }
                return Err(PostgresContextMaterialError::CorruptState);
            }
            entries.push(ContextEntry {
                history_page: None,
                runtime_events: Vec::new(),
                source_sequence: completion.sequence,
                presentation_order: ContextPresentationOrder::ledger(completion.sequence),
                source_record_ids: vec![completion.record_id],
                source_run_id: completion.run_id,
                assistant_origin: assistant_origins.get(&invocation_id).cloned(),
                message: CanonicalMessage {
                    role: CanonicalRole::Assistant,
                    content: if text.is_empty() {
                        Vec::new()
                    } else {
                        ModelText::chunked(text.to_owned())
                            .map_err(|_| PostgresContextMaterialError::CorruptState)?
                            .into_iter()
                            .map(CanonicalContentPart::Text)
                            .collect()
                    },
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                    tool_name: None,
                    reasoning,
                },
                continuation_delta_for: (matches!(
                    row.try_get::<Option<i16>, _>("terminal_stop_reason")?,
                    Some(3 | 7)
                ))
                .then_some(invocation_id),
            });
        }
        let references = grouped
            .values()
            .flat_map(|exchange| {
                std::iter::once(&exchange.response_ref)
                    .chain(exchange.reasoning.iter().map(|replay| &replay.content_ref))
                    .chain(
                        exchange
                            .calls
                            .iter()
                            .filter_map(|call| call.provider_replay_ref.as_ref()),
                    )
                    .chain(exchange.results.iter().map(|result| &result.content_ref))
            })
            .cloned()
            .collect::<Vec<_>>();
        let batch = ContentBatch::load(self.content.as_ref(), tenant_id, &references).await?;
        for (invocation_id, mut exchange) in grouped {
            // Runtime progress narration is public dialogue, never model history:
            // its text is already the assistant content of this turn.
            exchange.calls.retain(|call| {
                !musteragent_execution_context::is_runtime_progress_call(call.call_id.as_str())
            });
            exchange.results.retain(|result| {
                !musteragent_execution_context::is_runtime_progress_call(
                    result.expected_call_id.as_str(),
                )
            });
            assistant_content_ids.insert(exchange.response_ref.content_id);
            let assistant = musteragent_context::materialize_tool_turn_assistant_from_batch(
                &batch,
                &exchange.response_ref,
                exchange.calls,
                exchange.reasoning,
            )
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            entries.push(ContextEntry {
                history_page: None,
                runtime_events: Vec::new(),
                source_sequence: exchange.completion.sequence,
                presentation_order: ContextPresentationOrder::ledger(exchange.completion.sequence),
                source_record_ids: vec![exchange.completion.record_id],
                source_run_id: exchange.completion.run_id,
                assistant_origin: assistant_origins.get(&invocation_id).cloned(),
                message: assistant,
                continuation_delta_for: None,
            });
            for result in exchange.results {
                let mut message = musteragent_context::materialize_tool_message_from_batch(
                    &batch,
                    &result.content_ref,
                    "context.postgres-tool-message.v1",
                    &result.expected_call_id,
                    &result.expected_name,
                    |bytes| {
                        super::tool_codec::decode_tool_message_body(bytes)
                            .map_err(|_| musteragent_runtime_content::ContentStoreError::Invariant)
                    },
                )
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                musteragent_context::project_original_message(&result.tool_key, &mut message)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                entries.push(ContextEntry {
                    history_page: None,
                    runtime_events: Vec::new(),
                    source_sequence: result.sequence,
                    presentation_order: ContextPresentationOrder::tool_result(
                        exchange.completion.sequence,
                        result.source_ordinal,
                    ),
                    source_record_ids: vec![result.record_id],
                    source_run_id: exchange.completion.run_id,
                    assistant_origin: None,
                    message,
                    continuation_delta_for: Some(invocation_id),
                });
            }
        }
        let recovered = self
            .materialize_interrupted_exchanges(tenant_id, facts)
            .await?;
        entries.extend(recovered.entries);
        assistant_content_ids.extend(recovered.assistant_content_ids);
        Ok(MaterializedToolExchanges {
            entries,
            assistant_content_ids,
        })
    }
}

// Both Tool exchanges and no-Tool output-limit segments use the same durable
// replay integrity checks; route portability remains the Context core's job.
fn continuation_reasoning(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<ReasoningReplay>, PostgresContextMaterialError> {
    if row.try_get::<bool, _>("context_erased")? {
        return Ok(None);
    }
    let reasoning_replay_ref = content_ref_from_optional_columns(
        row.try_get("reasoning_replay_content_id")?,
        row.try_get("reasoning_replay_media_type")?,
        row.try_get("reasoning_replay_byte_length")?,
        row.try_get("reasoning_replay_content_hash")?,
    )?;
    match (
        &reasoning_replay_ref,
        row.try_get::<Option<Vec<u8>>, _>("reasoning_replay_persisted_hash")?,
        row.try_get::<Option<String>, _>("reasoning_replay_persisted_bytes")?,
    ) {
        (None, None, None) => {}
        (Some(reference), Some(hash), Some(bytes)) => {
            let persisted_hash =
                Sha256::from_bytes(hash).map_err(|_| PostgresContextMaterialError::CorruptState)?;
            if reference.media_type.as_str()
                != "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8"
                || reference.sha256 != persisted_hash
                || reference.byte_length != parse_u64(bytes)?
            {
                return Err(PostgresContextMaterialError::CorruptState);
            }
        }
        _ => return Err(PostgresContextMaterialError::CorruptState),
    }
    reasoning_replay_ref
        .map(|content_ref| {
            Ok::<ReasoningReplay, PostgresContextMaterialError>(ReasoningReplay {
                content_ref,
                source_route_ref: ModelRouteRef::from_key(
                    VersionedKey::new(row.try_get::<String, _>("model_route_ref")?)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                ),
                continuation_ref: None,
            })
        })
        .transpose()
}
