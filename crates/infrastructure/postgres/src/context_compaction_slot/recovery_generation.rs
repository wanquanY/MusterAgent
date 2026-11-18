use super::*;
use musteragent_context::{
    CompactionGenerationObservation, CompactionGenerationState, ContextPlanError,
};
use musteragent_runtime_contract::ErrorCode;
use std::num::NonZeroU32;

impl PostgresContextCompactionSlotStore {
    pub async fn latest_generation(
        &self,
        request: &ContextCompactionRequest,
    ) -> Result<Option<CompactionGenerationObservation>, PostgresContextCompactionSlotError> {
        self.observe_generation(request, false).await
    }
    pub async fn latest_role_candidate(
        &self,
        request: &ContextCompactionRequest,
    ) -> Result<Option<CompactionGenerationObservation>, PostgresContextCompactionSlotError> {
        self.observe_generation(request, true).await
    }
    async fn observe_generation(
        &self,
        request: &ContextCompactionRequest,
        exact_role_candidate: bool,
    ) -> Result<Option<CompactionGenerationObservation>, PostgresContextCompactionSlotError> {
        validate_request_contract(request)?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Context)
            .await?;
        validate_fence(&mut transaction, request).await?;
        let row = sqlx::query("SELECT MAX(slot.generation) OVER () AS generation_high_watermark, slot.invocation_id, slot.correlation_id, slot.generation, slot.plan_fingerprint, slot.logical_plan_hash, slot.ledger_cutoff::text AS ledger_cutoff, slot.input_tokens_before, MIN(slot.input_tokens_before) OVER () AS minimum_estimate, MAX(slot.input_tokens_before) OVER () AS maximum_estimate, COUNT(*) OVER () AS node_count, COUNT(slot.input_tokens_before) OVER () AS estimate_count, slot.owner_attempt_id, slot.owner_fencing_token::text AS owner_fence, slot.sealed_deadline_ms, slot.activity_timing, slot.activity_timing_hash, slot.dispatch_state, invocation.status, invocation.terminal_error, invocation.early_provider_cause, invocation.terminal_error_retryable, invocation.retry_not_before_ms FROM runtime.context_compaction_slots AS slot JOIN runtime.model_invocations AS invocation ON invocation.invocation_id = slot.invocation_id AND invocation.tenant_id = slot.tenant_id AND invocation.run_id = slot.run_id WHERE slot.run_id = $1 AND slot.tenant_id = $2 AND slot.loop_iteration = $3 AND slot.source_key = $6 ORDER BY CASE WHEN $4 AND slot.logical_plan_hash = $5 THEN 0 ELSE 1 END, slot.generation DESC LIMIT 1")
            .bind(request.invocation.run_context.run_id.into_uuid())
            .bind(request.invocation.run_context.tenant_id.into_uuid())
            .bind(i32::try_from(request.slot.loop_iteration)?)
            .bind(exact_role_candidate || musteragent_runtime_snapshot::uses_provider_capacity_recovery(request.slot.compaction_policy_ref.as_str()))
            .bind(request.logical_plan_fingerprint().map_err(|_| PostgresContextCompactionSlotError::CorruptState)?.into_bytes().to_vec()).bind(request.slot.source.key()).fetch_optional(&mut *transaction).await?;
        let now = UtcTimestamp::from_unix_millis(
            sqlx::query_scalar::<_, i64>(
                "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint",
            )
            .fetch_one(&mut *transaction)
            .await?,
        );
        let (mut result, changed_estimate, unknown_estimate) = match row {
            None => (None, false, false),
            Some(row) => {
                let estimate = i64::try_from(request.slot.input_tokens_before)?;
                let changed = row.try_get::<Option<i64>, _>("minimum_estimate")?.as_ref()
                    != Some(&estimate)
                    || row.try_get::<Option<i64>, _>("maximum_estimate")?.as_ref()
                        != Some(&estimate);
                let unknown = row.try_get::<i64, _>("node_count")?
                    != row.try_get::<i64, _>("estimate_count")?;
                (Some(decode_observation(row, now)?), changed, unknown)
            }
        };
        // Exact v1 lookup is the ordinary path. Only a changed primary
        // estimate requires reconstructing older, still immutable chunk nodes.
        let exact_match = result
            .as_ref()
            .map(|latest| request.matches_sealed_plan(latest))
            .transpose()
            .map_err(|_| PostgresContextCompactionSlotError::IdempotencyConflict)?
            .unwrap_or(false);
        if let Some(latest) = result.as_ref()
            && (changed_estimate || unknown_estimate)
            && (!exact_match || latest.generation < latest.generation_high_watermark)
        {
            let high_watermark = latest.generation_high_watermark;
            // An older exact-before success is not proof that a newer call of
            // the same summary operation stopped. Inspect only newer nodes in
            // that case, preserving the ordinary latest-exact fast path.
            let after_generation = if exact_match {
                latest.generation.get()
            } else {
                0
            };
            let rows = sqlx::query("SELECT MAX(slot.generation) OVER () AS generation_high_watermark, slot.invocation_id, slot.correlation_id, slot.generation, slot.plan_fingerprint, slot.logical_plan_hash, slot.ledger_cutoff::text AS ledger_cutoff, slot.input_tokens_before, MIN(slot.input_tokens_before) OVER () AS minimum_estimate, MAX(slot.input_tokens_before) OVER () AS maximum_estimate, COUNT(*) OVER () AS node_count, COUNT(slot.input_tokens_before) OVER () AS estimate_count, slot.owner_attempt_id, slot.owner_fencing_token::text AS owner_fence, slot.sealed_deadline_ms, slot.activity_timing, slot.activity_timing_hash, slot.dispatch_state, invocation.status, invocation.terminal_error, invocation.early_provider_cause, invocation.terminal_error_retryable, invocation.retry_not_before_ms FROM runtime.context_compaction_slots AS slot JOIN runtime.model_invocations AS invocation ON invocation.invocation_id = slot.invocation_id AND invocation.tenant_id = slot.tenant_id AND invocation.run_id = slot.run_id WHERE slot.run_id = $1 AND slot.tenant_id = $2 AND slot.loop_iteration = $3 AND slot.source_key = $4 AND (slot.input_tokens_before IS NULL OR slot.input_tokens_before <> $5) AND slot.generation > $6 ORDER BY slot.generation DESC LIMIT $7")
                .bind(request.invocation.run_context.run_id.into_uuid())
                .bind(request.invocation.run_context.tenant_id.into_uuid())
                .bind(i32::try_from(request.slot.loop_iteration)?)
                .bind(request.slot.source.key())
                .bind(i64::try_from(request.slot.input_tokens_before)?)
                .bind(i32::try_from(after_generation)?)
                .bind((MAX_ESTIMATE_RECOVERY_NODES + 1) as i64)
                .fetch_all(&mut *transaction).await?;
            let truncated = rows.len() > MAX_ESTIMATE_RECOVERY_NODES;
            let mut matched = None;
            for row in rows.into_iter().take(MAX_ESTIMATE_RECOVERY_NODES) {
                let mut candidate = decode_observation(row, now)?;
                // The subset is not a new generation domain.
                candidate.generation_high_watermark = high_watermark;
                if request
                    .matches_sealed_plan(&candidate)
                    .map_err(|_| PostgresContextCompactionSlotError::IdempotencyConflict)?
                {
                    matched = Some(candidate);
                    break;
                }
                if candidate.sealed_input_tokens_before.is_none() {
                    // A newer legacy node without its estimate cannot be
                    // proven to represent a different operation.
                    return Err(PostgresContextCompactionSlotError::IdempotencyConflict);
                }
            }
            if let Some(original) = matched {
                result = Some(original);
            } else if truncated {
                // An incomplete search cannot authorize another paid call.
                return Err(PostgresContextCompactionSlotError::IdempotencyConflict);
            }
        }
        if let Some(observation) = &mut result
            && matches!(observation.timing, musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(_))
            && matches!(observation.state, CompactionGenerationState::Failed(_))
            && !crate::execution_liveness::auxiliary_stop_confirmed(&mut transaction, request.invocation.run_context.tenant_id, request.invocation.run_context.run_id, observation.invocation_id).await.map_err(|_| PostgresContextCompactionSlotError::CorruptState)?
        {
            observation.state = CompactionGenerationState::DispatchedUnknown;
        }
        transaction.commit().await?;
        if let Some(observation) = &result
            && matches!(observation.timing, musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(_))
            && matches!(observation.state, CompactionGenerationState::DispatchedUnknown | CompactionGenerationState::Cancelled)
            && observation.owner_attempt_id != request.invocation.attempt_context.attempt_id
            && observation.owner_fencing_token < request.invocation.attempt_context.fencing_token
        {
            // Match Device recovery: close only the lost observation, outside
            // the slot transaction, retaining the unknown physical outcome.
            self.liveness.as_ref().ok_or(PostgresContextCompactionSlotError::CorruptState)?
                .recover_orphaned_model(&request.invocation.attempt_context, observation.invocation_id)
                .await.map_err(super::supervision::liveness_error)?;
        }
        Ok(result)
    }
}

// Metadata only; never read arbitrary history or retry an ambiguous provider.
const MAX_ESTIMATE_RECOVERY_NODES: usize = 256;

fn decode_observation(
    row: sqlx::postgres::PgRow,
    now: UtcTimestamp,
) -> Result<CompactionGenerationObservation, PostgresContextCompactionSlotError> {
    let state = match row.try_get::<i16, _>("status")? {
        1 if row.try_get::<i16, _>("dispatch_state")? == SEALED_NOT_DISPATCHED => {
            CompactionGenerationState::SealedNotDispatched
        }
        1 if row.try_get::<i16, _>("dispatch_state")? == DISPATCHED => {
            CompactionGenerationState::DispatchedUnknown
        }
        2 => CompactionGenerationState::Succeeded,
        3 => {
            let bytes = row
                .try_get::<Option<Vec<u8>>, _>("terminal_error")?
                .ok_or(PostgresContextCompactionSlotError::CorruptState)?;
            CompactionGenerationState::Failed(ContextPlanError {
                early_provider_cause: row
                    .try_get::<Option<String>, _>("early_provider_cause")?
                    .as_deref()
                    .map(|cause| {
                        musteragent_runtime_model::ProviderErrorCode::from_alternate_provider_cause(
                            cause,
                        )
                        .ok_or(PostgresContextCompactionSlotError::CorruptState)
                    })
                    .transpose()?,
                retry_after_ms: musteragent_runtime_model::remaining_model_retry_delay(
                    now,
                    row.try_get::<Option<i64>, _>("retry_not_before_ms")?
                        .map(UtcTimestamp::from_unix_millis),
                ),
                code: std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(ErrorCode::from_safe_code)
                    .ok_or(PostgresContextCompactionSlotError::CorruptState)?,
                retryable: row
                    .try_get::<Option<bool>, _>("terminal_error_retryable")?
                    .unwrap_or(false),
            })
        }
        4 => CompactionGenerationState::Cancelled,
        _ => return Err(PostgresContextCompactionSlotError::CorruptState),
    };
    Ok(CompactionGenerationObservation {
        invocation_id: InvocationId::from_uuid(row.try_get("invocation_id")?)?,
        correlation_id: musteragent_identity::CorrelationId::from_uuid(
            row.try_get("correlation_id")?,
        )?,
        generation: u32::try_from(row.try_get::<i32, _>("generation")?)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(PostgresContextCompactionSlotError::CorruptState)?,
        generation_high_watermark: u32::try_from(
            row.try_get::<i32, _>("generation_high_watermark")?,
        )
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(PostgresContextCompactionSlotError::CorruptState)?,
        plan_fingerprint: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("plan_fingerprint")?)?,
        logical_plan_fingerprint: row
            .try_get::<Option<Vec<u8>>, _>("logical_plan_hash")?
            .map(Sha256::from_bytes)
            .transpose()?,
        sealed_input_tokens_before: row
            .try_get::<Option<i64>, _>("input_tokens_before")?
            .map(parse_i64_u64)
            .transpose()?,
        sealed_ledger_cutoff: Some(
            musteragent_identity::ConversationSeq::new(
                row.try_get::<String, _>("ledger_cutoff")?
                    .parse::<u64>()
                    .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
            )
            .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
        ),
        owner_attempt_id: AttemptId::from_uuid(row.try_get("owner_attempt_id")?)?,
        owner_fencing_token: FencingToken::new(
            row.try_get::<String, _>("owner_fence")?
                .parse::<u64>()
                .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
        )
        .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
        timing: musteragent_persistence_codec::decode_invocation_timing_columns(
            musteragent_persistence_codec::EncodedInvocationTiming {
                legacy_deadline_ms: row.try_get("sealed_deadline_ms")?,
                activity: row.try_get("activity_timing")?,
                activity_hash: row.try_get("activity_timing_hash")?,
            },
        )
        .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
        state,
    })
}
