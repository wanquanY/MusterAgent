mod dialogue_source;
mod recovery_generation;
mod supervision;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::{
    COMPACTION_HISTORY_CODEC_SCHEMA, COMPACTION_PROMPT_SCHEMA, ContextCompactionRequest,
};
use musteragent_identity::{
    AttemptId, AttemptNumber, ContentId, CorrelationId, FencingToken, InvocationId, LeaseId,
    UtcTimestamp,
};
use musteragent_model::SignedModelRouteHash;
use musteragent_run::{AttemptContext, InvocationContext, WorkerId};
use musteragent_runtime_model::{
    CompleteModelInvocation, ModelContractError, ModelInvocation, ModelInvocationPurpose,
    ModelInvocationRouteRole, ModelInvocationStatus, ModelInvocationStore,
    ModelPersistenceDisposition, ModelRouteRef,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use sqlx::Row;
use thiserror::Error;

use crate::model::{purpose_code, route_role_code, status_code};
use crate::{
    PersistedModelInvocationRecovery, PostgresModelInvocationError, PostgresModelInvocationStore,
};

const SEALED_NOT_DISPATCHED: i16 = 1;
const DISPATCHED: i16 = 2;

#[derive(Clone)]
pub struct PostgresContextCompactionSlotStore {
    database: crate::PostgresDatabaseRuntime,
    model: PostgresModelInvocationStore,
    liveness: Option<Arc<crate::PostgresFencedExecutionLivenessJournal>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PersistedContextCompactionSlot {
    pub invocation: InvocationContext,
    pub request_ref: ContentRef,
    pub request_hash: Sha256,
    pub model_route_ref: ModelRouteRef,
    pub started_at: UtcTimestamp,
    pub dispatch_state: ContextCompactionDispatchState,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextCompactionDispatchState {
    SealedNotDispatched,
    Dispatched,
    Terminal,
}

impl PostgresContextCompactionSlotStore {
    pub async fn usage_attribution(
        &self,
        run: musteragent_identity::RunId,
    ) -> Result<musteragent_runtime_model::ModelUsageAttribution, PostgresModelInvocationError>
    {
        crate::model::usage_attribution(&mut *self.database.pool().acquire().await?, run).await
    }

    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        Self {
            model: PostgresModelInvocationStore::new(database.clone(), reference_keys),
            database,
            liveness: None,
        }
    }

    /// Loads and, only while still pre-dispatch, transfers a sealed logical
    /// slot to the current fenced Attempt. No candidate physical request is
    /// encoded on this recovery path.
    pub async fn load_or_claim(
        &self,
        request: &ContextCompactionRequest,
    ) -> Result<Option<PersistedContextCompactionSlot>, PostgresContextCompactionSlotError> {
        validate_request_contract(request)?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Context)
            .await?;
        validate_fence(&mut transaction, request).await?;
        let Some(row) = load_slot_for_update(&mut transaction, request).await? else {
            transaction.commit().await?;
            return Ok(None);
        };
        verify_slot(&row, request)?;
        claim_if_sealed(&mut transaction, &row, request).await?;
        let slot = decode_slot(&row, &request.invocation)?;
        transaction.commit().await?;
        Ok(Some(slot))
    }

    /// Atomically binds a semantic compaction plan, its original canonical
    /// request and one physical model invocation. A concurrent winner is
    /// loaded through the same strict fingerprint comparison.
    pub async fn seal_or_claim(
        &self,
        request: &ContextCompactionRequest,
        invocation: &ModelInvocation,
    ) -> Result<PersistedContextCompactionSlot, PostgresContextCompactionSlotError> {
        validate_request_contract(request)?;
        validate_new_invocation(request, invocation)?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Context)
            .await?;
        let now_ms = validate_fence(&mut transaction, request).await?;
        if let Some(row) = load_slot_for_update(&mut transaction, request).await? {
            verify_slot(&row, request)?;
            claim_if_sealed(&mut transaction, &row, request).await?;
            let slot = decode_slot(&row, &request.invocation)?;
            transaction.commit().await?;
            return Ok(slot);
        }
        insert_model_invocation(&mut transaction, invocation).await?;
        insert_slot(&mut transaction, request, invocation, now_ms).await?;
        transaction.commit().await?;
        Ok(PersistedContextCompactionSlot {
            invocation: request.invocation.clone(),
            request_ref: invocation.request_ref.clone(),
            request_hash: invocation.request_hash,
            model_route_ref: invocation.model_route_ref.clone(),
            started_at: invocation.started_at,
            dispatch_state: ContextCompactionDispatchState::SealedNotDispatched,
        })
    }

    /// The Provider may be called only by the owner that durably applies this
    /// transition. Duplicate/non-owner observations never dispatch.
    pub async fn mark_dispatched(
        &self,
        request: &ContextCompactionRequest,
    ) -> Result<ModelPersistenceDisposition, PostgresContextCompactionSlotError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Context)
            .await?;
        let now_ms = validate_fence(&mut transaction, request).await?;
        let update = sqlx::query(
            "UPDATE runtime.context_compaction_slots
             SET dispatch_state = $2, updated_at_ms = $3
             WHERE slot_id = $1
               AND owner_attempt_id = $4
               AND owner_fencing_token = $5::bigint
               AND dispatch_state = $6
               AND EXISTS (SELECT 1 FROM runtime.model_invocations AS model WHERE model.invocation_id=slot_id AND model.status=1)",
        )
        .bind(request.invocation.invocation_id.into_uuid())
        .bind(DISPATCHED)
        .bind(now_ms)
        .bind(request.invocation.attempt_context.attempt_id.into_uuid())
        .bind(
            request
                .invocation
                .attempt_context
                .fencing_token
                .get()
                .to_string(),
        )
        .bind(SEALED_NOT_DISPATCHED)
        .execute(&mut *transaction)
        .await?;
        if update.rows_affected() == 1 {
            transaction.commit().await?;
            return Ok(ModelPersistenceDisposition::Applied);
        }
        let state = sqlx::query_scalar::<_, i16>(
            "SELECT dispatch_state
             FROM runtime.context_compaction_slots
             WHERE slot_id = $1",
        )
        .bind(request.invocation.invocation_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        transaction.rollback().await?;
        if state == Some(DISPATCHED) {
            Ok(ModelPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresContextCompactionSlotError::FenceConflict)
        }
    }

    pub async fn load_recovery(
        &self,
        invocation_id: InvocationId,
    ) -> Result<Option<PersistedModelInvocationRecovery>, PostgresContextCompactionSlotError> {
        self.model
            .load_recovery(invocation_id)
            .await
            .map_err(Into::into)
    }

    pub async fn complete(
        &self,
        completion: CompleteModelInvocation,
    ) -> Result<ModelPersistenceDisposition, PostgresContextCompactionSlotError> {
        self.model.complete(completion).await.map_err(Into::into)
    }
}

async fn validate_fence(
    transaction: &mut sqlx::PgConnection,
    request: &ContextCompactionRequest,
) -> Result<i64, PostgresContextCompactionSlotError> {
    let run = &request.invocation.run_context;
    let attempt = &request.invocation.attempt_context;
    let now_ms = sqlx::query_scalar::<_, i64>(
        "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
         FROM runtime.runs AS run
         JOIN runtime.attempts AS attempt
           ON attempt.tenant_id = run.tenant_id
          AND attempt.run_id = run.run_id
         JOIN runtime.leases AS lease
           ON lease.tenant_id = run.tenant_id
          AND lease.run_id = run.run_id
          AND lease.attempt_id = attempt.attempt_id
         WHERE run.tenant_id = $1
           AND run.conversation_id = $2
           AND run.run_id = $3
           AND run.home_epoch = $4::bigint
           AND run.status IN (2, 4)
           AND attempt.attempt_id = $5
           AND attempt.attempt_number = $6
           AND attempt.status = 1
           AND lease.worker_id = $7
           AND lease.lease_id = $8
           AND lease.fencing_token = $9::bigint
           AND lease.expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
         FOR UPDATE OF run, attempt, lease",
    )
    .bind(run.tenant_id.into_uuid())
    .bind(run.conversation_id.into_uuid())
    .bind(run.run_id.into_uuid())
    .bind(run.home_epoch.get().to_string())
    .bind(attempt.attempt_id.into_uuid())
    .bind(i32::try_from(attempt.attempt_number.get())?)
    .bind(attempt.worker_id.as_str())
    .bind(attempt.lease_id.into_uuid())
    .bind(attempt.fencing_token.get().to_string())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresContextCompactionSlotError::FenceConflict)?;
    dialogue_source::validate(transaction, request).await?;
    Ok(now_ms)
}

async fn load_slot_for_update(
    transaction: &mut sqlx::PgConnection,
    request: &ContextCompactionRequest,
) -> Result<Option<sqlx::postgres::PgRow>, PostgresContextCompactionSlotError> {
    Ok(sqlx::query(
        "SELECT slot.*,
                slot.ledger_cutoff::text AS ledger_cutoff_text,
                slot.source_sequence_from::text AS source_sequence_from_text,
                slot.source_sequence_to::text AS source_sequence_to_text,
                slot.sealed_fencing_token::text AS sealed_fencing_token_text,
                invocation.request_hash, invocation.started_at_ms, invocation.status AS invocation_status,
                invocation.model_route_ref AS invocation_route_ref,
                content.content_id, content.media_type,
                content.byte_length::text AS request_byte_length,
                content.content_hash AS request_content_hash
         FROM runtime.context_compaction_slots AS slot
         JOIN runtime.model_invocations AS invocation
           ON invocation.invocation_id = slot.invocation_id
          AND invocation.tenant_id = slot.tenant_id
          AND invocation.run_id = slot.run_id
         JOIN artifact.content_refs AS content
           ON content.tenant_id = slot.tenant_id
          AND content.content_id = invocation.request_content_id
          AND content.state = 2
         WHERE slot.tenant_id = $1
           AND slot.run_id = $2
           AND slot.loop_iteration = $3
           AND slot.generation = $4
           AND slot.source_key = $5
         FOR UPDATE OF slot",
    )
    .bind(request.invocation.run_context.tenant_id.into_uuid())
    .bind(request.invocation.run_context.run_id.into_uuid())
    .bind(i32::try_from(request.slot.loop_iteration)?)
    .bind(i32::try_from(request.slot.generation.get())?)
    .bind(request.slot.source.key())
    .fetch_optional(&mut *transaction)
    .await?)
}

fn verify_slot(
    row: &sqlx::postgres::PgRow,
    request: &ContextCompactionRequest,
) -> Result<(), PostgresContextCompactionSlotError> {
    let slot = &request.slot;
    let route_hash = SignedModelRouteHash::digest(&request.model.route)?;
    let summarized = request
        .summarized_record_ids
        .iter()
        .map(|id| id.into_uuid())
        .collect::<Vec<_>>();
    let retained = slot
        .retained_record_ids
        .iter()
        .map(|id| id.into_uuid())
        .collect::<Vec<_>>();
    let previous_id = slot
        .previous_summary_ref
        .as_ref()
        .map(|reference| reference.content_id.into_uuid());
    let previous_hash = slot
        .previous_summary_ref
        .as_ref()
        .map(|reference| reference.sha256.into_bytes().to_vec());
    let matches = row.try_get::<uuid::Uuid, _>("tenant_id")?
        == request.invocation.run_context.tenant_id.into_uuid()
        && row.try_get::<uuid::Uuid, _>("conversation_id")?
            == request.invocation.run_context.conversation_id.into_uuid()
        && row.try_get::<uuid::Uuid, _>("run_id")?
            == request.invocation.run_context.run_id.into_uuid()
        && row.try_get::<i32, _>("loop_iteration")? == i32::try_from(slot.loop_iteration)?
        && row.try_get::<i32, _>("generation")? == i32::try_from(slot.generation.get())?
        && row.try_get::<Vec<u8>, _>("plan_fingerprint")? == slot.plan_fingerprint.into_bytes()
        && row.try_get::<Option<uuid::Uuid>, _>("active_branch_id")?
            == slot.active_branch_id.map(|id| id.into_uuid())
        && parse_u64(row.try_get("ledger_cutoff_text")?)? == slot.ledger_cutoff.get()
        && parse_u64(row.try_get("source_sequence_from_text")?)? == slot.source_seq_from.get()
        && parse_u64(row.try_get("source_sequence_to_text")?)? == slot.source_seq_to.get()
        && row.try_get::<Vec<uuid::Uuid>, _>("summarized_record_ids")? == summarized
        && row.try_get::<Vec<uuid::Uuid>, _>("retained_record_ids")? == retained
        && row.try_get::<Vec<u8>, _>("serialized_history_hash")?
            == Sha256::digest(&request.serialized_history).into_bytes()
        && row.try_get::<Option<uuid::Uuid>, _>("previous_summary_content_id")? == previous_id
        && row.try_get::<Option<Vec<u8>>, _>("previous_summary_hash")? == previous_hash
        && row.try_get::<String, _>("model_route_ref")? == request.model.route.route_ref.as_str()
        && row.try_get::<Vec<u8>, _>("route_identity_hash")? == route_hash.into_bytes()
        && row.try_get::<String, _>("credential_grant_ref")?
            == request.model.credential_grant_ref.as_str()
        && parse_i64_u64(row.try_get("input_budget")?)? == slot.input_budget
        && parse_i64_u64(row.try_get("input_tokens_before")?)? == slot.input_tokens_before
        && parse_i64_u64(row.try_get("summary_budget")?)? == slot.summary_budget
        && parse_i64_u64(row.try_get("max_output_tokens")?)? == request.max_output_tokens.get()
        && row.try_get::<String, _>("prompt_schema")? == COMPACTION_PROMPT_SCHEMA
        && row.try_get::<String, _>("codec_schema")? == COMPACTION_HISTORY_CODEC_SCHEMA
        && row.try_get::<String, _>("compaction_policy_ref")?
            == slot.compaction_policy_ref.as_str()
        && row.try_get::<uuid::Uuid, _>("causation_id")?
            == request.invocation.causation_id.into_uuid()
        && row.try_get::<uuid::Uuid, _>("invocation_id")?
            == request.invocation.invocation_id.into_uuid();
    if matches {
        Ok(())
    } else {
        Err(PostgresContextCompactionSlotError::IdempotencyConflict)
    }
}

async fn claim_if_sealed(
    transaction: &mut sqlx::PgConnection,
    row: &sqlx::postgres::PgRow,
    request: &ContextCompactionRequest,
) -> Result<(), PostgresContextCompactionSlotError> {
    if row.try_get::<i16, _>("dispatch_state")? != SEALED_NOT_DISPATCHED {
        return Ok(());
    }
    let attempt = &request.invocation.attempt_context;
    sqlx::query(
        "UPDATE runtime.context_compaction_slots
         SET owner_attempt_id = $2, owner_fencing_token = $3::bigint,
             updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
         WHERE slot_id = $1 AND dispatch_state = $4",
    )
    .bind(row.try_get::<uuid::Uuid, _>("slot_id")?)
    .bind(attempt.attempt_id.into_uuid())
    .bind(attempt.fencing_token.get().to_string())
    .bind(SEALED_NOT_DISPATCHED)
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

fn decode_slot(
    row: &sqlx::postgres::PgRow,
    candidate: &InvocationContext,
) -> Result<PersistedContextCompactionSlot, PostgresContextCompactionSlotError> {
    let sealed_attempt = AttemptContext {
        attempt_id: AttemptId::from_uuid(row.try_get("sealed_attempt_id")?)?,
        run_id: candidate.run_context.run_id,
        attempt_number: AttemptNumber::new(u32::try_from(
            row.try_get::<i32, _>("sealed_attempt_number")?,
        )?)
        .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
        worker_id: WorkerId::new(row.try_get::<String, _>("sealed_worker_id")?)?,
        lease_id: LeaseId::from_uuid(row.try_get("sealed_lease_id")?)?,
        fencing_token: FencingToken::new(parse_u64(row.try_get("sealed_fencing_token_text")?)?)
            .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
        lease_expires_at: UtcTimestamp::from_unix_millis(
            row.try_get("sealed_lease_expires_at_ms")?,
        ),
    };
    let request_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("request_hash")?)?;
    let content_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("request_content_hash")?)?;
    if request_hash != content_hash {
        return Err(PostgresContextCompactionSlotError::CorruptState);
    }
    let dispatch_state = if row.try_get::<i16, _>("invocation_status")? != 1 {
        ContextCompactionDispatchState::Terminal
    } else {
        match row.try_get::<i16, _>("dispatch_state")? {
            SEALED_NOT_DISPATCHED => ContextCompactionDispatchState::SealedNotDispatched,
            DISPATCHED => ContextCompactionDispatchState::Dispatched,
            _ => return Err(PostgresContextCompactionSlotError::CorruptState),
        }
    };
    Ok(PersistedContextCompactionSlot {
        invocation: InvocationContext {
            invocation_id: InvocationId::from_uuid(row.try_get("invocation_id")?)?,
            run_context: candidate.run_context,
            attempt_context: sealed_attempt,
            parent_invocation_id: None,
            causation_id: musteragent_identity::LedgerRecordId::from_uuid(
                row.try_get("causation_id")?,
            )?,
            correlation_id: CorrelationId::from_uuid(row.try_get("correlation_id")?)?,
            timing: musteragent_persistence_codec::decode_invocation_timing_columns(
                musteragent_persistence_codec::EncodedInvocationTiming {
                    legacy_deadline_ms: row.try_get("sealed_deadline_ms")?,
                    activity: row.try_get("activity_timing")?,
                    activity_hash: row.try_get("activity_timing_hash")?,
                },
            )
            .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
        },
        request_ref: ContentRef {
            content_id: ContentId::from_uuid(row.try_get("content_id")?)?,
            media_type: MediaType::new(row.try_get::<String, _>("media_type")?)?,
            byte_length: parse_u64(row.try_get("request_byte_length")?)?,
            sha256: request_hash,
        },
        request_hash,
        model_route_ref: ModelRouteRef::from_key(
            musteragent_identity::VersionedKey::new(
                row.try_get::<String, _>("invocation_route_ref")?,
            )
            .map_err(|_| PostgresContextCompactionSlotError::CorruptState)?,
        ),
        started_at: UtcTimestamp::from_unix_millis(row.try_get("started_at_ms")?),
        dispatch_state,
    })
}

async fn insert_model_invocation(
    transaction: &mut sqlx::PgConnection,
    invocation: &ModelInvocation,
) -> Result<(), PostgresContextCompactionSlotError> {
    let inserted = sqlx::query(
        "INSERT INTO runtime.model_invocations (
             invocation_id, tenant_id, conversation_id, run_id, attempt_id,
             purpose, route_role, status, model_route_ref, route_identity_hash,
             credential_grant_ref, requested_model_key, pricing_version_ref,
             idempotency_key, request_content_id, request_hash,
             continuation_input_ref, started_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5,
             $6, $7, $8, $9, $10,
             $11, $12, $13, $14, $15, $16, NULL, $17
         ) ON CONFLICT DO NOTHING",
    )
    .bind(invocation.invocation_id.into_uuid())
    .bind(invocation.tenant_id.into_uuid())
    .bind(invocation.conversation_id.into_uuid())
    .bind(invocation.run_id.into_uuid())
    .bind(invocation.attempt_id.into_uuid())
    .bind(purpose_code(invocation.purpose))
    .bind(route_role_code(invocation.route_role))
    .bind(status_code(invocation.status))
    .bind(invocation.model_route_ref.as_str())
    .bind(invocation.route_identity_hash.into_bytes().to_vec())
    .bind(invocation.credential_grant_ref.as_str())
    .bind(invocation.requested_model_key.as_str())
    .bind(invocation.pricing_version_ref.as_str())
    .bind(invocation.idempotency_key.as_str())
    .bind(invocation.request_ref.content_id.into_uuid())
    .bind(invocation.request_hash.into_bytes().to_vec())
    .bind(invocation.started_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    if inserted.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresContextCompactionSlotError::IdempotencyConflict)
    }
}

async fn insert_slot(
    transaction: &mut sqlx::PgConnection,
    request: &ContextCompactionRequest,
    invocation: &ModelInvocation,
    now_ms: i64,
) -> Result<(), PostgresContextCompactionSlotError> {
    let slot = &request.slot;
    let attempt = &request.invocation.attempt_context;
    let route_hash = SignedModelRouteHash::digest(&request.model.route)?;
    let timing =
        musteragent_persistence_codec::encode_invocation_timing_columns(&request.invocation.timing);
    sqlx::query(
        "INSERT INTO runtime.context_compaction_slots (
             slot_id, tenant_id, conversation_id, run_id, loop_iteration, generation,
             plan_fingerprint, active_branch_id, ledger_cutoff,
             source_sequence_from, source_sequence_to, summarized_record_ids,
             retained_record_ids, serialized_history_hash,
             previous_summary_content_id, previous_summary_hash,
             model_route_ref, route_identity_hash, credential_grant_ref,
             input_budget, input_tokens_before, summary_budget, max_output_tokens,
             prompt_schema, codec_schema, compaction_policy_ref, causation_id,
             invocation_id, correlation_id,
             sealed_attempt_id, sealed_attempt_number, sealed_worker_id,
             sealed_lease_id, sealed_fencing_token, sealed_lease_expires_at_ms,
             sealed_deadline_ms, owner_attempt_id, owner_fencing_token,
             dispatch_state, created_at_ms, updated_at_ms, logical_plan_hash, activity_timing, activity_timing_hash, source_key
         ) VALUES (
             $1, $2, $3, $4, $5, $6,
             $7, $8, $9::bigint, $10::bigint, $11::bigint, $12,
             $13, $14, $15, $16, $17, $18, $19,
             $20, $21, $22, $23, $24, $25, $26, $27,
             $1, $28, $29, $30, $31, $32, $33::bigint, $34,
             $35, $29, $33::bigint, $36, $37, $37, $38, $39, $40, $41)",
    )
    .bind(invocation.invocation_id.into_uuid())
    .bind(invocation.tenant_id.into_uuid())
    .bind(invocation.conversation_id.into_uuid())
    .bind(invocation.run_id.into_uuid())
    .bind(i32::try_from(slot.loop_iteration)?)
    .bind(i32::try_from(slot.generation.get())?)
    .bind(slot.plan_fingerprint.into_bytes().to_vec())
    .bind(slot.active_branch_id.map(|id| id.into_uuid()))
    .bind(slot.ledger_cutoff.get().to_string())
    .bind(slot.source_seq_from.get().to_string())
    .bind(slot.source_seq_to.get().to_string())
    .bind(
        request
            .summarized_record_ids
            .iter()
            .map(|id| id.into_uuid())
            .collect::<Vec<_>>(),
    )
    .bind(
        slot.retained_record_ids
            .iter()
            .map(|id| id.into_uuid())
            .collect::<Vec<_>>(),
    )
    .bind(
        Sha256::digest(&request.serialized_history)
            .into_bytes()
            .to_vec(),
    )
    .bind(
        slot.previous_summary_ref
            .as_ref()
            .map(|reference| reference.content_id.into_uuid()),
    )
    .bind(
        slot.previous_summary_ref
            .as_ref()
            .map(|reference| reference.sha256.into_bytes().to_vec()),
    )
    .bind(request.model.route.route_ref.as_str())
    .bind(route_hash.into_bytes().to_vec())
    .bind(request.model.credential_grant_ref.as_str())
    .bind(i64::try_from(slot.input_budget)?)
    .bind(i64::try_from(slot.input_tokens_before)?)
    .bind(i64::try_from(slot.summary_budget)?)
    .bind(i64::try_from(request.max_output_tokens.get())?)
    .bind(COMPACTION_PROMPT_SCHEMA)
    .bind(COMPACTION_HISTORY_CODEC_SCHEMA)
    .bind(slot.compaction_policy_ref.as_str())
    .bind(request.invocation.causation_id.into_uuid())
    .bind(request.invocation.correlation_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .bind(i32::try_from(attempt.attempt_number.get())?)
    .bind(attempt.worker_id.as_str())
    .bind(attempt.lease_id.into_uuid())
    .bind(attempt.fencing_token.get().to_string())
    .bind(attempt.lease_expires_at.unix_millis())
    .bind(timing.legacy_deadline_ms)
    .bind(SEALED_NOT_DISPATCHED)
    .bind(now_ms)
    .bind(
        request
            .logical_plan_fingerprint()
            .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)?
            .into_bytes()
            .to_vec(),
    )
    .bind(timing.activity)
    .bind(timing.activity_hash)
    .bind(slot.source.key())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

fn validate_request_contract(
    request: &ContextCompactionRequest,
) -> Result<(), PostgresContextCompactionSlotError> {
    request
        .validate()
        .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)?;
    let slot = &request.slot;
    if slot.loop_iteration == 0
        || slot.source_seq_from > slot.source_seq_to
        || slot.source_seq_to > slot.ledger_cutoff
        || slot.retained_record_ids.is_empty()
        || slot.input_budget == 0
        || slot.input_tokens_before == 0
        || slot.summary_budget == 0
        || request.invocation.attempt_context.run_id != request.invocation.run_context.run_id
    {
        return Err(PostgresContextCompactionSlotError::InvalidContract);
    }
    Ok(())
}

fn validate_new_invocation(
    request: &ContextCompactionRequest,
    invocation: &ModelInvocation,
) -> Result<(), PostgresContextCompactionSlotError> {
    invocation.validate()?;
    if invocation.invocation_id != request.invocation.invocation_id
        || invocation.tenant_id != request.invocation.run_context.tenant_id
        || invocation.conversation_id != request.invocation.run_context.conversation_id
        || invocation.run_id != request.invocation.run_context.run_id
        || invocation.attempt_id != request.invocation.attempt_context.attempt_id
        || invocation.purpose != ModelInvocationPurpose::ContextCompaction
        || invocation.route_role
            != ModelInvocationRouteRole::Auxiliary(
                musteragent_model::AuxiliaryModelRole::Compression,
            )
        || invocation.status != ModelInvocationStatus::Started
    {
        return Err(PostgresContextCompactionSlotError::InvalidContract);
    }
    Ok(())
}

fn parse_u64(value: String) -> Result<u64, PostgresContextCompactionSlotError> {
    value
        .parse()
        .map_err(|_| PostgresContextCompactionSlotError::CorruptState)
}

fn parse_i64_u64(value: i64) -> Result<u64, PostgresContextCompactionSlotError> {
    u64::try_from(value).map_err(Into::into)
}

#[derive(Debug, Error)]
pub enum PostgresContextCompactionSlotError {
    #[error("Context Compression supervision is unavailable")]
    SupervisionUnavailable,
    #[error("Context Compression slot contract is invalid")]
    InvalidContract,
    #[error("Context Compression slot plan fingerprint conflicts")]
    IdempotencyConflict,
    #[error("Context Compression slot lost its active Attempt fence")]
    FenceConflict,
    #[error("Context Compression slot is corrupt")]
    CorruptState,
    #[error(transparent)]
    ModelContract(#[from] ModelContractError),
    #[error(transparent)]
    ModelStore(#[from] PostgresModelInvocationError),
    #[error(transparent)]
    ModelRoute(#[from] musteragent_model::ModelRouteEncodingError),
    #[error(transparent)]
    Digest(#[from] musteragent_content_ref::DigestLengthError),
    #[error(transparent)]
    MediaType(#[from] musteragent_content_ref::MediaTypeError),
    #[error(transparent)]
    Identity(#[from] musteragent_identity::IdentityError),
    #[error(transparent)]
    Worker(#[from] musteragent_run::WorkerIdError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Context Compaction Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Numeric(#[from] std::num::TryFromIntError),
}
