//! Cloud governed Dream execution and crash recovery.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    HomeEpoch, IdempotencyKey, LedgerOffset, RunSeq, TenantId, UtcTimestamp,
};
use musteragent_ledger::AttemptFence;
use musteragent_loop::{AgentLoopJournal, LoopPortError, RunCompletionDisposition};
use musteragent_loop::{
    DreamAgentExchange, DreamAgentState, DreamAgentStateSource, EvolutionMaintenanceClaim,
    EvolutionMaintenanceExecutionStore, EvolutionMaintenanceMaterial, EvolutionMaintenanceTerminal,
    EvolutionMaintenanceTerminalReceipt, MaintenanceExecutionPortError,
    dream_output_from_result_message,
};
use musteragent_loop::{
    MaintenanceAdmissionContext, MaintenanceAdmissionContextSource, MaintenanceRunDependencyError,
    MaintenanceRunState, MaintenanceRunStateSource,
};
use musteragent_model::{AuxiliaryModelRole, ModelKey, SignedModelRouteHash};
use musteragent_persistence_codec::ProtobufMaintenanceSnapshotCodec;
use musteragent_run::{AttemptContext, InvocationContext, RunContext, RunStatus};
use musteragent_runtime_admission::derive_legacy_system_admission_correlation_id;
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, OpaqueExternalRef, TraceContext, TraceParent,
};
use musteragent_runtime_memory::{
    CompletedDreamModelRun, DreamModelRunRequest, MaintenanceRuntimeSnapshot,
    dream_maintenance_admission_identities, dream_maintenance_command_id,
};
use musteragent_runtime_model::{
    CanonicalMessage, CompleteModelInvocation, JsonValue, ModelInvocation, ModelInvocationPurpose,
    ModelInvocationRouteRole, ModelInvocationStatus, ModelPersistenceDisposition, ModelRouteRef,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::WireName;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row as _};

use crate::ledger::database_now_ms;
use crate::model::{
    encode_usage, insert_metering_outbox, insert_usage_fact, protect_optional, purpose_code,
    route_role_code, status_code,
};

const OUTCOME_ACTIVE: i16 = 1;
const OUTCOME_SUCCEEDED: i16 = 2;
const OUTCOME_FAILED: i16 = 3;
const OUTCOME_CANCELLED: i16 = 4;
const OUTCOME_INDETERMINATE: i16 = 5;
const TRACE_DOMAIN: &[u8] = b"musteragent.legacy.cloud.evolution-maintenance.trace.v1\0";
const DREAM_SUBMISSION_WIRE_NAME: &str = "submit_experience_candidates";

#[derive(Clone)]
pub struct PostgresEvolutionMaintenanceStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    terminal_journal: Arc<dyn AgentLoopJournal>,
    reference_keys: Arc<OpaqueRefKeyring>,
}

impl PostgresEvolutionMaintenanceStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        terminal_journal: Arc<dyn AgentLoopJournal>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            pool,
            database,
            content,
            terminal_journal,
            reference_keys,
        }
    }

    async fn claim_inner(
        &self,
        attempt: &AttemptContext,
    ) -> Result<EvolutionMaintenanceClaim, StoreFailure> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
            .await?;
        let live = lock_live_attempt(&mut transaction, attempt).await?;
        let material = load_material(&mut transaction, attempt, &live).await?;
        material
            .validate(attempt)
            .map_err(|_| StoreFailure::CorruptState)?;
        let row = sqlx::query(
            "SELECT slot.*,
                    slot.owner_fencing_token::text AS owner_fencing_token_text,
                    invocation.status AS invocation_status,
                    invocation.provider_dispatched_at_ms, invocation.started_at_ms
             FROM runtime.evolution_maintenance_executions AS slot
             JOIN runtime.model_invocations AS invocation
               ON invocation.invocation_id = slot.invocation_id
             WHERE slot.invocation_id = $1",
        )
        .bind(material.snapshot.invocation_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        let now = database_now_ms(&mut transaction).await?;
        let invocation = match row {
            None => {
                authorize_content(
                    &mut transaction,
                    material.snapshot.tenant_id,
                    &material.snapshot.model_input_ref,
                )
                .await?;
                let invocation = new_invocation(&material, now)?;
                insert_invocation(&mut transaction, &invocation).await?;
                insert_slot(&mut transaction, attempt, &material, now).await?;
                transaction.commit().await?;
                return Ok(EvolutionMaintenanceClaim::Sealed {
                    material: Box::new(material),
                    invocation: Box::new(invocation),
                });
            }
            Some(row) => {
                verify_slot_identity(&row, &material)?;
                let outcome: i16 = row.try_get("outcome_status")?;
                if outcome != OUTCOME_ACTIVE {
                    transaction.commit().await?;
                    return self.recover_terminal(attempt, &material, outcome).await;
                }
                if row
                    .try_get::<Option<i64>, _>("provider_dispatched_at_ms")?
                    .is_some()
                {
                    sqlx::query(
                        "UPDATE runtime.evolution_maintenance_executions
                         SET outcome_status = 5, updated_at_ms = $2
                         WHERE invocation_id = $1 AND outcome_status = 1",
                    )
                    .bind(material.snapshot.invocation_id.into_uuid())
                    .bind(now.unix_millis())
                    .execute(&mut *transaction)
                    .await?;
                    transaction.commit().await?;
                    self.terminalize(
                        attempt,
                        material.snapshot.invocation_id,
                        OUTCOME_INDETERMINATE,
                    )
                    .await?;
                    return Ok(EvolutionMaintenanceClaim::DispatchedUnknown);
                }
                if row.try_get::<i16, _>("invocation_status")? != 1 {
                    return Err(StoreFailure::CorruptState);
                }
                sqlx::query(
                    "UPDATE runtime.evolution_maintenance_executions
                     SET owner_attempt_id = $2, owner_fencing_token = $3::bigint,
                         updated_at_ms = $4
                     WHERE invocation_id = $1 AND outcome_status = 1",
                )
                .bind(material.snapshot.invocation_id.into_uuid())
                .bind(attempt.attempt_id.into_uuid())
                .bind(attempt.fencing_token.get().to_string())
                .bind(now.unix_millis())
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    "UPDATE runtime.model_invocations SET attempt_id = $2
                     WHERE invocation_id = $1 AND status = 1
                       AND provider_dispatched_at_ms IS NULL",
                )
                .bind(material.snapshot.invocation_id.into_uuid())
                .bind(attempt.attempt_id.into_uuid())
                .execute(&mut *transaction)
                .await?;
                let mut invocation = new_invocation(
                    &material,
                    UtcTimestamp::from_unix_millis(row.try_get("started_at_ms")?),
                )?;
                invocation.attempt_id = attempt.attempt_id;
                invocation
            }
        };
        transaction.commit().await?;
        Ok(EvolutionMaintenanceClaim::Sealed {
            material: Box::new(material),
            invocation: Box::new(invocation),
        })
    }

    async fn mark_dispatched_inner(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
    ) -> Result<ModelPersistenceDisposition, StoreFailure> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
            .await?;
        lock_live_attempt(&mut transaction, attempt).await?;
        let now = database_now_ms(&mut transaction).await?;
        let changed = sqlx::query(
            "UPDATE runtime.model_invocations AS invocation
             SET provider_dispatched_at_ms = $2
             FROM runtime.evolution_maintenance_executions AS slot
             WHERE invocation.invocation_id = $1
               AND slot.invocation_id = invocation.invocation_id
               AND slot.run_id = $3 AND slot.owner_attempt_id = $4
               AND slot.owner_fencing_token = $5::bigint
               AND slot.outcome_status = 1 AND invocation.status = 1
               AND invocation.provider_dispatched_at_ms IS NULL",
        )
        .bind(invocation_id.into_uuid())
        .bind(now.unix_millis())
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .execute(&mut *transaction)
        .await?;
        if changed.rows_affected() == 1 {
            transaction.commit().await?;
            return Ok(ModelPersistenceDisposition::Applied);
        }
        let current: Option<(i16, Option<i64>)> = sqlx::query_as(
            "SELECT slot.outcome_status, invocation.provider_dispatched_at_ms
             FROM runtime.evolution_maintenance_executions AS slot
             JOIN runtime.model_invocations AS invocation USING(invocation_id)
             WHERE slot.invocation_id = $1",
        )
        .bind(invocation_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        transaction.rollback().await?;
        match current {
            Some((_, Some(_))) | Some((OUTCOME_SUCCEEDED..=OUTCOME_INDETERMINATE, _)) => {
                Ok(ModelPersistenceDisposition::Duplicate)
            }
            Some(_) => Err(StoreFailure::IdempotencyConflict),
            None => Err(StoreFailure::CorruptState),
        }
    }

    async fn finish_inner(
        &self,
        attempt: &AttemptContext,
        terminal: EvolutionMaintenanceTerminal,
    ) -> Result<EvolutionMaintenanceTerminalReceipt, StoreFailure> {
        let (invocation_id, target) = terminal_identity(&terminal);
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
            .await?;
        lock_live_attempt(&mut transaction, attempt).await?;
        let row = sqlx::query(
            "SELECT slot.*,
                    slot.owner_fencing_token::text AS owner_fencing_token_text,
                    invocation.status AS invocation_status,
                    invocation.provider_dispatched_at_ms
             FROM runtime.evolution_maintenance_executions AS slot
             JOIN runtime.model_invocations AS invocation USING(invocation_id)
             WHERE slot.invocation_id = $1",
        )
        .bind(invocation_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StoreFailure::CorruptState)?;
        verify_owner(&row, attempt)?;
        let current: i16 = row.try_get("outcome_status")?;
        if current == OUTCOME_ACTIVE {
            persist_terminal(&self.reference_keys, &mut transaction, &row, &terminal).await?;
        } else if current != target {
            return Err(StoreFailure::IdempotencyConflict);
        }
        transaction.commit().await?;
        self.terminalize(attempt, invocation_id, target).await?;
        self.receipt_for_status(invocation_id, target).await
    }

    async fn recover_terminal(
        &self,
        attempt: &AttemptContext,
        material: &EvolutionMaintenanceMaterial,
        outcome: i16,
    ) -> Result<EvolutionMaintenanceClaim, StoreFailure> {
        self.terminalize(attempt, material.snapshot.invocation_id, outcome)
            .await?;
        Ok(match outcome {
            OUTCOME_SUCCEEDED => EvolutionMaintenanceClaim::Completed(Box::new(
                self.completed_receipt(material.snapshot.invocation_id)
                    .await?,
            )),
            OUTCOME_FAILED => EvolutionMaintenanceClaim::Failed,
            OUTCOME_CANCELLED => EvolutionMaintenanceClaim::Cancelled,
            OUTCOME_INDETERMINATE => EvolutionMaintenanceClaim::DispatchedUnknown,
            _ => return Err(StoreFailure::CorruptState),
        })
    }

    async fn terminalize(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
        outcome: i16,
    ) -> Result<(), StoreFailure> {
        match outcome {
            OUTCOME_SUCCEEDED => match self.terminal_journal.complete(attempt).await {
                Ok(RunCompletionDisposition::Completed(_)) => {}
                Ok(RunCompletionDisposition::Cancelled(_)) => {
                    return Err(StoreFailure::IdempotencyConflict);
                }
                Ok(RunCompletionDisposition::DelegationCompletionsPending(_)) => {
                    return Err(StoreFailure::IdempotencyConflict);
                }
                Ok(
                    RunCompletionDisposition::ContextRefreshPending
                    | RunCompletionDisposition::RequiredWorkPending,
                ) => {
                    return Err(StoreFailure::FenceConflict);
                }
                Ok(RunCompletionDisposition::ControlsPending) => {
                    return Err(StoreFailure::FenceConflict);
                }
                Err(error) => return Err(StoreFailure::from_loop(error.code, error.retryable)),
            },
            OUTCOME_FAILED => {
                self.terminal_journal
                    .fail(
                        attempt,
                        self.terminal_error(invocation_id).await?,
                        false,
                        None,
                        std::num::NonZeroU32::MIN.into(),
                    )
                    .await
                    .map_err(|error| StoreFailure::from_loop(error.code, error.retryable))?;
            }
            OUTCOME_CANCELLED => {
                self.terminal_journal
                    .acknowledge_cancellation(attempt, None)
                    .await
                    .map_err(|error| StoreFailure::from_loop(error.code, error.retryable))?;
            }
            OUTCOME_INDETERMINATE => {
                let reference = OpaqueExternalRef::new(format!(
                    "evolution-maintenance:{}:provider-outcome-unknown",
                    invocation_id
                ))
                .map_err(|_| StoreFailure::CorruptState)?;
                self.terminal_journal
                    .interrupt_external_outcome(attempt, reference)
                    .await
                    .map_err(|error| StoreFailure::from_loop(error.code, error.retryable))?;
            }
            _ => return Err(StoreFailure::CorruptState),
        }
        let now = system_now()?;
        sqlx::query(
            "UPDATE runtime.evolution_maintenance_executions
             SET terminalized_at_ms = COALESCE(terminalized_at_ms, $2),
                 updated_at_ms = GREATEST(updated_at_ms, $2)
             WHERE invocation_id = $1 AND outcome_status = $3",
        )
        .bind(invocation_id.into_uuid())
        .bind(now.unix_millis())
        .bind(outcome)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn terminal_error(
        &self,
        invocation_id: musteragent_identity::InvocationId,
    ) -> Result<ErrorCode, StoreFailure> {
        let bytes: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT terminal_error FROM runtime.model_invocations WHERE invocation_id = $1",
        )
        .bind(invocation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .flatten();
        bytes
            .as_deref()
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(ErrorCode::from_safe_code)
            .ok_or(StoreFailure::CorruptState)
    }

    async fn receipt_for_status(
        &self,
        invocation_id: musteragent_identity::InvocationId,
        outcome: i16,
    ) -> Result<EvolutionMaintenanceTerminalReceipt, StoreFailure> {
        Ok(match outcome {
            OUTCOME_SUCCEEDED => EvolutionMaintenanceTerminalReceipt::Completed(Box::new(
                self.completed_receipt(invocation_id).await?,
            )),
            OUTCOME_FAILED => EvolutionMaintenanceTerminalReceipt::Failed,
            OUTCOME_CANCELLED => EvolutionMaintenanceTerminalReceipt::Cancelled,
            OUTCOME_INDETERMINATE => EvolutionMaintenanceTerminalReceipt::DispatchedUnknown,
            _ => return Err(StoreFailure::CorruptState),
        })
    }

    async fn completed_receipt(
        &self,
        invocation_id: musteragent_identity::InvocationId,
    ) -> Result<CompletedDreamModelRun, StoreFailure> {
        let row = sqlx::query(
            "SELECT slot.*,
                    slot.owner_fencing_token::text AS owner_fencing_token_text,
                    run.status AS run_status,
                    invocation.model_route_ref, invocation.request_content_id,
                    invocation.request_hash, invocation.response_content_id,
                    invocation.finished_at_ms,
                    request.media_type AS request_media_type,
                    request.byte_length::text AS request_byte_length,
                    response.media_type AS response_media_type,
                    response.byte_length::text AS response_byte_length,
                    response.content_hash AS response_hash
             FROM runtime.evolution_maintenance_executions AS slot
             JOIN runtime.runs AS run ON run.run_id = slot.run_id
             JOIN runtime.model_invocations AS invocation USING(invocation_id)
             JOIN artifact.content_refs AS request
               ON request.content_id = invocation.request_content_id
              AND request.tenant_id = slot.tenant_id AND request.state = 2
             JOIN artifact.content_refs AS response
               ON response.content_id = invocation.response_content_id
              AND response.tenant_id = slot.tenant_id AND response.state = 2
             WHERE slot.invocation_id = $1",
        )
        .bind(invocation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreFailure::CorruptState)?;
        decode_completed_receipt(&row)
    }

    async fn load_state_inner(
        &self,
        request: &DreamModelRunRequest,
    ) -> Result<MaintenanceRunState, StoreFailure> {
        request.validate().map_err(|_| StoreFailure::CorruptState)?;
        let row = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                    run.runtime_snapshot_id, run.activity_kind,
                    run.status AS run_status, run.finished_at_ms,
                    convert_from(run.terminal_error, 'UTF8') AS terminal_error,
                    slot.outcome_status,
                    (SELECT attempt_id FROM runtime.attempts
                     WHERE run_id = run.run_id AND status = $1
                     ORDER BY attempt_number DESC LIMIT 1) AS terminal_attempt_id
             FROM runtime.runs AS run
             LEFT JOIN runtime.evolution_maintenance_executions AS slot
               ON slot.run_id = run.run_id
             WHERE run.run_id = $2",
        )
        .bind(crate::loop_journal::ATTEMPT_STATUS_SUCCEEDED)
        .bind(request.maintenance_run_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(MaintenanceRunState::Absent);
        };
        if row.try_get::<uuid::Uuid, _>("tenant_id")? != request.tenant_id.into_uuid()
            || row.try_get::<uuid::Uuid, _>("conversation_id")?
                != request.maintenance_conversation_id.into_uuid()
            || row.try_get::<uuid::Uuid, _>("activity_id")?
                != request.maintenance_activity_id.into_uuid()
            || row.try_get::<uuid::Uuid, _>("runtime_snapshot_id")?
                != request.maintenance_runtime_snapshot_id.into_uuid()
            || row.try_get::<i16, _>("activity_kind")? != 5
        {
            return Err(StoreFailure::CorruptState);
        }
        let run_status: i16 = row.try_get("run_status")?;
        let outcome: Option<i16> = row.try_get("outcome_status")?;
        match (run_status, outcome) {
            (5, Some(OUTCOME_SUCCEEDED)) => Ok(MaintenanceRunState::Completed(Box::new(
                self.completed_receipt(request.invocation_id).await?,
            ))),
            (5, None) => Ok(MaintenanceRunState::Completed(Box::new(
                self.completed_agent_receipt(
                    request,
                    id(row
                        .try_get::<Option<uuid::Uuid>, _>("terminal_attempt_id")?
                        .ok_or(StoreFailure::CorruptState)?)?,
                    UtcTimestamp::from_unix_millis(
                        row.try_get::<Option<i64>, _>("finished_at_ms")?
                            .ok_or(StoreFailure::CorruptState)?,
                    ),
                )
                .await?,
            ))),
            (6, None | Some(OUTCOME_FAILED)) => Ok(MaintenanceRunState::Failed {
                code: Self::terminal_run_error_code(&row)?,
            }),
            (7, None | Some(OUTCOME_CANCELLED)) => Ok(MaintenanceRunState::Cancelled),
            (8, None | Some(OUTCOME_INDETERMINATE)) => Ok(MaintenanceRunState::DispatchedUnknown),
            (1..=4, Some(OUTCOME_INDETERMINATE)) => Ok(MaintenanceRunState::DispatchedUnknown),
            (1..=4, None | Some(OUTCOME_ACTIVE..=OUTCOME_CANCELLED)) => {
                Ok(MaintenanceRunState::Pending)
            }
            _ => Err(StoreFailure::CorruptState),
        }
    }

    fn terminal_run_error_code(row: &PgRow) -> Result<ErrorCode, StoreFailure> {
        row.try_get::<Option<String>, _>("terminal_error")?
            .as_deref()
            .and_then(ErrorCode::from_safe_code)
            .ok_or(StoreFailure::CorruptState)
    }

    async fn completed_agent_receipt(
        &self,
        request: &DreamModelRunRequest,
        attempt_id: musteragent_identity::AttemptId,
        completed_at: UtcTimestamp,
    ) -> Result<CompletedDreamModelRun, StoreFailure> {
        let rows = sqlx::query(
            "SELECT pending.tool_call_id, pending.wire_name,
                    pending.result_content_id, pending.result_content_hash,
                    pending.result_content_bytes::text AS result_content_bytes,
                    result.media_type, result.byte_length::text AS stored_bytes,
                    result.content_hash AS stored_hash
             FROM runtime.pending_tool_calls AS pending
             JOIN runtime.model_invocations AS invocation
               ON invocation.run_id = pending.run_id
              AND invocation.invocation_id = pending.invocation_id
             JOIN artifact.content_refs AS result
               ON result.tenant_id = invocation.tenant_id
              AND result.content_id = pending.result_content_id
              AND result.state = 2
             WHERE pending.run_id = $1
               AND pending.completed_at_ms IS NOT NULL
               AND pending.wire_name = $2
             ORDER BY invocation.started_conversation_seq,
                      invocation.invocation_id, pending.ordinal",
        )
        .bind(request.maintenance_run_id.into_uuid())
        .bind(DREAM_SUBMISSION_WIRE_NAME)
        .fetch_all(&self.pool)
        .await?;
        let name =
            WireName::new(DREAM_SUBMISSION_WIRE_NAME).map_err(|_| StoreFailure::CorruptState)?;
        let mut accepted = None;
        for row in rows {
            let call_id =
                musteragent_identity::ToolCallId::new(row.try_get::<String, _>("tool_call_id")?)
                    .map_err(|_| StoreFailure::CorruptState)?;
            let pending_hash = row
                .try_get::<Option<Vec<u8>>, _>("result_content_hash")?
                .ok_or(StoreFailure::CorruptState)?;
            let stored_hash = row.try_get::<Vec<u8>, _>("stored_hash")?;
            let pending_bytes = row
                .try_get::<Option<String>, _>("result_content_bytes")?
                .ok_or(StoreFailure::CorruptState)?;
            let stored_bytes = row.try_get::<String, _>("stored_bytes")?;
            if pending_hash != stored_hash || pending_bytes != stored_bytes {
                return Err(StoreFailure::CorruptState);
            }
            let result_ref = ContentRef {
                content_id: id(row
                    .try_get::<Option<uuid::Uuid>, _>("result_content_id")?
                    .ok_or(StoreFailure::CorruptState)?)?,
                media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                    .map_err(|_| StoreFailure::CorruptState)?,
                byte_length: parse_u64(&pending_bytes)?,
                sha256: Sha256::from_bytes(pending_hash).map_err(|_| StoreFailure::CorruptState)?,
            };
            let bytes = self
                .content
                .read(request.tenant_id, &result_ref)
                .await
                .map_err(|_| StoreFailure::Unavailable)?;
            verify_content(&result_ref, &bytes).map_err(|_| StoreFailure::CorruptState)?;
            let message = crate::context_material::decode_tool_message(&bytes, &call_id, &name)
                .map_err(|_| StoreFailure::CorruptState)?;
            if let Some(output) = dream_output_from_result_message(&message)
                .map_err(|_| StoreFailure::CorruptState)?
                && accepted.replace(output).is_some()
            {
                return Err(StoreFailure::CorruptState);
            }
        }
        Ok(CompletedDreamModelRun {
            dream_job_id: request.dream_job_id,
            tenant_id: request.tenant_id,
            maintenance_conversation_id: request.maintenance_conversation_id,
            maintenance_command_id: request.maintenance_command_id,
            maintenance_activity_id: request.maintenance_activity_id,
            maintenance_run_id: request.maintenance_run_id,
            maintenance_runtime_snapshot_id: request.maintenance_runtime_snapshot_id,
            attempt_id,
            invocation_id: request.invocation_id,
            model_input_ref: request.model_input_ref.clone(),
            model_output_ref: accepted.ok_or(StoreFailure::CorruptState)?,
            model_route_ref: request.model_route_ref.clone(),
            activity_kind: ActivityKind::Maintenance,
            run_status: RunStatus::Completed,
            invocation_purpose: ModelInvocationPurpose::EvolutionInduction,
            route_role: ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Evolution),
            completed_at,
        })
    }

    async fn load_dream_agent_state_inner(
        &self,
        attempt: &AttemptContext,
    ) -> Result<DreamAgentState, StoreFailure> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
            .await?;
        let live = lock_live_attempt(&mut transaction, attempt).await?;
        let material = load_material(&mut transaction, attempt, &live).await?;
        material
            .validate(attempt)
            .map_err(|_| StoreFailure::CorruptState)?;
        let rows = sqlx::query(
            "SELECT pending.tool_call_id, pending.wire_name,
                    pending.arguments_json, pending.result_content_id,
                    pending.result_content_hash,
                    pending.result_content_bytes::text AS result_content_bytes,
                    result.media_type AS result_media_type,
                    result.byte_length::text AS stored_result_bytes,
                    result.content_hash AS stored_result_hash
             FROM runtime.pending_tool_calls AS pending
             JOIN runtime.model_invocations AS invocation
               ON invocation.run_id = pending.run_id
              AND invocation.invocation_id = pending.invocation_id
             JOIN artifact.content_refs AS result
               ON result.tenant_id = invocation.tenant_id
              AND result.content_id = pending.result_content_id
              AND result.state = 2
             WHERE pending.run_id = $1
               AND pending.completed_at_ms IS NOT NULL
               AND pending.wire_name = $2
             ORDER BY invocation.started_conversation_seq,
                      invocation.invocation_id, pending.ordinal",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(DREAM_SUBMISSION_WIRE_NAME)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;

        let tenant_id = material.snapshot.tenant_id;
        let expected_name =
            WireName::new(DREAM_SUBMISSION_WIRE_NAME).map_err(|_| StoreFailure::CorruptState)?;
        let mut exchanges = Vec::with_capacity(rows.len());
        for row in rows {
            let call_id =
                musteragent_identity::ToolCallId::new(row.try_get::<String, _>("tool_call_id")?)
                    .map_err(|_| StoreFailure::CorruptState)?;
            if row.try_get::<String, _>("wire_name")? != DREAM_SUBMISSION_WIRE_NAME {
                return Err(StoreFailure::CorruptState);
            }
            let result_content_id = row
                .try_get::<Option<uuid::Uuid>, _>("result_content_id")?
                .ok_or(StoreFailure::CorruptState)?;
            let persisted_hash = row
                .try_get::<Option<Vec<u8>>, _>("result_content_hash")?
                .ok_or(StoreFailure::CorruptState)?;
            let stored_hash = row.try_get::<Vec<u8>, _>("stored_result_hash")?;
            let persisted_bytes = row
                .try_get::<Option<String>, _>("result_content_bytes")?
                .ok_or(StoreFailure::CorruptState)?;
            let stored_bytes = row.try_get::<String, _>("stored_result_bytes")?;
            if persisted_hash != stored_hash || persisted_bytes != stored_bytes {
                return Err(StoreFailure::CorruptState);
            }
            let result_ref = ContentRef {
                content_id: musteragent_identity::ContentId::from_uuid(result_content_id)
                    .map_err(|_| StoreFailure::CorruptState)?,
                media_type: MediaType::new(row.try_get::<String, _>("result_media_type")?)
                    .map_err(|_| StoreFailure::CorruptState)?,
                byte_length: persisted_bytes
                    .parse()
                    .map_err(|_| StoreFailure::CorruptState)?,
                sha256: Sha256::from_bytes(persisted_hash)
                    .map_err(|_| StoreFailure::CorruptState)?,
            };
            let result_bytes = self
                .content
                .read(tenant_id, &result_ref)
                .await
                .map_err(|_| StoreFailure::Unavailable)?;
            verify_content(&result_ref, &result_bytes).map_err(|_| StoreFailure::CorruptState)?;
            let arguments: JsonValue =
                serde_to_runtime_json(row.try_get::<serde_json::Value, _>("arguments_json")?)?;
            let result: CanonicalMessage = crate::context_material::decode_tool_message(
                &result_bytes,
                &call_id,
                &expected_name,
            )
            .map_err(|_| StoreFailure::CorruptState)?;
            exchanges.push(DreamAgentExchange {
                call_id,
                arguments,
                result,
            });
        }
        Ok(DreamAgentState {
            material,
            exchanges,
        })
    }
}

impl MaintenanceAdmissionContextSource for PostgresEvolutionMaintenanceStore {
    fn resolve(
        &self,
        request: &DreamModelRunRequest,
    ) -> ApplicationFuture<'_, Result<MaintenanceAdmissionContext, MaintenanceRunDependencyError>>
    {
        let request = request.clone();
        Box::pin(async move {
            request
                .validate()
                .map_err(|_| MaintenanceRunDependencyError::InvalidResponse)?;
            let row: Option<(uuid::Uuid, String, String)> = sqlx::query_as(
                "SELECT tenant_id, home_epoch::text, last_sequence::text
                 FROM event_store.ledger_streams WHERE conversation_id = $1",
            )
            .bind(request.maintenance_conversation_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| MaintenanceRunDependencyError::Unavailable)?;
            let (home_epoch, expected_offset) = match row {
                Some((tenant, epoch, sequence)) if tenant == request.tenant_id.into_uuid() => (
                    HomeEpoch::new(parse_u64(&epoch).map_err(StoreFailure::dependency)?)
                        .map_err(|_| MaintenanceRunDependencyError::InvalidResponse)?,
                    LedgerOffset::new(parse_u64(&sequence).map_err(StoreFailure::dependency)?),
                ),
                Some(_) => return Err(MaintenanceRunDependencyError::Denied),
                None => (
                    HomeEpoch::new(1)
                        .map_err(|_| MaintenanceRunDependencyError::InvalidResponse)?,
                    LedgerOffset::ZERO,
                ),
            };
            Ok(MaintenanceAdmissionContext {
                execution_scope: ExecutionScope::Tenant {
                    tenant_id: request.tenant_id,
                },
                control_home: RuntimeHome::Cloud,
                home_epoch,
                expected_offset,
                trace_context: maintenance_trace_context(&request)
                    .map_err(StoreFailure::dependency)?,
                retain_execution_grant_proof: false,
            })
        })
    }
}

impl MaintenanceRunStateSource for PostgresEvolutionMaintenanceStore {
    fn load(
        &self,
        request: &DreamModelRunRequest,
    ) -> ApplicationFuture<'_, Result<MaintenanceRunState, MaintenanceRunDependencyError>> {
        let request = request.clone();
        Box::pin(async move {
            self.load_state_inner(&request)
                .await
                .map_err(StoreFailure::dependency)
        })
    }

    fn wait(
        &self,
        request: &DreamModelRunRequest,
        owner: Arc<dyn musteragent_runtime_memory::DreamExecutionOwner>,
    ) -> ApplicationFuture<'_, Result<MaintenanceRunState, MaintenanceRunDependencyError>> {
        let request = request.clone();
        Box::pin(async move {
            loop {
                let state = self
                    .load_state_inner(&request)
                    .await
                    .map_err(StoreFailure::dependency)?;
                if !matches!(state, MaintenanceRunState::Pending) || !owner.is_active() {
                    return Ok(state);
                }
                let now = system_now().map_err(StoreFailure::dependency)?;
                if now >= request.deadline {
                    return Ok(MaintenanceRunState::Pending);
                }
                let remaining = request
                    .deadline
                    .unix_millis()
                    .saturating_sub(now.unix_millis());
                tokio::time::sleep(Duration::from_millis(
                    u64::try_from(remaining.clamp(1, 250))
                        .map_err(|_| MaintenanceRunDependencyError::InvalidResponse)?,
                ))
                .await;
            }
        })
    }
}

impl DreamAgentStateSource for PostgresEvolutionMaintenanceStore {
    fn load(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<DreamAgentState, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.load_dream_agent_state_inner(&attempt)
                .await
                .map_err(|failure| LoopPortError {
                    code: match failure {
                        StoreFailure::FenceConflict => ErrorCode::AttemptLeaseStale,
                        StoreFailure::Unavailable => ErrorCode::DependencyUnavailable,
                        StoreFailure::IdempotencyConflict | StoreFailure::CorruptState => {
                            ErrorCode::InternalInvariantViolated
                        }
                    },
                    retryable: matches!(failure, StoreFailure::Unavailable),
                })
        })
    }
}

impl EvolutionMaintenanceExecutionStore for PostgresEvolutionMaintenanceStore {
    fn claim(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<EvolutionMaintenanceClaim, MaintenanceExecutionPortError>>
    {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.claim_inner(&attempt)
                .await
                .map_err(StoreFailure::execution)
        })
    }

    fn mark_dispatched(
        &self,
        attempt: &AttemptContext,
        invocation_id: musteragent_identity::InvocationId,
    ) -> ApplicationFuture<'_, Result<ModelPersistenceDisposition, MaintenanceExecutionPortError>>
    {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.mark_dispatched_inner(&attempt, invocation_id)
                .await
                .map_err(StoreFailure::execution)
        })
    }

    fn finish(
        &self,
        attempt: &AttemptContext,
        terminal: EvolutionMaintenanceTerminal,
    ) -> ApplicationFuture<
        '_,
        Result<EvolutionMaintenanceTerminalReceipt, MaintenanceExecutionPortError>,
    > {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.finish_inner(&attempt, terminal)
                .await
                .map_err(StoreFailure::execution)
        })
    }
}

#[derive(Debug)]
struct LiveRun {
    tenant_id: TenantId,
    conversation_id: musteragent_identity::ConversationId,
    activity_id: musteragent_identity::ActivityId,
    executor_participant_id: musteragent_identity::ParticipantId,
    requested_by_participant_id: musteragent_identity::ParticipantId,
    parent_run_id: Option<musteragent_identity::RunId>,
    delegation_id: Option<musteragent_identity::DelegationId>,
    execution_scope: ExecutionScope,
    control_home: RuntimeHome,
    execution_home: RuntimeHome,
    home_epoch: HomeEpoch,
    runtime_snapshot_id: musteragent_identity::RuntimeSnapshotId,
}

async fn lock_live_attempt(
    transaction: &mut sqlx::PgConnection,
    attempt: &AttemptContext,
) -> Result<LiveRun, StoreFailure> {
    let row = sqlx::query(
        "SELECT tenant_id, conversation_id, activity_id, activity_kind,
                executor_participant_id, requested_by_participant_id,
                parent_run_id, delegation_id, execution_scope_kind,
                workspace_id, device_id, control_home_kind, control_device_id,
                execution_home_kind, execution_device_id,
                home_epoch::text AS home_epoch,
                last_run_sequence::text AS last_run_sequence,
                runtime_snapshot_id
         FROM runtime.runs WHERE run_id = $1",
    )
    .bind(attempt.run_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(StoreFailure::FenceConflict)?;
    if row.try_get::<i16, _>("activity_kind")? != 5 {
        return Err(StoreFailure::CorruptState);
    }
    let home_epoch = HomeEpoch::new(parse_u64(&row.try_get::<String, _>("home_epoch")?)?)
        .map_err(|_| StoreFailure::CorruptState)?;
    let last_run_sequence =
        RunSeq::new(parse_u64(&row.try_get::<String, _>("last_run_sequence")?)?)
            .map_err(|_| StoreFailure::CorruptState)?;
    let fence = AttemptFence {
        run_id: attempt.run_id,
        attempt_id: attempt.attempt_id,
        attempt_number: attempt.attempt_number,
        worker_id: attempt.worker_id.clone(),
        lease_id: attempt.lease_id,
        fencing_token: attempt.fencing_token,
        home_epoch,
    };
    let now = database_now_ms(transaction).await?;
    crate::execution::lock_and_validate_fence(transaction, &fence, last_run_sequence, now)
        .await
        .map_err(|_| StoreFailure::FenceConflict)?;
    let tenant_id = id(row.try_get("tenant_id")?)?;
    Ok(LiveRun {
        tenant_id,
        conversation_id: id(row.try_get("conversation_id")?)?,
        activity_id: id(row.try_get("activity_id")?)?,
        executor_participant_id: id(row.try_get("executor_participant_id")?)?,
        requested_by_participant_id: id(row.try_get("requested_by_participant_id")?)?,
        parent_run_id: optional_id(row.try_get("parent_run_id")?)?,
        delegation_id: optional_id(row.try_get("delegation_id")?)?,
        execution_scope: decode_scope(
            row.try_get("execution_scope_kind")?,
            row.try_get("workspace_id")?,
            row.try_get("device_id")?,
            tenant_id,
        )?,
        control_home: decode_home(
            row.try_get("control_home_kind")?,
            row.try_get("control_device_id")?,
        )?,
        execution_home: decode_home(
            row.try_get("execution_home_kind")?,
            row.try_get("execution_device_id")?,
        )?,
        home_epoch,
        runtime_snapshot_id: id(row.try_get("runtime_snapshot_id")?)?,
    })
}

async fn load_material(
    transaction: &mut sqlx::PgConnection,
    attempt: &AttemptContext,
    live: &LiveRun,
) -> Result<EvolutionMaintenanceMaterial, StoreFailure> {
    let row = sqlx::query(
        "SELECT encoded_snapshot, content_hash, created_at_ms
         FROM capability.runtime_snapshots
         WHERE tenant_id = $1 AND conversation_id = $2 AND run_id = $3
           AND runtime_snapshot_id = $4",
    )
    .bind(live.tenant_id.into_uuid())
    .bind(live.conversation_id.into_uuid())
    .bind(attempt.run_id.into_uuid())
    .bind(live.runtime_snapshot_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(StoreFailure::CorruptState)?;
    let encoded: Vec<u8> = row.try_get("encoded_snapshot")?;
    let hash = digest(row.try_get("content_hash")?)?;
    let snapshot = MaintenanceRuntimeSnapshot::restore(
        encoded,
        hash,
        UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
        &ProtobufMaintenanceSnapshotCodec,
    )
    .map_err(|_| StoreFailure::CorruptState)?;
    let identities = dream_maintenance_admission_identities(snapshot.content().dream_job_id)
        .map_err(|_| StoreFailure::CorruptState)?;
    let command_id = dream_maintenance_command_id(snapshot.content().dream_job_id)
        .map_err(|_| StoreFailure::CorruptState)?;
    let run_context = RunContext {
        tenant_id: live.tenant_id,
        conversation_id: live.conversation_id,
        activity_id: live.activity_id,
        activity_kind: ActivityKind::Maintenance,
        run_id: attempt.run_id,
        executor_participant_id: live.executor_participant_id,
        requested_by_participant_id: live.requested_by_participant_id,
        parent_run_id: live.parent_run_id,
        delegation_id: live.delegation_id,
        execution_scope: live.execution_scope,
        control_home: live.control_home,
        execution_home: live.execution_home,
        home_epoch: live.home_epoch,
        runtime_snapshot_id: live.runtime_snapshot_id,
    };
    Ok(EvolutionMaintenanceMaterial {
        activity_id: live.activity_id,
        runtime_snapshot_id: live.runtime_snapshot_id,
        admitted_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
        snapshot: snapshot.content().clone(),
        invocation: InvocationContext {
            invocation_id: snapshot.content().invocation_id,
            run_context,
            attempt_context: attempt.clone(),
            parent_invocation_id: None,
            causation_id: identities.run_record_id,
            correlation_id: derive_legacy_system_admission_correlation_id(command_id)
                .ok_or(StoreFailure::CorruptState)?,
            timing: snapshot
                .content()
                .invocation_timing()
                .map_err(|_| StoreFailure::CorruptState)?,
        },
    })
}

fn new_invocation(
    material: &EvolutionMaintenanceMaterial,
    started_at: UtcTimestamp,
) -> Result<ModelInvocation, StoreFailure> {
    let snapshot = &material.snapshot;
    let invocation = ModelInvocation {
        invocation_id: snapshot.invocation_id,
        tenant_id: snapshot.tenant_id,
        conversation_id: material.invocation.run_context.conversation_id,
        run_id: material.invocation.run_context.run_id,
        attempt_id: material.invocation.attempt_context.attempt_id,
        purpose: ModelInvocationPurpose::EvolutionInduction,
        route_role: ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Evolution),
        status: ModelInvocationStatus::Started,
        model_route_ref: ModelRouteRef::from_key(snapshot.evolution_route.route_ref.clone()),
        route_identity_hash: SignedModelRouteHash::digest(&snapshot.evolution_route)
            .map_err(|_| StoreFailure::CorruptState)?,
        credential_grant_ref: snapshot.credential_grant_ref.clone(),
        requested_model_key: ModelKey::new(snapshot.evolution_route.resolved_model_key.clone())
            .map_err(|_| StoreFailure::CorruptState)?,
        response_model_key: None,
        pricing_version_ref: snapshot.evolution_route.pricing_version_ref.clone(),
        idempotency_key: IdempotencyKey::new(format!("dream-provider:{}", snapshot.dream_job_id))
            .map_err(|_| StoreFailure::CorruptState)?,
        request_ref: snapshot.model_input_ref.clone(),
        request_hash: snapshot.model_input_ref.sha256,
        response_ref: None,
        provider_request_ref: None,
        continuation_input_ref: None,
        continuation_output_ref: None,
        usage: None,
        started_at,
        finished_at: None,
        error: None,
    };
    invocation
        .validate()
        .map_err(|_| StoreFailure::CorruptState)?;
    Ok(invocation)
}

async fn insert_invocation(
    transaction: &mut sqlx::PgConnection,
    invocation: &ModelInvocation,
) -> Result<(), StoreFailure> {
    sqlx::query(
        "INSERT INTO runtime.model_invocations (
             invocation_id, tenant_id, conversation_id, run_id, attempt_id,
             purpose, route_role, status, model_route_ref, route_identity_hash,
             credential_grant_ref, requested_model_key, pricing_version_ref,
             idempotency_key, request_content_id, request_hash, started_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,1,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
    )
    .bind(invocation.invocation_id.into_uuid())
    .bind(invocation.tenant_id.into_uuid())
    .bind(invocation.conversation_id.into_uuid())
    .bind(invocation.run_id.into_uuid())
    .bind(invocation.attempt_id.into_uuid())
    .bind(purpose_code(invocation.purpose))
    .bind(route_role_code(invocation.route_role))
    .bind(invocation.model_route_ref.as_str())
    .bind(invocation.route_identity_hash.into_bytes().as_slice())
    .bind(invocation.credential_grant_ref.as_str())
    .bind(invocation.requested_model_key.as_str())
    .bind(invocation.pricing_version_ref.as_str())
    .bind(invocation.idempotency_key.as_str())
    .bind(invocation.request_ref.content_id.into_uuid())
    .bind(invocation.request_hash.into_bytes().as_slice())
    .bind(invocation.started_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn insert_slot(
    transaction: &mut sqlx::PgConnection,
    attempt: &AttemptContext,
    material: &EvolutionMaintenanceMaterial,
    now: UtcTimestamp,
) -> Result<(), StoreFailure> {
    let command_id = dream_maintenance_command_id(material.snapshot.dream_job_id)
        .map_err(|_| StoreFailure::CorruptState)?;
    let route_hash = SignedModelRouteHash::digest(&material.snapshot.evolution_route)
        .map_err(|_| StoreFailure::CorruptState)?;
    sqlx::query(
        "INSERT INTO runtime.evolution_maintenance_executions (
             invocation_id, dream_job_id, tenant_id, conversation_id,
             command_id, activity_id, run_id, runtime_snapshot_id,
             owner_attempt_id, owner_fencing_token, route_identity_hash,
             request_hash, outcome_status, created_at_ms, updated_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::bigint,$11,$12,1,$13,$13)",
    )
    .bind(material.snapshot.invocation_id.into_uuid())
    .bind(material.snapshot.dream_job_id.into_uuid())
    .bind(material.snapshot.tenant_id.into_uuid())
    .bind(material.invocation.run_context.conversation_id.into_uuid())
    .bind(command_id.into_uuid())
    .bind(material.activity_id.into_uuid())
    .bind(material.invocation.run_context.run_id.into_uuid())
    .bind(material.runtime_snapshot_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .bind(attempt.fencing_token.get().to_string())
    .bind(route_hash.into_bytes().as_slice())
    .bind(
        material
            .snapshot
            .model_input_ref
            .sha256
            .into_bytes()
            .as_slice(),
    )
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn persist_terminal(
    keys: &OpaqueRefKeyring,
    transaction: &mut sqlx::PgConnection,
    row: &sqlx::postgres::PgRow,
    terminal: &EvolutionMaintenanceTerminal,
) -> Result<(), StoreFailure> {
    let invocation_id = id(row.try_get("invocation_id")?)?;
    match terminal {
        EvolutionMaintenanceTerminal::DispatchedUnknown {
            invocation_id: observed,
            observed_at,
        } => {
            if *observed != invocation_id
                || row
                    .try_get::<Option<i64>, _>("provider_dispatched_at_ms")?
                    .is_none()
            {
                return Err(StoreFailure::CorruptState);
            }
            sqlx::query(
                "UPDATE runtime.evolution_maintenance_executions
                 SET outcome_status = 5, updated_at_ms = $2
                 WHERE invocation_id = $1 AND outcome_status = 1",
            )
            .bind(invocation_id.into_uuid())
            .bind(observed_at.unix_millis())
            .execute(&mut *transaction)
            .await?;
        }
        EvolutionMaintenanceTerminal::Succeeded(completion)
        | EvolutionMaintenanceTerminal::Failed(completion)
        | EvolutionMaintenanceTerminal::Cancelled(completion) => {
            completion
                .validate()
                .map_err(|_| StoreFailure::CorruptState)?;
            verify_completion_identity(row, completion)?;
            let (expected_status, outcome) = match terminal {
                EvolutionMaintenanceTerminal::Succeeded(_) => {
                    (ModelInvocationStatus::Succeeded, OUTCOME_SUCCEEDED)
                }
                EvolutionMaintenanceTerminal::Failed(_) => {
                    (ModelInvocationStatus::Failed, OUTCOME_FAILED)
                }
                EvolutionMaintenanceTerminal::Cancelled(_) => {
                    (ModelInvocationStatus::Cancelled, OUTCOME_CANCELLED)
                }
                EvolutionMaintenanceTerminal::DispatchedUnknown { .. } => unreachable!(),
            };
            let invocation = &completion.invocation;
            if invocation.status != expected_status {
                return Err(StoreFailure::CorruptState);
            }
            let dispatched = row
                .try_get::<Option<i64>, _>("provider_dispatched_at_ms")?
                .is_some();
            let contains_provider_outcome = invocation.provider_request_ref.is_some()
                || invocation.response_ref.is_some()
                || invocation.response_model_key.is_some()
                || invocation.usage.is_some();
            if !dispatched
                && (expected_status != ModelInvocationStatus::Failed || contains_provider_outcome)
            {
                return Err(StoreFailure::CorruptState);
            }
            if let Some(response) = &invocation.response_ref {
                authorize_content(transaction, invocation.tenant_id, response).await?;
            }
            let provider_ref = protect_optional(
                keys,
                invocation.invocation_id,
                "provider-request",
                invocation
                    .provider_request_ref
                    .as_ref()
                    .map(|value| value.as_str()),
            )?;
            let terminal_error = invocation
                .error
                .as_ref()
                .map(|error| error.code.as_str().as_bytes().to_vec());
            let changed = sqlx::query(
                "UPDATE runtime.model_invocations SET
                     status = $2, response_content_id = $3,
                     provider_request_ref = $4, usage_fact = $5,
                     finished_at_ms = $6, terminal_error = $7,
                     response_model_key = $8
                 WHERE invocation_id = $1 AND status = 1",
            )
            .bind(invocation.invocation_id.into_uuid())
            .bind(status_code(invocation.status))
            .bind(
                invocation
                    .response_ref
                    .as_ref()
                    .map(|value| value.content_id.into_uuid()),
            )
            .bind(provider_ref)
            .bind(invocation.usage.map(encode_usage))
            .bind(invocation.finished_at.map(UtcTimestamp::unix_millis))
            .bind(terminal_error)
            .bind(invocation.response_model_key.as_ref().map(ModelKey::as_str))
            .execute(&mut *transaction)
            .await?;
            if changed.rows_affected() != 1 {
                return Err(StoreFailure::IdempotencyConflict);
            }
            if let Some(fact) = completion.usage_fact.as_ref() {
                if let Some(publication) = completion.metering_publication.as_ref() {
                    insert_metering_outbox(transaction, fact, publication).await?;
                }
                insert_usage_fact(transaction, fact, completion.metering_publication.as_ref())
                    .await?;
            }
            sqlx::query(
                "UPDATE runtime.evolution_maintenance_executions
                 SET outcome_status = $2, updated_at_ms = $3
                 WHERE invocation_id = $1 AND outcome_status = 1",
            )
            .bind(invocation.invocation_id.into_uuid())
            .bind(outcome)
            .bind(
                invocation
                    .finished_at
                    .ok_or(StoreFailure::CorruptState)?
                    .unix_millis(),
            )
            .execute(&mut *transaction)
            .await?;
        }
    }
    Ok(())
}

async fn authorize_content(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    reference: &ContentRef,
) -> Result<(), StoreFailure> {
    let row = sqlx::query(
        "SELECT media_type, byte_length::text AS byte_length, content_hash, state
         FROM artifact.content_refs WHERE tenant_id = $1 AND content_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(reference.content_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(StoreFailure::CorruptState)?;
    if row.try_get::<String, _>("media_type")? != reference.media_type.as_str()
        || parse_u64(&row.try_get::<String, _>("byte_length")?)? != reference.byte_length
        || digest(row.try_get("content_hash")?)? != reference.sha256
        || row.try_get::<i16, _>("state")? != 2
    {
        return Err(StoreFailure::CorruptState);
    }
    Ok(())
}

fn verify_owner(row: &sqlx::postgres::PgRow, attempt: &AttemptContext) -> Result<(), StoreFailure> {
    if row.try_get::<uuid::Uuid, _>("run_id")? != attempt.run_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("owner_attempt_id")? != attempt.attempt_id.into_uuid()
        || parse_u64(&row.try_get::<String, _>("owner_fencing_token_text")?)?
            != attempt.fencing_token.get()
    {
        return Err(StoreFailure::FenceConflict);
    }
    Ok(())
}

fn verify_slot_identity(
    row: &sqlx::postgres::PgRow,
    material: &EvolutionMaintenanceMaterial,
) -> Result<(), StoreFailure> {
    let route_hash = SignedModelRouteHash::digest(&material.snapshot.evolution_route)
        .map_err(|_| StoreFailure::CorruptState)?;
    if row.try_get::<uuid::Uuid, _>("dream_job_id")? != material.snapshot.dream_job_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("tenant_id")? != material.snapshot.tenant_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("conversation_id")?
            != material.invocation.run_context.conversation_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("run_id")?
            != material.invocation.run_context.run_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("runtime_snapshot_id")?
            != material.runtime_snapshot_id.into_uuid()
        || row.try_get::<Vec<u8>, _>("route_identity_hash")? != route_hash.into_bytes()
        || row.try_get::<Vec<u8>, _>("request_hash")?
            != material.snapshot.model_input_ref.sha256.into_bytes()
    {
        return Err(StoreFailure::IdempotencyConflict);
    }
    Ok(())
}

fn verify_completion_identity(
    row: &sqlx::postgres::PgRow,
    completion: &CompleteModelInvocation,
) -> Result<(), StoreFailure> {
    let invocation = &completion.invocation;
    if row.try_get::<uuid::Uuid, _>("invocation_id")? != invocation.invocation_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("tenant_id")? != invocation.tenant_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("conversation_id")?
            != invocation.conversation_id.into_uuid()
        || row.try_get::<uuid::Uuid, _>("run_id")? != invocation.run_id.into_uuid()
        || row.try_get::<Vec<u8>, _>("route_identity_hash")?
            != invocation.route_identity_hash.into_bytes()
        || row.try_get::<Vec<u8>, _>("request_hash")? != invocation.request_hash.into_bytes()
    {
        return Err(StoreFailure::IdempotencyConflict);
    }
    Ok(())
}

fn decode_completed_receipt(
    row: &sqlx::postgres::PgRow,
) -> Result<CompletedDreamModelRun, StoreFailure> {
    if row.try_get::<i16, _>("run_status")? != 5
        || row.try_get::<i16, _>("outcome_status")? != OUTCOME_SUCCEEDED
    {
        return Err(StoreFailure::CorruptState);
    }
    Ok(CompletedDreamModelRun {
        dream_job_id: id(row.try_get("dream_job_id")?)?,
        tenant_id: id(row.try_get("tenant_id")?)?,
        maintenance_conversation_id: id(row.try_get("conversation_id")?)?,
        maintenance_command_id: id(row.try_get("command_id")?)?,
        maintenance_activity_id: id(row.try_get("activity_id")?)?,
        maintenance_run_id: id(row.try_get("run_id")?)?,
        maintenance_runtime_snapshot_id: id(row.try_get("runtime_snapshot_id")?)?,
        attempt_id: id(row.try_get("owner_attempt_id")?)?,
        invocation_id: id(row.try_get("invocation_id")?)?,
        model_input_ref: ContentRef {
            content_id: id(row.try_get("request_content_id")?)?,
            media_type: MediaType::new(row.try_get::<String, _>("request_media_type")?)
                .map_err(|_| StoreFailure::CorruptState)?,
            byte_length: parse_u64(&row.try_get::<String, _>("request_byte_length")?)?,
            sha256: digest(row.try_get("request_hash")?)?,
        },
        model_output_ref: ContentRef {
            content_id: id(row
                .try_get::<Option<uuid::Uuid>, _>("response_content_id")?
                .ok_or(StoreFailure::CorruptState)?)?,
            media_type: MediaType::new(row.try_get::<String, _>("response_media_type")?)
                .map_err(|_| StoreFailure::CorruptState)?,
            byte_length: parse_u64(&row.try_get::<String, _>("response_byte_length")?)?,
            sha256: digest(row.try_get("response_hash")?)?,
        },
        model_route_ref: musteragent_identity::VersionedKey::new(
            row.try_get::<String, _>("model_route_ref")?,
        )
        .map_err(|_| StoreFailure::CorruptState)?,
        activity_kind: ActivityKind::Maintenance,
        run_status: RunStatus::Completed,
        invocation_purpose: ModelInvocationPurpose::EvolutionInduction,
        route_role: ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Evolution),
        completed_at: UtcTimestamp::from_unix_millis(
            row.try_get::<Option<i64>, _>("finished_at_ms")?
                .ok_or(StoreFailure::CorruptState)?,
        ),
    })
}

fn terminal_identity(
    terminal: &EvolutionMaintenanceTerminal,
) -> (musteragent_identity::InvocationId, i16) {
    match terminal {
        EvolutionMaintenanceTerminal::Succeeded(value) => {
            (value.invocation.invocation_id, OUTCOME_SUCCEEDED)
        }
        EvolutionMaintenanceTerminal::Failed(value) => {
            (value.invocation.invocation_id, OUTCOME_FAILED)
        }
        EvolutionMaintenanceTerminal::Cancelled(value) => {
            (value.invocation.invocation_id, OUTCOME_CANCELLED)
        }
        EvolutionMaintenanceTerminal::DispatchedUnknown { invocation_id, .. } => {
            (*invocation_id, OUTCOME_INDETERMINATE)
        }
    }
}

fn decode_scope(
    kind: i16,
    workspace_id: Option<uuid::Uuid>,
    device_id: Option<uuid::Uuid>,
    tenant_id: TenantId,
) -> Result<ExecutionScope, StoreFailure> {
    match (kind, workspace_id, device_id) {
        (1, None, None) => Ok(ExecutionScope::Tenant { tenant_id }),
        (2, Some(workspace), None) => Ok(ExecutionScope::Workspace {
            workspace_id: id(workspace)?,
        }),
        (3, workspace, Some(device)) => Ok(ExecutionScope::DevicePrivate {
            device_id: id(device)?,
            workspace_id: workspace.map(id).transpose()?,
        }),
        _ => Err(StoreFailure::CorruptState),
    }
}

fn decode_home(kind: i16, device_id: Option<uuid::Uuid>) -> Result<RuntimeHome, StoreFailure> {
    match (kind, device_id) {
        (1, None) => Ok(RuntimeHome::Cloud),
        (2, Some(device)) => Ok(RuntimeHome::Device {
            device_id: id(device)?,
        }),
        _ => Err(StoreFailure::CorruptState),
    }
}

fn maintenance_trace_context(request: &DreamModelRunRequest) -> Result<TraceContext, StoreFailure> {
    let mut material = TRACE_DOMAIN.to_vec();
    material.extend_from_slice(request.dream_job_id.into_uuid().as_bytes());
    material.extend_from_slice(request.maintenance_run_id.into_uuid().as_bytes());
    let digest = Sha256::digest(material).to_string();
    Ok(TraceContext {
        traceparent: TraceParent::new(format!("00-{}-{}-00", &digest[..32], &digest[32..48]))
            .map_err(|_| StoreFailure::CorruptState)?,
        tracestate: None,
    })
}

fn parse_u64(value: &str) -> Result<u64, StoreFailure> {
    value.parse().map_err(|_| StoreFailure::CorruptState)
}

fn serde_to_runtime_json(value: serde_json::Value) -> Result<JsonValue, StoreFailure> {
    match value {
        serde_json::Value::Null => Ok(JsonValue::Null),
        serde_json::Value::Bool(value) => Ok(JsonValue::Bool(value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(JsonValue::Integer)
            .or_else(|| value.as_f64().map(JsonValue::Number))
            .ok_or(StoreFailure::CorruptState),
        serde_json::Value::String(value) => Ok(JsonValue::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(serde_to_runtime_json)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        serde_json::Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| serde_to_runtime_json(value).map(|value| (key, value)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(JsonValue::Object),
    }
}

fn digest(value: Vec<u8>) -> Result<Sha256, StoreFailure> {
    Sha256::from_bytes(value).map_err(|_| StoreFailure::CorruptState)
}

fn id<T>(value: uuid::Uuid) -> Result<T, StoreFailure>
where
    T: std::str::FromStr,
{
    value
        .to_string()
        .parse()
        .map_err(|_| StoreFailure::CorruptState)
}

fn optional_id<T>(value: Option<uuid::Uuid>) -> Result<Option<T>, StoreFailure>
where
    T: std::str::FromStr,
{
    value.map(id).transpose()
}

fn system_now() -> Result<UtcTimestamp, StoreFailure> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreFailure::Unavailable)?
        .as_millis();
    Ok(UtcTimestamp::from_unix_millis(
        i64::try_from(millis).map_err(|_| StoreFailure::Unavailable)?,
    ))
}

#[derive(Debug)]
enum StoreFailure {
    FenceConflict,
    IdempotencyConflict,
    CorruptState,
    Unavailable,
}

impl StoreFailure {
    fn from_loop(code: ErrorCode, retryable: bool) -> Self {
        if code == ErrorCode::AttemptLeaseStale {
            Self::FenceConflict
        } else if retryable {
            Self::Unavailable
        } else {
            Self::CorruptState
        }
    }

    const fn execution(self) -> MaintenanceExecutionPortError {
        match self {
            Self::FenceConflict => MaintenanceExecutionPortError {
                code: ErrorCode::AttemptLeaseStale,
                retryable: false,
            },
            Self::Unavailable => MaintenanceExecutionPortError {
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
            },
            Self::IdempotencyConflict | Self::CorruptState => MaintenanceExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            },
        }
    }

    const fn dependency(self) -> MaintenanceRunDependencyError {
        match self {
            Self::Unavailable => MaintenanceRunDependencyError::Unavailable,
            Self::FenceConflict => MaintenanceRunDependencyError::Denied,
            Self::IdempotencyConflict | Self::CorruptState => {
                MaintenanceRunDependencyError::InvalidResponse
            }
        }
    }
}

impl From<sqlx::Error> for StoreFailure {
    fn from(_: sqlx::Error) -> Self {
        Self::Unavailable
    }
}

impl From<crate::PostgresTransactionError> for StoreFailure {
    fn from(_: crate::PostgresTransactionError) -> Self {
        Self::Unavailable
    }
}

impl From<crate::PostgresLedgerError> for StoreFailure {
    fn from(_: crate::PostgresLedgerError) -> Self {
        Self::Unavailable
    }
}

impl From<crate::PostgresModelInvocationError> for StoreFailure {
    fn from(error: crate::PostgresModelInvocationError) -> Self {
        match error {
            crate::PostgresModelInvocationError::Sql(_) => Self::Unavailable,
            _ => Self::CorruptState,
        }
    }
}
