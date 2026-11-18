use std::num::NonZeroU32;
use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_discovery::ExposureState;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, CommandId, CorrelationId, DottedKey, HomeEpoch, IdempotencyKey, LedgerOffset,
    LedgerRecordId, MessageId, RunSeq, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, ConsumeRunControlsBatch, DomainEvent, FencedAppendBatch,
    LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_loop::{
    ExecutionPolicySource, LoopControlQueue, LoopPortError, PinnedRunExecutionPolicy,
    ToolExecutionPolicyDecision,
};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{AttemptContext, RunControlKind, RunEvent};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_snapshot::{RuntimeSnapshotContent, ToolExecutionMode};
use musteragent_tool::{ConcurrencyClass, WireName};
use sqlx::{PgPool, Row};

use crate::exposure_projection::{ExposureProjectionError, load_verified_exposure_projection};
use crate::{PostgresLedgerStore, PostgresRunControlError};

#[derive(Clone)]
pub struct PostgresExecutionPolicySource {
    application_catalogs: Option<
        Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    snapshots: Arc<dyn RuntimeSnapshotDecoder>,
    ledger_decoder: Arc<dyn LedgerPayloadDecoder>,
}

struct LoadedExecutionSnapshot {
    snapshot: RuntimeSnapshotContent,
    admitted_at: UtcTimestamp,
    tenant_id: TenantId,
}

impl PostgresExecutionPolicySource {
    pub fn with_application_catalogs(
        mut self,
        source: Option<Arc<dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource>>,
    ) -> Self {
        self.application_catalogs = source;
        self
    }

    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
        ledger_decoder: Arc<dyn LedgerPayloadDecoder>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            application_catalogs: None,
            pool,
            database,
            snapshots,
            ledger_decoder,
        }
    }

    async fn load_snapshot_inner(
        &self,
        attempt: &AttemptContext,
    ) -> Result<LoadedExecutionSnapshot, LoopPortError> {
        let row = sqlx::query(
            "SELECT run.tenant_id, run.admitted_at_ms,
                    snapshot.encoded_snapshot, snapshot.content_hash
             FROM runtime.runs AS run
             JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = run.tenant_id
              AND attempt.run_id = run.run_id
             JOIN runtime.leases AS lease
               ON lease.tenant_id = run.tenant_id
              AND lease.run_id = run.run_id
              AND lease.attempt_id = attempt.attempt_id
             JOIN capability.runtime_snapshots AS snapshot
               ON snapshot.runtime_snapshot_id = run.runtime_snapshot_id
              AND snapshot.tenant_id = run.tenant_id
              AND snapshot.conversation_id = run.conversation_id
              AND snapshot.run_id = run.run_id
             WHERE run.run_id = $1
               AND run.status IN (2, 4)
               AND attempt.attempt_id = $2
               AND attempt.attempt_number = $3
               AND attempt.status = 1
               AND lease.worker_id = $4
               AND lease.lease_id = $5
               AND lease.fencing_token = $6::bigint
               AND lease.expires_at_ms >
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(i32::try_from(attempt.attempt_number.get()).map_err(|_| invariant())?)
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(database)?
        .ok_or_else(stale_fence)?;
        let encoded: Vec<u8> = row.try_get("encoded_snapshot").map_err(database)?;
        let expected = Sha256::from_bytes(
            row.try_get::<Vec<u8>, _>("content_hash")
                .map_err(database)?,
        )
        .map_err(|_| invariant())?;
        if Sha256::digest(&encoded) != expected {
            return Err(invariant());
        }
        let snapshot = self
            .snapshots
            .decode_runtime_snapshot(&encoded)
            .map_err(|error| LoopPortError {
                code: error.code,
                retryable: error.retryable,
            })?;
        snapshot.validate_sealed().map_err(|_| invariant())?;
        // Sealed evidence may remain readable after execution support is retired.
        // Check at the original dispatch-policy owner, before any checkpoint or
        // tool is resumed; the Journal retains unknown-effect closure semantics.
        if let Some(reference) = &snapshot.product_release {
            musteragent_tool::require_product_release_admission(reference).map_err(|_| {
                LoopPortError {
                    code: ErrorCode::UnsupportedSchemaVersion,
                    retryable: false,
                }
            })?;
        }

        let admitted_at = musteragent_identity::UtcTimestamp::from_unix_millis(
            row.try_get("admitted_at_ms").map_err(database)?,
        );
        let tenant_id = TenantId::from_uuid(row.try_get("tenant_id").map_err(database)?)
            .map_err(|_| invariant())?;
        Ok(LoadedExecutionSnapshot {
            snapshot,
            admitted_at,
            tenant_id,
        })
    }

    async fn load_inner(
        &self,
        attempt: &AttemptContext,
    ) -> Result<PinnedRunExecutionPolicy, LoopPortError> {
        let loaded = self.load_snapshot_inner(attempt).await?;
        let snapshot = loaded.snapshot;
        PinnedRunExecutionPolicy {
            snapshot: snapshot.execution_policy,
            admitted_at: loaded.admitted_at,
            model_route_count: NonZeroU32::new(
                u32::try_from(
                    snapshot
                        .model
                        .fallback_candidates
                        .len()
                        .checked_add(1)
                        .ok_or_else(invariant)?,
                )
                .map_err(|_| invariant())?,
            )
            .ok_or_else(invariant)?,
        }
        .validate()
    }

    async fn exposure_state(
        &self,
        tenant_id: TenantId,
        run_id: musteragent_identity::RunId,
        snapshot: &RuntimeSnapshotContent,
    ) -> Result<Option<ExposureState>, LoopPortError> {
        let Some(discovery) = snapshot.discovery.as_ref() else {
            return Ok(None);
        };
        let resolved = musteragent_runtime_snapshot::application_catalog::resolve_tool_catalog(
            snapshot,
            self.application_catalogs.as_deref(),
        )
        .await
        .map_err(application_error)?;
        snapshot
            .validate_resolved_tool_catalog(&resolved.catalog)
            .map_err(|_| invariant())?;
        let catalog = &resolved.catalog;
        let state = load_verified_exposure_projection(
            &self.database,
            self.ledger_decoder.as_ref(),
            tenant_id,
            run_id,
            catalog,
            &discovery.initial_exposed_tool_keys,
        )
        .await
        .map_err(projection_error)?;
        if state.len() > usize::try_from(discovery.max_exposed_tools.get()).unwrap_or(usize::MAX) {
            return Err(invariant());
        }
        Ok(Some(state))
    }
}

fn projection_error(error: ExposureProjectionError) -> LoopPortError {
    match error {
        ExposureProjectionError::Corrupt => invariant(),
        ExposureProjectionError::Database(error) => database(error),
        ExposureProjectionError::Transaction(error) => database(error),
    }
}

fn parse_u64(row: &sqlx::postgres::PgRow, field: &str) -> Result<u64, LoopPortError> {
    row.try_get::<String, _>(field)
        .map_err(database)?
        .parse()
        .map_err(|_| invariant())
}

impl ExecutionPolicySource for PostgresExecutionPolicySource {
    fn load(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move { self.load_inner(&attempt).await })
    }

    fn context_compaction_policy_ref(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<Option<musteragent_identity::VersionedKey>, LoopPortError>>
    {
        let attempt = attempt.clone();
        Box::pin(async move {
            let loaded = self.load_snapshot_inner(&attempt).await?;
            Ok(Some(loaded.snapshot.context_policy.compaction_policy_ref))
        })
    }

    fn tool_execution_policy(
        &self,
        attempt: &AttemptContext,
        wire_name: &WireName,
        default_mode: ToolExecutionMode,
    ) -> ApplicationFuture<'_, Result<ToolExecutionPolicyDecision, LoopPortError>> {
        let attempt = attempt.clone();
        let wire_name = wire_name.clone();
        Box::pin(async move {
            let loaded = self.load_snapshot_inner(&attempt).await?;
            let resolved = musteragent_runtime_snapshot::application_catalog::resolve_tool_catalog(
                &loaded.snapshot,
                self.application_catalogs.as_deref(),
            )
            .await
            .map_err(application_error)?;
            loaded
                .snapshot
                .validate_resolved_tool_catalog(&resolved.catalog)
                .map_err(|_| invariant())?;
            let Some(descriptor) = resolved.catalog.get_by_wire_name(wire_name.as_str()) else {
                return Ok(ToolExecutionPolicyDecision::Unavailable);
            };
            let concurrency = match loaded
                .snapshot
                .tools
                .iter()
                .find(|t| t.manifest.key == descriptor.tool_ref)
            {
                Some(tool) => tool.concurrency_class,
                None if matches!(
                    descriptor.schema,
                    musteragent_discovery::ToolSchemaMaterial::Indexed(_)
                ) =>
                {
                    ConcurrencyClass::SequentialInteraction
                }
                None => return Err(invariant()),
            };
            if self
                .exposure_state(loaded.tenant_id, attempt.run_id, &loaded.snapshot)
                .await?
                .is_some_and(|state| state.ordinal_for(&descriptor.tool_ref).is_none())
            {
                return Ok(ToolExecutionPolicyDecision::Unavailable);
            }
            let execution_mode = match (default_mode, concurrency) {
                (ToolExecutionMode::Sequential, _) => ToolExecutionMode::Sequential,
                (
                    ToolExecutionMode::Parallel,
                    ConcurrencyClass::SequentialWrite | ConcurrencyClass::SequentialInteraction,
                ) => ToolExecutionMode::Sequential,
                (ToolExecutionMode::Parallel, _) => ToolExecutionMode::Parallel,
            };
            Ok(ToolExecutionPolicyDecision::Allowed(execution_mode))
        })
    }
}

/// PostgreSQL-owned consumption cursor for messages already committed to the
/// canonical conversation Ledger as steering or follow-up controls.
#[derive(Clone)]
pub struct PostgresLoopControlQueue {
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
}

impl PostgresLoopControlQueue {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
    ) -> Self {
        let database = database.into();
        Self {
            ledger: PostgresLedgerStore::new(database),
            encoder,
        }
    }

    async fn drain_kind(
        &self,
        attempt: &AttemptContext,
        kind: RunControlKind,
    ) -> Result<u32, LoopPortError> {
        let mut transaction =
            crate::commit_lane::acquire_for_run(self.ledger.database(), attempt.run_id)
                .await
                .map_err(database)?
                .ok_or_else(stale_fence)?;
        let state = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    stream.last_sequence::text AS conversation_offset,
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             FROM runtime.runs AS run
             JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = run.tenant_id
              AND attempt.run_id = run.run_id
             JOIN runtime.leases AS lease
               ON lease.tenant_id = run.tenant_id
              AND lease.run_id = run.run_id
              AND lease.attempt_id = attempt.attempt_id
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             WHERE run.run_id = $1
               AND run.status IN (2, 4)
               AND attempt.attempt_id = $2
               AND attempt.attempt_number = $3
               AND attempt.status = 1
               AND lease.worker_id = $4
               AND lease.lease_id = $5
               AND lease.fencing_token = $6::bigint
               AND lease.expires_at_ms >
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(i32::try_from(attempt.attempt_number.get()).map_err(|_| invariant())?)
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database)?
        .ok_or_else(stale_fence)?;
        let controls = sqlx::query(
            "SELECT control_id, message_id,
                    source_conversation_seq::text AS source_conversation_seq
             FROM runtime.loop_controls
             WHERE run_id = $1 AND kind = $2 AND status = 1
             ORDER BY source_conversation_seq, control_id
             LIMIT 256",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(control_kind_code(kind))
        .fetch_all(&mut *transaction)
        .await
        .map_err(database)?;
        if controls.is_empty() {
            return Ok(0);
        }
        let tenant_id = TenantId::from_uuid(state.try_get("tenant_id").map_err(database)?)
            .map_err(|_| invariant())?;
        let conversation_id = musteragent_identity::ConversationId::from_uuid(
            state.try_get("conversation_id").map_err(database)?,
        )
        .map_err(|_| invariant())?;
        let activity_id = ActivityId::from_uuid(state.try_get("activity_id").map_err(database)?)
            .map_err(|_| invariant())?;
        let home_epoch =
            HomeEpoch::new(parse_u64(&state, "home_epoch")?).map_err(|_| invariant())?;
        let last_run_sequence =
            RunSeq::new(parse_u64(&state, "last_run_sequence")?).map_err(|_| invariant())?;
        let conversation_offset = LedgerOffset::new(parse_u64(&state, "conversation_offset")?);
        let occurred_at =
            UtcTimestamp::from_unix_millis(state.try_get("now_ms").map_err(database)?);
        let mut control_ids = Vec::with_capacity(controls.len());
        let mut message_ids = Vec::with_capacity(controls.len());
        let mut source_sequences = Vec::with_capacity(controls.len());
        for row in controls {
            control_ids.push(
                CommandId::from_uuid(row.try_get("control_id").map_err(database)?)
                    .map_err(|_| invariant())?,
            );
            message_ids.push(
                MessageId::from_uuid(row.try_get("message_id").map_err(database)?)
                    .map_err(|_| invariant())?,
            );
            source_sequences.push(parse_u64(&row, "source_conversation_seq")?);
        }
        let first_sequence = source_sequences.first().copied().ok_or_else(invariant)?;
        let last_sequence = source_sequences.last().copied().ok_or_else(invariant)?;
        let event = LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::ControlConsumed {
            kind,
            message_ids: message_ids.clone(),
        }));
        let payload = self.encoder.encode(event);
        let record_id = consumed_record_id(attempt, kind, &control_ids)?;
        let record = ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .map_err(|_| invariant())?,
            record_id,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(activity_id),
            run_id: Some(attempt.run_id),
            run_seq: Some(
                RunSeq::new(
                    last_run_sequence
                        .get()
                        .checked_add(1)
                        .ok_or_else(invariant)?,
                )
                .map_err(|_| invariant())?,
            ),
            attempt_id: Some(attempt.attempt_id),
            participant_id: None,
            producer: Producer::Worker,
            occurred_at,
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&payload),
            payload,
        };
        let idempotency_key = IdempotencyKey::new(format!(
            "loop:control-consume:{}:{}:{}:{first_sequence}:{last_sequence}",
            attempt.run_id,
            attempt.attempt_id,
            control_kind_code(kind),
        ))
        .map_err(|_| invariant())?;
        let mut fingerprint = idempotency_key.as_str().as_bytes().to_vec();
        fingerprint.extend_from_slice(&record.payload_hash.into_bytes());
        for control_id in &control_ids {
            fingerprint.extend_from_slice(control_id.into_uuid().as_bytes());
        }
        let append = AppendRequest {
            tenant_id,
            conversation_id,
            expected_home_epoch: home_epoch,
            expected_offset: conversation_offset,
            idempotency_key,
            command_fingerprint: Sha256::digest(fingerprint),
            records: vec![record],
            outbox: Vec::new(),
        };
        self.ledger
            .consume_run_controls_in_transaction(
                transaction,
                ConsumeRunControlsBatch {
                    fenced: FencedAppendBatch {
                        fence: AttemptFence {
                            run_id: attempt.run_id,
                            attempt_id: attempt.attempt_id,
                            attempt_number: attempt.attempt_number,
                            worker_id: attempt.worker_id.clone(),
                            lease_id: attempt.lease_id,
                            fencing_token: attempt.fencing_token,
                            home_epoch,
                        },
                        expected_last_run_sequence: last_run_sequence,
                        append,
                    },
                    kind,
                    control_ids,
                    message_ids,
                    consumed_record_id: record_id,
                },
            )
            .await
            .map(|(_, count)| count)
            .map_err(control_error)
    }
}

impl LoopControlQueue for PostgresLoopControlQueue {
    fn drain_steering(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move { self.drain_kind(&attempt, RunControlKind::Steering).await })
    }

    fn drain_follow_up(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move { self.drain_kind(&attempt, RunControlKind::FollowUp).await })
    }
}

const fn control_kind_code(kind: RunControlKind) -> i16 {
    match kind {
        RunControlKind::Steering => 1,
        RunControlKind::FollowUp => 2,
    }
}

fn consumed_record_id(
    attempt: &AttemptContext,
    kind: RunControlKind,
    control_ids: &[CommandId],
) -> Result<LedgerRecordId, LoopPortError> {
    let mut seed = b"musteragent.legacy.loop-control-consumed.v1\0".to_vec();
    seed.extend_from_slice(attempt.run_id.into_uuid().as_bytes());
    seed.extend_from_slice(attempt.attempt_id.into_uuid().as_bytes());
    seed.extend_from_slice(&control_kind_code(kind).to_be_bytes());
    for control_id in control_ids {
        seed.extend_from_slice(control_id.into_uuid().as_bytes());
    }
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    LedgerRecordId::from_uuid(uuid::Uuid::from_bytes(value)).map_err(|_| invariant())
}

fn control_error(error: PostgresRunControlError) -> LoopPortError {
    match error {
        PostgresRunControlError::Execution(
            crate::execution::PostgresExecutionError::StaleFence,
        ) => stale_fence(),
        PostgresRunControlError::Ledger(crate::PostgresLedgerError::Contract(
            musteragent_ledger::LedgerError::SequenceConflict { .. },
        )) => LoopPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        },
        other => {
            tracing::error!(error = %other, "Run control consumption failed");
            match other {
                PostgresRunControlError::Database(_)
                | PostgresRunControlError::Transaction(_)
                | PostgresRunControlError::Ledger(_)
                | PostgresRunControlError::Execution(_) => LoopPortError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                },
                PostgresRunControlError::EnqueueContract(_)
                | PostgresRunControlError::ConsumeContract(_)
                | PostgresRunControlError::InvalidState
                | PostgresRunControlError::ContentNotAuthorized => invariant(),
            }
        }
    }
}

fn database(error: impl std::fmt::Display) -> LoopPortError {
    tracing::error!(error = %error, "Loop policy PostgreSQL operation failed");
    LoopPortError {
        code: ErrorCode::DependencyUnavailable,
        retryable: true,
    }
}

const fn invariant() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}

const fn stale_fence() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::AttemptLeaseStale,
        retryable: false,
    }
}

fn application_error(
    error: musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError,
) -> LoopPortError {
    match error {
        musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError::Unavailable => LoopPortError { code: ErrorCode::DependencyUnavailable, retryable: true },
        _ => invariant(),
    }
}
