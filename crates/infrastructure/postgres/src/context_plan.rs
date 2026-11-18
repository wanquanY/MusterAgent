use std::sync::Arc;

use musteragent_content_ref::Sha256;
pub(crate) mod dialogue;
pub(crate) mod dialogue_checkpoint;
pub(crate) mod lifecycle;
mod public_delivery_repair;
mod source_fence;
use musteragent_context::{
    CONTEXT_PLAN_GENERATION, ContextCompactionFailureCommit, ContextCompactionResolutionCommit,
    ContextCompactionStartCommit, ContextEvent, ContextPlanCommit, ContextPlanDisposition,
    ContextPlanError, ContextPlanResolution, ContextPlanStore, StoredContextPlanIdentity,
};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, AttemptId, ContextSnapshotId, ConversationId, CorrelationId, DottedKey, HomeEpoch,
    IdempotencyKey, InvocationId, LedgerOffset, LedgerRecordId, MessageId, OutputId, RunId, RunSeq,
    TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore};

#[derive(Clone)]
pub struct PostgresContextPlanStore {
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    content: Option<Arc<dyn musteragent_runtime_content::ContentStore>>,
    repair_decoder: Option<Arc<dyn musteragent_projection::LedgerPayloadDecoder>>,
    application_catalogs: Option<
        Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
}

impl PostgresContextPlanStore {
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
        encoder: Arc<dyn LedgerPayloadEncoder>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            ledger: PostgresLedgerStore::new(database),
            pool,
            encoder,
            content: None,
            repair_decoder: None,
            application_catalogs: None,
        }
    }

    #[must_use]
    pub fn with_delivery_repair_sources(
        mut self,
        content: Arc<dyn musteragent_runtime_content::ContentStore>,
        decoder: Arc<dyn musteragent_projection::LedgerPayloadDecoder>,
    ) -> Self {
        self.content = Some(content);
        self.repair_decoder = Some(decoder);
        self
    }

    pub async fn commit_inner(
        &self,
        plan: ContextPlanCommit,
        compaction: Option<ContextCompactionResolutionCommit>,
    ) -> Result<ContextPlanResolution, PostgresContextPlanError> {
        plan.snapshot.validate()?;
        if plan.snapshot.run_id != plan.attempt.run_id
            || plan.generation
                != plan
                    .snapshot
                    .plan_lineage
                    .map_or(CONTEXT_PLAN_GENERATION, |v| v.generation)
        {
            return Err(PostgresContextPlanError::IdentityMismatch);
        }
        if let Some(commit) = &compaction {
            commit
                .validate_for_plan(&plan)
                .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        }
        let committed_identity = StoredContextPlanIdentity::from_commit(&plan);
        let mut transaction =
            crate::commit_lane::acquire_for_run(self.ledger.database(), plan.attempt.run_id)
                .await?
                .ok_or(PostgresContextPlanError::FenceConflict)?;
        let state = self
            .load_run_state_on(&mut transaction, &plan.attempt)
            .await?;
        // A duplicate plan still belongs to the current physical Attempt.
        // Check before the idempotent append path can return an old identity.
        crate::execution::lock_and_validate_fence(
            &mut transaction,
            &AttemptFence {
                run_id: plan.attempt.run_id,
                attempt_id: plan.attempt.attempt_id,
                attempt_number: plan.attempt.attempt_number,
                worker_id: plan.attempt.worker_id.clone(),
                lease_id: plan.attempt.lease_id,
                fencing_token: plan.attempt.fencing_token,
                home_epoch: state.home_epoch,
            },
            state.last_run_sequence,
            state.now,
        )
        .await?;
        if let Some(cut) =
            crate::dialogue_context::run::run_cut(&mut transaction, plan.attempt.run_id, true)
                .await
                .map_err(|_| PostgresContextPlanError::SourceChanged)?
        {
            dialogue::require_complete_cut(&mut transaction, plan.attempt.run_id, &cut).await?;
        }
        source_fence::authorize_source(
            &mut transaction,
            state.tenant_id,
            state.conversation_id,
            &plan.source_fence,
        )
        .await?;
        lifecycle::validate_commit(
            &mut transaction,
            state.tenant_id,
            &plan,
            state.ledger_offset,
        )
        .await?;
        self.validate_delivery_repair(
            &mut transaction,
            state.tenant_id,
            &plan,
            state.ledger_offset,
        )
        .await?;
        let snapshot_payload =
            LedgerPayload::DomainEvent(DomainEvent::Context(ContextEvent::SnapshotCreated {
                snapshot: Box::new(plan.snapshot.clone()),
            }));
        let snapshot_encoded = self.encoder.encode(snapshot_payload);
        let snapshot_payload_hash = Sha256::digest(&snapshot_encoded);
        let ledger_record_id = LedgerRecordId::from_uuid(plan.snapshot.snapshot_id.into_uuid())
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        let correlation_id = CorrelationId::from_uuid(plan.invocation_id.into_uuid())
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        let first_run_sequence = state
            .last_run_sequence
            .get()
            .checked_add(1)
            .ok_or(PostgresContextPlanError::NumericRange)?;
        let snapshot_run_sequence =
            RunSeq::new(first_run_sequence).map_err(|_| PostgresContextPlanError::NumericRange)?;
        let mut records = vec![ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
            record_id: ledger_record_id,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(state.activity_id),
            run_id: Some(plan.snapshot.run_id),
            run_seq: Some(snapshot_run_sequence),
            attempt_id: Some(plan.attempt.attempt_id),
            participant_id: None,
            producer: Producer::Worker,
            occurred_at: state.now,
            causation_id: plan.snapshot.retained_record_ids.last().copied(),
            correlation_id,
            payload: snapshot_encoded,
            payload_hash: snapshot_payload_hash,
        }];
        if let Some(commit) = &compaction {
            let invocation_id = commit.resolution.invocation_id;
            let resolved_record = derived_record_id(invocation_id, b"resolved")
                .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
            let resolved_encoded =
                self.encoder
                    .encode(LedgerPayload::DomainEvent(DomainEvent::Context(
                        ContextEvent::CompactionResolved {
                            resolution: Box::new(commit.resolution.clone()),
                        },
                    )));
            records.push(ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1")
                    .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
                record_id: resolved_record,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(state.activity_id),
                run_id: Some(plan.snapshot.run_id),
                run_seq: Some(
                    RunSeq::new(
                        first_run_sequence
                            .checked_add(1)
                            .ok_or(PostgresContextPlanError::NumericRange)?,
                    )
                    .map_err(|_| PostgresContextPlanError::NumericRange)?,
                ),
                attempt_id: Some(plan.attempt.attempt_id),
                participant_id: None,
                producer: Producer::Worker,
                occurred_at: state.now,
                causation_id: Some(ledger_record_id),
                correlation_id: CorrelationId::from_uuid(invocation_id.into_uuid())
                    .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
                payload_hash: Sha256::digest(&resolved_encoded),
                payload: resolved_encoded,
            });
        }
        let final_run_sequence = state
            .last_run_sequence
            .get()
            .checked_add(
                u64::try_from(records.len()).map_err(|_| PostgresContextPlanError::NumericRange)?,
            )
            .ok_or(PostgresContextPlanError::NumericRange)?;
        let idempotency_key =
            IdempotencyKey::new(format!("context-plan:{}", plan.snapshot.snapshot_id))
                .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        let append = AppendRequest {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            expected_home_epoch: state.home_epoch,
            expected_offset: state.ledger_offset,
            command_fingerprint: plan.snapshot.checksum,
            idempotency_key,
            records,
            outbox: Vec::new(),
        };

        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return self.verify_duplicate(&plan, ledger_record_id).await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let committed_sequence = prepared
            .sequences()
            .first()
            .copied()
            .ok_or(PostgresContextPlanError::CorruptState)?;
        let mut staged = prepared.stage(&append).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &AttemptFence {
                run_id: plan.attempt.run_id,
                attempt_id: plan.attempt.attempt_id,
                attempt_number: plan.attempt.attempt_number,
                worker_id: plan.attempt.worker_id.clone(),
                lease_id: plan.attempt.lease_id,
                fencing_token: plan.attempt.fencing_token,
                home_epoch: state.home_epoch,
            },
            state.last_run_sequence,
            state.now,
        )
        .await?;
        insert_context_plan(
            staged.transaction_mut(),
            &plan,
            ledger_record_id,
            committed_sequence.get(),
            state.now,
        )
        .await?;
        for delivery in &plan.delegation_delivery_ids {
            let changed=sqlx::query("UPDATE runtime.delegation_deliveries SET state=2,snapshot_id=$3 WHERE delivery_id=$1 AND parent_run_id=$2 AND state IN (1,2)")
                .bind(delivery.into_uuid()).bind(plan.attempt.run_id.into_uuid()).bind(plan.snapshot.snapshot_id.into_uuid()).execute(staged.transaction_mut()).await?.rows_affected();
            if changed != 1 {
                return Err(PostgresContextPlanError::CorruptState);
            }
        }
        let advanced = sqlx::query(
            "UPDATE runtime.runs
             SET last_run_sequence = $2::bigint,
                 run_version = run_version + 1
             WHERE run_id = $1
               AND last_run_sequence = $3::bigint",
        )
        .bind(plan.attempt.run_id.into_uuid())
        .bind(final_run_sequence.to_string())
        .bind(state.last_run_sequence.get().to_string())
        .execute(&mut *staged.transaction_mut())
        .await?;
        if advanced.rows_affected() != 1 {
            return Err(PostgresContextPlanError::FenceConflict);
        }
        staged.commit().await?;
        Ok(ContextPlanResolution {
            disposition: ContextPlanDisposition::Applied,
            identity: committed_identity,
        })
    }

    async fn load_run_state_on(
        &self,
        connection: &mut sqlx::PgConnection,
        attempt: &musteragent_run::AttemptContext,
    ) -> Result<RunState, PostgresContextPlanError> {
        let row = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    stream.last_sequence::text AS ledger_offset,
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             FROM runtime.runs AS run
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             WHERE run.run_id = $1
               AND run.status IN (2, 4)",
        )
        .bind(attempt.run_id.into_uuid())
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(PostgresContextPlanError::FenceConflict)?;
        Ok(RunState {
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
            conversation_id: ConversationId::from_uuid(row.try_get("conversation_id")?)
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
            activity_id: ActivityId::from_uuid(row.try_get("activity_id")?)
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
            home_epoch: HomeEpoch::new(parse_u64(row.try_get("home_epoch")?)?)
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
            last_run_sequence: RunSeq::new(parse_u64(row.try_get("last_run_sequence")?)?)
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
            ledger_offset: LedgerOffset::new(parse_u64(row.try_get("ledger_offset")?)?),
            now: UtcTimestamp::from_unix_millis(row.try_get("now_ms")?),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_compaction_event(
        &self,
        attempt: &musteragent_run::AttemptContext,
        invocation_id: InvocationId,
        event: ContextEvent,
        record_id: LedgerRecordId,
        causation_id: Option<LedgerRecordId>,
        idempotency_tag: &str,
    ) -> Result<ContextPlanDisposition, PostgresContextPlanError> {
        let mut transaction =
            crate::commit_lane::acquire_for_run(self.ledger.database(), attempt.run_id)
                .await?
                .ok_or(PostgresContextPlanError::FenceConflict)?;
        let state = self.load_run_state_on(&mut transaction, attempt).await?;
        let encoded = self
            .encoder
            .encode(LedgerPayload::DomainEvent(DomainEvent::Context(event)));
        let payload_hash = Sha256::digest(&encoded);
        let next_run_sequence = state
            .last_run_sequence
            .get()
            .checked_add(1)
            .ok_or(PostgresContextPlanError::NumericRange)?;
        let run_sequence =
            RunSeq::new(next_run_sequence).map_err(|_| PostgresContextPlanError::NumericRange)?;
        let correlation_id = CorrelationId::from_uuid(invocation_id.into_uuid())
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        let append = AppendRequest {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            expected_home_epoch: state.home_epoch,
            expected_offset: state.ledger_offset,
            command_fingerprint: payload_hash,
            idempotency_key: IdempotencyKey::new(format!(
                "context-compaction:{idempotency_tag}:{invocation_id}"
            ))
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
            records: vec![ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1")
                    .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
                record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(state.activity_id),
                run_id: Some(attempt.run_id),
                run_seq: Some(run_sequence),
                attempt_id: Some(attempt.attempt_id),
                participant_id: None,
                producer: Producer::Worker,
                occurred_at: state.now,
                causation_id,
                correlation_id,
                payload: encoded,
                payload_hash,
            }],
            outbox: Vec::new(),
        };
        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => return Ok(ContextPlanDisposition::Duplicate),
            BeginAppend::Ready(prepared) => prepared,
        };
        let mut staged = prepared.stage(&append).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &AttemptFence {
                run_id: attempt.run_id,
                attempt_id: attempt.attempt_id,
                attempt_number: attempt.attempt_number,
                worker_id: attempt.worker_id.clone(),
                lease_id: attempt.lease_id,
                fencing_token: attempt.fencing_token,
                home_epoch: state.home_epoch,
            },
            state.last_run_sequence,
            state.now,
        )
        .await?;
        let advanced = sqlx::query(
            "UPDATE runtime.runs
             SET last_run_sequence = $2::bigint,
                 run_version = run_version + 1
             WHERE run_id = $1
               AND last_run_sequence = $3::bigint",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(next_run_sequence.to_string())
        .bind(state.last_run_sequence.get().to_string())
        .execute(&mut *staged.transaction_mut())
        .await?;
        if advanced.rows_affected() != 1 {
            return Err(PostgresContextPlanError::FenceConflict);
        }
        staged.commit().await?;
        Ok(ContextPlanDisposition::Applied)
    }

    async fn verify_duplicate(
        &self,
        plan: &ContextPlanCommit,
        ledger_record_id: LedgerRecordId,
    ) -> Result<ContextPlanResolution, PostgresContextPlanError> {
        let row = sqlx::query(
            "SELECT snapshot_id, run_id, attempt_id, iteration, checksum, invocation_id,
                    assistant_message_id, assistant_output_id,
                    reasoning_output_id, ledger_record_id, lineage_generation
             FROM runtime.context_plans
             WHERE snapshot_id = $1",
        )
        .bind(plan.snapshot.snapshot_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresContextPlanError::CorruptState)?;
        let identity = stored_context_plan_identity(&row)?;
        let duplicate = identity.run_id == plan.snapshot.run_id
            && identity.iteration == plan.snapshot.iteration
            && identity.generation == plan.generation
            && row.try_get::<Vec<u8>, _>("checksum")? == plan.snapshot.checksum.into_bytes()
            && identity.invocation_id == plan.invocation_id
            && identity.assistant_message_id == plan.assistant_message_id
            && identity.assistant_output_id == plan.assistant_output_id
            && identity.reasoning_output_id == plan.reasoning_output_id
            && row.try_get::<uuid::Uuid, _>("ledger_record_id")? == ledger_record_id.into_uuid();
        if duplicate {
            Ok(ContextPlanResolution {
                disposition: ContextPlanDisposition::Duplicate,
                identity,
            })
        } else {
            Err(PostgresContextPlanError::IdempotencyConflict)
        }
    }
}

impl ContextPlanStore for PostgresContextPlanStore {
    fn bind_owner_sources(
        &self,
        commits: Vec<musteragent_context::DialogueContextBindCommit>,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanDisposition, ContextPlanError>,
    > {
        Box::pin(async move {
            self.bind_dialogue_batch_inner(commits)
                .await
                .map_err(map_plan_error)
        })
    }
    fn commit_dialogue_checkpoint(
        &self,
        commit: musteragent_context::DialogueContextCheckpointCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanDisposition, ContextPlanError>,
    > {
        Box::pin(async move {
            self.dialogue_checkpoint_inner(commit)
                .await
                .inspect_err(|e| tracing::warn!(error = %e,"Dialogue checkpoint commit failed"))
                .map_err(map_plan_error)
        })
    }

    fn bind_dialogue_source(
        &self,
        commit: musteragent_context::DialogueContextBindCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanDisposition, ContextPlanError>,
    > {
        Box::pin(async move {
            self.bind_dialogue_inner(commit)
                .await
                .inspect_err(|e| tracing::warn!(error = %e,"Dialogue source binding failed"))
                .map_err(map_plan_error)
        })
    }

    fn begin_compaction(
        &self,
        commit: ContextCompactionStartCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanDisposition, ContextPlanError>,
    > {
        Box::pin(async move {
            if commit.source_seq_from > commit.source_seq_to || commit.input_tokens_before == 0 {
                return Err(ContextPlanError::invariant());
            }
            let record_id = LedgerRecordId::from_uuid(commit.invocation_id.into_uuid())
                .map_err(|_| ContextPlanError::invariant())?;
            self.commit_compaction_event(
                &commit.attempt,
                commit.invocation_id,
                ContextEvent::CompactionStarted {
                    invocation_id: commit.invocation_id,
                    loop_iteration: commit.loop_iteration,
                    source_seq_from: commit.source_seq_from,
                    source_seq_to: commit.source_seq_to,
                    model_route_ref: commit.model_route_ref,
                    input_tokens_before: commit.input_tokens_before,
                },
                record_id,
                Some(commit.causation_id),
                "started",
            )
            .await
            .map_err(map_plan_error)
        })
    }

    fn fail_compaction(
        &self,
        commit: ContextCompactionFailureCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanDisposition, ContextPlanError>,
    > {
        Box::pin(async move {
            let started_record = LedgerRecordId::from_uuid(commit.invocation_id.into_uuid())
                .map_err(|_| ContextPlanError::invariant())?;
            let failure_record = derived_record_id(commit.invocation_id, b"failed")
                .map_err(|_| ContextPlanError::invariant())?;
            self.commit_compaction_event(
                &commit.attempt,
                commit.invocation_id,
                ContextEvent::CompactionFailed {
                    invocation_id: commit.invocation_id,
                    error: commit.error,
                },
                failure_record,
                Some(started_record),
                "failed",
            )
            .await
            .map_err(map_plan_error)
        })
    }

    fn resolve_compaction(
        &self,
        commit: ContextCompactionResolutionCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanDisposition, ContextPlanError>,
    > {
        Box::pin(async move {
            commit.resolution.validate()?;
            let invocation_id = commit.resolution.invocation_id;
            let started_record = LedgerRecordId::from_uuid(invocation_id.into_uuid())
                .map_err(|_| ContextPlanError::invariant())?;
            let resolved_record = derived_record_id(invocation_id, b"resolved")
                .map_err(|_| ContextPlanError::invariant())?;
            self.commit_compaction_event(
                &commit.attempt,
                invocation_id,
                ContextEvent::CompactionResolved {
                    resolution: Box::new(commit.resolution),
                },
                resolved_record,
                Some(started_record),
                "resolved",
            )
            .await
            .map_err(map_plan_error)
        })
    }

    fn commit(
        &self,
        plan: ContextPlanCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanDisposition, ContextPlanError>,
    > {
        Box::pin(async move {
            self.commit_inner(plan, None)
                .await
                .map(|value| value.disposition)
                .map_err(map_plan_error)
        })
    }

    fn resolve_or_commit(
        &self,
        plan: ContextPlanCommit,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanResolution, ContextPlanError>,
    > {
        Box::pin(async move { self.commit_inner(plan, None).await.map_err(map_plan_error) })
    }

    fn resolve_or_commit_with_compaction(
        &self,
        plan: ContextPlanCommit,
        compaction: Option<ContextCompactionResolutionCommit>,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextPlanResolution, ContextPlanError>,
    > {
        Box::pin(async move {
            self.commit_inner(plan, compaction)
                .await
                .map_err(map_plan_error)
        })
    }
}

fn stored_context_plan_identity(
    row: &sqlx::postgres::PgRow,
) -> Result<StoredContextPlanIdentity, PostgresContextPlanError> {
    Ok(StoredContextPlanIdentity {
        run_id: RunId::from_uuid(row.try_get("run_id")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        attempt_id: AttemptId::from_uuid(row.try_get("attempt_id")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        iteration: u32::try_from(row.try_get::<i32, _>("iteration")?)?,
        generation: row
            .try_get::<Option<i32>, _>("lineage_generation")?
            .map(|value| {
                u32::try_from(value)
                    .ok()
                    .and_then(std::num::NonZeroU32::new)
                    .ok_or(PostgresContextPlanError::CorruptState)
            })
            .transpose()?
            .unwrap_or(CONTEXT_PLAN_GENERATION),
        snapshot_id: ContextSnapshotId::from_uuid(row.try_get("snapshot_id")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        invocation_id: InvocationId::from_uuid(row.try_get("invocation_id")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        assistant_message_id: MessageId::from_uuid(row.try_get("assistant_message_id")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        assistant_output_id: OutputId::from_uuid(row.try_get("assistant_output_id")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
        reasoning_output_id: OutputId::from_uuid(row.try_get("reasoning_output_id")?)
            .map_err(|_| PostgresContextPlanError::CorruptState)?,
    })
}

fn derived_record_id(
    invocation_id: InvocationId,
    tag: &[u8],
) -> Result<LedgerRecordId, musteragent_identity::IdentityError> {
    LedgerRecordId::from_uuid(uuid::Uuid::new_v5(&invocation_id.into_uuid(), tag))
}

fn map_plan_error(error: PostgresContextPlanError) -> ContextPlanError {
    if matches!(error, PostgresContextPlanError::IdempotencyConflict) {
        ContextPlanError::idempotency_conflict()
    } else if error.retryable() {
        ContextPlanError::unavailable()
    } else {
        ContextPlanError::invariant()
    }
}

async fn insert_context_plan(
    transaction: &mut sqlx::PgConnection,
    plan: &ContextPlanCommit,
    ledger_record_id: LedgerRecordId,
    committed_sequence: u64,
    now: UtcTimestamp,
) -> Result<(), PostgresContextPlanError> {
    let retained = plan
        .snapshot
        .retained_record_ids
        .iter()
        .map(|record_id| record_id.into_uuid())
        .collect::<Vec<_>>();
    let inserted = sqlx::query(
        "INSERT INTO runtime.context_plans (
             snapshot_id, tenant_id, conversation_id, run_id, attempt_id,
             iteration, parent_snapshot_id, source_sequence_from,
             source_sequence_to, retained_record_ids, summary_content_id,
             summary_content_hash, summary_content_bytes, request_content_id,
             request_content_hash, request_content_bytes, input_tokens_before,
             input_tokens_after, model_route_ref, checksum, invocation_id,
             assistant_message_id, assistant_output_id, reasoning_output_id,
             ledger_record_id, committed_sequence, created_at_ms,
             delivery_repair_binding, delivery_repair_source_invocation_id,
             lineage_generation,predecessor_snapshot_id,predecessor_generation,predecessor_checksum,
             control_record_id,control_sequence,control_kind
         )
         SELECT $1, run.tenant_id, run.conversation_id, $2, $3,
                $4, $5, $6::bigint, $7::bigint, $8, $9, $10,
                $11::bigint, $12, $13, $14::bigint, $15, $16,
                $17, $18, $19, $20, $21, $22, $23, $24::bigint, $25, $26, $27,
                $28,$29,$30,$31,$32,$33::bigint,$34
         FROM runtime.runs AS run
         WHERE run.run_id = $2",
    )
    .bind(plan.snapshot.snapshot_id.into_uuid())
    .bind(plan.snapshot.run_id.into_uuid())
    .bind(plan.attempt.attempt_id.into_uuid())
    .bind(i32::try_from(plan.snapshot.iteration)?)
    .bind(
        plan.snapshot
            .parent_snapshot_id
            .map(|value| value.into_uuid()),
    )
    .bind(plan.snapshot.source_seq_from.get().to_string())
    .bind(plan.snapshot.source_seq_to.get().to_string())
    .bind(retained)
    .bind(plan.snapshot.structured_summary_ref.content_id.into_uuid())
    .bind(
        plan.snapshot
            .structured_summary_ref
            .sha256
            .into_bytes()
            .to_vec(),
    )
    .bind(plan.snapshot.structured_summary_ref.byte_length.to_string())
    .bind(plan.snapshot.model_request_ref.content_id.into_uuid())
    .bind(plan.snapshot.model_request_ref.sha256.into_bytes().to_vec())
    .bind(plan.snapshot.model_request_ref.byte_length.to_string())
    .bind(i64::try_from(plan.snapshot.input_tokens_before)?)
    .bind(i64::try_from(plan.snapshot.input_tokens_after)?)
    .bind(plan.snapshot.model_route_ref.as_str())
    .bind(plan.snapshot.checksum.into_bytes().to_vec())
    .bind(plan.invocation_id.into_uuid())
    .bind(plan.assistant_message_id.into_uuid())
    .bind(plan.assistant_output_id.into_uuid())
    .bind(plan.reasoning_output_id.into_uuid())
    .bind(ledger_record_id.into_uuid())
    .bind(committed_sequence.to_string())
    .bind(now.unix_millis())
    .bind(plan.snapshot.delivery_repair.as_ref().map(musteragent_persistence_codec::public_delivery_repair_codec::encode_public_delivery_repair_binding)
        .transpose().map_err(|_|PostgresContextPlanError::IdentityMismatch)?)
    .bind(plan.snapshot.delivery_repair.as_ref().map(|b|b.rejection_invocation_id.into_uuid()))
    .bind(plan.snapshot.plan_lineage.map(|v|i32::try_from(v.generation.get())).transpose()?)
    .bind(plan.snapshot.plan_lineage.and_then(|v|v.predecessor).map(|v|v.snapshot_id.into_uuid()))
    .bind(plan.snapshot.plan_lineage.and_then(|v|v.predecessor).map(|v|i32::try_from(v.generation.get())).transpose()?)
    .bind(plan.snapshot.plan_lineage.and_then(|v|v.predecessor).map(|v|v.checksum.into_bytes().to_vec()))
    .bind(plan.snapshot.plan_lineage.and_then(|v|v.control).map(|v|v.record_id.into_uuid()))
    .bind(plan.snapshot.plan_lineage.and_then(|v|v.control).map(|v|v.sequence.get().to_string()))
    .bind(plan.snapshot.plan_lineage.and_then(|v|v.control).map(|v|match v.kind {musteragent_run::RunControlKind::Steering=>1i16,musteragent_run::RunControlKind::FollowUp=>2i16}))
    .execute(&mut *transaction)
    .await?;
    if inserted.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresContextPlanError::CorruptState)
    }
}

struct RunState {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    activity_id: ActivityId,
    home_epoch: HomeEpoch,
    last_run_sequence: RunSeq,
    ledger_offset: LedgerOffset,
    now: UtcTimestamp,
}

fn parse_u64(value: String) -> Result<u64, PostgresContextPlanError> {
    value
        .parse()
        .map_err(|_| PostgresContextPlanError::CorruptState)
}

#[derive(Debug, Error)]
pub enum PostgresContextPlanError {
    #[error("Context source changed before plan commit")]
    SourceChanged,
    #[error("Context plan identity is inconsistent")]
    IdentityMismatch,
    #[error("Context plan idempotency fingerprint conflicts")]
    IdempotencyConflict,
    #[error("Context plan is corrupt")]
    CorruptState,
    #[error("Context plan lost its active Attempt fence")]
    FenceConflict,
    #[error("Context plan numeric value is outside PostgreSQL range")]
    NumericRange,
    #[error(transparent)]
    Context(#[from] musteragent_context::ContextContractError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] crate::PostgresExecutionError),
    #[error(transparent)]
    Content(#[from] musteragent_runtime_content::ContentStoreError),
    #[error("PostgreSQL Context plan operation failed")]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Context plan Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("numeric conversion failed")]
    Numeric(#[from] std::num::TryFromIntError),
}

impl From<crate::commit_lane::CommitLaneError> for PostgresContextPlanError {
    fn from(error: crate::commit_lane::CommitLaneError) -> Self {
        match error {
            crate::commit_lane::CommitLaneError::StaleFence => Self::FenceConflict,
            crate::commit_lane::CommitLaneError::Corrupt => Self::CorruptState,
            crate::commit_lane::CommitLaneError::Numeric
            | crate::commit_lane::CommitLaneError::NumericConversion(_)
            | crate::commit_lane::CommitLaneError::NumericRange(_) => Self::NumericRange,
            crate::commit_lane::CommitLaneError::Database(error) => Self::Database(error),
            crate::commit_lane::CommitLaneError::Transaction(error) => Self::Transaction(error),
        }
    }
}

impl PostgresContextPlanError {
    const fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Ledger(_)
                | Self::Content(_)
                | Self::Execution(_)
                | Self::Database(_)
                | Self::FenceConflict
                | Self::SourceChanged
        )
    }
}
