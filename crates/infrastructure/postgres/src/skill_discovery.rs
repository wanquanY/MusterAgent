use std::num::NonZeroU32;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_discovery::{
    DiscoveryEvent, PriorSkillDisclosure, PriorSkillDisclosureProvenance, SkillDisclosed,
    SkillDisclosureItem, SkillDisclosureState,
};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    CorrelationId, InvocationId, LedgerRecordId, RunId, RunSeq, SkillVersionId, TenantId,
    UtcTimestamp,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{AttemptContext, RunContext, RunEvent};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_discovery::{
    CommitSkillDisclosure, DiscoveryStoreError, FencedSkillDisclosureCommitter,
    SkillDisclosureCommitDisposition, SkillDiscoveryMaterial, SkillDiscoveryMaterialSource,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::artifact::{PostgresArtifactError, authorize_result_content, complete_tool};
use crate::commit_lane::{CommitLaneError, CommitLaneState};
use crate::execution::{PostgresExecutionError, lock_and_validate_fence};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};
use crate::run_ledger::{RunLedgerReadError, load_run_domain_events};

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

#[derive(Clone)]
pub struct PostgresSkillDiscoveryMaterialSource {
    pool: PgPool,
    snapshots: Arc<dyn RuntimeSnapshotDecoder>,
    ledger_decoder: Arc<dyn LedgerPayloadDecoder>,
}

impl PostgresSkillDiscoveryMaterialSource {
    #[must_use]
    pub fn new(
        pool: PgPool,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
        ledger_decoder: Arc<dyn LedgerPayloadDecoder>,
    ) -> Self {
        Self {
            pool,
            snapshots,
            ledger_decoder,
        }
    }

    async fn load_current(
        &self,
        attempt: &AttemptContext,
        expected: &RunContext,
    ) -> Result<SkillDiscoveryMaterial, SkillDiscoveryError> {
        if attempt.run_id != expected.run_id {
            return Err(SkillDiscoveryError::InvalidCommand);
        }
        let row = sqlx::query(
            "SELECT snapshot.encoded_snapshot, snapshot.content_hash
             FROM runtime.runs AS run
             JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = run.tenant_id AND attempt.run_id = run.run_id
             JOIN runtime.leases AS lease
               ON lease.tenant_id = run.tenant_id AND lease.run_id = run.run_id
              AND lease.attempt_id = attempt.attempt_id
             JOIN capability.runtime_snapshots AS snapshot
               ON snapshot.tenant_id = run.tenant_id AND snapshot.run_id = run.run_id
              AND snapshot.runtime_snapshot_id = run.runtime_snapshot_id
             WHERE run.tenant_id = $1 AND run.run_id = $2
               AND run.conversation_id = $3 AND run.activity_id = $4
               AND run.home_epoch = $5::bigint AND run.runtime_snapshot_id = $6
               AND run.status IN (2, 4)
               AND attempt.attempt_id = $7 AND attempt.attempt_number = $8
               AND attempt.status = 1 AND lease.worker_id = $9
               AND lease.lease_id = $10 AND lease.fencing_token = $11::bigint
               AND lease.expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .bind(expected.tenant_id.into_uuid())
        .bind(expected.run_id.into_uuid())
        .bind(expected.conversation_id.into_uuid())
        .bind(expected.activity_id.into_uuid())
        .bind(expected.home_epoch.get().to_string())
        .bind(expected.runtime_snapshot_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(i32::try_from(attempt.attempt_number.get())?)
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SkillDiscoveryError::StaleFence)?;
        let material = self.material_from_snapshot_row(*expected, &row).await?;
        let mut connection = self.pool.acquire().await?;
        crate::dialogue_context::run::run_cut(&mut connection, attempt.run_id, false)
            .await
            .map_err(SkillDiscoveryError::DialogueAuthority)?;
        Ok(material)
    }

    async fn material_from_snapshot_row(
        &self,
        run_context: RunContext,
        row: &sqlx::postgres::PgRow,
    ) -> Result<SkillDiscoveryMaterial, SkillDiscoveryError> {
        let encoded: Vec<u8> = row.try_get("encoded_snapshot")?;
        let persisted = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
            .map_err(|_| SkillDiscoveryError::Corrupt)?;
        if Sha256::digest(&encoded) != persisted {
            return Err(SkillDiscoveryError::Corrupt);
        }
        let snapshot = self
            .snapshots
            .decode_runtime_snapshot(&encoded)
            .map_err(|_| SkillDiscoveryError::Invariant("snapshot.decode"))?;
        let discovery = snapshot
            .skill_discovery()
            .map_err(|_| SkillDiscoveryError::Invariant("snapshot.skill_discovery"))?
            .clone();
        let catalog = snapshot
            .frozen_authorized_skill_catalog()
            .map_err(|_| SkillDiscoveryError::Invariant("skill_catalog.frozen_material"))?;
        let authorized_skill_snapshot_hash = catalog.catalog_hash();
        let projected = self
            .load_projected(run_context.tenant_id, run_context.run_id)
            .await?;
        let disclosure_state = SkillDisclosureState::from_projection(
            &catalog,
            &discovery.initial_disclosed_skill_refs,
            &projected,
        )
        .map_err(|_| SkillDiscoveryError::Corrupt)?;
        Ok(SkillDiscoveryMaterial {
            run_context,
            discovery,
            authorized_skill_snapshot_hash,
            authorized_skills: catalog,
            disclosure_state,
        })
    }

    async fn load_projected(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> Result<Vec<SkillDisclosed>, SkillDiscoveryError> {
        let records =
            load_run_domain_events(&self.pool, self.ledger_decoder.as_ref(), tenant_id, run_id)
                .await?;
        Ok(records
            .into_iter()
            .filter_map(|record| match record.event {
                DomainEvent::Discovery(DiscoveryEvent::SkillDisclosed(event)) => Some(event),
                _ => None,
            })
            .collect())
    }

    /// Loads a prior terminal Run from its immutable Snapshot prefix and
    /// canonical Ledger suffix. The mutable active Skill pointer is never read.
    pub async fn load_prior_disclosures(
        &self,
        tenant_id: TenantId,
        source_run_id: RunId,
    ) -> Result<Vec<PriorSkillDisclosure>, DiscoveryStoreError> {
        self.load_prior_inner(tenant_id, source_run_id)
            .await
            .map_err(SkillDiscoveryError::port)
    }

    async fn load_prior_inner(
        &self,
        tenant_id: TenantId,
        source_run_id: RunId,
    ) -> Result<Vec<PriorSkillDisclosure>, SkillDiscoveryError> {
        let row = sqlx::query(
            "SELECT run.conversation_id, run.activity_id, run.activity_kind,
                    run.executor_participant_id,
                    run.requested_by_participant_id, run.parent_run_id, run.delegation_id,
                    run.execution_scope_kind, run.workspace_id, run.device_id,
                    run.control_home_kind, run.control_device_id, run.execution_home_kind,
                    run.execution_device_id, run.home_epoch::text AS home_epoch,
                    run.runtime_snapshot_id, snapshot.encoded_snapshot, snapshot.content_hash
             FROM runtime.runs AS run
             JOIN capability.runtime_snapshots AS snapshot
               ON snapshot.tenant_id = run.tenant_id AND snapshot.run_id = run.run_id
              AND snapshot.runtime_snapshot_id = run.runtime_snapshot_id
             WHERE run.tenant_id = $1 AND run.run_id = $2 AND run.status IN (5, 6, 7, 8)",
        )
        .bind(tenant_id.into_uuid())
        .bind(source_run_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SkillDiscoveryError::Corrupt)?;
        let run_context = crate::tool_material::decode_run_context_for_skill_discovery(
            tenant_id,
            source_run_id,
            &row,
        )
        .ok_or(SkillDiscoveryError::Corrupt)?;
        let material = self.material_from_snapshot_row(run_context, &row).await?;
        let records = load_run_domain_events(
            &self.pool,
            self.ledger_decoder.as_ref(),
            tenant_id,
            source_run_id,
        )
        .await?;
        let mut run_records = records.iter().filter(|record| {
            matches!(
                &record.event,
                DomainEvent::Run(RunEvent::Admitted { runtime_snapshot_id, .. })
                    if *runtime_snapshot_id == run_context.runtime_snapshot_id
            )
        });
        let admission_record_id = run_records
            .next()
            .map(|record| record.record_id)
            .ok_or(SkillDiscoveryError::Corrupt)?;
        if run_records.next().is_some() {
            return Err(SkillDiscoveryError::Corrupt);
        }
        let mut prior = material
            .discovery
            .initial_disclosed_skill_refs
            .iter()
            .enumerate()
            .map(|(index, skill_ref)| {
                let skill = material
                    .authorized_skills
                    .get(skill_ref.as_str())
                    .ok_or(SkillDiscoveryError::Corrupt)?;
                Ok(PriorSkillDisclosure {
                    source_run_id,
                    provenance: PriorSkillDisclosureProvenance::AdmissionSnapshot {
                        ledger_record_id: admission_record_id,
                        runtime_snapshot_id: run_context.runtime_snapshot_id,
                    },
                    disclosure_ordinal: u32::try_from(index.saturating_add(1))
                        .ok()
                        .and_then(NonZeroU32::new)
                        .ok_or(SkillDiscoveryError::Corrupt)?,
                    skill_version_id: skill.skill_version_id,
                    skill_ref: skill.skill_ref.clone(),
                    instructions_ref: skill.instructions_ref.clone(),
                    instructions_hash: skill.instructions_hash,
                })
            })
            .collect::<Result<Vec<_>, SkillDiscoveryError>>()?;
        for record in records {
            let DomainEvent::Discovery(DiscoveryEvent::SkillDisclosed(event)) = record.event else {
                continue;
            };
            let skill = material
                .authorized_skills
                .get(event.skill_ref.as_str())
                .ok_or(SkillDiscoveryError::Corrupt)?;
            if event.instructions_ref != skill.instructions_ref
                || event.instructions_hash != skill.instructions_hash
            {
                return Err(SkillDiscoveryError::Corrupt);
            }
            prior.push(PriorSkillDisclosure {
                source_run_id,
                provenance: PriorSkillDisclosureProvenance::LedgerDisclosure {
                    ledger_record_id: record.record_id,
                    source_invocation_id: event.source_invocation_id,
                },
                disclosure_ordinal: event.disclosure_ordinal,
                skill_version_id: skill.skill_version_id,
                skill_ref: event.skill_ref,
                instructions_ref: event.instructions_ref,
                instructions_hash: event.instructions_hash,
            });
        }
        Ok(prior)
    }
}

impl SkillDiscoveryMaterialSource for PostgresSkillDiscoveryMaterialSource {
    fn load<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        expected_run_context: &'a RunContext,
    ) -> ApplicationFuture<'a, Result<SkillDiscoveryMaterial, DiscoveryStoreError>> {
        Box::pin(async move {
            let result = self.load_current(attempt, expected_run_context).await;
            if let Err(error) = &result
                && let Some(check) = error.diagnostic_check()
            {
                tracing::error!(
                    tenant_id = %expected_run_context.tenant_id,
                    run_id = %attempt.run_id,
                    attempt_id = %attempt.attempt_id,
                    runtime_snapshot_id = %expected_run_context.runtime_snapshot_id,
                    invariant.check = check,
                    error = ?error,
                    "Skill discovery material rejected"
                );
            }
            result.map_err(SkillDiscoveryError::port)
        })
    }
}

#[derive(Clone)]
pub struct PostgresFencedSkillDisclosureCommitter {
    liveness_clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

impl PostgresFencedSkillDisclosureCommitter {
    /// Supply the same Home clock used by the invocation Journal and activity observers.
    #[must_use]
    pub fn with_execution_liveness(
        mut self,
        clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
    ) -> Self {
        self.liveness_clock = Some(clock);
        self
    }

    #[must_use]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        reference_keys: Arc<OpaqueRefKeyring>,
        audit: Arc<crate::PostgresSecurityAuditPipeline>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            liveness_clock: None,
            ledger: PostgresLedgerStore::new(database),
            pool,
            encoder,
            reference_keys,
            audit,
        }
    }

    async fn commit_inner(
        &self,
        attempt: &AttemptContext,
        command: CommitSkillDisclosure,
    ) -> Result<SkillDisclosureCommitDisposition, SkillDisclosureCommitError> {
        validate_command(attempt, &command)?;
        let (mut transaction, state) =
            crate::commit_lane::acquire(self.ledger.database(), attempt).await?;
        validate_run_context(&state, &command.run_context)?;
        crate::dialogue_context::run::run_cut(&mut transaction, attempt.run_id, true)
            .await
            .map_err(SkillDisclosureCommitError::DialogueAuthority)?;
        let mut append = self.append(&state, attempt, &command)?;
        let liveness = crate::execution_liveness::stage_tool_terminal(
            &mut transaction,
            self.liveness_clock.as_deref(),
            self.encoder.as_ref(),
            attempt,
            command.invocation_id,
            &mut append,
        )
        .await?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return if self
                    .verify_duplicate(state.tenant_id, attempt.run_id, &command)
                    .await?
                {
                    crate::tool_security_audit::replay_succeeded(
                        &self.audit,
                        &self.pool,
                        state.tenant_id,
                        command.invocation_id,
                    )
                    .await?;
                    Ok(SkillDisclosureCommitDisposition::AlreadyApplied)
                } else {
                    Err(SkillDisclosureCommitError::IdempotencyConflict)
                };
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let persisted =
            load_disclosures(prepared.transaction_mut(), state.tenant_id, attempt.run_id).await?;
        validate_next(&command.initial_disclosures, &persisted, &command.event)?;
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        lock_invocation(
            prepared.transaction_mut(),
            state.tenant_id,
            command.invocation_id,
            attempt,
        )
        .await?;
        validate_instructions_content(
            prepared.transaction_mut(),
            state.tenant_id,
            &command.event.instructions_ref,
        )
        .await?;
        authorize_result_content(prepared.transaction_mut(), state.tenant_id, &command.result)
            .await?;
        insert_disclosure(
            prepared.transaction_mut(),
            state.tenant_id,
            attempt.run_id,
            deterministic_record_id(command.invocation_id, 1)?,
            &command,
        )
        .await?;
        complete_tool(
            prepared.transaction_mut(),
            &self.reference_keys,
            state.tenant_id,
            command.invocation_id,
            &command.result,
            command.committed_at,
            false,
        )
        .await?;
        crate::tool_security_audit::append_succeeded_in_transaction(
            &self.audit,
            prepared.transaction_mut(),
            state.tenant_id,
            command.invocation_id,
        )
        .await?;
        if let Some(change) = &liveness {
            change.persist(prepared.transaction_mut()).await?;
        }
        advance_run_sequence(
            prepared.transaction_mut(),
            state.tenant_id,
            attempt.run_id,
            state.next_sequence(
                u64::try_from(append.records.len())
                    .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
            )?,
        )
        .await?;
        prepared.commit(&append).await?;
        Ok(SkillDisclosureCommitDisposition::Applied)
    }

    fn append(
        &self,
        state: &CommitLaneState,
        attempt: &AttemptContext,
        command: &CommitSkillDisclosure,
    ) -> Result<AppendRequest, SkillDisclosureCommitError> {
        let events = [
            DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                invocation_id: command.invocation_id,
                result_ref: command.result.result_ref.clone(),
                result_hash: command.result.result_hash,
                provider_request_ref: command.result.provider_request_ref.clone(),
                authorized_content: Vec::new(),
            }),
            DomainEvent::Discovery(DiscoveryEvent::SkillDisclosed(command.event.clone())),
        ];
        let records = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let payload = self.encoder.encode(LedgerPayload::DomainEvent(event));
                Ok(ProposedRecord {
                    schema_version: musteragent_identity::DottedKey::new(RECORD_SCHEMA)
                        .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
                    record_id: deterministic_record_id(command.invocation_id, index)?,
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(state.activity_id),
                    run_id: Some(attempt.run_id),
                    run_seq: Some(
                        RunSeq::new(state.next_sequence(u64::try_from(index)?.saturating_add(1))?)
                            .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
                    ),
                    attempt_id: Some(attempt.attempt_id),
                    participant_id: Some(command.run_context.executor_participant_id),
                    producer: Producer::Worker,
                    occurred_at: command.committed_at,
                    causation_id: None,
                    correlation_id: CorrelationId::from_uuid(command.invocation_id.into_uuid())
                        .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
                    payload_hash: Sha256::digest(&payload),
                    payload,
                })
            })
            .collect::<Result<Vec<_>, SkillDisclosureCommitError>>()?;
        Ok(state.append(
            format!("discovery-skill:{}", command.invocation_id),
            records,
        )?)
    }

    async fn verify_duplicate(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
        command: &CommitSkillDisclosure,
    ) -> Result<bool, SkillDisclosureCommitError> {
        let mut connection = self.pool.acquire().await?;
        let persisted = load_disclosures(&mut connection, tenant_id, run_id).await?;
        let result_matches: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM tool.tool_invocations AS invocation
                 JOIN tool.tool_results AS result USING (invocation_id)
                 WHERE invocation.tenant_id = $1 AND invocation.run_id = $2
                   AND invocation.invocation_id = $3 AND invocation.status = 4
                   AND invocation.result_content_id = $4 AND result.result_hash = $5
                   AND result.result_schema_key = $6 AND result.result_schema_hash = $7
                   AND result.committed_at_ms = $8
             )",
        )
        .bind(tenant_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(command.invocation_id.into_uuid())
        .bind(command.result.result_ref.content_id.into_uuid())
        .bind(command.result.result_hash.into_bytes().as_slice())
        .bind(command.result.result_schema.schema_key.as_str())
        .bind(
            command
                .result
                .result_schema
                .schema_hash
                .into_bytes()
                .as_slice(),
        )
        .bind(command.committed_at.unix_millis())
        .fetch_one(&mut *connection)
        .await?;
        Ok(result_matches
            && persisted.iter().any(|item| {
                item.event == command.event && item.skill_version_id == command.skill_version_id
            }))
    }
}

impl FencedSkillDisclosureCommitter for PostgresFencedSkillDisclosureCommitter {
    fn commit_skill_disclosure<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: CommitSkillDisclosure,
    ) -> ApplicationFuture<'a, Result<SkillDisclosureCommitDisposition, DiscoveryStoreError>> {
        Box::pin(async move {
            self.commit_inner(attempt, command).await.map_err(|error| {
                if matches!(
                    &error,
                    SkillDisclosureCommitError::DialogueAuthority(
                        musteragent_runtime_session::DialogueStoreError::RevisionConflict
                            | musteragent_runtime_session::DialogueStoreError::Inactive
                            | musteragent_runtime_session::DialogueStoreError::Unavailable
                    )
                ) {
                    tracing::warn!(
                        "fenced Skill disclosure rejected by current Dialogue authority"
                    );
                } else {
                    tracing::error!(error = ?error, "fenced Skill disclosure commit failed");
                }
                error.port()
            })
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PersistedDisclosure {
    skill_version_id: SkillVersionId,
    event: SkillDisclosed,
}

async fn load_disclosures(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<Vec<PersistedDisclosure>, SkillDisclosureCommitError> {
    let rows = sqlx::query(
        "SELECT skill_ref, skill_version_id, instructions_content_id,
                instructions_media_type, instructions_byte_length::text AS instructions_byte_length,
                instructions_hash, ordinal, source_invocation_id, disclosed_at_ms
         FROM runtime.skill_disclosures
         WHERE tenant_id = $1 AND run_id = $2 ORDER BY ordinal",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_all(connection)
    .await?;
    rows.into_iter().map(decode_disclosure).collect()
}

fn decode_disclosure(
    row: sqlx::postgres::PgRow,
) -> Result<PersistedDisclosure, SkillDisclosureCommitError> {
    let instructions_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("instructions_hash")?)
        .map_err(|_| SkillDisclosureCommitError::Corrupt)?;
    let ordinal = u32::try_from(row.try_get::<i32, _>("ordinal")?)
        .ok()
        .and_then(std::num::NonZeroU32::new)
        .ok_or(SkillDisclosureCommitError::Corrupt)?;
    Ok(PersistedDisclosure {
        skill_version_id: SkillVersionId::from_uuid(row.try_get("skill_version_id")?)
            .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
        event: SkillDisclosed {
            skill_ref: musteragent_identity::VersionedKey::new(
                row.try_get::<String, _>("skill_ref")?,
            )
            .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
            instructions_ref: ContentRef {
                content_id: musteragent_identity::ContentId::from_uuid(
                    row.try_get("instructions_content_id")?,
                )
                .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
                media_type: MediaType::new(row.try_get::<String, _>("instructions_media_type")?)
                    .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
                byte_length: row
                    .try_get::<String, _>("instructions_byte_length")?
                    .parse()
                    .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
                sha256: instructions_hash,
            },
            instructions_hash,
            disclosure_ordinal: ordinal,
            source_invocation_id: InvocationId::from_uuid(row.try_get("source_invocation_id")?)
                .map_err(|_| SkillDisclosureCommitError::Corrupt)?,
            disclosed_at: UtcTimestamp::from_unix_millis(row.try_get("disclosed_at_ms")?),
        },
    })
}

fn validate_command(
    attempt: &AttemptContext,
    command: &CommitSkillDisclosure,
) -> Result<(), SkillDisclosureCommitError> {
    if command.run_context.run_id != attempt.run_id
        || command.invocation_id != command.event.source_invocation_id
        || command.event.disclosed_at != command.committed_at
        || command.event.instructions_ref.sha256 != command.event.instructions_hash
        || command.result.provider_request_ref.is_some()
        || command.result.validate().is_err()
        || command
            .initial_disclosures
            .windows(2)
            .any(|pair| pair[0].disclosure_ordinal.get() + 1 != pair[1].disclosure_ordinal.get())
        || command
            .initial_disclosures
            .first()
            .is_some_and(|item| item.disclosure_ordinal.get() != 1)
    {
        return Err(SkillDisclosureCommitError::InvalidCommand);
    }
    Ok(())
}

fn validate_run_context(
    state: &CommitLaneState,
    context: &RunContext,
) -> Result<(), SkillDisclosureCommitError> {
    if state.tenant_id != context.tenant_id
        || state.conversation_id != context.conversation_id
        || state.activity_id != context.activity_id
        || state.home_epoch != context.home_epoch
    {
        Err(SkillDisclosureCommitError::InvalidCommand)
    } else {
        Ok(())
    }
}

fn validate_next(
    initial: &[SkillDisclosureItem],
    persisted: &[PersistedDisclosure],
    event: &SkillDisclosed,
) -> Result<(), SkillDisclosureCommitError> {
    let expected = initial
        .len()
        .saturating_add(persisted.len())
        .saturating_add(1);
    if usize::try_from(event.disclosure_ordinal.get()).ok() != Some(expected)
        || initial.iter().any(|item| item.skill_ref == event.skill_ref)
        || persisted
            .iter()
            .any(|item| item.event.skill_ref == event.skill_ref)
    {
        Err(SkillDisclosureCommitError::TransitionConflict)
    } else {
        Ok(())
    }
}

async fn lock_invocation(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    attempt: &AttemptContext,
) -> Result<(), SkillDisclosureCommitError> {
    let locked: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT invocation_id FROM tool.tool_invocations
         WHERE tenant_id = $1 AND invocation_id = $2 AND run_id = $3
           AND status = 3 AND execution_attempt_id = $4 FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    if locked.is_some() {
        Ok(())
    } else {
        Err(SkillDisclosureCommitError::ToolTransitionConflict)
    }
}

async fn validate_instructions_content(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    content: &ContentRef,
) -> Result<(), SkillDisclosureCommitError> {
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE (tenant_id = $1 OR tenant_id IS NULL)
               AND content_id = $2 AND state = 2
               AND media_type = $3 AND byte_length = $4::bigint AND content_hash = $5
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().as_slice())
    .fetch_one(&mut *transaction)
    .await?;
    if valid {
        Ok(())
    } else {
        Err(SkillDisclosureCommitError::InvalidReference)
    }
}

async fn insert_disclosure(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: RunId,
    record_id: LedgerRecordId,
    command: &CommitSkillDisclosure,
) -> Result<(), SkillDisclosureCommitError> {
    let event = &command.event;
    let changed = sqlx::query(
        "INSERT INTO runtime.skill_disclosures (
             tenant_id, run_id, skill_ref, skill_version_id,
             instructions_content_id, instructions_media_type, instructions_byte_length,
             instructions_hash, ordinal, source_invocation_id, disclosed_at_ms, record_id
         ) VALUES ($1, $2, $3, $4, $5, $6, $7::bigint, $8, $9, $10, $11, $12)",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(event.skill_ref.as_str())
    .bind(command.skill_version_id.into_uuid())
    .bind(event.instructions_ref.content_id.into_uuid())
    .bind(event.instructions_ref.media_type.as_str())
    .bind(event.instructions_ref.byte_length.to_string())
    .bind(event.instructions_hash.into_bytes().as_slice())
    .bind(i32::try_from(event.disclosure_ordinal.get())?)
    .bind(event.source_invocation_id.into_uuid())
    .bind(event.disclosed_at.unix_millis())
    .bind(record_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    if changed.rows_affected() == 1 {
        Ok(())
    } else {
        Err(SkillDisclosureCommitError::TransitionConflict)
    }
}

async fn advance_run_sequence(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: RunId,
    sequence: u64,
) -> Result<(), SkillDisclosureCommitError> {
    let changed = sqlx::query(
        "UPDATE runtime.runs SET last_run_sequence = $3::bigint, run_version = run_version + 1
         WHERE tenant_id = $1 AND run_id = $2 AND status = 2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(sequence.to_string())
    .execute(&mut *transaction)
    .await?;
    if changed.rows_affected() == 1 {
        Ok(())
    } else {
        Err(SkillDisclosureCommitError::TransitionConflict)
    }
}

fn attempt_fence(
    attempt: &AttemptContext,
    home_epoch: musteragent_identity::HomeEpoch,
) -> AttemptFence {
    AttemptFence {
        run_id: attempt.run_id,
        attempt_id: attempt.attempt_id,
        attempt_number: attempt.attempt_number,
        worker_id: attempt.worker_id.clone(),
        lease_id: attempt.lease_id,
        fencing_token: attempt.fencing_token,
        home_epoch,
    }
}

fn deterministic_record_id(
    invocation_id: InvocationId,
    index: usize,
) -> Result<LedgerRecordId, SkillDisclosureCommitError> {
    let mut seed = b"musteragent.discovery-skill.commit.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    seed.extend_from_slice(&u64::try_from(index)?.to_be_bytes());
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    LedgerRecordId::from_uuid(uuid::Uuid::from_bytes(value))
        .map_err(|_| SkillDisclosureCommitError::Corrupt)
}

#[derive(Debug, Error)]
enum SkillDiscoveryError {
    #[error("dialogue_family Skill authority changed or is unavailable")]
    DialogueAuthority(musteragent_runtime_session::DialogueStoreError),
    #[error("Skill discovery Attempt lease is stale")]
    StaleFence,
    #[error("Skill discovery command is invalid")]
    InvalidCommand,
    #[error("Skill discovery state is corrupt")]
    Corrupt,
    #[error("Skill discovery invariant failed: {0}")]
    Invariant(&'static str),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Ledger(#[from] RunLedgerReadError),
    #[error(transparent)]
    Numeric(#[from] std::num::TryFromIntError),
}

impl SkillDiscoveryError {
    const fn diagnostic_check(&self) -> Option<&'static str> {
        match self {
            Self::Invariant(check) => Some(check),
            _ => None,
        }
    }

    fn port(self) -> DiscoveryStoreError {
        match self {
            Self::DialogueAuthority(error) => dialogue_authority_error(error),
            Self::StaleFence => DiscoveryStoreError {
                code: ErrorCode::AttemptLeaseStale,
                retryable: false,
            },
            Self::InvalidCommand => DiscoveryStoreError {
                code: ErrorCode::InvalidCommand,
                retryable: false,
            },
            Self::Database(_) | Self::Ledger(RunLedgerReadError::Database(_)) => {
                DiscoveryStoreError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                }
            }
            Self::Corrupt | Self::Invariant(_) | Self::Ledger(_) | Self::Numeric(_) => {
                DiscoveryStoreError {
                    code: ErrorCode::InternalInvariantViolated,
                    retryable: false,
                }
            }
        }
    }
}

#[derive(Debug, Error)]
enum SkillDisclosureCommitError {
    #[error("dialogue_family Skill authority changed or is unavailable")]
    DialogueAuthority(musteragent_runtime_session::DialogueStoreError),
    #[error(transparent)]
    Liveness(#[from] musteragent_execution_liveness::LivenessJournalError),
    #[error("Skill disclosure command is invalid")]
    InvalidCommand,
    #[error("Skill disclosure transition conflicts with durable state")]
    TransitionConflict,
    #[error("Skill disclosure idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("Skill disclosure ContentRef is invalid")]
    InvalidReference,
    #[error("Skill discovery Tool is not Running under the active Attempt")]
    ToolTransitionConflict,
    #[error("Skill disclosure durable state is corrupt")]
    Corrupt,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error(transparent)]
    Artifact(#[from] PostgresArtifactError),
    #[error(transparent)]
    CommitLane(#[from] CommitLaneError),
    #[error(transparent)]
    Numeric(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    Audit(#[from] crate::tool_security_audit::AtomicToolSecurityAuditError),
}

impl SkillDisclosureCommitError {
    fn port(self) -> DiscoveryStoreError {
        match self {
            Self::DialogueAuthority(error) => dialogue_authority_error(error),
            Self::Liveness(error) => DiscoveryStoreError {
                code: error.code,
                retryable: error.retryable,
            },
            Self::CommitLane(CommitLaneError::StaleFence)
            | Self::Execution(PostgresExecutionError::StaleFence) => DiscoveryStoreError {
                code: ErrorCode::AttemptLeaseStale,
                retryable: false,
            },
            Self::TransitionConflict => DiscoveryStoreError {
                code: ErrorCode::ConversationSeqConflict,
                retryable: true,
            },
            Self::IdempotencyConflict => DiscoveryStoreError {
                code: ErrorCode::IdempotencyConflict,
                retryable: false,
            },
            Self::InvalidCommand | Self::InvalidReference => DiscoveryStoreError {
                code: ErrorCode::InvalidCommand,
                retryable: false,
            },
            Self::Database(_) | Self::Ledger(PostgresLedgerError::Database(_)) | Self::Audit(_) => {
                DiscoveryStoreError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                }
            }
            Self::Ledger(PostgresLedgerError::Contract(_)) => DiscoveryStoreError {
                code: ErrorCode::ConversationSeqConflict,
                retryable: true,
            },
            Self::ToolTransitionConflict => DiscoveryStoreError {
                code: ErrorCode::RunTransitionInvalid,
                retryable: false,
            },
            Self::Artifact(PostgresArtifactError::Sql(_)) => DiscoveryStoreError {
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
            },
            Self::Artifact(_)
            | Self::Corrupt
            | Self::Numeric(_)
            | Self::CommitLane(_)
            | Self::Execution(_)
            | Self::Ledger(_) => DiscoveryStoreError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            },
        }
    }
}

fn dialogue_authority_error(
    error: musteragent_runtime_session::DialogueStoreError,
) -> DiscoveryStoreError {
    use musteragent_runtime_capability::DialogueStoreError;
    match error {
        DialogueStoreError::Infrastructure => DiscoveryStoreError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        },
        DialogueStoreError::Corrupt => DiscoveryStoreError {
            code: ErrorCode::InternalInvariantViolated,
            retryable: false,
        },
        _ => DiscoveryStoreError {
            code: ErrorCode::PolicyDenied,
            retryable: false,
        },
    }
}
