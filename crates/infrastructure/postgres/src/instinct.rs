use std::collections::{BTreeMap, BTreeSet};

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{
    CandidateStatus, Instinct, InstinctLifecycle, InstinctMutationDisposition,
    InstinctMutationReceipt, PublishInstinct, SetInstinctLifecycle,
};
use musteragent_identity::{
    DottedKey, IdempotencyKey, InstinctId, LedgerRecordId, RunId, UtcTimestamp, VersionNumber,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{
    InstinctMutation, InstinctStore, ListAgentInstincts, MemoryPersistenceDisposition,
};
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::memory::{
    FromDatabaseUuid, approval_matches, digest, ensure_content_ref, evaluation_matches, id,
    proposal_matches,
};
use crate::{PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore};

const INSTINCT_TARGET_KIND: i16 = 1;

impl InstinctStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn publish_instinct(
        &self,
        command: PublishInstinct,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, Self::Error>> {
        Box::pin(async move { self.publish_instinct_inner(command).await })
    }

    fn set_instinct_lifecycle(
        &self,
        command: SetInstinctLifecycle,
    ) -> ApplicationFuture<'_, Result<InstinctMutation, Self::Error>> {
        Box::pin(async move { self.set_instinct_lifecycle_inner(command).await })
    }

    fn list_agent_instincts(
        &self,
        query: ListAgentInstincts,
    ) -> ApplicationFuture<'_, Result<Vec<Instinct>, Self::Error>> {
        Box::pin(async move { self.list_agent_instincts_inner(query).await })
    }
}

impl PostgresMemoryEvolutionStore {
    async fn publish_instinct_inner(
        &self,
        command: PublishInstinct,
    ) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
        command.validate()?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
            .await?;
        let disposition = publish_instinct_transaction(&mut transaction, &command).await?;
        if disposition == MemoryPersistenceDisposition::Applied {
            transaction.commit().await?;
        } else {
            transaction.rollback().await?;
        }
        Ok(disposition)
    }

    async fn set_instinct_lifecycle_inner(
        &self,
        command: SetInstinctLifecycle,
    ) -> Result<InstinctMutation, PostgresMemoryEvolutionError> {
        command.validate()?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
            .await?;
        if let Some(receipt) = load_receipt_by_idempotency(&mut transaction, &command).await? {
            transaction.rollback().await?;
            return Ok(InstinctMutation {
                disposition: InstinctMutationDisposition::Duplicate,
                receipt,
            });
        }
        let row = sqlx::query(
            "SELECT revision,lifecycle,updated_at_ms FROM evolution.instincts
             WHERE tenant_id=$1 AND target_agent_id=$2 AND instinct_id=$3 FOR UPDATE",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.target_agent_id.into_uuid())
        .bind(command.instinct_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresMemoryEvolutionError::MemoryNotFound)?;
        let previous_revision = version(row.try_get("revision")?)?;
        if previous_revision != command.expected_revision {
            return Err(PostgresMemoryEvolutionError::EvolutionConflict);
        }
        let previous_lifecycle = decode_lifecycle(row.try_get("lifecycle")?)?;
        if previous_lifecycle == command.lifecycle {
            return Err(PostgresMemoryEvolutionError::EvolutionConflict);
        }
        let previous_updated_at = UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?);
        if command.changed_at < previous_updated_at {
            return Err(PostgresMemoryEvolutionError::EvolutionConflict);
        }
        let revision = VersionNumber::new(
            previous_revision
                .get()
                .checked_add(1)
                .ok_or(PostgresMemoryEvolutionError::NumericRange)?,
        )
        .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?;
        let receipt = InstinctMutationReceipt {
            tenant_id: command.tenant_id,
            target_agent_id: command.target_agent_id,
            instinct_id: command.instinct_id,
            previous_revision,
            revision,
            lifecycle: command.lifecycle,
            actor_principal_id: command.actor_principal_id,
            reason_code: command.reason_code.clone(),
            idempotency_key: command.idempotency_key.clone(),
            changed_at: command.changed_at,
        };
        receipt.validate()?;
        let receipt_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!(
                "instinct-lifecycle:{}:{}:{}",
                command.tenant_id,
                command.actor_principal_id,
                command.idempotency_key.as_str()
            )
            .as_bytes(),
        );
        sqlx::query(
            "INSERT INTO evolution.instinct_lifecycle_receipts (
                 receipt_id,tenant_id,target_agent_id,instinct_id,previous_revision,
                 revision,lifecycle,actor_principal_id,reason_code,idempotency_key,changed_at_ms
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(receipt_id)
        .bind(receipt.tenant_id.into_uuid())
        .bind(receipt.target_agent_id.into_uuid())
        .bind(receipt.instinct_id.into_uuid())
        .bind(revision_i64(receipt.previous_revision)?)
        .bind(revision_i64(receipt.revision)?)
        .bind(lifecycle_code(receipt.lifecycle))
        .bind(receipt.actor_principal_id.into_uuid())
        .bind(receipt.reason_code.as_str())
        .bind(receipt.idempotency_key.as_str())
        .bind(receipt.changed_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        let updated = sqlx::query(
            "UPDATE evolution.instincts SET lifecycle=$1,revision=$2,updated_at_ms=$3
             WHERE tenant_id=$4 AND target_agent_id=$5 AND instinct_id=$6 AND revision=$7",
        )
        .bind(lifecycle_code(receipt.lifecycle))
        .bind(revision_i64(receipt.revision)?)
        .bind(receipt.changed_at.unix_millis())
        .bind(receipt.tenant_id.into_uuid())
        .bind(receipt.target_agent_id.into_uuid())
        .bind(receipt.instinct_id.into_uuid())
        .bind(revision_i64(receipt.previous_revision)?)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(PostgresMemoryEvolutionError::EvolutionConflict);
        }
        transaction.commit().await?;
        Ok(InstinctMutation {
            disposition: InstinctMutationDisposition::Applied,
            receipt,
        })
    }

    async fn list_agent_instincts_inner(
        &self,
        query: ListAgentInstincts,
    ) -> Result<Vec<Instinct>, PostgresMemoryEvolutionError> {
        query
            .validate()
            .map_err(|_| PostgresMemoryEvolutionError::EvolutionConflict)?;
        let rows = sqlx::query(
            "SELECT instinct.instinct_id,instinct.tenant_id,instinct.target_agent_id,
                    instinct.proposal_id,instinct.evaluation_id,
                    instinct.approval_interaction_id,instinct.promotion_id,
                    content.content_id AS payload_content_id,content.media_type,
                    content.byte_length::text AS byte_length,content.content_hash,
                    instinct.lifecycle,instinct.revision,instinct.created_by,
                    instinct.created_at_ms,instinct.updated_at_ms
             FROM evolution.instincts AS instinct
             JOIN artifact.content_refs AS content
               ON content.content_id=instinct.payload_content_id
              AND content.tenant_id=instinct.tenant_id
              AND content.content_hash=instinct.payload_content_hash
             WHERE instinct.tenant_id=$1 AND instinct.target_agent_id=$2
               AND ($3 OR instinct.lifecycle=1)
               AND instinct.created_at_ms <= $4
               AND ($6::uuid[] IS NULL OR instinct.instinct_id=ANY($6))
             ORDER BY instinct.updated_at_ms DESC,instinct.instinct_id
             LIMIT $5",
        )
        .bind(query.tenant_id.into_uuid())
        .bind(query.target_agent_id.into_uuid())
        .bind(query.include_disabled)
        .bind(query.observed_at.unix_millis())
        .bind(i64::from(query.limit.get()))
        .bind(query.instinct_ids.map(|ids| {
            ids.into_iter()
                .map(InstinctId::into_uuid)
                .collect::<Vec<_>>()
        }))
        .fetch_all(&self.pool)
        .await?;
        decode_instinct_rows(&self.pool, rows).await
    }
}

pub(crate) async fn publish_instinct_transaction(
    transaction: &mut sqlx::PgConnection,
    command: &PublishInstinct,
) -> Result<MemoryPersistenceDisposition, PostgresMemoryEvolutionError> {
    command.validate()?;
    let status: Option<i16> = sqlx::query_scalar(
        "SELECT status FROM evolution.proposals
             WHERE tenant_id=$1 AND proposal_id=$2 FOR UPDATE",
    )
    .bind(command.candidate.tenant_id.into_uuid())
    .bind(command.candidate.proposal_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    if status == Some(5) {
        let exact = instinct_publication_matches(transaction, command).await?;
        return if exact {
            Ok(MemoryPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresMemoryEvolutionError::EvolutionConflict)
        };
    }
    if status != Some(3) {
        return Err(PostgresMemoryEvolutionError::ProposalNotVerified);
    }
    let mut verified = command.candidate.clone();
    verified.status = CandidateStatus::Verified;
    if !proposal_matches(transaction, &verified).await?
        || !evaluation_matches(transaction, &command.evaluation).await?
        || !approval_matches(transaction, &command.approval).await?
    {
        return Err(PostgresMemoryEvolutionError::ApprovalNotSatisfied);
    }
    ensure_content_ref(
        transaction,
        command.instinct.tenant_id,
        &command.instinct.payload_ref,
    )
    .await?;
    sqlx::query(
        "INSERT INTO evolution.artifact_promotions (
                 promotion_id,tenant_id,proposal_id,evaluation_id,
                 approval_interaction_id,target_kind,target_id,promoted_by,promoted_at_ms
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(command.instinct.promotion_id.into_uuid())
    .bind(command.instinct.tenant_id.into_uuid())
    .bind(command.instinct.proposal_id.into_uuid())
    .bind(command.instinct.evaluation_id.into_uuid())
    .bind(command.instinct.approval_interaction_id.into_uuid())
    .bind(INSTINCT_TARGET_KIND)
    .bind(command.instinct.instinct_id.into_uuid())
    .bind(command.instinct.created_by.into_uuid())
    .bind(command.instinct.created_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO evolution.instincts (
                 instinct_id,tenant_id,target_agent_id,proposal_id,evaluation_id,
                 approval_interaction_id,promotion_id,payload_content_id,
                 payload_content_hash,lifecycle,revision,created_by,created_at_ms,updated_at_ms
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
    )
    .bind(command.instinct.instinct_id.into_uuid())
    .bind(command.instinct.tenant_id.into_uuid())
    .bind(command.instinct.target_agent_id.into_uuid())
    .bind(command.instinct.proposal_id.into_uuid())
    .bind(command.instinct.evaluation_id.into_uuid())
    .bind(command.instinct.approval_interaction_id.into_uuid())
    .bind(command.instinct.promotion_id.into_uuid())
    .bind(command.instinct.payload_ref.content_id.into_uuid())
    .bind(command.instinct.payload_ref.sha256.into_bytes().as_slice())
    .bind(lifecycle_code(command.instinct.lifecycle))
    .bind(revision_i64(command.instinct.revision)?)
    .bind(command.instinct.created_by.into_uuid())
    .bind(command.instinct.created_at.unix_millis())
    .bind(command.instinct.updated_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    for run_id in &command.instinct.evidence_run_ids {
        sqlx::query(
            "INSERT INTO evolution.instinct_evidence_runs(instinct_id,run_id)
                 VALUES ($1,$2)",
        )
        .bind(command.instinct.instinct_id.into_uuid())
        .bind(run_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    for record_id in &command.instinct.evidence_record_ids {
        sqlx::query(
            "INSERT INTO evolution.instinct_evidence_records(instinct_id,ledger_record_id)
                 VALUES ($1,$2)",
        )
        .bind(command.instinct.instinct_id.into_uuid())
        .bind(record_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    let updated = sqlx::query(
        "UPDATE evolution.proposals SET status=5
             WHERE proposal_id=$1 AND status=3",
    )
    .bind(command.instinct.proposal_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }
    insert_instinct_publication_outbox(transaction, command).await?;
    Ok(MemoryPersistenceDisposition::Applied)
}

async fn insert_instinct_publication_outbox(
    transaction: &mut sqlx::PgConnection,
    command: &PublishInstinct,
) -> Result<(), PostgresMemoryEvolutionError> {
    let source_run_id = command
        .candidate
        .source_run_ids
        .iter()
        .next()
        .ok_or(PostgresMemoryEvolutionError::EvolutionConflict)?;
    let conversation_id: Uuid = sqlx::query_scalar(
        "SELECT conversation_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(command.candidate.tenant_id.into_uuid())
    .bind(source_run_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema":"musteragent.legacy.evolution.instinct-published.v1",
        "proposal_id":command.instinct.proposal_id.to_string(),
        "evaluation_id":command.instinct.evaluation_id.to_string(),
        "approval_interaction_id":command.instinct.approval_interaction_id.to_string(),
        "instinct_id":command.instinct.instinct_id.to_string(),
        "target_agent_id":command.instinct.target_agent_id.to_string(),
        "revision":command.instinct.revision.get(),
    }))?;
    let message_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("instinct-published:{}", command.instinct.promotion_id).as_bytes(),
    );
    sqlx::query(
        "INSERT INTO event_store.outbox (
             message_id,tenant_id,conversation_id,topic,payload,payload_hash,
             committed_at_ms,available_at_ms
         ) VALUES ($1,$2,$3,'evolution.instinct-published.v1',$4,$5,$6,$6)
         ON CONFLICT(message_id) DO NOTHING",
    )
    .bind(message_id)
    .bind(command.instinct.tenant_id.into_uuid())
    .bind(conversation_id)
    .bind(&payload)
    .bind(Sha256::digest(&payload).into_bytes().as_slice())
    .bind(command.instinct.created_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn instinct_publication_matches(
    transaction: &mut sqlx::PgConnection,
    command: &PublishInstinct,
) -> Result<bool, PostgresMemoryEvolutionError> {
    if !proposal_matches(transaction, &command.candidate).await?
        || !evaluation_matches(transaction, &command.evaluation).await?
        || !approval_matches(transaction, &command.approval).await?
    {
        return Ok(false);
    }
    let row = sqlx::query(
        "SELECT instinct.tenant_id,instinct.target_agent_id,instinct.proposal_id,
                instinct.evaluation_id,instinct.approval_interaction_id,
                instinct.promotion_id,instinct.payload_content_id,
                instinct.payload_content_hash,instinct.lifecycle,instinct.revision,
                instinct.created_by,instinct.created_at_ms,instinct.updated_at_ms,
                promotion.target_kind,promotion.target_id,promotion.promoted_by,
                promotion.promoted_at_ms
         FROM evolution.instincts AS instinct
         JOIN evolution.artifact_promotions AS promotion
           ON promotion.promotion_id=instinct.promotion_id
         WHERE instinct.instinct_id=$1",
    )
    .bind(command.instinct.instinct_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else { return Ok(false) };
    let exact = row.try_get::<Uuid, _>("tenant_id")? == command.instinct.tenant_id.into_uuid()
        && row.try_get::<Uuid, _>("target_agent_id")?
            == command.instinct.target_agent_id.into_uuid()
        && row.try_get::<Uuid, _>("proposal_id")? == command.instinct.proposal_id.into_uuid()
        && row.try_get::<Uuid, _>("evaluation_id")? == command.instinct.evaluation_id.into_uuid()
        && row.try_get::<Uuid, _>("approval_interaction_id")?
            == command.instinct.approval_interaction_id.into_uuid()
        && row.try_get::<Uuid, _>("promotion_id")? == command.instinct.promotion_id.into_uuid()
        && row.try_get::<Uuid, _>("payload_content_id")?
            == command.instinct.payload_ref.content_id.into_uuid()
        && digest(row.try_get("payload_content_hash")?)? == command.instinct.payload_ref.sha256
        && row.try_get::<i16, _>("lifecycle")? == lifecycle_code(command.instinct.lifecycle)
        && version(row.try_get("revision")?)? == command.instinct.revision
        && row.try_get::<Uuid, _>("created_by")? == command.instinct.created_by.into_uuid()
        && row.try_get::<i64, _>("created_at_ms")? == command.instinct.created_at.unix_millis()
        && row.try_get::<i64, _>("updated_at_ms")? == command.instinct.updated_at.unix_millis()
        && row.try_get::<i16, _>("target_kind")? == INSTINCT_TARGET_KIND
        && row.try_get::<Uuid, _>("target_id")? == command.instinct.instinct_id.into_uuid()
        && row.try_get::<Uuid, _>("promoted_by")? == command.instinct.created_by.into_uuid()
        && row.try_get::<i64, _>("promoted_at_ms")? == command.instinct.created_at.unix_millis();
    if !exact {
        return Ok(false);
    }
    let runs = load_evidence_ids::<RunId>(
        transaction,
        "SELECT run_id FROM evolution.instinct_evidence_runs
         WHERE instinct_id=$1 ORDER BY run_id",
        command.instinct.instinct_id,
        "instinct.run_id",
    )
    .await?;
    let records = load_evidence_ids::<LedgerRecordId>(
        transaction,
        "SELECT ledger_record_id FROM evolution.instinct_evidence_records
         WHERE instinct_id=$1 ORDER BY ledger_record_id",
        command.instinct.instinct_id,
        "instinct.record_id",
    )
    .await?;
    Ok(
        runs == command.instinct.evidence_run_ids
            && records == command.instinct.evidence_record_ids,
    )
}

async fn load_evidence_ids<T>(
    transaction: &mut sqlx::PgConnection,
    sql: &str,
    instinct_id: InstinctId,
    field: &'static str,
) -> Result<BTreeSet<T>, PostgresMemoryEvolutionError>
where
    T: Ord + FromDatabaseUuid,
{
    sqlx::query_scalar::<_, Uuid>(sql)
        .bind(instinct_id.into_uuid())
        .fetch_all(&mut *transaction)
        .await?
        .into_iter()
        .map(|value| id(value, field))
        .collect()
}

async fn load_receipt_by_idempotency(
    transaction: &mut sqlx::PgConnection,
    command: &SetInstinctLifecycle,
) -> Result<Option<InstinctMutationReceipt>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT tenant_id,target_agent_id,instinct_id,previous_revision,revision,
                lifecycle,actor_principal_id,reason_code,idempotency_key,changed_at_ms
         FROM evolution.instinct_lifecycle_receipts
         WHERE tenant_id=$1 AND actor_principal_id=$2 AND idempotency_key=$3",
    )
    .bind(command.tenant_id.into_uuid())
    .bind(command.actor_principal_id.into_uuid())
    .bind(command.idempotency_key.as_str())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let receipt = decode_receipt(&row)?;
    if receipt.tenant_id != command.tenant_id
        || receipt.target_agent_id != command.target_agent_id
        || receipt.instinct_id != command.instinct_id
        || receipt.previous_revision != command.expected_revision
        || receipt.lifecycle != command.lifecycle
        || receipt.actor_principal_id != command.actor_principal_id
        || receipt.reason_code != command.reason_code
        || receipt.idempotency_key != command.idempotency_key
        || receipt.changed_at != command.changed_at
    {
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }
    Ok(Some(receipt))
}

fn decode_receipt(row: &PgRow) -> Result<InstinctMutationReceipt, PostgresMemoryEvolutionError> {
    let receipt = InstinctMutationReceipt {
        tenant_id: id(row.try_get("tenant_id")?, "instinct.receipt.tenant_id")?,
        target_agent_id: id(
            row.try_get("target_agent_id")?,
            "instinct.receipt.target_agent_id",
        )?,
        instinct_id: id(row.try_get("instinct_id")?, "instinct.receipt.instinct_id")?,
        previous_revision: version(row.try_get("previous_revision")?)?,
        revision: version(row.try_get("revision")?)?,
        lifecycle: decode_lifecycle(row.try_get("lifecycle")?)?,
        actor_principal_id: id(
            row.try_get("actor_principal_id")?,
            "instinct.receipt.actor_principal_id",
        )?,
        reason_code: DottedKey::new(row.try_get::<String, _>("reason_code")?)
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        idempotency_key: IdempotencyKey::new(row.try_get::<String, _>("idempotency_key")?)
            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
        changed_at: UtcTimestamp::from_unix_millis(row.try_get("changed_at_ms")?),
    };
    receipt.validate()?;
    Ok(receipt)
}

async fn decode_instinct_rows(
    pool: &sqlx::PgPool,
    rows: Vec<PgRow>,
) -> Result<Vec<Instinct>, PostgresMemoryEvolutionError> {
    let mut instincts = Vec::with_capacity(rows.len());
    for row in rows {
        instincts.push(Instinct {
            instinct_id: id(row.try_get("instinct_id")?, "instinct.instinct_id")?,
            tenant_id: id(row.try_get("tenant_id")?, "instinct.tenant_id")?,
            target_agent_id: id(row.try_get("target_agent_id")?, "instinct.agent_id")?,
            proposal_id: id(row.try_get("proposal_id")?, "instinct.proposal_id")?,
            evaluation_id: id(row.try_get("evaluation_id")?, "instinct.evaluation_id")?,
            approval_interaction_id: id(
                row.try_get("approval_interaction_id")?,
                "instinct.approval_id",
            )?,
            promotion_id: id(row.try_get("promotion_id")?, "instinct.promotion_id")?,
            payload_ref: ContentRef {
                content_id: id(
                    row.try_get("payload_content_id")?,
                    "instinct.payload_content_id",
                )?,
                media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                byte_length: row
                    .try_get::<String, _>("byte_length")?
                    .parse()
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                sha256: digest(row.try_get("content_hash")?)?,
            },
            evidence_run_ids: BTreeSet::new(),
            evidence_record_ids: BTreeSet::new(),
            lifecycle: decode_lifecycle(row.try_get("lifecycle")?)?,
            revision: version(row.try_get("revision")?)?,
            created_by: id(row.try_get("created_by")?, "instinct.created_by")?,
            created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
            updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?),
        });
    }
    if instincts.is_empty() {
        return Ok(instincts);
    }
    let ids = instincts
        .iter()
        .map(|value| value.instinct_id.into_uuid())
        .collect::<Vec<_>>();
    let run_rows = sqlx::query(
        "SELECT instinct_id,run_id FROM evolution.instinct_evidence_runs
         WHERE instinct_id=ANY($1) ORDER BY instinct_id,run_id",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    let record_rows = sqlx::query(
        "SELECT instinct_id,ledger_record_id FROM evolution.instinct_evidence_records
         WHERE instinct_id=ANY($1) ORDER BY instinct_id,ledger_record_id",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    let mut runs = BTreeMap::<Uuid, BTreeSet<RunId>>::new();
    for row in run_rows {
        runs.entry(row.try_get("instinct_id")?)
            .or_default()
            .insert(id(row.try_get("run_id")?, "instinct.run_id")?);
    }
    let mut records = BTreeMap::<Uuid, BTreeSet<LedgerRecordId>>::new();
    for row in record_rows {
        records
            .entry(row.try_get("instinct_id")?)
            .or_default()
            .insert(id(row.try_get("ledger_record_id")?, "instinct.record_id")?);
    }
    for instinct in &mut instincts {
        instinct.evidence_run_ids = runs
            .remove(&instinct.instinct_id.into_uuid())
            .unwrap_or_default();
        instinct.evidence_record_ids = records
            .remove(&instinct.instinct_id.into_uuid())
            .unwrap_or_default();
        instinct.validate()?;
    }
    Ok(instincts)
}

const fn lifecycle_code(value: InstinctLifecycle) -> i16 {
    match value {
        InstinctLifecycle::Enabled => 1,
        InstinctLifecycle::Disabled => 2,
    }
}

fn decode_lifecycle(value: i16) -> Result<InstinctLifecycle, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(InstinctLifecycle::Enabled),
        2 => Ok(InstinctLifecycle::Disabled),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

fn revision_i64(value: VersionNumber) -> Result<i64, PostgresMemoryEvolutionError> {
    i64::try_from(value.get()).map_err(|_| PostgresMemoryEvolutionError::NumericRange)
}

fn version(value: i64) -> Result<VersionNumber, PostgresMemoryEvolutionError> {
    let value = u64::try_from(value).map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
    VersionNumber::new(value).map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)
}
