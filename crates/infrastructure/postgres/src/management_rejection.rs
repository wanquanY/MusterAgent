use musteragent_identity::{DottedKey, IdempotencyKey, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{
    GetManagementEvolutionRejection, ManagementEvolutionRejectionDisposition,
    ManagementEvolutionRejectionMutation, ManagementEvolutionRejectionReceipt,
    ManagementEvolutionRejectionStore, PersistManagementEvolutionRejection,
};
use sqlx::Row as _;
use uuid::Uuid;

use crate::memory::{
    PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore, advisory_lock, id,
};

impl ManagementEvolutionRejectionStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn get_management_rejection(
        &self,
        query: GetManagementEvolutionRejection,
    ) -> ApplicationFuture<'_, Result<Option<ManagementEvolutionRejectionReceipt>, Self::Error>>
    {
        Box::pin(async move {
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
                .await?;
            let receipt = load_receipt(&mut transaction, &query).await?;
            transaction.rollback().await?;
            Ok(receipt)
        })
    }

    fn reject_management_evolution(
        &self,
        command: PersistManagementEvolutionRejection,
    ) -> ApplicationFuture<'_, Result<ManagementEvolutionRejectionMutation, Self::Error>> {
        Box::pin(async move { reject(self, command).await })
    }
}

async fn reject(
    store: &PostgresMemoryEvolutionStore,
    command: PersistManagementEvolutionRejection,
) -> Result<ManagementEvolutionRejectionMutation, PostgresMemoryEvolutionError> {
    let mut transaction = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Evolution)
        .await?;
    let receipt = command.receipt;
    advisory_lock(
        &mut transaction,
        receipt.tenant_id,
        &format!(
            "evolution-rejection:{}:{}",
            receipt.actor_principal_id,
            receipt.idempotency_key.as_str()
        ),
    )
    .await?;
    let lookup = GetManagementEvolutionRejection {
        tenant_id: receipt.tenant_id,
        actor_principal_id: receipt.actor_principal_id,
        idempotency_key: receipt.idempotency_key.clone(),
    };
    if let Some(existing) = load_receipt(&mut transaction, &lookup).await? {
        transaction.rollback().await?;
        return if existing == receipt {
            Ok(ManagementEvolutionRejectionMutation {
                disposition: ManagementEvolutionRejectionDisposition::Duplicate,
                receipt: existing,
            })
        } else {
            Err(PostgresMemoryEvolutionError::EvolutionConflict)
        };
    }
    let row = sqlx::query(
        "SELECT proposal.tenant_id,proposal.scope_kind,proposal.scope_id,proposal.status,
                evaluation.passed,evaluation.evaluated_at_ms
         FROM evolution.proposals AS proposal
         JOIN evolution.evaluations AS evaluation
           ON evaluation.proposal_id=proposal.proposal_id
          AND evaluation.evaluation_id=$1
         WHERE proposal.proposal_id=$2
         FOR UPDATE OF proposal",
    )
    .bind(receipt.evaluation_id.into_uuid())
    .bind(receipt.proposal_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresMemoryEvolutionError::EvolutionNotFound)?;
    if row.try_get::<Uuid, _>("tenant_id")? != receipt.tenant_id.into_uuid()
        || row.try_get::<i16, _>("scope_kind")? != 2
        || row.try_get::<Option<Uuid>, _>("scope_id")? != Some(receipt.agent_id.into_uuid())
        || row.try_get::<i16, _>("status")? != 3
        || !row.try_get::<bool, _>("passed")?
        || receipt.rejected_at.unix_millis() < row.try_get::<i64, _>("evaluated_at_ms")?
    {
        return Err(PostgresMemoryEvolutionError::ProposalNotVerified);
    }
    let updated = sqlx::query(
        "UPDATE evolution.proposals SET status=4
         WHERE proposal_id=$1 AND tenant_id=$2 AND scope_kind=2 AND scope_id=$3 AND status=3",
    )
    .bind(receipt.proposal_id.into_uuid())
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.agent_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(PostgresMemoryEvolutionError::EvolutionConflict);
    }
    sqlx::query(
        "INSERT INTO approval.evolution_management_rejections(
             tenant_id,target_agent_id,proposal_id,evaluation_id,rejected_by,
             reason_code,idempotency_key,rejected_at_ms
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(receipt.tenant_id.into_uuid())
    .bind(receipt.agent_id.into_uuid())
    .bind(receipt.proposal_id.into_uuid())
    .bind(receipt.evaluation_id.into_uuid())
    .bind(receipt.actor_principal_id.into_uuid())
    .bind(receipt.reason_code.as_str())
    .bind(receipt.idempotency_key.as_str())
    .bind(receipt.rejected_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(ManagementEvolutionRejectionMutation {
        disposition: ManagementEvolutionRejectionDisposition::Applied,
        receipt,
    })
}

async fn load_receipt(
    transaction: &mut sqlx::PgConnection,
    query: &GetManagementEvolutionRejection,
) -> Result<Option<ManagementEvolutionRejectionReceipt>, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT tenant_id,target_agent_id,proposal_id,evaluation_id,rejected_by,
                reason_code,idempotency_key,rejected_at_ms
         FROM approval.evolution_management_rejections
         WHERE tenant_id=$1 AND rejected_by=$2 AND idempotency_key=$3",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.actor_principal_id.into_uuid())
    .bind(query.idempotency_key.as_str())
    .fetch_optional(&mut *transaction)
    .await?;
    row.map(|row| {
        Ok(ManagementEvolutionRejectionReceipt {
            tenant_id: id(row.try_get::<Uuid, _>("tenant_id")?, "rejection.tenant_id")?,
            actor_principal_id: id(
                row.try_get::<Uuid, _>("rejected_by")?,
                "rejection.rejected_by",
            )?,
            agent_id: id(
                row.try_get::<Uuid, _>("target_agent_id")?,
                "rejection.agent_id",
            )?,
            proposal_id: id(
                row.try_get::<Uuid, _>("proposal_id")?,
                "rejection.proposal_id",
            )?,
            evaluation_id: id(
                row.try_get::<Uuid, _>("evaluation_id")?,
                "rejection.evaluation_id",
            )?,
            reason_code: DottedKey::new(row.try_get::<String, _>("reason_code")?)
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            idempotency_key: IdempotencyKey::new(row.try_get::<String, _>("idempotency_key")?)
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            rejected_at: UtcTimestamp::from_unix_millis(row.try_get("rejected_at_ms")?),
        })
    })
    .transpose()
}
