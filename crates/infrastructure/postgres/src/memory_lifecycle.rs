use musteragent_identity::UtcTimestamp;
use musteragent_runtime_memory::{
    AgentMemoryLifecycleDisposition, AgentMemoryLifecycleMutation, AgentMemoryLifecycleStore,
    AgentMemoryRevocationReceipt, RevokeAgentMemory,
};
use uuid::Uuid;

use crate::{PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore};

impl PostgresMemoryEvolutionStore {
    async fn revoke_agent_memory_inner(
        &self,
        command: RevokeAgentMemory,
    ) -> Result<AgentMemoryLifecycleMutation, PostgresMemoryEvolutionError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await?;
        if !crate::memory_management_authority::authorize(
            &mut transaction,
            musteragent_identity::Actor {
                tenant_id: command.tenant_id,
                principal_id: command.actor_principal_id,
                principal_kind: musteragent_identity::PrincipalKind::User,
            },
            command.agent_id,
        )
        .await?
        {
            return Err(PostgresMemoryEvolutionError::MemoryManagementUnauthorized);
        }
        let mut lock_keys = [
            format!(
                "agent-memory-revocation:idempotency:{}",
                command.idempotency_key.as_str()
            ),
            format!("agent-memory-revocation:memory:{}", command.memory_id),
        ];
        lock_keys.sort();
        for lock_key in lock_keys {
            crate::memory::advisory_lock(&mut transaction, command.tenant_id, &lock_key).await?;
        }
        let existing: Option<(String, Uuid, Uuid, Uuid, Uuid, String, i64)> = sqlx::query_as(
            "SELECT idempotency_key,memory_id,memory_version_id,agent_id,revoked_by,
                    reason_code,revoked_at_ms
             FROM memory.agent_memory_revocations
             WHERE tenant_id=$1 AND (idempotency_key=$2 OR memory_id=$3)
             FOR UPDATE",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.idempotency_key.as_str())
        .bind(command.memory_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(existing) = existing {
            transaction.rollback().await?;
            if existing.0 == command.idempotency_key.as_str()
                && existing.1 == command.memory_id.into_uuid()
                && existing.2 == command.expected_memory_version_id.into_uuid()
                && existing.3 == command.agent_id.into_uuid()
                && existing.4 == command.actor_principal_id.into_uuid()
                && existing.5 == command.reason_code.as_str()
            {
                let mut replayed = command.clone();
                replayed.revoked_at = UtcTimestamp::from_unix_millis(existing.6);
                return Ok(mutation(
                    &replayed,
                    AgentMemoryLifecycleDisposition::Duplicate,
                ));
            }
            return Err(PostgresMemoryEvolutionError::MemoryConflict);
        }

        let head: Option<(Uuid, i16)> = sqlx::query_as(
            "SELECT current_version_id,lifecycle
             FROM memory.memories
             WHERE tenant_id=$1 AND memory_id=$2 AND scope_kind=4 AND scope_id=$3
             FOR UPDATE",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.memory_id.into_uuid())
        .bind(command.agent_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some((current_version_id, lifecycle)) = head else {
            return Err(PostgresMemoryEvolutionError::MemoryNotFound);
        };
        if current_version_id != command.expected_memory_version_id.into_uuid() {
            return Err(PostgresMemoryEvolutionError::MemoryVersionConflict);
        }
        if lifecycle != 1 {
            return Err(PostgresMemoryEvolutionError::MemoryConflict);
        }

        let updated = sqlx::query(
            "UPDATE memory.memories
             SET lifecycle=3,revoked_at_ms=$4,updated_at_ms=$4
             WHERE tenant_id=$1 AND memory_id=$2 AND current_version_id=$3 AND lifecycle=1",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.memory_id.into_uuid())
        .bind(command.expected_memory_version_id.into_uuid())
        .bind(command.revoked_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(PostgresMemoryEvolutionError::MemoryConflict);
        }
        sqlx::query(
            "INSERT INTO memory.agent_memory_revocations (
                 tenant_id,idempotency_key,memory_id,memory_version_id,agent_id,
                 revoked_by,reason_code,revoked_at_ms
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.idempotency_key.as_str())
        .bind(command.memory_id.into_uuid())
        .bind(command.expected_memory_version_id.into_uuid())
        .bind(command.agent_id.into_uuid())
        .bind(command.actor_principal_id.into_uuid())
        .bind(command.reason_code.as_str())
        .bind(command.revoked_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(mutation(&command, AgentMemoryLifecycleDisposition::Applied))
    }
}

impl AgentMemoryLifecycleStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn revoke_agent_memory(
        &self,
        command: RevokeAgentMemory,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<AgentMemoryLifecycleMutation, Self::Error>,
    > {
        Box::pin(async move { self.revoke_agent_memory_inner(command).await })
    }
}

fn mutation(
    command: &RevokeAgentMemory,
    disposition: AgentMemoryLifecycleDisposition,
) -> AgentMemoryLifecycleMutation {
    AgentMemoryLifecycleMutation {
        disposition,
        receipt: AgentMemoryRevocationReceipt {
            tenant_id: command.tenant_id,
            actor_principal_id: command.actor_principal_id,
            agent_id: command.agent_id,
            memory_id: command.memory_id,
            memory_version_id: command.expected_memory_version_id,
            reason_code: command.reason_code.clone(),
            idempotency_key: command.idempotency_key.clone(),
            revoked_at: command.revoked_at,
        },
    }
}
