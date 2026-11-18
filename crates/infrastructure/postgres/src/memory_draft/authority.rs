//! The public Cloud Draft composition retains the original Memory authority.
//! The policy is selected by trusted Rust composition, never by wire input.
use super::*;
use musteragent_identity::{Actor, PrincipalKind};

impl PostgresMemoryEvolutionStore {
    #[must_use]
    pub fn for_agent_memory_management(mut self) -> Self {
        self.managed_drafts = true;
        self
    }

    pub async fn authorize_memory_draft_management(
        &self,
        tenant_id: TenantId,
        owner: PrincipalId,
        agent_id: AgentId,
    ) -> Result<bool, PostgresMemoryEvolutionError> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await?;
        let allowed = crate::memory_management_authority::authorize(
            &mut tx,
            Actor {
                tenant_id,
                principal_id: owner,
                principal_kind: PrincipalKind::User,
            },
            agent_id,
        )
        .await?;
        tx.rollback().await?;
        Ok(allowed)
    }

    pub(super) async fn draft_read_transaction(
        &self,
    ) -> Result<crate::GovernedPostgresTransaction<'static>, crate::PostgresTransactionError> {
        // Management reads retain a consistent snapshot while holding authority
        // rows against deactivation; original internal reads remain read-only.
        crate::begin_transaction(
            &self.database,
            crate::PostgresTransactionPolicy::single_attempt(
                &self.database,
                musteragent_telemetry::DbCommitKind::Memory,
                if self.managed_drafts {
                    crate::PostgresTransactionAccess::ReadWrite
                } else {
                    crate::PostgresTransactionAccess::ReadOnly
                },
            )
            .with_isolation(crate::PostgresTransactionIsolation::RepeatableRead),
        )
        .await
    }

    pub(super) async fn authorize_draft_transaction(
        &self,
        tx: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        owner: PrincipalId,
        agent_id: AgentId,
    ) -> Result<(), PostgresMemoryEvolutionError> {
        if self.managed_drafts
            && !crate::memory_management_authority::authorize(
                tx,
                Actor {
                    tenant_id,
                    principal_id: owner,
                    principal_kind: PrincipalKind::User,
                },
                agent_id,
            )
            .await?
        {
            return Err(PostgresMemoryEvolutionError::MemoryManagementUnauthorized);
        }
        Ok(())
    }
}
