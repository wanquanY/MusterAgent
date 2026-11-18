//! Authenticated management projection, using the original Memory head and
//! privacy-erasure fence. Revoked entries remain visible only to their owner.
use crate::PostgresMemoryEvolutionStore;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, MemoryId, MemoryVersionId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{
    AccountMemoryManagementIdentity, AccountMemoryManagementStore, AgentMemoryManagementIdentity,
    AgentMemoryManagementStore, ListAccountMemories, ListManagedMemories, ManagedMemoryCursor,
    ManagedMemoryHead, ManagedMemoryHeadPage, ManagedMemoryLifecycle,
    MemoryManagementError as Error,
};
use sqlx::Row;

const SELECT_HEAD: &str = "SELECT m.memory_id,m.current_version_id,m.lifecycle,m.updated_at_ms,v.source,
    v.work_item_erasure_job_id IS NOT NULL AS content_erased,c.content_id,c.content_hash,c.media_type,c.byte_length::text AS byte_length
    FROM memory.memories m
    JOIN memory.memory_versions v ON v.memory_id=m.memory_id AND v.memory_version_id=m.current_version_id
    JOIN artifact.content_refs c ON c.content_id=v.content_id AND c.tenant_id=m.tenant_id AND c.content_hash=v.content_hash
    WHERE m.tenant_id=$1 AND m.scope_kind=$3 AND m.scope_id=$2 AND m.lifecycle IN (1,3)
      AND (c.state=2 OR v.work_item_erasure_job_id IS NOT NULL) AND v.valid_from_ms<=(extract(epoch from clock_timestamp())*1000)::bigint
      AND (v.valid_to_ms IS NULL OR v.valid_to_ms>(extract(epoch from clock_timestamp())*1000)::bigint)
      AND NOT EXISTS(SELECT 1 FROM memory.agent_memory_erasure_lineage e WHERE e.tenant_id=m.tenant_id AND e.memory_id=m.memory_id)
      AND NOT EXISTS(SELECT 1 FROM memory.agent_memory_erasure_content_roots e WHERE e.tenant_id=m.tenant_id AND e.content_id=c.content_id)
      AND (v.source<>1 OR EXISTS(SELECT 1 FROM memory.explicit_publications p WHERE p.tenant_id=m.tenant_id AND p.memory_id=m.memory_id AND p.memory_version_id=v.memory_version_id)
        OR EXISTS(SELECT 1 FROM memory.governance_publication_outbox p WHERE p.tenant_id=m.tenant_id AND p.memory_id=m.memory_id AND p.memory_version_id=v.memory_version_id))";

#[derive(Clone, Copy)]
enum Identity {
    Agent(AgentMemoryManagementIdentity),
    Account(AccountMemoryManagementIdentity),
}
impl Identity {
    fn actor(self) -> musteragent_identity::Actor {
        match self {
            Self::Agent(i) => i.actor,
            Self::Account(i) => i.actor,
        }
    }
    fn scope(self) -> (uuid::Uuid, i16) {
        match self {
            Self::Agent(i) => (i.agent_id.into_uuid(), 4),
            Self::Account(i) => (i.actor.principal_id.into_uuid(), 1),
        }
    }
}
async fn authorize(connection: &mut sqlx::PgConnection, identity: Identity) -> Result<(), Error> {
    let allowed = match identity {
        Identity::Agent(i) => {
            crate::memory_management_authority::authorize(connection, i.actor, i.agent_id).await
        }
        Identity::Account(i) => {
            crate::memory_management_authority::authorize_account(connection, i.actor).await
        }
    }
    .map_err(|_| Error::Store)?;
    if allowed {
        Ok(())
    } else {
        Err(Error::Unauthorized)
    }
}
fn decode(row: sqlx::postgres::PgRow) -> Result<ManagedMemoryHead, Error> {
    let get_id = |name| row.try_get::<uuid::Uuid, _>(name).map_err(|_| Error::Store);
    Ok(ManagedMemoryHead {
        source: crate::memory::decode_memory_source(
            row.try_get("source").map_err(|_| Error::Store)?,
        )
        .map_err(|_| Error::Store)?,
        content_erased: row.try_get("content_erased").map_err(|_| Error::Store)?,
        memory_id: MemoryId::from_uuid(get_id("memory_id")?).map_err(|_| Error::Store)?,
        memory_version_id: MemoryVersionId::from_uuid(get_id("current_version_id")?)
            .map_err(|_| Error::Store)?,
        lifecycle: match row
            .try_get::<i16, _>("lifecycle")
            .map_err(|_| Error::Store)?
        {
            1 => ManagedMemoryLifecycle::Active,
            3 => ManagedMemoryLifecycle::Revoked,
            _ => return Err(Error::Store),
        },
        updated_at: UtcTimestamp::from_unix_millis(
            row.try_get("updated_at_ms").map_err(|_| Error::Store)?,
        ),
        content_ref: ContentRef {
            content_id: ContentId::from_uuid(get_id("content_id")?).map_err(|_| Error::Store)?,
            media_type: MediaType::new(
                row.try_get::<String, _>("media_type")
                    .map_err(|_| Error::Store)?,
            )
            .map_err(|_| Error::Store)?,
            sha256: Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("content_hash")
                    .map_err(|_| Error::Store)?,
            )
            .map_err(|_| Error::Store)?,
            byte_length: row
                .try_get::<String, _>("byte_length")
                .map_err(|_| Error::Store)?
                .parse()
                .map_err(|_| Error::Store)?,
        },
    })
}

// Management reads take the original authority FOR SHARE lock inside governed
// units of work. All read ports share the exact publication/content/fence query.
impl PostgresMemoryEvolutionStore {
    async fn managed_heads(
        &self,
        identity: Identity,
        lifecycle: ManagedMemoryLifecycle,
        observed_at: UtcTimestamp,
        after: Option<ManagedMemoryCursor>,
        limit: std::num::NonZeroU16,
    ) -> Result<ManagedMemoryHeadPage, Error> {
        if limit.get() > 50 {
            return Err(Error::InvalidQuery);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await
            .map_err(|_| Error::Store)?;
        authorize(&mut tx, identity).await?;
        let sql = format!("{SELECT_HEAD} AND m.lifecycle=$4 AND m.updated_at_ms<=$5
            AND ($6::bigint IS NULL OR m.updated_at_ms<$6 OR (m.updated_at_ms=$6 AND m.memory_id>$7))
            ORDER BY m.updated_at_ms DESC,m.memory_id LIMIT $8");
        let (scope_id, scope_kind) = identity.scope();
        let rows = sqlx::query(&sql)
            .bind(identity.actor().tenant_id.into_uuid())
            .bind(scope_id)
            .bind(scope_kind)
            .bind(match lifecycle {
                ManagedMemoryLifecycle::Active => 1_i16,
                ManagedMemoryLifecycle::Revoked => 3_i16,
            })
            .bind(observed_at.unix_millis())
            .bind(after.map(|c| c.updated_at.unix_millis()))
            .bind(after.map(|c| c.memory_id.into_uuid()))
            .bind(i64::from(limit.get()) + 1)
            .fetch_all(&mut *tx)
            .await
            .map_err(|_| Error::Store)?;
        let mut heads = rows
            .into_iter()
            .map(decode)
            .collect::<Result<Vec<_>, _>>()?;
        let more = heads.len() > usize::from(limit.get());
        heads.truncate(usize::from(limit.get()));
        let next_cursor = more.then(|| {
            let h = heads.last().expect("nonzero page limit");
            ManagedMemoryCursor {
                updated_at: h.updated_at,
                memory_id: h.memory_id,
            }
        });
        tx.commit().await.map_err(|_| Error::Store)?;
        Ok(ManagedMemoryHeadPage { heads, next_cursor })
    }
    async fn managed_head(
        &self,
        identity: Identity,
        memory_id: MemoryId,
        expected: MemoryVersionId,
    ) -> Result<ManagedMemoryHead, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await
            .map_err(|_| Error::Store)?;
        authorize(&mut tx, identity).await?;
        let (scope_id, scope_kind) = identity.scope();
        let row = sqlx::query(&format!("{SELECT_HEAD} AND m.memory_id=$4"))
            .bind(identity.actor().tenant_id.into_uuid())
            .bind(scope_id)
            .bind(scope_kind)
            .bind(memory_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| Error::Store)?
            .ok_or(Error::NotFound)?;
        let head = decode(row)?;
        if head.memory_version_id != expected {
            return Err(Error::Conflict);
        }
        tx.commit().await.map_err(|_| Error::Store)?;
        Ok(head)
    }
    async fn validate_heads(
        &self,
        identity: Identity,
        heads: Vec<ManagedMemoryHead>,
    ) -> Result<Vec<bool>, Error> {
        if heads.len() > 50 {
            return Err(Error::InvalidQuery);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await
            .map_err(|_| Error::Store)?;
        authorize(&mut tx, identity).await?;
        let (scope_id, scope_kind) = identity.scope();
        let rows = sqlx::query(&format!("{SELECT_HEAD} AND m.memory_id=ANY($4)"))
            .bind(identity.actor().tenant_id.into_uuid())
            .bind(scope_id)
            .bind(scope_kind)
            .bind(
                heads
                    .iter()
                    .map(|h| h.memory_id.into_uuid())
                    .collect::<Vec<_>>(),
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(|_| Error::Store)?;
        let current = rows
            .into_iter()
            .map(decode)
            .collect::<Result<Vec<_>, _>>()?;
        let valid = heads.iter().map(|h| current.contains(h)).collect();
        tx.commit().await.map_err(|_| Error::Store)?;
        Ok(valid)
    }
}
impl AgentMemoryManagementStore for PostgresMemoryEvolutionStore {
    fn list_managed_memories(
        &self,
        q: ListManagedMemories,
    ) -> ApplicationFuture<'_, Result<ManagedMemoryHeadPage, Error>> {
        Box::pin(self.managed_heads(
            Identity::Agent(q.identity),
            q.lifecycle,
            q.observed_at,
            q.after,
            q.limit,
        ))
    }
    fn read_managed_memory(
        &self,
        identity: AgentMemoryManagementIdentity,
        memory_id: MemoryId,
        expected: MemoryVersionId,
    ) -> ApplicationFuture<'_, Result<ManagedMemoryHead, Error>> {
        Box::pin(self.managed_head(Identity::Agent(identity), memory_id, expected))
    }
    fn validate_managed_memory_heads(
        &self,
        identity: AgentMemoryManagementIdentity,
        heads: Vec<ManagedMemoryHead>,
    ) -> ApplicationFuture<'_, Result<Vec<bool>, Error>> {
        Box::pin(self.validate_heads(Identity::Agent(identity), heads))
    }
}
impl AccountMemoryManagementStore for PostgresMemoryEvolutionStore {
    fn list_account_memories(
        &self,
        q: ListAccountMemories,
    ) -> ApplicationFuture<'_, Result<ManagedMemoryHeadPage, Error>> {
        Box::pin(self.managed_heads(
            Identity::Account(q.identity),
            q.lifecycle,
            q.observed_at,
            q.after,
            q.limit,
        ))
    }
    fn read_account_memory(
        &self,
        identity: AccountMemoryManagementIdentity,
        memory_id: MemoryId,
        expected: MemoryVersionId,
    ) -> ApplicationFuture<'_, Result<ManagedMemoryHead, Error>> {
        Box::pin(self.managed_head(Identity::Account(identity), memory_id, expected))
    }
    fn validate_account_memory_heads(
        &self,
        identity: AccountMemoryManagementIdentity,
        heads: Vec<ManagedMemoryHead>,
    ) -> ApplicationFuture<'_, Result<Vec<bool>, Error>> {
        Box::pin(self.validate_heads(Identity::Account(identity), heads))
    }
}
