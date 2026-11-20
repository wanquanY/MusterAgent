//! Agent memory revocation authority against a disposable PostgreSQL instance.
//! The built-in `main` Agent is a shared definition, not an owned Agent row; the
//! tenant's sole owner must still be able to revoke its memories, and with
//! co-owners in the tenant no owner may manage another's main-agent memories.
use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::MediaType;
use musteragent_identity::*;
use musteragent_postgres::{
    PostgresContentStore, PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore,
    PostgresPrivacyErasureError, PostgresPrivacyErasureStore,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_memory::*;
use musteragent_runtime_privacy::{AgentMemoryErasureAdmissionStore, RequestAgentMemoryErasure};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use std::sync::Arc;

struct Fixture {
    _postgres: IsolatedPostgres,
    pool: sqlx::PgPool,
    content: PostgresContentStore,
    tenant_id: TenantId,
    owner: PrincipalId,
}

impl Fixture {
    async fn new() -> Self {
        let postgres = IsolatedPostgres::start_with_pgvector()
            .await
            .expect("isolated Postgres");
        let pool = postgres.pool().await.expect("isolated pool");
        musteragent_conformance::migrate(&pool)
            .await
            .expect("original migrations");
        let key = DottedKey::new("test.memory.revocation").expect("key id");
        let content = PostgresContentStore::new(
            pool.clone(),
            Arc::new(MemoryBlobs::in_memory()),
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [53; 32])]).expect("fixture keys")),
        );
        let fixture = Self {
            _postgres: postgres,
            pool,
            content,
            tenant_id: TenantId::generate(),
            owner: PrincipalId::generate(),
        };
        sqlx::query(
            "INSERT INTO agent.owner_spaces (space_id,tenant_id,owner_id,product,revision,created_at_ms)
             VALUES ($1,$2,$3,'musteragent',1,1)",
        )
        .bind(TenantId::generate().into_uuid())
        .bind(fixture.tenant_id.into_uuid())
        .bind(fixture.owner.into_uuid())
        .execute(&fixture.pool)
        .await
        .expect("owner space");
        fixture
    }

    /// Another owner space in the same tenant (tenants may have several owners).
    async fn add_owner(&self) -> PrincipalId {
        let owner = PrincipalId::generate();
        sqlx::query(
            "INSERT INTO agent.owner_spaces (space_id,tenant_id,owner_id,product,revision,created_at_ms)
             VALUES ($1,$2,$3,'musteragent',1,1)",
        )
        .bind(TenantId::generate().into_uuid())
        .bind(self.tenant_id.into_uuid())
        .bind(owner.into_uuid())
        .execute(&self.pool)
        .await
        .expect("co-owner space");
        owner
    }

    fn actor(&self, principal_id: PrincipalId) -> Actor {
        Actor {
            tenant_id: self.tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        }
    }

    async fn list(
        &self,
        principal_id: PrincipalId,
        agent_id: AgentId,
    ) -> Result<ManagedMemoryHeadPage, MemoryManagementError> {
        self.store()
            .list_managed_memories(ListManagedMemories {
                identity: AgentMemoryManagementIdentity {
                    actor: self.actor(principal_id),
                    agent_id,
                },
                lifecycle: ManagedMemoryLifecycle::Active,
                observed_at: UtcTimestamp::from_unix_millis(i64::MAX / 2),
                after: None,
                limit: std::num::NonZeroU16::new(10).expect("limit"),
            })
            .await
    }

    async fn erase(
        &self,
        principal_id: PrincipalId,
        agent_id: AgentId,
        memory: (MemoryId, MemoryVersionId),
        key: &str,
    ) -> Result<(), PostgresPrivacyErasureError> {
        PostgresPrivacyErasureStore::new(self.pool.clone())
            .request_agent_memory_erasure(RequestAgentMemoryErasure {
                tenant_id: self.tenant_id,
                requested_by: self.actor(principal_id),
                agent_id,
                memory_id: memory.0,
                expected_memory_version_id: memory.1,
                reason_code: DottedKey::new("user.erased").expect("reason"),
                idempotency_key: IdempotencyKey::new(key).expect("key"),
                requested_at: UtcTimestamp::from_unix_millis(500),
                complete_by: UtcTimestamp::from_unix_millis(10_000),
            })
            .await
            .map(|_| ())
    }

    /// The migrations publish the one built-in Agent in the `main` slot.
    async fn main_agent(&self) -> AgentId {
        let agent_id: uuid::Uuid = sqlx::query_scalar(
            "SELECT agent_id FROM agent.agent_definitions
             WHERE built_in AND system_slot='main' AND lifecycle=1",
        )
        .fetch_one(&self.pool)
        .await
        .expect("built-in main agent");
        AgentId::from_uuid(agent_id).expect("agent id")
    }

    /// A shared custom definition that no owner space owns.
    async fn unowned_agent(&self) -> AgentId {
        let agent_id = AgentId::generate();
        sqlx::query(
            "INSERT INTO agent.agent_definitions (agent_id,agent_key,built_in,lifecycle,created_at_ms,
                 updated_at_ms,system_slot,display_name,agent_kind,deletable,replaceable,default_for_product)
             VALUES ($1,'fixture.unowned',false,1,1,1,NULL,'Unowned',2,true,true,false)",
        )
        .bind(agent_id.into_uuid())
        .execute(&self.pool)
        .await
        .expect("unowned agent definition");
        agent_id
    }

    /// One active agent-scoped memory head with its first version.
    async fn memory(&self, agent_id: AgentId) -> (MemoryId, MemoryVersionId) {
        let memory_id = MemoryId::generate();
        let version_id = MemoryVersionId::generate();
        let reference = self
            .content
            .put(PutContent {
                tenant_id: self.tenant_id,
                media_type: MediaType::new("text/plain").expect("media"),
                bytes: b"Prefers concise answers".to_vec(),
            })
            .await
            .expect("real content owner");
        let mut tx = self.pool.begin().await.expect("fixture transaction");
        sqlx::query("INSERT INTO memory.memories (memory_id,tenant_id,scope_kind,scope_id,memory_key,lifecycle,created_at_ms,updated_at_ms,kind,current_version_id,revoked_at_ms) VALUES ($1,$2,4,$3,'revocation:memory',1,100,100,2,$4,NULL)")
            .bind(memory_id.into_uuid()).bind(self.tenant_id.into_uuid())
            .bind(agent_id.into_uuid()).bind(version_id.into_uuid())
            .execute(&mut *tx).await.expect("fixture head");
        sqlx::query("INSERT INTO memory.memory_versions (memory_version_id,memory_id,version,content_id,content_hash,sensitivity,confidence,provenance,valid_from_ms,valid_to_ms,source,confidence_bps) VALUES ($1,$2,1,$3,$4,2,1,$5,100,NULL,3,10000)")
            .bind(version_id.into_uuid()).bind(memory_id.into_uuid())
            .bind(reference.content_id.into_uuid()).bind(reference.sha256.into_bytes().to_vec())
            .bind(b"normalized-provenance:v1".as_slice()).execute(&mut *tx).await.expect("fixture version");
        tx.commit().await.expect("fixture commit");
        (memory_id, version_id)
    }

    fn store(&self) -> PostgresMemoryEvolutionStore {
        PostgresMemoryEvolutionStore::new(self.pool.clone())
    }

    fn revoke(
        &self,
        actor: PrincipalId,
        agent_id: AgentId,
        memory: (MemoryId, MemoryVersionId),
        key: &str,
    ) -> RevokeAgentMemory {
        RevokeAgentMemory {
            tenant_id: self.tenant_id,
            actor_principal_id: actor,
            agent_id,
            memory_id: memory.0,
            expected_memory_version_id: memory.1,
            reason_code: DottedKey::new("user.revoked").expect("reason"),
            idempotency_key: IdempotencyKey::new(key).expect("key"),
            revoked_at: UtcTimestamp::from_unix_millis(500),
        }
    }

    async fn head(&self, memory_id: MemoryId) -> (i16, Option<i64>, i64) {
        sqlx::query_as(
            "SELECT lifecycle,revoked_at_ms,updated_at_ms FROM memory.memories WHERE memory_id=$1",
        )
        .bind(memory_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .expect("memory head")
    }

    async fn revocations(&self, memory_id: MemoryId) -> Vec<(uuid::Uuid, uuid::Uuid, String)> {
        sqlx::query_as(
            "SELECT agent_id,revoked_by,reason_code FROM memory.agent_memory_revocations
             WHERE tenant_id=$1 AND memory_id=$2",
        )
        .bind(self.tenant_id.into_uuid())
        .bind(memory_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .expect("revocation records")
    }
}

#[tokio::test]
async fn owner_revokes_a_memory_of_the_builtin_main_agent() {
    let fixture = Fixture::new().await;
    let main = fixture.main_agent().await;
    let memory = fixture.memory(main).await;

    let listed = fixture
        .store()
        .list_managed_memories(ListManagedMemories {
            identity: AgentMemoryManagementIdentity {
                actor: Actor {
                    tenant_id: fixture.tenant_id,
                    principal_id: fixture.owner,
                    principal_kind: PrincipalKind::User,
                },
                agent_id: main,
            },
            lifecycle: ManagedMemoryLifecycle::Active,
            observed_at: UtcTimestamp::from_unix_millis(i64::MAX / 2),
            after: None,
            limit: std::num::NonZeroU16::new(10).expect("limit"),
        })
        .await
        .expect("owner lists main-agent memories");
    assert_eq!(listed.heads.len(), 1);
    assert_eq!(listed.heads[0].memory_id, memory.0);

    let command = fixture.revoke(fixture.owner, main, memory, "revoke:main");
    let applied = fixture
        .store()
        .revoke_agent_memory(command.clone())
        .await
        .expect("owner revokes main-agent memory");
    assert_eq!(
        applied.disposition,
        AgentMemoryLifecycleDisposition::Applied
    );
    assert_eq!(applied.receipt.agent_id, main);
    assert_eq!(fixture.head(memory.0).await, (3, Some(500), 500));
    assert_eq!(
        fixture.revocations(memory.0).await,
        vec![(
            main.into_uuid(),
            fixture.owner.into_uuid(),
            "user.revoked".to_owned()
        )]
    );

    let replay = fixture
        .store()
        .revoke_agent_memory(command)
        .await
        .expect("idempotent replay");
    assert_eq!(
        replay.disposition,
        AgentMemoryLifecycleDisposition::Duplicate
    );
    assert_eq!(fixture.revocations(memory.0).await.len(), 1);
}

#[tokio::test]
async fn non_owners_and_unowned_agents_stay_denied() {
    let fixture = Fixture::new().await;
    let main = fixture.main_agent().await;
    let other = fixture.unowned_agent().await;
    let main_memory = fixture.memory(main).await;
    let other_memory = fixture.memory(other).await;

    // A principal without a space in this tenant cannot revoke main memories.
    let stranger = fixture.revoke(
        PrincipalId::generate(),
        main,
        main_memory,
        "revoke:stranger",
    );
    assert!(matches!(
        fixture.store().revoke_agent_memory(stranger).await,
        Err(PostgresMemoryEvolutionError::MemoryManagementUnauthorized)
    ));
    // The owner gains nothing on an Agent that is neither owned nor `main`.
    let unowned = fixture.revoke(fixture.owner, other, other_memory, "revoke:unowned");
    assert!(matches!(
        fixture.store().revoke_agent_memory(unowned).await,
        Err(PostgresMemoryEvolutionError::MemoryManagementUnauthorized)
    ));
    // A retired main definition no longer grants authority.
    sqlx::query("UPDATE agent.agent_definitions SET lifecycle=2 WHERE agent_id=$1")
        .bind(main.into_uuid())
        .execute(&fixture.pool)
        .await
        .expect("retire main");
    let retired = fixture.revoke(fixture.owner, main, main_memory, "revoke:retired");
    assert!(matches!(
        fixture.store().revoke_agent_memory(retired).await,
        Err(PostgresMemoryEvolutionError::MemoryManagementUnauthorized)
    ));
    for memory in [main_memory, other_memory] {
        assert_eq!(fixture.head(memory.0).await, (1, None, 100));
        assert!(fixture.revocations(memory.0).await.is_empty());
    }
}

/// Main-agent memories are tenant-scoped and carry no producing principal, so
/// with two owners in one tenant neither may list, read, revoke or erase them.
#[tokio::test]
async fn co_owners_cannot_manage_tenant_shared_main_agent_memories() {
    let fixture = Fixture::new().await;
    let main = fixture.main_agent().await;
    let memory = fixture.memory(main).await;

    // Sole owner A: allowed.
    assert_eq!(
        fixture
            .list(fixture.owner, main)
            .await
            .expect("sole owner lists")
            .heads
            .len(),
        1
    );

    // A second owner joins the tenant: both owners now fail closed.
    let co_owner = fixture.add_owner().await;
    for principal in [co_owner, fixture.owner] {
        assert!(matches!(
            fixture.list(principal, main).await,
            Err(MemoryManagementError::Unauthorized)
        ));
        let revoke = fixture.revoke(principal, main, memory, &format!("revoke:{principal}"));
        assert!(matches!(
            fixture.store().revoke_agent_memory(revoke).await,
            Err(PostgresMemoryEvolutionError::MemoryManagementUnauthorized)
        ));
        assert!(matches!(
            fixture
                .erase(principal, main, memory, &format!("erase:{principal}"))
                .await,
            Err(PostgresPrivacyErasureError::AgentMemoryUnauthorized)
        ));
    }
    assert_eq!(fixture.head(memory.0).await, (1, None, 100));
    assert!(fixture.revocations(memory.0).await.is_empty());
}
