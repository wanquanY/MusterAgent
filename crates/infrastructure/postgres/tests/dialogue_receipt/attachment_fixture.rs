#![allow(
    dead_code,
    reason = "Integration test binaries use different subsets of this shared fixture."
)]

//! Original released bootstrap; no synthetic Base pin can grant input custody.
use super::*;
use musteragent_postgres::{PostgresContentStore, PostgresTieredBlobStore};
use musteragent_storage_protection::OpaqueRefKeyring;
use std::sync::Arc;

pub(in super::super) async fn activate(pool: &PgPool, owner: Actor) -> OwnedAgentActivationReceipt {
    let content = content_store(pool);
    activate_with_content(pool, owner, content).await
}

pub(in super::super) fn content_store(pool: &PgPool) -> Arc<PostgresContentStore> {
    let key = DottedKey::new("test.input.release").unwrap();

    Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(PostgresTieredBlobStore::new(
            pool.clone(),
            Arc::new(crate::fixture::MemoryBlobs::default()),
        )),
        Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [79; 32])]).unwrap()),
    ))
}

pub(in super::super) async fn activate_partner(
    pool: &PgPool,
    owner: Actor,
    default: &OwnedAgentActivationReceipt,
    content: Arc<PostgresContentStore>,
) -> OwnedAgentActivationReceipt {
    let _ = content;
    let mut activation = crate::fixture::prepared(pool, owner, false).await;
    activation.space_id = default.space.space_id;
    activation.agent.space_id = default.space.space_id;
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(activation)
        .await
        .unwrap();
    assert_eq!(agent.space.space_id, default.space.space_id);
    assert_ne!(agent.agent.agent_id, default.agent.agent_id);
    agent
}

pub(in super::super) async fn activate_with_content(
    pool: &PgPool,
    owner: Actor,
    content: Arc<PostgresContentStore>,
) -> OwnedAgentActivationReceipt {
    musteragent_runtime_skill::OfficialSkillCatalogMaterialApplication::new(content.clone())
        .reconcile(owner.tenant_id)
        .await
        .unwrap();
    PostgresOwnedAgentStore::new(pool.clone())
        .activate(crate::fixture::prepared(pool, owner, true).await)
        .await
        .unwrap()
}
