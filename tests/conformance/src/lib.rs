//! PostgreSQL kernel conformance fixtures.
#![forbid(unsafe_code)]
mod blobs;
pub use blobs::MemoryBlobs;
pub mod fixtures;

/// Apply the real kernel baseline and publish the test host's explicit Agent.
/// The kernel itself never seeds a built-in persona or Agent definition.
pub async fn migrate(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    use musteragent_definition::AgentDefinitionStore;
    musteragent_postgres::migrate(pool).await?;
    let registry = musteragent_postgres::PostgresAgentDefinitionStore::new(pool.clone());
    registry
        .insert_definition(fixtures::agent::system_definition())
        .await?;
    Ok(())
}
