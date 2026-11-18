//! Immutable settings use the existing Agent/version publication fence.
use super::*;
impl PostgresAgentDefinitionStore {
    /// Read immutable execution/visibility settings for host choosers and
    /// orchestration. Missing settings do not silently select a default model.
    pub async fn subagent_version_settings(
        &self,
        version: AgentVersionId,
    ) -> Result<Option<musteragent_definition::SubagentVersionSettings>, AgentDefinitionStoreError>
    {
        let row=sqlx::query("SELECT settings,settings_hash FROM agent.subagent_version_settings WHERE agent_version_id=$1")
            .bind(version.into_uuid()).fetch_optional(&self.pool).await.map_err(|_|AgentDefinitionStoreError::Unavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let settings: musteragent_definition::SubagentVersionSettings = serde_json::from_value(
            row.try_get("settings")
                .map_err(|_| AgentDefinitionStoreError::Corrupt)?,
        )
        .map_err(|_| AgentDefinitionStoreError::Corrupt)?;
        settings
            .validate()
            .map_err(|_| AgentDefinitionStoreError::Corrupt)?;
        let hash = Sha256::digest(
            serde_json::to_vec(&settings).map_err(|_| AgentDefinitionStoreError::Corrupt)?,
        );
        if hash.into_bytes().as_slice()
            != row
                .try_get::<Vec<u8>, _>("settings_hash")
                .map_err(|_| AgentDefinitionStoreError::Corrupt)?
                .as_slice()
        {
            return Err(AgentDefinitionStoreError::Corrupt);
        }
        Ok(Some(settings))
    }
    /// Install settings before publishing this version. A published version's
    /// configuration can never change; use a new Agent version to update it.
    pub async fn configure_subagent_version(
        &self,
        version: AgentVersionId,
        settings: musteragent_definition::SubagentVersionSettings,
    ) -> Result<(), AgentDefinitionStoreError> {
        settings
            .validate()
            .map_err(|_| AgentDefinitionStoreError::Corrupt)?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(|_| AgentDefinitionStoreError::Unavailable)?;
        let agent:Option<uuid::Uuid>=sqlx::query_scalar("SELECT d.agent_id FROM agent.agent_definitions d JOIN agent.agent_versions v ON v.agent_id=d.agent_id WHERE v.agent_version_id=$1 FOR UPDATE OF d")
            .bind(version.into_uuid()).fetch_optional(&mut *tx).await.map_err(|_|AgentDefinitionStoreError::Unavailable)?;
        if agent.is_none() {
            return Err(AgentDefinitionStoreError::Corrupt);
        }
        let published:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.published_agent_version_pointers WHERE agent_version_id=$1)")
            .bind(version.into_uuid()).fetch_one(&mut *tx).await.map_err(|_|AgentDefinitionStoreError::Unavailable)?;
        if published {
            return Err(AgentDefinitionStoreError::Corrupt);
        }
        let value =
            serde_json::to_value(&settings).map_err(|_| AgentDefinitionStoreError::Corrupt)?;
        let hash = Sha256::digest(
            serde_json::to_vec(&settings).map_err(|_| AgentDefinitionStoreError::Corrupt)?,
        );
        sqlx::query("INSERT INTO agent.subagent_version_settings(agent_version_id,settings,settings_hash) VALUES($1,$2,$3)")
            .bind(version.into_uuid()).bind(value).bind(hash.into_bytes().as_slice()).execute(&mut *tx).await.map_err(|_|AgentDefinitionStoreError::Corrupt)?;
        tx.commit()
            .await
            .map_err(|_| AgentDefinitionStoreError::Unavailable)?;
        Ok(())
    }
}
