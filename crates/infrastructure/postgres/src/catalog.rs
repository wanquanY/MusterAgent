use musteragent_definition::ResolvedPublishedAgentVersion;
use musteragent_definition::{AgentDefinition, AgentVersion, PublishedAgentVersionPointer};
use musteragent_identity::{AgentKey, AgentVersionId, PrincipalId, UtcTimestamp};
use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::agent_definition::{decode_definition, decode_version};

#[derive(Debug, Clone)]
pub struct PostgresAgentCatalog {
    pool: PgPool,
}

impl PostgresAgentCatalog {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn system_builtin_count(&self) -> Result<i64, PostgresAgentCatalogError> {
        Ok(sqlx::query_scalar(
            "SELECT count(*)
             FROM agent.agent_definitions
             WHERE built_in AND lifecycle = 1",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    /// Resolve the host-selected Agent through its authoritative published pointer.
    pub async fn resolve(
        &self,
        agent_key: &AgentKey,
    ) -> Result<(AgentDefinition, AgentVersion), PostgresAgentCatalogError> {
        let resolved = self.resolve_published(agent_key).await?;
        Ok((resolved.definition, resolved.version))
    }

    /// Resolve the exact published version of the requested Agent. Unpublished
    /// higher versions are never visible to admission.
    pub async fn resolve_published(
        &self,
        agent_key: &AgentKey,
    ) -> Result<ResolvedPublishedAgentVersion, PostgresAgentCatalogError> {
        let row = sqlx::query(
            "SELECT
                 definition.agent_id,
                 definition.agent_key,
                 definition.system_slot,
                 definition.display_name,
                 definition.agent_kind,
                 definition.built_in,
                 definition.deletable,
                 definition.replaceable,
                 definition.default_for_product,
                 definition.lifecycle,
                 version.agent_version_id,
                 version.version::text AS version_number,
                 version.persona_version::text AS persona_version,
                 version.persona_content_id,
                 version.persona_hash,
                 version.policy_hash,
                 version.base_skill_refs,
                 version.policy_ref,
                 version.memory_policy_ref,
                 version.model_requirement_ref,
                 version.runtime_contract_ref,
                 version.release_channel,
                 version.published_at_ms,
                 version.activated_at_ms,
                 version.supersedes_agent_version_id,
                 version.material_schema_version,
                 version.manifest_content_id,
                 version.manifest_hash,
                 version.policy_content_id,
                 version.memory_policy_content_id,
                 version.memory_policy_hash,
                 version.user_memory_collection_id,
                 version.long_term_memory_collection_id,
                 pointer.pointer_version::text AS pointer_version,
                 pointer.published_at_ms AS pointer_published_at_ms,
                 pointer.published_by,
                 persona.media_type,
                 persona.byte_length::text AS byte_length,
                 persona.content_hash,
                 manifest.media_type AS manifest_media_type,
                 manifest.byte_length::text AS manifest_byte_length,
                 manifest.content_hash AS manifest_content_hash,
                 policy.media_type AS policy_media_type,
                 policy.byte_length::text AS policy_byte_length,
                 policy.content_hash AS policy_content_hash,
                 memory_policy.media_type AS memory_policy_media_type,
                 memory_policy.byte_length::text AS memory_policy_byte_length,
                 memory_policy.content_hash AS memory_policy_content_hash
             FROM agent.agent_definitions AS definition
             JOIN LATERAL (
                 SELECT agent_version_id, pointer_version, published_at_ms, published_by
                 FROM agent.published_agent_version_pointers
                 WHERE agent_id = definition.agent_id
                 ORDER BY pointer_version DESC
                 LIMIT 1
             ) AS pointer ON TRUE
             JOIN agent.agent_versions AS version
               ON version.agent_id = definition.agent_id
              AND version.agent_version_id = pointer.agent_version_id
             JOIN artifact.content_refs AS persona
               ON persona.content_id = version.persona_content_id
              AND persona.state = 2
             LEFT JOIN artifact.content_refs AS manifest
               ON manifest.content_id = version.manifest_content_id
              AND manifest.state = 2
             LEFT JOIN artifact.content_refs AS policy
               ON policy.content_id = version.policy_content_id
              AND policy.state = 2
             LEFT JOIN artifact.content_refs AS memory_policy
               ON memory_policy.content_id = version.memory_policy_content_id
              AND memory_policy.state = 2
             WHERE definition.agent_key = $1
               AND definition.lifecycle = 1
               AND version.retired_at_ms IS NULL
             LIMIT 1",
        )
        .bind(agent_key.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresAgentCatalogError::AgentUnavailable)?;

        let definition = decode_definition(&row).map_err(|_| PostgresAgentCatalogError::Corrupt)?;
        let version = decode_version(&row).map_err(|_| PostgresAgentCatalogError::Corrupt)?;
        if version.agent_id != definition.agent_id {
            return Err(PostgresAgentCatalogError::Corrupt);
        }
        let pointer = PublishedAgentVersionPointer {
            agent_id: definition.agent_id,
            agent_version_id: AgentVersionId::from_uuid(row.try_get("agent_version_id")?)?,
            version: row
                .try_get::<String, _>("pointer_version")?
                .parse()
                .map_err(|_| PostgresAgentCatalogError::InvalidNumeric)?,
            published_at: UtcTimestamp::from_unix_millis(row.try_get("pointer_published_at_ms")?),
            published_by: PrincipalId::from_uuid(row.try_get("published_by")?)?,
        };
        pointer.validate(&definition, &version)?;
        Ok(ResolvedPublishedAgentVersion {
            definition,
            version,
            pointer,
        })
    }
}

#[derive(Debug, Error)]
pub enum PostgresAgentCatalogError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Identity(#[from] musteragent_identity::IdentityError),
    #[error(transparent)]
    Key(#[from] musteragent_identity::LogicalKeyError),
    #[error(transparent)]
    Scalar(#[from] musteragent_identity::NonZeroScalarError),
    #[error(transparent)]
    Digest(#[from] musteragent_content_ref::DigestLengthError),
    #[error(transparent)]
    MediaType(#[from] musteragent_content_ref::MediaTypeError),
    #[error(transparent)]
    Contract(#[from] musteragent_definition::AgentContractError),
    #[error("Agent has no active catalog version")]
    AgentUnavailable,
    #[error("Agent version is published but not active")]
    InactiveAgentVersion,
    #[error("Agent persona reference and version hash disagree")]
    PersonaHashMismatch,
    #[error("catalog numeric value is invalid")]
    InvalidNumeric,
    #[error("catalog contains a value outside a closed enum")]
    ClosedEnumViolation,
    #[error("catalog Agent Version material is corrupt or unavailable")]
    Corrupt,
}
