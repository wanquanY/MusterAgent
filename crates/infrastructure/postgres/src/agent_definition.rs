pub(crate) mod publication;
mod subagent;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_definition::{
    AgentDefinition, AgentKind, AgentLifecycle, AgentVersion, AgentVersionMaterialV1,
    PublishedAgentVersionPointer, ReleaseChannel,
};
use musteragent_definition::{
    AgentDefinitionStore, AgentDefinitionStoreError, DefinitionInsertOutcome,
    PublishedAgentVersionGuard, PublishedPointerCasOutcome, VersionInsertOutcome,
};
use musteragent_identity::{
    AgentId, AgentKey, AgentVersionId, MemoryCollectionId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_runtime_contract::ApplicationFuture;
use sqlx::{PgPool, Row};

#[derive(Clone)]
pub struct PostgresAgentDefinitionStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
}

impl PostgresAgentDefinitionStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self {
            pool: database.pool().clone(),
            database,
        }
    }

    async fn load_definition_inner(
        &self,
        agent_key: &AgentKey,
    ) -> Result<Option<AgentDefinition>, AgentDefinitionAdapterError> {
        let row = sqlx::query(DEFINITION_SELECT_BY_KEY)
            .bind(agent_key.as_str())
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| decode_definition(&row)).transpose()
    }

    async fn insert_definition_inner(
        &self,
        definition: AgentDefinition,
    ) -> Result<DefinitionInsertOutcome, AgentDefinitionAdapterError> {
        definition
            .validate()
            .map_err(|_| AgentDefinitionAdapterError::Corrupt)?;
        let inserted = sqlx::query(
            "INSERT INTO agent.agent_definitions (
                 agent_id, agent_key, built_in, lifecycle, created_at_ms, updated_at_ms,
                 system_slot, display_name, agent_kind, deletable, replaceable,
                 default_for_product
             ) VALUES (
                 $1, $2, $3, $4,
                 floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
                 floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
                 $5, $6, $7, $8, $9, $10
             ) ON CONFLICT DO NOTHING",
        )
        .bind(definition.agent_id.into_uuid())
        .bind(definition.agent_key.as_str())
        .bind(definition.built_in)
        .bind(agent_lifecycle(definition.lifecycle))
        .bind(definition.system_slot.as_deref())
        .bind(&definition.display_name)
        .bind(agent_kind(definition.kind))
        .bind(definition.deletable)
        .bind(definition.replaceable)
        .bind(definition.default_for_product)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            return Ok(DefinitionInsertOutcome::Inserted);
        }

        let mut conflicts = sqlx::query(
            "SELECT agent_id, agent_key, system_slot, display_name, agent_kind,
                    built_in, deletable, replaceable, default_for_product, lifecycle
             FROM agent.agent_definitions
             WHERE agent_id = $1 OR agent_key = $2 OR ($3 AND built_in)",
        )
        .bind(definition.agent_id.into_uuid())
        .bind(definition.agent_key.as_str())
        .bind(definition.built_in)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| decode_definition(&row))
        .collect::<Result<Vec<_>, _>>()?;
        if conflicts.len() != 1 {
            return Err(AgentDefinitionAdapterError::Corrupt);
        }
        Ok(DefinitionInsertOutcome::Existing(conflicts.remove(0)))
    }

    async fn load_version_inner(
        &self,
        agent_version_id: AgentVersionId,
    ) -> Result<Option<AgentVersion>, AgentDefinitionAdapterError> {
        let row = sqlx::query(VERSION_SELECT_BY_ID)
            .bind(agent_version_id.into_uuid())
            .fetch_optional(&self.pool)
            .await?;
        if let Some(row) = row {
            return decode_version(&row).map(Some);
        }
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM agent.agent_versions WHERE agent_version_id = $1
             )",
        )
        .bind(agent_version_id.into_uuid())
        .fetch_one(&self.pool)
        .await?;
        if exists {
            Err(AgentDefinitionAdapterError::Corrupt)
        } else {
            Ok(None)
        }
    }

    async fn insert_version_inner(
        &self,
        version: AgentVersion,
    ) -> Result<VersionInsertOutcome, AgentDefinitionAdapterError> {
        version
            .validate_complete_for_publication()
            .map_err(|_| AgentDefinitionAdapterError::Corrupt)?;
        let material = version
            .material
            .as_ref()
            .ok_or(AgentDefinitionAdapterError::Corrupt)?;
        let inserted = sqlx::query(
            "INSERT INTO agent.agent_versions (
                 agent_version_id, agent_id, version, persona_content_id, persona_hash,
                 policy_ref, published_at_ms, retired_at_ms, persona_version, policy_hash,
                 base_skill_refs, memory_policy_ref, model_requirement_ref,
                 runtime_contract_ref, release_channel, activated_at_ms,
                 supersedes_agent_version_id, material_schema_version,
                 manifest_content_id, manifest_hash, policy_content_id,
                 memory_policy_content_id, memory_policy_hash,
                 user_memory_collection_id, long_term_memory_collection_id
             ) SELECT
                 $1, definition.agent_id, $3::bigint, persona.content_id, $5,
                 $6, $7, NULL, $8::bigint, $9, $10, $11, $12, $13, $14, $15, $16,
                 1, manifest.content_id, $18, policy.content_id,
                 memory_policy.content_id, $21, $22, $23
             FROM agent.agent_definitions AS definition
             JOIN artifact.content_refs AS persona
               ON persona.content_id = $4
              AND persona.media_type = $24
              AND persona.byte_length = $25::bigint
              AND persona.content_hash = $5
              AND persona.state = 2
             JOIN artifact.content_refs AS manifest
               ON manifest.content_id = $17
              AND manifest.media_type = $26
              AND manifest.byte_length = $27::bigint
              AND manifest.content_hash = $18
              AND manifest.state = 2
              AND manifest.tenant_id IS NOT DISTINCT FROM persona.tenant_id
             JOIN artifact.content_refs AS policy
               ON policy.content_id = $19
              AND policy.media_type = $28
              AND policy.byte_length = $29::bigint
              AND policy.content_hash = $9
              AND policy.state = 2
              AND policy.tenant_id IS NOT DISTINCT FROM persona.tenant_id
             JOIN artifact.content_refs AS memory_policy
               ON memory_policy.content_id = $20
              AND memory_policy.media_type = $30
              AND memory_policy.byte_length = $31::bigint
              AND memory_policy.content_hash = $21
              AND memory_policy.state = 2
              AND memory_policy.tenant_id IS NOT DISTINCT FROM persona.tenant_id
             WHERE definition.agent_id = $2
               AND (NOT definition.built_in OR persona.tenant_id IS NULL)
             ON CONFLICT DO NOTHING",
        )
        .bind(version.agent_version_id.into_uuid())
        .bind(version.agent_id.into_uuid())
        .bind(version.version.get().to_string())
        .bind(version.persona_content_ref.content_id.into_uuid())
        .bind(version.persona_content_ref.sha256.into_bytes().as_slice())
        .bind(version.policy_ref.as_str())
        .bind(version.published_at.unix_millis())
        .bind(version.persona_version.get().to_string())
        .bind(version.policy_hash.into_bytes().as_slice())
        .bind(
            version
                .base_skill_refs
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(version.memory_policy_ref.as_str())
        .bind(version.model_requirement_ref.as_str())
        .bind(version.supported_runtime_contract.as_str())
        .bind(release_channel(version.release_channel))
        .bind(version.activated_at.unix_millis())
        .bind(version.supersedes.map(AgentVersionId::into_uuid))
        .bind(material.manifest_ref.content_id.into_uuid())
        .bind(material.manifest_hash.into_bytes().as_slice())
        .bind(material.policy_content_ref.content_id.into_uuid())
        .bind(material.memory_policy_content_ref.content_id.into_uuid())
        .bind(material.memory_policy_hash.into_bytes().as_slice())
        .bind(
            material
                .user_memory_collection_id
                .map(MemoryCollectionId::into_uuid),
        )
        .bind(
            material
                .long_term_memory_collection_id
                .map(MemoryCollectionId::into_uuid),
        )
        .bind(version.persona_content_ref.media_type.as_str())
        .bind(version.persona_content_ref.byte_length.to_string())
        .bind(material.manifest_ref.media_type.as_str())
        .bind(material.manifest_ref.byte_length.to_string())
        .bind(material.policy_content_ref.media_type.as_str())
        .bind(material.policy_content_ref.byte_length.to_string())
        .bind(material.memory_policy_content_ref.media_type.as_str())
        .bind(material.memory_policy_content_ref.byte_length.to_string())
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            return Ok(VersionInsertOutcome::Inserted);
        }

        let rows = sqlx::query(VERSION_SELECT_CONFLICT)
            .bind(version.agent_version_id.into_uuid())
            .bind(version.agent_id.into_uuid())
            .bind(version.version.get().to_string())
            .fetch_all(&self.pool)
            .await?;
        let mut conflicts = rows
            .into_iter()
            .map(|row| decode_version(&row))
            .collect::<Result<Vec<_>, _>>()?;
        if conflicts.len() != 1 {
            return Err(AgentDefinitionAdapterError::Corrupt);
        }
        Ok(VersionInsertOutcome::Existing(Box::new(
            conflicts.remove(0),
        )))
    }

    async fn load_published_inner(
        &self,
        agent_id: AgentId,
    ) -> Result<Option<PublishedAgentVersionPointer>, AgentDefinitionAdapterError> {
        let row = sqlx::query(
            "SELECT agent_id, agent_version_id, pointer_version::text AS pointer_version,
                    published_at_ms, published_by
             FROM agent.published_agent_version_pointers AS pointer
             WHERE pointer.agent_id = $1
             ORDER BY pointer.pointer_version DESC
             LIMIT 1",
        )
        .bind(agent_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| decode_pointer(&row)).transpose()
    }

    async fn cas_inner(
        &self,
        agent_id: AgentId,
        expected: Option<PublishedAgentVersionGuard>,
        replacement: PublishedAgentVersionPointer,
    ) -> Result<PublishedPointerCasOutcome, AgentDefinitionAdapterError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await?;
        let outcome = publication::compare_and_swap_in_transaction(
            &mut transaction,
            agent_id,
            expected,
            replacement,
        )
        .await?;
        transaction.commit().await?;
        Ok(outcome)
    }
}

impl AgentDefinitionStore for PostgresAgentDefinitionStore {
    fn load_definition<'a>(
        &'a self,
        agent_key: &'a AgentKey,
    ) -> ApplicationFuture<'a, Result<Option<AgentDefinition>, AgentDefinitionStoreError>> {
        Box::pin(async move {
            self.load_definition_inner(agent_key)
                .await
                .map_err(AgentDefinitionAdapterError::port)
        })
    }

    fn insert_definition(
        &self,
        definition: AgentDefinition,
    ) -> ApplicationFuture<'_, Result<DefinitionInsertOutcome, AgentDefinitionStoreError>> {
        Box::pin(async move {
            self.insert_definition_inner(definition)
                .await
                .map_err(AgentDefinitionAdapterError::port)
        })
    }

    fn load_version(
        &self,
        agent_version_id: AgentVersionId,
    ) -> ApplicationFuture<'_, Result<Option<AgentVersion>, AgentDefinitionStoreError>> {
        Box::pin(async move {
            self.load_version_inner(agent_version_id)
                .await
                .map_err(AgentDefinitionAdapterError::port)
        })
    }

    fn insert_version(
        &self,
        version: AgentVersion,
    ) -> ApplicationFuture<'_, Result<VersionInsertOutcome, AgentDefinitionStoreError>> {
        Box::pin(async move {
            self.insert_version_inner(version)
                .await
                .map_err(AgentDefinitionAdapterError::port)
        })
    }

    fn load_published(
        &self,
        agent_id: AgentId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<PublishedAgentVersionPointer>, AgentDefinitionStoreError>,
    > {
        Box::pin(async move {
            self.load_published_inner(agent_id)
                .await
                .map_err(AgentDefinitionAdapterError::port)
        })
    }

    fn compare_and_swap_published(
        &self,
        agent_id: AgentId,
        expected: Option<PublishedAgentVersionGuard>,
        replacement: PublishedAgentVersionPointer,
    ) -> ApplicationFuture<'_, Result<PublishedPointerCasOutcome, AgentDefinitionStoreError>> {
        Box::pin(async move {
            self.cas_inner(agent_id, expected, replacement)
                .await
                .map_err(AgentDefinitionAdapterError::port)
        })
    }
}

const DEFINITION_SELECT_BY_KEY: &str =
    "SELECT agent_id, agent_key, system_slot, display_name, agent_kind, built_in,
            deletable, replaceable, default_for_product, lifecycle
     FROM agent.agent_definitions WHERE agent_key = $1";

macro_rules! version_select {
    ($predicate:literal) => {
        concat!(
            "SELECT version.agent_version_id, version.agent_id,
            version.version::text AS version_number,
            version.persona_version::text AS persona_version,
            version.persona_content_id, version.persona_hash, version.policy_hash,
            version.base_skill_refs, version.policy_ref, version.memory_policy_ref,
            version.model_requirement_ref, version.runtime_contract_ref,
            version.release_channel, version.published_at_ms, version.activated_at_ms,
            version.supersedes_agent_version_id, version.material_schema_version,
            version.manifest_content_id, version.manifest_hash, version.policy_content_id,
            version.memory_policy_content_id, version.memory_policy_hash,
            version.user_memory_collection_id, version.long_term_memory_collection_id,
            persona.media_type, persona.byte_length::text AS byte_length,
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
     FROM agent.agent_versions AS version
     JOIN artifact.content_refs AS persona
       ON persona.content_id = version.persona_content_id AND persona.state = 2
     LEFT JOIN artifact.content_refs AS manifest
       ON manifest.content_id = version.manifest_content_id AND manifest.state = 2
     LEFT JOIN artifact.content_refs AS policy
       ON policy.content_id = version.policy_content_id AND policy.state = 2
     LEFT JOIN artifact.content_refs AS memory_policy
       ON memory_policy.content_id = version.memory_policy_content_id
      AND memory_policy.state = 2
",
            $predicate
        )
    };
}
pub(crate) const VERSION_SELECT_BY_ID: &str =
    version_select!(" WHERE version.agent_version_id = $1");
pub(crate) const VERSION_SELECT_BY_IDS: &str =
    version_select!(" WHERE version.agent_version_id = ANY($1)");
const VERSION_SELECT_CONFLICT: &str = version_select!(
    " WHERE version.agent_version_id = $1 OR (version.agent_id = $2 AND version.version = $3::bigint)"
);

pub(crate) fn decode_definition(
    row: &sqlx::postgres::PgRow,
) -> Result<AgentDefinition, AgentDefinitionAdapterError> {
    let definition = AgentDefinition {
        agent_id: id(row.try_get("agent_id")?)?,
        agent_key: AgentKey::new(row.try_get::<String, _>("agent_key")?)
            .map_err(|_| AgentDefinitionAdapterError::Corrupt)?,
        system_slot: row.try_get("system_slot")?,
        display_name: row.try_get("display_name")?,
        kind: decode_agent_kind(row.try_get("agent_kind")?)?,
        built_in: row.try_get("built_in")?,
        deletable: row.try_get("deletable")?,
        replaceable: row.try_get("replaceable")?,
        default_for_product: row.try_get("default_for_product")?,
        lifecycle: decode_agent_lifecycle(row.try_get("lifecycle")?)?,
    };
    definition
        .validate()
        .map_err(|_| AgentDefinitionAdapterError::Corrupt)?;
    Ok(definition)
}

pub(crate) fn decode_version(
    row: &sqlx::postgres::PgRow,
) -> Result<AgentVersion, AgentDefinitionAdapterError> {
    let persona_hash = hash(row, "persona_hash")?;
    let content_hash = hash(row, "content_hash")?;
    if persona_hash != content_hash {
        return Err(AgentDefinitionAdapterError::Corrupt);
    }
    let policy_hash = hash(row, "policy_hash")?;
    let material = decode_material(row, policy_hash)?;
    let version = AgentVersion {
        agent_version_id: id(row.try_get("agent_version_id")?)?,
        agent_id: id(row.try_get("agent_id")?)?,
        version: nonzero_version(row.try_get("version_number")?)?,
        persona_version: nonzero_version(row.try_get("persona_version")?)?,
        persona_content_ref: ContentRef {
            content_id: id(row.try_get("persona_content_id")?)?,
            media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                .map_err(|_| AgentDefinitionAdapterError::Corrupt)?,
            byte_length: parse_u64(row.try_get("byte_length")?)?,
            sha256: content_hash,
        },
        base_skill_refs: row
            .try_get::<Vec<String>, _>("base_skill_refs")?
            .into_iter()
            .map(|value| VersionedKey::new(value).map_err(|_| AgentDefinitionAdapterError::Corrupt))
            .collect::<Result<_, _>>()?,
        policy_ref: versioned_key(row, "policy_ref")?,
        policy_hash,
        memory_policy_ref: versioned_key(row, "memory_policy_ref")?,
        model_requirement_ref: versioned_key(row, "model_requirement_ref")?,
        supported_runtime_contract: versioned_key(row, "runtime_contract_ref")?,
        release_channel: decode_release_channel(row.try_get("release_channel")?)?,
        published_at: UtcTimestamp::from_unix_millis(row.try_get("published_at_ms")?),
        activated_at: UtcTimestamp::from_unix_millis(
            row.try_get::<Option<i64>, _>("activated_at_ms")?
                .ok_or(AgentDefinitionAdapterError::Corrupt)?,
        ),
        supersedes: optional_id(row.try_get("supersedes_agent_version_id")?)?,
        material,
    };
    version
        .validate()
        .map_err(|_| AgentDefinitionAdapterError::Corrupt)?;
    Ok(version)
}

fn decode_material(
    row: &sqlx::postgres::PgRow,
    policy_hash: Sha256,
) -> Result<Option<AgentVersionMaterialV1>, AgentDefinitionAdapterError> {
    let schema = row.try_get::<Option<i16>, _>("material_schema_version")?;
    if schema.is_none() {
        let has_partial_material = row
            .try_get::<Option<uuid::Uuid>, _>("manifest_content_id")?
            .is_some()
            || row
                .try_get::<Option<Vec<u8>>, _>("manifest_hash")?
                .is_some()
            || row
                .try_get::<Option<uuid::Uuid>, _>("policy_content_id")?
                .is_some()
            || row
                .try_get::<Option<uuid::Uuid>, _>("memory_policy_content_id")?
                .is_some()
            || row
                .try_get::<Option<Vec<u8>>, _>("memory_policy_hash")?
                .is_some()
            || row
                .try_get::<Option<uuid::Uuid>, _>("user_memory_collection_id")?
                .is_some()
            || row
                .try_get::<Option<uuid::Uuid>, _>("long_term_memory_collection_id")?
                .is_some();
        return if has_partial_material {
            Err(AgentDefinitionAdapterError::Corrupt)
        } else {
            Ok(None)
        };
    }
    if schema != Some(1) {
        return Err(AgentDefinitionAdapterError::Corrupt);
    }

    let manifest_ref = material_content_ref(
        row,
        "manifest_content_id",
        "manifest_media_type",
        "manifest_byte_length",
        "manifest_content_hash",
    )?;
    let manifest_hash = hash(row, "manifest_hash")?;
    let policy_content_ref = material_content_ref(
        row,
        "policy_content_id",
        "policy_media_type",
        "policy_byte_length",
        "policy_content_hash",
    )?;
    let memory_policy_content_ref = material_content_ref(
        row,
        "memory_policy_content_id",
        "memory_policy_media_type",
        "memory_policy_byte_length",
        "memory_policy_content_hash",
    )?;
    let memory_policy_hash = hash(row, "memory_policy_hash")?;
    if manifest_ref.sha256 != manifest_hash
        || policy_content_ref.sha256 != policy_hash
        || memory_policy_content_ref.sha256 != memory_policy_hash
    {
        return Err(AgentDefinitionAdapterError::Corrupt);
    }
    Ok(Some(AgentVersionMaterialV1 {
        manifest_ref,
        manifest_hash,
        policy_content_ref,
        memory_policy_content_ref,
        memory_policy_hash,
        user_memory_collection_id: optional_id(row.try_get("user_memory_collection_id")?)?,
        long_term_memory_collection_id: optional_id(
            row.try_get("long_term_memory_collection_id")?,
        )?,
    }))
}

fn material_content_ref(
    row: &sqlx::postgres::PgRow,
    id_column: &str,
    media_type_column: &str,
    byte_length_column: &str,
    hash_column: &str,
) -> Result<ContentRef, AgentDefinitionAdapterError> {
    Ok(ContentRef {
        content_id: id(row
            .try_get::<Option<uuid::Uuid>, _>(id_column)?
            .ok_or(AgentDefinitionAdapterError::Corrupt)?)?,
        media_type: MediaType::new(
            row.try_get::<Option<String>, _>(media_type_column)?
                .ok_or(AgentDefinitionAdapterError::Corrupt)?,
        )
        .map_err(|_| AgentDefinitionAdapterError::Corrupt)?,
        byte_length: parse_u64(
            row.try_get::<Option<String>, _>(byte_length_column)?
                .ok_or(AgentDefinitionAdapterError::Corrupt)?,
        )?,
        sha256: Sha256::from_bytes(
            row.try_get::<Option<Vec<u8>>, _>(hash_column)?
                .ok_or(AgentDefinitionAdapterError::Corrupt)?,
        )
        .map_err(|_| AgentDefinitionAdapterError::Corrupt)?,
    })
}

pub(crate) fn decode_pointer(
    row: &sqlx::postgres::PgRow,
) -> Result<PublishedAgentVersionPointer, AgentDefinitionAdapterError> {
    Ok(PublishedAgentVersionPointer {
        agent_id: id(row.try_get("agent_id")?)?,
        agent_version_id: id(row.try_get("agent_version_id")?)?,
        version: parse_u64(row.try_get("pointer_version")?)?,
        published_at: UtcTimestamp::from_unix_millis(row.try_get("published_at_ms")?),
        published_by: id(row.try_get("published_by")?)?,
    })
}

fn versioned_key(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<VersionedKey, AgentDefinitionAdapterError> {
    VersionedKey::new(row.try_get::<String, _>(column)?)
        .map_err(|_| AgentDefinitionAdapterError::Corrupt)
}

fn hash(row: &sqlx::postgres::PgRow, column: &str) -> Result<Sha256, AgentDefinitionAdapterError> {
    Sha256::from_bytes(row.try_get::<Vec<u8>, _>(column)?)
        .map_err(|_| AgentDefinitionAdapterError::Corrupt)
}

fn nonzero_version(value: String) -> Result<VersionNumber, AgentDefinitionAdapterError> {
    VersionNumber::new(parse_u64(value)?).map_err(|_| AgentDefinitionAdapterError::Corrupt)
}

fn parse_u64(value: String) -> Result<u64, AgentDefinitionAdapterError> {
    value
        .parse()
        .map_err(|_| AgentDefinitionAdapterError::Corrupt)
}

fn id<T>(value: uuid::Uuid) -> Result<T, AgentDefinitionAdapterError>
where
    T: std::str::FromStr,
{
    value
        .to_string()
        .parse()
        .map_err(|_| AgentDefinitionAdapterError::Corrupt)
}

fn optional_id<T>(value: Option<uuid::Uuid>) -> Result<Option<T>, AgentDefinitionAdapterError>
where
    T: std::str::FromStr,
{
    value.map(id).transpose()
}

const fn agent_kind(value: AgentKind) -> i16 {
    match value {
        AgentKind::SystemBuiltin => 1,
        AgentKind::Custom => 2,
        AgentKind::Installed => 3,
    }
}

const fn decode_agent_kind(value: i16) -> Result<AgentKind, AgentDefinitionAdapterError> {
    match value {
        1 => Ok(AgentKind::SystemBuiltin),
        2 => Ok(AgentKind::Custom),
        3 => Ok(AgentKind::Installed),
        _ => Err(AgentDefinitionAdapterError::Corrupt),
    }
}

const fn agent_lifecycle(value: AgentLifecycle) -> i16 {
    match value {
        AgentLifecycle::Active => 1,
        AgentLifecycle::Retired => 2,
        AgentLifecycle::Deleted => 3,
    }
}

const fn decode_agent_lifecycle(value: i16) -> Result<AgentLifecycle, AgentDefinitionAdapterError> {
    match value {
        1 => Ok(AgentLifecycle::Active),
        2 => Ok(AgentLifecycle::Retired),
        3 => Ok(AgentLifecycle::Deleted),
        _ => Err(AgentDefinitionAdapterError::Corrupt),
    }
}

const fn release_channel(value: ReleaseChannel) -> i16 {
    match value {
        ReleaseChannel::Stable => 1,
        ReleaseChannel::Canary => 2,
    }
}

const fn decode_release_channel(value: i16) -> Result<ReleaseChannel, AgentDefinitionAdapterError> {
    match value {
        1 => Ok(ReleaseChannel::Stable),
        2 => Ok(ReleaseChannel::Canary),
        _ => Err(AgentDefinitionAdapterError::Corrupt),
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AgentDefinitionAdapterError {
    #[error("PostgreSQL Agent Definition state is corrupt")]
    Corrupt,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Transaction(#[from] crate::PostgresTransactionError),
}

impl AgentDefinitionAdapterError {
    fn port(self) -> AgentDefinitionStoreError {
        match self {
            Self::Corrupt => AgentDefinitionStoreError::Corrupt,
            Self::Database(_) | Self::Transaction(_) => AgentDefinitionStoreError::Unavailable,
        }
    }
}
