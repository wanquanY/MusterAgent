//! Batched content materialization of authorized Snapshot instructions.
use super::*;

impl PostgresContextMaterialSource {
    pub(super) async fn materialize_memories(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
        selected: &[musteragent_memory::SelectedMemoryVersion],
    ) -> Result<Vec<MaterializedMemory>, PostgresContextMaterialError> {
        let mut persisted = BTreeMap::new();
        for group in selected.chunks(128) {
            let ids = group
                .iter()
                .map(|item| item.memory_version_id.into_uuid())
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                "SELECT version.memory_version_id, version.memory_id, version.content_id,
                        content.media_type, content.byte_length::text AS byte_length, content.content_hash
                 FROM memory.memory_refs AS reference
                 JOIN memory.memory_versions AS version ON version.memory_version_id=reference.memory_version_id
                 JOIN artifact.content_refs AS content ON content.content_id=version.content_id
                 WHERE reference.tenant_id=$1 AND reference.run_id=$2
                   AND reference.memory_version_id=ANY($3)
                   AND content.tenant_id=$1 AND content.state=2"
            ).bind(tenant_id.into_uuid()).bind(run_id.into_uuid()).bind(ids).fetch_all(&self.pool).await?;
            for row in rows {
                let version: uuid::Uuid = row.try_get("memory_version_id")?;
                let memory: MemoryId = id(row.try_get("memory_id")?)?;
                persisted.insert(version, (memory, content_ref_from_row(&row)?));
            }
        }
        let references = selected
            .iter()
            .map(|item| {
                let (memory, reference) = persisted
                    .get(&item.memory_version_id.into_uuid())
                    .ok_or(PostgresContextMaterialError::CorruptState)?;
                if *memory != item.memory_id {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                Ok(reference)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let contents = self
            .materialize_contents(tenant_id, references.into_iter())
            .await?;
        Ok(selected
            .iter()
            .zip(contents)
            .map(|(selected, content)| MaterializedMemory {
                memory_version_id: selected.memory_version_id,
                content,
            })
            .collect())
    }

    pub(super) async fn materialize_instincts(
        &self,
        tenant_id: TenantId,
        selected: &[musteragent_runtime_snapshot::InstinctSelectionSnapshot],
    ) -> Result<Vec<MaterializedInstinct>, PostgresContextMaterialError> {
        let contents = self
            .materialize_contents(tenant_id, selected.iter().map(|item| &item.payload_ref))
            .await?;
        Ok(selected
            .iter()
            .zip(contents)
            .map(|(selected, payload)| MaterializedInstinct {
                instinct_id: selected.instinct_id,
                revision: selected.revision,
                payload,
            })
            .collect())
    }

    pub(super) async fn materialize_skills(
        &self,
        tenant_id: TenantId,
        authorized: &[musteragent_skill::SkillDefinition],
        disclosed: &[VersionedKey],
    ) -> Result<Vec<MaterializedSkill>, PostgresContextMaterialError> {
        let selected = disclosed
            .iter()
            .map(|reference| {
                authorized
                    .iter()
                    .find(|skill| skill.skill_ref == *reference)
                    .ok_or(PostgresContextMaterialError::CorruptState)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let contents = self
            .materialize_contents(
                tenant_id,
                selected.iter().map(|skill| &skill.instructions_ref),
            )
            .await?;
        selected
            .into_iter()
            .zip(contents)
            .enumerate()
            .map(|(index, (skill, instructions))| {
                let ordinal = u32::try_from(index.saturating_add(1))
                    .ok()
                    .and_then(std::num::NonZeroU32::new)
                    .ok_or(PostgresContextMaterialError::CorruptState)?;
                Ok(MaterializedSkill {
                    skill_ref: skill.skill_ref.clone(),
                    skill_version_id: skill.skill_version_id,
                    instructions_hash: skill.instructions_hash,
                    disclosure_ordinal: ordinal,
                    instructions,
                })
            })
            .collect()
    }
}
