//! All read lanes materialize the same immutable facts before fusion.
//! Batch the normalized provenance queries; never add two round trips per hit.
use std::collections::BTreeMap;

use musteragent_runtime_memory::MemorySearchHit;
use sqlx::PgPool;
use uuid::Uuid;

use super::{PostgresMemoryEvolutionError, id};

pub(crate) async fn load_memory_provenance(
    pool: &PgPool,
    hits: &mut [MemorySearchHit],
) -> Result<(), PostgresMemoryEvolutionError> {
    if hits.len() > 100 {
        return Err(PostgresMemoryEvolutionError::CorruptDatabase);
    }
    let mut positions = BTreeMap::new();
    for (index, hit) in hits.iter_mut().enumerate() {
        if positions
            .insert(hit.version.memory_version_id.into_uuid(), index)
            .is_some()
        {
            return Err(PostgresMemoryEvolutionError::CorruptDatabase);
        }
        hit.version.source_run_ids.clear();
        hit.version.evidence_record_ids.clear();
    }
    if positions.is_empty() {
        return Ok(());
    }
    let versions = positions.keys().copied().collect::<Vec<_>>();
    let runs = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT memory_version_id,run_id FROM memory.memory_version_source_runs
         WHERE memory_version_id=ANY($1) ORDER BY memory_version_id,run_id",
    )
    .bind(&versions)
    .fetch_all(pool)
    .await?;
    for (version, run) in runs {
        let index = *positions
            .get(&version)
            .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?;
        if !hits[index]
            .version
            .source_run_ids
            .insert(id(run, "memory.source_run_id")?)
        {
            return Err(PostgresMemoryEvolutionError::CorruptDatabase);
        }
    }
    let records = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT memory_version_id,ledger_record_id FROM memory.memory_version_evidence_records
         WHERE memory_version_id=ANY($1) ORDER BY memory_version_id,ledger_record_id",
    )
    .bind(&versions)
    .fetch_all(pool)
    .await?;
    for (version, record) in records {
        let index = *positions
            .get(&version)
            .ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?;
        if !hits[index]
            .version
            .evidence_record_ids
            .insert(id(record, "memory.evidence_record_id")?)
        {
            return Err(PostgresMemoryEvolutionError::CorruptDatabase);
        }
    }
    for hit in hits {
        hit.record.validate()?;
        hit.version.validate()?;
    }
    Ok(())
}
