//! Same typed retention contract as Device SQLite. Opaque bytes are not parsed
//! as JSON; when the typed projection is unavailable the roots are the
//! conservative superset of identities the payload text names, never empty.
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_persistence_codec::{
    LedgerContentReferencesError, ledger_content_reference_candidates, ledger_content_references,
    ledger_record_schema_is_current,
};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

type Result<T> = std::result::Result<T, sqlx::Error>;

pub(crate) const COVERAGE_COMPLETE: i16 = 1;
pub(crate) const COVERAGE_CONSERVATIVE: i16 = 6;
const COVERAGE_MISSING_PAYLOAD: i16 = 4;

fn corrupt() -> sqlx::Error {
    sqlx::Error::Protocol("Ledger content retention projection is corrupt".into())
}

pub(crate) async fn capture(
    tx: &mut PgConnection,
    record: Uuid,
    tenant: Uuid,
    schema: &str,
    kind: LedgerRecordKind,
    payload: &[u8],
) -> Result<()> {
    let typed = if ledger_record_schema_is_current(schema) {
        match ledger_content_references(kind, payload) {
            Ok(references) => Some(references),
            Err(
                LedgerContentReferencesError::Incomplete | LedgerContentReferencesError::Bounds,
            ) => None,
            Err(LedgerContentReferencesError::Schema) => return Err(corrupt()),
        }
    } else {
        None
    };
    let (coverage, references) = match typed {
        Some(references) => (COVERAGE_COMPLETE, references),
        None => (
            COVERAGE_CONSERVATIVE,
            resolve_candidates(tx, tenant, ledger_content_reference_candidates(payload)).await?,
        ),
    };
    if !references.is_empty() {
        // Serialize with GC's row lock. A root becomes visible in the same
        // commit as its event, before a waiting collector can delete the bytes.
        let ids: Vec<_> = references
            .iter()
            .map(|r| r.content_id.into_uuid())
            .collect();
        sqlx::query("SELECT content_id FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=ANY($2) ORDER BY content_id FOR KEY SHARE")
            .bind(tenant).bind(ids).fetch_all(&mut *tx).await?;
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO event_store.ledger_content_references(record_id,tenant_id,content_id,media_type,byte_length,content_hash) ",
        );
        query.push_values(&references, |mut row, reference| {
            row.push_bind(record)
                .push_bind(tenant)
                .push_bind(reference.content_id.into_uuid())
                .push_bind(reference.media_type.as_str())
                .push_bind(reference.byte_length.to_string())
                .push_unseparated("::numeric")
                .push_bind(reference.sha256.into_bytes().to_vec());
        });
        query.build().execute(&mut *tx).await?;
    }
    finish(tx, record, coverage).await
}

/// Candidates that are not this tenant's (or Release) Content name some other
/// identity. Erased rows keep their digests, so they still resolve.
async fn resolve_candidates(
    tx: &mut PgConnection,
    tenant: Uuid,
    candidates: Vec<musteragent_identity::ContentId>,
) -> Result<Vec<ContentRef>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<_> = candidates.into_iter().map(|id| id.into_uuid()).collect();
    let rows = sqlx::query(
        "SELECT content_id, media_type, byte_length::text AS byte_length, content_hash
           FROM artifact.content_refs
          WHERE content_id=ANY($2) AND (tenant_id=$1 OR tenant_id IS NULL)
          ORDER BY content_id",
    )
    .bind(tenant)
    .bind(ids)
    .fetch_all(&mut *tx)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ContentRef {
                content_id: musteragent_identity::ContentId::from_uuid(row.try_get("content_id")?)
                    .map_err(|_| corrupt())?,
                media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                    .map_err(|_| corrupt())?,
                byte_length: row
                    .try_get::<String, _>("byte_length")?
                    .parse()
                    .map_err(|_| corrupt())?,
                sha256: Sha256::from_bytes(&row.try_get::<Vec<u8>, _>("content_hash")?)
                    .map_err(|_| corrupt())?,
            })
        })
        .collect()
}

async fn finish(tx: &mut PgConnection, record: Uuid, coverage: i16) -> Result<()> {
    if sqlx::query("UPDATE event_store.ledger_records SET content_reference_coverage=$2 WHERE record_id=$1 AND content_reference_coverage IN (0,2,3,5)")
        .bind(record).bind(coverage).execute(tx).await?.rows_affected()!=1 {
        return Err(corrupt());
    }
    Ok(())
}

/// Incremental, restartable upgrade. Pending rows come from the SQL barrier
/// and old writers; 2/3/5 are typed-projection failures from earlier releases
/// that are re-projected once into an exact or conservative root set.
pub(crate) async fn backfill(
    database: &crate::PostgresDatabaseRuntime,
    batches: usize,
) -> std::result::Result<usize, crate::PostgresContentError> {
    let mut completed = 0;
    for _ in 0..batches {
        let mut tx = database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Migration)
            .await?;
        let count = backfill_batch(&mut tx).await?;
        tx.commit().await?;
        completed += count;
        if count < 64 {
            break;
        }
    }
    Ok(completed)
}

/// Reuse the caller's governed transaction, including a migrator whose pool
/// contains only the already-held migration-lock connection.
pub(crate) async fn backfill_batch(
    tx: &mut PgConnection,
) -> std::result::Result<usize, crate::PostgresContentError> {
    let rows = sqlx::query("SELECT r.record_id,r.tenant_id,r.schema_version,r.record_kind,r.payload_hash,
        CASE WHEN r.record_kind=1 THEN octet_length(e.encoded_event) ELSE octet_length(f.encoded_frame) END AS bytes
        FROM event_store.ledger_records r
        LEFT JOIN event_store.domain_events e USING(record_id)
        LEFT JOIN event_store.output_frames f USING(record_id)
        WHERE r.content_reference_coverage IN (0,2,3,5) ORDER BY r.record_id LIMIT 64 FOR UPDATE OF r SKIP LOCKED")
        .fetch_all(&mut *tx).await?;
    let count = rows.len();
    for row in rows {
        let record = row.try_get("record_id")?;
        let bytes: Option<i32> = row.try_get("bytes")?;
        if bytes.is_none() {
            finish(tx, record, COVERAGE_MISSING_PAYLOAD).await?;
            continue;
        }
        let kind = match row.try_get::<i16, _>("record_kind")? {
            1 => LedgerRecordKind::DomainEvent,
            2 => LedgerRecordKind::OutputFrame,
            _ => return Err(corrupt().into()),
        };
        let (payload, hash): (Vec<u8>,Vec<u8>) = sqlx::query_as(
            if kind == LedgerRecordKind::DomainEvent {
                "SELECT encoded_event,payload_hash FROM event_store.domain_events WHERE record_id=$1"
            } else {"SELECT encoded_frame,payload_hash FROM event_store.output_frames WHERE record_id=$1"})
            .bind(record).fetch_one(&mut *tx).await?;
        if hash != row.try_get::<Vec<u8>, _>("payload_hash")?
            || hash != Sha256::digest(&payload).into_bytes()
        {
            return Err(corrupt().into());
        }
        capture(
            tx,
            record,
            row.try_get("tenant_id")?,
            &row.try_get::<String, _>("schema_version")?,
            kind,
            &payload,
        )
        .await?;
    }
    Ok(count)
}
