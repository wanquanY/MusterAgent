//! Original provider completion facts survive worker and materialization failures.
use musteragent_content_ref::Sha256;
use musteragent_run::AttemptContext;
use musteragent_runtime_artifact::{
    MediaGenerationJournal, MediaJournalError, RecordedMediaResult,
};
use musteragent_runtime_capability::CapabilityInvocation;
use musteragent_runtime_contract::ApplicationFuture;
use sqlx::Row;
mod codec;

/// PostgreSQL append-only media result journal. Uses the same invocation and
/// execution fence as the final Artifact/Tool commit.
#[derive(Clone)]
pub struct PostgresMediaGenerationJournal {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresMediaGenerationJournal {
    /// Share the database and commit lane used by the Tool and Artifact owners.
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
}
fn binding(i: &CapabilityInvocation) -> Sha256 {
    Sha256::digest(format!(
        "media-result@1\0{}\0{}\0{:?}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
        i.tenant_id,
        i.actor.principal_id,
        i.actor.principal_kind,
        i.run_id,
        i.conversation_id,
        i.capability_provider_id,
        i.tool_key.as_str(),
        i.idempotency_key.as_str(),
        i.request_ref.content_id,
        i.request_ref.sha256,
        i.result_schema.schema_hash
    ))
}
async fn load_on(
    connection: &mut sqlx::PgConnection,
    i: &CapabilityInvocation,
) -> Result<Option<RecordedMediaResult>, MediaJournalError> {
    let row = sqlx::query("SELECT binding_sha256,source_json,observed_at_ms FROM artifact.media_generation_results WHERE tenant_id=$1 AND invocation_id=$2")
        .bind(i.tenant_id.into_uuid()).bind(i.invocation_id.into_uuid())
        .fetch_optional(connection).await.map_err(|_| MediaJournalError::Unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let hash: Vec<u8> = row
        .try_get("binding_sha256")
        .map_err(|_| MediaJournalError::Invalid)?;
    if hash != binding(i).into_bytes() {
        return Err(MediaJournalError::Conflict);
    }
    Ok(Some(RecordedMediaResult {
        source: codec::decode(
            &row.try_get::<Vec<u8>, _>("source_json")
                .map_err(|_| MediaJournalError::Invalid)?,
        )?,
        observed_at: musteragent_identity::UtcTimestamp::from_unix_millis(
            row.try_get("observed_at_ms")
                .map_err(|_| MediaJournalError::Invalid)?,
        ),
    }))
}
impl MediaGenerationJournal for PostgresMediaGenerationJournal {
    fn load<'a>(
        &'a self,
        invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<RecordedMediaResult>, MediaJournalError>> {
        Box::pin(async move {
            let mut connection = self
                .database
                .pool()
                .acquire()
                .await
                .map_err(|_| MediaJournalError::Unavailable)?;
            load_on(&mut connection, invocation).await
        })
    }
    fn record<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        i: &'a CapabilityInvocation,
        result: RecordedMediaResult,
    ) -> ApplicationFuture<'a, Result<RecordedMediaResult, MediaJournalError>> {
        Box::pin(async move {
            if attempt.run_id != i.run_id || i.actor.tenant_id != i.tenant_id {
                return Err(MediaJournalError::Invalid);
            }
            let encoded = codec::encode(&result.source)?;
            let mut tx = crate::commit_lane::acquire_for_run(&self.database, attempt.run_id)
                .await
                .map_err(|_| MediaJournalError::Unavailable)?
                .ok_or(MediaJournalError::StaleFence)?;
            if let Some(old) = load_on(&mut tx, i).await? {
                if old.source != result.source {
                    return Err(MediaJournalError::Conflict);
                }
                return Ok(old);
            }
            let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations i JOIN runtime.runs r ON r.run_id=i.run_id AND r.tenant_id=i.tenant_id JOIN runtime.leases l ON l.run_id=r.run_id AND l.tenant_id=r.tenant_id JOIN runtime.attempts a ON a.attempt_id=l.attempt_id AND a.run_id=r.run_id WHERE i.tenant_id=$1 AND i.invocation_id=$2 AND i.run_id=$3 AND i.capability_provider_id=$4 AND i.request_content_id=$5 AND i.request_hash=$6 AND i.status IN (3,6) AND i.erasure_job_id IS NULL AND i.tool_ref=$11 AND i.idempotency_key=$12 AND i.conversation_id=$13 AND i.execution_attempt_id=$7 AND a.status=1 AND l.attempt_id=$7 AND l.worker_id=$8 AND l.lease_id=$9 AND l.fencing_token=$10 AND l.expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint AND r.status IN (2,4) AND r.erasure_job_id IS NULL AND r.cancel_requested_at_ms IS NULL)")
                .bind(i.tenant_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(i.run_id.into_uuid())
                .bind(i.capability_provider_id.into_uuid()).bind(i.request_ref.content_id.into_uuid()).bind(i.request_ref.sha256.into_bytes().to_vec())
                .bind(attempt.attempt_id.into_uuid()).bind(attempt.worker_id.as_str()).bind(attempt.lease_id.into_uuid())
                .bind(i64::try_from(attempt.fencing_token.get()).map_err(|_| MediaJournalError::Invalid)?)
                .bind(i.tool_key.as_str()).bind(i.idempotency_key.as_str()).bind(i.conversation_id.into_uuid())
                .fetch_one(&mut *tx).await.map_err(|_| MediaJournalError::Unavailable)?;
            if !valid {
                return Err(MediaJournalError::StaleFence);
            }
            sqlx::query("INSERT INTO artifact.media_generation_results(tenant_id,invocation_id,binding_sha256,source_json,observed_at_ms) VALUES($1,$2,$3,$4,$5)")
                .bind(i.tenant_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(binding(i).into_bytes().to_vec())
                .bind(encoded).bind(result.observed_at.unix_millis()).execute(&mut *tx).await.map_err(|_| MediaJournalError::Unavailable)?;
            tx.commit()
                .await
                .map_err(|_| MediaJournalError::Unavailable)?;
            Ok(result)
        })
    }
}
