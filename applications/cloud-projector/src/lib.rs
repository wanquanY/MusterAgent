//! Idempotent dispatch and evolution outbox consumers. No billing or product RPC.
use musteragent_identity::{DottedKey, RunId};
use musteragent_projection::{
    ProjectionDisposition, ProjectionError, ProjectionHandler, ProjectionSubscription,
    ProjectionWork,
};
use musteragent_runtime_contract::ApplicationFuture;
use serde::Deserialize;
use sqlx::{PgPool, Row};
const DISPATCH_TOPIC: &str = "run.dispatch";
const LEARNING_RUN_EXTRACTED_TOPIC: &str = "learning.run.extracted.v1";
const LEARNING_DREAM_COMPLETED_TOPIC: &str = "learning.dream.completed.v1";
const DISPATCH_PROJECTION_KEY: &str = "scheduler-dispatch@1";
const LEARNING_RUN_PROJECTION_KEY: &str = "learning-run-extracted@1";
const LEARNING_DREAM_PROJECTION_KEY: &str = "learning-dream-completed@1";
/// Closed set of kernel outbox topics consumed by this process.
pub fn subscription()
-> Result<ProjectionSubscription, musteragent_projection::ProjectionPolicyError> {
    ProjectionSubscription::new(
        [
            DISPATCH_TOPIC,
            LEARNING_RUN_EXTRACTED_TOPIC,
            LEARNING_DREAM_COMPLETED_TOPIC,
        ]
        .iter()
        .map(|topic| DottedKey::new(*topic).expect("static topic"))
        .collect(),
    )
}
/// Idempotent kernel projection receipt validator backed by the original database.
pub struct CloudProjectionHandler {
    pool: PgPool,
}
impl CloudProjectionHandler {
    /// Use the same database as the outbox lease owner.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    async fn apply_dispatch(
        &self,
        work: &ProjectionWork,
    ) -> Result<ProjectionDisposition, ProjectionError> {
        if let Some(disposition) = self.existing_receipt(DISPATCH_PROJECTION_KEY, work).await? {
            return Ok(disposition);
        }
        let run_id = parse_dispatch_run_id(&work.payload)?;
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM runtime.runs
                 WHERE tenant_id = $1
                   AND conversation_id = $2
                   AND run_id = $3
             )",
        )
        .bind(work.tenant_id.into_uuid())
        .bind(work.conversation_id.into_uuid())
        .bind(run_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if !exists {
            return Err(ProjectionError::CorruptPayload);
        }
        self.record_receipt(DISPATCH_PROJECTION_KEY, work).await
    }
    async fn apply_learning_run_extracted(
        &self,
        work: &ProjectionWork,
    ) -> Result<ProjectionDisposition, ProjectionError> {
        if let Some(disposition) = self
            .existing_receipt(LEARNING_RUN_PROJECTION_KEY, work)
            .await?
        {
            return Ok(disposition);
        }
        let payload: LearningRunExtractedPayload =
            serde_json::from_slice(&work.payload).map_err(|_| ProjectionError::CorruptPayload)?;
        if payload.schema != "musteragent.legacy.learning.run-extracted.v1" {
            return Err(ProjectionError::CorruptPayload);
        }
        let run_id = parse_uuid(&payload.run_id)?;
        let expected_signal_ids = payload
            .signal_ids
            .iter()
            .map(|value| parse_uuid(value))
            .collect::<Result<Vec<_>, _>>()?;
        let row = sqlx::query(
            "SELECT tenant_id, conversation_id, ledger_watermark::text AS ledger_watermark, status
             FROM evolution.learning_run_queue
             WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?
        .ok_or(ProjectionError::CorruptPayload)?;
        let tenant_id: uuid::Uuid = row
            .try_get("tenant_id")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let conversation_id: uuid::Uuid = row
            .try_get("conversation_id")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let ledger_watermark = row
            .try_get::<String, _>("ledger_watermark")
            .map_err(|_| ProjectionError::CorruptPayload)?
            .parse::<u64>()
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let status: i16 = row
            .try_get("status")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let signal_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT signal_id
             FROM evolution.learning_run_signals
             WHERE run_id = $1
             ORDER BY signal_ordinal",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if tenant_id != work.tenant_id.into_uuid()
            || conversation_id != work.conversation_id.into_uuid()
            || ledger_watermark != payload.ledger_watermark
            || status != 3
            || signal_ids != expected_signal_ids
        {
            return Err(ProjectionError::CorruptPayload);
        }
        self.record_receipt(LEARNING_RUN_PROJECTION_KEY, work).await
    }
    async fn apply_learning_dream_completed(
        &self,
        work: &ProjectionWork,
    ) -> Result<ProjectionDisposition, ProjectionError> {
        if let Some(disposition) = self
            .existing_receipt(LEARNING_DREAM_PROJECTION_KEY, work)
            .await?
        {
            return Ok(disposition);
        }
        let payload: LearningDreamCompletedPayload =
            serde_json::from_slice(&work.payload).map_err(|_| ProjectionError::CorruptPayload)?;
        if payload.schema != "musteragent.legacy.learning.dream-completed.v1" {
            return Err(ProjectionError::CorruptPayload);
        }
        let dream_job_id = parse_uuid(&payload.dream_job_id)?;
        let expected_proposal_ids = payload
            .proposal_ids
            .iter()
            .map(|value| parse_uuid(value))
            .collect::<Result<Vec<_>, _>>()?;
        let row = sqlx::query(
            "SELECT job.tenant_id, job.status, evidence.conversation_id
             FROM evolution.dream_jobs AS job
             JOIN evolution.dream_job_signals AS signal
               ON signal.dream_job_id = job.dream_job_id
              AND signal.signal_ordinal = 0
             JOIN evolution.learning_signal_evidence AS evidence
               ON evidence.signal_id = signal.signal_id
              AND evidence.span_ordinal = 0
             WHERE job.dream_job_id = $1",
        )
        .bind(dream_job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?
        .ok_or(ProjectionError::CorruptPayload)?;
        let tenant_id: uuid::Uuid = row
            .try_get("tenant_id")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let conversation_id: uuid::Uuid = row
            .try_get("conversation_id")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let status: i16 = row
            .try_get("status")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let proposal_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT proposal_id
             FROM evolution.dream_job_candidates
             WHERE dream_job_id = $1
             ORDER BY proposal_ordinal",
        )
        .bind(dream_job_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if tenant_id != work.tenant_id.into_uuid()
            || conversation_id != work.conversation_id.into_uuid()
            || status != 3
            || proposal_ids != expected_proposal_ids
        {
            return Err(ProjectionError::CorruptPayload);
        }
        self.record_receipt(LEARNING_DREAM_PROJECTION_KEY, work)
            .await
    }
    async fn existing_receipt(
        &self,
        projection_key: &str,
        work: &ProjectionWork,
    ) -> Result<Option<ProjectionDisposition>, ProjectionError> {
        let row = sqlx::query(
            "SELECT tenant_id, conversation_id, payload_hash
             FROM projection.delivery_receipts
             WHERE projection_key = $1
               AND message_id = $2",
        )
        .bind(projection_key)
        .bind(work.message_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let tenant_id: uuid::Uuid = row
            .try_get("tenant_id")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let conversation_id: uuid::Uuid = row
            .try_get("conversation_id")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        let payload_hash: Vec<u8> = row
            .try_get("payload_hash")
            .map_err(|_| ProjectionError::CorruptPayload)?;
        if tenant_id != work.tenant_id.into_uuid()
            || conversation_id != work.conversation_id.into_uuid()
            || payload_hash != work.payload_hash.into_bytes()
        {
            return Err(ProjectionError::CorruptPayload);
        }
        Ok(Some(ProjectionDisposition::Duplicate))
    }
    async fn record_receipt(
        &self,
        projection_key: &str,
        work: &ProjectionWork,
    ) -> Result<ProjectionDisposition, ProjectionError> {
        let result = sqlx::query(
            "INSERT INTO projection.delivery_receipts (
                 projection_key,
                 message_id,
                 tenant_id,
                 conversation_id,
                 payload_hash,
                 applied_at_ms
             )
             VALUES (
                 $1, $2, $3, $4, $5,
                 floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
             )
             ON CONFLICT (projection_key, message_id) DO NOTHING",
        )
        .bind(projection_key)
        .bind(work.message_id.into_uuid())
        .bind(work.tenant_id.into_uuid())
        .bind(work.conversation_id.into_uuid())
        .bind(work.payload_hash.into_bytes().to_vec())
        .execute(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if result.rows_affected() == 1 {
            Ok(ProjectionDisposition::Applied)
        } else {
            self.existing_receipt(projection_key, work)
                .await?
                .ok_or(ProjectionError::DependencyUnavailable)
        }
    }
}
impl ProjectionHandler for CloudProjectionHandler {
    fn apply(
        &self,
        work: &ProjectionWork,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>> {
        let work = work.clone();
        Box::pin(async move {
            match work.topic.as_str() {
                DISPATCH_TOPIC => self.apply_dispatch(&work).await,
                LEARNING_RUN_EXTRACTED_TOPIC => self.apply_learning_run_extracted(&work).await,
                LEARNING_DREAM_COMPLETED_TOPIC => self.apply_learning_dream_completed(&work).await,
                _ => Err(ProjectionError::UnsupportedTopic),
            }
        })
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LearningRunExtractedPayload {
    schema: String,
    run_id: String,
    ledger_watermark: u64,
    signal_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LearningDreamCompletedPayload {
    schema: String,
    dream_job_id: String,
    proposal_ids: Vec<String>,
}

fn parse_uuid(value: &str) -> Result<uuid::Uuid, ProjectionError> {
    uuid::Uuid::parse_str(value).map_err(|_| ProjectionError::CorruptPayload)
}

fn parse_dispatch_run_id(payload: &[u8]) -> Result<RunId, ProjectionError> {
    let value = std::str::from_utf8(payload).map_err(|_| ProjectionError::CorruptPayload)?;
    let value = value
        .strip_prefix("run:")
        .ok_or(ProjectionError::CorruptPayload)?;
    let parsed = uuid::Uuid::parse_str(value).map_err(|_| ProjectionError::CorruptPayload)?;
    RunId::from_uuid(parsed).map_err(|_| ProjectionError::CorruptPayload)
}
