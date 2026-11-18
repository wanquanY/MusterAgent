mod content;
pub(crate) mod proposals;
use proposals::load_proposals;

use std::collections::BTreeSet;
use std::str::FromStr;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{LedgerRecordId, MemoryVersionId, RunId, UtcTimestamp};
use musteragent_runtime_memory::{
    AgentEvolutionInspection, AgentEvolutionInspectionQuery, AgentEvolutionInspectionStore,
    AgentEvolutionMetrics, AgentGrowthActivityBucket, AgentGrowthSummary, DreamRecordCursor,
    DreamRecordHealth, DreamRecordInspection, DreamRecordState, DreamScheduleInspection,
    LearningGraphNode, LearningGraphNodeKind, LearningGraphNodeState, MemorySearchHit,
    MemorySearchIndexBackfillStore,
};
use sqlx::{PgPool, Row as _};
use uuid::Uuid;

use crate::memory::{
    PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore, decode_memory_hit,
};

impl AgentEvolutionInspectionStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn authorize_growth_content(
        &self,
        query: musteragent_runtime_memory::AgentGrowthContentQuery,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<bool, Self::Error>> {
        Box::pin(async move { content::authorize(&self.pool, query).await })
    }

    fn inspect_agent(
        &self,
        query: AgentEvolutionInspectionQuery,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<AgentEvolutionInspection, Self::Error>,
    > {
        Box::pin(async move { inspect_agent(self, query).await })
    }
}

async fn inspect_agent(
    store: &PostgresMemoryEvolutionStore,
    query: AgentEvolutionInspectionQuery,
) -> Result<AgentEvolutionInspection, PostgresMemoryEvolutionError> {
    query
        .validate()
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
    let proposals = load_proposals(store, &query).await?;
    let active_memories = load_memories(&store.pool, &query).await?;
    let metrics = load_metrics(&store.pool, &query).await?;
    let learning_graph = load_learning_graph(&store.pool, &query).await?;
    let growth_summary = load_growth_summary(&store.pool, &query).await?;
    let growth_activity = load_growth_activity(&store.pool, &query).await?;
    let (dream_records, dream_records_next_cursor) =
        load_dream_records(&store.pool, &query).await?;
    let dream_schedule = load_dream_schedule(&store.pool, &query).await?;
    let search_backfill = MemorySearchIndexBackfillStore::backfill_status(store).await?;
    let inspection = AgentEvolutionInspection {
        tenant_id: query.tenant_id,
        agent_id: query.agent_id,
        observed_at: query.observed_at,
        metrics,
        proposals,
        active_memories,
        search_backfill,
        learning_graph,
        growth_summary,
        growth_activity,
        dream_records,
        dream_records_next_cursor,
        dream_schedule,
    };
    inspection
        .validate_for(&query)
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
    Ok(inspection)
}

async fn load_growth_activity(
    pool: &PgPool,
    query: &AgentEvolutionInspectionQuery,
) -> Result<Vec<AgentGrowthActivityBucket>, PostgresMemoryEvolutionError> {
    const DAY_MS: i64 = 86_400_000;
    const BUCKET_COUNT: usize = 14;
    let activity_start = query.observed_at.unix_millis().saturating_sub(
        i64::try_from(BUCKET_COUNT)
            .unwrap_or(i64::MAX)
            .saturating_mul(DAY_MS),
    );
    let rows = sqlx::query(
        "SELECT ((activity.occurred_at_ms-$3)/86400000) AS bucket_index,
                count(*) AS successful_uses
         FROM (
           SELECT ref.selected_at_ms AS occurred_at_ms
           FROM memory.memory_refs AS ref
           JOIN memory.memory_versions AS version
             ON version.memory_version_id=ref.memory_version_id
           JOIN memory.memories AS memory ON memory.memory_id=version.memory_id
           JOIN runtime.runs AS run ON run.tenant_id=ref.tenant_id AND run.run_id=ref.run_id
           WHERE memory.tenant_id=$1 AND memory.scope_kind=4 AND memory.scope_id=$2
             AND ref.selected_at_ms >= $3 AND ref.selected_at_ms < $4 AND run.status=5
           UNION ALL
           SELECT ref.selected_at_ms AS occurred_at_ms
           FROM evolution.instinct_refs AS ref
           JOIN evolution.instincts AS instinct
             ON instinct.tenant_id=ref.tenant_id
            AND instinct.target_agent_id=ref.target_agent_id
            AND instinct.instinct_id=ref.instinct_id
           JOIN runtime.runs AS run ON run.tenant_id=ref.tenant_id AND run.run_id=ref.run_id
           WHERE instinct.tenant_id=$1 AND instinct.target_agent_id=$2
             AND ref.selected_at_ms >= $3 AND ref.selected_at_ms < $4 AND run.status=5
         ) AS activity
         GROUP BY bucket_index ORDER BY bucket_index",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(activity_start)
    .bind(query.observed_at.unix_millis())
    .fetch_all(pool)
    .await?;
    let mut buckets = (0..BUCKET_COUNT)
        .map(|index| {
            let started_at = activity_start.saturating_add(
                i64::try_from(index)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(DAY_MS),
            );
            AgentGrowthActivityBucket {
                window_started_at: UtcTimestamp::from_unix_millis(started_at),
                window_ended_at: UtcTimestamp::from_unix_millis(started_at.saturating_add(DAY_MS)),
                successful_uses: 0,
            }
        })
        .collect::<Vec<_>>();
    for row in rows {
        let index = usize::try_from(row.try_get::<i64, _>("bucket_index")?)
            .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?;
        let Some(bucket) = buckets.get_mut(index) else {
            return Err(PostgresMemoryEvolutionError::CorruptDatabase);
        };
        bucket.successful_uses = u64::try_from(row.try_get::<i64, _>("successful_uses")?)
            .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?;
    }
    Ok(buckets)
}

async fn load_dream_schedule(
    pool: &PgPool,
    query: &AgentEvolutionInspectionQuery,
) -> Result<DreamScheduleInspection, PostgresMemoryEvolutionError> {
    let pending = sqlx::query(
        "WITH pending_signal AS (
             SELECT signal.signal_id,signal.created_at_ms,evidence.run_id
             FROM evolution.learning_signals AS signal
             JOIN evolution.learning_signal_evidence AS evidence
               ON evidence.signal_id=signal.signal_id AND evidence.span_ordinal=0
             JOIN runtime.runs AS run ON run.run_id=evidence.run_id
             WHERE signal.tenant_id=$1 AND signal.target_agent_id=$2
               AND signal.created_at_ms <= $3
               AND NOT EXISTS (
                 SELECT 1 FROM evolution.dream_job_signals AS assigned
                 WHERE assigned.signal_id=signal.signal_id
               )
               AND NOT EXISTS (
                 SELECT 1 FROM evolution.learning_signal_exclusions AS excluded
                 WHERE excluded.signal_id=signal.signal_id
               )
               AND NOT EXISTS (
                 SELECT 1
                 FROM evolution.learning_signal_evidence AS other_evidence
                 JOIN runtime.runs AS other_run ON other_run.run_id=other_evidence.run_id
                 WHERE other_evidence.signal_id=signal.signal_id
                   AND other_run.activity_kind NOT BETWEEN 1 AND 4
               )
               AND run.activity_kind BETWEEN 1 AND 4
             ORDER BY signal.created_at_ms ASC, signal.signal_id ASC
             LIMIT 24
         )
         SELECT count(DISTINCT signal_id) AS signal_count,
                count(DISTINCT run_id) AS source_run_count,
                min(created_at_ms) AS oldest_at_ms,
                max(created_at_ms) AS newest_at_ms
         FROM pending_signal",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(query.observed_at.unix_millis())
    .fetch_one(pool)
    .await?;
    let active_ordinary_run: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM runtime.runs AS run
             JOIN conversation.participants AS participant
               ON participant.tenant_id=run.tenant_id
              AND participant.conversation_id=run.conversation_id
              AND participant.participant_id=run.executor_participant_id
             WHERE run.tenant_id=$1 AND participant.agent_ref=$2
               AND run.activity_kind BETWEEN 1 AND 4
               AND run.status BETWEEN 1 AND 4
         )",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .fetch_one(pool)
    .await?;
    let active_dream_job: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM evolution.dream_jobs
             WHERE tenant_id=$1 AND target_agent_id=$2 AND status IN (1,2)
         )",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .fetch_one(pool)
    .await?;
    let last_dream_job_created_at = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT max(created_at_ms)
         FROM evolution.dream_jobs
         WHERE tenant_id=$1 AND target_agent_id=$2 AND created_at_ms <= $3",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(query.observed_at.unix_millis())
    .fetch_one(pool)
    .await?
    .map(UtcTimestamp::from_unix_millis);
    let quota_consumed_at = sqlx::query_scalar::<_, i64>(
        "SELECT job.created_at_ms
         FROM evolution.dream_jobs AS job
         WHERE job.tenant_id=$1 AND job.target_agent_id=$2
           AND job.created_at_ms BETWEEN $3 AND $4
           AND job.manual_request_generation=0
           AND (
             job.status=3 OR EXISTS (
               SELECT 1
               FROM runtime.evolution_maintenance_executions AS execution
               JOIN runtime.model_invocations AS invocation
                 ON invocation.invocation_id=execution.invocation_id
               WHERE execution.dream_job_id=job.dream_job_id
                 AND invocation.provider_dispatched_at_ms IS NOT NULL
             )
           )
         ORDER BY job.created_at_ms,job.dream_job_id",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(query.observed_at.unix_millis().saturating_sub(86_400_000))
    .bind(query.observed_at.unix_millis())
    .fetch_all(pool)
    .await?;
    Ok(DreamScheduleInspection {
        pending_signal_count: u64::try_from(pending.try_get::<i64, _>("signal_count")?)
            .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
        pending_source_run_count: u64::try_from(pending.try_get::<i64, _>("source_run_count")?)
            .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
        oldest_pending_signal_at: pending
            .try_get::<Option<i64>, _>("oldest_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
        newest_pending_signal_at: pending
            .try_get::<Option<i64>, _>("newest_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
        active_ordinary_run,
        active_dream_job,
        last_dream_job_created_at,
        automatic_quota_consumed_at: quota_consumed_at
            .into_iter()
            .map(UtcTimestamp::from_unix_millis)
            .collect(),
    })
}

async fn load_dream_records(
    pool: &PgPool,
    query: &AgentEvolutionInspectionQuery,
) -> Result<(Vec<DreamRecordInspection>, Option<DreamRecordCursor>), PostgresMemoryEvolutionError> {
    let cursor_created_at = query
        .dream_cursor
        .map(|value| value.created_at.unix_millis());
    let cursor_id = query
        .dream_cursor
        .map(|value| value.dream_job_id.into_uuid());
    let mut rows = sqlx::query(
        "SELECT dream_job_id,status,retry_count,available_at_ms,created_at_ms,completed_at_ms,
                manual_request_generation::text AS manual_request_generation
         FROM evolution.dream_jobs
         WHERE tenant_id=$1 AND target_agent_id=$2 AND created_at_ms <= $3
           AND ($4::bigint IS NULL OR created_at_ms >= $4)
           AND ($5::bigint IS NULL OR created_at_ms < $5
                OR (created_at_ms = $5 AND dream_job_id > $6::uuid))
         ORDER BY created_at_ms DESC,dream_job_id
         LIMIT $7",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(query.observed_at.unix_millis())
    .bind(query.dream_since.map(|value| value.unix_millis()))
    .bind(cursor_created_at)
    .bind(cursor_id)
    .bind(i64::from(query.dream_limit.get()) + 1)
    .fetch_all(pool)
    .await?;
    let has_more = rows.len() > usize::from(query.dream_limit.get());
    rows.truncate(usize::from(query.dream_limit.get()));
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let dream_job_id: Uuid = row.try_get("dream_job_id")?;
        let source_run_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT DISTINCT evidence.run_id
             FROM evolution.dream_job_signals AS link
             JOIN evolution.learning_signal_evidence AS evidence
               ON evidence.signal_id=link.signal_id
             WHERE link.dream_job_id=$1 ORDER BY evidence.run_id",
        )
        .bind(dream_job_id)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(parse_uuid)
        .collect::<Result<Vec<_>, _>>()?;
        let proposal_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT proposal_id FROM evolution.dream_job_candidates
             WHERE dream_job_id=$1 ORDER BY proposal_ordinal",
        )
        .bind(dream_job_id)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(parse_uuid)
        .collect::<Result<Vec<_>, _>>()?;
        let state = match row.try_get::<i16, _>("status")? {
            1 => DreamRecordState::Pending,
            2 => DreamRecordState::Running,
            3 => DreamRecordState::Completed,
            4 => DreamRecordState::Failed,
            5 => DreamRecordState::Cancelled,
            _ => return Err(PostgresMemoryEvolutionError::CorruptDatabase),
        };
        let retry_count = u16::try_from(row.try_get::<i32, _>("retry_count")?)
            .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?;
        let next_action_at = if state == DreamRecordState::Pending {
            Some(musteragent_identity::UtcTimestamp::from_unix_millis(
                row.try_get("available_at_ms")?,
            ))
        } else {
            None
        };
        records.push(DreamRecordInspection {
            dream_job_id: parse_uuid(dream_job_id)?,
            state,
            health: DreamRecordHealth::from_state(state, retry_count),
            retry_count,
            source_run_ids,
            proposal_ids,
            created_at: musteragent_identity::UtcTimestamp::from_unix_millis(
                row.try_get("created_at_ms")?,
            ),
            completed_at: row
                .try_get::<Option<i64>, _>("completed_at_ms")?
                .map(musteragent_identity::UtcTimestamp::from_unix_millis),
            next_action_at,
            manual_request_generation: row
                .try_get::<String, _>("manual_request_generation")?
                .parse()
                .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
        });
    }
    let next_cursor = has_more.then(|| {
        let last = records.last().expect("a page with more rows is non-empty");
        DreamRecordCursor {
            created_at: last.created_at,
            dream_job_id: last.dream_job_id,
        }
    });
    Ok((records, next_cursor))
}

async fn load_learning_graph(
    pool: &PgPool,
    query: &AgentEvolutionInspectionQuery,
) -> Result<Vec<LearningGraphNode>, PostgresMemoryEvolutionError> {
    let since = query
        .growth_since
        .map_or(i64::MIN, |value| value.unix_millis());
    let rows = sqlx::query(
        "SELECT memory.memory_id,version.memory_version_id,
                content.content_id,content.media_type,
                content.byte_length::text AS byte_length,content.content_hash,
                promotion.proposal_id,version.valid_from_ms AS created_at_ms,
                count(ref.run_id) AS uses,
                count(ref.run_id) FILTER (WHERE run.status=5) AS successes,
                count(ref.run_id) FILTER (WHERE run.status=6) AS failures,
                max(ref.selected_at_ms) AS last_used_at_ms
         FROM memory.memories AS memory
         JOIN memory.memory_versions AS version
           ON version.memory_version_id=memory.current_version_id
          AND version.memory_id=memory.memory_id
         JOIN artifact.content_refs AS content
           ON content.content_id=version.content_id
          AND content.tenant_id=memory.tenant_id
          AND content.content_hash=version.content_hash
         JOIN evolution.promotions AS promotion
           ON promotion.published_memory_version_id=version.memory_version_id
         LEFT JOIN memory.memory_refs AS ref
           ON ref.tenant_id=memory.tenant_id
          AND ref.memory_version_id=version.memory_version_id
          AND ref.selected_at_ms BETWEEN $1 AND $2
         LEFT JOIN runtime.runs AS run ON run.run_id=ref.run_id
         WHERE memory.tenant_id=$3 AND memory.scope_kind=4 AND memory.scope_id=$4
           AND memory.lifecycle=1 AND version.valid_from_ms <= $2
         GROUP BY memory.memory_id,version.memory_version_id,
                  content.content_id,content.media_type,content.byte_length,content.content_hash,
                  promotion.proposal_id,version.valid_from_ms
         ORDER BY last_used_at_ms DESC NULLS LAST,version.valid_from_ms DESC,
                  version.memory_version_id
         LIMIT $5",
    )
    .bind(since)
    .bind(query.observed_at.unix_millis())
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(i64::from(query.graph_limit.get()))
    .fetch_all(pool)
    .await?;
    let mut graph = rows
        .into_iter()
        .map(|row| {
            let memory_version_id: MemoryVersionId = parse_uuid(row.try_get("memory_version_id")?)?;
            Ok(LearningGraphNode {
                node_id: format!("experience:{memory_version_id}"),
                kind: LearningGraphNodeKind::Experience,
                state: LearningGraphNodeState::Active,
                payload_ref: ContentRef {
                    content_id: parse_uuid(row.try_get("content_id")?)?,
                    media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                    byte_length: row
                        .try_get::<String, _>("byte_length")?
                        .parse()
                        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                    sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                },
                proposal_id: Some(parse_uuid(row.try_get("proposal_id")?)?),
                memory_id: Some(parse_uuid(row.try_get("memory_id")?)?),
                memory_version_id: Some(memory_version_id),
                skill_id: None,
                instinct_id: None,
                studio_revision_draft_id: None,
                uses: u64::try_from(row.try_get::<i64, _>("uses")?)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
                successes: u64::try_from(row.try_get::<i64, _>("successes")?)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
                failures: u64::try_from(row.try_get::<i64, _>("failures")?)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
                last_used_at: row
                    .try_get::<Option<i64>, _>("last_used_at_ms")?
                    .map(musteragent_identity::UtcTimestamp::from_unix_millis),
                learned_at: musteragent_identity::UtcTimestamp::from_unix_millis(
                    row.try_get("created_at_ms")?,
                ),
                claim_detail: None,
            })
        })
        .collect::<Result<Vec<_>, PostgresMemoryEvolutionError>>()?;
    let instinct_rows = sqlx::query(
        "SELECT instinct.instinct_id,instinct.proposal_id,instinct.lifecycle,
                instinct.payload_content_id AS content_id,
                content.media_type,content.byte_length::text AS byte_length,
                instinct.payload_content_hash AS content_hash,
                instinct.created_at_ms,
                count(ref.run_id) AS uses,
                count(ref.run_id) FILTER (WHERE run.status=5) AS successes,
                count(ref.run_id) FILTER (WHERE run.status=6) AS failures,
                max(ref.selected_at_ms) AS last_used_at_ms
         FROM evolution.instincts AS instinct
         JOIN artifact.content_refs AS content
           ON content.tenant_id=instinct.tenant_id
          AND content.content_id=instinct.payload_content_id
          AND content.content_hash=instinct.payload_content_hash
         LEFT JOIN evolution.instinct_refs AS ref
           ON ref.tenant_id=instinct.tenant_id
          AND ref.target_agent_id=instinct.target_agent_id
          AND ref.instinct_id=instinct.instinct_id
          AND ref.selected_at_ms BETWEEN $1 AND $2
         LEFT JOIN runtime.runs AS run
           ON run.tenant_id=ref.tenant_id AND run.run_id=ref.run_id
         WHERE instinct.tenant_id=$3 AND instinct.target_agent_id=$4
           AND instinct.created_at_ms <= $2
         GROUP BY instinct.instinct_id,instinct.proposal_id,instinct.lifecycle,
                  instinct.payload_content_id,content.media_type,content.byte_length,
                  instinct.payload_content_hash,instinct.created_at_ms
         ORDER BY last_used_at_ms DESC NULLS LAST,instinct.created_at_ms DESC,
                  instinct.instinct_id
         LIMIT $5",
    )
    .bind(since)
    .bind(query.observed_at.unix_millis())
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(i64::from(query.graph_limit.get()))
    .fetch_all(pool)
    .await?;
    for row in instinct_rows {
        let instinct_id = parse_uuid(row.try_get("instinct_id")?)?;
        graph.push(LearningGraphNode {
            node_id: format!("instinct:{instinct_id}"),
            kind: LearningGraphNodeKind::Instinct,
            state: match row.try_get::<i16, _>("lifecycle")? {
                1 => LearningGraphNodeState::Active,
                2 => LearningGraphNodeState::Disabled,
                _ => return Err(PostgresMemoryEvolutionError::CorruptDatabase),
            },
            payload_ref: ContentRef {
                content_id: parse_uuid(row.try_get("content_id")?)?,
                media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                byte_length: row
                    .try_get::<String, _>("byte_length")?
                    .parse()
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            },
            proposal_id: Some(parse_uuid(row.try_get("proposal_id")?)?),
            memory_id: None,
            memory_version_id: None,
            skill_id: None,
            instinct_id: Some(instinct_id),
            studio_revision_draft_id: None,
            uses: u64::try_from(row.try_get::<i64, _>("uses")?)
                .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
            successes: u64::try_from(row.try_get::<i64, _>("successes")?)
                .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
            failures: u64::try_from(row.try_get::<i64, _>("failures")?)
                .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
            last_used_at: row
                .try_get::<Option<i64>, _>("last_used_at_ms")?
                .map(musteragent_identity::UtcTimestamp::from_unix_millis),
            learned_at: musteragent_identity::UtcTimestamp::from_unix_millis(
                row.try_get("created_at_ms")?,
            ),
            claim_detail: None,
        });
    }
    let studio_rows = sqlx::query(
        "SELECT draft.studio_revision_draft_id,draft.proposal_id,
                draft.payload_content_id AS content_id,content.media_type,
                content.byte_length::text AS byte_length,
                draft.payload_content_hash AS content_hash,draft.created_at_ms
         FROM evolution.studio_revision_drafts AS draft
         JOIN artifact.content_refs AS content
           ON content.tenant_id=draft.tenant_id
          AND content.content_id=draft.payload_content_id
          AND content.content_hash=draft.payload_content_hash
         WHERE draft.tenant_id=$1 AND draft.target_agent_id=$2
           AND draft.artifact_kind=1 AND draft.created_at_ms <= $3
         ORDER BY draft.created_at_ms DESC,draft.studio_revision_draft_id
         LIMIT $4",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(query.observed_at.unix_millis())
    .bind(i64::from(query.graph_limit.get()))
    .fetch_all(pool)
    .await?;
    for row in studio_rows {
        let draft_id = parse_uuid(row.try_get("studio_revision_draft_id")?)?;
        graph.push(LearningGraphNode {
            node_id: format!("skill-draft:{draft_id}"),
            kind: LearningGraphNodeKind::Skill,
            state: LearningGraphNodeState::Candidate,
            payload_ref: ContentRef {
                content_id: parse_uuid(row.try_get("content_id")?)?,
                media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                byte_length: row
                    .try_get::<String, _>("byte_length")?
                    .parse()
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                    .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
            },
            proposal_id: Some(parse_uuid(row.try_get("proposal_id")?)?),
            memory_id: None,
            memory_version_id: None,
            skill_id: None,
            instinct_id: None,
            studio_revision_draft_id: Some(draft_id),
            uses: 0,
            successes: 0,
            failures: 0,
            last_used_at: None,
            learned_at: musteragent_identity::UtcTimestamp::from_unix_millis(
                row.try_get("created_at_ms")?,
            ),
            claim_detail: None,
        });
    }
    graph.sort_by(|left, right| {
        right
            .last_used_at
            .cmp(&left.last_used_at)
            .then_with(|| right.learned_at.cmp(&left.learned_at))
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    graph.truncate(usize::from(query.graph_limit.get()));
    Ok(graph)
}

async fn load_growth_summary(
    pool: &PgPool,
    query: &AgentEvolutionInspectionQuery,
) -> Result<AgentGrowthSummary, PostgresMemoryEvolutionError> {
    let since = query
        .growth_since
        .map_or(i64::MIN, |value| value.unix_millis());
    let proposal = sqlx::query(
        "SELECT count(*) AS proposed,
                count(*) FILTER (WHERE status=5) AS published,
                count(*) FILTER (WHERE status=4) AS rejected
         FROM evolution.proposals
         WHERE tenant_id=$1 AND scope_kind=2 AND scope_id=$2
           AND proposed_at_ms BETWEEN $3 AND $4",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(since)
    .bind(query.observed_at.unix_millis())
    .fetch_one(pool)
    .await?;
    let telemetry = sqlx::query(
        "SELECT count(DISTINCT memory.memory_id) FILTER (WHERE memory.lifecycle=1) AS active_nodes,
                count(ref.run_id) AS uses,
                count(ref.run_id) FILTER (WHERE run.status=5) AS successes,
                count(ref.run_id) FILTER (WHERE run.status=6) AS failures,
                max(version.valid_from_ms) AS last_learned_at_ms
         FROM memory.memories AS memory
         JOIN memory.memory_versions AS version ON version.memory_id=memory.memory_id
         JOIN evolution.promotions AS promotion
           ON promotion.published_memory_version_id=version.memory_version_id
         LEFT JOIN memory.memory_refs AS ref
           ON ref.tenant_id=memory.tenant_id
          AND ref.memory_version_id=version.memory_version_id
          AND ref.selected_at_ms BETWEEN $1 AND $2
         LEFT JOIN runtime.runs AS run ON run.run_id=ref.run_id
         WHERE memory.tenant_id=$3 AND memory.scope_kind=4 AND memory.scope_id=$4
           AND version.valid_from_ms <= $2",
    )
    .bind(since)
    .bind(query.observed_at.unix_millis())
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .fetch_one(pool)
    .await?;
    let instinct_telemetry = sqlx::query(
        "SELECT count(*) FILTER (WHERE instinct.lifecycle=1) AS active_nodes,
                count(ref.run_id) AS uses,
                count(ref.run_id) FILTER (WHERE run.status=5) AS successes,
                count(ref.run_id) FILTER (WHERE run.status=6) AS failures,
                max(instinct.created_at_ms) AS last_learned_at_ms
         FROM evolution.instincts AS instinct
         LEFT JOIN evolution.instinct_refs AS ref
           ON ref.tenant_id=instinct.tenant_id
          AND ref.target_agent_id=instinct.target_agent_id
          AND ref.instinct_id=instinct.instinct_id
          AND ref.selected_at_ms BETWEEN $1 AND $2
         LEFT JOIN runtime.runs AS run
           ON run.tenant_id=ref.tenant_id AND run.run_id=ref.run_id
         WHERE instinct.tenant_id=$3 AND instinct.target_agent_id=$4
           AND instinct.created_at_ms <= $2",
    )
    .bind(since)
    .bind(query.observed_at.unix_millis())
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .fetch_one(pool)
    .await?;
    let range =
        |value: i64| u64::try_from(value).map_err(|_| PostgresMemoryEvolutionError::NumericRange);
    let successes = range(telemetry.try_get::<i64, _>("successes")?)?
        .saturating_add(range(instinct_telemetry.try_get("successes")?)?);
    let failures = range(telemetry.try_get::<i64, _>("failures")?)?
        .saturating_add(range(instinct_telemetry.try_get("failures")?)?);
    let terminal = successes.saturating_add(failures);
    Ok(AgentGrowthSummary {
        proposed: range(proposal.try_get("proposed")?)?,
        published: range(proposal.try_get("published")?)?,
        rejected: range(proposal.try_get("rejected")?)?,
        active_nodes: range(telemetry.try_get::<i64, _>("active_nodes")?)?
            .saturating_add(range(instinct_telemetry.try_get("active_nodes")?)?),
        uses: range(telemetry.try_get::<i64, _>("uses")?)?
            .saturating_add(range(instinct_telemetry.try_get("uses")?)?),
        successes,
        failures,
        success_rate_bps: if terminal == 0 {
            0
        } else {
            u16::try_from(successes.saturating_mul(10_000) / terminal)
                .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?
        },
        last_learned_at: telemetry
            .try_get::<Option<i64>, _>("last_learned_at_ms")?
            .into_iter()
            .chain(
                instinct_telemetry
                    .try_get::<Option<i64>, _>("last_learned_at_ms")?
                    .into_iter(),
            )
            .max()
            .map(musteragent_identity::UtcTimestamp::from_unix_millis),
    })
}

async fn load_memories(
    pool: &PgPool,
    query: &AgentEvolutionInspectionQuery,
) -> Result<Vec<MemorySearchHit>, PostgresMemoryEvolutionError> {
    let rows = sqlx::query(
        "SELECT memory.memory_id,memory.tenant_id,memory.scope_kind,memory.scope_id,
                memory.memory_key,memory.kind,memory.lifecycle,memory.current_version_id,
                memory.created_at_ms,memory.updated_at_ms,memory.revoked_at_ms,
                version.memory_version_id,version.version::text AS version_number,
                version.source,version.confidence_bps,version.sensitivity,
                version.valid_from_ms,version.valid_to_ms,
                content.content_id,content.media_type,
                content.byte_length::text AS byte_length,content.content_hash
         FROM memory.memories AS memory
         JOIN memory.memory_versions AS version
           ON version.memory_version_id=memory.current_version_id
          AND version.memory_id=memory.memory_id
         JOIN artifact.content_refs AS content
           ON content.content_id=version.content_id AND content.tenant_id=memory.tenant_id
          AND content.content_hash=version.content_hash
         WHERE memory.tenant_id=$1 AND memory.scope_kind=4 AND memory.scope_id=$2
           AND memory.lifecycle=1 AND version.valid_from_ms <= $3
           AND (version.valid_to_ms IS NULL OR version.valid_to_ms > $3)
           AND (version.source <> 1 OR EXISTS (
               SELECT 1 FROM memory.explicit_publications AS publication
               WHERE publication.memory_version_id=version.memory_version_id
                 AND publication.memory_id=memory.memory_id
                 AND publication.tenant_id=memory.tenant_id
           ))
         ORDER BY memory.updated_at_ms DESC,memory.memory_id
         LIMIT $4",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .bind(query.observed_at.unix_millis())
    .bind(i64::from(query.memory_limit.get()))
    .fetch_all(pool)
    .await?;
    let mut memories = Vec::with_capacity(rows.len());
    for row in rows {
        let mut hit = decode_memory_hit(row)?;
        hit.version.source_run_ids = load_memory_uuid_set::<RunId>(
            pool,
            "SELECT run_id FROM memory.memory_version_source_runs
             WHERE memory_version_id=$1 ORDER BY run_id",
            hit.version.memory_version_id,
        )
        .await?;
        hit.version.evidence_record_ids = load_memory_uuid_set::<LedgerRecordId>(
            pool,
            "SELECT ledger_record_id FROM memory.memory_version_evidence_records
             WHERE memory_version_id=$1 ORDER BY ledger_record_id",
            hit.version.memory_version_id,
        )
        .await?;
        memories.push(hit);
    }
    Ok(memories)
}

async fn load_metrics(
    pool: &PgPool,
    query: &AgentEvolutionInspectionQuery,
) -> Result<AgentEvolutionMetrics, PostgresMemoryEvolutionError> {
    let row = sqlx::query(
        "SELECT
             count(*) FILTER (WHERE status=1) AS candidate,
             count(*) FILTER (WHERE status=2) AS quarantined,
             count(*) FILTER (WHERE status=3) AS verified,
             count(*) FILTER (WHERE status=4) AS rejected,
             count(*) FILTER (WHERE status=5) AS published
         FROM evolution.proposals
         WHERE tenant_id=$1 AND scope_kind=2 AND scope_id=$2",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .fetch_one(pool)
    .await?;
    let active_memories: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.memories
         WHERE tenant_id=$1 AND scope_kind=4 AND scope_id=$2 AND lifecycle=1",
    )
    .bind(query.tenant_id.into_uuid())
    .bind(query.agent_id.into_uuid())
    .fetch_one(pool)
    .await?;
    Ok(AgentEvolutionMetrics {
        candidate: count(&row, "candidate")?,
        quarantined: count(&row, "quarantined")?,
        verified: count(&row, "verified")?,
        rejected: count(&row, "rejected")?,
        published: count(&row, "published")?,
        active_memories: u64::try_from(active_memories)
            .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
    })
}

fn count(
    row: &sqlx::postgres::PgRow,
    field: &'static str,
) -> Result<u64, PostgresMemoryEvolutionError> {
    u64::try_from(row.try_get::<i64, _>(field)?)
        .map_err(|_| PostgresMemoryEvolutionError::NumericRange)
}

async fn load_memory_uuid_set<T>(
    pool: &PgPool,
    sql: &'static str,
    memory_version_id: MemoryVersionId,
) -> Result<BTreeSet<T>, PostgresMemoryEvolutionError>
where
    T: FromStr + Ord,
{
    sqlx::query_scalar::<_, Uuid>(sql)
        .bind(memory_version_id.into_uuid())
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(parse_uuid)
        .collect()
}

fn parse_uuid<T>(value: Uuid) -> Result<T, PostgresMemoryEvolutionError>
where
    T: FromStr,
{
    value
        .to_string()
        .parse()
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)
}
