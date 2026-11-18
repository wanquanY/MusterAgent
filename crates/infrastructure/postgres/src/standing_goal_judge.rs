//! Durable lookup for private Standing Goal Judge Runs in Cloud Runtime.

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, RunId, TenantId};
use musteragent_loop::LoopPortError;
use musteragent_runtime_automation::{
    EVALUATION_DECIDE_WIRE_NAME, EvaluationAgentStateSource, EvaluationDecisionReceipt,
    EvaluationEvidence, StandingGoalJudgeRunSource, StandingGoalJudgeRunSourceError,
    StandingGoalJudgeRunSourceFailure, StandingGoalJudgeRunState, decision_from_result_message,
};
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_tool::WireName;
use sqlx::{PgPool, Row as _};
use std::sync::Arc;

mod batch;

const EVALUATION_ACTIVITY_KIND: i16 = 6;

#[derive(Clone)]
pub struct PostgresStandingGoalJudgeRunSource {
    pool: PgPool,
    content: Arc<dyn ContentStore>,
}

impl PostgresStandingGoalJudgeRunSource {
    #[must_use]
    pub fn new(pool: PgPool, content: Arc<dyn ContentStore>) -> Self {
        Self { pool, content }
    }

    async fn load_inner(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
        expected_activity_kind: Option<i16>,
    ) -> Result<StandingGoalJudgeRunState, StandingGoalJudgeRunSourceError> {
        let row = sqlx::query(
            "SELECT run.tenant_id, run.activity_kind, run.status, run.wait_error_code,
                    invocation.status AS invocation_status,
                    content.content_id, content.media_type,
                    content.byte_length::text AS byte_length,
                    content.content_hash, content.state AS content_state
             FROM runtime.runs AS run
             LEFT JOIN LATERAL (
                 SELECT candidate.status, candidate.response_content_id,
                        candidate.started_at_ms, candidate.invocation_id
                 FROM runtime.model_invocations AS candidate
                 WHERE candidate.run_id = run.run_id
                 ORDER BY candidate.started_at_ms DESC, candidate.invocation_id DESC
                 LIMIT 1
             ) AS invocation ON TRUE
             LEFT JOIN artifact.content_refs AS content
               ON content.tenant_id = run.tenant_id
              AND content.content_id = invocation.response_content_id
             WHERE run.run_id = $1",
        )
        .bind(run_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        let Some(row) = row else {
            return Ok(StandingGoalJudgeRunState::Missing);
        };
        let activity_kind = row
            .try_get::<i16, _>("activity_kind")
            .map_err(unavailable)?;
        let activity_kind_matches = expected_activity_kind
            .map_or(matches!(activity_kind, 1 | 3), |expected| {
                activity_kind == expected
            });
        if row
            .try_get::<uuid::Uuid, _>("tenant_id")
            .map_err(unavailable)?
            != tenant_id.into_uuid()
            || !activity_kind_matches
        {
            return Err(corrupt());
        }
        match row.try_get::<i16, _>("status").map_err(unavailable)? {
            3 if row
                .try_get::<Option<String>, _>("wait_error_code")
                .map_err(unavailable)?
                .is_some() =>
            {
                let value: String = row.try_get("wait_error_code").map_err(unavailable)?;
                let code = ErrorCode::from_safe_code(&value).ok_or_else(corrupt)?;
                Ok(StandingGoalJudgeRunState::RepairRequired { code })
            }
            1..=4 => Ok(StandingGoalJudgeRunState::Active),
            5 => {
                let mut completed = decode_completed(&row)?;
                if expected_activity_kind.is_none() {
                    let StandingGoalJudgeRunState::Completed {
                        response_ref,
                        evidence_refs,
                    } = &mut completed
                    else {
                        return Err(corrupt());
                    };
                    *evidence_refs = self
                        .load_execution_evidence(tenant_id, run_id, response_ref)
                        .await?;
                }
                Ok(completed)
            }
            6..=8 => Ok(StandingGoalJudgeRunState::Failed),
            _ => Err(corrupt()),
        }
    }

    async fn load_execution_evidence(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
        response_ref: &ContentRef,
    ) -> Result<Vec<ContentRef>, StandingGoalJudgeRunSourceError> {
        let (runs, mut evidence) = self.evidence_batch(tenant_id, run_id).await?;
        let rows = sqlx::query(
            "SELECT content.content_id, content.media_type,
                    content.byte_length::text AS byte_length, result.result_hash
             FROM tool.tool_invocations AS invocation
             JOIN tool.tool_results AS result USING(invocation_id)
             JOIN artifact.content_refs AS content
               ON content.tenant_id = invocation.tenant_id
              AND content.content_id = result.result_content_id
             WHERE invocation.tenant_id = $1 AND invocation.run_id = ANY($2)
               AND invocation.status = 4 AND content.state = 2
             ORDER BY result.committed_at_ms, invocation.invocation_id
             LIMIT 127",
        )
        .bind(tenant_id.into_uuid())
        .bind(&runs)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        for row in rows {
            let reference = ContentRef {
                content_id: ContentId::from_uuid(
                    row.try_get::<uuid::Uuid, _>("content_id")
                        .map_err(unavailable)?,
                )
                .map_err(|_| corrupt())?,
                media_type: MediaType::new(
                    row.try_get::<String, _>("media_type")
                        .map_err(unavailable)?,
                )
                .map_err(|_| corrupt())?,
                byte_length: row
                    .try_get::<String, _>("byte_length")
                    .map_err(unavailable)?
                    .parse::<u64>()
                    .map_err(|_| corrupt())?,
                sha256: Sha256::from_bytes(
                    row.try_get::<Vec<u8>, _>("result_hash")
                        .map_err(unavailable)?,
                )
                .map_err(|_| corrupt())?,
            };
            if &reference != response_ref && !evidence.contains(&reference) {
                evidence.push(reference);
                if evidence.len() == 127 {
                    break;
                }
            }
        }
        if evidence.len() < 127 {
            let artifact_rows = sqlx::query(
                "SELECT content.content_id, content.media_type,
                        content.byte_length::text AS byte_length, content.content_hash
                 FROM artifact.artifact_versions AS version
                 JOIN artifact.artifacts AS artifact
                   ON artifact.artifact_id = version.artifact_id
                 JOIN artifact.content_refs AS content
                   ON content.tenant_id = artifact.tenant_id
                  AND content.content_id = version.content_id
                 WHERE artifact.tenant_id = $1 AND content.state = 2
                   AND (artifact.created_by_run_id = ANY($2) OR version.source_invocation_id IN (
                     SELECT invocation_id FROM tool.tool_invocations
                     WHERE tenant_id = $1 AND run_id = ANY($2) AND status = 4
                   ))
                 ORDER BY version.committed_at_ms, version.artifact_version_id
                 LIMIT 127",
            )
            .bind(tenant_id.into_uuid())
            .bind(&runs)
            .fetch_all(&self.pool)
            .await
            .map_err(unavailable)?;
            for row in artifact_rows {
                let reference = ContentRef {
                    content_id: ContentId::from_uuid(
                        row.try_get::<uuid::Uuid, _>("content_id")
                            .map_err(unavailable)?,
                    )
                    .map_err(|_| corrupt())?,
                    media_type: MediaType::new(
                        row.try_get::<String, _>("media_type")
                            .map_err(unavailable)?,
                    )
                    .map_err(|_| corrupt())?,
                    byte_length: row
                        .try_get::<String, _>("byte_length")
                        .map_err(unavailable)?
                        .parse::<u64>()
                        .map_err(|_| corrupt())?,
                    sha256: Sha256::from_bytes(
                        row.try_get::<Vec<u8>, _>("content_hash")
                            .map_err(unavailable)?,
                    )
                    .map_err(|_| corrupt())?,
                };
                if &reference != response_ref && !evidence.contains(&reference) {
                    evidence.push(reference);
                    if evidence.len() == 127 {
                        break;
                    }
                }
            }
        }
        Ok(evidence)
    }
}

impl StandingGoalJudgeRunSource for PostgresStandingGoalJudgeRunSource {
    fn load(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<StandingGoalJudgeRunState, StandingGoalJudgeRunSourceError>>
    {
        Box::pin(self.load_inner(tenant_id, run_id, Some(EVALUATION_ACTIVITY_KIND)))
    }

    fn load_execution_output(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<StandingGoalJudgeRunState, StandingGoalJudgeRunSourceError>>
    {
        Box::pin(self.load_inner(tenant_id, run_id, None))
    }
}

impl EvaluationAgentStateSource for PostgresStandingGoalJudgeRunSource {
    fn unjournaled_protocol_receipts(
        &self,
        run_id: RunId,
    ) -> ApplicationFuture<
        '_,
        Result<Vec<musteragent_runtime_automation::EvaluationProtocolReceipt>, LoopPortError>,
    > {
        Box::pin(async move { self.load_protocol_receipts(run_id).await })
    }

    fn decision_protocol_stalled(
        &self,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<bool, LoopPortError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT terminal_stop_reason FROM runtime.model_invocations AS model WHERE model.run_id = $1 AND model.status = 2 AND model.attempt_id = (SELECT attempt.attempt_id FROM runtime.attempts AS attempt WHERE attempt.run_id = model.run_id ORDER BY attempt.attempt_number DESC LIMIT 1) ORDER BY started_conversation_seq DESC LIMIT 3")
                .bind(run_id.into_uuid()).fetch_all(&self.pool).await.map_err(|_| loop_corrupt())?;
            Ok(rows.len() == 3
                && rows.iter().all(|row| {
                    row.try_get::<Option<i16>, _>("terminal_stop_reason")
                        .ok()
                        .flatten()
                        == Some(1)
                }))
        })
    }

    fn accepted_decision_for_run(
        &self,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<Option<EvaluationDecisionReceipt>, LoopPortError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT run.activity_kind, run.tenant_id, pending.tool_call_id,
                        pending.result_content_hash, pending.result_content_bytes::text AS result_content_bytes,
                        content.content_id, content.media_type,
                        content.byte_length::text AS stored_content_bytes,
                        content.content_hash, content.state
                 FROM runtime.runs AS run
                 LEFT JOIN runtime.pending_tool_calls AS pending
                   ON pending.run_id = run.run_id AND pending.completed_at_ms IS NOT NULL
                  AND pending.wire_name = $1
                 LEFT JOIN artifact.content_refs AS content
                   ON content.tenant_id = run.tenant_id
                  AND content.content_id = pending.result_content_id
                 WHERE run.run_id = $2
                 ORDER BY pending.completed_at_ms DESC, pending.tool_call_id DESC
                 LIMIT 1",
            )
            .bind(EVALUATION_DECIDE_WIRE_NAME)
            .bind(run_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(loop_unavailable)?;
            let Some(row) = row else {
                return Err(loop_corrupt());
            };
            if row
                .try_get::<i16, _>("activity_kind")
                .map_err(loop_unavailable)?
                != EVALUATION_ACTIVITY_KIND
            {
                return Err(loop_corrupt());
            }
            let Some(call_id) = row
                .try_get::<Option<String>, _>("tool_call_id")
                .map_err(loop_unavailable)?
            else {
                return Ok(None);
            };
            if row
                .try_get::<Option<i16>, _>("state")
                .map_err(loop_unavailable)?
                != Some(2)
            {
                return Err(loop_corrupt());
            }
            let tenant_id = TenantId::from_uuid(
                row.try_get::<uuid::Uuid, _>("tenant_id")
                    .map_err(loop_unavailable)?,
            )
            .map_err(|_| loop_corrupt())?;
            let reference = ContentRef {
                content_id: ContentId::from_uuid(
                    row.try_get::<Option<uuid::Uuid>, _>("content_id")
                        .map_err(loop_unavailable)?
                        .ok_or_else(loop_corrupt)?,
                )
                .map_err(|_| loop_corrupt())?,
                media_type: MediaType::new(
                    row.try_get::<Option<String>, _>("media_type")
                        .map_err(loop_unavailable)?
                        .ok_or_else(loop_corrupt)?,
                )
                .map_err(|_| loop_corrupt())?,
                byte_length: row
                    .try_get::<Option<String>, _>("stored_content_bytes")
                    .map_err(loop_unavailable)?
                    .ok_or_else(loop_corrupt)?
                    .parse()
                    .map_err(|_| loop_corrupt())?,
                sha256: Sha256::from_bytes(
                    row.try_get::<Option<Vec<u8>>, _>("content_hash")
                        .map_err(loop_unavailable)?
                        .ok_or_else(loop_corrupt)?,
                )
                .map_err(|_| loop_corrupt())?,
            };
            if row
                .try_get::<Option<Vec<u8>>, _>("result_content_hash")
                .map_err(loop_unavailable)?
                != Some(reference.sha256.into_bytes().to_vec())
                || row
                    .try_get::<Option<String>, _>("result_content_bytes")
                    .map_err(loop_unavailable)?
                    .as_deref()
                    != Some(reference.byte_length.to_string().as_str())
            {
                return Err(loop_corrupt());
            }
            let bytes = self
                .content
                .read(tenant_id, &reference)
                .await
                .map_err(loop_content)?;
            verify_content(&reference, &bytes).map_err(|_| loop_corrupt())?;
            let call_id =
                musteragent_identity::ToolCallId::new(call_id).map_err(|_| loop_corrupt())?;
            let name = WireName::new(EVALUATION_DECIDE_WIRE_NAME).map_err(|_| loop_corrupt())?;
            let message = crate::context_material::decode_tool_message(&bytes, &call_id, &name)
                .map_err(|_| loop_corrupt())?;
            decision_from_result_message(&message)
        })
    }

    fn read_evidence_for_run(
        &self,
        run_id: RunId,
        index: usize,
    ) -> ApplicationFuture<'_, Result<Option<EvaluationEvidence>, LoopPortError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT tenant_id, activity_kind, parent_run_id
                 FROM runtime.runs WHERE run_id = $1",
            )
            .bind(run_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(loop_unavailable)?
            .ok_or_else(loop_corrupt)?;
            if row
                .try_get::<i16, _>("activity_kind")
                .map_err(loop_unavailable)?
                != EVALUATION_ACTIVITY_KIND
            {
                return Err(loop_corrupt());
            }
            let tenant_id = TenantId::from_uuid(
                row.try_get::<uuid::Uuid, _>("tenant_id")
                    .map_err(loop_unavailable)?,
            )
            .map_err(|_| loop_corrupt())?;
            let source_run_id = RunId::from_uuid(
                row.try_get::<Option<uuid::Uuid>, _>("parent_run_id")
                    .map_err(loop_unavailable)?
                    .ok_or_else(loop_corrupt)?,
            )
            .map_err(|_| loop_corrupt())?;
            let StandingGoalJudgeRunState::Completed {
                response_ref,
                mut evidence_refs,
            } = self
                .load_inner(tenant_id, source_run_id, None)
                .await
                .map_err(loop_judge_source)?
            else {
                return Err(loop_corrupt());
            };
            if !evidence_refs.contains(&response_ref) {
                evidence_refs.push(response_ref);
            }
            let Some(reference) = evidence_refs.get(index).cloned() else {
                return Ok(None);
            };
            let bytes = self
                .content
                .read(tenant_id, &reference)
                .await
                .map_err(loop_content)?;
            verify_content(&reference, &bytes).map_err(|_| loop_corrupt())?;
            let (text, truncated) = bounded_text(&reference, &bytes);
            Ok(Some(EvaluationEvidence {
                index,
                content_id: reference.content_id.to_string(),
                media_type: reference.media_type.as_str().to_owned(),
                byte_length: reference.byte_length,
                text,
                truncated,
            }))
        })
    }
}

fn decode_completed(
    row: &sqlx::postgres::PgRow,
) -> Result<StandingGoalJudgeRunState, StandingGoalJudgeRunSourceError> {
    if row
        .try_get::<Option<i16>, _>("invocation_status")
        .map_err(unavailable)?
        != Some(2)
        || row
            .try_get::<Option<i16>, _>("content_state")
            .map_err(unavailable)?
            != Some(2)
    {
        return Err(corrupt());
    }
    let content_id = ContentId::from_uuid(
        row.try_get::<Option<uuid::Uuid>, _>("content_id")
            .map_err(unavailable)?
            .ok_or_else(corrupt)?,
    )
    .map_err(|_| corrupt())?;
    let media_type = MediaType::new(
        row.try_get::<Option<String>, _>("media_type")
            .map_err(unavailable)?
            .ok_or_else(corrupt)?,
    )
    .map_err(|_| corrupt())?;
    let byte_length = row
        .try_get::<Option<String>, _>("byte_length")
        .map_err(unavailable)?
        .ok_or_else(corrupt)?
        .parse::<u64>()
        .map_err(|_| corrupt())?;
    let sha256 = Sha256::from_bytes(
        row.try_get::<Option<Vec<u8>>, _>("content_hash")
            .map_err(unavailable)?
            .ok_or_else(corrupt)?,
    )
    .map_err(|_| corrupt())?;
    Ok(StandingGoalJudgeRunState::Completed {
        response_ref: ContentRef {
            content_id,
            media_type,
            byte_length,
            sha256,
        },
        evidence_refs: Vec::new(),
    })
}

const fn corrupt() -> StandingGoalJudgeRunSourceError {
    StandingGoalJudgeRunSourceError {
        kind: StandingGoalJudgeRunSourceFailure::Corrupt,
        retryable: false,
    }
}

fn unavailable(_: sqlx::Error) -> StandingGoalJudgeRunSourceError {
    StandingGoalJudgeRunSourceError {
        kind: StandingGoalJudgeRunSourceFailure::Unavailable,
        retryable: true,
    }
}

fn loop_corrupt() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}

fn loop_unavailable(_: sqlx::Error) -> LoopPortError {
    LoopPortError {
        code: ErrorCode::DependencyUnavailable,
        retryable: true,
    }
}

fn loop_content(_: musteragent_runtime_content::ContentStoreError) -> LoopPortError {
    LoopPortError {
        code: ErrorCode::DependencyUnavailable,
        retryable: true,
    }
}

fn loop_judge_source(error: StandingGoalJudgeRunSourceError) -> LoopPortError {
    LoopPortError {
        code: match error.kind {
            StandingGoalJudgeRunSourceFailure::Unavailable => ErrorCode::DependencyUnavailable,
            StandingGoalJudgeRunSourceFailure::Corrupt => ErrorCode::InternalInvariantViolated,
        },
        retryable: error.retryable,
    }
}

fn bounded_text(reference: &ContentRef, bytes: &[u8]) -> (Option<String>, bool) {
    const LIMIT: usize = 256 * 1024;
    let media_type = reference.media_type.as_str();
    if !(media_type.starts_with("text/")
        || media_type == "application/json"
        || media_type.ends_with("+json"))
    {
        return (None, false);
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        return (None, false);
    };
    if text.len() <= LIMIT {
        return (Some(text.to_owned()), false);
    }
    let mut end = LIMIT;
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    (Some(text[..end].to_owned()), true)
}

#[path = "standing_goal_judge/protocol_receipts.rs"]
mod protocol_receipts;
