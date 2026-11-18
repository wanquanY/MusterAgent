//! Evaluation and content-reference row decoding.

use super::*;

pub(crate) async fn decode_evaluation<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    row: &PgRow,
) -> Result<GoalEvaluation, PostgresStandingGoalError> {
    let tenant_id: TenantId = id(row.try_get("tenant_id")?, "tenant_id")?;
    let evaluation_id: GoalEvaluationId = id(row.try_get("evaluation_id")?, "evaluation_id")?;
    let evidence_rows = sqlx::query(
        "SELECT content.content_id, content.media_type,
                content.byte_length::text AS byte_length, content.content_hash
         FROM runtime.goal_evaluation_evidence AS evidence
         JOIN artifact.content_refs AS content
           ON content.tenant_id = evidence.tenant_id
          AND content.content_id = evidence.content_id
         WHERE evidence.tenant_id = $1 AND evidence.evaluation_id = $2
         ORDER BY evidence.ordinal",
    )
    .bind(tenant_id.into_uuid())
    .bind(evaluation_id.into_uuid())
    .fetch_all(executor)
    .await?;
    let evidence = evidence_rows
        .iter()
        .map(|value| content_ref(value, ""))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(GoalEvaluation {
        tenant_id,
        evaluation_id,
        goal_id: id(row.try_get("goal_id")?, "goal_id")?,
        pursuit_id: id(row.try_get("pursuit_id")?, "pursuit_id")?,
        run_id: optional_id(row.try_get("run_id")?, "run_id")?,
        verdict: decode_verdict(row.try_get("verdict")?)?,
        evidence,
        summary_ref: content_ref(row, "summary")?,
        deterministic: row.try_get("deterministic")?,
        occurred_at: UtcTimestamp::from_unix_millis(row.try_get("occurred_at_ms")?),
    })
}
pub(crate) async fn decode_evaluation_on(
    connection: &mut sqlx::PgConnection,
    row: &PgRow,
) -> Result<GoalEvaluation, PostgresStandingGoalError> {
    decode_evaluation(connection, row).await
}

pub(super) fn content_ref(
    row: &PgRow,
    prefix: &str,
) -> Result<ContentRef, PostgresStandingGoalError> {
    let content_id_field = if prefix.is_empty() {
        "content_id".to_owned()
    } else {
        format!("{prefix}_content_id")
    };
    let media_type_field = if prefix.is_empty() {
        "media_type".to_owned()
    } else {
        format!("{prefix}_media_type")
    };
    let byte_length_field = if prefix.is_empty() {
        "byte_length".to_owned()
    } else {
        format!("{prefix}_byte_length")
    };
    let hash_field = if prefix.is_empty() {
        "content_hash".to_owned()
    } else {
        format!("{prefix}_hash")
    };
    Ok(ContentRef {
        content_id: id::<ContentId>(
            row.try_get::<uuid::Uuid, _>(content_id_field.as_str())?,
            "content_id",
        )?,
        media_type: MediaType::new(row.try_get::<String, _>(media_type_field.as_str())?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
        byte_length: parse_u64(
            row.try_get::<String, _>(byte_length_field.as_str())?,
            "byte_length",
        )?,
        sha256: digest(
            row.try_get::<Vec<u8>, _>(hash_field.as_str())?,
            "content_hash",
        )?,
    })
}
