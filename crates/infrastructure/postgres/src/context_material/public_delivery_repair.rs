//! Materialize only the exact V6 private candidate retained by the original
//! rejection transaction. None inside Some deliberately suppresses replay after
//! a new consumed control; it does not erase the durable episode count.
use super::*;
use musteragent_execution_context::PublicDeliveryContract;
use std::num::NonZeroU32;

pub(super) async fn load(
    source: &PostgresContextMaterialSource,
    tenant: TenantId,
    run: RunId,
    cutoff: ConversationSeq,
) -> Result<Option<Option<MaterializedModelActionRejection>>, PostgresContextMaterialError> {
    let mut conn = source.pool.acquire().await?;
    let rows=sqlx::query(&format!("{} SELECT r.invocation_id,r.delivery_contract_key,r.repair_kind
        FROM runtime.model_action_rejections r JOIN runtime.model_invocations i USING(invocation_id)
        JOIN dialogue_session ON dialogue_session.tenant_id=i.tenant_id AND dialogue_session.run_id=i.run_id
        WHERE i.tenant_id=$1 AND i.run_id=$2 AND i.purpose=1 AND i.status=3
          AND i.terminal_error=convert_to(r.error_code,'UTF8') AND r.error_code='model_response_invalid'
          AND r.rejection_kind='public_delivery'
          AND i.started_conversation_seq>COALESCE((SELECT max(sequence) FROM progress WHERE sequence<=$3),0)
          AND i.started_conversation_seq<=$3 ORDER BY i.started_conversation_seq DESC LIMIT $4",
        crate::public_delivery_repair::PROGRESS_CTE))
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(cutoff.get().to_string().parse::<i64>().map_err(|_|PostgresContextMaterialError::CorruptState)?)
        .bind(i64::from(musteragent_context::MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS))
        .fetch_all(&mut *conn).await?;
    let Some(latest) = rows.first() else {
        return Ok(None);
    };
    let key: String = latest.try_get("delivery_contract_key")?;
    if !matches!(
        PublicDeliveryContract::from_key(&key),
        Some(PublicDeliveryContract::V6 | PublicDeliveryContract::V7)
    ) {
        return Ok(None);
    }
    if rows
        .iter()
        .any(|r| r.try_get::<&str, _>("delivery_contract_key").ok() != Some(key.as_str()))
        || latest.try_get::<Option<&str>, _>("repair_kind")? != Some("candidate")
    {
        return Err(PostgresContextMaterialError::CorruptState);
    }
    let invocation = InvocationId::from_uuid(latest.try_get("invocation_id")?)
        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    let (issue, repair) =
        crate::public_delivery_repair::load_candidate(&mut conn, tenant, run, invocation)
            .await
            .map_err(map_error)?;
    let count = NonZeroU32::new(u32::try_from(rows.len())?)
        .ok_or(PostgresContextMaterialError::CorruptState)?;
    let current = crate::public_delivery_repair::cut(&mut conn, tenant, run, cutoff)
        .await
        .map_err(map_error)?;
    if count.get() < musteragent_context::MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS
        && current != repair.cut
    {
        // The private evidence remains the source row plus both immutable cuts;
        // no progress/status entry is published into the conversation.
        return Ok(Some(None));
    }
    let bytes = source
        .content
        .read(tenant, &repair.candidate.content_ref)
        .await?;
    verify_content(&repair.candidate.content_ref, &bytes)
        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    Ok(Some(Some(
        MaterializedModelActionRejection::InvalidPublicDeliveryWithCandidate {
            contract_ref: VersionedKey::new(key)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            issue,
            candidate: MaterializedContent {
                content_ref: repair.candidate.content_ref.clone(),
                bytes,
            },
            repair,
            consecutive_count: count,
        },
    )))
}
fn map_error(
    error: crate::public_delivery_repair::RepairStoreError,
) -> PostgresContextMaterialError {
    match error {
        crate::public_delivery_repair::RepairStoreError::Database(error) => error.into(),
        _ => PostgresContextMaterialError::CorruptState,
    }
}
