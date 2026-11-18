//! Read only the original Interaction receipt bound to this exact verification
//! invocation. No model-supplied interaction ID can satisfy manual acceptance.
use super::*;
pub(super) async fn load(
    tx: &mut PgConnection,
    request: &ResolveWorkItemStart,
) -> Result<WorkItemHumanAcceptance, DialogueStoreError> {
    let row=sqlx::query("SELECT tool.interaction_id,i.kind,i.status,retired.interaction_id AS retired_id,i.responded_by,response.response_kind,response.approval_decision,response.responded_by AS response_actor
 FROM tool.tool_invocations tool
 LEFT JOIN approval.interactions i ON i.tenant_id=tool.tenant_id AND i.interaction_id=tool.interaction_id AND i.run_id=tool.run_id AND i.conversation_id=tool.conversation_id AND i.requested_by_invocation_id=tool.invocation_id
 LEFT JOIN approval.interaction_responses response ON response.interaction_id=i.interaction_id
 LEFT JOIN approval.task_result_review_retirements retired ON retired.interaction_id=i.interaction_id AND retired.tenant_id=tool.tenant_id AND retired.invocation_id=tool.invocation_id AND retired.run_id=tool.run_id
 WHERE tool.tenant_id=$1 AND tool.invocation_id=$2 AND tool.run_id=$3 AND tool.tool_ref IN ('tasks.verify@1','tasks.verify@2')")
 .bind(request.actor.tenant_id.into_uuid()).bind(request.invocation_id.into_uuid()).bind(request.run_context.run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if row
        .try_get::<Option<uuid::Uuid>, _>("interaction_id")
        .map_err(db)?
        .is_none()
    {
        let declined:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations previous JOIN approval.interactions i ON i.tenant_id=previous.tenant_id AND i.run_id=previous.run_id AND i.requested_by_invocation_id=previous.invocation_id AND i.interaction_id=previous.interaction_id LEFT JOIN approval.interaction_responses response ON response.interaction_id=i.interaction_id WHERE previous.tenant_id=$1 AND previous.run_id=$2 AND previous.tool_ref IN ('tasks.verify@1','tasks.verify@2') AND previous.invocation_id<>$3 AND i.kind=3 AND NOT EXISTS(SELECT 1 FROM approval.task_result_review_retirements retired WHERE retired.interaction_id=i.interaction_id AND retired.tenant_id=previous.tenant_id AND retired.invocation_id=previous.invocation_id AND retired.run_id=previous.run_id) AND (i.status IN (3,4) OR (i.status=2 AND response.response_kind=1 AND response.approval_decision=2 AND response.responded_by=$4)))")
   .bind(request.actor.tenant_id.into_uuid()).bind(request.run_context.run_id.into_uuid()).bind(request.invocation_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        return Ok(if declined {
            WorkItemHumanAcceptance::PreviouslyDeclined
        } else {
            WorkItemHumanAcceptance::Absent
        });
    }
    if row.try_get::<Option<i16>, _>("kind").map_err(db)? != Some(3) {
        return Err(DialogueStoreError::Unavailable);
    }
    match row.try_get::<Option<i16>, _>("status").map_err(db)? {
        Some(1) => Ok(WorkItemHumanAcceptance::Pending),
        Some(2) => {
            if row
                .try_get::<Option<uuid::Uuid>, _>("responded_by")
                .map_err(db)?
                != Some(request.actor.principal_id.into_uuid())
                || row
                    .try_get::<Option<uuid::Uuid>, _>("response_actor")
                    .map_err(db)?
                    != Some(request.actor.principal_id.into_uuid())
                || row.try_get::<Option<i16>, _>("response_kind").map_err(db)? != Some(1)
            {
                return Err(DialogueStoreError::Unavailable);
            }
            match row
                .try_get::<Option<i16>, _>("approval_decision")
                .map_err(db)?
            {
                Some(1) => Ok(WorkItemHumanAcceptance::Approved),
                Some(2) => Ok(WorkItemHumanAcceptance::Rejected),
                _ => Err(DialogueStoreError::Unavailable),
            }
        }
        Some(3) => Ok(WorkItemHumanAcceptance::Expired),
        Some(4) => Ok(
            if row
                .try_get::<Option<uuid::Uuid>, _>("retired_id")
                .map_err(db)?
                .is_some()
            {
                WorkItemHumanAcceptance::Retired
            } else {
                WorkItemHumanAcceptance::Cancelled
            },
        ),
        _ => Err(DialogueStoreError::Corrupt),
    }
}

pub(super) async fn load_prompt(
    tx: &mut PgConnection,
    request: &ResolveWorkItemStart,
) -> Result<Option<musteragent_content_ref::ContentRef>, DialogueStoreError> {
    let id: Option<uuid::Uuid> = sqlx::query_scalar("SELECT i.prompt_content_id FROM tool.tool_invocations tool JOIN approval.interactions i ON i.tenant_id=tool.tenant_id AND i.interaction_id=tool.interaction_id AND i.run_id=tool.run_id AND i.conversation_id=tool.conversation_id AND i.requested_by_invocation_id=tool.invocation_id WHERE tool.tenant_id=$1 AND tool.invocation_id=$2 AND tool.run_id=$3 AND tool.tool_ref IN ('tasks.verify@1','tasks.verify@2') AND i.kind=3")
        .bind(request.actor.tenant_id.into_uuid()).bind(request.invocation_id.into_uuid()).bind(request.run_context.run_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    match id {
        Some(id) => super::verification::content(tx, request.actor.tenant_id, id)
            .await
            .map(Some),
        None => Ok(None),
    }
}
