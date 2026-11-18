use super::*;
use musteragent_content_ref::Sha256;
use serde_json::{Value, json};
use sqlx::postgres::PgRow;

pub(super) fn db(error: sqlx::Error) -> WorkItemAuthorityError {
    match &error {
        sqlx::Error::Database(e) if e.is_unique_violation() => {
            WorkItemAuthorityError::IdempotencyConflict
        }
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::ColumnNotFound(_) => {
            WorkItemAuthorityError::Corrupt
        }
        _ => WorkItemAuthorityError::Unavailable,
    }
}

pub(super) fn transaction(error: crate::PostgresTransactionError) -> WorkItemAuthorityError {
    match error {
        crate::PostgresTransactionError::Database(error) => db(error),
        _ => WorkItemAuthorityError::Unavailable,
    }
}

pub(super) fn signed(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| WorkItemAuthorityError::InvalidRequest)
}
fn unsigned(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| WorkItemAuthorityError::Corrupt)
}
fn corrupt<T>(_: T) -> WorkItemAuthorityError {
    WorkItemAuthorityError::Corrupt
}

pub(super) fn decode(row: &PgRow) -> Result<WorkItemAuthorityRecord> {
    macro_rules! id {
        ($name:literal, $ty:ty) => {
            <$ty>::from_uuid(row.try_get($name).map_err(db)?).map_err(corrupt)?
        };
    }
    let status = match row.try_get::<&str, _>("status").map_err(db)? {
        "queued" => WorkItemStatus::Queued,
        "ready" => WorkItemStatus::Ready,
        "active" => WorkItemStatus::Active,
        "blocked" => WorkItemStatus::Blocked,
        "verifying" => WorkItemStatus::Verifying,
        "completed" => WorkItemStatus::Completed,
        "superseded" => WorkItemStatus::Superseded,
        "cancelled" => WorkItemStatus::Cancelled,
        _ => return Err(WorkItemAuthorityError::Corrupt),
    };
    let priority = match row.try_get::<&str, _>("priority").map_err(db)? {
        "low" => WorkItemPriority::Low,
        "normal" => WorkItemPriority::Normal,
        "high" => WorkItemPriority::High,
        "critical" => WorkItemPriority::Critical,
        _ => return Err(WorkItemAuthorityError::Corrupt),
    };
    let optional_id = |key| -> Result<Option<WorkItemId>> {
        row.try_get::<Option<uuid::Uuid>, _>(key)
            .map_err(db)?
            .map(WorkItemId::from_uuid)
            .transpose()
            .map_err(corrupt)
    };
    let core = WorkItemCoreMetadata {
        work_item_id: id!("work_item_id", WorkItemId),
        tenant_id: id!("tenant_id", TenantId),
        scope: WorkItemScope::OwnedAgent(OwnedAgentWorkItemScope {
            space_id: id!("space_id", OwnerSpaceId),
            owner_id: id!("owner_id", PrincipalId),
            agent_id: id!("agent_id", AgentId),
        }),
        revisions: WorkItemRevisions {
            goal: WorkItemGoalRevision::new(unsigned(row.try_get("goal_revision").map_err(db)?)?)
                .map_err(corrupt)?,
            control: WorkItemControlRevision::new(unsigned(
                row.try_get("control_revision").map_err(db)?,
            )?)
            .map_err(corrupt)?,
            state: WorkItemStateRevision::new(unsigned(
                row.try_get("selected_revision").map_err(db)?,
            )?)
            .map_err(corrupt)?,
        },
        spec: WorkItemSpecMetadata {
            dependencies: row
                .try_get::<Vec<uuid::Uuid>, _>("dependencies")
                .map_err(db)?
                .into_iter()
                .map(WorkItemId::from_uuid)
                .collect::<std::result::Result<_, _>>()
                .map_err(corrupt)?,
            priority,
            follow_up_of: optional_id("follow_up_of")?,
        },
        status,
        assignment_epoch: row
            .try_get::<Option<i64>, _>("assignment_epoch")
            .map_err(db)?
            .map(|x| AssignmentEpoch::new(unsigned(x)?).map_err(corrupt))
            .transpose()?,
    };
    core.spec
        .validate_contract(core.work_item_id)
        .map_err(corrupt)?;
    let goal_content_ref =
        crate::dialogue_codec::content_from_bytes(row.try_get("goal_content_ref").map_err(db)?)
            .map_err(corrupt)?;
    if goal_content_ref.content_id != id!("goal_content_id", ContentId) {
        return Err(WorkItemAuthorityError::Corrupt);
    }
    Ok(WorkItemAuthorityRecord {
        core,
        goal_content_ref,
        source_dialogue_id: id!("source_dialogue_id", AgentDialogueId),
        source_intent_id: id!("source_intent_id", UserIntentId),
        parent_work_item_id: optional_id("parent_work_item_id")?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(db)?),
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms").map_err(db)?),
        directory_seq: unsigned(row.try_get("directory_seq").map_err(db)?)?,
    })
}

pub(super) fn priority(value: WorkItemPriority) -> &'static str {
    match value {
        WorkItemPriority::Low => "low",
        WorkItemPriority::Normal => "normal",
        WorkItemPriority::High => "high",
        WorkItemPriority::Critical => "critical",
    }
}
pub(super) fn status(value: WorkItemStatus) -> &'static str {
    match value {
        WorkItemStatus::Queued => "queued",
        WorkItemStatus::Ready => "ready",
        WorkItemStatus::Active => "active",
        WorkItemStatus::Blocked => "blocked",
        WorkItemStatus::Verifying => "verifying",
        WorkItemStatus::Completed => "completed",
        WorkItemStatus::Superseded => "superseded",
        WorkItemStatus::Cancelled => "cancelled",
    }
}

/// Fixed internal fingerprint contract, not a model output protocol. The
/// transport's retry time and command ID are not semantic request content.
pub(super) fn fingerprint(
    scope: OwnedAgentWorkItemScope,
    spec: &WorkItemSpec,
    operation: Value,
) -> Vec<u8> {
    let value = json!({"contract": "work-item-command.v1", "space":scope.space_id.to_string(),
        "owner":scope.owner_id.to_string(),"agent":scope.agent_id.to_string(),"operation":operation,
        "spec":{"objective":spec.objective,"success_criteria":spec.success_criteria,
            "dependencies":spec.dependencies.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "priority":priority(spec.priority),"evidence_boundary":spec.evidence_boundary,
            "follow_up_of":spec.follow_up_of.map(|x| x.to_string())}});
    Sha256::digest(value.to_string()).into_bytes().to_vec()
}
