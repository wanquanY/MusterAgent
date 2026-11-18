use super::*;
use musteragent_identity::ContentId;
use serde_json::json;

pub const WORK_ITEM_GOAL_MEDIA_TYPE: &str = "application/vnd.musteragent.work-item-goal+json";

/// Runtime-owned content encoding, never parsed from model prose to execute.
#[must_use]
pub fn work_item_goal_bytes(spec: &WorkItemSpec) -> Vec<u8> {
    json!({"version":1,"objective":spec.objective,"success_criteria":spec.success_criteria,
        "evidence_boundary":spec.evidence_boundary})
    .to_string()
    .into_bytes()
}

/// Independent identity prevents identical text in two tasks sharing one
/// erasable blob. Retries of the same command retain this identity.
#[must_use]
pub fn work_item_goal_content_id(task: WorkItemId, command: CommandId) -> ContentId {
    let mut bytes = b"musteragent.work-item-goal.v1\0".to_vec();
    bytes.extend_from_slice(task.into_uuid().as_bytes());
    bytes.extend_from_slice(command.into_uuid().as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&Sha256::digest(bytes).into_bytes()[..16]);
    id[6] = (id[6] & 0x0f) | 0x50;
    id[8] = (id[8] & 0x3f) | 0x80;
    ContentId::from_uuid(uuid::Uuid::from_bytes(id)).expect("derived UUID is nonzero")
}

pub(super) fn validate_work_item_goal_ref(
    task: WorkItemId,
    command: CommandId,
    spec: &WorkItemSpec,
    reference: &ContentRef,
) -> Result<(), WorkItemAuthorityError> {
    let bytes = work_item_goal_bytes(spec);
    if reference.content_id != work_item_goal_content_id(task, command)
        || reference.media_type.as_str() != WORK_ITEM_GOAL_MEDIA_TYPE
        || reference.byte_length != bytes.len() as u64
        || reference.sha256 != Sha256::digest(&bytes)
    {
        return Err(WorkItemAuthorityError::InvalidRequest);
    }
    Ok(())
}

/// Call only with bytes from a fresh authorized ContentStore read. Its
/// NotFound/tombstone is ContentErased for the caller, never a reason to fall
/// back to a prior command or cached plaintext.
pub fn decode_work_item_goal(
    record: &WorkItemAuthorityRecord,
    bytes: &[u8],
) -> Result<WorkItemSpec, WorkItemAuthorityError> {
    if bytes.len() as u64 != record.goal_content_ref.byte_length
        || Sha256::digest(bytes) != record.goal_content_ref.sha256
    {
        return Err(WorkItemAuthorityError::Corrupt);
    }
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| WorkItemAuthorityError::Corrupt)?;
    let object = value.as_object().ok_or(WorkItemAuthorityError::Corrupt)?;
    if object.len() != 4 || value["version"] != 1 {
        return Err(WorkItemAuthorityError::Corrupt);
    }
    let text = |k: &str| {
        value[k]
            .as_str()
            .map(str::to_owned)
            .ok_or(WorkItemAuthorityError::Corrupt)
    };
    let spec = WorkItemSpec {
        objective: text("objective")?,
        success_criteria: value["success_criteria"]
            .as_array()
            .ok_or(WorkItemAuthorityError::Corrupt)?
            .iter()
            .map(|x| {
                x.as_str()
                    .map(str::to_owned)
                    .ok_or(WorkItemAuthorityError::Corrupt)
            })
            .collect::<Result<_, _>>()?,
        evidence_boundary: text("evidence_boundary")?,
        dependencies: record.core.spec.dependencies.clone(),
        priority: record.core.spec.priority,
        follow_up_of: record.core.spec.follow_up_of,
    };
    spec.validate_contract(record.core.work_item_id)
        .map_err(|_| WorkItemAuthorityError::Corrupt)?;
    Ok(spec)
}
