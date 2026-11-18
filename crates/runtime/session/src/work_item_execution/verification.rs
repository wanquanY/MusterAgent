//! Original Task business acceptance. This never fabricates Runtime completion,
//! public delivery, or a user's confirmation of a manual acceptance criterion.
use super::*;
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemVerificationDecision {
    Accepted,
    Failed,
    NeedsReview,
}
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemCriterionAssessment {
    pub index: u16,
    pub decision: WorkItemVerificationDecision,
    pub evidence_entry_ids: Vec<uuid::Uuid>,
}
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemVerificationInput {
    pub task_id: uuid::Uuid,
    pub goal_revision: u64,
    pub control_revision: u64,
    pub assignment_epoch: u64,
    pub producing_run_id: uuid::Uuid,
    pub report_content_id: uuid::Uuid,
    pub decision: WorkItemVerificationDecision,
    pub criteria: Vec<WorkItemCriterionAssessment>,
}
impl WorkItemVerificationInput {
    pub fn parse(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > 65536 {
            return Err(DialogueStoreError::Corrupt);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        WorkItemId::from_uuid(value.task_id).map_err(|_| DialogueStoreError::Corrupt)?;
        if value.producing_run_id.is_nil()
            || value.report_content_id.is_nil()
            || value.goal_revision == 0
            || value.control_revision == 0
            || value.assignment_epoch == 0
            || value.criteria.is_empty()
            || value.criteria.len() > 100
        {
            return Err(DialogueStoreError::Corrupt);
        }
        for (index, criterion) in value.criteria.iter().enumerate() {
            if usize::from(criterion.index) != index
                || criterion.evidence_entry_ids.len() > 32
                || criterion.evidence_entry_ids.iter().any(uuid::Uuid::is_nil)
                || criterion
                    .evidence_entry_ids
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != criterion.evidence_entry_ids.len()
                || (criterion.decision == WorkItemVerificationDecision::Accepted
                    && criterion.evidence_entry_ids.is_empty())
            {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        if value.decision == WorkItemVerificationDecision::Accepted
            && value
                .criteria
                .iter()
                .any(|c| c.decision != WorkItemVerificationDecision::Accepted)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if value.decision == WorkItemVerificationDecision::Failed
            && !value
                .criteria
                .iter()
                .any(|c| c.decision == WorkItemVerificationDecision::Failed)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(value)
    }
}
#[derive(Debug, Clone)]
pub struct PrepareWorkItemVerification {
    pub invocation: ResolveWorkItemStart,
    pub input: WorkItemVerificationInput,
    pub request_bytes: Vec<u8>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemVerificationMaterial {
    pub task: WorkItemAuthorityRecord,
    pub report_ref: ContentRef,
    pub evidence: Vec<ContentRef>,
    /// Original immutable start/amend Tool requests determine the retained human
    /// confirmation boundary. The handler verifies these bytes before decoding it.
    pub command_requests: Vec<ContentRef>,
    /// Replays preserve the historical verification result and never rewrite it.
    pub historical_receipt: bool,
    /// Set only from the original persisted tasks.verify Tool key.
    pub direct_result_contract: bool,
    pub human_acceptance: WorkItemHumanAcceptance,
    pub human_prompt_ref: Option<ContentRef>,
    pub review_evidence: Vec<musteragent_runtime_interaction::TaskReviewEvidence>,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemHumanAcceptance {
    Absent,
    Pending,
    Approved,
    Rejected,
    PreviouslyDeclined,
    Cancelled,
    Expired,
    /// Original owner retired a result review, never a user approval.
    Retired,
}
#[derive(Debug, Clone)]
pub struct CommitWorkItemVerification {
    pub preparation: PrepareWorkItemVerification,
    pub material: WorkItemVerificationMaterial,
    pub goal_bytes: Vec<u8>,
    pub command_bytes: Vec<Vec<u8>>,
    pub human_prompt_bytes: Option<Vec<u8>>,
    pub decision: WorkItemVerificationDecision,
    pub result: ContentRef,
    pub result_schema: SchemaRef,
    pub at: UtcTimestamp,
}
pub fn work_item_verification_id(invocation: InvocationId) -> CommandId {
    CommandId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-verification@1",
    ))
    .expect("derived UUID")
}
pub fn work_item_verification_result_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-verification-result@1",
    ))
    .expect("derived UUID")
}
pub fn work_item_verification_result_bytes(
    input: &WorkItemVerificationInput,
    decision: WorkItemVerificationDecision,
    human: WorkItemHumanAcceptance,
    direct_contract: bool,
) -> Result<Vec<u8>, DialogueStoreError> {
    if human == WorkItemHumanAcceptance::Retired {
        return Err(DialogueStoreError::Corrupt);
    }
    if direct_contract {
        return Ok(serde_json::json!({"task_id":input.task_id,"goal_revision":input.goal_revision,"control_revision":input.control_revision,"assignment_epoch":input.assignment_epoch,"producing_run_id":input.producing_run_id,"decision":decision,"assessment_kind":"agent_evidence_assessment","status":if decision==WorkItemVerificationDecision::Accepted{"completed"}else if decision==WorkItemVerificationDecision::Failed{"blocked"}else{"verifying"}}).to_string().into_bytes());
    }
    Ok(serde_json::json!({"task_id":input.task_id,"goal_revision":input.goal_revision,"control_revision":input.control_revision,"assignment_epoch":input.assignment_epoch,"producing_run_id":input.producing_run_id,"decision":decision,"assessment_kind":if human==WorkItemHumanAcceptance::Approved{"agent_evidence_and_user_confirmation"}else{"agent_evidence_assessment"},"user_confirmation":match human{WorkItemHumanAcceptance::Absent=>"not_requested",WorkItemHumanAcceptance::Pending=>"pending",WorkItemHumanAcceptance::Approved=>"approved",WorkItemHumanAcceptance::Rejected=>"rejected",WorkItemHumanAcceptance::PreviouslyDeclined=>"previously_declined",WorkItemHumanAcceptance::Cancelled=>"cancelled",WorkItemHumanAcceptance::Expired=>"expired",WorkItemHumanAcceptance::Retired=>return Err(DialogueStoreError::Corrupt)},"status":if decision==WorkItemVerificationDecision::Accepted{"completed"}else if decision==WorkItemVerificationDecision::Failed{"blocked"}else{"verifying"}}).to_string().into_bytes())
}
impl PrepareWorkItemVerification {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.invocation.request_ref.sha256 != Sha256::digest(&self.request_bytes)
            || self.invocation.request_ref.byte_length != self.request_bytes.len() as u64
            || WorkItemVerificationInput::parse(&self.request_bytes)? != self.input
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

pub fn validate_work_item_verification_content(
    input: &WorkItemVerificationInput,
    material: &WorkItemVerificationMaterial,
    goal: &[u8],
    commands: &[Vec<u8>],
) -> Result<WorkItemVerificationDecision, DialogueStoreError> {
    let check =
        |r: &ContentRef, b: &[u8]| r.byte_length == b.len() as u64 && r.sha256 == Sha256::digest(b);
    if !check(&material.task.goal_content_ref, goal)
        || commands.len() != material.command_requests.len()
    {
        return Err(DialogueStoreError::Corrupt);
    }
    let spec = crate::decode_work_item_goal(&material.task, goal)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    if spec.success_criteria.len() != input.criteria.len() {
        return Err(DialogueStoreError::Corrupt);
    }
    let mut human = false;
    for (reference, bytes) in material.command_requests.iter().zip(commands) {
        if !check(reference, bytes) {
            return Err(DialogueStoreError::Corrupt);
        }
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        match value.get("human_verification_required") {
            None | Some(serde_json::Value::Bool(false)) => {}
            Some(serde_json::Value::Bool(true)) => human = true,
            _ => return Err(DialogueStoreError::Corrupt),
        }
    }
    // Historical receipts retain their original interpretation. Future evidence
    // assessments never manufacture user consent or reopen a review UI.
    let withheld = if material.historical_receipt {
        human && material.human_acceptance != WorkItemHumanAcceptance::Approved
    } else {
        matches!(
            material.human_acceptance,
            WorkItemHumanAcceptance::Rejected
                | WorkItemHumanAcceptance::PreviouslyDeclined
                | WorkItemHumanAcceptance::Cancelled
                | WorkItemHumanAcceptance::Expired
                | WorkItemHumanAcceptance::Pending
        )
    };
    Ok(
        if withheld && input.decision == WorkItemVerificationDecision::Accepted {
            WorkItemVerificationDecision::NeedsReview
        } else {
            input.decision
        },
    )
}

/// The immutable Task and evidence are carried by a versioned original
/// Confirmation presentation, independent of model-authored question limits.
pub fn work_item_confirmation_prompt(
    input: &WorkItemVerificationInput,
    material: &WorkItemVerificationMaterial,
    goal: &[u8],
) -> Result<musteragent_runtime_interaction::TaskAcceptancePrompt, DialogueStoreError> {
    use musteragent_runtime_interaction::{
        TaskAcceptancePrompt, TaskAcceptanceReview, TaskReviewCriterion,
    };
    let spec = crate::decode_work_item_goal(&material.task, goal)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    if spec.success_criteria.len() != input.criteria.len() {
        return Err(DialogueStoreError::Corrupt);
    }
    TaskAcceptancePrompt::new(TaskAcceptanceReview {
        schema: "work-item.acceptance-review@1".into(),
        task_id: input.task_id,
        goal_revision: input.goal_revision.to_string(),
        control_revision: input.control_revision.to_string(),
        assignment_epoch: input.assignment_epoch.to_string(),
        producing_run_id: input.producing_run_id,
        report_content_ref: (&material.report_ref).into(),
        goal_content_ref: (&material.task.goal_content_ref).into(),
        objective: spec.objective,
        evidence_boundary: spec.evidence_boundary,
        criteria: spec
            .success_criteria
            .into_iter()
            .zip(&input.criteria)
            .map(|(text, c)| TaskReviewCriterion {
                index: c.index,
                text,
                evidence_entry_ids: c.evidence_entry_ids.clone(),
            })
            .collect(),
        evidence: material.review_evidence.clone(),
    })
    .map_err(|_| DialogueStoreError::Corrupt)
}
/// A positive owner receipt applies only to the exact immutable presentation
/// that the original Interaction owner showed. No subsequent tool call can
/// borrow the response for changed criteria, revisions, or evidence.
pub fn validate_work_item_acceptance_prompt(
    input: &WorkItemVerificationInput,
    material: &WorkItemVerificationMaterial,
    goal: &[u8],
    bytes: Option<&[u8]>,
) -> Result<(), DialogueStoreError> {
    if material.human_acceptance != WorkItemHumanAcceptance::Approved {
        return Ok(());
    }
    let bytes = bytes.ok_or(DialogueStoreError::Unavailable)?;
    let reference = material
        .human_prompt_ref
        .as_ref()
        .ok_or(DialogueStoreError::Unavailable)?;
    if reference.byte_length != bytes.len() as u64 || reference.sha256 != Sha256::digest(bytes) {
        return Err(DialogueStoreError::Corrupt);
    }
    let original = musteragent_runtime_interaction::TaskAcceptancePrompt::parse(bytes)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    if original.task_review != work_item_confirmation_prompt(input, material, goal)?.task_review {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}
