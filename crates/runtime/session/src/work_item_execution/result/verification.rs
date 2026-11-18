use super::*;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemResultEvidenceRef {
    pub result_id: uuid::Uuid,
    pub item_id: uuid::Uuid,
}
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemResultCriterion {
    pub index: u16,
    pub decision: WorkItemVerificationDecision,
    pub evidence: Vec<WorkItemResultEvidenceRef>,
}
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemResultVerificationInput {
    pub task_id: uuid::Uuid,
    pub goal_revision: u64,
    pub control_revision: u64,
    pub assignment_epoch: u64,
    pub producing_run_id: uuid::Uuid,
    pub report_content_id: uuid::Uuid,
    pub result_id: uuid::Uuid,
    pub manifest_sha256: String,
    pub decision: WorkItemVerificationDecision,
    pub criteria: Vec<WorkItemResultCriterion>,
}
impl WorkItemResultVerificationInput {
    pub fn parse(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let invalid = DialogueStoreError::Corrupt;
        if bytes.len() > 65536 {
            return Err(invalid);
        }
        let input: Self = serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        let hash =
            Sha256::from_hex(&input.manifest_sha256).map_err(|_| DialogueStoreError::Corrupt)?;
        if hash.to_string() != input.manifest_sha256
            || [
                input.task_id,
                input.producing_run_id,
                input.report_content_id,
                input.result_id,
            ]
            .iter()
            .any(uuid::Uuid::is_nil)
            || [
                input.goal_revision,
                input.control_revision,
                input.assignment_epoch,
            ]
            .iter()
            .any(|v| *v == 0 || *v > i64::MAX as u64)
            || input.criteria.is_empty()
            || input.criteria.len() > 100
        {
            return Err(invalid);
        }
        for (index, criterion) in input.criteria.iter().enumerate() {
            let mut seen = std::collections::BTreeSet::new();
            if criterion.index as usize != index
                || criterion.evidence.len() > 32
                || (criterion.decision == WorkItemVerificationDecision::Accepted
                    && criterion.evidence.is_empty())
                || criterion.evidence.iter().any(|r| {
                    r.result_id != input.result_id || r.item_id.is_nil() || !seen.insert(r.item_id)
                })
            {
                return Err(invalid);
            }
        }
        let accepted = input
            .criteria
            .iter()
            .all(|v| v.decision == WorkItemVerificationDecision::Accepted);
        let failed = input
            .criteria
            .iter()
            .any(|v| v.decision == WorkItemVerificationDecision::Failed);
        if input.decision == WorkItemVerificationDecision::Accepted && !accepted
            || input.decision == WorkItemVerificationDecision::Failed && !failed
        {
            return Err(invalid);
        }
        Ok(input)
    }
    pub fn validate_manifest(
        &self,
        manifest: &WorkItemResultManifest,
    ) -> Result<(), DialogueStoreError> {
        let scope = &manifest.scope;
        let receipt = manifest.receipt()?;
        if self.task_id != scope.work_item_id.into_uuid()
            || self.goal_revision != scope.goal_revision
            || self.control_revision != scope.control_revision
            || self.assignment_epoch != scope.assignment_epoch
            || self.producing_run_id != scope.producing_run_id.into_uuid()
            || self.result_id != receipt.result_id
            || self.manifest_sha256 != receipt.manifest_sha256.to_string()
            || self
                .criteria
                .iter()
                .flat_map(|c| &c.evidence)
                .any(|e| !receipt.item_ids.contains(&e.item_id))
        {
            return Err(DialogueStoreError::RevisionConflict);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PrepareWorkItemResultVerification {
    pub invocation: ResolveWorkItemStart,
    pub request_bytes: Vec<u8>,
}
impl PrepareWorkItemResultVerification {
    pub fn validate(&self) -> Result<WorkItemResultVerificationInput, DialogueStoreError> {
        verify_bytes(&self.invocation.request_ref, &self.request_bytes)?;
        WorkItemResultVerificationInput::parse(&self.request_bytes)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultVerificationMaterial {
    pub task: WorkItemAuthorityRecord,
    pub report_ref: ContentRef,
    pub manifest_ref: ContentRef,
    pub manifest: WorkItemResultManifest,
}
#[derive(Debug, Clone)]
pub struct CommitWorkItemResultVerification {
    pub preparation: PrepareWorkItemResultVerification,
    pub material: WorkItemResultVerificationMaterial,
    pub goal_bytes: Vec<u8>,
    pub result: ContentRef,
    pub result_schema: SchemaRef,
    pub at: UtcTimestamp,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultVerificationReceipt {
    pub work_item_id: WorkItemId,
    pub result_id: uuid::Uuid,
    pub manifest_sha256: Sha256,
    pub decision: WorkItemVerificationDecision,
    pub delivery_id: Option<CommandId>,
}
impl WorkItemResultVerificationMaterial {
    pub fn receipt(
        &self,
        input: &WorkItemResultVerificationInput,
        goal: &[u8],
    ) -> Result<WorkItemResultVerificationReceipt, DialogueStoreError> {
        input.validate_manifest(&self.manifest)?;
        let m = &self.manifest;
        let scope = &m.scope;
        verify_ref(
            &self.manifest_ref,
            work_item_result_manifest_id(m.submission_invocation_id),
            WORK_ITEM_RESULT_MANIFEST_MEDIA,
            &m.canonical_bytes()?,
        )?;
        verify_bytes(&self.task.goal_content_ref, goal)?;
        let spec = crate::decode_work_item_goal(&self.task, goal)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if self.task.core.work_item_id != scope.work_item_id
            || self.report_ref.content_id.into_uuid() != input.report_content_id
            || spec.success_criteria.len() != input.criteria.len()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(WorkItemResultVerificationReceipt {
            work_item_id: scope.work_item_id,
            result_id: m.result_id(),
            manifest_sha256: self.manifest_ref.sha256,
            decision: input.decision,
            delivery_id: (input.decision == WorkItemVerificationDecision::Accepted)
                .then(|| work_item_result_delivery_id(m.result_id(), scope.source_dialogue_id)),
        })
    }
}
impl WorkItemResultVerificationReceipt {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let status = match self.decision {
            WorkItemVerificationDecision::Accepted => "completed",
            WorkItemVerificationDecision::Failed => "blocked",
            WorkItemVerificationDecision::NeedsReview => "verifying",
        };
        serde_json::json!({"task_id":self.work_item_id.to_string(),"result_id":self.result_id,"manifest_sha256":self.manifest_sha256.to_string(),"decision":self.decision,
            "assessment_kind":"agent_evidence_assessment","status":status,"delivery_id":self.delivery_id.map(|id|id.to_string())}).to_string().into_bytes()
    }
}
impl CommitWorkItemResultVerification {
    pub fn validate(&self) -> Result<WorkItemResultVerificationReceipt, DialogueStoreError> {
        let input = self.preparation.validate()?;
        let receipt = self.material.receipt(&input, &self.goal_bytes)?;
        verify_ref(
            &self.result,
            work_item_result_verify_receipt_id(self.preparation.invocation.invocation_id),
            "application/json",
            &receipt.canonical_bytes(),
        )?;
        Ok(receipt)
    }
}
