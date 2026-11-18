//! Task privacy scope owned by the original Erasure aggregate. v1 preserves
//! unexecuted goal-history semantics; v2 fences executed tasks and must prove
//! the independent execution closure before deleting bytes or issuing a receipt.
use crate::{
    ErasureAppendDisposition, ErasureContractError, ErasureDataClass, ErasureJob,
    ErasurePlanSnapshot, ErasurePlanVersion, ErasureRequest, ErasureRetentionException,
};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, DottedKey, ErasureJobId, IdempotencyKey, OwnerSpaceId,
    PrincipalKind, UserIntentId, UtcTimestamp, WorkItemId,
};
use musteragent_runtime_contract::{ApplicationFuture, DataRetentionPolicyRef, ResourceRef};

pub const WORK_ITEM_ERASURE_SCHEMA: &str = "musteragent.privacy.unexecuted-work-item-erasure.v1";
pub const WORK_ITEM_ERASURE_POLICY: &str = "privacy.unexecuted-work-item-goals@1";
pub const WORK_ITEM_EXECUTION_ERASURE_SCHEMA: &str =
    "musteragent.privacy.work-item-execution-erasure.v2";
pub const WORK_ITEM_EXECUTION_ERASURE_POLICY: &str = "privacy.work-item-execution@2";

#[must_use]
pub fn is_work_item_erasure_schema(schema: &str) -> bool {
    matches!(
        schema,
        WORK_ITEM_ERASURE_SCHEMA | WORK_ITEM_EXECUTION_ERASURE_SCHEMA
    )
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RequestWorkItemErasure {
    pub actor: Actor,
    pub space_id: OwnerSpaceId,
    pub agent_id: AgentId,
    pub work_item_id: WorkItemId,
    pub expected_state_revision: u64,
    pub expected_control_revision: u64,
    pub reason_code: DottedKey,
    pub idempotency_key: IdempotencyKey,
    pub requested_at: UtcTimestamp,
    pub complete_by: UtcTimestamp,
    /// Exactly ObjectHistory and Backup, resolved from real provider policy.
    /// Neither a user/tool-selected deadline nor an assertion of physical purge.
    pub retention_exceptions: Vec<ErasureRetentionException>,
}

impl RequestWorkItemErasure {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        if self.actor.principal_kind != PrincipalKind::User {
            return Err(ErasureContractError::ActorTenantMismatch);
        }
        if self.requested_at >= self.complete_by
            || self.expected_state_revision == 0
            || self.expected_control_revision == 0
        {
            return Err(ErasureContractError::InvalidCompletionDeadline);
        }
        let classes: std::collections::BTreeSet<_> = self
            .retention_exceptions
            .iter()
            .map(|r| r.data_class)
            .collect();
        if self.retention_exceptions.len() != 2
            || classes != [ErasureDataClass::ObjectHistory, ErasureDataClass::Backup].into()
            || self
                .retention_exceptions
                .iter()
                .any(|r| r.expires_at <= self.complete_by)
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        Ok(())
    }

    pub fn matches_admitted_intent(
        &self,
        job: &ErasureJob,
        lineage: &WorkItemErasureLineage,
    ) -> Result<bool, ErasureContractError> {
        self.validate()?;
        job.validate()?;
        lineage.validate()?;
        let roots: Vec<_> = lineage
            .goal_content_refs
            .iter()
            .cloned()
            .map(ResourceRef::Content)
            .collect();
        if !is_work_item_erasure_schema(job.request.schema_version.as_str())
            || job.request.tenant_id != lineage.actor.tenant_id
            || job.request.erasure_job_id != lineage.erasure_job_id
            || job.request.requested_by != lineage.actor
            || job.request.requested_at != lineage.frozen_at
            || job.request.root_resource_refs != roots
            || job
                .plans
                .first()
                .is_none_or(|p| p.lineage_fingerprint != lineage.fingerprint())
            || (job.request.schema_version.as_str() == WORK_ITEM_ERASURE_SCHEMA
                && job
                    .plans
                    .iter()
                    .any(|p| p.lineage_fingerprint != lineage.fingerprint()))
            || (job.request.schema_version.as_str() == WORK_ITEM_EXECUTION_ERASURE_SCHEMA
                && job.plans.len() > 2)
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        Ok(self.actor == lineage.actor
            && self.space_id == lineage.space_id
            && self.agent_id == lineage.agent_id
            && self.work_item_id == lineage.work_item_id
            && self.expected_state_revision == lineage.state_revision
            && self.expected_control_revision == lineage.control_revision
            && self.reason_code == lineage.reason_code
            && self.idempotency_key == job.request.idempotency_key)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemErasureLineage {
    pub actor: Actor,
    pub erasure_job_id: ErasureJobId,
    pub space_id: OwnerSpaceId,
    pub agent_id: AgentId,
    pub work_item_id: WorkItemId,
    pub source_dialogue_id: AgentDialogueId,
    pub source_intent_id: UserIntentId,
    pub state_revision: u64,
    pub control_revision: u64,
    pub revision_count: u64,
    /// Complete revision order, deduplicated by exact ContentRef. No text.
    pub goal_content_refs: Vec<ContentRef>,
    pub reason_code: DottedKey,
    pub frozen_at: UtcTimestamp,
}
impl WorkItemErasureLineage {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        if self.goal_content_refs.is_empty()
            || self.state_revision == 0
            || self.revision_count != self.state_revision
            || self.control_revision == 0
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        if self.goal_content_refs.len() > 4096 {
            return Err(ErasureContractError::TooManyRootResources);
        }
        let mut ids = std::collections::BTreeSet::new();
        if self
            .goal_content_refs
            .iter()
            .any(|r| !ids.insert(r.content_id))
        {
            return Err(ErasureContractError::DuplicateRootResource);
        }
        Ok(())
    }
    #[must_use]
    pub fn fingerprint(&self) -> Sha256 {
        let mut bytes = b"musteragent.privacy.unexecuted-work-item-lineage.v1".to_vec();
        for id in [
            self.actor.tenant_id.into_uuid(),
            self.actor.principal_id.into_uuid(),
            self.erasure_job_id.into_uuid(),
            self.space_id.into_uuid(),
            self.agent_id.into_uuid(),
            self.work_item_id.into_uuid(),
            self.source_dialogue_id.into_uuid(),
            self.source_intent_id.into_uuid(),
        ] {
            bytes.extend_from_slice(id.as_bytes());
        }
        for number in [
            self.state_revision,
            self.control_revision,
            self.revision_count,
        ] {
            bytes.extend_from_slice(&number.to_be_bytes());
        }
        for root in &self.goal_content_refs {
            bytes.extend_from_slice(
                &ResourceRef::Content(root.clone())
                    .fingerprint()
                    .into_bytes(),
            );
        }
        bytes.extend_from_slice(self.reason_code.as_str().as_bytes());
        bytes.extend_from_slice(&self.frozen_at.unix_millis().to_be_bytes());
        Sha256::digest(bytes)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemErasureAdmission {
    pub disposition: ErasureAppendDisposition,
    pub lineage: WorkItemErasureLineage,
    pub job: ErasureJob,
}
pub trait WorkItemErasureAdmissionStore: Send + Sync {
    type Error: Send;
    fn request_work_item_erasure(
        &self,
        command: RequestWorkItemErasure,
    ) -> ApplicationFuture<'_, Result<WorkItemErasureAdmission, Self::Error>>;
}

pub fn build_work_item_erasure_facts(
    command: &RequestWorkItemErasure,
    lineage: &WorkItemErasureLineage,
) -> Result<(ErasureRequest, ErasurePlanSnapshot), ErasureContractError> {
    build_work_item_scope_facts(
        command,
        lineage,
        WORK_ITEM_ERASURE_SCHEMA,
        WORK_ITEM_ERASURE_POLICY,
    )
}

/// The persistence Owner selects this contract only after proving a canonical
/// reservation. It admits a fence, not proof that execution bytes were erased.
pub fn build_work_item_execution_erasure_facts(
    command: &RequestWorkItemErasure,
    lineage: &WorkItemErasureLineage,
) -> Result<(ErasureRequest, ErasurePlanSnapshot), ErasureContractError> {
    build_work_item_scope_facts(
        command,
        lineage,
        WORK_ITEM_EXECUTION_ERASURE_SCHEMA,
        WORK_ITEM_EXECUTION_ERASURE_POLICY,
    )
}

fn build_work_item_scope_facts(
    command: &RequestWorkItemErasure,
    lineage: &WorkItemErasureLineage,
    schema: &str,
    policy: &str,
) -> Result<(ErasureRequest, ErasurePlanSnapshot), ErasureContractError> {
    command.validate()?;
    lineage.validate()?;
    if command.actor != lineage.actor
        || command.space_id != lineage.space_id
        || command.agent_id != lineage.agent_id
        || command.work_item_id != lineage.work_item_id
        || command.expected_state_revision != lineage.state_revision
        || command.expected_control_revision != lineage.control_revision
        || command.reason_code != lineage.reason_code
        || command.requested_at != lineage.frozen_at
    {
        return Err(ErasureContractError::PlanLineageMismatch);
    }
    let request = ErasureRequest {
        schema_version: DottedKey::new(schema).expect("built-in schema"),
        erasure_job_id: lineage.erasure_job_id,
        tenant_id: command.actor.tenant_id,
        requested_by: command.actor,
        root_resource_refs: lineage
            .goal_content_refs
            .iter()
            .cloned()
            .map(ResourceRef::Content)
            .collect(),
        retention_policy_ref: DataRetentionPolicyRef::new(policy).expect("built-in policy"),
        idempotency_key: command.idempotency_key.clone(),
        requested_at: command.requested_at,
        complete_by: command.complete_by,
    };
    let plan = ErasurePlanSnapshot::new(
        request.schema_version.clone(),
        request.erasure_job_id,
        ErasurePlanVersion::new(1)?,
        request.root_resource_refs.clone(),
        ErasureDataClass::ALL.to_vec(),
        command.retention_exceptions.clone(),
        lineage.fingerprint(),
        command.requested_at,
    )?;
    Ok((request, plan))
}
