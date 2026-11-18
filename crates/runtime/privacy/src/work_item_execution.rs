//! Versioned provider coverage for a permanently fenced Task. The user roots
//! stay unchanged; the second original plan binds the quiescent derived set.
use crate::{
    ErasureContractError, ErasureJob, ErasurePlanSnapshot, ErasurePlanVersion,
    WORK_ITEM_EXECUTION_ERASURE_SCHEMA,
};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, ConversationId, ErasureJobId, HomeEpoch, RunId, UtcTimestamp, WorkItemId,
};
use musteragent_runtime_contract::ResourceRef;

/// The request is assembled from the original lineage plus an exact typed
/// Task session. The cancellation Owner must recheck it at the Run commit.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemErasureCancellation {
    pub actor: Actor,
    pub job_id: ErasureJobId,
    pub task_id: WorkItemId,
    pub control_revision: u64,
    pub run_id: RunId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub requested_at: UtcTimestamp,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("original Task Run cancellation is unavailable or has not settled")]
pub struct WorkItemErasureCancellationError;

/// Adapter over the existing Runtime CancelRun application, never a second
/// producer state machine. A successful call acknowledges cancellation only;
/// the original Erasure Owner must still query actual Run quiescence.
pub trait WorkItemErasureRunControl: Send + Sync {
    fn cancel(
        &self,
        request: WorkItemErasureCancellation,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<(), WorkItemErasureCancellationError>,
    >;
}

pub const WORK_ITEM_EXECUTION_COVERAGE_VERSION: u16 = 1;
pub const MAX_WORK_ITEM_ERASURE_EXECUTIONS: usize = 4096;
pub const MAX_WORK_ITEM_ERASURE_CONTENTS: usize = 65_536;

/// Identities come only from the original immutable reservation and typed
/// session, never from a caller-selected Run or the source parent Run.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemErasureExecution {
    pub run_id: RunId,
    pub conversation_id: ConversationId,
    pub goal_revision: u64,
    pub control_revision: u64,
    pub assignment_epoch: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemExecutionErasureCoverage {
    pub scope_fingerprint: Sha256,
    /// Canonical Run ID order, including reserved executions that never started.
    pub executions: Vec<WorkItemErasureExecution>,
    /// Canonical Content ID order. Includes original goal roots and only
    /// exclusively owned derived content; shared inputs are not copied here.
    pub contents: Vec<ContentRef>,
    /// Exact original producer cut, sorted by invocation ID. Empty preserves
    /// the released v1 coverage bytes; nonempty is forward coverage v2.
    pub source_evidence: Vec<crate::SourceEvidenceProducer>,
    pub frozen_at: UtcTimestamp,
}

impl WorkItemExecutionErasureCoverage {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        if self.executions.is_empty()
            || self.executions.len() > MAX_WORK_ITEM_ERASURE_EXECUTIONS
            || self.contents.is_empty()
            || self.contents.len() > MAX_WORK_ITEM_ERASURE_CONTENTS
            || self
                .executions
                .windows(2)
                .any(|w| w[0].run_id >= w[1].run_id)
            || self
                .contents
                .windows(2)
                .any(|w| w[0].content_id >= w[1].content_id)
            || self
                .executions
                .iter()
                .any(|e| e.goal_revision == 0 || e.control_revision == 0 || e.assignment_epoch == 0)
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        let conversations: std::collections::BTreeSet<_> =
            self.executions.iter().map(|e| e.conversation_id).collect();
        if conversations.len() != self.executions.len() {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        let executions: std::collections::BTreeSet<_> = self
            .executions
            .iter()
            .map(|e| (e.run_id, e.conversation_id))
            .collect();
        if self.source_evidence.len() > crate::MAX_SOURCE_EVIDENCE_PRODUCERS
            || self
                .source_evidence
                .windows(2)
                .any(|w| w[0].invocation_id >= w[1].invocation_id)
            || self
                .source_evidence
                .iter()
                .any(|p| !executions.contains(&(p.run_id, p.conversation_id)))
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        Ok(())
    }
    pub fn version(&self) -> u16 {
        if self.source_evidence.is_empty() {
            1
        } else {
            2
        }
    }

    pub fn fingerprint(&self) -> Result<Sha256, ErasureContractError> {
        self.validate()?;
        let mut bytes = if self.version() == 1 {
            b"musteragent.privacy.work-item-execution-coverage.v1\0".to_vec()
        } else {
            b"musteragent.privacy.work-item-execution-coverage.v2\0".to_vec()
        };
        bytes.extend_from_slice(&self.version().to_be_bytes());
        bytes.extend_from_slice(&self.scope_fingerprint.into_bytes());
        bytes.extend_from_slice(&(self.executions.len() as u64).to_be_bytes());
        for execution in &self.executions {
            bytes.extend_from_slice(execution.run_id.into_uuid().as_bytes());
            bytes.extend_from_slice(execution.conversation_id.into_uuid().as_bytes());
            for revision in [
                execution.goal_revision,
                execution.control_revision,
                execution.assignment_epoch,
            ] {
                bytes.extend_from_slice(&revision.to_be_bytes());
            }
        }
        bytes.extend_from_slice(&(self.contents.len() as u64).to_be_bytes());
        for content in &self.contents {
            bytes.extend_from_slice(
                &ResourceRef::Content(content.clone())
                    .fingerprint()
                    .into_bytes(),
            );
        }
        if self.version() == 2 {
            bytes.extend_from_slice(&(self.source_evidence.len() as u64).to_be_bytes());
            for p in &self.source_evidence {
                bytes.extend_from_slice(p.run_id.into_uuid().as_bytes());
                bytes.extend_from_slice(p.conversation_id.into_uuid().as_bytes());
                bytes.extend_from_slice(p.invocation_id.into_uuid().as_bytes());
            }
        }
        bytes.extend_from_slice(&self.frozen_at.unix_millis().to_be_bytes());
        Ok(Sha256::digest(bytes))
    }
}

/// Freeze only after the original provider proves every reserved Run is either
/// never admitted or fully quiescent. That proof is the persistence Owner's
/// responsibility; this constructor cannot turn an arbitrary Run ID into it.
pub fn build_work_item_execution_plan(
    job: &ErasureJob,
    coverage: &WorkItemExecutionErasureCoverage,
) -> Result<ErasurePlanSnapshot, ErasureContractError> {
    job.validate()?;
    coverage.validate()?;
    let first = job
        .plans
        .first()
        .ok_or(ErasureContractError::PlanRequired)?;
    if job.request.schema_version.as_str() != WORK_ITEM_EXECUTION_ERASURE_SCHEMA
        || first.schema_version != job.request.schema_version
        || first.lineage_fingerprint != coverage.scope_fingerprint
        || coverage.frozen_at < first.frozen_at
        || job.plans.len() > 2
        || job.request.root_resource_refs.iter().any(|root| {
            !matches!(root, ResourceRef::Content(content) if coverage.contents.contains(content))
        })
    {
        return Err(ErasureContractError::PlanLineageMismatch);
    }
    let plan = ErasurePlanSnapshot::new(
        first.schema_version.clone(),
        first.erasure_job_id,
        ErasurePlanVersion::new(2)?,
        first.root_resource_refs.clone(),
        first.data_classes.clone(),
        first.retention_exceptions.clone(),
        coverage.fingerprint()?,
        coverage.frozen_at,
    )?;
    if job.plans.get(1).is_some_and(|existing| existing != &plan) {
        return Err(ErasureContractError::PlanLineageMismatch);
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::MediaType;
    use musteragent_identity::ContentId;
    fn fixture() -> WorkItemExecutionErasureCoverage {
        WorkItemExecutionErasureCoverage {
            scope_fingerprint: Sha256::digest(b"exact actor and task lineage"),
            executions: vec![WorkItemErasureExecution {
                run_id: RunId::generate(),
                conversation_id: ConversationId::generate(),
                goal_revision: 1,
                control_revision: 2,
                assignment_epoch: 3,
            }],
            contents: vec![ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain").unwrap(),
                byte_length: 4,
                sha256: Sha256::digest(b"task"),
            }],
            source_evidence: Vec::new(),
            frozen_at: UtcTimestamp::from_unix_millis(1000),
        }
    }
    #[test]
    fn coverage_binds_scope_execution_generation_and_exact_content() {
        let coverage = fixture();
        let original = coverage.fingerprint().unwrap();
        for index in 0..8 {
            let mut changed = coverage.clone();
            match index {
                0 => changed.scope_fingerprint = Sha256::digest(b"other task"),
                1 => changed.executions[0].run_id = RunId::generate(),
                2 => changed.executions[0].conversation_id = ConversationId::generate(),
                3 => changed.executions[0].goal_revision += 1,
                4 => changed.executions[0].control_revision += 1,
                5 => changed.executions[0].assignment_epoch += 1,
                6 => changed.contents[0].sha256 = Sha256::digest(b"edit"),
                _ => changed.frozen_at = UtcTimestamp::from_unix_millis(1001),
            }
            assert_ne!(original, changed.fingerprint().unwrap());
        }
    }
    #[test]
    fn coverage_rejects_ambiguous_identity_and_noncanonical_sets() {
        let mut coverage = fixture();
        coverage.executions.push(coverage.executions[0].clone());
        assert!(coverage.validate().is_err());
        coverage = fixture();
        coverage.contents.push(coverage.contents[0].clone());
        assert!(coverage.validate().is_err());
        coverage = fixture();
        coverage.executions[0].assignment_epoch = 0;
        assert!(coverage.validate().is_err());
        coverage = fixture();
        coverage.executions.clear();
        assert!(coverage.validate().is_err());
    }
    #[test]
    fn execution_plan_keeps_accepted_roots_and_freezes_only_derived_provider_scope() {
        use crate::*;
        use musteragent_identity::*;
        use musteragent_runtime_contract::DataRetentionPolicyRef;
        let actor = Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let mut coverage = fixture();
        let command = RequestWorkItemErasure {
            actor,
            space_id: OwnerSpaceId::generate(),
            agent_id: AgentId::generate(),
            work_item_id: WorkItemId::generate(),
            expected_state_revision: 1,
            expected_control_revision: 1,
            reason_code: DottedKey::new("user.erase-task").unwrap(),
            idempotency_key: IdempotencyKey::new("erase-task").unwrap(),
            requested_at: UtcTimestamp::from_unix_millis(100),
            complete_by: UtcTimestamp::from_unix_millis(200),
            retention_exceptions: [ErasureDataClass::ObjectHistory, ErasureDataClass::Backup]
                .into_iter()
                .map(|data_class| ErasureRetentionException {
                    data_class,
                    policy_ref: DataRetentionPolicyRef::new("test.provider@1").unwrap(),
                    owner_principal_id: actor.principal_id,
                    expires_at: UtcTimestamp::from_unix_millis(5000),
                })
                .collect(),
        };
        let lineage = WorkItemErasureLineage {
            actor,
            erasure_job_id: ErasureJobId::generate(),
            space_id: command.space_id,
            agent_id: command.agent_id,
            work_item_id: command.work_item_id,
            source_dialogue_id: AgentDialogueId::generate(),
            source_intent_id: UserIntentId::generate(),
            state_revision: 1,
            control_revision: 1,
            revision_count: 1,
            goal_content_refs: coverage.contents.clone(),
            reason_code: command.reason_code.clone(),
            frozen_at: command.requested_at,
        };
        let (request, first) = build_work_item_execution_erasure_facts(&command, &lineage).unwrap();
        let mut job = ErasureJob {
            request_fingerprint: request.fingerprint().unwrap(),
            request: request.clone(),
            plans: vec![first.clone()],
            transitions: [
                ErasureJobStatus::PendingPlan,
                ErasureJobStatus::Planned,
                ErasureJobStatus::Fenced,
            ]
            .into_iter()
            .enumerate()
            .map(|(i, status)| ErasureTransition {
                sequence: ErasureTransitionSequence::new(i as u64 + 1).unwrap(),
                status,
                occurred_at: command.requested_at,
            })
            .collect(),
            receipt: None,
        };
        coverage.scope_fingerprint = lineage.fingerprint();
        let derived = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/plain").unwrap(),
            byte_length: 7,
            sha256: Sha256::digest(b"derived"),
        };
        coverage.contents.push(derived.clone());
        coverage.contents.sort_by_key(|r| r.content_id);
        let second = build_work_item_execution_plan(&job, &coverage).unwrap();
        assert_eq!(second.root_resource_refs, request.root_resource_refs);
        assert_ne!(second.lineage_fingerprint, first.lineage_fingerprint);
        job.plans.push(second.clone());
        job.validate().unwrap();
        assert_eq!(job.request, request);
        assert_eq!(job.plans[0], first);
        assert_eq!(
            build_work_item_execution_plan(&job, &coverage).unwrap(),
            second
        );
        assert!(command.matches_admitted_intent(&job, &lineage).unwrap());
        let mut changed = coverage.clone();
        changed.contents.retain(|c| c != &derived);
        assert!(build_work_item_execution_plan(&job, &changed).is_err());
        let (legacy_request, legacy_plan) =
            build_work_item_erasure_facts(&command, &lineage).unwrap();
        job.request = legacy_request;
        job.request_fingerprint = job.request.fingerprint().unwrap();
        job.plans = vec![legacy_plan];
        assert!(build_work_item_execution_plan(&job, &coverage).is_err());
    }
}
