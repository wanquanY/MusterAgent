use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    Actor, AgentId, DottedKey, ErasureJobId, IdempotencyKey, MemoryId, MemoryVersionId, TenantId,
    UtcTimestamp,
};
use musteragent_runtime_contract::{ApplicationFuture, DataRetentionPolicyRef, ResourceRef};

use crate::{
    ErasureAppendDisposition, ErasureContractError, ErasureDataClass, ErasureJob,
    ErasurePlanSnapshot, ErasurePlanVersion, ErasureRequest, ErasureRetentionException,
};

pub const AGENT_MEMORY_ERASURE_SCHEMA: &str = "musteragent.privacy.agent-memory-erasure.v2";
pub const AGENT_MEMORY_RETENTION_POLICY: &str = "privacy.agent-memory.standard@1";
pub const AGENT_MEMORY_OBJECT_HISTORY_RETENTION_POLICY: &str =
    "privacy.agent-memory.object-history-expiry@1";
pub const AGENT_MEMORY_BACKUP_RETENTION_POLICY: &str = "privacy.agent-memory.backup-expiry@1";
pub const MAX_AGENT_MEMORY_ERASURE_CONTENT_ROOTS: usize = 4_096;
pub const AGENT_MEMORY_IMMEDIATE_ERASURE_DATA_CLASSES: [ErasureDataClass; 6] = [
    ErasureDataClass::ContentCiphertext,
    ErasureDataClass::Projection,
    ErasureDataClass::SnapshotInlineContent,
    ErasureDataClass::FullTextIndex,
    ErasureDataClass::EmbeddingIndex,
    ErasureDataClass::Cache,
];

/// Trusted command produced by the Learning management service after user
/// authorization. The client never supplies roots, data classes or a job id.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RequestAgentMemoryErasure {
    pub tenant_id: TenantId,
    pub requested_by: Actor,
    pub agent_id: AgentId,
    pub memory_id: MemoryId,
    pub expected_memory_version_id: MemoryVersionId,
    pub reason_code: DottedKey,
    pub idempotency_key: IdempotencyKey,
    pub requested_at: UtcTimestamp,
    pub complete_by: UtcTimestamp,
}

impl RequestAgentMemoryErasure {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        if self.requested_by.tenant_id != self.tenant_id {
            return Err(ErasureContractError::ActorTenantMismatch);
        }
        if self.requested_at >= self.complete_by {
            return Err(ErasureContractError::InvalidCompletionDeadline);
        }
        Ok(())
    }

    /// Match the user intent against immutable, mutually consistent admission
    /// facts. Service-generated attempt times are not part of the user intent:
    /// a lost-response retry must return the first deadline, never renew it.
    /// This does not weaken generic `ErasureRequest` fingerprint equality.
    pub fn matches_admitted_intent(
        &self,
        job: &ErasureJob,
        lineage: &AgentMemoryErasureLineage,
    ) -> Result<bool, ErasureContractError> {
        self.validate()?;
        job.validate()?;
        lineage.validate()?;
        job.latest_plan()?;
        let request = &job.request;
        let roots = request
            .root_resource_refs
            .iter()
            .map(|root| match root {
                ResourceRef::Content(content) => Some(content),
                _ => None,
            })
            .collect::<Option<std::collections::HashSet<_>>>();
        let lineage_roots = std::iter::once(&lineage.content_ref)
            .chain(&lineage.related_content_refs)
            .collect();
        let lineage_fingerprint = lineage.fingerprint();
        if request.tenant_id != lineage.tenant_id
            || request.erasure_job_id != lineage.erasure_job_id
            || request.requested_by != lineage.requested_by
            || request.requested_at != lineage.frozen_at
            || roots != Some(lineage_roots)
            || job
                .plans
                .iter()
                .any(|plan| plan.lineage_fingerprint != lineage_fingerprint)
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        Ok(self.tenant_id == lineage.tenant_id
            && self.requested_by == lineage.requested_by
            && self.agent_id == lineage.agent_id
            && self.memory_id == lineage.memory_id
            && self.expected_memory_version_id == lineage.memory_version_id
            && self.reason_code == lineage.reason_code
            && self.idempotency_key == request.idempotency_key)
    }
}

#[cfg(test)]
mod replay_tests;

/// Immutable proof that an Agent Memory was resolved to its complete Content
/// closure before the production read fence was committed. `content_ref` is
/// the exact requested head; `related_content_refs` contains every other
/// version, Draft, Candidate, Evaluation report and Approval prompt known at
/// admission time. No provider is allowed to infer or truncate this closure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryErasureLineage {
    pub tenant_id: TenantId,
    pub erasure_job_id: ErasureJobId,
    pub agent_id: AgentId,
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
    pub content_ref: ContentRef,
    pub related_content_refs: Vec<ContentRef>,
    pub requested_by: Actor,
    pub reason_code: DottedKey,
    pub frozen_at: UtcTimestamp,
}

impl AgentMemoryErasureLineage {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        if self.related_content_refs.len() + 1 > MAX_AGENT_MEMORY_ERASURE_CONTENT_ROOTS {
            return Err(ErasureContractError::TooManyRootResources);
        }
        let mut fingerprints = std::collections::BTreeSet::new();
        for content in std::iter::once(&self.content_ref).chain(&self.related_content_refs) {
            if !fingerprints.insert(
                ResourceRef::Content(content.clone())
                    .fingerprint()
                    .into_bytes(),
            ) {
                return Err(ErasureContractError::DuplicateRootResource);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn root_content_refs(&self) -> Vec<ContentRef> {
        std::iter::once(self.content_ref.clone())
            .chain(self.related_content_refs.iter().cloned())
            .collect()
    }

    #[must_use]
    pub fn fingerprint(&self) -> Sha256 {
        // Preserve the v1 fingerprint for migrated single-root lineages.
        let mut bytes = if self.related_content_refs.is_empty() {
            b"musteragent.legacy.privacy.agent-memory-lineage.v1".to_vec()
        } else {
            b"musteragent.legacy.privacy.agent-memory-lineage.v2".to_vec()
        };
        bytes.extend_from_slice(self.tenant_id.into_uuid().as_bytes());
        bytes.extend_from_slice(self.erasure_job_id.into_uuid().as_bytes());
        bytes.extend_from_slice(self.agent_id.into_uuid().as_bytes());
        bytes.extend_from_slice(self.memory_id.into_uuid().as_bytes());
        bytes.extend_from_slice(self.memory_version_id.into_uuid().as_bytes());
        bytes.extend_from_slice(
            &ResourceRef::Content(self.content_ref.clone())
                .fingerprint()
                .into_bytes(),
        );
        for content in &self.related_content_refs {
            bytes.extend_from_slice(
                &ResourceRef::Content(content.clone())
                    .fingerprint()
                    .into_bytes(),
            );
        }
        bytes.extend_from_slice(self.requested_by.principal_id.into_uuid().as_bytes());
        bytes.push(match self.requested_by.principal_kind {
            musteragent_identity::PrincipalKind::User => 1,
            musteragent_identity::PrincipalKind::Service => 2,
            musteragent_identity::PrincipalKind::System => 3,
        });
        bytes.extend_from_slice(self.reason_code.as_str().as_bytes());
        bytes.extend_from_slice(&self.frozen_at.unix_millis().to_be_bytes());
        Sha256::digest(bytes)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryErasureAdmission {
    pub disposition: ErasureAppendDisposition,
    pub lineage: AgentMemoryErasureLineage,
    pub job: ErasureJob,
}

/// Atomic persistence boundary: resolve current Memory head, install the
/// production read fence, persist immutable lineage, request, plan and Fenced
/// transition in one transaction.
pub trait AgentMemoryErasureAdmissionStore: Send + Sync {
    type Error: Send;

    fn request_agent_memory_erasure(
        &self,
        command: RequestAgentMemoryErasure,
    ) -> ApplicationFuture<'_, Result<AgentMemoryErasureAdmission, Self::Error>>;

    fn load_agent_memory_erasure_lineage(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> ApplicationFuture<'_, Result<Option<AgentMemoryErasureLineage>, Self::Error>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListAgentMemoryErasureJobs {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub after: Option<crate::ErasureJobCursor>,
    pub limit: crate::ErasureQueryLimit,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagedAgentMemoryErasureJob {
    pub job: ErasureJob,
    pub lineage: AgentMemoryErasureLineage,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryErasureJobPage {
    pub jobs: Vec<ManagedAgentMemoryErasureJob>,
    pub next_cursor: Option<crate::ErasureJobCursor>,
}
/// An account- and Agent-filtered projection over the original erasure jobs.
/// Filtering occurs before pagination; callers never scan a tenant-wide page.
pub trait AgentMemoryErasureQueryStore: Send + Sync {
    type Error: Send;
    fn list_agent_memory_erasure_jobs(
        &self,
        query: ListAgentMemoryErasureJobs,
    ) -> ApplicationFuture<'_, Result<AgentMemoryErasureJobPage, Self::Error>>;
}

pub fn build_agent_memory_erasure_facts(
    command: &RequestAgentMemoryErasure,
    lineage: &AgentMemoryErasureLineage,
) -> Result<(ErasureRequest, ErasurePlanSnapshot), ErasureContractError> {
    command.validate()?;
    lineage.validate()?;
    if lineage.tenant_id != command.tenant_id
        || lineage.agent_id != command.agent_id
        || lineage.memory_id != command.memory_id
        || lineage.memory_version_id != command.expected_memory_version_id
        || lineage.requested_by != command.requested_by
        || lineage.reason_code != command.reason_code
        || lineage.frozen_at != command.requested_at
    {
        return Err(ErasureContractError::PlanLineageMismatch);
    }
    let root_resource_refs = lineage
        .root_content_refs()
        .into_iter()
        .map(ResourceRef::Content)
        .collect::<Vec<_>>();
    let request = ErasureRequest {
        schema_version: DottedKey::new(AGENT_MEMORY_ERASURE_SCHEMA)
            .expect("built-in Agent Memory erasure schema is valid"),
        erasure_job_id: lineage.erasure_job_id,
        tenant_id: command.tenant_id,
        requested_by: command.requested_by,
        root_resource_refs: root_resource_refs.clone(),
        retention_policy_ref: DataRetentionPolicyRef::new(AGENT_MEMORY_RETENTION_POLICY)
            .expect("built-in Agent Memory retention policy is valid"),
        idempotency_key: command.idempotency_key.clone(),
        requested_at: command.requested_at,
        complete_by: command.complete_by,
    };
    let plan = ErasurePlanSnapshot::new(
        request.schema_version.clone(),
        request.erasure_job_id,
        ErasurePlanVersion::new(1).expect("initial plan version is valid"),
        root_resource_refs,
        ErasureDataClass::ALL.to_vec(),
        vec![
            ErasureRetentionException {
                data_class: ErasureDataClass::ObjectHistory,
                policy_ref: DataRetentionPolicyRef::new(
                    AGENT_MEMORY_OBJECT_HISTORY_RETENTION_POLICY,
                )
                .expect("built-in object-history retention policy is valid"),
                owner_principal_id: command.requested_by.principal_id,
                expires_at: command.complete_by,
            },
            ErasureRetentionException {
                data_class: ErasureDataClass::Backup,
                policy_ref: DataRetentionPolicyRef::new(AGENT_MEMORY_BACKUP_RETENTION_POLICY)
                    .expect("built-in backup retention policy is valid"),
                owner_principal_id: command.requested_by.principal_id,
                expires_at: command.complete_by,
            },
        ],
        lineage.fingerprint(),
        command.requested_at,
    )?;
    Ok((request, plan))
}
