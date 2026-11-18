use musteragent_content_ref::Sha256;
use musteragent_identity::{ErasureJobId, TenantId, UtcTimestamp};
use musteragent_runtime_content::{ContentStore, ContentStoreError};
use musteragent_runtime_contract::ResourceRef;
use thiserror::Error;

use crate::{
    ErasureAppendDisposition, ErasureContractError, ErasureDataClass, ErasureJob, ErasureJobStatus,
    ErasureJobStore, ErasureMutationReceipt, ErasureTransition, ErasureTransitionSequence,
};

/// Compatibility name: existing Memory consumers and proof bytes are unchanged.
pub type AgentMemoryErasureExecutor<'a, S, C> = ErasureExecutor<'a, S, C>;

/// Executes one fenced erasure aggregate without weakening consumer-owned
/// provider verification. The Content Store destroys bytes first; the
/// ErasureJobStore then atomically verifies every remaining provider and
/// persists the terminal receipt.
pub struct ErasureExecutor<'a, S, C: ?Sized> {
    jobs: &'a S,
    content: &'a C,
}

impl<'a, S, C> ErasureExecutor<'a, S, C>
where
    S: ErasureJobStore,
    C: ContentStore + ?Sized,
{
    #[must_use]
    pub const fn new(jobs: &'a S, content: &'a C) -> Self {
        Self { jobs, content }
    }

    pub async fn execute(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
        started_at: UtcTimestamp,
    ) -> Result<ErasureMutationReceipt, AgentMemoryErasureExecutionError<S::Error>> {
        let mut job = self
            .jobs
            .load(tenant_id, erasure_job_id)
            .await
            .map_err(AgentMemoryErasureExecutionError::Store)?
            .ok_or(AgentMemoryErasureExecutionError::JobNotFound)?;
        job.validate()?;
        if job.request.tenant_id != tenant_id {
            return Err(AgentMemoryErasureExecutionError::IdentityMismatch);
        }
        if job.status().is_some_and(ErasureJobStatus::is_terminal) {
            return Ok(ErasureMutationReceipt {
                disposition: ErasureAppendDisposition::Duplicate,
                job,
            });
        }
        let work_item = crate::is_work_item_erasure_schema(job.request.schema_version.as_str());
        // Keep historical Memory deadline semantics. A WorkItem retry must
        // still destroy online bytes after an operational deadline was missed.
        if started_at < job.request.requested_at
            || (!work_item && started_at >= job.request.complete_by)
        {
            return Err(AgentMemoryErasureExecutionError::InvalidCompletionTime);
        }

        let accepted_request = job.request.clone();
        let accepted_fingerprint = job.request_fingerprint;
        job = self
            .jobs
            .prepare_execution(job, started_at)
            .await
            .map_err(AgentMemoryErasureExecutionError::Store)?;
        job.validate()?;
        if job.request != accepted_request || job.request_fingerprint != accepted_fingerprint {
            return Err(AgentMemoryErasureExecutionError::IdentityMismatch);
        }
        job = self.enter_executing(job, started_at).await?;
        let plan = job.latest_plan()?.clone();
        if plan
            .data_classes
            .contains(&ErasureDataClass::ContentCiphertext)
        {
            let roots = self
                .jobs
                .resolve_execution_roots(tenant_id, plan.clone())
                .await
                .map_err(AgentMemoryErasureExecutionError::Store)?;
            for root in &roots {
                let ResourceRef::Content(content) = root else {
                    return Err(AgentMemoryErasureExecutionError::UnsupportedContentRoot);
                };
                if let Err(source) = self.content.delete(tenant_id, content).await {
                    return self.record_retry(job, started_at, source).await;
                }
            }
        }

        if work_item
            && plan
                .retention_exceptions
                .iter()
                .any(|e| e.expires_at <= started_at)
        {
            self.jobs
                .append_transition(
                    tenant_id,
                    erasure_job_id,
                    ErasureTransition {
                        sequence: next_sequence(&job)?,
                        status: ErasureJobStatus::AwaitingBackupExpiry,
                        occurred_at: started_at,
                    },
                )
                .await
                .map_err(AgentMemoryErasureExecutionError::Store)?;
            // Elapsed time is not provider evidence. Bytes are purged, but the
            // original aggregate remains nonterminal for retention reconciliation.
            return Err(AgentMemoryErasureExecutionError::RetentionReconciliationRequired);
        }

        self.jobs
            .complete_execution(crate::CompleteErasureExecution::from_job(&job, started_at)?)
            .await
            .map_err(AgentMemoryErasureExecutionError::Store)
    }

    async fn enter_executing(
        &self,
        job: ErasureJob,
        occurred_at: UtcTimestamp,
    ) -> Result<ErasureJob, AgentMemoryErasureExecutionError<S::Error>> {
        match job.status() {
            Some(ErasureJobStatus::Executing) => Ok(job),
            Some(
                ErasureJobStatus::Fenced
                | ErasureJobStatus::AwaitingBackupExpiry
                | ErasureJobStatus::RetryScheduled,
            ) => self
                .jobs
                .append_transition(
                    job.request.tenant_id,
                    job.request.erasure_job_id,
                    ErasureTransition {
                        sequence: next_sequence(&job)?,
                        status: ErasureJobStatus::Executing,
                        occurred_at,
                    },
                )
                .await
                .map(|mutation| mutation.job)
                .map_err(AgentMemoryErasureExecutionError::Store),
            _ => Err(AgentMemoryErasureExecutionError::JobNotFenced),
        }
    }

    async fn record_retry(
        &self,
        job: ErasureJob,
        occurred_at: UtcTimestamp,
        content: ContentStoreError,
    ) -> Result<ErasureMutationReceipt, AgentMemoryErasureExecutionError<S::Error>> {
        let transition = ErasureTransition {
            sequence: next_sequence(&job)?,
            status: ErasureJobStatus::RetryScheduled,
            occurred_at,
        };
        match self
            .jobs
            .append_transition(
                job.request.tenant_id,
                job.request.erasure_job_id,
                transition,
            )
            .await
        {
            Ok(_) => Err(AgentMemoryErasureExecutionError::Content(content)),
            Err(store) => {
                Err(AgentMemoryErasureExecutionError::ContentRetryTransition { content, store })
            }
        }
    }
}

fn next_sequence<E>(
    job: &ErasureJob,
) -> Result<ErasureTransitionSequence, AgentMemoryErasureExecutionError<E>> {
    let current = job
        .transitions
        .last()
        .ok_or(AgentMemoryErasureExecutionError::JobNotFenced)?
        .sequence
        .get();
    ErasureTransitionSequence::new(
        current
            .checked_add(1)
            .ok_or(AgentMemoryErasureExecutionError::SequenceExhausted)?,
    )
    .map_err(Into::into)
}

pub fn erasure_proof_digest(job: &ErasureJob, completed_at: UtcTimestamp) -> Sha256 {
    let plan = job.latest_plan().expect("validated job has a plan");
    let mut proof =
        if job.request.schema_version.as_str() == crate::WORK_ITEM_EXECUTION_ERASURE_SCHEMA {
            b"musteragent.privacy.work-item-execution-erasure-proof.v2".to_vec()
        } else if job.request.schema_version.as_str() == crate::WORK_ITEM_ERASURE_SCHEMA {
            b"musteragent.privacy.unexecuted-work-item-erasure-proof.v1".to_vec()
        } else if job
            .request
            .schema_version
            .as_str()
            .starts_with("musteragent.")
            && !job
                .request
                .schema_version
                .as_str()
                .starts_with("musteragent.legacy.")
        {
            b"musteragent.privacy.agent-memory-erasure-proof.v2".to_vec()
        } else {
            b"musteragent.legacy.privacy.agent-memory-erasure-proof.v1".to_vec()
        };
    proof.extend_from_slice(job.request.tenant_id.into_uuid().as_bytes());
    proof.extend_from_slice(job.request.erasure_job_id.into_uuid().as_bytes());
    proof.extend_from_slice(&plan.plan_hash.into_bytes());
    for root in &plan.root_resource_refs {
        proof.extend_from_slice(&root.fingerprint().into_bytes());
    }
    for data_class in &plan.data_classes {
        proof.extend_from_slice(&data_class.code().to_be_bytes());
    }
    for exception in &plan.retention_exceptions {
        proof.extend_from_slice(&exception.data_class.code().to_be_bytes());
        proof.extend_from_slice(exception.policy_ref.as_str().as_bytes());
        proof.extend_from_slice(exception.owner_principal_id.into_uuid().as_bytes());
        proof.extend_from_slice(&exception.expires_at.unix_millis().to_be_bytes());
    }
    proof.extend_from_slice(&completed_at.unix_millis().to_be_bytes());
    Sha256::digest(proof)
}

#[derive(Debug, Error)]
pub enum AgentMemoryErasureExecutionError<E> {
    #[error("privacy-erasure persistence failed: {0:?}")]
    Store(E),
    #[error("privacy-erasure content destruction failed: {0}")]
    Content(ContentStoreError),
    #[error(
        "privacy-erasure content destruction and durable retry transition both failed: content={content}, store={store:?}"
    )]
    ContentRetryTransition {
        content: ContentStoreError,
        store: E,
    },
    #[error("privacy-erasure job does not exist")]
    JobNotFound,
    #[error("privacy-erasure job tenant identity drifted")]
    IdentityMismatch,
    #[error("privacy-erasure job is not fenced for execution")]
    JobNotFenced,
    #[error("privacy-erasure completion time is outside the admitted deadline")]
    InvalidCompletionTime,
    #[error("retention deadline elapsed; actual provider reconciliation is required")]
    RetentionReconciliationRequired,
    #[error("ContentCiphertext erasure supports exact Content roots only")]
    UnsupportedContentRoot,
    #[error("privacy-erasure transition sequence is exhausted")]
    SequenceExhausted,
    #[error(transparent)]
    Contract(#[from] ErasureContractError),
}
