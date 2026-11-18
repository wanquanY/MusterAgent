use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroU64;

use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, DottedKey, ErasureJobId, ErasureReceiptId, IdempotencyKey, PrincipalId, TenantId,
    UtcTimestamp,
};
use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};
use thiserror::Error;

use crate::fingerprint::Fingerprint;

/// A permanent Memory erasure can legitimately cover thousands of immutable
/// versions and governance artifacts. The contract stays bounded for wire and
/// persistence safety, but must never force a provider to truncate lineage.
pub const MAX_ERASURE_ROOT_RESOURCES: usize = 4_096;
pub const MAX_ERASURE_RETENTION_EXCEPTIONS: usize = ErasureDataClass::ALL.len();

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ErasurePlanVersion(NonZeroU64);

impl ErasurePlanVersion {
    pub fn new(value: u64) -> Result<Self, ErasureContractError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ErasureContractError::InvalidPlanVersion)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ErasureTransitionSequence(NonZeroU64);

impl ErasureTransitionSequence {
    pub fn new(value: u64) -> Result<Self, ErasureContractError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ErasureContractError::InvalidTransitionSequence)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ErasureDataClass {
    ContentCiphertext,
    Projection,
    SnapshotInlineContent,
    FullTextIndex,
    EmbeddingIndex,
    Cache,
    ObjectHistory,
    Backup,
}

impl ErasureDataClass {
    pub const ALL: [Self; 8] = [
        Self::ContentCiphertext,
        Self::Projection,
        Self::SnapshotInlineContent,
        Self::FullTextIndex,
        Self::EmbeddingIndex,
        Self::Cache,
        Self::ObjectHistory,
        Self::Backup,
    ];

    #[must_use]
    pub const fn code(self) -> i16 {
        match self {
            Self::ContentCiphertext => 1,
            Self::Projection => 2,
            Self::SnapshotInlineContent => 3,
            Self::FullTextIndex => 4,
            Self::EmbeddingIndex => 5,
            Self::Cache => 6,
            Self::ObjectHistory => 7,
            Self::Backup => 8,
        }
    }

    pub fn from_code(value: i16) -> Result<Self, ErasureContractError> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.code() == value)
            .ok_or(ErasureContractError::UnknownDataClass)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ErasureJobStatus {
    PendingPlan,
    Planned,
    Fenced,
    Executing,
    AwaitingBackupExpiry,
    RetryScheduled,
    Completed,
    CompletedWithRetentionException,
}

impl ErasureJobStatus {
    pub const ALL: [Self; 8] = [
        Self::PendingPlan,
        Self::Planned,
        Self::Fenced,
        Self::Executing,
        Self::AwaitingBackupExpiry,
        Self::RetryScheduled,
        Self::Completed,
        Self::CompletedWithRetentionException,
    ];

    #[must_use]
    pub const fn code(self) -> i16 {
        match self {
            Self::PendingPlan => 1,
            Self::Planned => 2,
            Self::Fenced => 3,
            Self::Executing => 4,
            Self::AwaitingBackupExpiry => 5,
            Self::RetryScheduled => 6,
            Self::Completed => 7,
            Self::CompletedWithRetentionException => 8,
        }
    }

    pub fn from_code(value: i16) -> Result<Self, ErasureContractError> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.code() == value)
            .ok_or(ErasureContractError::UnknownJobStatus)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::CompletedWithRetentionException
        )
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::PendingPlan, Self::Planned)
                | (Self::Planned, Self::Fenced)
                | (Self::Fenced, Self::Executing)
                | (Self::Executing, Self::AwaitingBackupExpiry)
                | (Self::Executing, Self::RetryScheduled)
                | (Self::Executing, Self::Completed)
                | (Self::Executing, Self::CompletedWithRetentionException)
                | (Self::AwaitingBackupExpiry, Self::Executing)
                | (Self::AwaitingBackupExpiry, Self::Completed)
                | (
                    Self::AwaitingBackupExpiry,
                    Self::CompletedWithRetentionException
                )
                | (Self::RetryScheduled, Self::Executing)
        )
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ErasureReceiptOutcome {
    FullyErased,
    ErasedWithRetentionException,
}

impl ErasureReceiptOutcome {
    #[must_use]
    pub const fn code(self) -> i16 {
        match self {
            Self::FullyErased => 1,
            Self::ErasedWithRetentionException => 2,
        }
    }

    pub fn from_code(value: i16) -> Result<Self, ErasureContractError> {
        match value {
            1 => Ok(Self::FullyErased),
            2 => Ok(Self::ErasedWithRetentionException),
            _ => Err(ErasureContractError::UnknownReceiptOutcome),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasureRequest {
    pub schema_version: DottedKey,
    pub erasure_job_id: ErasureJobId,
    pub tenant_id: TenantId,
    pub requested_by: Actor,
    pub root_resource_refs: Vec<ResourceRef>,
    pub retention_policy_ref: DataRetentionPolicyRef,
    pub idempotency_key: IdempotencyKey,
    pub requested_at: UtcTimestamp,
    pub complete_by: UtcTimestamp,
}

impl ErasureRequest {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        validate_root_resources(&self.root_resource_refs)?;
        if self.requested_by.tenant_id != self.tenant_id {
            return Err(ErasureContractError::ActorTenantMismatch);
        }
        if self.requested_at >= self.complete_by {
            return Err(ErasureContractError::InvalidCompletionDeadline);
        }
        Ok(())
    }

    pub fn fingerprint(&self) -> Result<Sha256, ErasureContractError> {
        self.validate()?;
        let domain = if self.schema_version.as_str().starts_with("musteragent.")
            && !self
                .schema_version
                .as_str()
                .starts_with("musteragent.legacy.")
        {
            b"musteragent.privacy.erasure.request.v2".as_slice()
        } else {
            b"musteragent.legacy.privacy.erasure.request.v1".as_slice()
        };
        let mut value = Fingerprint::new(domain);
        value.uuid(self.erasure_job_id.into_uuid());
        value.uuid(self.tenant_id.into_uuid());
        value.actor(self.requested_by);
        value.resources(&self.root_resource_refs);
        value.retention(&self.retention_policy_ref);
        value.idempotency_key(&self.idempotency_key);
        value.timestamp(self.requested_at);
        value.timestamp(self.complete_by);
        Ok(value.finish())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasureRetentionException {
    pub data_class: ErasureDataClass,
    pub policy_ref: DataRetentionPolicyRef,
    pub owner_principal_id: PrincipalId,
    pub expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasurePlanSnapshot {
    pub schema_version: DottedKey,
    pub erasure_job_id: ErasureJobId,
    pub plan_version: ErasurePlanVersion,
    pub root_resource_refs: Vec<ResourceRef>,
    pub data_classes: Vec<ErasureDataClass>,
    pub retention_exceptions: Vec<ErasureRetentionException>,
    pub lineage_fingerprint: Sha256,
    pub plan_hash: Sha256,
    pub frozen_at: UtcTimestamp,
}

impl ErasurePlanSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema_version: DottedKey,
        erasure_job_id: ErasureJobId,
        plan_version: ErasurePlanVersion,
        root_resource_refs: Vec<ResourceRef>,
        data_classes: Vec<ErasureDataClass>,
        retention_exceptions: Vec<ErasureRetentionException>,
        lineage_fingerprint: Sha256,
        frozen_at: UtcTimestamp,
    ) -> Result<Self, ErasureContractError> {
        let mut value = Self {
            schema_version,
            erasure_job_id,
            plan_version,
            root_resource_refs,
            data_classes,
            retention_exceptions,
            lineage_fingerprint,
            plan_hash: Sha256::digest([]),
            frozen_at,
        };
        value.validate_without_hash()?;
        value.plan_hash = value.compute_hash();
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), ErasureContractError> {
        self.validate_without_hash()?;
        if self.plan_hash != self.compute_hash() {
            return Err(ErasureContractError::PlanHashMismatch);
        }
        Ok(())
    }

    fn validate_without_hash(&self) -> Result<(), ErasureContractError> {
        validate_root_resources(&self.root_resource_refs)?;
        let classes = validate_data_classes(&self.data_classes)?;
        if self.retention_exceptions.len() > MAX_ERASURE_RETENTION_EXCEPTIONS {
            return Err(ErasureContractError::TooManyRetentionExceptions);
        }
        let mut exception_classes = BTreeSet::new();
        for exception in &self.retention_exceptions {
            if !classes.contains(&exception.data_class) {
                return Err(ErasureContractError::ExceptionOutsidePlan);
            }
            if !exception_classes.insert(exception.data_class) {
                return Err(ErasureContractError::DuplicateRetentionException);
            }
            if exception.expires_at <= self.frozen_at {
                return Err(ErasureContractError::ExpiredRetentionException);
            }
        }
        Ok(())
    }

    fn compute_hash(&self) -> Sha256 {
        let domain = if self.schema_version.as_str().starts_with("musteragent.")
            && !self
                .schema_version
                .as_str()
                .starts_with("musteragent.legacy.")
        {
            b"musteragent.privacy.erasure.plan.v2".as_slice()
        } else {
            b"musteragent.legacy.privacy.erasure.plan.v1".as_slice()
        };
        let mut value = Fingerprint::new(domain);
        value.uuid(self.erasure_job_id.into_uuid());
        value.u64(self.plan_version.get());
        value.resources(&self.root_resource_refs);
        let mut classes = self.data_classes.clone();
        classes.sort_unstable();
        value.u64(classes.len() as u64);
        for data_class in classes {
            value.u64(data_class.code() as u64);
        }
        let mut exceptions = self.retention_exceptions.clone();
        exceptions.sort_by_key(|exception| exception.data_class);
        value.u64(exceptions.len() as u64);
        for exception in exceptions {
            value.u64(exception.data_class.code() as u64);
            value.retention(&exception.policy_ref);
            value.uuid(exception.owner_principal_id.into_uuid());
            value.timestamp(exception.expires_at);
        }
        value.sha256(self.lineage_fingerprint);
        value.timestamp(self.frozen_at);
        value.finish()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ErasureTransition {
    pub sequence: ErasureTransitionSequence,
    pub status: ErasureJobStatus,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasureReceipt {
    pub schema_version: DottedKey,
    pub erasure_receipt_id: ErasureReceiptId,
    pub erasure_job_id: ErasureJobId,
    pub tenant_id: TenantId,
    pub outcome: ErasureReceiptOutcome,
    pub plan_hash: Sha256,
    pub completed_data_classes: Vec<ErasureDataClass>,
    pub retention_exceptions: Vec<ErasureRetentionException>,
    pub proof_digest: Sha256,
    pub completed_at: UtcTimestamp,
}

impl ErasureReceipt {
    pub fn validate_shape(&self) -> Result<(), ErasureContractError> {
        let completed = validate_completed_data_classes(&self.completed_data_classes)?;
        if self.retention_exceptions.len() > MAX_ERASURE_RETENTION_EXCEPTIONS {
            return Err(ErasureContractError::TooManyRetentionExceptions);
        }
        let mut exception_classes = BTreeSet::new();
        for exception in &self.retention_exceptions {
            if !exception_classes.insert(exception.data_class) {
                return Err(ErasureContractError::DuplicateRetentionException);
            }
        }
        if !completed.is_disjoint(&exception_classes) {
            return Err(ErasureContractError::ReceiptCoverageMismatch);
        }
        match self.outcome {
            ErasureReceiptOutcome::FullyErased if !exception_classes.is_empty() => {
                Err(ErasureContractError::UnexpectedRetentionException)
            }
            ErasureReceiptOutcome::ErasedWithRetentionException if exception_classes.is_empty() => {
                Err(ErasureContractError::RetentionExceptionRequired)
            }
            _ => Ok(()),
        }
    }

    pub fn validate_against(
        &self,
        tenant_id: TenantId,
        plan: &ErasurePlanSnapshot,
    ) -> Result<(), ErasureContractError> {
        self.validate_shape()?;
        plan.validate()?;
        if self.tenant_id != tenant_id || self.erasure_job_id != plan.erasure_job_id {
            return Err(ErasureContractError::ReceiptIdentityMismatch);
        }
        if self.plan_hash != plan.plan_hash || self.completed_at < plan.frozen_at {
            return Err(ErasureContractError::ReceiptPlanMismatch);
        }
        let completed = validate_completed_data_classes(&self.completed_data_classes)?;
        validate_receipt_exceptions(&self.retention_exceptions, plan, self.completed_at)?;
        let exceptions = self
            .retention_exceptions
            .iter()
            .map(|exception| exception.data_class)
            .collect::<BTreeSet<_>>();
        if !completed.is_disjoint(&exceptions)
            || completed
                .union(&exceptions)
                .copied()
                .collect::<BTreeSet<_>>()
                != plan.data_classes.iter().copied().collect()
        {
            return Err(ErasureContractError::ReceiptCoverageMismatch);
        }
        match self.outcome {
            ErasureReceiptOutcome::FullyErased if !exceptions.is_empty() => {
                Err(ErasureContractError::UnexpectedRetentionException)
            }
            ErasureReceiptOutcome::ErasedWithRetentionException if exceptions.is_empty() => {
                Err(ErasureContractError::RetentionExceptionRequired)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasureJob {
    pub request: ErasureRequest,
    pub request_fingerprint: Sha256,
    pub plans: Vec<ErasurePlanSnapshot>,
    pub transitions: Vec<ErasureTransition>,
    pub receipt: Option<ErasureReceipt>,
}

impl ErasureJob {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        self.request.validate()?;
        if self.request_fingerprint != self.request.fingerprint()? {
            return Err(ErasureContractError::RequestFingerprintMismatch);
        }
        for (index, plan) in self.plans.iter().enumerate() {
            plan.validate()?;
            if plan.erasure_job_id != self.request.erasure_job_id
                || plan.root_resource_refs.iter().collect::<HashSet<_>>()
                    != self.request.root_resource_refs.iter().collect()
                || plan.plan_version.get() != (index + 1) as u64
            {
                return Err(ErasureContractError::PlanLineageMismatch);
            }
        }
        validate_completion(
            &self.request,
            self.plans.last(),
            &self.transitions,
            self.receipt.as_ref(),
        )
    }

    pub fn latest_plan(&self) -> Result<&ErasurePlanSnapshot, ErasureContractError> {
        self.plans.last().ok_or(ErasureContractError::PlanRequired)
    }

    #[must_use]
    pub fn status(&self) -> Option<ErasureJobStatus> {
        self.transitions.last().map(|transition| transition.status)
    }
}

/// Read-only projection carried by the existing public query. A projection has
/// the latest immutable plan, not the complete plan history; it must never be
/// promoted into a writable aggregate or fabricate missing earlier plans.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasureJobProjection {
    pub request: ErasureRequest,
    pub latest_plan: ErasurePlanSnapshot,
    pub transitions: Vec<ErasureTransition>,
    pub receipt: Option<ErasureReceipt>,
}

impl ErasureJobProjection {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        self.request.validate()?;
        self.latest_plan.validate()?;
        if self.latest_plan.erasure_job_id != self.request.erasure_job_id
            || self
                .latest_plan
                .root_resource_refs
                .iter()
                .collect::<HashSet<_>>()
                != self.request.root_resource_refs.iter().collect()
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        validate_completion(
            &self.request,
            Some(&self.latest_plan),
            &self.transitions,
            self.receipt.as_ref(),
        )
    }

    #[must_use]
    pub fn status(&self) -> Option<ErasureJobStatus> {
        self.transitions.last().map(|transition| transition.status)
    }
}

// Storage aggregates and read-only wire projections use exactly one original
// transition/terminal-receipt contract. Transport adapters cannot relax it.
fn validate_completion(
    request: &ErasureRequest,
    latest_plan: Option<&ErasurePlanSnapshot>,
    transitions: &[ErasureTransition],
    receipt: Option<&ErasureReceipt>,
) -> Result<(), ErasureContractError> {
    validate_transitions(transitions, request.requested_at)?;
    let status = transitions
        .last()
        .ok_or(ErasureContractError::MissingInitialTransition)?
        .status;
    match (receipt, status) {
        (Some(receipt), ErasureJobStatus::Completed) => {
            if receipt.outcome != ErasureReceiptOutcome::FullyErased {
                return Err(ErasureContractError::ReceiptOutcomeMismatch);
            }
            receipt.validate_against(
                request.tenant_id,
                latest_plan.ok_or(ErasureContractError::PlanRequired)?,
            )
        }
        (Some(receipt), ErasureJobStatus::CompletedWithRetentionException) => {
            if receipt.outcome != ErasureReceiptOutcome::ErasedWithRetentionException {
                return Err(ErasureContractError::ReceiptOutcomeMismatch);
            }
            receipt.validate_against(
                request.tenant_id,
                latest_plan.ok_or(ErasureContractError::PlanRequired)?,
            )
        }
        (None, value) if !value.is_terminal() => Ok(()),
        (Some(_), _) => Err(ErasureContractError::ReceiptBeforeTerminal),
        (None, _) => Err(ErasureContractError::TerminalReceiptRequired),
    }
}

fn validate_root_resources(resources: &[ResourceRef]) -> Result<(), ErasureContractError> {
    if resources.is_empty() {
        return Err(ErasureContractError::EmptyRootResources);
    }
    if resources.len() > MAX_ERASURE_ROOT_RESOURCES {
        return Err(ErasureContractError::TooManyRootResources);
    }
    if resources.iter().any(|resource| {
        matches!(
            resource,
            ResourceRef::ErasureJob(_) | ResourceRef::ErasureReceipt(_)
        )
    }) {
        return Err(ErasureContractError::ProtectedErasureFactRoot);
    }
    if resources.iter().collect::<HashSet<_>>().len() != resources.len() {
        return Err(ErasureContractError::DuplicateRootResource);
    }
    Ok(())
}

fn validate_data_classes(
    classes: &[ErasureDataClass],
) -> Result<BTreeSet<ErasureDataClass>, ErasureContractError> {
    if classes.is_empty() {
        return Err(ErasureContractError::EmptyDataClasses);
    }
    let values = classes.iter().copied().collect::<BTreeSet<_>>();
    if values.len() != classes.len() {
        return Err(ErasureContractError::DuplicateDataClass);
    }
    Ok(values)
}

fn validate_completed_data_classes(
    classes: &[ErasureDataClass],
) -> Result<BTreeSet<ErasureDataClass>, ErasureContractError> {
    let values = classes.iter().copied().collect::<BTreeSet<_>>();
    if values.len() != classes.len() {
        return Err(ErasureContractError::DuplicateDataClass);
    }
    Ok(values)
}

fn validate_receipt_exceptions(
    exceptions: &[ErasureRetentionException],
    plan: &ErasurePlanSnapshot,
    completed_at: UtcTimestamp,
) -> Result<(), ErasureContractError> {
    if exceptions != plan.retention_exceptions {
        return Err(ErasureContractError::ReceiptExceptionDrift);
    }
    if exceptions
        .iter()
        .any(|exception| exception.expires_at <= completed_at)
    {
        return Err(ErasureContractError::ExpiredRetentionException);
    }
    Ok(())
}

fn validate_transitions(
    transitions: &[ErasureTransition],
    requested_at: UtcTimestamp,
) -> Result<(), ErasureContractError> {
    let first = transitions
        .first()
        .ok_or(ErasureContractError::MissingInitialTransition)?;
    if first.sequence.get() != 1
        || first.status != ErasureJobStatus::PendingPlan
        || first.occurred_at < requested_at
    {
        return Err(ErasureContractError::InvalidInitialTransition);
    }
    for pair in transitions.windows(2) {
        let previous = pair[0];
        let current = pair[1];
        if current.sequence.get() != previous.sequence.get() + 1
            || current.occurred_at < previous.occurred_at
            || !previous.status.can_transition_to(current.status)
        {
            return Err(ErasureContractError::InvalidTransition);
        }
    }
    Ok(())
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ErasureContractError {
    #[error("erasure plan version must be positive")]
    InvalidPlanVersion,
    #[error("erasure transition sequence must be positive")]
    InvalidTransitionSequence,
    #[error("erasure request must contain at least one root resource")]
    EmptyRootResources,
    #[error("erasure request exceeds the root resource limit")]
    TooManyRootResources,
    #[error("erasure root resources must be unique")]
    DuplicateRootResource,
    #[error("erasure facts and receipts cannot be erased through the general port")]
    ProtectedErasureFactRoot,
    #[error("erasure actor tenant does not match the request tenant")]
    ActorTenantMismatch,
    #[error("erasure completion deadline must follow the request time")]
    InvalidCompletionDeadline,
    #[error("erasure plan must contain at least one data class")]
    EmptyDataClasses,
    #[error("erasure plan data classes must be unique")]
    DuplicateDataClass,
    #[error("erasure plan contains too many retention exceptions")]
    TooManyRetentionExceptions,
    #[error("erasure retention exception is outside the plan")]
    ExceptionOutsidePlan,
    #[error("erasure retention exceptions must be unique by data class")]
    DuplicateRetentionException,
    #[error("erasure retention exception must expire after the relevant fact")]
    ExpiredRetentionException,
    #[error("erasure plan hash does not match its canonical fields")]
    PlanHashMismatch,
    #[error("erasure request fingerprint does not match its canonical fields")]
    RequestFingerprintMismatch,
    #[error("erasure plan identity, roots or version lineage does not match the job")]
    PlanLineageMismatch,
    #[error("erasure job is missing its initial PendingPlan transition")]
    MissingInitialTransition,
    #[error("erasure initial transition is invalid")]
    InvalidInitialTransition,
    #[error("erasure state transition is invalid")]
    InvalidTransition,
    #[error("erasure plan is required before this operation")]
    PlanRequired,
    #[error("erasure receipt identity does not match its job")]
    ReceiptIdentityMismatch,
    #[error("erasure receipt does not match the immutable plan")]
    ReceiptPlanMismatch,
    #[error("erasure receipt data classes do not exactly cover the plan")]
    ReceiptCoverageMismatch,
    #[error("fully erased receipt cannot contain retention exceptions")]
    UnexpectedRetentionException,
    #[error("retention-exception receipt must contain at least one exception")]
    RetentionExceptionRequired,
    #[error("erasure receipt retention decisions drift from the immutable plan")]
    ReceiptExceptionDrift,
    #[error("erasure receipt outcome does not match the terminal state")]
    ReceiptOutcomeMismatch,
    #[error("erasure receipt cannot exist before a terminal transition")]
    ReceiptBeforeTerminal,
    #[error("terminal erasure state requires one receipt")]
    TerminalReceiptRequired,
    #[error("erasure data class code is unknown")]
    UnknownDataClass,
    #[error("erasure job status code is unknown")]
    UnknownJobStatus,
    #[error("erasure receipt outcome code is unknown")]
    UnknownReceiptOutcome,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        Actor, ContentId, DottedKey, ErasureJobId, ErasureReceiptId, IdempotencyKey, PrincipalId,
        PrincipalKind, TenantId, UtcTimestamp,
    };
    use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};

    use super::{
        ErasureContractError, ErasureDataClass, ErasureJob, ErasureJobStatus, ErasurePlanSnapshot,
        ErasurePlanVersion, ErasureReceipt, ErasureReceiptOutcome, ErasureRequest,
        ErasureRetentionException, ErasureTransition, ErasureTransitionSequence,
    };

    fn request() -> ErasureRequest {
        let tenant_id = TenantId::generate();
        ErasureRequest {
            schema_version: DottedKey::new("musteragent.legacy.privacy.erasure.v1")
                .expect("schema"),
            erasure_job_id: ErasureJobId::generate(),
            tenant_id,
            requested_by: Actor {
                tenant_id,
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            root_resource_refs: vec![ResourceRef::Content(ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain").expect("media type"),
                byte_length: 6,
                sha256: Sha256::digest(b"secret"),
            })],
            retention_policy_ref: DataRetentionPolicyRef::new("privacy.standard@1")
                .expect("retention"),
            idempotency_key: IdempotencyKey::new("erase:test").expect("idempotency"),
            requested_at: UtcTimestamp::from_unix_millis(10),
            complete_by: UtcTimestamp::from_unix_millis(100),
        }
    }

    fn plan(
        request: &ErasureRequest,
        exceptions: Vec<ErasureRetentionException>,
    ) -> ErasurePlanSnapshot {
        ErasurePlanSnapshot::new(
            request.schema_version.clone(),
            request.erasure_job_id,
            ErasurePlanVersion::new(1).expect("version"),
            request.root_resource_refs.clone(),
            vec![
                ErasureDataClass::ContentCiphertext,
                ErasureDataClass::Backup,
            ],
            exceptions,
            request.fingerprint().expect("lineage"),
            UtcTimestamp::from_unix_millis(20),
        )
        .expect("plan")
    }

    fn transitions(terminal: ErasureJobStatus) -> Vec<ErasureTransition> {
        [
            (1, ErasureJobStatus::PendingPlan),
            (2, ErasureJobStatus::Planned),
            (3, ErasureJobStatus::Fenced),
            (4, ErasureJobStatus::Executing),
            (5, terminal),
        ]
        .into_iter()
        .map(|(sequence, status)| ErasureTransition {
            sequence: ErasureTransitionSequence::new(sequence).expect("sequence"),
            status,
            occurred_at: UtcTimestamp::from_unix_millis(10 + sequence as i64),
        })
        .collect()
    }

    #[test]
    fn request_and_plan_are_bounded_deterministic_and_protect_erasure_facts() {
        let request = request();
        assert_eq!(
            request.fingerprint().expect("fingerprint"),
            request.fingerprint().expect("replay")
        );

        let mut protected = request.clone();
        protected.root_resource_refs = vec![ResourceRef::ErasureJob(ErasureJobId::generate())];
        assert_eq!(
            protected.validate(),
            Err(ErasureContractError::ProtectedErasureFactRoot)
        );

        let mut reversed = request.clone();
        reversed.complete_by = reversed.requested_at;
        assert_eq!(
            reversed.validate(),
            Err(ErasureContractError::InvalidCompletionDeadline)
        );

        let plan = plan(&request, Vec::new());
        plan.validate().expect("valid plan");
        let mut drifted = plan.clone();
        drifted.data_classes.pop();
        assert_eq!(
            drifted.validate(),
            Err(ErasureContractError::PlanHashMismatch)
        );
    }

    #[test]
    fn receipt_requires_exact_plan_coverage_and_unexpired_exceptions() {
        let request = request();
        let exception = ErasureRetentionException {
            data_class: ErasureDataClass::Backup,
            policy_ref: DataRetentionPolicyRef::new("legal.hold@1").expect("policy"),
            owner_principal_id: PrincipalId::generate(),
            expires_at: UtcTimestamp::from_unix_millis(200),
        };
        let plan = plan(&request, vec![exception.clone()]);
        let receipt = ErasureReceipt {
            schema_version: request.schema_version.clone(),
            erasure_receipt_id: ErasureReceiptId::generate(),
            erasure_job_id: request.erasure_job_id,
            tenant_id: request.tenant_id,
            outcome: ErasureReceiptOutcome::ErasedWithRetentionException,
            plan_hash: plan.plan_hash,
            completed_data_classes: vec![ErasureDataClass::ContentCiphertext],
            retention_exceptions: vec![exception],
            proof_digest: request.fingerprint().expect("proof"),
            completed_at: UtcTimestamp::from_unix_millis(50),
        };
        receipt
            .validate_against(request.tenant_id, &plan)
            .expect("receipt");

        let mut missing = receipt.clone();
        missing.completed_data_classes.clear();
        assert_eq!(
            missing.validate_against(request.tenant_id, &plan),
            Err(ErasureContractError::ReceiptCoverageMismatch)
        );
    }

    #[test]
    fn aggregate_requires_append_only_state_machine_and_terminal_receipt() {
        let request = request();
        let plan = plan(&request, Vec::new());
        let receipt = ErasureReceipt {
            schema_version: request.schema_version.clone(),
            erasure_receipt_id: ErasureReceiptId::generate(),
            erasure_job_id: request.erasure_job_id,
            tenant_id: request.tenant_id,
            outcome: ErasureReceiptOutcome::FullyErased,
            plan_hash: plan.plan_hash,
            completed_data_classes: plan.data_classes.clone(),
            retention_exceptions: Vec::new(),
            proof_digest: request.fingerprint().expect("proof"),
            completed_at: UtcTimestamp::from_unix_millis(50),
        };
        let job = ErasureJob {
            request_fingerprint: request.fingerprint().expect("fingerprint"),
            plans: vec![plan],
            transitions: transitions(ErasureJobStatus::Completed),
            receipt: Some(receipt),
            request,
        };
        job.validate().expect("valid aggregate");

        let mut invalid = job.clone();
        invalid.transitions[2].status = ErasureJobStatus::Executing;
        assert_eq!(
            invalid.validate(),
            Err(ErasureContractError::InvalidTransition)
        );
    }

    #[test]
    fn closed_codes_reject_unknown_values() {
        for data_class in ErasureDataClass::ALL {
            assert_eq!(
                ErasureDataClass::from_code(data_class.code()).expect("class"),
                data_class
            );
        }
        for status in ErasureJobStatus::ALL {
            assert_eq!(
                ErasureJobStatus::from_code(status.code()).expect("status"),
                status
            );
        }
        assert!(ErasureDataClass::from_code(0).is_err());
        assert!(ErasureJobStatus::from_code(9).is_err());
        assert!(ErasureReceiptOutcome::from_code(3).is_err());
    }
}
