//! Independent, append-only Security Audit contracts.
//!
//! Audit facts are deliberately not Conversation Ledger records. Producers
//! reference an existing Ledger fact when one exists; independent denials and
//! credential accesses remain first-class audit facts without inventing a
//! business event.

#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashSet};

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    AuditRecordId, CorrelationId, DottedKey, ErasureJobId, InvocationId, LedgerRecordId,
    PrincipalId, PrincipalKind, TenantId, UtcTimestamp,
};
pub use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_contract::ResourceRef;
use thiserror::Error;

mod producer;

pub use producer::{
    ApprovalAuditDecision, SecurityAuditFact, SecurityAuditProducer, SecurityAuditProducerContext,
};

pub const MAX_AUDIT_RESOURCE_REFS: usize = 32;
pub const MAX_AUDIT_QUERY_PAGE_SIZE: u16 = 200;
pub use musteragent_runtime_contract::{DataRetentionPolicyRef, DataRetentionPolicyRefError};

/// Security-relevant operation. This vocabulary is closed at every adapter.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SecurityAuditAction {
    ApprovalDecision,
    CredentialAccess,
    AuthorizationDenied,
    SkillLifecycleChange,
    AgentVersionPublished,
    DataErasure,
    OwnershipCutover,
    AdmissionDecision,
    ModelInvocation,
    ToolInvocation,
    InteractionResponse,
    ToolInvocationIndeterminate,
}

impl SecurityAuditAction {
    #[must_use]
    pub const fn code(self) -> i16 {
        match self {
            Self::ApprovalDecision => 1,
            Self::CredentialAccess => 2,
            Self::AuthorizationDenied => 3,
            Self::SkillLifecycleChange => 4,
            Self::AgentVersionPublished => 5,
            Self::DataErasure => 6,
            Self::OwnershipCutover => 7,
            Self::AdmissionDecision => 8,
            Self::ModelInvocation => 9,
            Self::ToolInvocation => 10,
            Self::InteractionResponse => 11,
            Self::ToolInvocationIndeterminate => 12,
        }
    }

    pub fn from_code(value: i16) -> Result<Self, SecurityAuditContractError> {
        match value {
            1 => Ok(Self::ApprovalDecision),
            2 => Ok(Self::CredentialAccess),
            3 => Ok(Self::AuthorizationDenied),
            4 => Ok(Self::SkillLifecycleChange),
            5 => Ok(Self::AgentVersionPublished),
            6 => Ok(Self::DataErasure),
            7 => Ok(Self::OwnershipCutover),
            8 => Ok(Self::AdmissionDecision),
            9 => Ok(Self::ModelInvocation),
            10 => Ok(Self::ToolInvocation),
            11 => Ok(Self::InteractionResponse),
            12 => Ok(Self::ToolInvocationIndeterminate),
            _ => Err(SecurityAuditContractError::UnknownAction),
        }
    }
}

/// Closed reference to the canonical fact that caused an audit record.
///
/// New source owners extend this enum additively. An action must never borrow
/// an unrelated Ledger record merely to satisfy source uniqueness.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SecurityAuditSourceRef {
    LedgerRecord(LedgerRecordId),
    ErasureJob(ErasureJobId),
    ModelInvocation(InvocationId),
    ToolInvocation(InvocationId),
}

/// Fixed-width W3C trace identity. The full traceparent/tracestate headers are
/// transport material and are deliberately not copied into the audit fact.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SecurityAuditTraceId([u8; 16]);

impl SecurityAuditTraceId {
    pub fn from_trace_context(
        trace: &musteragent_runtime_contract::TraceContext,
    ) -> Result<Self, SecurityAuditContractError> {
        let traceparent = trace.traceparent.as_str().as_bytes();
        let encoded = traceparent
            .get(3..35)
            .ok_or(SecurityAuditContractError::InvalidTraceIdentity)?;
        let mut bytes = [0_u8; 16];
        for (index, pair) in encoded.chunks_exact(2).enumerate() {
            bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
        }
        if bytes == [0; 16] {
            return Err(SecurityAuditContractError::InvalidTraceIdentity);
        }
        Ok(Self(bytes))
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, SecurityAuditContractError> {
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| SecurityAuditContractError::InvalidTraceIdentity)?;
        if bytes == [0; 16] {
            return Err(SecurityAuditContractError::InvalidTraceIdentity);
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SecurityAuditOutcome {
    Succeeded,
    Denied,
    Failed,
}

impl SecurityAuditOutcome {
    #[must_use]
    pub const fn code(self) -> i16 {
        match self {
            Self::Succeeded => 1,
            Self::Denied => 2,
            Self::Failed => 3,
        }
    }

    pub fn from_code(value: i16) -> Result<Self, SecurityAuditContractError> {
        match value {
            1 => Ok(Self::Succeeded),
            2 => Ok(Self::Denied),
            3 => Ok(Self::Failed),
            _ => Err(SecurityAuditContractError::UnknownOutcome),
        }
    }
}

/// Producer-owned proposal. PostgreSQL or SQLite supplies committed time.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProposedSecurityAuditRecord {
    pub schema_version: DottedKey,
    pub audit_record_id: AuditRecordId,
    pub tenant_id: TenantId,
    pub actor_principal_id: PrincipalId,
    pub actor_principal_kind: PrincipalKind,
    pub action: SecurityAuditAction,
    pub outcome: SecurityAuditOutcome,
    pub resource_refs: Vec<ResourceRef>,
    pub source_fact_ref: Option<SecurityAuditSourceRef>,
    pub correlation_id: CorrelationId,
    pub trace_id: Option<SecurityAuditTraceId>,
    pub detail_ref: Option<ContentRef>,
    pub retention_policy_ref: DataRetentionPolicyRef,
    pub occurred_at: UtcTimestamp,
}

impl ProposedSecurityAuditRecord {
    pub fn validate(&self) -> Result<(), SecurityAuditContractError> {
        validate_resources(&self.resource_refs)?;
        validate_source(self.action, self.source_fact_ref)?;
        validate_trace(self.action, self.trace_id)?;
        Ok(())
    }

    /// Stable canonical fingerprint used to reject same-ID payload drift.
    pub fn fingerprint(&self) -> Result<Sha256, SecurityAuditContractError> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(512);
        push_text(&mut bytes, self.schema_version.as_str());
        push_uuid(&mut bytes, self.audit_record_id.into_uuid());
        push_uuid(&mut bytes, self.tenant_id.into_uuid());
        push_uuid(&mut bytes, self.actor_principal_id.into_uuid());
        push_i16(&mut bytes, principal_kind_code(self.actor_principal_kind));
        push_i16(&mut bytes, self.action.code());
        push_i16(&mut bytes, self.outcome.code());
        push_u64(&mut bytes, self.resource_refs.len() as u64);
        for resource in &self.resource_refs {
            encode_resource(&mut bytes, resource);
        }
        match self.source_fact_ref {
            Some(SecurityAuditSourceRef::LedgerRecord(source)) => {
                bytes.push(1);
                push_uuid(&mut bytes, source.into_uuid());
            }
            Some(SecurityAuditSourceRef::ErasureJob(source)) => {
                bytes.push(2);
                push_uuid(&mut bytes, source.into_uuid());
            }
            Some(SecurityAuditSourceRef::ModelInvocation(source)) => {
                bytes.push(3);
                push_uuid(&mut bytes, source.into_uuid());
            }
            Some(SecurityAuditSourceRef::ToolInvocation(source)) => {
                bytes.push(4);
                push_uuid(&mut bytes, source.into_uuid());
            }
            None => bytes.push(0),
        }
        push_uuid(&mut bytes, self.correlation_id.into_uuid());
        match self.trace_id {
            Some(trace_id) => {
                bytes.push(1);
                bytes.extend_from_slice(trace_id.as_bytes());
            }
            None => bytes.push(0),
        }
        match &self.detail_ref {
            Some(detail) => {
                bytes.push(1);
                encode_content(&mut bytes, detail);
            }
            None => bytes.push(0),
        }
        push_text(&mut bytes, self.retention_policy_ref.as_str());
        bytes.extend_from_slice(&self.occurred_at.unix_millis().to_be_bytes());
        Ok(Sha256::digest(bytes))
    }
}

/// Stable fingerprint for enforcing uniqueness of normalized resource rows.
#[must_use]
pub fn resource_fingerprint(resource: &ResourceRef) -> Sha256 {
    resource.fingerprint()
}

/// Persisted fact with the producer's occurrence time and the database's commit
/// time. These are independent UTC clocks, not a cross-host ordering proof.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SecurityAuditRecord {
    pub proposal: ProposedSecurityAuditRecord,
    pub committed_at: UtcTimestamp,
}

impl SecurityAuditRecord {
    pub fn validate(&self) -> Result<(), SecurityAuditContractError> {
        // Source identity, authority and transactional publication establish
        // causality. Rejecting clock skew would roll back valid source facts.
        self.proposal.validate()
    }
}

/// Admission-time set of retention policies approved by the control plane.
#[derive(Debug, Clone)]
pub struct ApprovedRetentionPolicies {
    values: BTreeSet<DataRetentionPolicyRef>,
}

impl ApprovedRetentionPolicies {
    pub fn new(
        values: impl IntoIterator<Item = DataRetentionPolicyRef>,
    ) -> Result<Self, SecurityAuditContractError> {
        let values = values.into_iter().collect::<BTreeSet<_>>();
        if values.is_empty() {
            return Err(SecurityAuditContractError::NoApprovedRetentionPolicy);
        }
        Ok(Self { values })
    }

    pub fn authorize(
        &self,
        proposal: &ProposedSecurityAuditRecord,
    ) -> Result<(), SecurityAuditContractError> {
        proposal.validate()?;
        if self.values.contains(&proposal.retention_policy_ref) {
            Ok(())
        } else {
            Err(SecurityAuditContractError::RetentionPolicyNotApproved)
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SecurityAuditAppendDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SecurityAuditAppendReceipt {
    pub disposition: SecurityAuditAppendDisposition,
    pub record: SecurityAuditRecord,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct SecurityAuditCursor {
    pub committed_at: UtcTimestamp,
    pub audit_record_id: AuditRecordId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SecurityAuditQuery {
    pub tenant_id: TenantId,
    pub after: Option<SecurityAuditCursor>,
    pub limit: u16,
}

impl SecurityAuditQuery {
    pub fn validate(&self) -> Result<(), SecurityAuditContractError> {
        if self.limit == 0 || self.limit > MAX_AUDIT_QUERY_PAGE_SIZE {
            return Err(SecurityAuditContractError::InvalidQueryLimit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SecurityAuditPage {
    pub records: Vec<SecurityAuditRecord>,
    pub next_cursor: Option<SecurityAuditCursor>,
}

/// Read-only query port. Production compositions expose this separately from
/// the typed writer so a query consumer cannot append raw proposals.
pub trait SecurityAuditReader: Send + Sync {
    type Error: Send;

    fn list(
        &self,
        query: SecurityAuditQuery,
    ) -> ApplicationFuture<'_, Result<SecurityAuditPage, Self::Error>>;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SecurityAuditContractError {
    #[error("security audit record must reference at least one resource")]
    EmptyResources,
    #[error("security audit record exceeds the resource reference limit")]
    TooManyResources,
    #[error("security audit resource references must be unique")]
    DuplicateResource,
    #[error("security audit action requires its canonical typed source fact")]
    SourceFactRequired,
    #[error("security audit source fact type does not match the action")]
    SourceFactTypeMismatch,
    #[error("security audit action must not carry a source fact")]
    SourceFactNotAllowed,
    #[error("security audit action has no admitted typed source owner yet")]
    SourceOwnerNotImplemented,
    #[error("at least one retention policy must be approved")]
    NoApprovedRetentionPolicy,
    #[error("security audit retention policy is not approved")]
    RetentionPolicyNotApproved,
    #[error("security audit query limit must be between 1 and 200")]
    InvalidQueryLimit,
    #[error("security audit action code is unknown")]
    UnknownAction,
    #[error("security audit outcome code is unknown")]
    UnknownOutcome,
    #[error("security audit principal kind code is unknown")]
    UnknownPrincipalKind,
    #[error("security audit producer identity cannot be derived")]
    InvalidProducerIdentity,
    #[error("security audit trace identity is invalid")]
    InvalidTraceIdentity,
    #[error("security audit action requires a durable ingress trace identity")]
    TraceIdentityRequired,
}

fn validate_resources(resources: &[ResourceRef]) -> Result<(), SecurityAuditContractError> {
    if resources.is_empty() {
        return Err(SecurityAuditContractError::EmptyResources);
    }
    if resources.len() > MAX_AUDIT_RESOURCE_REFS {
        return Err(SecurityAuditContractError::TooManyResources);
    }
    let mut unique = HashSet::with_capacity(resources.len());
    if resources.iter().any(|resource| !unique.insert(resource)) {
        return Err(SecurityAuditContractError::DuplicateResource);
    }
    Ok(())
}

fn validate_source(
    action: SecurityAuditAction,
    source: Option<SecurityAuditSourceRef>,
) -> Result<(), SecurityAuditContractError> {
    match (action, source) {
        (SecurityAuditAction::ApprovalDecision, Some(SecurityAuditSourceRef::LedgerRecord(_)))
        | (SecurityAuditAction::AdmissionDecision, Some(SecurityAuditSourceRef::LedgerRecord(_)))
        | (
            SecurityAuditAction::InteractionResponse,
            Some(SecurityAuditSourceRef::LedgerRecord(_)),
        )
        | (SecurityAuditAction::DataErasure, Some(SecurityAuditSourceRef::ErasureJob(_)))
        | (
            SecurityAuditAction::CredentialAccess | SecurityAuditAction::ModelInvocation,
            Some(SecurityAuditSourceRef::ModelInvocation(_)),
        )
        | (
            SecurityAuditAction::ToolInvocation | SecurityAuditAction::ToolInvocationIndeterminate,
            Some(SecurityAuditSourceRef::ToolInvocation(_)),
        )
        | (SecurityAuditAction::AuthorizationDenied, None) => Ok(()),
        (
            SecurityAuditAction::ApprovalDecision
            | SecurityAuditAction::AdmissionDecision
            | SecurityAuditAction::ModelInvocation
            | SecurityAuditAction::ToolInvocation
            | SecurityAuditAction::ToolInvocationIndeterminate
            | SecurityAuditAction::InteractionResponse
            | SecurityAuditAction::DataErasure,
            None,
        ) => Err(SecurityAuditContractError::SourceFactRequired),
        (
            SecurityAuditAction::ApprovalDecision
            | SecurityAuditAction::AdmissionDecision
            | SecurityAuditAction::ModelInvocation
            | SecurityAuditAction::ToolInvocation
            | SecurityAuditAction::ToolInvocationIndeterminate
            | SecurityAuditAction::InteractionResponse
            | SecurityAuditAction::DataErasure,
            Some(_),
        ) => Err(SecurityAuditContractError::SourceFactTypeMismatch),
        (SecurityAuditAction::AuthorizationDenied, Some(_)) => {
            Err(SecurityAuditContractError::SourceFactNotAllowed)
        }
        (SecurityAuditAction::CredentialAccess, None | Some(_)) => {
            Err(SecurityAuditContractError::SourceFactTypeMismatch)
        }
        (
            SecurityAuditAction::SkillLifecycleChange
            | SecurityAuditAction::AgentVersionPublished
            | SecurityAuditAction::OwnershipCutover,
            _,
        ) => Err(SecurityAuditContractError::SourceOwnerNotImplemented),
    }
}

fn validate_trace(
    action: SecurityAuditAction,
    trace_id: Option<SecurityAuditTraceId>,
) -> Result<(), SecurityAuditContractError> {
    match (action, trace_id) {
        (
            SecurityAuditAction::ApprovalDecision
            | SecurityAuditAction::CredentialAccess
            | SecurityAuditAction::AuthorizationDenied
            | SecurityAuditAction::DataErasure,
            _,
        ) => Ok(()),
        (
            SecurityAuditAction::SkillLifecycleChange
            | SecurityAuditAction::AgentVersionPublished
            | SecurityAuditAction::OwnershipCutover,
            _,
        ) => Ok(()),
        (_, Some(_)) => Ok(()),
        (_, None) => Err(SecurityAuditContractError::TraceIdentityRequired),
    }
}

fn hex_nibble(value: u8) -> Result<u8, SecurityAuditContractError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(SecurityAuditContractError::InvalidTraceIdentity),
    }
}

#[must_use]
pub const fn principal_kind_code(value: PrincipalKind) -> i16 {
    match value {
        PrincipalKind::User => 1,
        PrincipalKind::Service => 2,
        PrincipalKind::System => 3,
    }
}

pub fn principal_kind_from_code(value: i16) -> Result<PrincipalKind, SecurityAuditContractError> {
    match value {
        1 => Ok(PrincipalKind::User),
        2 => Ok(PrincipalKind::Service),
        3 => Ok(PrincipalKind::System),
        _ => Err(SecurityAuditContractError::UnknownPrincipalKind),
    }
}

fn push_uuid(bytes: &mut Vec<u8>, value: uuid::Uuid) {
    bytes.extend_from_slice(value.as_bytes());
}

fn push_i16(bytes: &mut Vec<u8>, value: i16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_text(bytes: &mut Vec<u8>, value: &str) {
    push_u64(bytes, value.len() as u64);
    bytes.extend_from_slice(value.as_bytes());
}

fn encode_content(bytes: &mut Vec<u8>, content: &ContentRef) {
    push_uuid(bytes, content.content_id.into_uuid());
    push_text(bytes, content.media_type.as_str());
    push_u64(bytes, content.byte_length);
    bytes.extend_from_slice(&content.sha256.into_bytes());
}

fn encode_resource(bytes: &mut Vec<u8>, resource: &ResourceRef) {
    bytes.extend_from_slice(&resource.canonical_bytes());
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{
        AuditRecordId, CorrelationId, DottedKey, ErasureJobId, InteractionId, InvocationId,
        LedgerRecordId, PrincipalId, PrincipalKind, TenantId, UtcTimestamp,
    };
    use musteragent_runtime_contract::ResourceRef;

    use super::{
        ApprovedRetentionPolicies, DataRetentionPolicyRef, MAX_AUDIT_RESOURCE_REFS,
        ProposedSecurityAuditRecord, SecurityAuditAction, SecurityAuditContractError,
        SecurityAuditOutcome, SecurityAuditQuery, SecurityAuditSourceRef, SecurityAuditTraceId,
    };

    fn retention() -> DataRetentionPolicyRef {
        DataRetentionPolicyRef::new("security.audit.standard@1").expect("retention")
    }

    fn proposal(action: SecurityAuditAction) -> ProposedSecurityAuditRecord {
        ProposedSecurityAuditRecord {
            schema_version: DottedKey::new("musteragent.legacy.security.audit.v1").expect("schema"),
            audit_record_id: AuditRecordId::generate(),
            tenant_id: TenantId::generate(),
            actor_principal_id: PrincipalId::generate(),
            actor_principal_kind: PrincipalKind::User,
            action,
            outcome: SecurityAuditOutcome::Succeeded,
            resource_refs: vec![ResourceRef::Interaction(InteractionId::generate())],
            source_fact_ref: match action {
                SecurityAuditAction::ApprovalDecision
                | SecurityAuditAction::AdmissionDecision
                | SecurityAuditAction::InteractionResponse => Some(
                    SecurityAuditSourceRef::LedgerRecord(LedgerRecordId::generate()),
                ),
                SecurityAuditAction::CredentialAccess | SecurityAuditAction::ModelInvocation => {
                    Some(SecurityAuditSourceRef::ModelInvocation(
                        InvocationId::generate(),
                    ))
                }
                SecurityAuditAction::ToolInvocation
                | SecurityAuditAction::ToolInvocationIndeterminate => Some(
                    SecurityAuditSourceRef::ToolInvocation(InvocationId::generate()),
                ),
                SecurityAuditAction::DataErasure => {
                    Some(SecurityAuditSourceRef::ErasureJob(ErasureJobId::generate()))
                }
                _ => None,
            },
            correlation_id: CorrelationId::generate(),
            trace_id: Some(SecurityAuditTraceId::from_bytes(vec![1; 16]).expect("trace identity")),
            detail_ref: None,
            retention_policy_ref: retention(),
            occurred_at: UtcTimestamp::from_unix_millis(10),
        }
    }

    #[test]
    fn closed_contract_rejects_missing_sources_duplicate_resources_and_unbounded_queries() {
        let mut empty = proposal(SecurityAuditAction::CredentialAccess);
        empty.resource_refs.clear();
        assert_eq!(
            empty.validate(),
            Err(SecurityAuditContractError::EmptyResources)
        );

        let mut approval = proposal(SecurityAuditAction::ApprovalDecision);
        approval.source_fact_ref = None;
        assert_eq!(
            approval.validate(),
            Err(SecurityAuditContractError::SourceFactRequired)
        );

        let mut erasure = proposal(SecurityAuditAction::DataErasure);
        erasure.source_fact_ref = Some(SecurityAuditSourceRef::LedgerRecord(
            LedgerRecordId::generate(),
        ));
        assert_eq!(
            erasure.validate(),
            Err(SecurityAuditContractError::SourceFactTypeMismatch)
        );

        let skill = proposal(SecurityAuditAction::SkillLifecycleChange);
        assert_eq!(
            skill.validate(),
            Err(SecurityAuditContractError::SourceOwnerNotImplemented)
        );

        let mut duplicate = proposal(SecurityAuditAction::CredentialAccess);
        duplicate
            .resource_refs
            .push(duplicate.resource_refs[0].clone());
        assert_eq!(
            duplicate.validate(),
            Err(SecurityAuditContractError::DuplicateResource)
        );

        let mut oversized = proposal(SecurityAuditAction::CredentialAccess);
        oversized.resource_refs = (0..=MAX_AUDIT_RESOURCE_REFS)
            .map(|_| ResourceRef::Interaction(InteractionId::generate()))
            .collect();
        assert_eq!(
            oversized.validate(),
            Err(SecurityAuditContractError::TooManyResources)
        );

        assert_eq!(
            SecurityAuditQuery {
                tenant_id: TenantId::generate(),
                after: None,
                limit: 201,
            }
            .validate(),
            Err(SecurityAuditContractError::InvalidQueryLimit)
        );
    }

    #[test]
    fn retention_admission_and_fingerprints_fail_closed() {
        let approved = ApprovedRetentionPolicies::new([retention()]).expect("approved");
        let proposal = proposal(SecurityAuditAction::AuthorizationDenied);
        approved.authorize(&proposal).expect("authorized");
        assert_eq!(
            proposal.fingerprint().expect("fingerprint"),
            proposal.fingerprint().expect("fingerprint replay")
        );

        let mut drifted = proposal.clone();
        drifted.outcome = SecurityAuditOutcome::Denied;
        assert_ne!(
            proposal.fingerprint().expect("fingerprint"),
            drifted.fingerprint().expect("drifted fingerprint")
        );

        let unapproved =
            ApprovedRetentionPolicies::new([
                DataRetentionPolicyRef::new("security.audit.short@1").expect("retention")
            ])
            .expect("approved set");
        assert_eq!(
            unapproved.authorize(&proposal),
            Err(SecurityAuditContractError::RetentionPolicyNotApproved)
        );
    }

    #[test]
    fn action_and_outcome_codes_are_closed_and_exhaustive() {
        for action in [
            SecurityAuditAction::ApprovalDecision,
            SecurityAuditAction::CredentialAccess,
            SecurityAuditAction::AuthorizationDenied,
            SecurityAuditAction::SkillLifecycleChange,
            SecurityAuditAction::AgentVersionPublished,
            SecurityAuditAction::DataErasure,
            SecurityAuditAction::OwnershipCutover,
            SecurityAuditAction::AdmissionDecision,
            SecurityAuditAction::ModelInvocation,
            SecurityAuditAction::ToolInvocation,
            SecurityAuditAction::InteractionResponse,
            SecurityAuditAction::ToolInvocationIndeterminate,
        ] {
            assert_eq!(
                SecurityAuditAction::from_code(action.code()).expect("action"),
                action
            );
        }
        for outcome in [
            SecurityAuditOutcome::Succeeded,
            SecurityAuditOutcome::Denied,
            SecurityAuditOutcome::Failed,
        ] {
            assert_eq!(
                SecurityAuditOutcome::from_code(outcome.code()).expect("outcome"),
                outcome
            );
        }
        assert!(SecurityAuditAction::from_code(0).is_err());
        assert!(SecurityAuditOutcome::from_code(4).is_err());
        assert!(super::principal_kind_from_code(0).is_err());

        let proposal = proposal(SecurityAuditAction::CredentialAccess);
        let reversed = super::SecurityAuditRecord {
            committed_at: UtcTimestamp::from_unix_millis(9),
            proposal,
        };
        assert!(reversed.validate().is_ok());
    }
}
