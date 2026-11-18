use musteragent_identity::DottedKey;
use musteragent_runtime_audit::{
    DataRetentionPolicyRef, ProposedSecurityAuditRecord, SecurityAuditAction,
    SecurityAuditContractError, SecurityAuditOutcome, SecurityAuditRecord, SecurityAuditSourceRef,
    SecurityAuditTraceId,
};
use prost::Message as _;
use thiserror::Error;

use crate::wire;
use crate::wire_common::{
    WireContractError, content_ref, decode_content_ref, decode_principal_kind, decode_resource_ref,
    decode_timestamp, parse_id, principal_kind, resource_ref, timestamp,
};

/// Validate and encode a structured security audit record without ledger payload bytes.
pub fn encode_security_audit_record(
    record: &SecurityAuditRecord,
) -> Result<Vec<u8>, SecurityAuditCodecError> {
    Ok(security_audit_record_message(record)?.encode_to_vec())
}

/// Decode a security audit record, rejecting malformed or invalid contracts.
pub fn decode_security_audit_record(
    bytes: &[u8],
) -> Result<SecurityAuditRecord, SecurityAuditCodecError> {
    let message =
        wire::SecurityAuditRecord::decode(bytes).map_err(|_| SecurityAuditCodecError::Malformed)?;
    security_audit_record(message)
}

/// Project only the validated, typed audit facts into the wire contract.
pub fn security_audit_record_message(
    record: &SecurityAuditRecord,
) -> Result<wire::SecurityAuditRecord, SecurityAuditCodecError> {
    record.validate()?;
    let proposal = &record.proposal;
    Ok(wire::SecurityAuditRecord {
        schema_version: proposal.schema_version.as_str().to_owned(),
        audit_record_id: Some(wire::AuditRecordId {
            value: proposal.audit_record_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: proposal.tenant_id.to_string(),
        }),
        actor_principal_id: Some(wire::PrincipalId {
            value: proposal.actor_principal_id.to_string(),
        }),
        actor_principal_kind: principal_kind(proposal.actor_principal_kind) as i32,
        action: action(proposal.action) as i32,
        outcome: outcome(proposal.outcome) as i32,
        resource_refs: proposal.resource_refs.iter().map(resource_ref).collect(),
        source_fact: proposal.source_fact_ref.map(|source| match source {
            SecurityAuditSourceRef::LedgerRecord(value) => {
                wire::security_audit_record::SourceFact::SourceLedgerRecordId(
                    wire::LedgerRecordId {
                        value: value.to_string(),
                    },
                )
            }
            SecurityAuditSourceRef::ErasureJob(value) => {
                wire::security_audit_record::SourceFact::SourceErasureJobId(wire::ErasureJobId {
                    value: value.to_string(),
                })
            }
            SecurityAuditSourceRef::ModelInvocation(value) => {
                wire::security_audit_record::SourceFact::SourceModelInvocationId(
                    wire::InvocationId {
                        value: value.to_string(),
                    },
                )
            }
            SecurityAuditSourceRef::ToolInvocation(value) => {
                wire::security_audit_record::SourceFact::SourceToolInvocationId(
                    wire::InvocationId {
                        value: value.to_string(),
                    },
                )
            }
        }),
        correlation_id: Some(wire::CorrelationId {
            value: proposal.correlation_id.to_string(),
        }),
        detail_ref: proposal.detail_ref.as_ref().map(content_ref),
        retention_policy_ref: proposal.retention_policy_ref.as_str().to_owned(),
        occurred_at: Some(timestamp(proposal.occurred_at)),
        committed_at: Some(timestamp(record.committed_at)),
        trace_id: proposal.trace_id.map(|value| value.as_bytes().to_vec()),
    })
}

fn security_audit_record(
    value: wire::SecurityAuditRecord,
) -> Result<SecurityAuditRecord, SecurityAuditCodecError> {
    let proposal = ProposedSecurityAuditRecord {
        schema_version: DottedKey::new(value.schema_version)?,
        audit_record_id: parse_id(value.audit_record_id.map(|value| value.value))?,
        tenant_id: parse_id(value.tenant_id.map(|value| value.value))?,
        actor_principal_id: parse_id(value.actor_principal_id.map(|value| value.value))?,
        actor_principal_kind: decode_principal_kind(value.actor_principal_kind)?,
        action: decode_action(value.action)?,
        outcome: decode_outcome(value.outcome)?,
        resource_refs: value
            .resource_refs
            .into_iter()
            .map(decode_resource_ref)
            .collect::<Result<_, _>>()?,
        source_fact_ref: value
            .source_fact
            .map(|source| match source {
                wire::security_audit_record::SourceFact::SourceLedgerRecordId(value) => {
                    parse_id(Some(value.value)).map(SecurityAuditSourceRef::LedgerRecord)
                }
                wire::security_audit_record::SourceFact::SourceErasureJobId(value) => {
                    parse_id(Some(value.value)).map(SecurityAuditSourceRef::ErasureJob)
                }
                wire::security_audit_record::SourceFact::SourceModelInvocationId(value) => {
                    parse_id(Some(value.value)).map(SecurityAuditSourceRef::ModelInvocation)
                }
                wire::security_audit_record::SourceFact::SourceToolInvocationId(value) => {
                    parse_id(Some(value.value)).map(SecurityAuditSourceRef::ToolInvocation)
                }
            })
            .transpose()?,
        correlation_id: parse_id(value.correlation_id.map(|value| value.value))?,
        trace_id: value
            .trace_id
            .map(SecurityAuditTraceId::from_bytes)
            .transpose()?,
        detail_ref: value.detail_ref.map(decode_content_ref).transpose()?,
        retention_policy_ref: DataRetentionPolicyRef::new(value.retention_policy_ref)?,
        occurred_at: decode_timestamp(value.occurred_at)?,
    };
    let record = SecurityAuditRecord {
        proposal,
        committed_at: decode_timestamp(value.committed_at)?,
    };
    record.validate()?;
    Ok(record)
}

fn action(value: SecurityAuditAction) -> wire::SecurityAuditAction {
    match value {
        SecurityAuditAction::ApprovalDecision => wire::SecurityAuditAction::ApprovalDecision,
        SecurityAuditAction::CredentialAccess => wire::SecurityAuditAction::CredentialAccess,
        SecurityAuditAction::AuthorizationDenied => wire::SecurityAuditAction::AuthorizationDenied,
        SecurityAuditAction::SkillLifecycleChange => {
            wire::SecurityAuditAction::SkillLifecycleChange
        }
        SecurityAuditAction::AgentVersionPublished => {
            wire::SecurityAuditAction::AgentVersionPublished
        }
        SecurityAuditAction::DataErasure => wire::SecurityAuditAction::DataErasure,
        SecurityAuditAction::OwnershipCutover => wire::SecurityAuditAction::OwnershipCutover,
        SecurityAuditAction::AdmissionDecision => wire::SecurityAuditAction::AdmissionDecision,
        SecurityAuditAction::ModelInvocation => wire::SecurityAuditAction::ModelInvocation,
        SecurityAuditAction::ToolInvocation => wire::SecurityAuditAction::ToolInvocation,
        SecurityAuditAction::InteractionResponse => wire::SecurityAuditAction::InteractionResponse,
        SecurityAuditAction::ToolInvocationIndeterminate => {
            wire::SecurityAuditAction::ToolInvocationIndeterminate
        }
    }
}

fn decode_action(value: i32) -> Result<SecurityAuditAction, SecurityAuditCodecError> {
    match wire::SecurityAuditAction::try_from(value) {
        Ok(wire::SecurityAuditAction::ApprovalDecision) => {
            Ok(SecurityAuditAction::ApprovalDecision)
        }
        Ok(wire::SecurityAuditAction::CredentialAccess) => {
            Ok(SecurityAuditAction::CredentialAccess)
        }
        Ok(wire::SecurityAuditAction::AuthorizationDenied) => {
            Ok(SecurityAuditAction::AuthorizationDenied)
        }
        Ok(wire::SecurityAuditAction::SkillLifecycleChange) => {
            Ok(SecurityAuditAction::SkillLifecycleChange)
        }
        Ok(wire::SecurityAuditAction::AgentVersionPublished) => {
            Ok(SecurityAuditAction::AgentVersionPublished)
        }
        Ok(wire::SecurityAuditAction::DataErasure) => Ok(SecurityAuditAction::DataErasure),
        Ok(wire::SecurityAuditAction::OwnershipCutover) => {
            Ok(SecurityAuditAction::OwnershipCutover)
        }
        Ok(wire::SecurityAuditAction::AdmissionDecision) => {
            Ok(SecurityAuditAction::AdmissionDecision)
        }
        Ok(wire::SecurityAuditAction::ModelInvocation) => Ok(SecurityAuditAction::ModelInvocation),
        Ok(wire::SecurityAuditAction::ToolInvocation) => Ok(SecurityAuditAction::ToolInvocation),
        Ok(wire::SecurityAuditAction::InteractionResponse) => {
            Ok(SecurityAuditAction::InteractionResponse)
        }
        Ok(wire::SecurityAuditAction::ToolInvocationIndeterminate) => {
            Ok(SecurityAuditAction::ToolInvocationIndeterminate)
        }
        _ => Err(SecurityAuditCodecError::InvalidContract),
    }
}

fn outcome(value: SecurityAuditOutcome) -> wire::SecurityAuditOutcome {
    match value {
        SecurityAuditOutcome::Succeeded => wire::SecurityAuditOutcome::Succeeded,
        SecurityAuditOutcome::Denied => wire::SecurityAuditOutcome::Denied,
        SecurityAuditOutcome::Failed => wire::SecurityAuditOutcome::Failed,
    }
}

fn decode_outcome(value: i32) -> Result<SecurityAuditOutcome, SecurityAuditCodecError> {
    match wire::SecurityAuditOutcome::try_from(value) {
        Ok(wire::SecurityAuditOutcome::Succeeded) => Ok(SecurityAuditOutcome::Succeeded),
        Ok(wire::SecurityAuditOutcome::Denied) => Ok(SecurityAuditOutcome::Denied),
        Ok(wire::SecurityAuditOutcome::Failed) => Ok(SecurityAuditOutcome::Failed),
        _ => Err(SecurityAuditCodecError::InvalidContract),
    }
}

#[derive(Debug, Error)]
/// Malformed protobuf or a violation of the closed audit contract.
pub enum SecurityAuditCodecError {
    #[error("security audit protobuf payload is malformed")]
    Malformed,
    #[error("security audit protobuf violates the closed contract")]
    InvalidContract,
    #[error(transparent)]
    Contract(#[from] SecurityAuditContractError),
    #[error(transparent)]
    LogicalKey(#[from] musteragent_identity::LogicalKeyError),
    #[error(transparent)]
    RetentionPolicy(#[from] musteragent_runtime_audit::DataRetentionPolicyRefError),
}

impl From<WireContractError> for SecurityAuditCodecError {
    fn from(_: WireContractError) -> Self {
        Self::InvalidContract
    }
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{
        AuditRecordId, CorrelationId, DottedKey, ErasureJobId, InteractionId, LedgerRecordId,
        PrincipalId, PrincipalKind, TenantId, UtcTimestamp,
    };
    use musteragent_runtime_audit::{
        DataRetentionPolicyRef, ProposedSecurityAuditRecord, SecurityAuditAction,
        SecurityAuditOutcome, SecurityAuditRecord, SecurityAuditSourceRef, SecurityAuditTraceId,
    };
    use musteragent_runtime_contract::ResourceRef;

    use prost::Message as _;

    use super::{
        decode_security_audit_record, encode_security_audit_record, security_audit_record_message,
    };

    fn record() -> SecurityAuditRecord {
        SecurityAuditRecord {
            proposal: ProposedSecurityAuditRecord {
                schema_version: DottedKey::new("musteragent.security.audit.v1").expect("schema"),
                audit_record_id: AuditRecordId::generate(),
                tenant_id: TenantId::generate(),
                actor_principal_id: PrincipalId::generate(),
                actor_principal_kind: PrincipalKind::User,
                action: SecurityAuditAction::ApprovalDecision,
                outcome: SecurityAuditOutcome::Succeeded,
                resource_refs: vec![ResourceRef::Interaction(InteractionId::generate())],
                source_fact_ref: Some(SecurityAuditSourceRef::LedgerRecord(
                    LedgerRecordId::generate(),
                )),
                correlation_id: CorrelationId::generate(),
                trace_id: Some(
                    SecurityAuditTraceId::from_bytes(vec![1; 16]).expect("trace identity"),
                ),
                detail_ref: None,
                retention_policy_ref: DataRetentionPolicyRef::new("security.audit.standard@1")
                    .expect("retention"),
                occurred_at: UtcTimestamp::from_unix_millis(100),
            },
            committed_at: UtcTimestamp::from_unix_millis(101),
        }
    }

    #[test]
    fn security_audit_round_trips_without_entering_the_ledger_payload() {
        let expected = record();
        let bytes = encode_security_audit_record(&expected).expect("encode");
        assert_eq!(
            decode_security_audit_record(&bytes).expect("decode"),
            expected
        );
    }

    #[test]
    fn malformed_or_incomplete_security_audit_payload_is_rejected() {
        assert!(decode_security_audit_record(b"not-protobuf").is_err());
        let mut invalid = record();
        invalid.proposal.source_fact_ref = None;
        assert!(encode_security_audit_record(&invalid).is_err());

        let mut unknown_action = security_audit_record_message(&record()).expect("message");
        unknown_action.action = 99;
        assert!(decode_security_audit_record(&unknown_action.encode_to_vec()).is_err());

        let mut unknown_outcome = security_audit_record_message(&record()).expect("message");
        unknown_outcome.outcome = 99;
        assert!(decode_security_audit_record(&unknown_outcome.encode_to_vec()).is_err());
    }

    #[test]
    fn data_erasure_uses_the_typed_erasure_job_source_and_resource() {
        let erasure_job_id = ErasureJobId::generate();
        let mut expected = record();
        expected.proposal.action = SecurityAuditAction::DataErasure;
        expected.proposal.trace_id = None;
        expected.proposal.resource_refs = vec![ResourceRef::ErasureJob(erasure_job_id)];
        expected.proposal.source_fact_ref =
            Some(SecurityAuditSourceRef::ErasureJob(erasure_job_id));
        let bytes = encode_security_audit_record(&expected).expect("encode erasure audit");
        assert_eq!(
            decode_security_audit_record(&bytes).expect("decode erasure audit"),
            expected
        );

        expected.proposal.source_fact_ref = Some(SecurityAuditSourceRef::LedgerRecord(
            LedgerRecordId::generate(),
        ));
        assert!(encode_security_audit_record(&expected).is_err());
    }
}
