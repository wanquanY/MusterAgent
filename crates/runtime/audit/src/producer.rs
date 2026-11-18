use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, AuditRecordId, CommandId, CorrelationId, DottedKey, ErasureJobId, ErasureReceiptId,
    InteractionId, InvocationId, LedgerRecordId, RunId, UtcTimestamp,
};
use musteragent_runtime_contract::ResourceRef;

use crate::{
    DataRetentionPolicyRef, ProposedSecurityAuditRecord, SecurityAuditAction,
    SecurityAuditContractError, SecurityAuditOutcome, SecurityAuditSourceRef, SecurityAuditTraceId,
};

const AUDIT_SCHEMA: &str = "musteragent.legacy.security.audit.v2";

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ApprovalAuditDecision {
    Approved,
    Denied,
}

/// Closed production truth table. Callers supply canonical identities only;
/// action, outcome, resources and source ownership cannot drift independently.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SecurityAuditFact {
    AdmissionAllowed {
        conversation_id: musteragent_identity::ConversationId,
        run_id: RunId,
        source_ledger_record_id: LedgerRecordId,
    },
    AdmissionDenied {
        command_id: CommandId,
        conversation_id: musteragent_identity::ConversationId,
    },
    ModelInvocationTerminal {
        run_id: RunId,
        invocation_id: InvocationId,
        outcome: SecurityAuditOutcome,
    },
    ToolInvocationTerminal {
        run_id: RunId,
        invocation_id: InvocationId,
        outcome: SecurityAuditOutcome,
    },
    /// Immutable observation that an invocation could not yet be proven
    /// applied or not applied. A later terminal transition is a distinct fact.
    ToolInvocationIndeterminate {
        run_id: RunId,
        invocation_id: InvocationId,
    },
    InteractionResponded {
        run_id: RunId,
        interaction_id: InteractionId,
        source_ledger_record_id: LedgerRecordId,
    },
    ApprovalDecided {
        run_id: RunId,
        interaction_id: InteractionId,
        source_ledger_record_id: LedgerRecordId,
        decision: ApprovalAuditDecision,
    },
    CredentialAccessed {
        run_id: RunId,
        invocation_id: InvocationId,
    },
    DataErasureCompleted {
        erasure_job_id: ErasureJobId,
        erasure_receipt_id: ErasureReceiptId,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct SecurityAuditProducerContext {
    pub actor: Actor,
    pub correlation_id: CorrelationId,
    pub trace_id: Option<SecurityAuditTraceId>,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone)]
pub struct SecurityAuditProducer {
    retention_policy_ref: DataRetentionPolicyRef,
}

impl SecurityAuditProducer {
    #[must_use]
    pub fn new(retention_policy_ref: DataRetentionPolicyRef) -> Self {
        Self {
            retention_policy_ref,
        }
    }

    pub fn propose(
        &self,
        context: SecurityAuditProducerContext,
        fact: SecurityAuditFact,
    ) -> Result<ProposedSecurityAuditRecord, SecurityAuditContractError> {
        let (identity, action, outcome, resource_refs, source_fact_ref) = map_fact(&fact);
        if action != SecurityAuditAction::DataErasure && context.trace_id.is_none() {
            return Err(SecurityAuditContractError::TraceIdentityRequired);
        }
        let proposal = ProposedSecurityAuditRecord {
            schema_version: DottedKey::new(AUDIT_SCHEMA)
                .map_err(|_| SecurityAuditContractError::InvalidProducerIdentity)?,
            audit_record_id: audit_record_id(context.actor, action, identity)?,
            tenant_id: context.actor.tenant_id,
            actor_principal_id: context.actor.principal_id,
            actor_principal_kind: context.actor.principal_kind,
            action,
            outcome,
            resource_refs,
            source_fact_ref,
            correlation_id: context.correlation_id,
            trace_id: context.trace_id,
            detail_ref: None,
            retention_policy_ref: self.retention_policy_ref.clone(),
            occurred_at: context.occurred_at,
        };
        proposal.validate()?;
        Ok(proposal)
    }
}

fn map_fact(
    fact: &SecurityAuditFact,
) -> (
    uuid::Uuid,
    SecurityAuditAction,
    SecurityAuditOutcome,
    Vec<ResourceRef>,
    Option<SecurityAuditSourceRef>,
) {
    match *fact {
        SecurityAuditFact::AdmissionAllowed {
            conversation_id,
            run_id,
            source_ledger_record_id,
        } => (
            source_ledger_record_id.into_uuid(),
            SecurityAuditAction::AdmissionDecision,
            SecurityAuditOutcome::Succeeded,
            vec![
                ResourceRef::Conversation(conversation_id),
                ResourceRef::Run(run_id),
            ],
            Some(SecurityAuditSourceRef::LedgerRecord(
                source_ledger_record_id,
            )),
        ),
        SecurityAuditFact::AdmissionDenied {
            command_id,
            conversation_id,
        } => (
            command_id.into_uuid(),
            SecurityAuditAction::AuthorizationDenied,
            SecurityAuditOutcome::Denied,
            vec![ResourceRef::Conversation(conversation_id)],
            None,
        ),
        SecurityAuditFact::ModelInvocationTerminal {
            run_id,
            invocation_id,
            outcome,
        } => (
            invocation_id.into_uuid(),
            SecurityAuditAction::ModelInvocation,
            outcome,
            vec![ResourceRef::Run(run_id)],
            Some(SecurityAuditSourceRef::ModelInvocation(invocation_id)),
        ),
        SecurityAuditFact::ToolInvocationTerminal {
            run_id,
            invocation_id,
            outcome,
        } => (
            invocation_id.into_uuid(),
            SecurityAuditAction::ToolInvocation,
            outcome,
            vec![ResourceRef::Run(run_id)],
            Some(SecurityAuditSourceRef::ToolInvocation(invocation_id)),
        ),
        SecurityAuditFact::ToolInvocationIndeterminate {
            run_id,
            invocation_id,
        } => (
            invocation_id.into_uuid(),
            SecurityAuditAction::ToolInvocationIndeterminate,
            SecurityAuditOutcome::Failed,
            vec![ResourceRef::Run(run_id)],
            Some(SecurityAuditSourceRef::ToolInvocation(invocation_id)),
        ),
        SecurityAuditFact::InteractionResponded {
            run_id,
            interaction_id,
            source_ledger_record_id,
        } => (
            source_ledger_record_id.into_uuid(),
            SecurityAuditAction::InteractionResponse,
            SecurityAuditOutcome::Succeeded,
            vec![
                ResourceRef::Run(run_id),
                ResourceRef::Interaction(interaction_id),
            ],
            Some(SecurityAuditSourceRef::LedgerRecord(
                source_ledger_record_id,
            )),
        ),
        SecurityAuditFact::ApprovalDecided {
            run_id,
            interaction_id,
            source_ledger_record_id,
            decision,
        } => (
            source_ledger_record_id.into_uuid(),
            SecurityAuditAction::ApprovalDecision,
            match decision {
                ApprovalAuditDecision::Approved => SecurityAuditOutcome::Succeeded,
                ApprovalAuditDecision::Denied => SecurityAuditOutcome::Denied,
            },
            vec![
                ResourceRef::Run(run_id),
                ResourceRef::Interaction(interaction_id),
            ],
            Some(SecurityAuditSourceRef::LedgerRecord(
                source_ledger_record_id,
            )),
        ),
        SecurityAuditFact::CredentialAccessed {
            run_id,
            invocation_id,
        } => (
            invocation_id.into_uuid(),
            SecurityAuditAction::CredentialAccess,
            SecurityAuditOutcome::Succeeded,
            vec![ResourceRef::Run(run_id)],
            Some(SecurityAuditSourceRef::ModelInvocation(invocation_id)),
        ),
        SecurityAuditFact::DataErasureCompleted {
            erasure_job_id,
            erasure_receipt_id,
        } => (
            erasure_job_id.into_uuid(),
            SecurityAuditAction::DataErasure,
            SecurityAuditOutcome::Succeeded,
            vec![
                ResourceRef::ErasureJob(erasure_job_id),
                ResourceRef::ErasureReceipt(erasure_receipt_id),
            ],
            Some(SecurityAuditSourceRef::ErasureJob(erasure_job_id)),
        ),
    }
}

fn audit_record_id(
    actor: Actor,
    action: SecurityAuditAction,
    identity: uuid::Uuid,
) -> Result<AuditRecordId, SecurityAuditContractError> {
    let mut seed = b"musteragent.legacy.security.audit.producer.v2\0".to_vec();
    seed.extend_from_slice(actor.tenant_id.into_uuid().as_bytes());
    seed.extend_from_slice(&action.code().to_be_bytes());
    seed.extend_from_slice(identity.as_bytes());
    let digest = Sha256::digest(seed).into_bytes();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    AuditRecordId::from_uuid(uuid::Uuid::from_bytes(bytes))
        .map_err(|_| SecurityAuditContractError::InvalidProducerIdentity)
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{
        Actor, CommandId, ConversationId, CorrelationId, ErasureJobId, ErasureReceiptId,
        InteractionId, InvocationId, LedgerRecordId, PrincipalId, PrincipalKind, RunId, TenantId,
        UtcTimestamp,
    };
    use musteragent_runtime_contract::ResourceRef;

    use super::{
        ApprovalAuditDecision, SecurityAuditFact, SecurityAuditProducer,
        SecurityAuditProducerContext,
    };
    use crate::{
        DataRetentionPolicyRef, SecurityAuditAction, SecurityAuditOutcome, SecurityAuditSourceRef,
        SecurityAuditTraceId,
    };

    fn producer() -> SecurityAuditProducer {
        SecurityAuditProducer::new(
            DataRetentionPolicyRef::new("security.audit.standard@1").expect("retention policy"),
        )
    }

    fn context(tenant_id: TenantId, principal_id: PrincipalId) -> SecurityAuditProducerContext {
        SecurityAuditProducerContext {
            actor: Actor {
                tenant_id,
                principal_id,
                principal_kind: PrincipalKind::User,
            },
            correlation_id: CorrelationId::generate(),
            trace_id: Some(
                SecurityAuditTraceId::from_bytes(vec![31_u8; 16]).expect("trace identity"),
            ),
            occurred_at: UtcTimestamp::from_unix_millis(1_700_000_000_000),
        }
    }

    #[test]
    fn production_truth_table_seals_action_outcome_resources_and_source() {
        let tenant_id = TenantId::generate();
        let context = context(tenant_id, PrincipalId::generate());
        let conversation_id = ConversationId::generate();
        let run_id = RunId::generate();
        let command_id = CommandId::generate();
        let model_invocation_id = InvocationId::generate();
        let tool_invocation_id = InvocationId::generate();
        let interaction_id = InteractionId::generate();
        let admitted_record_id = LedgerRecordId::generate();
        let interaction_record_id = LedgerRecordId::generate();
        let approval_record_id = LedgerRecordId::generate();
        let erasure_job_id = ErasureJobId::generate();
        let erasure_receipt_id = ErasureReceiptId::generate();
        let cases = vec![
            (
                SecurityAuditFact::AdmissionAllowed {
                    conversation_id,
                    run_id,
                    source_ledger_record_id: admitted_record_id,
                },
                SecurityAuditAction::AdmissionDecision,
                SecurityAuditOutcome::Succeeded,
                vec![
                    ResourceRef::Conversation(conversation_id),
                    ResourceRef::Run(run_id),
                ],
                Some(SecurityAuditSourceRef::LedgerRecord(admitted_record_id)),
            ),
            (
                SecurityAuditFact::AdmissionDenied {
                    command_id,
                    conversation_id,
                },
                SecurityAuditAction::AuthorizationDenied,
                SecurityAuditOutcome::Denied,
                vec![ResourceRef::Conversation(conversation_id)],
                None,
            ),
            (
                SecurityAuditFact::ModelInvocationTerminal {
                    run_id,
                    invocation_id: model_invocation_id,
                    outcome: SecurityAuditOutcome::Failed,
                },
                SecurityAuditAction::ModelInvocation,
                SecurityAuditOutcome::Failed,
                vec![ResourceRef::Run(run_id)],
                Some(SecurityAuditSourceRef::ModelInvocation(model_invocation_id)),
            ),
            (
                SecurityAuditFact::ToolInvocationTerminal {
                    run_id,
                    invocation_id: tool_invocation_id,
                    outcome: SecurityAuditOutcome::Denied,
                },
                SecurityAuditAction::ToolInvocation,
                SecurityAuditOutcome::Denied,
                vec![ResourceRef::Run(run_id)],
                Some(SecurityAuditSourceRef::ToolInvocation(tool_invocation_id)),
            ),
            (
                SecurityAuditFact::ToolInvocationIndeterminate {
                    run_id,
                    invocation_id: tool_invocation_id,
                },
                SecurityAuditAction::ToolInvocationIndeterminate,
                SecurityAuditOutcome::Failed,
                vec![ResourceRef::Run(run_id)],
                Some(SecurityAuditSourceRef::ToolInvocation(tool_invocation_id)),
            ),
            (
                SecurityAuditFact::InteractionResponded {
                    run_id,
                    interaction_id,
                    source_ledger_record_id: interaction_record_id,
                },
                SecurityAuditAction::InteractionResponse,
                SecurityAuditOutcome::Succeeded,
                vec![
                    ResourceRef::Run(run_id),
                    ResourceRef::Interaction(interaction_id),
                ],
                Some(SecurityAuditSourceRef::LedgerRecord(interaction_record_id)),
            ),
            (
                SecurityAuditFact::ApprovalDecided {
                    run_id,
                    interaction_id,
                    source_ledger_record_id: approval_record_id,
                    decision: ApprovalAuditDecision::Denied,
                },
                SecurityAuditAction::ApprovalDecision,
                SecurityAuditOutcome::Denied,
                vec![
                    ResourceRef::Run(run_id),
                    ResourceRef::Interaction(interaction_id),
                ],
                Some(SecurityAuditSourceRef::LedgerRecord(approval_record_id)),
            ),
            (
                SecurityAuditFact::CredentialAccessed {
                    run_id,
                    invocation_id: model_invocation_id,
                },
                SecurityAuditAction::CredentialAccess,
                SecurityAuditOutcome::Succeeded,
                vec![ResourceRef::Run(run_id)],
                Some(SecurityAuditSourceRef::ModelInvocation(model_invocation_id)),
            ),
            (
                SecurityAuditFact::DataErasureCompleted {
                    erasure_job_id,
                    erasure_receipt_id,
                },
                SecurityAuditAction::DataErasure,
                SecurityAuditOutcome::Succeeded,
                vec![
                    ResourceRef::ErasureJob(erasure_job_id),
                    ResourceRef::ErasureReceipt(erasure_receipt_id),
                ],
                Some(SecurityAuditSourceRef::ErasureJob(erasure_job_id)),
            ),
        ];

        for (fact, action, outcome, resources, source) in cases {
            let proposal = producer()
                .propose(context, fact)
                .expect("closed producer mapping");
            assert_eq!(
                proposal.schema_version.as_str(),
                "musteragent.legacy.security.audit.v2"
            );
            assert_eq!(proposal.tenant_id, tenant_id);
            assert_eq!(proposal.action, action);
            assert_eq!(proposal.outcome, outcome);
            assert_eq!(proposal.resource_refs, resources);
            assert_eq!(proposal.source_fact_ref, source);
            assert_eq!(proposal.trace_id, context.trace_id);
            assert_eq!(
                proposal.detail_ref, None,
                "producer must never carry content"
            );
        }
    }

    #[test]
    fn canonical_identity_is_deterministic_and_namespaced_by_action_and_tenant() {
        let tenant_id = TenantId::generate();
        let principal_id = PrincipalId::generate();
        let producer_context = context(tenant_id, principal_id);
        let run_id = RunId::generate();
        let invocation_id = InvocationId::generate();
        let fact = SecurityAuditFact::ModelInvocationTerminal {
            run_id,
            invocation_id,
            outcome: SecurityAuditOutcome::Succeeded,
        };
        let first = producer()
            .propose(producer_context, fact.clone())
            .expect("first proposal");
        let replay = producer()
            .propose(producer_context, fact)
            .expect("replayed proposal");
        assert_eq!(first, replay);

        let credential = producer()
            .propose(
                producer_context,
                SecurityAuditFact::CredentialAccessed {
                    run_id,
                    invocation_id,
                },
            )
            .expect("credential proposal");
        assert_ne!(first.audit_record_id, credential.audit_record_id);

        let other_tenant = producer()
            .propose(
                context(TenantId::generate(), principal_id),
                SecurityAuditFact::ModelInvocationTerminal {
                    run_id,
                    invocation_id,
                    outcome: SecurityAuditOutcome::Succeeded,
                },
            )
            .expect("other tenant proposal");
        assert_ne!(first.audit_record_id, other_tenant.audit_record_id);
    }

    #[test]
    fn production_v2_requires_trace_for_ingress_facts_but_not_erasure() {
        let tenant_id = TenantId::generate();
        let mut producer_context = context(tenant_id, PrincipalId::generate());
        producer_context.trace_id = None;
        let run_id = RunId::generate();
        let credential = producer().propose(
            producer_context,
            SecurityAuditFact::CredentialAccessed {
                run_id,
                invocation_id: InvocationId::generate(),
            },
        );
        assert_eq!(
            credential,
            Err(crate::SecurityAuditContractError::TraceIdentityRequired)
        );

        let erasure = producer()
            .propose(
                producer_context,
                SecurityAuditFact::DataErasureCompleted {
                    erasure_job_id: ErasureJobId::generate(),
                    erasure_receipt_id: ErasureReceiptId::generate(),
                },
            )
            .expect("Erasure does not invent a Run trace");
        assert_eq!(erasure.trace_id, None);
    }
}
