//! Original Integration Evidence provider, driven by the original Erasure job.
//! This port carries no mailbox locator, credentials, byte content or new queue.
use crate::{
    ErasureContractError, ErasureDataClass, ErasurePlanSnapshot, ErasureRequest,
    WORK_ITEM_EXECUTION_ERASURE_SCHEMA,
};
use musteragent_content_ref::Sha256;
use musteragent_identity::{ConversationId, InvocationId, PrincipalKind, RunId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;

pub const SOURCE_ATTACHMENT_PRODUCER_TOOL: &str = "sources.attachments.import@1";
pub const MAX_SOURCE_EVIDENCE_PRODUCERS: usize = 65_536;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct SourceEvidenceProducer {
    pub run_id: RunId,
    pub conversation_id: ConversationId,
    pub invocation_id: InvocationId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceEvidenceErasureCommand {
    pub request: ErasureRequest,
    pub plan: ErasurePlanSnapshot,
    pub producer: SourceEvidenceProducer,
}
impl SourceEvidenceErasureCommand {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        self.request.validate()?;
        self.plan.validate()?;
        if self.request.schema_version.as_str() != WORK_ITEM_EXECUTION_ERASURE_SCHEMA
            || self.plan.schema_version != self.request.schema_version
            || self.request.requested_by.principal_kind != PrincipalKind::User
            || self.request.requested_by.tenant_id != self.request.tenant_id
            || self.plan.erasure_job_id != self.request.erasure_job_id
            || self.plan.plan_version.get() != 2
            || self.plan.root_resource_refs != self.request.root_resource_refs
            || self.plan.frozen_at < self.request.requested_at
            || !self
                .plan
                .data_classes
                .contains(&ErasureDataClass::ContentCiphertext)
            || self
                .plan
                .retention_exceptions
                .iter()
                .any(|e| e.data_class == ErasureDataClass::ContentCiphertext)
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceEvidenceErasureReceipt {
    pub command: SourceEvidenceErasureCommand,
    pub object_ids: Vec<uuid::Uuid>,
    pub completed_at: UtcTimestamp,
    pub proof_digest: Sha256,
}
impl SourceEvidenceErasureReceipt {
    pub fn validate(&self) -> Result<(), ErasureContractError> {
        self.command.validate()?;
        if self.object_ids.len() > 2
            || self.object_ids.iter().any(uuid::Uuid::is_nil)
            || self.object_ids.windows(2).any(|w| w[0] >= w[1])
            || self.completed_at < self.command.plan.frozen_at
            || self.proof_digest != self.expected_digest()
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        Ok(())
    }
    pub fn expected_digest(&self) -> Sha256 {
        let c = &self.command;
        let mut bytes = b"musteragent.integration-evidence.erasure-receipt.v1\0".to_vec();
        for id in [
            c.request.tenant_id.into_uuid(),
            c.request.requested_by.principal_id.into_uuid(),
            c.request.erasure_job_id.into_uuid(),
            c.producer.run_id.into_uuid(),
            c.producer.conversation_id.into_uuid(),
            c.producer.invocation_id.into_uuid(),
        ] {
            bytes.extend_from_slice(id.as_bytes());
        }
        bytes.extend_from_slice(&c.plan.plan_hash.into_bytes());
        bytes.extend_from_slice(&(self.object_ids.len() as u64).to_be_bytes());
        for id in &self.object_ids {
            bytes.extend_from_slice(id.as_bytes());
        }
        bytes.extend_from_slice(&self.completed_at.unix_millis().to_be_bytes());
        Sha256::digest(bytes)
    }
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("original Integration Evidence erasure is not confirmed")]
pub struct SourceEvidenceErasureError;

/// Called only after the original Task producer cut is permanently frozen.
/// Transport loss/revoked source credentials cannot imply a provider receipt.
/// A repeat uses the same original job/plan/producer, including after restart.
pub trait SourceEvidenceErasureProvider: Send + Sync {
    fn erase(
        &self,
        command: SourceEvidenceErasureCommand,
    ) -> ApplicationFuture<'_, Result<SourceEvidenceErasureReceipt, SourceEvidenceErasureError>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use musteragent_content_ref::{ContentRef, MediaType};
    use musteragent_identity::*;
    use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};
    fn fixture() -> SourceEvidenceErasureCommand {
        let u = uuid::Uuid::from_u128;
        let actor = Actor {
            tenant_id: TenantId::from_uuid(u(1)).unwrap(),
            principal_id: PrincipalId::from_uuid(u(2)).unwrap(),
            principal_kind: PrincipalKind::User,
        };
        let request = ErasureRequest {
            schema_version: DottedKey::new(WORK_ITEM_EXECUTION_ERASURE_SCHEMA).unwrap(),
            erasure_job_id: ErasureJobId::from_uuid(u(6)).unwrap(),
            tenant_id: actor.tenant_id,
            requested_by: actor,
            root_resource_refs: vec![ResourceRef::Content(ContentRef {
                content_id: ContentId::from_uuid(u(7)).unwrap(),
                media_type: MediaType::new("text/plain").unwrap(),
                byte_length: 4,
                sha256: Sha256::digest(b"goal"),
            })],
            retention_policy_ref: DataRetentionPolicyRef::new(WORK_ITEM_EXECUTION_ERASURE_POLICY)
                .unwrap(),
            idempotency_key: IdempotencyKey::new("erase-original-task").unwrap(),
            requested_at: UtcTimestamp::from_unix_millis(1000),
            complete_by: UtcTimestamp::from_unix_millis(5000),
        };
        let plan = ErasurePlanSnapshot::new(
            request.schema_version.clone(),
            request.erasure_job_id,
            ErasurePlanVersion::new(2).unwrap(),
            request.root_resource_refs.clone(),
            vec![
                ErasureDataClass::ContentCiphertext,
                ErasureDataClass::Projection,
                ErasureDataClass::SnapshotInlineContent,
                ErasureDataClass::FullTextIndex,
                ErasureDataClass::EmbeddingIndex,
                ErasureDataClass::Cache,
                ErasureDataClass::ObjectHistory,
                ErasureDataClass::Backup,
            ],
            vec![
                ErasureRetentionException {
                    data_class: ErasureDataClass::ObjectHistory,
                    policy_ref: DataRetentionPolicyRef::new("privacy.original@1").unwrap(),
                    owner_principal_id: actor.principal_id,
                    expires_at: UtcTimestamp::from_unix_millis(9000),
                },
                ErasureRetentionException {
                    data_class: ErasureDataClass::Backup,
                    policy_ref: DataRetentionPolicyRef::new("privacy.original@1").unwrap(),
                    owner_principal_id: actor.principal_id,
                    expires_at: UtcTimestamp::from_unix_millis(10000),
                },
            ],
            Sha256::digest(b"coverage"),
            UtcTimestamp::from_unix_millis(1200),
        )
        .unwrap();
        SourceEvidenceErasureCommand {
            request,
            plan,
            producer: SourceEvidenceProducer {
                run_id: RunId::from_uuid(u(3)).unwrap(),
                conversation_id: ConversationId::from_uuid(u(4)).unwrap(),
                invocation_id: InvocationId::from_uuid(u(5)).unwrap(),
            },
        }
    }
    #[test]
    fn original_plan_and_provider_receipt_match_kernel_golden() {
        let c = fixture();
        c.validate().unwrap();
        assert_eq!(
            c.plan.plan_hash.to_string(),
            "07d4e5d1592a5c5ed530ea2383c428e6792951fab3a624aa29fe1e67c9a52097"
        );
        let r = SourceEvidenceErasureReceipt {
            command: c,
            object_ids: vec![uuid::Uuid::from_u128(8), uuid::Uuid::from_u128(9)],
            completed_at: UtcTimestamp::from_unix_millis(2345),
            proof_digest: Sha256::from_hex(
                "fbd3b7ce4276c1002bab56aa5f8024a315cae83315d20c4e5add1a69ebaf0b36",
            )
            .unwrap(),
        };
        assert_eq!(r.expected_digest().to_string(), r.proof_digest.to_string());
        r.validate().unwrap();
        for index in 0..6 {
            let mut changed = r.clone();
            match index {
                0 => changed.command.producer.invocation_id = InvocationId::generate(),
                1 => changed.command.request.requested_by.principal_id = PrincipalId::generate(),
                2 => changed.object_ids.reverse(),
                3 => changed.object_ids.push(uuid::Uuid::from_u128(10)),
                4 => changed.completed_at = UtcTimestamp::from_unix_millis(1100),
                _ => changed.object_ids.clear(),
            }
            assert!(changed.validate().is_err());
        }
    }
    #[test]
    fn forward_source_coverage_binds_exact_task_execution_not_shared_input() {
        let c = fixture();
        let ResourceRef::Content(content) = c.request.root_resource_refs[0].clone() else {
            panic!()
        };
        let mut coverage = WorkItemExecutionErasureCoverage {
            scope_fingerprint: Sha256::digest(b"task"),
            executions: vec![WorkItemErasureExecution {
                run_id: c.producer.run_id,
                conversation_id: c.producer.conversation_id,
                goal_revision: 1,
                control_revision: 1,
                assignment_epoch: 1,
            }],
            contents: vec![content],
            source_evidence: vec![],
            frozen_at: UtcTimestamp::from_unix_millis(1200),
        };
        let old = coverage.fingerprint().unwrap();
        assert_eq!(coverage.version(), 1);
        coverage.source_evidence.push(c.producer);
        let new = coverage.fingerprint().unwrap();
        assert_eq!(coverage.version(), 2);
        assert_ne!(old, new);
        coverage.source_evidence[0].run_id = RunId::generate();
        assert!(coverage.validate().is_err());
        coverage.source_evidence[0] = c.producer;
        coverage.source_evidence.push(c.producer);
        assert!(coverage.validate().is_err());
        coverage.source_evidence.clear();
        assert_eq!(old, coverage.fingerprint().unwrap());
    }
}
