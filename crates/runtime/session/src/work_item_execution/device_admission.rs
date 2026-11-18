//! Device Task admission and terminal evidence are distinct from Intent custody.
use super::*;
mod result_cut;
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_execution_grant::{ExecutionGrant, ExecutionGrantAuthorityScope};
pub use result_cut::*;

pub fn validate_work_item_device_grant(
    source: &WorkItemDeviceExecutionPreparation,
    grant: &ExecutionGrant,
) -> Result<(), DialogueStoreError> {
    source.validate()?;
    grant.validate().map_err(|_| DialogueStoreError::Corrupt)?;
    let p = source.preparation();
    let target = source.target();
    let identity = grant.identity_binding.ok_or(DialogueStoreError::Corrupt)?;
    if p.delegation
        .product_release
        .as_ref()
        .is_some_and(|r| grant.product_release.as_ref() != Some(r))
        || grant.tenant_id != p.key.actor.tenant_id
        || grant.run_id != p.identities().run_id()?
        || grant.audience.as_str() != format!("musteragent.runtime.device.id-{}", target.device_id)
        || grant.nonce.as_str() != source.grant_nonce()?
        || !matches!(
            grant.surface.as_ref().map(|v| v.as_str()),
            Some("desktop" | "mobile")
        )
        || grant.permission_mode != Some(p.configuration.permission_mode)
        || identity.principal_id != p.key.scope.owner_id
        || identity.authority_scope
            != (ExecutionGrantAuthorityScope::Tenant {
                tenant_id: p.key.actor.tenant_id,
            })
        || identity.device_id != Some(target.device_id)
        || identity.device_workspace_id != source.device_workspace_id()
        || !source.permits(&grant.capability_entitlements)
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(())
}

/// A new finite authorization for exactly the already frozen Task business.
pub fn validate_work_item_device_reauthorization(
    source: &WorkItemDeviceExecutionPreparation,
    previous: &ExecutionGrant,
    successor: &ExecutionGrant,
) -> Result<(), DialogueStoreError> {
    validate_work_item_device_grant(source, previous)?;
    validate_work_item_device_grant(source, successor)?;
    let mut business = successor.clone();
    business.issued_at = previous.issued_at;
    business.expires_at = previous.expires_at;
    if business != *previous
        || successor.issued_at < previous.expires_at
        || successor.expires_at <= successor.issued_at
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkItemDeviceAdmissionCommit {
    pub source: WorkItemDeviceExecutionPreparation,
    pub admission: AdmissionCommit,
}
impl WorkItemDeviceAdmissionCommit {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.source.validate()?;
        WorkItemExecutionAdmissionCommit {
            preparation: self.source.preparation().clone(),
            admission: self.admission.clone(),
        }
        .validate()?;
        let b = &self.admission.batch;
        let device = self.source.target().device_id;
        validate_work_item_device_grant(&self.source, &b.runtime_snapshot.execution_grant)?;
        if b.run.context.control_home != (RuntimeHome::Device { device_id: device })
            || b.run.context.execution_home != (RuntimeHome::Device { device_id: device })
            || b.run.context.execution_scope
                != (ExecutionScope::DevicePrivate {
                    device_id: device,
                    workspace_id: self.source.device_workspace_id(),
                })
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

/// Exact durable local admission. Neither a preparation nor an HTTP success is
/// a substitute for this receipt committed alongside the original local Run.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceAdmissionReceipt {
    pub key: WorkItemExecutionKey,
    pub target: WorkItemDeviceTarget,
    pub run_id: RunId,
    pub conversation_id: ConversationId,
    pub source_digest: Sha256,
    pub snapshot_hash: Sha256,
    pub grant_proof_hash: Sha256,
}
impl WorkItemDeviceAdmissionReceipt {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.key.validate()?;
        self.target.validate()?;
        if self.target.consent_sha256.into_bytes() == [0; 32] {
            return Err(DialogueStoreError::Corrupt);
        }
        if self.key.report_from_run_id.is_some()
            || self.run_id != self.key.identities().run_id()?
            || self.conversation_id != self.key.identities().conversation_id
            || [
                self.source_digest,
                self.snapshot_hash,
                self.grant_proof_hash,
            ]
            .iter()
            .any(|h| h.into_bytes() == [0; 32])
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        self.validate()?;
        let mut b = if self.target.consent_anchor.is_some() {
            b"musteragent.work-item-device-admission.v2\0"
        } else {
            b"musteragent.work-item-device-admission.v1\0"
        }
        .to_vec();
        for id in [
            self.key.actor.tenant_id.into_uuid(),
            self.key.scope.owner_id.into_uuid(),
            self.key.scope.space_id.into_uuid(),
            self.key.scope.agent_id.into_uuid(),
            self.key.work_item_id.into_uuid(),
            self.key.executing_agent_id.into_uuid(),
            self.run_id.into_uuid(),
            self.conversation_id.into_uuid(),
            self.target.device_id.into_uuid(),
            self.target.installation_id.into_uuid(),
            self.target.membership_id,
            self.target.consent_id,
        ] {
            b.extend_from_slice(id.as_bytes());
        }
        for n in [
            self.key.goal_revision.get(),
            self.key.control_revision.get(),
            self.key.assignment_epoch.get(),
            self.target.membership_epoch,
        ] {
            b.extend_from_slice(&n.to_be_bytes());
        }
        for h in [
            self.target.consent_sha256,
            self.source_digest,
            self.snapshot_hash,
            self.grant_proof_hash,
        ] {
            b.extend_from_slice(&h.into_bytes());
        }
        if let Some(anchor) = &self.target.consent_anchor {
            b.extend_from_slice(&anchor.canonical_bytes()?);
        }
        Ok(b)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemDeviceRunOutcome {
    Succeeded,
    Failed,
    Cancelled,
}
/// A settled cut of original Run/Ledger/liveness facts. It says nothing about
/// business acceptance; the original WorkItem report/verify owners decide that.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceTerminalReceipt {
    pub admission: WorkItemDeviceAdmissionReceipt,
    pub outcome: WorkItemDeviceRunOutcome,
    pub last_run_sequence: u64,
    pub terminal_record_id: LedgerRecordId,
    pub terminal_payload_hash: Sha256,
    pub liveness_commitment: Sha256,
    pub publication_cut: WorkItemDevicePublicationCut,
    /// Present exactly for the verified Task result contract, including a failed attempt with no observed ACK.
    pub result_cut: Option<WorkItemDeviceResultCut>,
}
impl WorkItemDeviceTerminalReceipt {
    pub fn settlement(&self) -> crate::DeviceRunSettlement {
        crate::DeviceRunSettlement {
            outcome: self.outcome,
            last_run_sequence: self.last_run_sequence,
            terminal_record_id: self.terminal_record_id,
            terminal_payload_hash: self.terminal_payload_hash,
            liveness_commitment: self.liveness_commitment,
            publication_cut: self.publication_cut.clone(),
        }
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        self.admission.validate()?;
        crate::device_terminal::validate_settlement(
            self.last_run_sequence,
            self.terminal_payload_hash,
            self.liveness_commitment,
        )?;
        let mut b = if self.result_cut.is_some() {
            b"musteragent.work-item-device-terminal.v2\0".to_vec()
        } else {
            b"musteragent.work-item-device-terminal.v1\0".to_vec()
        };
        let admission = self.admission.canonical_bytes()?;
        b.extend_from_slice(&(admission.len() as u32).to_be_bytes());
        b.extend_from_slice(&admission);
        b.push(match self.outcome {
            WorkItemDeviceRunOutcome::Succeeded => 1,
            WorkItemDeviceRunOutcome::Failed => 2,
            WorkItemDeviceRunOutcome::Cancelled => 3,
        });
        b.extend_from_slice(&self.last_run_sequence.to_be_bytes());
        b.extend_from_slice(self.terminal_record_id.into_uuid().as_bytes());
        b.extend_from_slice(&self.terminal_payload_hash.into_bytes());
        b.extend_from_slice(&self.liveness_commitment.into_bytes());
        let cut = self.publication_cut.canonical_bytes()?;
        b.extend_from_slice(&(cut.len() as u32).to_be_bytes());
        b.extend_from_slice(&cut);
        if let Some(cut) = &self.result_cut {
            cut.validate_for(&self.admission, self.outcome)?;
            let encoded = cut.canonical_bytes()?;
            b.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            b.extend_from_slice(&encoded);
        }
        Ok(b)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
}

pub(super) struct Reader<'a>(pub(super) &'a [u8]);
impl<'a> Reader<'a> {
    pub(super) fn take<const N: usize>(&mut self) -> Result<[u8; N], DialogueStoreError> {
        let (a, b) = self
            .0
            .split_at_checked(N)
            .ok_or(DialogueStoreError::Corrupt)?;
        self.0 = b;
        a.try_into().map_err(|_| DialogueStoreError::Corrupt)
    }
    pub(super) fn uuid(&mut self) -> Result<uuid::Uuid, DialogueStoreError> {
        Ok(uuid::Uuid::from_bytes(self.take()?))
    }
    pub(super) fn id<T: std::str::FromStr>(&mut self) -> Result<T, DialogueStoreError> {
        self.uuid()?
            .to_string()
            .parse()
            .map_err(|_| DialogueStoreError::Corrupt)
    }
    pub(super) fn n(&mut self) -> Result<u64, DialogueStoreError> {
        Ok(u64::from_be_bytes(self.take()?))
    }
    pub(super) fn hash(&mut self) -> Result<Sha256, DialogueStoreError> {
        Sha256::from_bytes(self.take::<32>()?).map_err(|_| DialogueStoreError::Corrupt)
    }
    pub(super) fn frame(&mut self, limit: usize) -> Result<&'a [u8], DialogueStoreError> {
        let n = u32::from_be_bytes(self.take()?) as usize;
        if n > limit {
            return Err(DialogueStoreError::Corrupt);
        }
        let (a, b) = self
            .0
            .split_at_checked(n)
            .ok_or(DialogueStoreError::Corrupt)?;
        self.0 = b;
        Ok(a)
    }
}
impl WorkItemDeviceAdmissionReceipt {
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let (payload, anchored) = if let Some(body) =
            bytes.strip_prefix(b"musteragent.work-item-device-admission.v2\0")
        {
            (body, true)
        } else {
            (
                bytes
                    .strip_prefix(b"musteragent.work-item-device-admission.v1\0")
                    .ok_or(DialogueStoreError::Corrupt)?,
                false,
            )
        };
        let mut r = Reader(payload);
        let tenant = r.id()?;
        let owner = r.id()?;
        let space = r.id()?;
        let agent = r.id()?;
        let task = r.id()?;
        let executor = r.id()?;
        let run = r.id()?;
        let conversation = r.id()?;
        let device = r.id()?;
        let installation = r.id()?;
        let membership = r.uuid()?;
        let consent = r.uuid()?;
        let goal = WorkItemGoalRevision::new(r.n()?).map_err(|_| DialogueStoreError::Corrupt)?;
        let control =
            WorkItemControlRevision::new(r.n()?).map_err(|_| DialogueStoreError::Corrupt)?;
        let assignment = AssignmentEpoch::new(r.n()?).map_err(|_| DialogueStoreError::Corrupt)?;
        let epoch = r.n()?;
        let mut value = Self {
            key: WorkItemExecutionKey {
                actor: Actor {
                    tenant_id: tenant,
                    principal_id: owner,
                    principal_kind: PrincipalKind::User,
                },
                scope: OwnedAgentWorkItemScope {
                    owner_id: owner,
                    space_id: space,
                    agent_id: agent,
                },
                work_item_id: task,
                goal_revision: goal,
                control_revision: control,
                assignment_epoch: assignment,
                report_from_run_id: None,
                executing_agent_id: executor,
            },
            target: WorkItemDeviceTarget {
                device_id: device,
                installation_id: installation,
                membership_id: membership,
                membership_epoch: epoch,
                consent_id: consent,
                consent_sha256: r.hash()?,
                consent_anchor: None,
            },
            run_id: run,
            conversation_id: conversation,
            source_digest: r.hash()?,
            snapshot_hash: r.hash()?,
            grant_proof_hash: r.hash()?,
        };
        if anchored {
            value.target.consent_anchor = Some(WorkItemDeviceConsentAnchor::read(&mut r)?);
        }
        if !r.0.is_empty() || value.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(value)
    }
}
impl WorkItemDeviceTerminalReceipt {
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > 4 * 1024 * 1024 + 4096 {
            return Err(DialogueStoreError::Corrupt);
        }
        let (body, verified) =
            if let Some(body) = bytes.strip_prefix(b"musteragent.work-item-device-terminal.v2\0") {
                (body, true)
            } else {
                (
                    bytes
                        .strip_prefix(b"musteragent.work-item-device-terminal.v1\0")
                        .ok_or(DialogueStoreError::Corrupt)?,
                    false,
                )
            };
        let mut r = Reader(body);
        let admission = WorkItemDeviceAdmissionReceipt::decode(r.frame(2048)?)?;
        let outcome = match r.take::<1>()?[0] {
            1 => WorkItemDeviceRunOutcome::Succeeded,
            2 => WorkItemDeviceRunOutcome::Failed,
            3 => WorkItemDeviceRunOutcome::Cancelled,
            _ => return Err(DialogueStoreError::Corrupt),
        };
        let value = Self {
            admission,
            outcome,
            last_run_sequence: r.n()?,
            terminal_record_id: r.id()?,
            terminal_payload_hash: r.hash()?,
            liveness_commitment: r.hash()?,
            publication_cut: WorkItemDevicePublicationCut::decode(r.frame(4 * 1024 * 1024)?)?,
            result_cut: if verified {
                Some(WorkItemDeviceResultCut::decode(r.frame(2048)?)?)
            } else {
                None
            },
        };
        if !r.0.is_empty() || value.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(value)
    }
}

impl WorkItemExecutionRequest {
    pub fn device(
        actor: Actor,
        work_item_id: WorkItemId,
        goal: u64,
        control: u64,
        assignment: u64,
    ) -> Result<Self, DialogueStoreError> {
        if actor.principal_kind != PrincipalKind::User {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(Self {
            actor,
            work_item_id,
            goal_revision: WorkItemGoalRevision::new(goal)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            control_revision: WorkItemControlRevision::new(control)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            assignment_epoch: AssignmentEpoch::new(assignment)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            report_from_run_id: None,
        })
    }
}

impl WorkItemExecutionClaim {
    pub fn canonical_device_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        if self.request.actor.principal_kind != PrincipalKind::User
            || self.request.report_from_run_id.is_some()
            || self.owner.is_nil()
            || [self.event_seq, self.generation, self.attempt_count]
                .iter()
                .any(|n| *n == 0 || *n > i64::MAX as u64)
            || self.lease_expires_at.unix_millis() <= 0
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut b = b"musteragent.work-item-device-claim.v1\0".to_vec();
        for id in [
            self.request.actor.tenant_id.into_uuid(),
            self.request.actor.principal_id.into_uuid(),
            self.request.work_item_id.into_uuid(),
            self.owner,
        ] {
            b.extend_from_slice(id.as_bytes());
        }
        for n in [
            self.request.goal_revision.get(),
            self.request.control_revision.get(),
            self.request.assignment_epoch.get(),
            self.event_seq,
            self.generation,
            self.attempt_count,
        ] {
            b.extend_from_slice(&n.to_be_bytes());
        }
        b.extend_from_slice(&self.lease_expires_at.unix_millis().to_be_bytes());
        Ok(b)
    }
    pub fn decode_device(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let mut r = Reader(
            bytes
                .strip_prefix(b"musteragent.work-item-device-claim.v1\0")
                .ok_or(DialogueStoreError::Corrupt)?,
        );
        let actor = Actor {
            tenant_id: r.id()?,
            principal_id: r.id()?,
            principal_kind: PrincipalKind::User,
        };
        let task = r.id()?;
        let owner = r.uuid()?;
        let v = Self {
            request: WorkItemExecutionRequest::device(actor, task, r.n()?, r.n()?, r.n()?)?,
            owner,
            event_seq: r.n()?,
            generation: r.n()?,
            attempt_count: r.n()?,
            lease_expires_at: UtcTimestamp::from_unix_millis(i64::from_be_bytes(r.take()?)),
        };
        if !r.0.is_empty() || v.canonical_device_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(v)
    }
}

#[cfg(test)]
mod device_receipt_tests {
    use super::*;
    fn receipt() -> WorkItemDeviceAdmissionReceipt {
        let owner = PrincipalId::generate();
        let key = WorkItemExecutionKey {
            actor: Actor {
                tenant_id: TenantId::generate(),
                principal_id: owner,
                principal_kind: PrincipalKind::User,
            },
            scope: OwnedAgentWorkItemScope {
                owner_id: owner,
                space_id: OwnerSpaceId::generate(),
                agent_id: AgentId::generate(),
            },
            work_item_id: WorkItemId::generate(),
            goal_revision: WorkItemGoalRevision::new(2).expect("valid test fixture"),
            control_revision: WorkItemControlRevision::new(3).expect("valid test fixture"),
            assignment_epoch: AssignmentEpoch::new(4).expect("valid test fixture"),
            report_from_run_id: None,
            executing_agent_id: AgentId::generate(),
        };
        WorkItemDeviceAdmissionReceipt {
            key,
            target: WorkItemDeviceTarget {
                device_id: DeviceId::generate(),
                installation_id: DeviceInstallationId::generate(),
                membership_id: uuid::Uuid::from_u128(1),
                membership_epoch: 5,
                consent_id: uuid::Uuid::from_u128(2),
                consent_sha256: Sha256::digest(b"consent"),
                consent_anchor: None,
            },
            run_id: key.identities().run_id().expect("valid test fixture"),
            conversation_id: key.identities().conversation_id,
            source_digest: Sha256::digest(b"source"),
            snapshot_hash: Sha256::digest(b"snapshot"),
            grant_proof_hash: Sha256::digest(b"proof"),
        }
    }
    #[test]
    fn exact_consent_anchor_is_bound_to_admission_and_not_admitted_in_the_original_formats() {
        let mut a = receipt();
        let legacy = a.canonical_bytes().expect("valid test fixture");
        let legacy_body = legacy
            .strip_prefix(b"musteragent.work-item-device-admission.v1\0")
            .expect("valid test fixture");
        assert_eq!(legacy_body.len(), 12 * 16 + 4 * 8 + 4 * 32);
        let anchor = WorkItemDeviceConsentAnchor {
            authority_ref: "auth-2".to_owned(),
            policy_ref: "policy-2".to_owned(),
            effective_capabilities_sha256: Sha256::digest(b"effective approved set"),
        };
        let mut suffix = b"\0\0\0\x06auth-2\0\0\0\x08policy-2".to_vec();
        suffix.extend(anchor.effective_capabilities_sha256.into_bytes());
        a.target.consent_anchor = Some(anchor.clone());
        let anchored = a.canonical_bytes().expect("valid test fixture");
        let expected = [
            b"musteragent.work-item-device-admission.v2\0".as_slice(),
            legacy_body,
            &suffix,
        ]
        .concat();
        assert_eq!(anchored, expected);
        assert_eq!(
            WorkItemDeviceAdmissionReceipt::decode(&anchored).expect("valid test fixture"),
            a
        );
        assert!(WorkItemDeviceAdmissionReceipt::decode(&[&legacy[..], &suffix].concat()).is_err());
        assert!(WorkItemDeviceAdmissionReceipt::decode(&anchored[..anchored.len() - 1]).is_err());
        assert_ne!(
            Sha256::digest(&legacy),
            a.commitment().expect("valid test fixture")
        );

        let mut c = WorkItemDeviceControl {
            key: a.key,
            target: a.target.clone(),
            run_id: a.run_id,
            conversation_id: a.conversation_id,
            source_digest: a.source_digest,
            grant_proof_hash: a.grant_proof_hash,
            current_control_revision: WorkItemControlRevision::new(4).expect("valid test fixture"),
            reason: WorkItemRunControlReason::TaskCommand {
                command_id: CommandId::generate(),
            },
        };
        assert!(c.matches_admission(&a));
        let sealed = WorkItemDeviceNotAdmittedReceipt { control: c.clone() };
        let encoded = sealed.canonical_bytes().expect("valid test fixture");
        assert!(encoded.starts_with(
            b"musteragent.work-item-device-not-admitted.v1\0musteragent.work-item-device-control.v2\0"
        ));
        assert!(encoded.ends_with(&suffix));
        assert_eq!(
            WorkItemDeviceNotAdmittedReceipt::decode(&encoded).expect("valid test fixture"),
            sealed
        );
        c.target
            .consent_anchor
            .as_mut()
            .expect("valid test fixture")
            .policy_ref
            .push('x');
        assert!(!c.matches_admission(&a));
        assert_ne!(
            c.commitment().expect("valid test fixture"),
            sealed.control.commitment().expect("valid test fixture")
        );
        c.target.consent_anchor = None;
        assert!(!c.matches_admission(&a));
        assert!(
            c.canonical_bytes()
                .expect("valid test fixture")
                .starts_with(b"musteragent.work-item-device-control.v1\0")
        );
        a.target.consent_anchor = None;
        assert!(c.matches_admission(&a));
        assert_eq!(legacy, a.canonical_bytes().expect("valid test fixture"));
    }

    #[test]
    fn consent_checkpoint_rejects_invalid_hash_and_noncanonical_references() {
        let mut anchor = WorkItemDeviceConsentAnchor {
            authority_ref: "a".repeat(160),
            policy_ref: "!~".repeat(80),
            effective_capabilities_sha256: Sha256::digest(b"retained"),
        };
        assert!(anchor.validate().is_ok());
        for invalid in ["", "two refs", "line\n", "策略", &"x".repeat(161)] {
            let mut bad = anchor.clone();
            bad.authority_ref = invalid.into();
            assert!(bad.validate().is_err());
            bad = anchor.clone();
            bad.policy_ref = invalid.into();
            assert!(bad.validate().is_err());
        }
        anchor.effective_capabilities_sha256 =
            Sha256::from_bytes([0; 32]).expect("valid test fixture");
        assert!(anchor.validate().is_err());
    }

    #[test]
    fn exact_receipt_roundtrip_rejects_trailing_or_truncated_bytes() {
        let a = receipt();
        let b = a.canonical_bytes().expect("valid test fixture");
        assert_eq!(
            WorkItemDeviceAdmissionReceipt::decode(&b).expect("valid test fixture"),
            a
        );
        for end in [0, 1, b.len() - 1] {
            assert!(WorkItemDeviceAdmissionReceipt::decode(&b[..end]).is_err());
        }
        let mut more = b;
        more.push(0);
        assert!(WorkItemDeviceAdmissionReceipt::decode(&more).is_err());
    }
    #[test]
    fn terminal_commits_actual_ledger_liveness_and_publication_cut() {
        let r = WorkItemDeviceTerminalReceipt {
            admission: receipt(),
            outcome: WorkItemDeviceRunOutcome::Cancelled,
            last_run_sequence: 7,
            terminal_record_id: LedgerRecordId::generate(),
            terminal_payload_hash: Sha256::digest(b"ledger"),
            liveness_commitment: Sha256::digest(b"closed"),
            publication_cut: WorkItemDevicePublicationCut::default(),
            result_cut: None,
        };
        let b = r.canonical_bytes().expect("valid test fixture");
        // Frozen v1 layout predates shared settlement validation. Preserve it
        // exactly; no new completion witness is inserted into Task receipts.
        let admission = r.admission.canonical_bytes().expect("valid test fixture");
        let cut = r
            .publication_cut
            .canonical_bytes()
            .expect("valid test fixture");
        let mut original = b"musteragent.work-item-device-terminal.v1\0".to_vec();
        original.extend((admission.len() as u32).to_be_bytes());
        original.extend(admission);
        original.push(3);
        original.extend(7u64.to_be_bytes());
        original.extend(r.terminal_record_id.into_uuid().as_bytes());
        original.extend(r.terminal_payload_hash.into_bytes());
        original.extend(r.liveness_commitment.into_bytes());
        original.extend((cut.len() as u32).to_be_bytes());
        original.extend(cut);
        assert_eq!(b, original);
        assert_eq!(r.settlement().validate(), Ok(()));
        assert_eq!(
            WorkItemDeviceTerminalReceipt::decode(&b).expect("valid test fixture"),
            r
        );
        let mut other = r.clone();
        other.terminal_record_id = LedgerRecordId::generate();
        assert_ne!(
            r.commitment().expect("valid test fixture"),
            other.commitment().expect("valid test fixture")
        );
        other = r.clone();
        other.liveness_commitment = Sha256::digest(b"unconfirmed");
        assert_ne!(
            r.commitment().expect("valid test fixture"),
            other.commitment().expect("valid test fixture")
        );
    }
    #[test]
    fn private_terminal_keeps_legacy_bytes_and_rejects_partial_success_or_wrong_submission() {
        let admission = receipt();
        let invocation = InvocationId::generate();
        let result_id = work_item_result_id(invocation);
        let confirmed = WorkItemDeviceSubmittedResult {
            invocation_id: invocation,
            command_commitment: Sha256::digest(b"command"),
            receipt_commitment: Sha256::digest(b"receipt"),
            submission: WorkItemResultSubmissionReceipt {
                result_id,
                work_item_id: admission.key.work_item_id,
                producing_run_id: admission.run_id,
                manifest_sha256: Sha256::digest(b"manifest"),
                item_ids: vec![work_item_result_item_id(result_id, 0)],
            },
        };
        let mut terminal = WorkItemDeviceTerminalReceipt {
            admission,
            outcome: WorkItemDeviceRunOutcome::Succeeded,
            last_run_sequence: 8,
            terminal_record_id: LedgerRecordId::generate(),
            terminal_payload_hash: Sha256::digest(b"terminal"),
            liveness_commitment: Sha256::digest(b"settled"),
            publication_cut: WorkItemDevicePublicationCut::default(),
            result_cut: Some(WorkItemDeviceResultCut::default()),
        };
        assert!(terminal.canonical_bytes().is_err());
        terminal.outcome = WorkItemDeviceRunOutcome::Failed;
        assert!(terminal.canonical_bytes().is_ok());
        terminal.outcome = WorkItemDeviceRunOutcome::Succeeded;
        terminal.result_cut = Some(WorkItemDeviceResultCut {
            confirmed: Some(confirmed.clone()),
        });
        let encoded = terminal.canonical_bytes().expect("valid test fixture");
        assert!(encoded.starts_with(b"musteragent.work-item-device-terminal.v2\0"));
        assert_eq!(
            WorkItemDeviceTerminalReceipt::decode(&encoded).expect("valid test fixture"),
            terminal
        );
        let mut downgraded = encoded.clone();
        downgraded[b"musteragent.work-item-device-terminal.v".len()] = b'1';
        assert!(WorkItemDeviceTerminalReceipt::decode(&downgraded).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(WorkItemDeviceTerminalReceipt::decode(&trailing).is_err());
        let mut foreign = confirmed;
        foreign.submission.producing_run_id = RunId::generate();
        terminal.result_cut = Some(WorkItemDeviceResultCut {
            confirmed: Some(foreign),
        });
        assert!(terminal.canonical_bytes().is_err());
    }
    #[test]
    fn no_admission_fact_commits_exact_control_and_source() {
        let a = receipt();
        let c = WorkItemDeviceControl {
            key: a.key,
            target: a.target.clone(),
            run_id: a.run_id,
            conversation_id: a.conversation_id,
            source_digest: a.source_digest,
            grant_proof_hash: a.grant_proof_hash,
            current_control_revision: WorkItemControlRevision::new(4).expect("valid test fixture"),
            reason: WorkItemRunControlReason::TaskCommand {
                command_id: CommandId::generate(),
            },
        };
        assert!(c.matches_admission(&a));
        let r = WorkItemDeviceNotAdmittedReceipt { control: c };
        assert_eq!(
            WorkItemDeviceNotAdmittedReceipt::decode(
                &r.canonical_bytes().expect("valid test fixture")
            )
            .expect("valid test fixture"),
            r
        );
        let mut other = r.clone();
        other.control.grant_proof_hash = Sha256::digest(b"other");
        assert!(!other.control.matches_admission(&a));
        assert_ne!(
            r.commitment().expect("valid test fixture"),
            other.commitment().expect("valid test fixture")
        );
    }
    #[test]
    fn delivery_claim_recovery_does_not_renew_lease() {
        let a = receipt();
        let c = WorkItemExecutionClaim {
            request: a.key.request(),
            event_seq: 1,
            owner: uuid::Uuid::from_u128(3),
            generation: 2,
            attempt_count: 3,
            lease_expires_at: UtcTimestamp::from_unix_millis(42),
        };
        assert_eq!(
            WorkItemExecutionClaim::decode_device(
                &c.canonical_device_bytes().expect("valid test fixture")
            )
            .expect("valid test fixture"),
            c
        );
        let mut other = c;
        other.lease_expires_at = UtcTimestamp::from_unix_millis(43);
        assert_ne!(
            c.canonical_device_bytes().expect("valid test fixture"),
            other.canonical_device_bytes().expect("valid test fixture")
        );
    }
}
