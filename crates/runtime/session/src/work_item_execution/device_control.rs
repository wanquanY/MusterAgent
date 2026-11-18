//! Original WorkItem control targeting retained Device custody. No snapshot is
//! assumed when the admission ACK may have been lost.
use super::device_admission::Reader;
use super::*;
use musteragent_content_ref::Sha256;
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceControl {
    pub key: WorkItemExecutionKey,
    pub target: WorkItemDeviceTarget,
    pub run_id: RunId,
    pub conversation_id: ConversationId,
    pub source_digest: Sha256,
    pub grant_proof_hash: Sha256,
    pub current_control_revision: WorkItemControlRevision,
    pub reason: WorkItemRunControlReason,
}
impl WorkItemDeviceControl {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.key.validate()?;
        self.target.validate()?;
        if self.target.consent_sha256.into_bytes() == [0; 32] {
            return Err(DialogueStoreError::Corrupt);
        }
        if self.key.report_from_run_id.is_some()
            || self.run_id != self.key.identities().run_id()?
            || self.conversation_id != self.key.identities().conversation_id
            || self.current_control_revision < self.key.control_revision
            || [self.source_digest, self.grant_proof_hash]
                .iter()
                .any(|h| h.into_bytes() == [0; 32])
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
    pub fn matches_admission(&self, a: &WorkItemDeviceAdmissionReceipt) -> bool {
        self.key == a.key
            && self.target == a.target
            && self.run_id == a.run_id
            && self.conversation_id == a.conversation_id
            && self.source_digest == a.source_digest
            && self.grant_proof_hash == a.grant_proof_hash
    }
    pub fn fence(&self) -> WorkItemRunControlFence {
        WorkItemRunControlFence {
            actor: self.key.actor,
            task_id: self.key.work_item_id,
            control_revision: self.current_control_revision,
            run_id: self.run_id,
            reason: self.reason,
        }
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        self.validate()?;
        let mut b = if self.target.consent_anchor.is_some() {
            b"musteragent.work-item-device-control.v2\0"
        } else {
            b"musteragent.work-item-device-control.v1\0"
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
            self.current_control_revision.get(),
        ] {
            b.extend_from_slice(&n.to_be_bytes());
        }
        for h in [
            self.target.consent_sha256,
            self.source_digest,
            self.grant_proof_hash,
        ] {
            b.extend_from_slice(&h.into_bytes());
        }
        let (kind, id) = match self.reason {
            WorkItemRunControlReason::TaskCommand { command_id } => (1, command_id.into_uuid()),
            WorkItemRunControlReason::PrivacyErasure { job_id } => (2, job_id.into_uuid()),
        };
        b.push(kind);
        b.extend_from_slice(id.as_bytes());
        if let Some(anchor) = &self.target.consent_anchor {
            b.extend_from_slice(&anchor.canonical_bytes()?);
        }
        Ok(b)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let (payload, anchored) =
            if let Some(body) = bytes.strip_prefix(b"musteragent.work-item-device-control.v2\0") {
                (body, true)
            } else {
                (
                    bytes
                        .strip_prefix(b"musteragent.work-item-device-control.v1\0")
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
        let run_id = r.id()?;
        let conversation_id = r.id()?;
        let device_id = r.id()?;
        let installation_id = r.id()?;
        let membership_id = r.uuid()?;
        let consent_id = r.uuid()?;
        let goal_revision =
            WorkItemGoalRevision::new(r.n()?).map_err(|_| DialogueStoreError::Corrupt)?;
        let control_revision =
            WorkItemControlRevision::new(r.n()?).map_err(|_| DialogueStoreError::Corrupt)?;
        let assignment_epoch =
            AssignmentEpoch::new(r.n()?).map_err(|_| DialogueStoreError::Corrupt)?;
        let membership_epoch = r.n()?;
        let current_control_revision =
            WorkItemControlRevision::new(r.n()?).map_err(|_| DialogueStoreError::Corrupt)?;
        let target = WorkItemDeviceTarget {
            device_id,
            installation_id,
            membership_id,
            membership_epoch,
            consent_id,
            consent_sha256: r.hash()?,
            consent_anchor: None,
        };
        let source_digest = r.hash()?;
        let grant_proof_hash = r.hash()?;
        let reason = match r.take::<1>()?[0] {
            1 => WorkItemRunControlReason::TaskCommand {
                command_id: r.id()?,
            },
            2 => WorkItemRunControlReason::PrivacyErasure { job_id: r.id()? },
            _ => return Err(DialogueStoreError::Corrupt),
        };
        let mut v = Self {
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
                goal_revision,
                control_revision,
                assignment_epoch,
                report_from_run_id: None,
                executing_agent_id: executor,
            },
            target,
            run_id,
            conversation_id,
            source_digest,
            grant_proof_hash,
            current_control_revision,
            reason,
        };
        if anchored {
            v.target.consent_anchor = Some(WorkItemDeviceConsentAnchor::read(&mut r)?);
        }
        if !r.0.is_empty() || v.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(v)
    }
}
/// Only the local writer can issue this after storing an admission fence and
/// proving the exact source has no original local admission in that transaction.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceNotAdmittedReceipt {
    pub control: WorkItemDeviceControl,
}
impl WorkItemDeviceNotAdmittedReceipt {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        let mut b = b"musteragent.work-item-device-not-admitted.v1\0".to_vec();
        b.extend_from_slice(&self.control.canonical_bytes()?);
        Ok(b)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let v = Self {
            control: WorkItemDeviceControl::decode(
                bytes
                    .strip_prefix(b"musteragent.work-item-device-not-admitted.v1\0")
                    .ok_or(DialogueStoreError::Corrupt)?,
            )?,
        };
        if v.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(v)
    }
}

impl WorkItemDeviceControl {
    pub fn cancel_command(
        &self,
        home_epoch: HomeEpoch,
        now: UtcTimestamp,
    ) -> Result<
        musteragent_runtime_contract::CommandEnvelope<musteragent_run::CancelRun>,
        DialogueStoreError,
    > {
        use musteragent_runtime_contract::*;
        self.validate()?;
        let mut seed = b"musteragent.work-item-device-cancel.v1\0".to_vec();
        seed.extend_from_slice(&self.commitment()?.into_bytes());
        seed.extend_from_slice(&home_epoch.get().to_be_bytes());
        let id = uuid::Uuid::new_v5(&self.run_id.into_uuid(), &seed);
        let span = id.simple().to_string();
        let value = CommandEnvelope {
            metadata: CommandMetadata {
                schema_version: DottedKey::new(COMMAND_SCHEMA_VERSION)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                command_id: CommandId::from_uuid(id).map_err(|_| DialogueStoreError::Corrupt)?,
                actor: self.key.actor,
                idempotency_key: IdempotencyKey::new(format!("task-device-cancel:{id}"))
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                issued_at: now,
                deadline: now
                    .checked_add_millis(60_000)
                    .ok_or(DialogueStoreError::Corrupt)?,
                correlation_id: CorrelationId::from_uuid(id)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                causation_id: None,
                conversation_guard: Some(ConversationGuard {
                    conversation_id: self.conversation_id,
                    expected_home_epoch: home_epoch,
                    expected_conversation_seq: None,
                }),
                trace_context: TraceContext {
                    traceparent: TraceParent::new(format!("00-{span}-{}-01", &span[..16]))
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    tracestate: None,
                },
            },
            command_type: CommandType::CancelRun,
            payload: musteragent_run::CancelRun {
                run_id: self.run_id,
                reason: CancelReason::UserRequested,
            },
        };
        value.validate().map_err(|_| DialogueStoreError::Corrupt)?;
        Ok(value)
    }
}
