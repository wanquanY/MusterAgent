//! Dialogue remote delegation adapter for the shared assignment/claim contract.
//!
//! These relationship-checked facts are NOT a grant, current lease or execution
//! receipt. Admission must obtain them from the original Dialogue, account,
//! native membership, device registration and consent owners. Nothing here
//! allocates a Run, chooses a device, starts a Worker or authorizes an effect.
//!
//! Local Desktop execution uses the existing DevicePrivate admission and native
//! Loop. It must not require this delegation binding, an Executor registration,
//! or remote-execution consent. This adapter only describes delegated execution
//! by an independently qualified Executor; it does not enable Agent scopes
//! on the currently Hosted-only Executor and consent owners.
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    AgentDialogueId, AgentId, AttemptId, DeviceInstallationId, ExecutorConsentId, LoginSessionId,
    PrincipalId, PrincipalKind, TenantId, UserIntentId,
};
use musteragent_runtime_contract::{
    execution_assignment::{
        ExecutionAllocationBinding, ExecutionAssignmentAnchor, ExecutionAttemptIdentity,
        ExecutionBindingError, ExecutorBinding,
    },
    execution_claim::ExecutionLeaseRequest,
};

use crate::{DialogueExecutionFactBinding, DialogueInputOrigin, DialogueInputSurface};

/// Product ownership never impersonates a Hosted Project or Workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceDelegationSubject {
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub intent_id: UserIntentId,
}

/// Pins for current authority observations. Positive counters are not proof of
/// authorization: signature, membership/login activity, consent and freshness
/// must still be independently checked at admission and every effect gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceDelegationQualification {
    pub account_auth_epoch: u64,
    pub applications_revision: u64,
    pub executor_device_revision: u64,
    pub consent_id: ExecutorConsentId,
    /// Exact consent facts for this Agent scope. A Hosted Project consent
    /// is not interchangeable; the original consent owner must verify the scope.
    pub consent_digest: [u8; 32],
    pub target_login_session_id: LoginSessionId,
    /// Digest of the exact original Native membership subject/authority.
    /// This is a reference, never a copied credential or a new membership.
    pub target_membership_digest: [u8; 32],
}
impl DeviceDelegationQualification {
    fn validate(self) -> Result<(), DeviceDelegationError> {
        if [
            self.account_auth_epoch,
            self.applications_revision,
            self.executor_device_revision,
        ]
        .iter()
        .any(|v| !(1..=i64::MAX as u64).contains(v))
            || self.target_membership_digest == [0; 32]
            || self.consent_digest == [0; 32]
        {
            return Err(DeviceDelegationError::Qualification);
        }
        Ok(())
    }
}

/// Frozen facts for delegation to an independently qualified Executor.
/// Input location is provenance, never a selection of the local execution path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceDelegationFacts {
    /// Exact existing admission facts including material/configuration digest.
    pub source: DialogueExecutionFactBinding,
    pub origin: DialogueInputOrigin,
    pub origin_login_session_id: LoginSessionId,
    /// Client installation mapped to the Executor by verified Native membership.
    pub target_installation_id: DeviceInstallationId,
    pub attempt: ExecutionAttemptIdentity,
    pub executor: ExecutorBinding,
    pub qualification: DeviceDelegationQualification,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceDelegationBinding {
    facts: DeviceDelegationFacts,
    digest: Sha256,
}
impl DeviceDelegationBinding {
    pub fn from_facts(facts: DeviceDelegationFacts) -> Result<Self, DeviceDelegationError> {
        let source = &facts.source;
        if source.actor.principal_kind != PrincipalKind::User
            || source.run_id != facts.attempt.run_id
            || source.context_watermark.validate().is_err()
            || source.context_watermark.seq == 0
            || [
                source.manifest_revision,
                source.instructions_revision,
                source.permission_revision,
            ]
            .iter()
            .any(|v| !(1..=i64::MAX as u64).contains(v))
            || source.source_sha256.into_bytes() == [0; 32]
        {
            return Err(DeviceDelegationError::Source);
        }
        facts
            .origin
            .validate()
            .map_err(|_| DeviceDelegationError::Origin)?;
        if facts.executor.owner_principal_id != source.actor.principal_id {
            return Err(DeviceDelegationError::ExecutorOwner);
        }
        if facts.executor.environment_spec_digest == [0; 32] {
            return Err(DeviceDelegationError::Environment);
        }
        facts.qualification.validate()?;
        let digest = Sha256::digest(canonical_bytes(&facts));
        Ok(Self { facts, digest })
    }

    pub fn facts(&self) -> &DeviceDelegationFacts {
        &self.facts
    }
    pub fn digest(&self) -> Sha256 {
        self.digest
    }
    /// New Dialogue v1 encoding. It cannot change existing Hosted signed bytes.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_bytes(&self.facts)
    }
    pub fn subject(&self) -> DeviceDelegationSubject {
        let source = &self.facts.source;
        DeviceDelegationSubject {
            tenant_id: source.actor.tenant_id,
            owner_id: source.actor.principal_id,
            agent_id: source.agent_id,
            dialogue_id: source.dialogue_id,
            intent_id: source.intent_id,
        }
    }
    fn anchor(&self) -> ExecutionAssignmentAnchor<DeviceDelegationSubject> {
        ExecutionAssignmentAnchor {
            scope: self.subject(),
            attempt: self.facts.attempt,
            executor: self.facts.executor,
        }
    }

    /// Called with fresh independently verified owner facts, never a client echo
    /// or stored receipt. Counter changes invalidate this exact allocation.
    pub fn check_current(&self, current: &Self) -> Result<(), DeviceDelegationError> {
        self.anchor().check_current(&current.anchor())?;
        if self.facts != current.facts {
            return Err(DeviceDelegationError::CurrentFactsChanged);
        }
        Ok(())
    }

    /// Recovery may requalify a new device instance, but cannot swap business
    /// inputs or silently change the required environment. This authorizes no
    /// recovery and does not make an effect_unknown result safe to retry.
    pub fn check_successor(&self, previous: &Self) -> Result<(), DeviceDelegationError> {
        if self.facts.source != previous.facts.source
            || self.facts.origin != previous.facts.origin
            || self.facts.origin_login_session_id != previous.facts.origin_login_session_id
            || self.facts.executor.environment_spec_digest
                != previous.facts.executor.environment_spec_digest
        {
            return Err(DeviceDelegationError::RecoveryInputChanged);
        }
        self.facts.attempt.check_successor(previous.facts.attempt)?;
        Ok(())
    }

    pub fn check_lease_request(
        &self,
        request: &ExecutionLeaseRequest<DeviceDelegationSubject>,
    ) -> Result<(), DeviceDelegationError> {
        request
            .validate()
            .map_err(|_| DeviceDelegationError::LeaseBinding)?;
        if request.scope != self.subject()
            || request.run_id != self.facts.source.run_id
            || request.assignment_digest != self.digest.into_bytes()
        {
            return Err(DeviceDelegationError::LeaseBinding);
        }
        Ok(())
    }
}
impl ExecutionAllocationBinding for DeviceDelegationBinding {
    fn assignment_digest(&self) -> [u8; 32] {
        self.digest.into_bytes()
    }
    fn initial_attempt_id(&self) -> AttemptId {
        self.facts.attempt.attempt_id
    }
}

fn canonical_bytes(facts: &DeviceDelegationFacts) -> Vec<u8> {
    // Fixed UUID/u64/hash fields use their explicit byte widths. The only
    // variable field (Channel provider) is length-framed. No serde enum order.
    let source = &facts.source;
    let mut out = b"musteragent.device-delegation.v1\0".to_vec();
    for id in [
        source.actor.tenant_id.into_uuid(),
        source.actor.principal_id.into_uuid(),
        source.agent_id.into_uuid(),
        source.agent_version_id.into_uuid(),
        source.dialogue_id.into_uuid(),
        source.intent_id.into_uuid(),
        source.context_conversation_id.into_uuid(),
        source.command_id.into_uuid(),
        source.run_id.into_uuid(),
        facts.origin_login_session_id.into_uuid(),
        facts.target_installation_id.into_uuid(),
        facts.attempt.assignment_id.into_uuid(),
        facts.attempt.attempt_id.into_uuid(),
        facts.executor.device_id.into_uuid(),
        facts.executor.owner_principal_id.into_uuid(),
        facts.executor.instance_id.into_uuid(),
        facts.qualification.target_login_session_id.into_uuid(),
        facts.qualification.consent_id.into_uuid(),
    ] {
        out.extend_from_slice(id.as_bytes());
    }
    for number in [
        source.context_watermark.epoch,
        source.context_watermark.seq,
        source.manifest_revision,
        source.instructions_revision,
        source.permission_revision,
        facts.attempt.run_attempt_epoch.get(),
        facts.executor.runtime_generation.get(),
        facts.executor.registration_revision.get(),
        facts.qualification.account_auth_epoch,
        facts.qualification.applications_revision,
        facts.qualification.executor_device_revision,
    ] {
        out.extend_from_slice(&number.to_be_bytes());
    }
    out.extend_from_slice(&source.permission_mode.to_be_bytes());
    out.push(u8::from(source.default_agent_direct_intent));
    out.extend_from_slice(&source.source_sha256.into_bytes());
    out.extend_from_slice(&facts.executor.environment_spec_digest);
    out.extend_from_slice(&facts.qualification.target_membership_digest);
    out.extend_from_slice(&facts.qualification.consent_digest);
    match &facts.origin.surface {
        DialogueInputSurface::Desktop => out.push(1),
        DialogueInputSurface::Web => out.push(2),
        DialogueInputSurface::Mobile => out.push(3),
        DialogueInputSurface::Schedule => out.push(5),
        DialogueInputSurface::Capability => out.push(6),
    }
    match facts.origin.installation_id {
        Some(id) => {
            out.push(1);
            out.extend_from_slice(id.into_uuid().as_bytes());
        }
        None => out.push(0),
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeviceDelegationError {
    #[error("Dialogue execution source facts do not match the accepted intent")]
    Source,
    #[error("Dialogue delegation input origin is invalid")]
    Origin,
    #[error("Dialogue executor belongs to another account owner")]
    ExecutorOwner,
    #[error("Dialogue execution environment is missing")]
    Environment,
    #[error("Dialogue device delegation qualification is incomplete")]
    Qualification,
    #[error("Dialogue execution inputs or authority pins changed")]
    CurrentFactsChanged,
    #[error("Dialogue recovery must preserve original business inputs and environment")]
    RecoveryInputChanged,
    #[error("Dialogue lease request belongs to another execution binding")]
    LeaseBinding,
    #[error(transparent)]
    Assignment(#[from] ExecutionBindingError),
}
