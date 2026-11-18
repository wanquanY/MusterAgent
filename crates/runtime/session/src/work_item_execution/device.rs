//! Device is an explicit execution destination of an existing WorkItem.
//! The source Cloud delegation and the separately approved Device ceiling are
//! retained independently. A selected membership is never an authorization.
use super::*;
mod consent_anchor;
pub use consent_anchor::WorkItemDeviceConsentAnchor;
use musteragent_capability::CapabilityEntitlement;
use musteragent_content_ref::Sha256;

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub enum WorkItemExecutionTarget {
    #[default]
    Cloud,
    RemoteDevice(WorkItemDeviceTarget),
}
impl WorkItemExecutionTarget {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        match self {
            Self::Cloud => Ok(()),
            Self::RemoteDevice(target) => target.validate(),
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceTarget {
    pub device_id: DeviceId,
    pub installation_id: DeviceInstallationId,
    pub membership_id: uuid::Uuid,
    pub membership_epoch: u64,
    pub consent_id: uuid::Uuid,
    pub consent_sha256: Sha256,
    pub consent_anchor: Option<WorkItemDeviceConsentAnchor>,
}
impl WorkItemDeviceTarget {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if let Some(anchor) = &self.consent_anchor {
            anchor.validate()?;
        }
        if self.membership_id.is_nil()
            || self.membership_epoch == 0
            || self.membership_epoch > i64::MAX as u64
            || self.consent_id.is_nil()
            || self.consent_sha256.into_bytes() == [0; 32]
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
/// Shared public placement selector; authentication remains with the host.
pub use musteragent_placement::DeviceSelector as WorkItemDeviceSelector;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceExecutionPreparation {
    preparation: WorkItemExecutionPreparation,
    approved_delegation: WorkItemDelegationCeiling,
    device_workspace_id: Option<WorkspaceId>,
}
impl WorkItemDeviceExecutionPreparation {
    /// Called only from the original durable Task/consent owner. This validates
    /// consistency; it cannot authenticate caller-supplied permission material.
    pub fn new(
        preparation: WorkItemExecutionPreparation,
        authorized_device_capabilities: Vec<CapabilityEntitlement>,
    ) -> Result<Self, DialogueStoreError> {
        let approved_delegation = WorkItemDelegationCeiling {
            capabilities: authorized_device_capabilities,
            ..preparation.delegation.clone()
        };
        Self::from_approved_delegation(preparation, approved_delegation)
    }
    pub fn from_approved_delegation(
        preparation: WorkItemExecutionPreparation,
        approved_delegation: WorkItemDelegationCeiling,
    ) -> Result<Self, DialogueStoreError> {
        Self::from_approved_delegation_in_workspace(preparation, approved_delegation, None)
    }
    /// A physical root chosen and frozen by the original Device reservation owner.
    /// None is an immutable historical placement, not a request for today's default.
    pub fn from_approved_delegation_in_workspace(
        preparation: WorkItemExecutionPreparation,
        approved_delegation: WorkItemDelegationCeiling,
        device_workspace_id: Option<WorkspaceId>,
    ) -> Result<Self, DialogueStoreError> {
        let value = Self {
            preparation,
            approved_delegation,
            device_workspace_id,
        };
        value.validate()?;
        Ok(value)
    }
    pub const fn device_workspace_id(&self) -> Option<WorkspaceId> {
        self.device_workspace_id
    }
    pub fn preparation(&self) -> &WorkItemExecutionPreparation {
        &self.preparation
    }
    pub fn target(&self) -> &WorkItemDeviceTarget {
        match &self.preparation.target {
            WorkItemExecutionTarget::RemoteDevice(target) => target,
            WorkItemExecutionTarget::Cloud => unreachable!("closed validated Device preparation"),
        }
    }
    pub fn authorized_device_capabilities(&self) -> &[CapabilityEntitlement] {
        &self.approved_delegation.capabilities
    }
    pub fn approved_delegation(&self) -> &WorkItemDelegationCeiling {
        &self.approved_delegation
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.preparation.validate()?;
        if !matches!(
            self.preparation.target,
            WorkItemExecutionTarget::RemoteDevice(_)
        ) || self.preparation.report.is_some()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let original = &self.preparation.delegation;
        let approved = &self.approved_delegation;
        if approved.product_release != original.product_release
            || approved.source_run_id != original.source_run_id
            || approved.source_invocation_id != original.source_invocation_id
            || approved.source_snapshot_id != original.source_snapshot_id
            || approved.input_refs != original.input_refs
        {
            return Err(DialogueStoreError::Corrupt);
        }
        approved.validate()
    }
    /// Provider, execution location, effect, scopes and approval posture are
    /// compared against the separately consented Device ceiling without
    /// rewriting or weakening the source Cloud delegation.
    pub fn permits(&self, capabilities: &[CapabilityEntitlement]) -> bool {
        self.validate().is_ok() && self.approved_delegation.permits(capabilities)
    }
    pub fn source_sha256(&self) -> Result<Sha256, DialogueStoreError> {
        self.validate()?;
        let mut bytes = if self.device_workspace_id.is_some() {
            b"musteragent.work-item-device-source.v2\0".to_vec()
        } else {
            b"musteragent.work-item-device-source.v1\0".to_vec()
        };
        bytes.extend_from_slice(&self.preparation.source_sha256()?.into_bytes());
        bytes.extend_from_slice(&self.approved_delegation.sha256()?.into_bytes());
        if let Some(workspace) = self.device_workspace_id {
            bytes.extend_from_slice(workspace.into_uuid().as_bytes());
        }
        Ok(Sha256::digest(bytes))
    }
    pub fn grant_nonce(&self) -> Result<String, DialogueStoreError> {
        Ok(format!("work_item_device_source_{}", self.source_sha256()?))
    }
}

/// Selection is public input; only this trusted authority resolves it into an
/// owned, consented destination. It must use current Backend membership and
/// Dialogue executor-consent owners, never an arbitrary client-supplied target.
pub type WorkItemDeviceSelection = WorkItemDeviceSelector;
pub trait WorkItemDeviceTargetAuthority: Send + Sync {
    fn resolve(
        &self,
        actor: Actor,
        requesting_agent_id: AgentId,
        executing_agent_id: AgentId,
        selection: WorkItemDeviceSelection,
    ) -> ApplicationFuture<'_, Result<WorkItemDeviceTarget, DialogueStoreError>>;
    /// Only host continuity: delayed facts and stop commands must not require
    /// permission to perform a new Tool effect. Current-only adapters fail
    /// closed for every retained epoch until their original Owner is connected.
    fn qualify_retained_host<'a>(
        &'a self,
        current: &'a crate::DeviceHostAuthority,
        selection: WorkItemDeviceSelection,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceHostQualification, DialogueStoreError>> {
        Box::pin(
            async move { WorkItemDeviceHostQualification::current_selection(current, selection) },
        )
    }
    /// Inspect the original destination without authorizing a device or a tool effect.
    /// An unavailable authority leaves the reservation pending.
    fn inspect_membership_lineage(
        &self,
        _actor: Actor,
        _destination: crate::DeviceDestination,
    ) -> ApplicationFuture<'_, Result<WorkItemDeviceMembershipLineage, DialogueStoreError>> {
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }

    /// Existing reservations retain their target and consent commitment. The
    /// original Owner rechecks current policy in addition to exact host lineage.
    fn resolve_retained<'a>(
        &'a self,
        current: &'a crate::DeviceHostAuthority,
        requesting_agent_id: AgentId,
        executing_agent_id: AgentId,
        target: &'a WorkItemDeviceTarget,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceTargetQualification, DialogueStoreError>> {
        Box::pin(async move {
            let host = WorkItemDeviceHostQualification::current(current, target)?;
            let resolved = self
                .resolve(
                    current.actor(),
                    requesting_agent_id,
                    executing_agent_id,
                    WorkItemDeviceSelection {
                        membership_id: target.membership_id,
                        expected_membership_epoch: target.membership_epoch,
                    },
                )
                .await?;
            Ok(WorkItemDeviceTargetQualification::new(resolved, host))
        })
    }
}

/// A permanent ending is an explicit decision of the original membership authority.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WorkItemDeviceMembershipLineage {
    Continuing,
    Ended,
}

/// Native transport authenticates the exact request bytes before this original
/// Task owner is called. A current host envelope is not itself a grant.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkItemDeviceGrantCustody {
    /// Exact retained original envelope, including its historical codec version.
    pub source_envelope: Vec<u8>,
    pub source: WorkItemDeviceExecutionPreparation,
    pub grant: musteragent_execution_grant::SignedExecutionGrant,
    pub encoded_proof: Vec<u8>,
}
/// Placement is read from the original reservation before grant issuance.
/// It is not an approved delegation and cannot authorize an execution.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceExecutionPlacement {
    pub preparation: WorkItemExecutionPreparation,
    pub device_workspace_id: Option<WorkspaceId>,
}
pub trait WorkItemDeviceExecutionStore: Send + Sync {
    /// No fresh default is available. Only an already frozen placement may be returned.
    fn prepare_retained_device<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
    ) -> ApplicationFuture<'a, Result<Option<WorkItemDeviceExecutionPlacement>, DialogueStoreError>>
    {
        let _ = (authority, claim);
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }
    /// This is a nonterminal observation of the original queue, not a rejection
    /// or an admission receipt. A stale claim cannot change a newer delivery.
    fn defer_workspace<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        source_digest: Sha256,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        let _ = (authority, claim, source_digest);
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }

    fn read_device_source<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
    ) -> ApplicationFuture<'a, Result<Option<WorkItemDeviceGrantCustody>, DialogueStoreError>>;
    /// One exact original proof for material recovery; never fresh authority.
    fn read_material_grant<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        source_digest: Sha256,
        current_grant_hash: Sha256,
        retained_grant_hash: Sha256,
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_execution_grant::SignedExecutionGrant, DialogueStoreError>,
    >;
    fn claim_device<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        owner: uuid::Uuid,
        lease: std::time::Duration,
    ) -> ApplicationFuture<'a, Result<Option<WorkItemExecutionClaim>, DialogueStoreError>>;
    fn prepare_device<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
    ) -> ApplicationFuture<'a, Result<WorkItemExecutionPreparation, DialogueStoreError>>;
    fn prepare_device_in_workspace<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        chosen: Option<WorkspaceId>,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceExecutionPlacement, DialogueStoreError>> {
        Box::pin(async move {
            if chosen.is_some() {
                return Err(DialogueStoreError::Unavailable);
            }
            Ok(WorkItemDeviceExecutionPlacement {
                preparation: self.prepare_device(authority, claim).await?,
                device_workspace_id: None,
            })
        })
    }
    fn freeze_device_source<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        source: WorkItemDeviceExecutionPreparation,
        grant: &'a musteragent_execution_grant::SignedExecutionGrant,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceExecutionPreparation, DialogueStoreError>>;
    fn reauthorize_device_source<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        expected_grant_hash: Sha256,
        source_digest: Sha256,
        grant: &'a musteragent_execution_grant::SignedExecutionGrant,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceGrantCustody, DialogueStoreError>>;
    fn complete_device<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        receipt: WorkItemDeviceAdmissionReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>>;
    fn lookup_device_receipt<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        request: WorkItemExecutionRequest,
    ) -> ApplicationFuture<'a, Result<Option<WorkItemDeviceAdmissionReceipt>, DialogueStoreError>>;
    fn record_device_terminal<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        receipt: WorkItemDeviceTerminalReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>>;
}
