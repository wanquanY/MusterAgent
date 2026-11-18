//! A current host may carry facts for an exact retained release subject only
//! after the original membership Owner has authenticated its complete lineage.
//! These transient projections are neither grants nor persisted authority.
use super::*;
use crate::DeviceHostAuthority;
use musteragent_content_ref::Sha256;
use musteragent_device_authority::{Subject, decode_canonical};

#[derive(Debug, Clone)]
pub struct WorkItemDeviceHostQualification {
    current_authority_hash: Sha256,
    membership_id: uuid::Uuid,
    membership_epoch: u64,
    subject: Subject,
}
impl WorkItemDeviceHostQualification {
    /// The authenticated Backend capability transport alone calls this decoder.
    /// Structural equality does not prove ancestry; the membership Owner must
    /// verify the exact signed chain, current account/head, and revocations.
    pub fn from_trusted_backend_transport(
        current: &DeviceHostAuthority,
        echoed_current_authority: &[u8],
        membership_id: uuid::Uuid,
        membership_epoch: u64,
        retained_subject: &[u8],
    ) -> Result<Self, DialogueStoreError> {
        let subject: Subject =
            decode_canonical(retained_subject).map_err(|_| DialogueStoreError::Corrupt)?;
        subject
            .validate_active()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let mut stable = subject.clone();
        stable.client_release_ref = current.subject().client_release_ref.clone();
        stable.client_release_sha256 = current.subject().client_release_sha256.clone();
        if echoed_current_authority != current.native_membership_authority()
            || membership_id != current.destination().membership_id
            || membership_epoch == 0
            || membership_epoch > i64::MAX as u64
            || stable != *current.subject()
            || (membership_epoch == current.destination().membership_epoch.get()
                && subject != *current.subject())
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(Self {
            current_authority_hash: Sha256::digest(echoed_current_authority),
            membership_id,
            membership_epoch,
            subject,
        })
    }
    /// Preserves the existing exact-current transport contract. Implementations
    /// without a retained-membership Owner cannot authorize any older subject.
    pub fn current(
        current: &DeviceHostAuthority,
        target: &WorkItemDeviceTarget,
    ) -> Result<Self, DialogueStoreError> {
        target.validate()?;
        let d = current.destination();
        if d.device_id != target.device_id
            || d.installation_id != target.installation_id
            || d.membership_id != target.membership_id
            || d.membership_epoch.get() != target.membership_epoch
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(Self {
            current_authority_hash: Sha256::digest(current.native_membership_authority()),
            membership_id: target.membership_id,
            membership_epoch: target.membership_epoch,
            subject: current.subject().clone(),
        })
    }
    pub fn current_selection(
        current: &DeviceHostAuthority,
        selection: WorkItemDeviceSelection,
    ) -> Result<Self, DialogueStoreError> {
        selection
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if selection.membership_id != current.destination().membership_id
            || selection.expected_membership_epoch != current.destination().membership_epoch.get()
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(Self {
            current_authority_hash: Sha256::digest(current.native_membership_authority()),
            membership_id: selection.membership_id,
            membership_epoch: selection.expected_membership_epoch,
            subject: current.subject().clone(),
        })
    }
    pub fn require_selection(
        &self,
        current: &DeviceHostAuthority,
        selection: WorkItemDeviceSelection,
        now: UtcTimestamp,
    ) -> Result<(), DialogueStoreError> {
        selection
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        current
            .require_current_at(now)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        if self.current_authority_hash != Sha256::digest(current.native_membership_authority())
            || self.membership_id != selection.membership_id
            || self.membership_epoch != selection.expected_membership_epoch
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
    pub fn require_for(
        &self,
        current: &DeviceHostAuthority,
        actor: Actor,
        target: &WorkItemDeviceTarget,
        now: UtcTimestamp,
    ) -> Result<(), DialogueStoreError> {
        target.validate()?;
        self.require_selection(
            current,
            WorkItemDeviceSelection {
                membership_id: target.membership_id,
                expected_membership_epoch: target.membership_epoch,
            },
            now,
        )?;
        if current.actor() != actor
            || self.subject.device_id.as_str() != target.device_id.to_string()
            || self.subject.client_installation_id.as_str() != target.installation_id.to_string()
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WorkItemDeviceTargetQualification {
    target: WorkItemDeviceTarget,
    host: WorkItemDeviceHostQualification,
}
impl WorkItemDeviceTargetQualification {
    pub fn new(target: WorkItemDeviceTarget, host: WorkItemDeviceHostQualification) -> Self {
        Self { target, host }
    }
    pub fn require_for(
        &self,
        current: &DeviceHostAuthority,
        actor: Actor,
        target: &WorkItemDeviceTarget,
        now: UtcTimestamp,
    ) -> Result<(), DialogueStoreError> {
        if self.target != *target {
            return Err(DialogueStoreError::RevisionConflict);
        }
        self.host.require_for(current, actor, target, now)
    }
}
