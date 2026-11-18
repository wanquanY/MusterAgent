//! Device destination of the original accepted-input handoff. This does not own
//! Run state, register an Executor, or change the accepted execution Home.
use super::*;
use crate::{DialogueIntentClaim, OriginDeviceExecutionTarget};
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_identity::{DeviceId, DeviceInstallationId, UtcTimestamp};
use std::time::Duration;

/// The exact signed proof recorded by the accepted-source owner before it can
/// be returned to a device. Retention is not permission to admit after expiry.
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceGrantCustody {
    pub source: DeviceSourceCustody,
    pub grant: SignedExecutionGrant,
    pub encoded_proof: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DeviceIntentClaim {
    pub claim: DialogueIntentClaim,
    pub device_id: DeviceId,
    pub installation_id: DeviceInstallationId,
}
/// Exact material frozen once by the source owner, before any execution grant.
/// This receipt is not evidence that a Run exists on the device.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceSourceCustody {
    pub device_id: DeviceId,
    pub installation_id: DeviceInstallationId,
    pub source: DialogueExecutionPreparation,
    pub source_envelope: Vec<u8>,
    pub source_digest: Sha256,
    pub created_at: UtcTimestamp,
}
impl DeviceSourceCustody {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.source.validate()?;
        let binding = self.source.local_execution_binding(self.device_id)?;
        if binding.installation_id() != self.installation_id
            || binding.digest() != self.source_digest
            || self.source_envelope.is_empty()
            || self.source_envelope.len() > 4 * 1024 * 1024
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}
/// Bounded hints from original accepted-input custody, never execution authority.
/// A cursor is only ordering state; callers revalidate every original admission.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceDiscoveryPage {
    pub intents: Vec<UserIntentId>,
    pub next_after: Option<UserIntentId>,
    /// Empty means a legacy server; otherwise one flag for each exact intent.
    pub prepare_allowed: Vec<bool>,
    /// Durable source refusals are obligations, never permission to execute.
    pub source_refusals: Vec<crate::DeviceSourceRefusal>,
}
pub const DIALOGUE_DEVICE_DISCOVERY_LIMIT: usize = 32;
// 4 KiB for identifiers/cursors/hints plus one bounded canonical obligation per intent.
pub const DIALOGUE_DEVICE_DISCOVERY_MAX_BYTES: usize =
    4096 + DIALOGUE_DEVICE_DISCOVERY_LIMIT * (crate::MAX_DEVICE_SOURCE_REFUSAL_BYTES + 4);
impl DeviceDiscoveryPage {
    pub fn validate_refusals(&self) -> Result<(), DialogueStoreError> {
        if self.intents.len() > DIALOGUE_DEVICE_DISCOVERY_LIMIT
            || (!self.prepare_allowed.is_empty()
                && self.prepare_allowed.len() != self.intents.len())
            || self.source_refusals.len() > self.intents.len()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut seen = std::collections::HashSet::new();
        for refusal in &self.source_refusals {
            refusal.validate()?;
            let index = self
                .intents
                .iter()
                .position(|id| *id == refusal.target.intent_id)
                .ok_or(DialogueStoreError::Corrupt)?;
            if !seen.insert(refusal.target.intent_id)
                || self.prepare_allowed.get(index) != Some(&false)
            {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        Ok(())
    }
}

pub trait DeviceHandoffStore: Send + Sync {
    /// Independent source refusal settlement. Absence of an implementation never acknowledges no-admission.
    fn record_source_not_admitted<'a>(
        &'a self,
        _actor: Actor,
        _target: &'a OriginDeviceExecutionTarget,
        _receipt: &'a crate::DeviceSourceNotAdmittedReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        Box::pin(async { Err(DialogueStoreError::Unavailable) })
    }
    fn record_device_observation<'a>(
        &'a self,
        target: &'a OriginDeviceExecutionTarget,
        observation: DeviceAdmissionObservation,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>>;
    fn discover_device<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        after: Option<UserIntentId>,
    ) -> ApplicationFuture<'a, Result<DeviceDiscoveryPage, DialogueStoreError>>;

    /// Records the exact original SQLite admission receipt through a freshly
    /// authenticated Native subject. This is an observation, not a Cloud Run,
    /// renewed execution lease or permission to replay the accepted input.
    fn acknowledge_device_admission<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        receipt: &'a crate::DeviceDeliveryBinding,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>>;
    /// The internal adapter must freshly validate the original signed Native
    /// membership, login and installation before invoking this bounded method.
    fn claim_device(
        &self,
        actor: Actor,
        target: OriginDeviceExecutionTarget,
        intent: UserIntentId,
        owner: uuid::Uuid,
        lease: Duration,
    ) -> ApplicationFuture<'_, Result<Option<DeviceIntentClaim>, DialogueStoreError>>;
    /// Uses the queue generation, account/source owner and current configuration
    /// in one transaction. Replays read the original immutable envelope.
    fn capture_device_source(
        &self,
        claim: DeviceIntentClaim,
    ) -> ApplicationFuture<'_, Result<DeviceSourceCustody, DialogueStoreError>>;
    /// The authenticated pickup selects a physical root before the first source
    /// freeze. Legacy owners may only preserve None; they cannot drop Some.
    fn capture_device_source_in_workspace(
        &self,
        claim: DeviceIntentClaim,
        chosen: Option<musteragent_identity::WorkspaceId>,
    ) -> ApplicationFuture<'_, Result<DeviceSourceCustody, DialogueStoreError>> {
        Box::pin(async move {
            if chosen.is_some() {
                return Err(DialogueStoreError::Unavailable);
            }
            self.capture_device_source(claim).await
        })
    }
    /// A new, independently authorized finite version of the same frozen source.
    /// The expected hash is a CAS token, not authority. Cloud acknowledgement is
    /// unknown until the original SQLite receipt fixes its actual grant version.
    fn reauthorize_device_grant<'a>(
        &'a self,
        claim: DeviceIntentClaim,
        target: &'a OriginDeviceExecutionTarget,
        expected_grant_hash: Sha256,
        source_digest: Sha256,
        signed: SignedExecutionGrant,
    ) -> ApplicationFuture<'a, Result<DeviceGrantCustody, DialogueStoreError>>;
    /// First bind is fresh and current; a different later signed grant cannot
    /// overwrite the execution authority already delivered for this intent.
    fn bind_device_grant(
        &self,
        claim: DeviceIntentClaim,
        signed: SignedExecutionGrant,
    ) -> ApplicationFuture<'_, Result<DeviceGrantCustody, DialogueStoreError>>;
    /// Lost responses recover the exact original proof, never re-sign. A fresh
    /// native membership and live queue claim are still required by the caller.
    fn read_device_grant(
        &self,
        claim: DeviceIntentClaim,
    ) -> ApplicationFuture<'_, Result<Option<DeviceGrantCustody>, DialogueStoreError>>;
}
