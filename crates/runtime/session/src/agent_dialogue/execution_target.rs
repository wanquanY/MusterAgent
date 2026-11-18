//! A fixed admission destination, not execution authority. Only the trusted
//! native transport may carry the original verified membership envelope here.
//! The original Grant signer revalidates current authority before execution.
use super::{AcceptDialogueIntent, DialogueStoreError};
use musteragent_content_ref::Sha256;
use musteragent_device_authority::{Authority, BackendEnvelope, BackendPayload, decode_canonical};
use musteragent_execution_context::DialogueInputSurface;
use musteragent_identity::{DeviceId, DeviceInstallationId, UtcTimestamp};

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub enum DialogueIntentExecutionTarget {
    #[default]
    Cloud,
    CloudFromDesktop(crate::WorkItemDeviceSelection),
    DesktopTools(DeviceId),
    OriginDevice(OriginDeviceExecutionTarget),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OriginDeviceExecutionTarget {
    device_id: DeviceId,
    installation_id: DeviceInstallationId,
    membership_authority: Vec<u8>,
    authority: Box<Authority>,
}
impl OriginDeviceExecutionTarget {
    /// This decodes/binds data; it does NOT verify signature trust or live
    /// membership. Those checks belong to the authenticated Backend owner.
    pub fn from_trusted_native_transport(
        device_id: DeviceId,
        installation_id: DeviceInstallationId,
        membership_authority: Vec<u8>,
    ) -> Result<Self, DialogueStoreError> {
        let envelope: BackendEnvelope =
            decode_canonical(&membership_authority).map_err(|_| DialogueStoreError::Unavailable)?;
        let BackendPayload::Authority(authority) = envelope
            .decode_payload()
            .map_err(|_| DialogueStoreError::Unavailable)?;
        if authority.subject.device_id.as_str() != device_id.to_string()
            || authority.subject.application_id
                != musteragent_device_authority::ApplicationId::Musteragent
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(Self {
            device_id,
            installation_id,
            membership_authority,
            authority,
        })
    }
    pub const fn device_id(&self) -> DeviceId {
        self.device_id
    }
    /// Immutable product data origin. Backend binds it through the verified
    /// Device key; Native independently checks its protected client placement.
    /// It is not the Native client's registration ID or the shared Host ID.
    pub const fn installation_id(&self) -> DeviceInstallationId {
        self.installation_id
    }
    /// Verified by the original Backend membership owner, never request text.
    pub fn client_release_sha256(&self) -> &str {
        self.authority.subject.client_release_sha256.as_str()
    }
    pub fn membership_authority(&self) -> &[u8] {
        &self.membership_authority
    }
    pub fn require_actor(
        &self,
        actor: musteragent_identity::Actor,
    ) -> Result<(), DialogueStoreError> {
        if actor.principal_kind != musteragent_identity::PrincipalKind::User
            || self.authority.subject.tenant_id.as_str() != actor.tenant_id.to_string()
            || self.authority.subject.principal_id.as_str() != actor.principal_id.to_string()
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
    pub fn require_current_at(&self, now: UtcTimestamp) -> Result<(), DialogueStoreError> {
        let now = u64::try_from(now.unix_millis()).map_err(|_| DialogueStoreError::Unavailable)?;
        if now < self.authority.issued_at_unix_ms.get()
            || now >= self.authority.expires_at_unix_ms.get()
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
}
impl DialogueIntentExecutionTarget {
    pub fn validate_for(&self, command: &AcceptDialogueIntent) -> Result<(), DialogueStoreError> {
        if let Self::CloudFromDesktop(host) = self {
            host.validate()
                .map_err(|_| DialogueStoreError::Unavailable)?;
        }
        if matches!(self, Self::DesktopTools(_) | Self::CloudFromDesktop(_))
            && (command.origin.surface != DialogueInputSurface::Desktop
                || command.origin.installation_id.is_none())
        {
            return Err(DialogueStoreError::Unavailable);
        }
        if let Self::OriginDevice(target) = self
            && (!matches!(
                command.origin.surface,
                DialogueInputSurface::Desktop | DialogueInputSurface::Mobile
            ) || command.origin.installation_id != Some(target.installation_id)
                || target.authority.subject.tenant_id.as_str()
                    != command.actor.tenant_id.to_string()
                || target.authority.subject.principal_id.as_str()
                    != command.actor.principal_id.to_string())
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
    pub fn fingerprint(&self, command: &AcceptDialogueIntent) -> Sha256 {
        if let Self::DesktopTools(device) = self {
            let mut bytes = b"musteragent.desktop-origin-tools.v1".to_vec();
            bytes.extend_from_slice(&command.fingerprint().into_bytes());
            bytes.extend_from_slice(device.into_uuid().as_bytes());
            return Sha256::digest(bytes);
        }
        if let Self::CloudFromDesktop(host) = self {
            let mut bytes = b"musteragent.dialogue-intent.desktop-cloud-source.v1\0".to_vec();
            bytes.extend_from_slice(&command.fingerprint().into_bytes());
            bytes.extend_from_slice(host.membership_id.as_bytes());
            bytes.extend_from_slice(&host.expected_membership_epoch.to_be_bytes());
            return Sha256::digest(bytes);
        }
        let Self::OriginDevice(target) = self else {
            return command.fingerprint();
        };
        let mut bytes = b"musteragent.dialogue-intent.v3\0".to_vec();
        bytes.extend_from_slice(&command.fingerprint().into_bytes());
        bytes.extend_from_slice(target.device_id.into_uuid().as_bytes());
        bytes.extend_from_slice(target.installation_id.into_uuid().as_bytes());
        bytes.extend_from_slice(&Sha256::digest(&target.membership_authority).into_bytes());
        Sha256::digest(bytes)
    }
}
