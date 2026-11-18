//! Decoding at a trusted Backend transport boundary; Backend's original Device
//! proof + current membership owner authenticates this exact envelope first.
use musteragent_device_authority::{
    ApplicationId, BackendEnvelope, BackendPayload, decode_canonical,
};
use musteragent_identity::PrincipalKind;
use musteragent_identity::{Actor, DeviceId, DeviceInstallationId, UtcTimestamp};
use std::num::NonZeroU64;

#[derive(Debug, Clone)]
pub struct DeviceHostAuthority {
    actor: Actor,
    destination: DeviceDestination,
    issued_at_ms: u64,
    expires_at_ms: u64,
    native_membership_authority: Vec<u8>,
    subject: musteragent_device_authority::Subject,
}
impl DeviceHostAuthority {
    /// Account-authenticated Backend management requests derive their physical
    /// subject only from the original membership envelope, not public JSON.
    pub fn from_trusted_account_transport(
        actor: Actor,
        envelope: &[u8],
    ) -> Result<Self, DeviceAuthorityError> {
        let decoded: BackendEnvelope =
            decode_canonical(envelope).map_err(|_| DeviceAuthorityError::Invalid)?;
        let BackendPayload::Authority(value) = decoded
            .decode_payload()
            .map_err(|_| DeviceAuthorityError::Invalid)?;
        let destination = DeviceDestination {
            device_id: value
                .subject
                .device_id
                .as_str()
                .parse()
                .map_err(|_| DeviceAuthorityError::Invalid)?,
            installation_id: value
                .subject
                .client_installation_id
                .as_str()
                .parse()
                .map_err(|_| DeviceAuthorityError::Invalid)?,
            membership_id: value
                .membership_id
                .as_str()
                .parse()
                .map_err(|_| DeviceAuthorityError::Invalid)?,
            membership_epoch: NonZeroU64::new(value.membership_epoch.get() as u64)
                .ok_or(DeviceAuthorityError::Invalid)?,
        };
        Self::from_trusted_native_transport(actor, destination, envelope)
    }

    /// Binds existing authenticated authority; does not independently verify a
    /// signature or create an Agent permission, execution destination, or grant.
    pub fn from_trusted_native_transport(
        actor: Actor,
        destination: DeviceDestination,
        envelope: &[u8],
    ) -> Result<Self, DeviceAuthorityError> {
        destination.validate()?;
        let native_membership_authority = envelope.to_vec();
        let envelope: BackendEnvelope =
            decode_canonical(envelope).map_err(|_| DeviceAuthorityError::Invalid)?;
        let BackendPayload::Authority(value) = envelope
            .decode_payload()
            .map_err(|_| DeviceAuthorityError::Invalid)?;
        if actor.principal_kind != PrincipalKind::User
            || value.subject.tenant_id.as_str() != actor.tenant_id.to_string()
            || value.subject.principal_id.as_str() != actor.principal_id.to_string()
            || value.subject.application_id != ApplicationId::Musteragent
            || value.subject.device_id.as_str() != destination.device_id.to_string()
            || value.subject.client_installation_id.as_str()
                != destination.installation_id.to_string()
            || value.membership_id.as_str() != destination.membership_id.to_string()
            || value.membership_epoch.get() as u64 != destination.membership_epoch.get()
        {
            return Err(DeviceAuthorityError::Forbidden);
        }
        Ok(Self {
            actor,
            destination,
            issued_at_ms: value.issued_at_unix_ms.get(),
            expires_at_ms: value.expires_at_unix_ms.get(),
            native_membership_authority,
            subject: value.subject,
        })
    }
    pub const fn actor(&self) -> Actor {
        self.actor
    }
    pub const fn destination(&self) -> DeviceDestination {
        self.destination
    }
    /// Exact current transport evidence. A retained subject must never replace
    /// this envelope or extend its original validity interval.
    pub fn native_membership_authority(&self) -> &[u8] {
        &self.native_membership_authority
    }
    pub(crate) fn subject(&self) -> &musteragent_device_authority::Subject {
        &self.subject
    }
    pub fn require_current_at(&self, now: UtcTimestamp) -> Result<(), DeviceAuthorityError> {
        let now = u64::try_from(now.unix_millis()).map_err(|_| DeviceAuthorityError::Forbidden)?;
        if now < self.issued_at_ms || now >= self.expires_at_ms {
            return Err(DeviceAuthorityError::Forbidden);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DeviceDestination {
    pub device_id: DeviceId,
    /// Product client installation, not the underlying signed Host installation.
    pub installation_id: DeviceInstallationId,
    pub membership_id: uuid::Uuid,
    pub membership_epoch: NonZeroU64,
}
impl DeviceDestination {
    pub fn validate(&self) -> Result<(), DeviceAuthorityError> {
        if self.membership_id.is_nil() || self.membership_epoch.get() > i64::MAX as u64 {
            return Err(DeviceAuthorityError::Invalid);
        }
        Ok(())
    }
}

/// A device subject is malformed or no longer authorized by its original owner.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum DeviceAuthorityError {
    #[error("invalid device authority")]
    Invalid,
    #[error("device authority is not current")]
    Forbidden,
}
