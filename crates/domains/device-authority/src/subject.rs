use serde::{Deserialize, Serialize};

use crate::{
    ApplicationId, CanonicalContract, CanonicalUuid, ContractError, DeploymentOrigin,
    LoginRevision, PositiveDecimal, ReleaseRef, Result, Sha256Hex,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub deployment_origin: DeploymentOrigin,
    pub installation_id: CanonicalUuid,
    pub installation_key_id: Sha256Hex,
    pub host_id: CanonicalUuid,
    pub device_id: CanonicalUuid,
    pub device_key_id: Sha256Hex,
    pub application_id: ApplicationId,
    pub client_installation_id: CanonicalUuid,
    pub client_release_ref: ReleaseRef,
    pub client_release_sha256: Sha256Hex,
}

impl CanonicalContract for Target {
    fn validate(&self) -> Result<()> {
        if !self.client_release_ref.matches(&self.client_release_sha256) {
            return Err(ContractError::Binding("client_release"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subject {
    pub deployment_origin: DeploymentOrigin,
    pub tenant_id: CanonicalUuid,
    pub principal_id: CanonicalUuid,
    pub installation_id: CanonicalUuid,
    pub installation_key_id: Sha256Hex,
    pub host_id: CanonicalUuid,
    pub device_id: CanonicalUuid,
    pub device_key_id: Sha256Hex,
    pub application_id: ApplicationId,
    pub client_installation_id: CanonicalUuid,
    pub client_release_ref: ReleaseRef,
    pub client_release_sha256: Sha256Hex,
    pub login_session_id: CanonicalUuid,
    pub login_revision: LoginRevision,
    pub account_auth_epoch: PositiveDecimal,
}
impl Subject {
    pub fn target(&self) -> Target {
        Target {
            deployment_origin: self.deployment_origin.clone(),
            installation_id: self.installation_id.clone(),
            installation_key_id: self.installation_key_id.clone(),
            host_id: self.host_id.clone(),
            device_id: self.device_id.clone(),
            device_key_id: self.device_key_id.clone(),
            application_id: self.application_id,
            client_installation_id: self.client_installation_id.clone(),
            client_release_ref: self.client_release_ref.clone(),
            client_release_sha256: self.client_release_sha256.clone(),
        }
    }
    pub fn validate_active(&self) -> Result<()> {
        self.validate()?;
        if self.login_revision != LoginRevision::Active {
            return Err(ContractError::Binding("active_login_revision"));
        }
        Ok(())
    }
}
impl CanonicalContract for Subject {
    fn validate(&self) -> Result<()> {
        self.target().validate()
    }
}
