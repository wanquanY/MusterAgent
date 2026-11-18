//! Original marketplace installation continuity attached to an observed signed bundle.
//! A pin identifies evidence; current account visibility and Device membership are
//! still checked by their original owners at admission and physical dispatch.
use crate::McpContractError;
use std::num::NonZeroU64;
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ManagedComponentAuthorityPin {
    pub product_id: String,
    pub entry_id: Uuid,
    pub release_id: Uuid,
    pub installation_authority_id: Uuid,
    pub authorization_epoch: NonZeroU64,
    pub component_id: String,
    pub target: String,
    pub archive_sha256: [u8; 32],
    pub bundle_sha256: [u8; 32],
}
impl ManagedComponentAuthorityPin {
    pub fn validate(&self) -> Result<(), McpContractError> {
        if musteragent_identity::ProductId::parse(&self.product_id).is_err()
            || [
                self.entry_id,
                self.release_id,
                self.installation_authority_id,
            ]
            .iter()
            .any(Uuid::is_nil)
            || self.authorization_epoch.get() > i64::MAX as u64
            || self.component_id.is_empty()
            || self.component_id.len() > 128
            || !self.component_id.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'_')
            })
            || !matches!(
                self.target.as_str(),
                "darwin-arm64"
                    | "darwin-x64"
                    | "linux-arm64"
                    | "linux-x64"
                    | "win32-arm64"
                    | "win32-x64"
            )
            || self.archive_sha256 == [0; 32]
            || self.bundle_sha256 == [0; 32]
        {
            return Err(McpContractError::InvalidToolFilter);
        }
        Ok(())
    }
    /// Only necessary continuity; retained publication membership is separately
    /// proven by the original account installation receipt and Component Host.
    pub fn same_installation(&self, other: &Self) -> bool {
        self.product_id == other.product_id
            && self.entry_id == other.entry_id
            && self.installation_authority_id == other.installation_authority_id
            && self.authorization_epoch == other.authorization_epoch
            && self.component_id == other.component_id
            && self.target == other.target
    }
}
