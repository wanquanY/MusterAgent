use crate::{
    CanonicalContract, CanonicalUuid, ContractError, PositiveDecimal, Result, Sha256Hex, Subject,
    UnixMillis, canonical_bytes, tags::*,
};
use serde::{Deserialize, Serialize};
pub const AUTHORITY_DOMAIN: &[u8] = b"musteragent.device-authority.authority.v1\0";
pub const AUTHORITY_DURATION_MS: u64 = 300_000;
pub trait SigningPayload: CanonicalContract {
    fn domain(&self) -> &'static [u8];
    /// Input construction only. Private keys and signing stay in their owner.
    fn signing_input(&self) -> Result<Vec<u8>>
    where
        Self: Sized,
    {
        let mut bytes = self.domain().to_vec();
        bytes.extend(canonical_bytes(self)?);
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub schema_version: AuthoritySchema,
    pub issuer: Issuer,
    pub audience: AuthorityAudience,
    pub operation_id: CanonicalUuid,
    pub operation_digest: Sha256Hex,
    pub subject: Subject,
    pub membership_id: CanonicalUuid,
    pub membership_epoch: PositiveDecimal,
    pub state: ActiveState,
    pub issued_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
}
impl CanonicalContract for Authority {
    fn validate(&self) -> Result<()> {
        self.subject.validate_active()?;
        duration(
            self.issued_at_unix_ms,
            self.expires_at_unix_ms,
            AUTHORITY_DURATION_MS,
        )
    }
}
impl SigningPayload for Authority {
    fn domain(&self) -> &'static [u8] {
        AUTHORITY_DOMAIN
    }
}
pub(crate) fn require(holds: bool, field: &'static str) -> Result<()> {
    if !holds {
        return Err(ContractError::Binding(field));
    }
    Ok(())
}
fn duration(start: UnixMillis, end: UnixMillis, expected: u64) -> Result<()> {
    require(
        end.as_u64().checked_sub(start.as_u64()) == Some(expected),
        "lease_duration",
    )
}
