//! Immutable policy checkpoint captured by the original consent owner. The
//! Runtime binds it to its Task. Only the Backend proves capability succession.
use super::super::device_admission::Reader;
use super::*;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceConsentAnchor {
    pub authority_ref: String,
    pub policy_ref: String,
    pub effective_capabilities_sha256: Sha256,
}
impl WorkItemDeviceConsentAnchor {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        for key in [&self.authority_ref, &self.policy_ref] {
            if !(1..=160).contains(&key.len()) || !key.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        if self.effective_capabilities_sha256.into_bytes() == [0; 32] {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
    pub(in crate::work_item_execution) fn canonical_bytes(
        &self,
    ) -> Result<Vec<u8>, DialogueStoreError> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(self.authority_ref.len() + self.policy_ref.len() + 40);
        for key in [&self.authority_ref, &self.policy_ref] {
            bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
            bytes.extend_from_slice(key.as_bytes());
        }
        bytes.extend_from_slice(&self.effective_capabilities_sha256.into_bytes());
        Ok(bytes)
    }
    pub(in crate::work_item_execution) fn read(
        r: &mut Reader<'_>,
    ) -> Result<Self, DialogueStoreError> {
        let value = Self {
            authority_ref: std::str::from_utf8(r.frame(160)?)
                .map_err(|_| DialogueStoreError::Corrupt)?
                .to_owned(),
            policy_ref: std::str::from_utf8(r.frame(160)?)
                .map_err(|_| DialogueStoreError::Corrupt)?
                .to_owned(),
            effective_capabilities_sha256: r.hash()?,
        };
        value.validate()?;
        Ok(value)
    }
}
