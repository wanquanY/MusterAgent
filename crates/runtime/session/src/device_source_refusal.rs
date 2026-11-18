//! A source owner can stop issuing admission authority without cancelling a Run.
//! Only the original local admission writer can prove that no Run was admitted.
use crate::{DialogueStoreError, NativeDialogueExecutionControlTarget};
use musteragent_content_ref::Sha256;
use musteragent_identity::UtcTimestamp;

pub const MAX_DEVICE_SOURCE_REFUSAL_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum DeviceSourceRefusalReason {
    SourceChanged = 1,
    SourceInactive = 2,
}
impl DeviceSourceRefusalReason {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceSourceRefusal {
    pub target: NativeDialogueExecutionControlTarget,
    pub reason: DeviceSourceRefusalReason,
    pub detected_at: UtcTimestamp,
}
impl DeviceSourceRefusal {
    pub const PREFIX: &'static [u8] = b"musteragent.device-source-refusal.v1\0";
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.target.validate()?;
        if self.detected_at.unix_millis() <= 0 {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        self.validate()?;
        let mut bytes = Self::PREFIX.to_vec();
        bytes.push(self.reason.as_u8());
        bytes.extend_from_slice(&self.detected_at.unix_millis().to_be_bytes());
        bytes.extend_from_slice(&self.target.canonical_bytes()?);
        if bytes.len() > MAX_DEVICE_SOURCE_REFUSAL_BYTES {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(bytes)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let bad = DialogueStoreError::Corrupt;
        if bytes.len() > MAX_DEVICE_SOURCE_REFUSAL_BYTES {
            return Err(bad);
        }
        let tail = bytes.strip_prefix(Self::PREFIX).ok_or(bad)?;
        let (head, target) = tail.split_at_checked(9).ok_or(bad)?;
        let reason = match head[0] {
            1 => DeviceSourceRefusalReason::SourceChanged,
            2 => DeviceSourceRefusalReason::SourceInactive,
            _ => return Err(bad),
        };
        let value = Self {
            target: NativeDialogueExecutionControlTarget::decode(target)?,
            reason,
            detected_at: UtcTimestamp::from_unix_millis(i64::from_be_bytes(
                head[1..].try_into().map_err(|_| bad)?,
            )),
        };
        if value.canonical_bytes()? != bytes {
            return Err(bad);
        }
        Ok(value)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceSourceNotAdmittedReceipt {
    pub refusal: DeviceSourceRefusal,
}
impl DeviceSourceNotAdmittedReceipt {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.refusal.validate()
    }
    pub const PREFIX: &'static [u8] = b"musteragent.device-source-not-admitted.v1\0";
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        let mut bytes = Self::PREFIX.to_vec();
        bytes.extend_from_slice(&self.refusal.canonical_bytes()?);
        if bytes.len() > MAX_DEVICE_SOURCE_REFUSAL_BYTES {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(bytes)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > MAX_DEVICE_SOURCE_REFUSAL_BYTES {
            return Err(DialogueStoreError::Corrupt);
        }
        let value = Self {
            refusal: DeviceSourceRefusal::decode(
                bytes
                    .strip_prefix(Self::PREFIX)
                    .ok_or(DialogueStoreError::Corrupt)?,
            )?,
        };
        if value.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::*;
    fn id<T: std::str::FromStr>(n: u128) -> T
    where
        T::Err: std::fmt::Debug,
    {
        uuid::Uuid::from_u128(n)
            .to_string()
            .parse()
            .expect("valid test fixture")
    }
    fn refusal() -> DeviceSourceRefusal {
        DeviceSourceRefusal {
            target: NativeDialogueExecutionControlTarget {
                actor: Actor {
                    tenant_id: id(1),
                    principal_id: id(2),
                    principal_kind: PrincipalKind::User,
                },
                agent_id: id(3),
                dialogue_id: id(4),
                intent_id: id(5),
                run_id: id(6),
                conversation_id: id(7),
                device_id: id(8),
                installation_id: id(9),
                source_digest: Sha256::digest(b"source"),
                grant_revision: 1,
                grant_proof_hash: Some(Sha256::digest(b"proof")),
            },
            reason: DeviceSourceRefusalReason::SourceChanged,
            detected_at: UtcTimestamp::from_unix_millis(1),
        }
    }
    #[test]
    fn source_refusal_is_closed_canonical_and_not_a_cancellation_receipt() {
        let original = refusal();
        let bytes = original.canonical_bytes().expect("valid test fixture");
        assert_eq!(
            DeviceSourceRefusal::decode(&bytes).expect("valid test fixture"),
            original
        );
        let receipt = DeviceSourceNotAdmittedReceipt {
            refusal: original.clone(),
        };
        let encoded = receipt.canonical_bytes().expect("valid test fixture");
        assert_eq!(
            DeviceSourceNotAdmittedReceipt::decode(&encoded).expect("valid test fixture"),
            receipt
        );
        assert!(crate::NativeDialogueExecutionNotAdmittedReceipt::decode(&encoded).is_err());
        assert!(DeviceSourceRefusal::decode(&encoded).is_err());
        for index in [0, DeviceSourceRefusal::PREFIX.len()] {
            let mut invalid = bytes.clone();
            invalid[index] = 0xff;
            assert!(DeviceSourceRefusal::decode(&invalid).is_err());
        }
        for length in 0..bytes.len() {
            assert!(DeviceSourceRefusal::decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(DeviceSourceRefusal::decode(&trailing).is_err());
        let mut changed = original.clone();
        changed.target.grant_revision = 0;
        assert!(changed.canonical_bytes().is_err());
        changed.target.grant_proof_hash = None;
        assert!(changed.canonical_bytes().is_ok());
        changed.detected_at = UtcTimestamp::from_unix_millis(0);
        assert!(changed.canonical_bytes().is_err());
    }
}
