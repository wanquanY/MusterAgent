use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{CommandId, ContentId, TenantId};

use crate::{ContentStoreError, MAX_CONTENT_BYTES};

/// Product mutation slot bound into an admission content receipt.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AdmissionContentSlot {
    SubmitMessage,
    EditAndResubmit,
    RunControl,
    DelegationObjective,
    CoordinationContext,
}

impl AdmissionContentSlot {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::SubmitMessage => 1,
            Self::EditAndResubmit => 2,
            Self::RunControl => 3,
            Self::DelegationObjective => 4,
            Self::CoordinationContext => 5,
        }
    }

    pub fn from_code(value: u8) -> Result<Self, ContentStoreError> {
        match value {
            1 => Ok(Self::SubmitMessage),
            2 => Ok(Self::EditAndResubmit),
            3 => Ok(Self::RunControl),
            4 => Ok(Self::DelegationObjective),
            5 => Ok(Self::CoordinationContext),
            _ => Err(ContentStoreError::Invariant),
        }
    }
}

/// Side-effect-free source for an admission content plan.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AdmissionContentSource {
    Inline {
        media_type: MediaType,
        bytes: Vec<u8>,
    },
    CarryForward(ContentRef),
}

/// Fully bound plan produced only after command-level validation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionContentPlan {
    pub tenant_id: TenantId,
    pub command_id: CommandId,
    pub command_fingerprint: Sha256,
    pub slot: AdmissionContentSlot,
    pub proposed_content_id: ContentId,
    pub source: AdmissionContentSource,
}

impl AdmissionContentPlan {
    pub fn validate(&self) -> Result<(), ContentStoreError> {
        let reference = self.proposed_reference()?;
        if reference.byte_length > MAX_CONTENT_BYTES {
            return Err(ContentStoreError::TooLarge);
        }
        Ok(())
    }

    pub fn proposed_reference(&self) -> Result<ContentRef, ContentStoreError> {
        match &self.source {
            AdmissionContentSource::Inline { media_type, bytes } => {
                let byte_length =
                    u64::try_from(bytes.len()).map_err(|_| ContentStoreError::TooLarge)?;
                Ok(ContentRef {
                    content_id: self.proposed_content_id,
                    media_type: media_type.clone(),
                    byte_length,
                    sha256: Sha256::digest(bytes),
                })
            }
            AdmissionContentSource::CarryForward(reference) => {
                if reference.content_id != self.proposed_content_id {
                    return Err(ContentStoreError::Invariant);
                }
                Ok(reference.clone())
            }
        }
    }

    pub fn content_identity(&self) -> Result<Sha256, ContentStoreError> {
        Ok(semantic_identity(
            self.tenant_id,
            &self.proposed_reference()?,
        ))
    }

    pub fn stage_identity(&self) -> Result<Sha256, ContentStoreError> {
        let mut bytes = b"musteragent.legacy.admission-content-stage.v1\0".to_vec();
        bytes.extend_from_slice(self.tenant_id.into_uuid().as_bytes());
        bytes.extend_from_slice(self.command_id.into_uuid().as_bytes());
        bytes.extend_from_slice(&self.command_fingerprint.into_bytes());
        bytes.push(self.slot.code());
        bytes.extend_from_slice(&self.content_identity()?.into_bytes());
        Ok(Sha256::digest(bytes))
    }

    #[must_use]
    pub const fn is_inline(&self) -> bool {
        matches!(&self.source, AdmissionContentSource::Inline { .. })
    }

    #[must_use]
    pub fn inline_bytes(&self) -> Option<&[u8]> {
        match &self.source {
            AdmissionContentSource::Inline { bytes, .. } => Some(bytes),
            AdmissionContentSource::CarryForward(_) => None,
        }
    }
}

/// Opaque product-admission receipt. A persistence adapter must revalidate all
/// fields in the same transaction that publishes the content reference.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionContentStage {
    pub tenant_id: TenantId,
    pub command_id: CommandId,
    pub command_fingerprint: Sha256,
    pub slot: AdmissionContentSlot,
    pub stage_identity: Sha256,
    pub content_identity: Sha256,
    pub content: ContentRef,
}

impl AdmissionContentStage {
    pub fn validate_for(&self, plan: &AdmissionContentPlan) -> Result<(), ContentStoreError> {
        plan.validate()?;
        let proposed = plan.proposed_reference()?;
        if self.tenant_id != plan.tenant_id
            || self.command_id != plan.command_id
            || self.command_fingerprint != plan.command_fingerprint
            || self.slot != plan.slot
            || self.stage_identity != plan.stage_identity()?
            || self.content_identity != plan.content_identity()?
            || self.content.media_type != proposed.media_type
            || self.content.byte_length != proposed.byte_length
            || self.content.sha256 != proposed.sha256
            || semantic_identity(self.tenant_id, &self.content) != self.content_identity
            || matches!(&plan.source, AdmissionContentSource::CarryForward(_))
                && self.content.content_id != proposed.content_id
        {
            return Err(ContentStoreError::Invariant);
        }
        Ok(())
    }
}

#[must_use]
pub fn semantic_identity(tenant_id: TenantId, reference: &ContentRef) -> Sha256 {
    let mut bytes = b"musteragent.legacy.content-identity.v1\0".to_vec();
    bytes.extend_from_slice(tenant_id.into_uuid().as_bytes());
    bytes.extend_from_slice(reference.media_type.as_str().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(&reference.byte_length.to_be_bytes());
    bytes.extend_from_slice(&reference.sha256.into_bytes());
    Sha256::digest(bytes)
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{MediaType, Sha256};
    use musteragent_identity::{CommandId, ContentId, TenantId};

    use super::{
        AdmissionContentPlan, AdmissionContentSlot, AdmissionContentSource, AdmissionContentStage,
    };

    fn plan() -> AdmissionContentPlan {
        AdmissionContentPlan {
            tenant_id: TenantId::generate(),
            command_id: CommandId::generate(),
            command_fingerprint: Sha256::digest(b"command"),
            slot: AdmissionContentSlot::SubmitMessage,
            proposed_content_id: ContentId::generate(),
            source: AdmissionContentSource::Inline {
                media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
                bytes: b"hello".to_vec(),
            },
        }
    }

    #[test]
    fn receipt_is_bound_to_every_admission_identity() {
        let plan = plan();
        let content = plan.proposed_reference().expect("reference");
        let receipt = AdmissionContentStage {
            tenant_id: plan.tenant_id,
            command_id: plan.command_id,
            command_fingerprint: plan.command_fingerprint,
            slot: plan.slot,
            stage_identity: plan.stage_identity().expect("stage identity"),
            content_identity: plan.content_identity().expect("content identity"),
            content,
        };
        assert_eq!(receipt.validate_for(&plan), Ok(()));
        let mut wrong = plan;
        wrong.command_fingerprint = Sha256::digest(b"wrong");
        assert!(receipt.validate_for(&wrong).is_err());
    }
}
