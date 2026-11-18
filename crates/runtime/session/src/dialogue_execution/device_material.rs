//! Exact foundation and accepted body bytes from retained Dialogue custody.
//! No client-supplied ContentRef, attachment, workspace or write authority is accepted.
use super::*;
use crate::DeviceIntentClaim;
use musteragent_content_ref::ContentRef;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_content::verify_content;
use std::collections::BTreeMap;

pub const DEVICE_SOURCE_ITEM_LIMIT: usize = 1024 * 1024;
pub const DEVICE_SOURCE_TOTAL_LIMIT: usize = 6 * DEVICE_SOURCE_ITEM_LIMIT;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceSourceContent {
    pub reference: ContentRef,
    pub bytes: Vec<u8>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceSourceMaterial {
    pub claim: DeviceIntentClaim,
    pub source_digest: Sha256,
    pub signed_grant_hash: Sha256,
    pub resolved_at: UtcTimestamp,
    pub valid_until: UtcTimestamp,
    pub contents: Vec<DeviceSourceContent>,
}
/// The original accepted source alone chooses these references. Public history,
/// media attachments and installed packages have their own qualified read owners.
pub fn device_source_references(
    source: &DialogueExecutionPreparation,
) -> Result<Vec<ContentRef>, DialogueStoreError> {
    source.validate()?;
    let version = &source.assistant_identity.version;
    let material = version
        .material
        .as_ref()
        .ok_or(DialogueStoreError::Corrupt)?;
    let mut references = BTreeMap::new();
    for reference in [
        Some(&material.manifest_ref),
        Some(&material.policy_content_ref),
        Some(&material.memory_policy_content_ref),
        Some(&version.persona_content_ref),
        source.configuration.instructions_ref.as_ref(),
        Some(&source.intent.content_ref),
    ]
    .into_iter()
    .flatten()
    {
        if reference.byte_length > DEVICE_SOURCE_ITEM_LIMIT as u64 {
            return Err(DialogueStoreError::Unavailable);
        }
        if references
            .insert(reference.content_id, reference.clone())
            .is_some_and(|old| old != *reference)
        {
            return Err(DialogueStoreError::Corrupt);
        }
    }
    Ok(references.into_values().collect())
}
impl DeviceSourceMaterial {
    pub fn validate_binding(
        &self,
        claim: DeviceIntentClaim,
        source: &DialogueExecutionPreparation,
        grant: &SignedExecutionGrant,
        encoded_proof: &[u8],
        now: UtcTimestamp,
    ) -> Result<(), DialogueStoreError> {
        source.validate()?;
        let binding = source.local_execution_binding(claim.device_id)?;
        if self.claim != claim
            || self.source_digest != binding.digest()
            || self.signed_grant_hash != Sha256::digest(encoded_proof)
            || binding.installation_id() != claim.installation_id
            || source.actor() != claim.claim.actor
            || source.scope.source_intent_id != claim.claim.intent_id
            || self.resolved_at >= self.valid_until
            || self.resolved_at > now
            || self.valid_until <= now
            || self.resolved_at < grant.claims.issued_at
            || self.valid_until > grant.claims.expires_at
            || self.valid_until > claim.claim.lease_expires_at
            || self.valid_until
                > self
                    .resolved_at
                    .checked_add_millis(super::DEVICE_MEMORY_WINDOW_MS)
                    .ok_or(DialogueStoreError::Corrupt)?
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let references = device_source_references(source)?;
        if references.len() != self.contents.len() {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut total = 0usize;
        for (reference, material) in references.iter().zip(&self.contents) {
            total = total
                .checked_add(material.bytes.len())
                .ok_or(DialogueStoreError::Corrupt)?;
            if reference != &material.reference
                || material.bytes.len() > DEVICE_SOURCE_ITEM_LIMIT
                || total > DEVICE_SOURCE_TOTAL_LIMIT
            {
                return Err(DialogueStoreError::Corrupt);
            }
            verify_content(reference, &material.bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        }
        Ok(())
    }
}
