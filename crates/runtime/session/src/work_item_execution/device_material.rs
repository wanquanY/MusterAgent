//! Exact foundation and Task goal bytes from retained WorkItem custody.
//! No client-supplied ContentRef, attachment, workspace or write authority is accepted.
use super::*;
use crate::WorkItemExecutionClaim;
use crate::{
    DEVICE_MEMORY_WINDOW_MS, DEVICE_SOURCE_ITEM_LIMIT, DEVICE_SOURCE_TOTAL_LIMIT,
    DeviceSourceContent,
};
use musteragent_content_ref::ContentRef;
use musteragent_content_ref::Sha256;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_content::verify_content;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceSourceMaterial {
    pub claim: WorkItemExecutionClaim,
    pub source_digest: Sha256,
    pub signed_grant_hash: Sha256,
    pub resolved_at: UtcTimestamp,
    pub valid_until: UtcTimestamp,
    pub contents: Vec<DeviceSourceContent>,
}
/// The original accepted source alone chooses these references. Public history,
/// media attachments and installed packages have their own qualified read owners.
pub fn work_item_device_source_references(
    source: &WorkItemDeviceExecutionPreparation,
) -> Result<Vec<ContentRef>, DialogueStoreError> {
    source.validate()?;
    let source = source.preparation();
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
        Some(source.input_content_ref()),
        source.feedback.as_ref().map(|f| &f.previous_result),
        source
            .feedback
            .as_ref()
            .map(|_| &source.work_item.goal_content_ref),
    ]
    .into_iter()
    .flatten()
    .chain(source.dependency_results.iter().map(|d| &d.content_ref))
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
impl WorkItemDeviceSourceMaterial {
    pub fn validate_binding(
        &self,
        claim: WorkItemExecutionClaim,
        source: &WorkItemDeviceExecutionPreparation,
        grant: &SignedExecutionGrant,
        encoded_proof: &[u8],
        now: UtcTimestamp,
    ) -> Result<(), DialogueStoreError> {
        source.validate()?;
        validate_work_item_device_grant(source, &grant.claims)?;
        if self.claim != claim
            || self.source_digest != source.source_sha256()?
            || self.signed_grant_hash != Sha256::digest(encoded_proof)
            || source.preparation().key.request() != claim.request
            || self.resolved_at >= self.valid_until
            || self.resolved_at > now
            || self.valid_until <= now
            || self.resolved_at < grant.claims.issued_at
            || self.valid_until > grant.claims.expires_at
            || self.valid_until > claim.lease_expires_at
            || self.valid_until
                > self
                    .resolved_at
                    .checked_add_millis(DEVICE_MEMORY_WINDOW_MS)
                    .ok_or(DialogueStoreError::Corrupt)?
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let references = work_item_device_source_references(source)?;
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
