//! Current account Memory for one retained Device admission. This is a bounded
//! source read through original Memory/Content owners, not another Memory store.
use super::*;
use crate::{
    DEVICE_MEMORY_CONTENT_LIMIT, DEVICE_MEMORY_WINDOW_MS, DeviceInstinctMaterial,
    DialogueStoreError, WorkItemDeviceGrantCustody, WorkItemExecutionClaim,
};
use musteragent_content_ref::Sha256;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_content::verify_content;
use musteragent_runtime_memory::UserProfilePrompt;

#[derive(Debug, Clone, PartialEq)]
pub struct WorkItemDeviceAdmissionMemory {
    pub claim: WorkItemExecutionClaim,
    pub source_digest: Sha256,
    pub signed_grant_hash: Sha256,
    pub resolved_at: UtcTimestamp,
    pub valid_until: UtcTimestamp,
    pub profile: Option<UserProfilePrompt>,
    pub instincts: Vec<DeviceInstinctMaterial>,
}
impl WorkItemDeviceAdmissionMemory {
    /// Transport validation never creates authority: the caller first obtains
    /// authentic custody and a current Native membership, then uses this exact
    /// material only for that admission. SQLite caches cannot substitute it.
    pub fn validate_for(
        &self,
        claim: WorkItemExecutionClaim,
        custody: &WorkItemDeviceGrantCustody,
        now: UtcTimestamp,
    ) -> Result<(), DialogueStoreError> {
        custody.source.validate()?;
        self.validate_binding(
            claim,
            &custody.source,
            &custody.grant,
            &custody.encoded_proof,
            now,
        )
    }
    /// Original signature authentication precedes this relationship check.
    /// Native can validate without inventing a server-side custody timestamp.
    pub fn validate_binding(
        &self,
        claim: WorkItemExecutionClaim,
        source: &WorkItemDeviceExecutionPreparation,
        grant: &musteragent_execution_grant::SignedExecutionGrant,
        encoded_proof: &[u8],
        now: UtcTimestamp,
    ) -> Result<(), DialogueStoreError> {
        let bad = DialogueStoreError::Corrupt;
        source.validate()?;
        validate_work_item_device_grant(source, &grant.claims)?;
        if self.claim != claim
            || self.source_digest != source.source_sha256()?
            || self.signed_grant_hash != Sha256::digest(encoded_proof)
            || source.preparation().key.request() != claim.request
            || self.resolved_at >= self.valid_until
            || self.valid_until > claim.lease_expires_at
            || self.valid_until > grant.claims.expires_at
            || self.resolved_at < grant.claims.issued_at
            || self.valid_until
                > self
                    .resolved_at
                    .checked_add_millis(DEVICE_MEMORY_WINDOW_MS)
                    .ok_or(bad)?
            || self.resolved_at > now
            || self.valid_until <= now
        {
            return Err(DialogueStoreError::Unavailable);
        }
        if let Some(profile) = &self.profile
            && (profile.bytes.is_empty()
                || profile.bytes.len() > 32 * 1024
                || std::str::from_utf8(&profile.bytes).is_err()
                || profile.source_versions.is_empty()
                || profile.source_versions.len() > 64
                || profile
                    .source_versions
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1]))
        {
            return Err(bad);
        }
        if self.instincts.len() > 100 {
            return Err(bad);
        }
        let mut previous = None;
        let mut total = self
            .profile
            .as_ref()
            .map_or(0, |profile| profile.bytes.len());
        for value in &self.instincts {
            value.selection.validate().map_err(|_| bad)?;
            if previous.is_some_and(|id| id >= value.selection.instinct_id) {
                return Err(bad);
            }
            previous = Some(value.selection.instinct_id);
            total = total.checked_add(value.bytes.len()).ok_or(bad)?;
            if total > DEVICE_MEMORY_CONTENT_LIMIT {
                return Err(bad);
            }
            verify_content(&value.selection.payload_ref, &value.bytes).map_err(|_| bad)?;
        }
        Ok(())
    }
}
