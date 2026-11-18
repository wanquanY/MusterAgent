//! Compare frozen Memory decisions inside the original storage transaction.
use crate::{MemoryApplicationError, MemorySearchHit, PromoteExperience, RememberUserMemory};
use musteragent_identity::{MemoryVersionId, UtcTimestamp};
use musteragent_memory::{MemoryLifecycle, MemoryRecord, MemoryVersion};

impl RememberUserMemory {
    /// An approved no-op retains the original version and its provenance.
    pub fn retains_current_version(&self) -> bool {
        self.expected_current_version_id == Some(self.version.memory_version_id)
    }

    /// Adapters must read and lock the current head in the same transaction as
    /// the write. Exact duplicates still pass through their version/index checks.
    pub fn validate_current_head(
        &self,
        current: Option<&MemorySearchHit>,
        committed_at: UtcTimestamp,
    ) -> Result<(), MemoryApplicationError> {
        self.validate()?;
        validate_head(
            &self.record,
            &self.version,
            self.expected_current_version_id,
            current,
            committed_at,
            true,
        )
        .map_err(|_| MemoryApplicationError::InvalidUserMemoryWrite)
    }
}

impl PromoteExperience {
    /// Publication has no no-op: it must create the exact approved successor.
    pub fn validate_current_head(
        &self,
        current: Option<&MemorySearchHit>,
        committed_at: UtcTimestamp,
    ) -> Result<(), MemoryApplicationError> {
        self.validate()?;
        validate_head(
            &self.memory_record,
            &self.memory_version,
            self.expected_current_version_id,
            current,
            committed_at,
            false,
        )
        .map_err(|_| MemoryApplicationError::InvalidPromotion)
    }
}

fn validate_head(
    record: &MemoryRecord,
    version: &MemoryVersion,
    expected: Option<MemoryVersionId>,
    current: Option<&MemorySearchHit>,
    committed_at: UtcTimestamp,
    allow_retained: bool,
) -> Result<(), MemoryApplicationError> {
    let invalid = || MemoryApplicationError::InvalidPromotion;
    if record.lifecycle != MemoryLifecycle::Active
        || record.revoked_at.is_some()
        || record.updated_at > committed_at
        || version.created_at > committed_at
        || version.valid_until.is_some_and(|at| at <= committed_at)
    {
        return Err(invalid());
    }
    let Some(head) = current else {
        return if expected.is_none() && version.version.get() == 1 {
            Ok(())
        } else {
            Err(invalid())
        };
    };
    head.record.validate()?;
    head.version.validate()?;
    if head.record.memory_id != record.memory_id
        || head.record.tenant_id != record.tenant_id
        || head.record.scope != record.scope
        || head.record.memory_key != record.memory_key
        || head.record.created_at != record.created_at
        || head.record.kind != record.kind
        || head.record.current_version_id != head.version.memory_version_id
        || head.record.memory_id != head.version.memory_id
        || head.record.lifecycle != MemoryLifecycle::Active
        || head.record.revoked_at.is_some()
        || head.record.updated_at > committed_at
        || head.version.created_at > committed_at
        || head
            .version
            .valid_until
            .is_some_and(|at| at <= committed_at)
    {
        return Err(invalid());
    }
    if head.record.current_version_id == version.memory_version_id {
        // No-op or exact retry: never disguise a lifecycle/kind change as
        // Duplicate. The adapter verifies all version and index bytes.
        return if allow_retained && head.record == *record {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    if expected != Some(head.record.current_version_id)
        || head.record.updated_at > record.updated_at
        || head.version.version.get().checked_add(1) != Some(version.version.get())
    {
        return Err(invalid());
    }
    Ok(())
}
