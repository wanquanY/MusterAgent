use super::{ContextPolicySnapshot, SnapshotContractError};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextSourceSelectionPolicy {
    LegacyAdmissionV1,
    ExecutionIsolatedV1,
}

impl ContextPolicySnapshot {
    pub fn validate_source_selection_schema(
        &self,
        schema: &str,
    ) -> Result<(), SnapshotContractError> {
        if schema != crate::SNAPSHOT_SCHEMA_VERSION || self.source_selection.is_none() {
            return Err(SnapshotContractError::SchemaMismatch);
        }
        Ok(())
    }

    #[must_use]
    pub fn effective_source_selection(&self) -> ContextSourceSelectionPolicy {
        self.source_selection
            .unwrap_or(ContextSourceSelectionPolicy::LegacyAdmissionV1)
    }
}
