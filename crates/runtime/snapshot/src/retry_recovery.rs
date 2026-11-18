use super::{RuntimeSnapshotContent, SnapshotContractError};
impl RuntimeSnapshotContent {
    /// Explicit retries retain the admitted recovery policy and authority.
    pub fn with_explicit_retry_recovery(self) -> Result<Self, SnapshotContractError> {
        self.validate()?;
        Ok(self)
    }
}
