use thiserror::Error;

const MAX_RETENTION_POLICY_REF_BYTES: usize = 256;

/// Opaque reference to an approved, versioned data-retention policy.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DataRetentionPolicyRef(String);

impl DataRetentionPolicyRef {
    pub fn new(value: impl Into<String>) -> Result<Self, DataRetentionPolicyRefError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_RETENTION_POLICY_REF_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(DataRetentionPolicyRefError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("retention policy reference must be 1..=256 visible ASCII bytes")]
pub struct DataRetentionPolicyRefError;

#[cfg(test)]
mod tests {
    use super::DataRetentionPolicyRef;

    #[test]
    fn policy_reference_is_bounded_and_log_safe() {
        assert!(DataRetentionPolicyRef::new("privacy.standard@1").is_ok());
        assert!(DataRetentionPolicyRef::new("").is_err());
        assert!(DataRetentionPolicyRef::new("contains space").is_err());
        assert!(DataRetentionPolicyRef::new("x".repeat(257)).is_err());
    }
}
