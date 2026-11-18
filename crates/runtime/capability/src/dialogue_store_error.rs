//! Shared typed errors for host-owned conversation operations.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum DialogueStoreError {
    #[error("current device permission is narrower than the original run")]
    PermissionReduced,
    #[error("dialogue object is unavailable to this account")]
    Unavailable,
    #[error("command identity was already used with different content")]
    IdempotencyConflict,
    #[error("dialogue object was changed; reload before retrying")]
    RevisionConflict,
    #[error("account-owned Agent is not active")]
    Inactive,
    #[error("dialogue store contains noncanonical or inconsistent data")]
    Corrupt,
    #[error("dialogue persistence is temporarily unavailable")]
    Infrastructure,
}
