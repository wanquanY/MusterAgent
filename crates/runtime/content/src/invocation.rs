//! Host-authorized prewrites, reserved before any output is consumed. The
//! original Runtime owns execution authority and durable accounting; this port
//! neither creates a task nor publishes an Artifact.
use crate::{ApplicationFuture, ContentStore, ContentStoreError};
use musteragent_content_ref::ContentRef;
use musteragent_identity::{InvocationId, TenantId};

pub const MAX_INVOCATION_CONTENT_OBJECTS: u32 = 65_536;

/// Immutable lifetime limits for one Invocation, including reserved but not yet
/// materialized outputs. Retries share the same account and exact identities.
/// These limits do not describe process memory, worker spool or device capacity.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InvocationContentLimits {
    max_objects: u32,
    max_bytes: u64,
}
impl InvocationContentLimits {
    pub fn new(max_objects: u32, max_bytes: u64) -> Result<Self, ContentStoreError> {
        if max_objects == 0 || max_bytes == 0 {
            return Err(ContentStoreError::Invariant);
        }
        if max_objects > MAX_INVOCATION_CONTENT_OBJECTS || max_bytes > i64::MAX as u64 {
            return Err(ContentStoreError::TooLarge);
        }
        Ok(Self {
            max_objects,
            max_bytes,
        })
    }
    pub fn max_objects(self) -> u32 {
        self.max_objects
    }
    pub fn max_bytes(self) -> u64 {
        self.max_bytes
    }
}

/// A ContentStore explicitly bound by the host to its original execution scope.
/// Ordinary stores do not implement this authority. Writes on a scoped store
/// require a prior exact reservation, including byte length, media type and hash.
pub trait InvocationContentStore: ContentStore {
    /// Atomically reserves the complete set or changes nothing. Identical
    /// reservations are idempotent across restart; conflicting identities,
    /// changed limits, foreign scopes and stale/cancelled executions fail.
    fn reserve_outputs<'a>(
        &'a self,
        tenant: TenantId,
        invocation: InvocationId,
        outputs: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>>;
}
