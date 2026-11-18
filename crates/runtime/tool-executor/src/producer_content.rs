//! Each invocation owns its output identity independently of tenant-wide text
//! deduplication. This names a byte object; it does not grant content access.
use musteragent_identity::{ContentId, InvocationId};

pub(crate) fn identity(invocation: InvocationId, purpose: &str) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        format!("tool-private-content@1:{purpose}").as_bytes(),
    ))
    .expect("a versioned UUID is non-nil")
}
