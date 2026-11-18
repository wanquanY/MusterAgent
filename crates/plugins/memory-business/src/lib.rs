//! Memory business rules, independent of Runtime, storage and execution authority.
//!
//! The host supplies verified, visible candidate text and binds every write to
//! the original actor, approval and fenced transaction. This crate cannot read
//! Content, query storage, grant consent or commit a mutation.
//!
//! Extraction is the first migration step; linking this library into a host is
//! not a signed component release or an independently upgradeable execution path.

pub mod experience_ranking;
pub mod features;
pub mod inspection;
pub mod merge;
pub mod preparation;
pub mod proposal;
pub mod ranking;
pub mod read_plan;
pub mod user_memory_document;

/// Identifies the compiled transition implementation, not a signed release.
pub const COMPILED_IMPLEMENTATION: &str = concat!(
    "compiled:musteragent-memory-business@",
    env!("CARGO_PKG_VERSION")
);

#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
#[error("Memory business input is invalid")]
pub struct InvalidInput;
