//! Compatibility boundary for exact retained proposal contracts.
//! Input business rules live in the Memory package; authority stays in Runtime.
pub(super) use musteragent_memory_business::proposal::ProposalInput;

#[cfg(test)]
mod tests;
