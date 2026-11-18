//! Kernel RPC contracts and canonical ledger/scheduler encoding.
#![forbid(unsafe_code)]
pub use musteragent_persistence_codec::*;
mod outbound;
mod payload_codec;
mod scheduler;
pub use payload_codec::ProtobufLedgerPayloadCodec;
pub use scheduler::ProtobufSchedulerEventEncoder;
mod project_task_codec;

/// Generated kernel protocol values and RPC client/server traits.
#[allow(
    clippy::large_enum_variant,
    reason = "Prost generates the public protobuf oneof payload types here."
)]
pub mod wire {
    pub use musteragent_persistence_codec::wire::*;
    include!(concat!(env!("OUT_DIR"), "/musteragent.v1.rs"));
}
/// Complete descriptors for the RPC contracts and their shared message dependencies.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/wire-descriptor.bin"));

mod usage_codec;
pub use musteragent_event_envelope::RECORD_SCHEMA_VERSION;
pub use musteragent_runtime_contract::COMMAND_SCHEMA_VERSION;
pub use musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION;
pub use usage_codec::*;

#[cfg(test)]
mod contract_tests;

mod projection_snapshot_codec;
pub use projection_snapshot_codec::{
    ProtobufConversationProjectionCodec, conversation_projection_snapshot_message,
};

mod agent_context_codec;
mod approval_observation_codec;
mod context_support;
pub use agent_context_codec::*;

mod security_audit_codec;
pub use security_audit_codec::*;
