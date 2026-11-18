//! Transport-free, strict codecs for durable kernel storage.
//! This crate generates no RPC clients, servers or product service contracts.
#![forbid(unsafe_code)]
/// Versioned persistence messages; domain types remain the public aggregate model.
#[allow(
    clippy::large_enum_variant,
    reason = "Prost generates the public protobuf oneof payload types here."
)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/musteragent.v1.rs"));
}
#[doc(hidden)]
pub mod snapshot_codec;
pub use snapshot_codec::{ProtobufSnapshotCodec, SnapshotCodecError};
mod maintenance_snapshot_codec;
pub use maintenance_snapshot_codec::ProtobufMaintenanceSnapshotCodec;
#[doc(hidden)]
pub mod wire_common;
pub use wire_common::WireContractError;
pub use wire_common::{actor as encode_actor, content_ref as encode_content_ref};
mod application_catalog_codec;
pub mod dialogue_context_codec;
mod product_release_codec;
mod snapshot_components;
pub use snapshot_components::*;
mod execution_liveness_codec;
pub use execution_liveness_codec::*;

mod browser_codec;
pub use browser_codec::*;
mod provider_round;
pub use provider_round::ProviderRoundFence;

#[doc(hidden)]
pub mod context_codec;
pub mod dialogue_codec;
mod scalar;
pub use context_codec::CanonicalJsonContextCodec;
mod model_begin_intent;
pub mod public_delivery_repair_codec;
#[cfg(test)]
mod public_delivery_repair_codec_tests;
pub use model_begin_intent::*;

#[doc(hidden)]
pub mod bounded_protobuf;

const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/storage-descriptor.bin"));

pub mod agent_release_codec;
pub mod content_references;
pub mod device_context_codec;
pub mod device_source_codec;
pub mod device_terminal_codec;
pub mod dialogue_admission_codec;
pub mod dialogue_admission_progress_codec;
pub mod dialogue_execution_activity_codec;
pub mod execution_grant_codec;
pub mod execution_grant_decode;
pub mod goal_processing_rules;
pub mod ledger_content;
pub mod native_dialogue_interaction_codec;
pub mod owner_goal_admission_codec;
pub mod owner_goal_creation_codec;
pub mod public_reactions_codec;
pub mod skill_product_import_codec;
pub mod wire_decode;
pub mod work_item_device_source_codec;
pub use execution_grant_codec::*;
pub use ledger_content::*;
pub use product_release_codec::{
    decode as decode_product_release, encode as encode_product_release,
};
pub mod device_publication_codec;
pub mod dialogue_approval_context_codec;
pub mod dialogue_execution_context_codec;
pub mod error_codec;
mod hosted_scope_codec;
pub mod submission_decode;
pub use hosted_scope_codec::*;
pub use submission_decode::content_ref as decode_content_ref;
pub mod account_skill_codec;
pub mod goal_evidence_batch;
#[cfg(test)]
mod grant_test_fixture;
pub mod owner_event_sources_codec;
pub mod private_execution_policy;
pub mod work_item_admission_codec;
pub use private_execution_policy::*;

pub mod context_plan_lifecycle_codec;
