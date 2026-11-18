//! Ordered, append-only extension composition for the durable execution kernel.
//!
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod audit;
mod audit_index;
mod history;
mod rewrite;
pub use history::*;
pub use rewrite::{PayloadDigest, RewriteContent};
mod hooks;
mod host;
mod loop_detection;
mod model;
mod policy;
mod registry;
mod runtime;
pub mod samples;
mod tools;

pub use audit::*;
pub use hooks::*;
pub use host::*;
pub use loop_detection::*;
pub use model::ExtendedModelProvider;
pub use policy::ExtensionLoopPolicy;
pub use registry::*;
pub use runtime::*;
pub use tools::*;

use musteragent_runtime_model::{CanonicalContentPart, CanonicalMessage, JsonValue};

/// Stable JSON representation; object keys are sorted independently of serde features.
pub fn canonical_arguments(value: &JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(v) => (*v).into(),
        JsonValue::Integer(v) => (*v).into(),
        JsonValue::Number(v) => serde_json::json!(v),
        JsonValue::String(v) => v.clone().into(),
        JsonValue::Array(v) => v.iter().map(canonical_arguments).collect(),
        JsonValue::Object(v) => v
            .iter()
            .map(|(k, v)| (k.clone(), canonical_arguments(v)))
            .collect(),
    }
}

pub(crate) fn message_value(message: &CanonicalMessage) -> serde_json::Value {
    let content: Vec<_> = message
        .content
        .iter()
        .map(|part| match part {
            CanonicalContentPart::Text(v) => serde_json::json!({"text":v.as_str()}),
            CanonicalContentPart::Image(v) => serde_json::json!({"image":content_value(v)}),
            CanonicalContentPart::Audio(v) => serde_json::json!({"audio":content_value(v)}),
            CanonicalContentPart::Video(v) => serde_json::json!({"video":content_value(v)}),
            CanonicalContentPart::File(v) => serde_json::json!({"file":content_value(v)}),
        })
        .collect();
    serde_json::json!({"content":content})
}

fn content_value(v: &musteragent_content_ref::ContentRef) -> serde_json::Value {
    serde_json::json!({"content_id":v.content_id.to_string(),"sha256":v.sha256.to_string(),"media_type":v.media_type.as_str(),"byte_length":v.byte_length})
}
