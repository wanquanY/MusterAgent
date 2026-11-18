//! Closed, bounded data codecs for the original Agent Context ports.
//! Native provenance, current Worker permission and content access are checked
//! independently by the Home and Executor owners around each invocation.
mod command;
mod compaction;
mod material;
mod plan;
mod reply;
use crate::wire;
use musteragent_loop::{AgentContextCommand, AgentContextReply};
use prost::Message;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AgentContextCodecError {
    #[error("Agent Context data exceeds its decoding budget")]
    Bounds,
    #[error("Agent Context data is invalid or noncanonical")]
    Invalid,
    #[error("Agent Context trusted schema is unavailable")]
    Schema,
}
type Result<T> = std::result::Result<T, AgentContextCodecError>;
fn checked<T, E>(value: std::result::Result<T, E>) -> Result<T> {
    value.map_err(|_| AgentContextCodecError::Invalid)
}
fn required<T>(value: Option<T>) -> Result<T> {
    value.ok_or(AgentContextCodecError::Invalid)
}
fn id<T: std::str::FromStr>(value: Option<String>) -> Result<T> {
    checked(required(value)?.parse())
}
fn positive(value: u32) -> Result<NonZeroU32> {
    required(NonZeroU32::new(value))
}
fn positive64(value: u64) -> Result<NonZeroU64> {
    required(NonZeroU64::new(value))
}
fn content(value: wire::ContentRef) -> Result<musteragent_content_ref::ContentRef> {
    checked(crate::wire_common::decode_content_ref(value))
}
fn hash(value: wire::Sha256) -> Result<musteragent_content_ref::Sha256> {
    let bytes: [u8; 32] = value
        .value
        .try_into()
        .map_err(|_| AgentContextCodecError::Invalid)?;
    checked(musteragent_content_ref::Sha256::from_bytes(bytes))
}
fn hash_wire(value: musteragent_content_ref::Sha256) -> wire::Sha256 {
    wire::Sha256 {
        value: value.into_bytes().to_vec(),
    }
}

fn budget(bytes: &[u8], maximum: NonZeroUsize, message: &str) -> Result<()> {
    static SCHEMA: std::sync::OnceLock<
        std::result::Result<prost_reflect::DescriptorPool, crate::bounded_protobuf::BudgetError>,
    > = std::sync::OnceLock::new();
    crate::bounded_protobuf::check_descriptor(
        bytes,
        maximum,
        message,
        crate::bounded_protobuf::Profile::Context,
        crate::FILE_DESCRIPTOR_SET,
        &SCHEMA,
    )
    .map_err(|e| match e {
        crate::bounded_protobuf::BudgetError::Bounds => AgentContextCodecError::Bounds,
        crate::bounded_protobuf::BudgetError::Invalid => AgentContextCodecError::Invalid,
        crate::bounded_protobuf::BudgetError::Schema => AgentContextCodecError::Schema,
    })
}
fn bytes<M: Message>(value: M, maximum: NonZeroUsize, name: &str) -> Result<Vec<u8>> {
    if value.encoded_len() > maximum.get() {
        return Err(AgentContextCodecError::Bounds);
    }
    let bytes = value.encode_to_vec();
    budget(&bytes, maximum, name)?;
    Ok(bytes)
}
pub fn encode_agent_context_command(
    value: &AgentContextCommand,
    maximum: NonZeroUsize,
) -> Result<Vec<u8>> {
    bytes(
        command::encode(value)?,
        maximum,
        "musteragent.v1.AgentContextCommand",
    )
}
pub fn decode_agent_context_command(
    data: &[u8],
    maximum: NonZeroUsize,
) -> Result<AgentContextCommand> {
    budget(data, maximum, "musteragent.v1.AgentContextCommand")?;
    let value = command::decode(checked(wire::AgentContextCommand::decode(data))?)?;
    if encode_agent_context_command(&value, maximum)? != data {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(value)
}
pub fn encode_agent_context_reply(
    value: &AgentContextReply,
    maximum: NonZeroUsize,
) -> Result<Vec<u8>> {
    bytes(
        reply::encode(value)?,
        maximum,
        "musteragent.v1.AgentContextReply",
    )
}
pub fn decode_agent_context_reply(data: &[u8], maximum: NonZeroUsize) -> Result<AgentContextReply> {
    let value = decode_canonical_reply(data, maximum)?;
    if matches!(&value, AgentContextReply::Material(material) if !material.runtime_snapshot.application_catalogs.is_empty())
    {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(value)
}

fn decode_canonical_reply(data: &[u8], maximum: NonZeroUsize) -> Result<AgentContextReply> {
    budget(data, maximum, "musteragent.v1.AgentContextReply")?;
    let value = reply::decode(checked(wire::AgentContextReply::decode(data))?)?;
    if encode_agent_context_reply(&value, maximum)? != data {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(value)
}
pub async fn decode_agent_context_reply_with_applications(
    data: &[u8],
    maximum: NonZeroUsize,
    source: Option<
        &dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
    >,
) -> std::result::Result<
    AgentContextReply,
    musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError,
> {
    use musteragent_runtime_snapshot::application_catalog::{
        ApplicationCatalogMaterialError as E, resolve_tool_catalog,
    };
    let reply = decode_canonical_reply(data, maximum).map_err(|_| E::Corrupt)?;
    if let AgentContextReply::Material(material) = &reply
        && !material.runtime_snapshot.application_catalogs.is_empty()
    {
        let catalog = resolve_tool_catalog(&material.runtime_snapshot, source).await?;
        let expected = musteragent_context::resolve_context_tools(
            &material.runtime_snapshot,
            &catalog,
            source,
            &material.exposed_tool_keys,
        )
        .await?;
        if expected != material.resolved_tools {
            return Err(E::Corrupt);
        }
    }
    Ok(reply)
}

fn route_ref(value: Option<String>) -> Result<musteragent_runtime_model::ModelRouteRef> {
    Ok(musteragent_runtime_model::ModelRouteRef::from_key(checked(
        musteragent_identity::VersionedKey::new(required(value)?),
    )?))
}

#[cfg(test)]
mod tests;
