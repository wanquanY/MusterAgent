//! Immutable, bounded source bytes frozen by the Dialogue custody transaction.
//! Unknown fields, duplicate encodings and mismatched relationships fail closed.
//! This material is not a grant and never creates a second Agent/config authority.
use crate::{
    agent_release_codec as release, dialogue_codec::*, dialogue_context_codec as context, wire,
};
use musteragent_execution_context::DialogueOutputContract;
use musteragent_identity::*;
use musteragent_runtime_session::*;
use prost::Message;

type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;
pub const MAX_DEVICE_SOURCE_BYTES: usize = 4 * 1024 * 1024;

pub fn encode(device: DeviceId, source: &DialogueExecutionPreparation) -> Result<Vec<u8>, Error> {
    encode_mode(device, source, false)
}
pub fn encode_frozen(
    device: DeviceId,
    source: &DialogueExecutionPreparation,
) -> Result<Vec<u8>, Error> {
    encode_mode(device, source, false)
}
fn encode_mode(
    device: DeviceId,
    source: &DialogueExecutionPreparation,
    frozen: bool,
) -> Result<Vec<u8>, Error> {
    source.validate().map_err(|_| BAD)?;
    source.local_execution_binding(device).map_err(|_| BAD)?;
    if !source.delivery_contract.is_native() {
        return Err(BAD);
    }
    let value = wire::DeviceExecutionSource {
        schema_version: (if source.device_workspace_id.is_some() {
            2
        } else {
            1
        }) + if frozen { 2 } else { 0 },
        device_workspace_id: source.device_workspace_id.map(|id| wire::WorkspaceId {
            value: id.to_string(),
        }),
        device_id: Some(wire::DeviceId {
            value: device.to_string(),
        }),
        default_agent_id: Some(wire::AgentId {
            value: source.default_agent_id.to_string(),
        }),
        intent: Some(intent(&source.intent)),
        agent: Some(agent(&source.agent)),
        configuration: Some(configuration(&source.configuration)),
        definition: Some(release::definition(&source.assistant_identity.definition)?),
        version: Some(release::version(&source.assistant_identity.version)?),
        context_cut: Some(context::cut(&source.context_cut)),
        context_has_input_attachments: source.context_has_input_attachments,
        delivery_contract: Some(wire::VersionedRef {
            key: source.delivery_contract.key().into(),
        }),
    };
    let bytes = value.encode_to_vec();
    if bytes.len() > MAX_DEVICE_SOURCE_BYTES {
        return Err(BAD);
    }
    Ok(bytes)
}
pub fn decode(bytes: &[u8]) -> Result<(DeviceId, DialogueExecutionPreparation), Error> {
    if bytes.is_empty() || bytes.len() > MAX_DEVICE_SOURCE_BYTES {
        return Err(BAD);
    }
    let value = wire::DeviceExecutionSource::decode(bytes).map_err(|_| BAD)?;
    let frozen = false;
    if value.encode_to_vec() != bytes
        || value.schema_version
            != (if value.device_workspace_id.is_some() {
                2
            } else {
                1
            }) + if frozen { 2 } else { 0 }
    {
        return Err(BAD);
    }
    let delivery_contract =
        DialogueOutputContract::native_from_key(&required(value.delivery_contract)?.key)
            .ok_or(BAD)?;
    let device = id(value.device_id.map(|v| v.value))?;
    let intent = decode_intent(required(value.intent)?)?;
    let agent = decode_agent(required(value.agent)?)?;
    let scope = DialogueExecutionScope {
        tenant_id: intent.tenant_id,
        owner_id: intent.owner_id,
        agent_id: agent.agent_id,
        dialogue_id: intent.dialogue_id,
        source_intent_id: intent.intent_id,
        context_watermark: intent.accepted_cursor,
    };
    let source = DialogueExecutionPreparation {
        origin_tool_device_id: None,
        product_release: None,
        device_workspace_id: value
            .device_workspace_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        scope,
        default_agent_id: id(value.default_agent_id.map(|v| v.value))?,
        identities: DialogueExecutionIdentities::derive(scope).map_err(|_| BAD)?,
        intent,
        agent,
        configuration: decode_configuration(required(value.configuration)?)?,
        assistant_identity: AgentAdmissionIdentity {
            definition: release::decode_definition(required(value.definition)?)?,
            version: release::decode_version(required(value.version)?)?,
        },
        context_cut: context::decode_cut(required(value.context_cut)?)?,
        context_has_input_attachments: value.context_has_input_attachments,
        delivery_contract,
    };
    source.validate().map_err(|_| BAD)?;
    source.local_execution_binding(device).map_err(|_| BAD)?;
    // Domain codecs normalize bounded scalars. Only the exact canonical owner
    // representation is durable; alternate normalized strings cannot alias it.
    if encode_mode(device, &source, frozen)? != bytes {
        return Err(BAD);
    }
    Ok((device, source))
}
