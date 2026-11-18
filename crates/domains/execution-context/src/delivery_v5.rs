//! Forward discriminated syntax. Decode only; the shared delivery validator and
//! original publication owners still decide purpose, identity and authority.
use crate::delivery_contract::{Delivery, NullableString, PublicDeliveryParseError};
use musteragent_identity::{CommandId, ToolCallId};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    deliveries: Vec<DeliveryV5>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum DeliveryV5 {
    Message {
        purpose: String,
        text: String,
    },
    Interaction {
        purpose: String,
        interaction_id: String,
    },
    Artifact {
        purpose: String,
        artifact_ref: ArtifactRef,
        artifact_tool_call_id: String,
    },
    PartnerCreated {
        purpose: String,
        creation_command_id: String,
        partner_tool_call_id: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRef {
    kind: LocatorKind,
    id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum LocatorKind {
    Artifact,
    FileNode,
}

pub(super) fn decode(bytes: &[u8]) -> Result<Vec<Delivery>, PublicDeliveryParseError> {
    let envelope: Envelope =
        serde_json::from_slice(bytes).map_err(|_| PublicDeliveryParseError::Invalid)?;
    envelope
        .deliveries
        .into_iter()
        .map(|entry| {
            let mut raw = Delivery {
                kind: String::new(),
                purpose: String::new(),
                text: NullableString(None),
                interaction_id: NullableString(None),
                artifact_id: NullableString(None),
                artifact_handle: NullableString(None),
                file_node: false,
                partner: None,
            };
            match entry {
                DeliveryV5::Message { purpose, text } => {
                    raw.kind = "message".into();
                    raw.purpose = purpose;
                    raw.text = NullableString(Some(text));
                }
                DeliveryV5::Interaction {
                    purpose,
                    interaction_id,
                } => {
                    raw.kind = "interaction".into();
                    raw.purpose = purpose;
                    raw.interaction_id = NullableString(Some(interaction_id));
                }
                DeliveryV5::Artifact {
                    purpose,
                    artifact_ref,
                    artifact_tool_call_id,
                } => {
                    raw.kind = "artifact".into();
                    raw.purpose = purpose;
                    raw.file_node = matches!(artifact_ref.kind, LocatorKind::FileNode);
                    raw.artifact_id = NullableString(Some(artifact_ref.id));
                    raw.artifact_handle = NullableString(Some(artifact_tool_call_id));
                }
                DeliveryV5::PartnerCreated {
                    purpose,
                    creation_command_id,
                    partner_tool_call_id,
                } => {
                    raw.kind = "partner_created".into();
                    raw.purpose = purpose;
                    raw.partner = Some((
                        creation_command_id
                            .parse::<CommandId>()
                            .map_err(|_| PublicDeliveryParseError::Invalid)?,
                        ToolCallId::new(partner_tool_call_id)
                            .map_err(|_| PublicDeliveryParseError::Invalid)?,
                    ));
                }
            }
            Ok(raw)
        })
        .collect()
}
