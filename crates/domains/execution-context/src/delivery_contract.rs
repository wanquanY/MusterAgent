//! Explicit dialogue delivery protocol. Arbitrary assistant prose is not a
//! candidate. Runtime binds recipients and persists message bytes itself.
use crate::{DialoguePublicationPurpose, PublicDeliveryContent};
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, CommandId, InteractionId, ResourceNodeId, ToolCallId,
};
use serde::Deserialize;
use std::collections::BTreeSet;

pub const PUBLIC_DELIVERY_CONTRACT_KEY: &str = "musteragent.public-delivery@1";
pub const PUBLIC_DELIVERY_SCHEMA: &str = include_str!("delivery_schema.json");
pub const MAX_PUBLIC_DELIVERY_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_PUBLIC_DELIVERIES: usize = 16;
pub const MAX_DIALOGUE_MESSAGE_BYTES: usize = 64 * 1024;
/// Native publication Tools declare `maxLength: 65536` on message text, and
/// JSON Schema counts characters. Count the same way so Chinese text that the
/// schema admits is never rejected afterwards (byte size is bounded by
/// `MAX_PUBLICATION_TOOL_BYTES`).
pub const MAX_DIALOGUE_MESSAGE_CHARS: usize = 65_536;

pub fn public_delivery_contract_hash() -> Sha256 {
    Sha256::digest(PUBLIC_DELIVERY_SCHEMA.as_bytes())
}

pub const fn public_delivery_instruction() -> &'static str {
    "You are responding as the agent for this user’s account. Work privately with tools as needed. When ready to deliver, return the public-delivery response schema, containing only a useful answer, a necessary question/decision, an important change affecting the user's goal, or committed artifacts. Do not include acknowledgements without new information, reasoning, tool progress, execution logs, or completion notices after an answer. Message text is user-readable Markdown. Never specify a recipient, account, Agent, ContentId or arbitrary action. Artifact and interaction identifiers must come from authoritative tool results in this task; never invent them. Only claim an action succeeded when an authoritative receipt verifies it. Plain assistant prose outside this contract is internal and will not be sent to the user. Do not use an empty delivery to silently end an unanswered user request."
}

/// The sealed prompt binds both behavioral instruction and exact schema bytes.
/// Changing either requires a new contract identity before release.
pub fn public_delivery_prompt() -> String {
    format!(
        "{}\nOutput contract: {}\nSchema:\n{}",
        public_delivery_instruction(),
        PUBLIC_DELIVERY_CONTRACT_KEY,
        PUBLIC_DELIVERY_SCHEMA
    )
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParsedPublicDelivery {
    pub purpose: DialoguePublicationPurpose,
    pub content: PublicDeliveryDraft,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PublicDeliveryDraft {
    /// Acknowledges an already committed creation card. Only the original
    /// successful Tool and activation publication can satisfy this reference.
    PartnerCreated {
        creation_command_id: CommandId,
        partner_tool_call_id: ToolCallId,
    },
    /// Runtime must store these actual bytes; no model-supplied ContentId.
    Message(String),
    Interaction(InteractionId),
    /// V2 references only an actually visible successful Tool call.
    ArtifactFromTool {
        artifact: AgentArtifactLocator,
        artifact_tool_call_id: ToolCallId,
    },
    Artifact {
        artifact_id: ArtifactId,
        artifact_version_id: ArtifactVersionId,
    },
}

/// Namespaces are explicit: a Resource node is not an Artifact identity.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AgentArtifactLocator {
    Artifact(ArtifactId),
    FileNode(ResourceNodeId),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum PublicDeliveryParseError {
    #[error("dialogue delivery response violates its explicit contract")]
    Invalid,
    #[error("dialogue delivery response exceeds its bounded contract")]
    TooLarge,
}

/// Value-free reason retained for a bounded private correction turn.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PublicDeliveryIssue {
    InvalidSchema,
    TooLarge,
    InvalidReference,
}
impl PublicDeliveryIssue {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidSchema => "invalid_schema",
            Self::TooLarge => "too_large",
            Self::InvalidReference => "invalid_reference",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "invalid_schema" => Some(Self::InvalidSchema),
            "too_large" => Some(Self::TooLarge),
            "invalid_reference" => Some(Self::InvalidReference),
            _ => None,
        }
    }
}
impl From<PublicDeliveryParseError> for PublicDeliveryIssue {
    fn from(value: PublicDeliveryParseError) -> Self {
        match value {
            PublicDeliveryParseError::Invalid => Self::InvalidSchema,
            PublicDeliveryParseError::TooLarge => Self::TooLarge,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    deliveries: Vec<Delivery>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Delivery {
    pub(super) kind: String,
    pub(super) purpose: String,
    pub(super) text: NullableString,
    pub(super) interaction_id: NullableString,
    pub(super) artifact_id: NullableString,
    #[serde(rename = "artifact_version_id")]
    pub(super) artifact_handle: NullableString,
    #[serde(skip)]
    pub(super) file_node: bool,
    #[serde(skip)]
    pub(super) partner: Option<(CommandId, ToolCallId)>,
}
/// Nullable fields are still required. Deriving Option directly would silently
/// accept missing fields, contrary to the provider's strict schema.
pub(super) struct NullableString(pub(super) Option<String>);
impl<'de> Deserialize<'de> for NullableString {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = NullableString;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an explicitly present string or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(NullableString(None))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(NullableString(Some(value.to_owned())))
            }
            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(NullableString(Some(value)))
            }
        }
        // deserialize_option also accepts serde's missing-field sentinel; using
        // deserialize_any distinguishes an explicit JSON null from absent data.
        d.deserialize_any(Visitor)
    }
}

pub fn parse_public_delivery(
    bytes: &[u8],
) -> Result<Vec<ParsedPublicDelivery>, PublicDeliveryParseError> {
    parse_for_contract(crate::PublicDeliveryContract::V1, bytes)
}

pub(super) fn parse_for_contract(
    contract: crate::PublicDeliveryContract,
    bytes: &[u8],
) -> Result<Vec<ParsedPublicDelivery>, PublicDeliveryParseError> {
    use PublicDeliveryParseError::{Invalid, TooLarge};
    if bytes.len() > MAX_PUBLIC_DELIVERY_BYTES {
        return Err(TooLarge);
    }
    // All versions reject missing, unknown and duplicate fields. The sealed
    // contract selects the decoder; content cannot upgrade its own authority.
    let envelope = match contract {
        crate::PublicDeliveryContract::V1 => {
            serde_json::from_slice::<Envelope>(bytes).map_err(|_| Invalid)?
        }
        crate::PublicDeliveryContract::V2 | crate::PublicDeliveryContract::V3 => Envelope {
            deliveries: crate::delivery_versions::decode_v2(bytes)?,
        },
        crate::PublicDeliveryContract::V4 => Envelope {
            deliveries: crate::delivery_versions::decode_v4(bytes)?,
        },
        crate::PublicDeliveryContract::V5
        | crate::PublicDeliveryContract::V6
        | crate::PublicDeliveryContract::V7 => Envelope {
            deliveries: crate::delivery_v5::decode(bytes)?,
        },
    };
    if envelope.deliveries.is_empty() || envelope.deliveries.len() > MAX_PUBLIC_DELIVERIES {
        return Err(Invalid);
    }
    let mut seen = BTreeSet::new();
    let mut deliveries = Vec::with_capacity(envelope.deliveries.len());
    for raw in envelope.deliveries {
        let purpose = match raw.purpose.as_str() {
            "reply" => DialoguePublicationPurpose::Reply,
            "result" => DialoguePublicationPurpose::Result,
            "important_update" => DialoguePublicationPurpose::ImportantUpdate,
            "clarification" => DialoguePublicationPurpose::Clarification,
            "required_decision" => DialoguePublicationPurpose::RequiredDecision,
            "blocked" => DialoguePublicationPurpose::Blocked,
            _ => return Err(Invalid),
        };
        if let Some((creation_command_id, partner_tool_call_id)) = raw.partner {
            if !matches!(
                contract,
                crate::PublicDeliveryContract::V4
                    | crate::PublicDeliveryContract::V5
                    | crate::PublicDeliveryContract::V6
                    | crate::PublicDeliveryContract::V7
            ) || raw.kind != "partner_created"
                || purpose != DialoguePublicationPurpose::Result
                || !seen.insert(format!("partner_created:{creation_command_id}"))
            {
                return Err(Invalid);
            }
            deliveries.push(ParsedPublicDelivery {
                purpose,
                content: PublicDeliveryDraft::PartnerCreated {
                    creation_command_id,
                    partner_tool_call_id,
                },
            });
            continue;
        }
        let content = match (
            raw.kind.as_str(),
            raw.text.0,
            raw.interaction_id.0,
            raw.artifact_id.0,
            raw.artifact_handle.0,
        ) {
            ("message", Some(text), None, None, None) => {
                if text.trim().is_empty() {
                    return Err(Invalid);
                }
                if text.len() > MAX_DIALOGUE_MESSAGE_BYTES {
                    return Err(TooLarge);
                }
                if !seen.insert(format!("message:{}", Sha256::digest(text.as_bytes()))) {
                    return Err(Invalid);
                }
                PublicDeliveryDraft::Message(text)
            }
            ("interaction", None, Some(id), None, None) => {
                let id: InteractionId = id.parse().map_err(|_| Invalid)?;
                PublicDeliveryContent::Interaction(id)
                    .validate(purpose)
                    .map_err(|_| Invalid)?;
                if !seen.insert(format!("interaction:{id}")) {
                    return Err(Invalid);
                }
                PublicDeliveryDraft::Interaction(id)
            }
            ("artifact", None, None, Some(id), Some(version)) => {
                let artifact_id: ArtifactId = id.parse().map_err(|_| Invalid)?;
                if !matches!(
                    purpose,
                    DialoguePublicationPurpose::Reply
                        | DialoguePublicationPurpose::Result
                        | DialoguePublicationPurpose::ImportantUpdate
                ) {
                    return Err(Invalid);
                }
                // A single delivery never repeats one file with multiple versions.
                if !seen.insert(format!("artifact:{artifact_id}")) {
                    return Err(Invalid);
                }
                match contract {
                    crate::PublicDeliveryContract::V1 => PublicDeliveryDraft::Artifact {
                        artifact_id,
                        artifact_version_id: version
                            .parse::<ArtifactVersionId>()
                            .map_err(|_| Invalid)?,
                    },
                    crate::PublicDeliveryContract::V2
                    | crate::PublicDeliveryContract::V3
                    | crate::PublicDeliveryContract::V4
                    | crate::PublicDeliveryContract::V5
                    | crate::PublicDeliveryContract::V6
                    | crate::PublicDeliveryContract::V7 => PublicDeliveryDraft::ArtifactFromTool {
                        artifact: if raw.file_node {
                            AgentArtifactLocator::FileNode(
                                ResourceNodeId::from_uuid(artifact_id.into_uuid())
                                    .map_err(|_| Invalid)?,
                            )
                        } else {
                            AgentArtifactLocator::Artifact(artifact_id)
                        },
                        artifact_tool_call_id: ToolCallId::new(version).map_err(|_| Invalid)?,
                    },
                }
            }
            _ => return Err(Invalid),
        };
        deliveries.push(ParsedPublicDelivery { purpose, content });
    }
    Ok(deliveries)
}
