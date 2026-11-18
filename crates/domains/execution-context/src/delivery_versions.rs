//! Immutable output versions: new admissions pin CURRENT, admitted Runs keep
//! their sealed prompt. Tool-call handles are resolved only by the Artifact owner.
use crate::delivery_contract::*;
use musteragent_content_ref::{ContentRef, Sha256};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PublicDeliveryContract {
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
}
impl PublicDeliveryContract {
    pub const CURRENT: Self = Self::V7;
    pub const fn key(self) -> &'static str {
        match self {
            Self::V1 => PUBLIC_DELIVERY_CONTRACT_KEY,
            Self::V2 => "musteragent.public-delivery@2",
            Self::V3 => "musteragent.public-delivery@3",
            Self::V4 => "musteragent.public-delivery@4",
            Self::V5 => "musteragent.public-delivery@5",
            Self::V6 => "musteragent.public-delivery@6",
            Self::V7 => "musteragent.public-delivery@7",
        }
    }
    /// Whether a model delivery names the original successful Tool receipt.
    /// Exhaustive matching forces each new immutable contract to qualify its
    /// reference projection, rather than silently falling through in storage.
    pub const fn uses_tool_result_delivery_references(self) -> bool {
        match self {
            Self::V1 => false,
            Self::V2 | Self::V3 | Self::V4 | Self::V5 | Self::V6 | Self::V7 => true,
        }
    }
    pub const fn schema(self) -> &'static str {
        match self {
            Self::V1 => PUBLIC_DELIVERY_SCHEMA,
            Self::V2 => include_str!("delivery_schema_v2.json"),
            Self::V3 => include_str!("delivery_schema_v3.json"),
            Self::V4 => include_str!("delivery_schema_v4.json"),
            Self::V5 | Self::V6 | Self::V7 => include_str!("delivery_schema_v5.json"),
        }
    }
    pub fn hash(self) -> Sha256 {
        Sha256::digest(self.schema().as_bytes())
    }
    pub fn prompt(self) -> String {
        match self {
            Self::V1 => public_delivery_prompt(),
            Self::V2 => format!(
                "{}\nFor a simple question, answer directly from the current user request and the supplied public conversation history when they contain the answer. Request an interaction only when completing the task genuinely requires a missing user choice or information; ask a concrete, complete question identifying the decision, object and available options. Never create a placeholder or an objectless confirmation. Security approval must be triggered by the original owner of an actual protected operation; a generic confirm interaction does not grant permission.\nFor an artifact delivery, copy an actual successful Tool result into artifact_ref. Use {{kind:'file_node',id:<file_node_id>}} for resource.file.create; use {{kind:'artifact',id:<file_id or note_id or artifact.artifact_id>}} when the Tool returns an Artifact identity. These namespaces are distinct: never relabel a file_node_id as an artifact id. Set artifact_tool_call_id to that Tool call's exact tool_call_id from the Tool message. Choose the successful creation, update, or export call that committed the desired artifact, not a read/search call. Runtime resolves the exact committed version; never search for or invent an internal artifact_version_id, hash, or storage path. This applies to documents, notes, presentations, images, video and audio. One Tool call may produce several artifacts: specify each returned artifact_ref with that same tool_call_id.\nOutput contract: {}\nSchema:\n{}",
                public_delivery_instruction(),
                self.key(),
                self.schema()
            ),
            Self::V3 => format!(
                "{}\nOutput contract: {}\nSchema:\n{}",
                include_str!("delivery_instruction_v3.txt").trim_end(),
                self.key(),
                self.schema()
            ),
            Self::V4 => format!(
                "{}\nOutput contract: {}\nSchema:\n{}",
                include_str!("delivery_instruction_v4.txt").trim_end(),
                self.key(),
                self.schema()
            ),
            Self::V5 => format!(
                "{}\nOutput contract: {}\nSchema:\n{}",
                include_str!("delivery_instruction_v5.txt").trim_end(),
                self.key(),
                self.schema()
            ),
            Self::V6 => format!(
                "{}\nPrivate repair contract: {}\nPrivate repair material SHA-256: {}\nOutput contract: {}\nSchema:\n{}",
                include_str!("delivery_instruction_v5.txt").trim_end(),
                crate::PUBLIC_DELIVERY_REPAIR_REF,
                crate::public_delivery_repair_hash(),
                self.key(),
                self.schema()
            ),
            Self::V7 => format!(
                "{}\nPrivate recovery contract: {}\nPrivate recovery material SHA-256: {}\nOutput contract: {}\nSchema:\n{}",
                include_str!("delivery_instruction_v5.txt").trim_end(),
                crate::PUBLIC_DELIVERY_RECOVERY_V7_REF,
                crate::public_delivery_recovery_v7_hash(),
                self.key(),
                self.schema()
            ),
        }
    }
    pub fn from_key(key: &str) -> Option<Self> {
        [
            Self::V1,
            Self::V2,
            Self::V3,
            Self::V4,
            Self::V5,
            Self::V6,
            Self::V7,
        ]
        .into_iter()
        .find(|v| v.key() == key)
    }
    pub fn from_prompt_ref(reference: &ContentRef) -> Option<Self> {
        [
            Self::V1,
            Self::V2,
            Self::V3,
            Self::V4,
            Self::V5,
            Self::V6,
            Self::V7,
        ]
        .into_iter()
        .find(|v| {
            let prompt = v.prompt();
            reference.sha256 == Sha256::digest(prompt.as_bytes())
                && reference.byte_length == prompt.len() as u64
        })
    }
    pub fn parse(
        self,
        bytes: &[u8],
    ) -> Result<Vec<ParsedPublicDelivery>, PublicDeliveryParseError> {
        parse_for_contract(self, bytes)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeV2 {
    deliveries: Vec<DeliveryV2>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryV2 {
    kind: String,
    purpose: String,
    text: NullableString,
    interaction_id: NullableString,
    artifact_ref: NullableArtifact,
    artifact_tool_call_id: NullableString,
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
// Option directly would accept a missing field; the wrapper keeps null explicit.
struct NullableArtifact(Option<ArtifactRef>);
impl<'de> Deserialize<'de> for NullableArtifact {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = NullableArtifact;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an explicit artifact locator or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(NullableArtifact(None))
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                m: M,
            ) -> Result<Self::Value, M::Error> {
                ArtifactRef::deserialize(serde::de::value::MapAccessDeserializer::new(m))
                    .map(|v| NullableArtifact(Some(v)))
            }
        }
        d.deserialize_any(Visitor)
    }
}
pub(super) fn decode_v2(bytes: &[u8]) -> Result<Vec<Delivery>, PublicDeliveryParseError> {
    let envelope: EnvelopeV2 =
        serde_json::from_slice(bytes).map_err(|_| PublicDeliveryParseError::Invalid)?;
    Ok(envelope
        .deliveries
        .into_iter()
        .map(|v| {
            let (id, file_node) = match v.artifact_ref.0 {
                None => (None, false),
                Some(ArtifactRef { kind, id }) => (Some(id), matches!(kind, LocatorKind::FileNode)),
            };
            Delivery {
                kind: v.kind,
                purpose: v.purpose,
                text: v.text,
                interaction_id: v.interaction_id,
                artifact_id: NullableString(id),
                artifact_handle: v.artifact_tool_call_id,
                file_node,
                partner: None,
            }
        })
        .collect())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeV4 {
    deliveries: Vec<DeliveryV4>,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum DeliveryV4 {
    Existing(DeliveryV2),
    Partner(PartnerDelivery),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PartnerDelivery {
    kind: String,
    purpose: String,
    creation_command_id: String,
    partner_tool_call_id: String,
}
pub(super) fn decode_v4(bytes: &[u8]) -> Result<Vec<Delivery>, PublicDeliveryParseError> {
    use musteragent_identity::{CommandId, ToolCallId};
    let envelope: EnvelopeV4 =
        serde_json::from_slice(bytes).map_err(|_| PublicDeliveryParseError::Invalid)?;
    envelope
        .deliveries
        .into_iter()
        .map(|entry| {
            Ok(match entry {
                DeliveryV4::Existing(v) => {
                    let (id, file_node) = match v.artifact_ref.0 {
                        None => (None, false),
                        Some(ArtifactRef { kind, id }) => {
                            (Some(id), matches!(kind, LocatorKind::FileNode))
                        }
                    };
                    Delivery {
                        kind: v.kind,
                        purpose: v.purpose,
                        text: v.text,
                        interaction_id: v.interaction_id,
                        artifact_id: NullableString(id),
                        artifact_handle: v.artifact_tool_call_id,
                        file_node,
                        partner: None,
                    }
                }
                DeliveryV4::Partner(v) => Delivery {
                    kind: v.kind,
                    purpose: v.purpose,
                    text: NullableString(None),
                    interaction_id: NullableString(None),
                    artifact_id: NullableString(None),
                    artifact_handle: NullableString(None),
                    file_node: false,
                    partner: Some((
                        v.creation_command_id
                            .parse::<CommandId>()
                            .map_err(|_| PublicDeliveryParseError::Invalid)?,
                        ToolCallId::new(v.partner_tool_call_id)
                            .map_err(|_| PublicDeliveryParseError::Invalid)?,
                    )),
                },
            })
        })
        .collect()
}

#[cfg(test)]
mod delivery_reference_contract_tests {
    use super::PublicDeliveryContract;

    #[test]
    fn reference_projection_matches_each_immutable_output_schema() {
        use PublicDeliveryContract::*;
        fn requires_tool_reference(value: &serde_json::Value) -> bool {
            match value {
                serde_json::Value::Object(fields) => {
                    fields
                        .get("required")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|required| {
                            required
                                .iter()
                                .any(|field| field == "artifact_tool_call_id")
                        })
                        || fields.values().any(requires_tool_reference)
                }
                serde_json::Value::Array(values) => values.iter().any(requires_tool_reference),
                _ => false,
            }
        }
        for contract in [V1, V2, V3, V4, V5, V6, V7] {
            let schema = serde_json::from_str(contract.schema()).unwrap();
            assert_eq!(
                contract.uses_tool_result_delivery_references(),
                requires_tool_reference(&schema),
                "{}",
                contract.key()
            );
        }
    }
}
