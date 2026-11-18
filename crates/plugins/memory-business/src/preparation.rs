//! Closed business decisions for Memory writes. These bytes never authorize or
//! commit a write. Host-only identity, approved interactions, head/review facts,
//! timestamps and fenced mutations must not be accepted through this protocol.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{InvalidInput, proposal::ProposalInput, user_memory_document::UserMemoryDocument};

pub const MAX_PREPARATION_BYTES: usize = 1024 * 1024;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum PreparationProtocol {
    #[serde(rename = "musteragent.memory.write-plan.v1")]
    V1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "input", deny_unknown_fields)]
pub enum WritePlan {
    #[serde(rename = "user_memory.remember@2")]
    Remember(UserMemoryDocument),
    #[serde(rename = "experience.propose@2")]
    ProposeV2(ProposalInput),
    #[serde(rename = "experience.propose@3")]
    ProposeV3(ProposalInput),
    #[serde(rename = "experience.publish@1")]
    Publish(PublishInput),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishInput {
    pub proposal_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritePreparation {
    pub protocol: PreparationProtocol,
    pub plan: WritePlan,
}

impl WritePreparation {
    pub fn prepare(exact_tool_key: &str, request: &[u8]) -> Result<Self, InvalidInput> {
        if request.len() > MAX_PREPARATION_BYTES {
            return Err(InvalidInput);
        }
        let plan = match exact_tool_key {
            "user_memory.remember@2" => {
                WritePlan::Remember(UserMemoryDocument::decode(request).map_err(|_| InvalidInput)?)
            }
            "experience.propose@2" => {
                WritePlan::ProposeV2(ProposalInput::decode(exact_tool_key, request)?)
            }
            "experience.propose@3" => {
                WritePlan::ProposeV3(ProposalInput::decode(exact_tool_key, request)?)
            }
            "experience.publish@1" => {
                let input: PublishInput =
                    serde_json::from_slice(request).map_err(|_| InvalidInput)?;
                if input.proposal_id.is_nil() {
                    return Err(InvalidInput);
                }
                WritePlan::Publish(input)
            }
            _ => return Err(InvalidInput),
        };
        let prepared = Self {
            protocol: PreparationProtocol::V1,
            plan,
        };
        // A request at the byte limit can expand when normalized or wrapped.
        // Reject before the Host persists a plan it cannot subsequently read.
        prepared.canonical_bytes()?;
        Ok(prepared)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, InvalidInput> {
        let bytes = serde_json::to_vec(self).map_err(|_| InvalidInput)?;
        if bytes.len() > MAX_PREPARATION_BYTES {
            return Err(InvalidInput);
        }
        Ok(bytes)
    }

    /// Validate the stable Host ABI against the original request. Do not rerun
    /// package policy here: an independently released package may change a v3
    /// candidate's descriptive purpose without changing its authority or claim.
    pub fn decode_for_request(
        exact_tool_key: &str,
        request: &[u8],
        bytes: &[u8],
    ) -> Result<Self, InvalidInput> {
        if bytes.len() > MAX_PREPARATION_BYTES || request.len() > MAX_PREPARATION_BYTES {
            return Err(InvalidInput);
        }
        let prepared: Self = serde_json::from_slice(bytes).map_err(|_| InvalidInput)?;
        if prepared.canonical_bytes()? != bytes {
            return Err(InvalidInput);
        }
        let matches = match (exact_tool_key, &prepared.plan) {
            ("user_memory.remember@2", WritePlan::Remember(input)) => {
                *input == UserMemoryDocument::decode(request).map_err(|_| InvalidInput)?
            }
            ("experience.propose@2", WritePlan::ProposeV2(input)) => {
                input.matches_request(exact_tool_key, request)?
            }
            ("experience.propose@3", WritePlan::ProposeV3(input)) => {
                input.matches_request(exact_tool_key, request)?
            }
            ("experience.publish@1", WritePlan::Publish(input)) => {
                let original: PublishInput =
                    serde_json::from_slice(request).map_err(|_| InvalidInput)?;
                !original.proposal_id.is_nil() && original == *input
            }
            _ => false,
        };
        if !matches {
            return Err(InvalidInput);
        }
        Ok(prepared)
    }
}

#[cfg(test)]
mod tests;
