//! Bounded read intent, without identity, credentials, storage or permissions.
//! The Host checks explicit caller constraints and resource limits. It does not
//! rerun defaults or query policy from the independently published program.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

use crate::{
    InvalidInput,
    features::{lexical_features, query_search_features},
};

pub const MAX_READ_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_CANDIDATES: u16 = 100;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryStrategy {
    pub index_features: String,
    pub normalized_features: Vec<String>,
    pub candidate_limit: u16,
}

impl QueryStrategy {
    fn prepare(query: &str) -> Result<Self, InvalidInput> {
        let value = Self {
            index_features: query_search_features(query),
            normalized_features: lexical_features(query).into_iter().collect(),
            candidate_limit: MAX_CANDIDATES,
        };
        value.validate_boundary()?;
        Ok(value)
    }

    pub fn validate_boundary(&self) -> Result<(), InvalidInput> {
        if self.index_features.is_empty()
            || self.index_features.len() > MAX_READ_REQUEST_BYTES
            || self.normalized_features.is_empty()
            || self.normalized_features.len() > 4096
            || self
                .normalized_features
                .iter()
                .any(|part| part.is_empty() || part.len() > 4096)
            || self
                .normalized_features
                .iter()
                .map(String::len)
                .sum::<usize>()
                > MAX_READ_REQUEST_BYTES
            || !self
                .normalized_features
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || !(1..=MAX_CANDIDATES).contains(&self.candidate_limit)
        {
            return Err(InvalidInput);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub enum ReadPlan {
    #[serde(rename = "memory_search")]
    MemorySearch {
        query: String,
        limit: u16,
        strategy: QueryStrategy,
    },
    #[serde(rename = "memory_list")]
    MemoryList { limit: u16 },
    #[serde(rename = "memory_get")]
    MemoryGet { memory_id: Uuid },
    #[serde(rename = "experience_search")]
    ExperienceSearch {
        query: String,
        kinds: Vec<String>,
        limit: u16,
    },
    #[serde(rename = "experience_inspect")]
    ExperienceInspect { proposal_id: Uuid },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReadProtocol {
    #[serde(rename = "musteragent.memory.read-plan.v1")]
    V1,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPreparation {
    pub protocol: ReadProtocol,
    pub plan: ReadPlan,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchIntent {
    query: String,
    #[serde(default, deserialize_with = "present")]
    limit: Option<u16>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListIntent {
    #[serde(default, deserialize_with = "present")]
    limit: Option<u16>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GetIntent {
    memory_id: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExperienceIntent {
    query: String,
    #[serde(default, deserialize_with = "present")]
    kinds: Option<Vec<String>>,
    #[serde(default, deserialize_with = "present")]
    limit: Option<u16>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectIntent {
    proposal_id: Uuid,
}

fn present<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, InvalidInput> {
    if bytes.is_empty() || bytes.len() > MAX_READ_REQUEST_BYTES {
        return Err(InvalidInput);
    }
    serde_json::from_slice(bytes).map_err(|_| InvalidInput)
}
fn limit(value: Option<u16>) -> Result<u16, InvalidInput> {
    let value = value.unwrap_or(5);
    if (1..=20).contains(&value) {
        Ok(value)
    } else {
        Err(InvalidInput)
    }
}
fn query(value: String) -> Result<String, InvalidInput> {
    if value.trim().is_empty() || value.len() > 1000 {
        Err(InvalidInput)
    } else {
        Ok(value)
    }
}
fn kinds(values: &[String]) -> Result<(), InvalidInput> {
    const KINDS: &[&str] = &[
        "semantic",
        "procedure",
        "failure",
        "preference",
        "policy_proposal",
        "skill",
        "instinct",
        "persona_patch",
    ];
    if values.len() > 5
        || values.iter().collect::<BTreeSet<_>>().len() != values.len()
        || values.iter().any(|value| !KINDS.contains(&value.as_str()))
    {
        return Err(InvalidInput);
    }
    Ok(())
}
fn identity(value: Uuid) -> Result<Uuid, InvalidInput> {
    if value.is_nil() {
        Err(InvalidInput)
    } else {
        Ok(value)
    }
}
fn explicit_limit(requested: Option<u16>, selected: u16) -> bool {
    (1..=MAX_CANDIDATES).contains(&selected) && requested.is_none_or(|value| value == selected)
}

impl ReadPreparation {
    pub fn prepare(key: &str, bytes: &[u8]) -> Result<Self, InvalidInput> {
        let plan = match key {
            "memory.search@1" => {
                let value: SearchIntent = decode(bytes)?;
                let query = query(value.query)?;
                ReadPlan::MemorySearch {
                    strategy: QueryStrategy::prepare(&query)?,
                    query,
                    limit: limit(value.limit)?,
                }
            }
            "memory.list@1" => ReadPlan::MemoryList {
                limit: limit(decode::<ListIntent>(bytes)?.limit)?,
            },
            "memory.get@1" => ReadPlan::MemoryGet {
                memory_id: identity(decode::<GetIntent>(bytes)?.memory_id)?,
            },
            "experience.search@1" => {
                let value: ExperienceIntent = decode(bytes)?;
                let selected = value.kinds.unwrap_or_default();
                kinds(&selected)?;
                ReadPlan::ExperienceSearch {
                    query: query(value.query)?,
                    kinds: selected,
                    limit: limit(value.limit)?,
                }
            }
            "experience.inspect@1" => ReadPlan::ExperienceInspect {
                proposal_id: identity(decode::<InspectIntent>(bytes)?.proposal_id)?,
            },
            _ => return Err(InvalidInput),
        };
        let result = Self {
            protocol: ReadProtocol::V1,
            plan,
        };
        result.canonical_bytes()?;
        Ok(result)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, InvalidInput> {
        let bytes = serde_json::to_vec(self).map_err(|_| InvalidInput)?;
        if bytes.len() > MAX_READ_REQUEST_BYTES {
            return Err(InvalidInput);
        }
        Ok(bytes)
    }

    /// Caller-chosen targets, filters, text and explicit limits cannot change.
    /// An omitted default and query features remain package business policy.
    pub fn decode_for_request(
        key: &str,
        request: &[u8],
        bytes: &[u8],
    ) -> Result<Self, InvalidInput> {
        let prepared: Self = decode(bytes)?;
        if prepared.canonical_bytes()? != bytes {
            return Err(InvalidInput);
        }
        let matches = match (key, &prepared.plan) {
            (
                "memory.search@1",
                ReadPlan::MemorySearch {
                    query,
                    limit,
                    strategy,
                },
            ) => {
                let original: SearchIntent = decode(request)?;
                strategy.validate_boundary()?;
                *query == original.query && explicit_limit(original.limit, *limit)
            }
            ("memory.list@1", ReadPlan::MemoryList { limit }) => {
                explicit_limit(decode::<ListIntent>(request)?.limit, *limit)
            }
            ("memory.get@1", ReadPlan::MemoryGet { memory_id }) => {
                *memory_id == identity(decode::<GetIntent>(request)?.memory_id)?
            }
            (
                "experience.search@1",
                ReadPlan::ExperienceSearch {
                    query,
                    kinds: selected,
                    limit,
                },
            ) => {
                let original: ExperienceIntent = decode(request)?;
                kinds(selected)?;
                *query == original.query
                    && original.kinds.is_none_or(|value| value == *selected)
                    && explicit_limit(original.limit, *limit)
            }
            ("experience.inspect@1", ReadPlan::ExperienceInspect { proposal_id }) => {
                *proposal_id == identity(decode::<InspectIntent>(request)?.proposal_id)?
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
