//! Versioned request decoding. New model names resolve through the original
//! complete Run catalog before authorization; legacy exact requests never alias.
use musteragent_discovery::AuthorizedSkillCatalog;
use musteragent_identity::VersionedKey;
use musteragent_loop::LoopPortError;
use musteragent_runtime_contract::ErrorCode;
use serde::Deserialize;

use super::port;

#[derive(Clone, Copy)]
pub(super) enum Interface {
    ExactV1,
    NamedV2,
}

impl Interface {
    pub(super) fn new(version: u32) -> Result<Self, LoopPortError> {
        match version {
            1 => Ok(Self::ExactV1),
            2 => Ok(Self::NamedV2),
            _ => Err(port(ErrorCode::RuntimeSnapshotInvalid, false)),
        }
    }

    pub(super) fn name<'a>(
        self,
        catalog: &'a AuthorizedSkillCatalog,
        reference: &VersionedKey,
    ) -> Result<Option<&'a str>, LoopPortError> {
        match self {
            Self::ExactV1 => Ok(None),
            Self::NamedV2 => catalog
                .model_names()
                .name(reference)
                .map(Some)
                .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false)),
        }
    }

    pub(super) fn view(
        self,
        bytes: &[u8],
        catalog: &AuthorizedSkillCatalog,
    ) -> Result<VersionedKey, LoopPortError> {
        match self {
            Self::ExactV1 => {
                let value: ExactView = decode(bytes)?;
                exact(value.skill_ref)
            }
            Self::NamedV2 => {
                let value: NamedView = decode(bytes)?;
                named(value.skill_name, catalog)
            }
        }
    }

    pub(super) fn resource(
        self,
        bytes: &[u8],
        catalog: &AuthorizedSkillCatalog,
    ) -> Result<(VersionedKey, Option<String>), LoopPortError> {
        match self {
            Self::ExactV1 => {
                let value: ExactResource = decode(bytes)?;
                Ok((exact(value.skill_ref)?, value.path))
            }
            Self::NamedV2 => {
                let value: NamedResource = decode(bytes)?;
                Ok((named(value.skill_name, catalog)?, value.path))
            }
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, LoopPortError> {
    serde_json::from_slice(bytes).map_err(|_| port(ErrorCode::InvalidCommand, false))
}
fn exact(value: String) -> Result<VersionedKey, LoopPortError> {
    VersionedKey::new(value).map_err(|_| port(ErrorCode::InvalidCommand, false))
}
fn named(value: String, catalog: &AuthorizedSkillCatalog) -> Result<VersionedKey, LoopPortError> {
    catalog
        .model_names()
        .resolve(&value)
        .cloned()
        .ok_or_else(|| port(ErrorCode::PolicyDenied, false))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactView {
    skill_ref: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedView {
    skill_name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactResource {
    skill_ref: String,
    path: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedResource {
    skill_name: String,
    path: Option<String>,
}
