use std::str::FromStr;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{Actor, IdentityError, PrincipalKind, UtcTimestamp};
use musteragent_runtime_contract::ResourceRef;
use prost_types::Timestamp;
use thiserror::Error;

use crate::wire;

const MILLIS_PER_SECOND: i64 = 1_000;
const NANOS_PER_MILLISECOND: i32 = 1_000_000;

pub fn actor(value: Actor) -> wire::Actor {
    wire::Actor {
        tenant_id: Some(wire::TenantId {
            value: value.tenant_id.to_string(),
        }),
        principal_id: Some(wire::PrincipalId {
            value: value.principal_id.to_string(),
        }),
        principal_kind: principal_kind(value.principal_kind) as i32,
    }
}

pub fn decode_actor(value: Option<wire::Actor>) -> Result<Actor, WireContractError> {
    let value = value.ok_or(WireContractError::InvalidContract)?;
    Ok(Actor {
        tenant_id: parse_id(value.tenant_id.map(|value| value.value))?,
        principal_id: parse_id(value.principal_id.map(|value| value.value))?,
        principal_kind: decode_principal_kind(value.principal_kind)?,
    })
}

pub fn principal_kind(value: PrincipalKind) -> wire::PrincipalKind {
    match value {
        PrincipalKind::User => wire::PrincipalKind::User,
        PrincipalKind::Service => wire::PrincipalKind::Service,
        PrincipalKind::System => wire::PrincipalKind::System,
    }
}

pub fn decode_principal_kind(value: i32) -> Result<PrincipalKind, WireContractError> {
    match wire::PrincipalKind::try_from(value) {
        Ok(wire::PrincipalKind::User) => Ok(PrincipalKind::User),
        Ok(wire::PrincipalKind::Service) => Ok(PrincipalKind::Service),
        Ok(wire::PrincipalKind::System) => Ok(PrincipalKind::System),
        _ => Err(WireContractError::InvalidContract),
    }
}

pub fn resource_ref(value: &ResourceRef) -> wire::ResourceRef {
    use wire::resource_ref::Resource;
    let resource = match value {
        ResourceRef::Conversation(value) => Resource::ConversationId(wire::ConversationId {
            value: value.to_string(),
        }),
        ResourceRef::Message(value) => Resource::MessageId(wire::MessageId {
            value: value.to_string(),
        }),
        ResourceRef::Activity(value) => Resource::ActivityId(wire::ActivityId {
            value: value.to_string(),
        }),
        ResourceRef::Run(value) => Resource::RunId(wire::RunId {
            value: value.to_string(),
        }),
        ResourceRef::Interaction(value) => Resource::InteractionId(wire::InteractionId {
            value: value.to_string(),
        }),
        ResourceRef::Artifact(value) => Resource::ArtifactId(wire::ArtifactId {
            value: value.to_string(),
        }),
        ResourceRef::Content(value) => Resource::Content(content_ref(value)),
        ResourceRef::ContextSnapshot(value) => {
            Resource::ContextSnapshotId(wire::ContextSnapshotId {
                value: value.to_string(),
            })
        }
        ResourceRef::ErasureJob(value) => Resource::ErasureJobId(wire::ErasureJobId {
            value: value.to_string(),
        }),
        ResourceRef::ErasureReceipt(value) => Resource::ErasureReceiptId(wire::ErasureReceiptId {
            value: value.to_string(),
        }),
    };
    wire::ResourceRef {
        resource: Some(resource),
    }
}

pub fn decode_resource_ref(value: wire::ResourceRef) -> Result<ResourceRef, WireContractError> {
    use wire::resource_ref::Resource;
    match value.resource.ok_or(WireContractError::InvalidContract)? {
        Resource::ConversationId(value) => Ok(ResourceRef::Conversation(value.value.parse()?)),
        Resource::MessageId(value) => Ok(ResourceRef::Message(value.value.parse()?)),
        Resource::ActivityId(value) => Ok(ResourceRef::Activity(value.value.parse()?)),
        Resource::RunId(value) => Ok(ResourceRef::Run(value.value.parse()?)),
        Resource::InteractionId(value) => Ok(ResourceRef::Interaction(value.value.parse()?)),
        Resource::ArtifactId(value) => Ok(ResourceRef::Artifact(value.value.parse()?)),
        Resource::Content(value) => Ok(ResourceRef::Content(decode_content_ref(value)?)),
        Resource::ContextSnapshotId(value) => {
            Ok(ResourceRef::ContextSnapshot(value.value.parse()?))
        }
        Resource::ErasureJobId(value) => Ok(ResourceRef::ErasureJob(value.value.parse()?)),
        Resource::ErasureReceiptId(value) => Ok(ResourceRef::ErasureReceipt(value.value.parse()?)),
    }
}

pub fn content_ref(value: &ContentRef) -> wire::ContentRef {
    wire::ContentRef {
        content_id: Some(wire::ContentId {
            value: value.content_id.to_string(),
        }),
        media_type: value.media_type.as_str().to_owned(),
        byte_length: value.byte_length,
        sha256: Some(sha256(value.sha256)),
    }
}

pub fn decode_content_ref(value: wire::ContentRef) -> Result<ContentRef, WireContractError> {
    Ok(ContentRef {
        content_id: parse_id(value.content_id.map(|value| value.value))?,
        media_type: MediaType::new(value.media_type)?,
        byte_length: value.byte_length,
        sha256: decode_sha256(value.sha256)?,
    })
}

pub fn sha256(value: Sha256) -> wire::Sha256 {
    wire::Sha256 {
        value: value.into_bytes().to_vec(),
    }
}

pub fn decode_sha256(value: Option<wire::Sha256>) -> Result<Sha256, WireContractError> {
    Ok(Sha256::from_bytes(
        value.ok_or(WireContractError::InvalidContract)?.value,
    )?)
}

pub fn timestamp(value: UtcTimestamp) -> Timestamp {
    let millis = value.unix_millis();
    Timestamp {
        seconds: millis.div_euclid(MILLIS_PER_SECOND),
        nanos: i32::try_from(millis.rem_euclid(MILLIS_PER_SECOND))
            .expect("millisecond remainder fits i32")
            * NANOS_PER_MILLISECOND,
    }
}

pub fn decode_timestamp(value: Option<Timestamp>) -> Result<UtcTimestamp, WireContractError> {
    let value = value.ok_or(WireContractError::InvalidContract)?;
    if !(0..1_000_000_000).contains(&value.nanos) || value.nanos % NANOS_PER_MILLISECOND != 0 {
        return Err(WireContractError::InvalidContract);
    }
    let millis = value
        .seconds
        .checked_mul(MILLIS_PER_SECOND)
        .and_then(|seconds| seconds.checked_add(i64::from(value.nanos / NANOS_PER_MILLISECOND)))
        .ok_or(WireContractError::InvalidContract)?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}

pub fn parse_id<T>(value: Option<String>) -> Result<T, WireContractError>
where
    T: FromStr<Err = IdentityError>,
{
    value
        .ok_or(WireContractError::InvalidContract)?
        .parse()
        .map_err(WireContractError::from)
}

#[derive(Debug, Error)]
pub enum WireContractError {
    #[error("protobuf value violates the shared closed wire contract")]
    InvalidContract,
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    MediaType(#[from] musteragent_content_ref::MediaTypeError),
    #[error(transparent)]
    Digest(#[from] musteragent_content_ref::DigestLengthError),
}
