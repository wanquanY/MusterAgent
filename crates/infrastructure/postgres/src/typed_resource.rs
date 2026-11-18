use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ActivityId, ArtifactId, ContentId, ContextSnapshotId, ConversationId, ErasureJobId,
    ErasureReceiptId, InteractionId, MessageId, RunId,
};
use musteragent_runtime_contract::ResourceRef;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use thiserror::Error;

pub(crate) struct ContentColumns {
    pub(crate) content_id: uuid::Uuid,
    pub(crate) media_type: String,
    pub(crate) byte_length: String,
    pub(crate) sha256: Vec<u8>,
}

impl From<&ContentRef> for ContentColumns {
    fn from(value: &ContentRef) -> Self {
        Self {
            content_id: value.content_id.into_uuid(),
            media_type: value.media_type.as_str().to_owned(),
            byte_length: value.byte_length.to_string(),
            sha256: value.sha256.into_bytes().to_vec(),
        }
    }
}

pub(crate) struct ResourceColumns {
    pub(crate) kind: i16,
    pub(crate) resource_id: Option<uuid::Uuid>,
    pub(crate) content: Option<ContentColumns>,
}

impl From<&ResourceRef> for ResourceColumns {
    fn from(value: &ResourceRef) -> Self {
        let (kind, resource_id, content) = match value {
            ResourceRef::Conversation(value) => (1, Some(value.into_uuid()), None),
            ResourceRef::Message(value) => (2, Some(value.into_uuid()), None),
            ResourceRef::Activity(value) => (3, Some(value.into_uuid()), None),
            ResourceRef::Run(value) => (4, Some(value.into_uuid()), None),
            ResourceRef::Interaction(value) => (5, Some(value.into_uuid()), None),
            ResourceRef::Artifact(value) => (6, Some(value.into_uuid()), None),
            ResourceRef::Content(value) => (7, None, Some(ContentColumns::from(value))),
            ResourceRef::ContextSnapshot(value) => (8, Some(value.into_uuid()), None),
            ResourceRef::ErasureJob(value) => (9, Some(value.into_uuid()), None),
            ResourceRef::ErasureReceipt(value) => (10, Some(value.into_uuid()), None),
        };
        Self {
            kind,
            resource_id,
            content,
        }
    }
}

pub(crate) fn decode_resource(row: &PgRow) -> Result<ResourceRef, TypedResourceError> {
    let kind: i16 = row.try_get("resource_kind")?;
    if kind == 7 {
        return Ok(ResourceRef::Content(decode_content(row, "")?.ok_or(
            TypedResourceError::CorruptState("content resource is incomplete"),
        )?));
    }
    let id: uuid::Uuid = row.try_get::<Option<uuid::Uuid>, _>("resource_id")?.ok_or(
        TypedResourceError::CorruptState("typed resource id is missing"),
    )?;
    match kind {
        1 => ConversationId::from_uuid(id)
            .map(ResourceRef::Conversation)
            .map_err(|_| TypedResourceError::CorruptState("invalid conversation id")),
        2 => MessageId::from_uuid(id)
            .map(ResourceRef::Message)
            .map_err(|_| TypedResourceError::CorruptState("invalid message id")),
        3 => ActivityId::from_uuid(id)
            .map(ResourceRef::Activity)
            .map_err(|_| TypedResourceError::CorruptState("invalid activity id")),
        4 => RunId::from_uuid(id)
            .map(ResourceRef::Run)
            .map_err(|_| TypedResourceError::CorruptState("invalid run id")),
        5 => InteractionId::from_uuid(id)
            .map(ResourceRef::Interaction)
            .map_err(|_| TypedResourceError::CorruptState("invalid interaction id")),
        6 => ArtifactId::from_uuid(id)
            .map(ResourceRef::Artifact)
            .map_err(|_| TypedResourceError::CorruptState("invalid artifact id")),
        8 => ContextSnapshotId::from_uuid(id)
            .map(ResourceRef::ContextSnapshot)
            .map_err(|_| TypedResourceError::CorruptState("invalid snapshot id")),
        9 => ErasureJobId::from_uuid(id)
            .map(ResourceRef::ErasureJob)
            .map_err(|_| TypedResourceError::CorruptState("invalid erasure job id")),
        10 => ErasureReceiptId::from_uuid(id)
            .map(ResourceRef::ErasureReceipt)
            .map_err(|_| TypedResourceError::CorruptState("invalid erasure receipt id")),
        _ => Err(TypedResourceError::CorruptState("unknown resource kind")),
    }
}

pub(crate) fn decode_content(
    row: &PgRow,
    prefix: &str,
) -> Result<Option<ContentRef>, TypedResourceError> {
    let content_id_column = content_column(prefix, "content_id");
    let media_type_column = content_column(prefix, "media_type");
    let byte_length_column = content_column(prefix, "byte_length");
    let sha256_column = content_column(prefix, "sha256");
    let content_id = row.try_get::<Option<uuid::Uuid>, _>(content_id_column.as_str())?;
    let Some(content_id) = content_id else {
        return Ok(None);
    };
    let media_type = row
        .try_get::<Option<String>, _>(media_type_column.as_str())?
        .ok_or(TypedResourceError::CorruptState(
            "content media type is missing",
        ))?;
    let byte_length = row
        .try_get::<Option<String>, _>(byte_length_column.as_str())?
        .ok_or(TypedResourceError::CorruptState(
            "content byte length is missing",
        ))?
        .parse::<u64>()
        .map_err(|_| TypedResourceError::CorruptState("invalid content byte length"))?;
    let digest = row
        .try_get::<Option<Vec<u8>>, _>(sha256_column.as_str())?
        .ok_or(TypedResourceError::CorruptState(
            "content digest is missing",
        ))?;
    Ok(Some(ContentRef {
        content_id: ContentId::from_uuid(content_id)
            .map_err(|_| TypedResourceError::CorruptState("invalid content id"))?,
        media_type: MediaType::new(media_type)
            .map_err(|_| TypedResourceError::CorruptState("invalid media type"))?,
        byte_length,
        sha256: Sha256::from_bytes(digest)
            .map_err(|_| TypedResourceError::CorruptState("invalid content digest"))?,
    }))
}

fn content_column(prefix: &str, field: &str) -> String {
    if prefix.is_empty() {
        field.to_owned()
    } else {
        format!("{prefix}_{field}")
    }
}

#[derive(Debug, Error)]
pub(crate) enum TypedResourceError {
    #[error("corrupt typed resource state: {0}")]
    CorruptState(&'static str),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
