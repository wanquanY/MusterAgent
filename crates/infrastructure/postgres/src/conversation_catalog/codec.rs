//! Storage identity and projection value codecs for the Conversation Catalog.

use super::*;

pub(super) fn storage_uuid(
    identity: CatalogSourceIdentity,
) -> Result<Uuid, PostgresConversationCatalogError> {
    identity
        .storage_identity()
        .parse()
        .map_err(|_| PostgresConversationCatalogError::CorruptProjection)
}

pub(super) fn storage_action_uuid(
    identity: CatalogActionIdentity,
) -> Result<Uuid, PostgresConversationCatalogError> {
    identity
        .storage_identity()
        .parse()
        .map_err(|_| PostgresConversationCatalogError::CorruptProjection)
}

pub(super) fn optional_dotted(
    value: Option<String>,
) -> Result<Option<DottedKey>, PostgresConversationCatalogError> {
    value
        .map(|item| {
            DottedKey::new(item).map_err(|_| PostgresConversationCatalogError::CorruptProjection)
        })
        .transpose()
}

pub(super) fn required_strong_id<T: std::str::FromStr>(
    value: Uuid,
) -> Result<T, PostgresConversationCatalogError> {
    value
        .to_string()
        .parse()
        .map_err(|_| PostgresConversationCatalogError::CorruptProjection)
}

pub(super) fn optional_strong_id<T: std::str::FromStr>(
    value: Option<Uuid>,
) -> Result<Option<T>, PostgresConversationCatalogError> {
    value.map(required_strong_id).transpose()
}

pub(super) fn parse_u64(value: String) -> Result<u64, PostgresConversationCatalogError> {
    value
        .parse()
        .map_err(|_| PostgresConversationCatalogError::CorruptProjection)
}

pub(super) fn decode_counts(
    row: &sqlx::postgres::PgRow,
    prefix: &str,
) -> Result<musteragent_projection::CatalogExecutionCounts, PostgresConversationCatalogError> {
    Ok(musteragent_projection::CatalogExecutionCounts {
        queued: count_u32(row.try_get(format!("{prefix}_queued").as_str())?)?,
        running: count_u32(row.try_get(format!("{prefix}_running").as_str())?)?,
        waiting: count_u32(row.try_get(format!("{prefix}_waiting").as_str())?)?,
        cancelling: count_u32(row.try_get(format!("{prefix}_cancelling").as_str())?)?,
    })
}

pub(super) fn count_u32(value: i64) -> Result<u32, PostgresConversationCatalogError> {
    u32::try_from(value).map_err(|_| PostgresConversationCatalogError::CorruptProjection)
}

pub(super) fn decode_owned_outcome(
    kind: Option<i16>,
    error_class: Option<String>,
    recoverability: Option<i16>,
) -> Result<Option<ConversationOwnedOutcomeSummaryV2>, PostgresConversationCatalogError> {
    kind.map(|value| {
        Ok(ConversationOwnedOutcomeSummaryV2 {
            kind: ExecutionOutcomeKind::from_storage_code(value)
                .ok_or(PostgresConversationCatalogError::CorruptProjection)?,
            error_class,
            recoverability: recoverability
                .map(|item| {
                    CatalogRecoverability::from_storage_code(item)
                        .ok_or(PostgresConversationCatalogError::CorruptProjection)
                })
                .transpose()?,
        })
    })
    .transpose()
}

pub(super) fn decode_title(
    value: Option<String>,
) -> Result<Option<DisplayText>, PostgresConversationCatalogError> {
    value
        .filter(|item| !item.is_empty())
        .map(|item| {
            DisplayText::new(item).map_err(|_| PostgresConversationCatalogError::CorruptProjection)
        })
        .transpose()
}
