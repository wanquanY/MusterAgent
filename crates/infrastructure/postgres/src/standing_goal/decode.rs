//! Shared primitive decoders for Standing Goal persistence rows.

use std::str::FromStr;

use super::*;

pub(super) fn json(value: &[String]) -> Result<String, PostgresStandingGoalError> {
    serde_json::to_string(value).map_err(|_| PostgresStandingGoalError::Corrupt)
}

pub(super) fn decode_json(
    row: &PgRow,
    field: &str,
) -> Result<Vec<String>, PostgresStandingGoalError> {
    serde_json::from_str(&row.try_get::<String, _>(field)?)
        .map_err(|_| PostgresStandingGoalError::Corrupt)
}

pub(super) fn id<T>(value: uuid::Uuid, field: &'static str) -> Result<T, PostgresStandingGoalError>
where
    T: FromStr,
{
    value
        .to_string()
        .parse()
        .map_err(|_| PostgresStandingGoalError::corrupt(field))
}

pub(super) fn optional_id<T>(
    value: Option<uuid::Uuid>,
    field: &'static str,
) -> Result<Option<T>, PostgresStandingGoalError>
where
    T: FromStr,
{
    value.map(|value| id(value, field)).transpose()
}

pub(super) fn parse_u64(
    value: String,
    field: &'static str,
) -> Result<u64, PostgresStandingGoalError> {
    value
        .parse()
        .map_err(|_| PostgresStandingGoalError::corrupt(field))
}

pub(super) fn digest(
    value: Vec<u8>,
    field: &'static str,
) -> Result<Sha256, PostgresStandingGoalError> {
    Sha256::from_bytes(value).map_err(|_| PostgresStandingGoalError::corrupt(field))
}
