//! Shared wire scalar contracts. These decode structure; they do not authenticate it.
use musteragent_identity::{IdentityError, UtcTimestamp};
use prost_types::Timestamp;
use std::str::FromStr;
use thiserror::Error;
const NANOS_PER_MILLISECOND: i32 = 1_000_000;
const MILLIS_PER_SECOND: i64 = 1_000;
#[derive(Debug, Error)]
pub enum WireDecodeError {
    #[error("required field is missing: {0}")]
    Missing(&'static str),
    #[error("invalid field {field}: {reason}")]
    Invalid { field: &'static str, reason: String },
    #[error("unsupported enum value for {field}: {value}")]
    UnsupportedEnum { field: &'static str, value: i32 },
}

impl WireDecodeError {
    pub fn field(&self) -> &'static str {
        match self {
            Self::Missing(field)
            | Self::Invalid { field, .. }
            | Self::UnsupportedEnum { field, .. } => field,
        }
    }
}
pub fn invalid(field: &'static str, reason: impl ToString) -> WireDecodeError {
    WireDecodeError::Invalid {
        field,
        reason: reason.to_string(),
    }
}

pub fn required<T>(value: Option<T>, field: &'static str) -> Result<T, WireDecodeError> {
    value.ok_or(WireDecodeError::Missing(field))
}

pub fn parse_id<T>(value: Option<String>, field: &'static str) -> Result<T, WireDecodeError>
where
    T: FromStr<Err = IdentityError>,
{
    required(value, field)?
        .parse()
        .map_err(|error| invalid(field, error))
}

pub fn timestamp(
    value: Option<Timestamp>,
    field: &'static str,
) -> Result<UtcTimestamp, WireDecodeError> {
    let value = required(value, field)?;
    if !(0..1_000_000_000).contains(&value.nanos) {
        return Err(invalid(field, "nanos must be within 0..1_000_000_000"));
    }
    let millis = value
        .seconds
        .checked_mul(MILLIS_PER_SECOND)
        .and_then(|seconds| seconds.checked_add(i64::from(value.nanos / NANOS_PER_MILLISECOND)))
        .ok_or_else(|| invalid(field, "timestamp millisecond overflow"))?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}
