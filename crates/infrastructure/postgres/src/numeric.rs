//! Checked PostgreSQL BIGINT boundary for Runtime unsigned counters.

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Runtime field {field} is outside PostgreSQL BIGINT range")]
pub(crate) struct BigintRangeError {
    pub(crate) field: &'static str,
}

pub(crate) fn encode_u64(value: u64, field: &'static str) -> Result<i64, BigintRangeError> {
    i64::try_from(value).map_err(|_| BigintRangeError { field })
}

pub(crate) fn decode_u64(value: i64, field: &'static str) -> Result<u64, BigintRangeError> {
    u64::try_from(value).map_err(|_| BigintRangeError { field })
}

#[cfg(test)]
mod tests {
    use super::{decode_u64, encode_u64};

    #[test]
    fn bigint_codec_is_checked_in_both_directions() {
        assert_eq!(encode_u64(i64::MAX as u64, "sequence"), Ok(i64::MAX));
        assert!(encode_u64(i64::MAX as u64 + 1, "sequence").is_err());
        assert_eq!(decode_u64(i64::MAX, "sequence"), Ok(i64::MAX as u64));
        assert!(decode_u64(-1, "sequence").is_err());
    }
}
