//! Retry-After is a lower bound owned by the provider, in seconds or HTTP-date.
use std::time::SystemTime;

pub(super) fn delay_ms(value: &str, now: SystemTime) -> Option<u64> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return value
            .parse::<u64>()
            .ok()
            .map(|seconds| seconds.saturating_mul(1_000));
    }
    let deadline = httpdate::parse_http_date(value).ok()?;
    let delay = deadline.duration_since(now).unwrap_or_default();
    // Round up: truncating the sub-millisecond remainder would violate the floor.
    Some(u64::try_from(delay.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn accepts_long_waits_and_http_dates_without_retrying_early() {
        let date = httpdate::parse_http_date("Wed, 21 Oct 2015 07:28:00 GMT")
            .expect("valid recovery test fixture");
        assert_eq!(delay_ms("259200", date), Some(259_200_000));
        assert_eq!(
            delay_ms(
                "Wed, 21 Oct 2015 07:28:00 GMT",
                date - Duration::from_nanos(1)
            ),
            Some(1)
        );
        assert_eq!(
            delay_ms(
                "Wed, 21 Oct 2015 07:28:00 GMT",
                date + Duration::from_secs(1)
            ),
            Some(0)
        );
        assert_eq!(delay_ms("18446744073709551615", UNIX_EPOCH), Some(u64::MAX));
    }

    #[test]
    fn malformed_headers_do_not_become_a_zero_second_retry() {
        for value in ["", "-1", "+1", "1.5", "tomorrow", "18446744073709551616"] {
            assert_eq!(delay_ms(value, UNIX_EPOCH), None);
        }
    }
}
