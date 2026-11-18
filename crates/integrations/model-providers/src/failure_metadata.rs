//! Trusted-hop failure facts shared by status responses and terminal stream frames.
use musteragent_runtime_model::{ProviderErrorCode as Code, ProviderErrorKind as Kind};
use serde_json::Value;

pub(crate) fn hard_quota(code: &str) -> bool {
    matches!(
        code,
        "arrearage"
            | "billing_hard_limit_reached"
            | "credit_balance_too_low"
            | "insufficient_balance"
            | "insufficient_credits"
            | "insufficient_quota"
            | "quota_exceeded"
    )
}

#[derive(Default)]
pub(crate) struct FailureMetadata {
    code: Option<String>,
    upstream_status: Option<u16>,
    upstream_failure: Option<String>,
    pub(crate) retry_after_ms: Option<u64>,
}

impl FailureMetadata {
    pub(crate) fn from_body(value: &Value) -> Self {
        let detail = value
            .pointer("/detail/details")
            .or_else(|| value.get("details"))
            .or_else(|| value.get("error"))
            .unwrap_or(value);
        let code = [
            "/detail/error_code",
            "/error/code",
            "/error/type",
            "/code",
            "/type",
            "/error/status",
            "/status",
        ]
        .into_iter()
        .filter_map(|key| value.pointer(key)?.as_str())
        .find(|code| code.is_ascii() && !code.is_empty() && code.len() <= 128)
        .map(str::to_ascii_lowercase);
        Self {
            code,
            upstream_status: detail
                .get("upstream_status")
                .and_then(Value::as_u64)
                .and_then(|n| u16::try_from(n).ok())
                .filter(|n| (400..600).contains(n)),
            upstream_failure: detail
                .get("upstream_failure")
                .and_then(Value::as_str)
                .filter(|s| matches!(*s, "transport_failed" | "quota_exceeded"))
                .map(str::to_owned),
            retry_after_ms: detail.get("retry_after_ms").and_then(Value::as_u64),
        }
    }

    pub(crate) fn classify(
        &self,
        gateway: bool,
        http_status: Option<u16>,
    ) -> Option<(Kind, Code, bool)> {
        match self.code.as_deref() {
            Some("billing_credits_exhausted" | "insufficient_model_usage_points") => {
                return Some((Kind::Quota, Code::CreditsExhausted, false));
            }
            Some("billing_model_not_entitled") => {
                return Some((Kind::Authorization, Code::ModelNotEntitled, false));
            }
            Some("billing_model_route_unavailable") => {
                return Some((Kind::InvalidRequest, Code::RouteConfigurationInvalid, false));
            }
            _ => {}
        }
        if gateway {
            match self.upstream_failure.as_deref() {
                Some("transport_failed") => {
                    return Some((Kind::Transport, Code::TransportFailed, true));
                }
                Some("quota_exceeded") => return Some((Kind::Quota, Code::QuotaExceeded, false)),
                _ => {}
            }
            if self.upstream_status.is_none()
                && let Some(status) = http_status
            {
                return Some(match status {
                    401 | 403 => (Kind::Authorization, Code::AuthorizationFailed, false),
                    408 | 429 | 500..=599 => (Kind::Server, Code::GatewayUnavailable, true),
                    _ => (Kind::InvalidRequest, Code::InvalidRequest, false),
                });
            }
            // Only the gateway emits these envelopes. A bare network failure
            // between gateway and Runtime cannot identify a provider.
            if self.upstream_status.is_none()
                && matches!(
                    self.code.as_deref(),
                    Some(
                        "model_invocation_failed"
                            | "model_gateway_internal_error"
                            | "billing_unavailable"
                            | "upstream_unavailable"
                    )
                )
            {
                return Some((Kind::Server, Code::GatewayUnavailable, true));
            }
        }
        if self.code.as_deref().is_some_and(hard_quota) {
            return Some((Kind::Quota, Code::QuotaExceeded, false));
        }
        let status = if gateway {
            self.upstream_status.or(http_status)
        } else {
            http_status.or(self.upstream_status)
        }?;
        Some(match status {
            401 => (Kind::Authentication, Code::AuthenticationFailed, false),
            402 => (Kind::Quota, Code::PaymentRequired, false),
            403 => (Kind::Authorization, Code::AuthorizationFailed, false),
            408 => (Kind::Timeout, Code::DeadlineExceeded, true),
            429 => (Kind::RateLimited, Code::RateLimited, true),
            400..=499 => (Kind::InvalidRequest, Code::InvalidRequest, false),
            _ => (Kind::Server, Code::ProviderServer, true),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gateway_errors_only_charge_upstream_when_source_is_known() {
        for (value, status, expected) in [
            (
                json!({"detail":{"details":{"upstream_status":402}}}),
                Some(502),
                Code::PaymentRequired,
            ),
            (
                json!({"error":{"code":"UPSTREAM_RATE_LIMITED","upstream_status":429,"retry_after_ms":77000}}),
                None,
                Code::RateLimited,
            ),
            (
                json!({"error":{"code":"billing_credits_exhausted"}}),
                None,
                Code::CreditsExhausted,
            ),
            (
                json!({"detail":{"error_code":"billing_model_not_entitled"}}),
                Some(402),
                Code::ModelNotEntitled,
            ),
            (
                json!({"detail":{"details":{"upstream_failure":"quota_exceeded","upstream_status":429}}}),
                Some(429),
                Code::QuotaExceeded,
            ),
            (
                json!({"detail":{"details":{"upstream_failure":"transport_failed"}}}),
                Some(502),
                Code::TransportFailed,
            ),
            (json!({}), Some(429), Code::GatewayUnavailable),
            (json!({}), Some(403), Code::AuthorizationFailed),
            (
                json!({"error":{"code":"MODEL_INVOCATION_FAILED"}}),
                None,
                Code::GatewayUnavailable,
            ),
            (json!({}), Some(400), Code::InvalidRequest),
        ] {
            assert_eq!(
                FailureMetadata::from_body(&value)
                    .classify(true, status)
                    .expect("valid test fixture")
                    .1,
                expected
            );
        }
        assert_eq!(
            FailureMetadata::from_body(&json!({"error":{"retry_after_ms":77000}})).retry_after_ms,
            Some(77000)
        );
    }
}
