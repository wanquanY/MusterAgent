use std::collections::BTreeMap;
use std::env;
use std::num::NonZeroUsize;
use std::time::Duration;

use serde::Serialize;
use thiserror::Error;
use url::Url;

const DEFAULT_QUEUE_CAPACITY: usize = 8_192;
const DEFAULT_BATCH_SIZE: usize = 512;
const DEFAULT_LOG_QUEUE_CAPACITY: usize = 8_192;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OtlpMode {
    Disabled,
    Optional,
    Required,
}

impl OtlpMode {
    fn parse(value: &str) -> Result<Self, TelemetryConfigError> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "optional" => Ok(Self::Optional),
            "required" => Ok(Self::Required),
            _ => Err(TelemetryConfigError::InvalidMode),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TelemetryConfig {
    pub mode: OtlpMode,
    pub endpoint: Option<String>,
    pub service_name: String,
    pub service_version: String,
    pub deployment_environment: String,
    pub cluster_identity: String,
    pub deployment_identity: String,
    pub build_identity: String,
    pub queue_capacity: NonZeroUsize,
    pub log_queue_capacity: NonZeroUsize,
    pub batch_size: NonZeroUsize,
    pub export_interval: Duration,
    pub export_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl TelemetryConfig {
    pub fn from_environment(
        service_name: &'static str,
        service_version: &'static str,
        deployment_environment: &str,
        protected: bool,
    ) -> Result<Self, TelemetryConfigError> {
        Self::from_values(
            &env::vars().collect(),
            service_name,
            service_version,
            deployment_environment,
            protected,
        )
    }

    pub fn from_values(
        values: &BTreeMap<String, String>,
        service_name: &'static str,
        service_version: &'static str,
        deployment_environment: &str,
        protected: bool,
    ) -> Result<Self, TelemetryConfigError> {
        let default_mode = if protected { "required" } else { "disabled" };
        let mode = OtlpMode::parse(
            values
                .get("MUSTERAGENT_OTLP_MODE")
                .map(String::as_str)
                .unwrap_or(default_mode),
        )?;
        if protected && mode != OtlpMode::Required {
            return Err(TelemetryConfigError::ProtectedDeploymentRequiresOtlp);
        }
        let endpoint = values
            .get("MUSTERAGENT_OTLP_ENDPOINT")
            .map(|value| validate_endpoint(value))
            .transpose()?;
        if mode != OtlpMode::Disabled && endpoint.is_none() {
            return Err(TelemetryConfigError::EndpointRequired);
        }
        if mode == OtlpMode::Disabled && endpoint.is_some() {
            return Err(TelemetryConfigError::DisabledEndpointConflict);
        }
        let cluster_identity =
            parse_deployment_identity(values, "MUSTERAGENT_CLUSTER_IDENTITY", "local", protected)?;
        let deployment_identity = parse_deployment_identity(
            values,
            "MUSTERAGENT_DEPLOYMENT_IDENTITY",
            service_name,
            protected,
        )?;
        let build_identity = parse_deployment_identity(
            values,
            "MUSTERAGENT_BUILD_IDENTITY",
            service_version,
            protected,
        )?;
        let queue_capacity = bounded_usize(
            values,
            "MUSTERAGENT_OTLP_TRACE_QUEUE_CAPACITY",
            DEFAULT_QUEUE_CAPACITY,
            128,
            1_000_000,
        )?;
        let batch_size = bounded_usize(
            values,
            "MUSTERAGENT_OTLP_TRACE_BATCH_SIZE",
            DEFAULT_BATCH_SIZE,
            1,
            10_000,
        )?;
        if batch_size > queue_capacity {
            return Err(TelemetryConfigError::BatchExceedsQueue);
        }
        let log_queue_capacity = bounded_usize(
            values,
            "MUSTERAGENT_LOG_QUEUE_CAPACITY",
            DEFAULT_LOG_QUEUE_CAPACITY,
            128,
            1_000_000,
        )?;
        Ok(Self {
            mode,
            endpoint,
            service_name: service_name.to_owned(),
            service_version: service_version.to_owned(),
            deployment_environment: deployment_environment.to_owned(),
            cluster_identity,
            deployment_identity,
            build_identity,
            queue_capacity,
            log_queue_capacity,
            batch_size,
            export_interval: duration_ms(
                values,
                "MUSTERAGENT_OTLP_EXPORT_INTERVAL_MS",
                5_000,
                100,
                60_000,
            )?,
            export_timeout: duration_ms(
                values,
                "MUSTERAGENT_OTLP_EXPORT_TIMEOUT_MS",
                5_000,
                100,
                30_000,
            )?,
            shutdown_timeout: duration_ms(
                values,
                "MUSTERAGENT_OTLP_SHUTDOWN_TIMEOUT_MS",
                5_000,
                100,
                30_000,
            )?,
        })
    }

    #[must_use]
    pub const fn readiness_required(&self) -> bool {
        matches!(self.mode, OtlpMode::Required)
    }

    #[must_use]
    pub fn deployment_snapshot(
        &self,
        build_artifact_sha256: String,
    ) -> TelemetryDeploymentSnapshot {
        TelemetryDeploymentSnapshot {
            environment: self.deployment_environment.clone(),
            cluster_identity: self.cluster_identity.clone(),
            deployment_identity: self.deployment_identity.clone(),
            build_identity: self.build_identity.clone(),
            build_artifact_sha256,
            collector_endpoint_identity: self
                .endpoint
                .clone()
                .unwrap_or_else(|| "disabled".to_owned()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct TelemetryDeploymentSnapshot {
    pub environment: String,
    pub cluster_identity: String,
    pub deployment_identity: String,
    pub build_identity: String,
    /// SHA-256 of the executable currently serving this readiness response.
    /// This is measured locally at process startup rather than accepted from
    /// deployment configuration.
    pub build_artifact_sha256: String,
    pub collector_endpoint_identity: String,
}

fn parse_deployment_identity(
    values: &BTreeMap<String, String>,
    key: &'static str,
    default: &str,
    required: bool,
) -> Result<String, TelemetryConfigError> {
    let value = values.get(key).map(String::as_str).unwrap_or(default);
    if required && !values.contains_key(key) {
        return Err(TelemetryConfigError::MissingDeploymentIdentity(key));
    }
    if value.is_empty()
        || value.len() > 256
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/' | b'@')
        })
    {
        return Err(TelemetryConfigError::InvalidDeploymentIdentity(key));
    }
    Ok(value.to_owned())
}

fn validate_endpoint(value: &str) -> Result<String, TelemetryConfigError> {
    let parsed = Url::parse(value).map_err(|_| TelemetryConfigError::InvalidEndpoint)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
    {
        return Err(TelemetryConfigError::InvalidEndpoint);
    }
    Ok(value.trim_end_matches('/').to_owned())
}

fn bounded_usize(
    values: &BTreeMap<String, String>,
    key: &'static str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<NonZeroUsize, TelemetryConfigError> {
    let parsed = values
        .get(key)
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| TelemetryConfigError::InvalidInteger(key))?
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&parsed) {
        return Err(TelemetryConfigError::InvalidInteger(key));
    }
    NonZeroUsize::new(parsed).ok_or(TelemetryConfigError::InvalidInteger(key))
}

fn duration_ms(
    values: &BTreeMap<String, String>,
    key: &'static str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<Duration, TelemetryConfigError> {
    let parsed = values
        .get(key)
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| TelemetryConfigError::InvalidInteger(key))?
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&parsed) {
        return Err(TelemetryConfigError::InvalidInteger(key));
    }
    Ok(Duration::from_millis(parsed))
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum TelemetryConfigError {
    #[error("MUSTERAGENT_OTLP_MODE must be disabled, optional or required")]
    InvalidMode,
    #[error("protected Runtime deployments require OTLP readiness")]
    ProtectedDeploymentRequiresOtlp,
    #[error("MUSTERAGENT_OTLP_ENDPOINT is required when OTLP is enabled")]
    EndpointRequired,
    #[error("MUSTERAGENT_OTLP_ENDPOINT must be absent when OTLP is disabled")]
    DisabledEndpointConflict,
    #[error("OTLP endpoint must be an http(s) origin without credentials, path or query")]
    InvalidEndpoint,
    #[error("telemetry integer configuration is invalid: {0}")]
    InvalidInteger(&'static str),
    #[error("OTLP trace batch size cannot exceed queue capacity")]
    BatchExceedsQueue,
    #[error("protected Runtime deployment is missing immutable identity {0}")]
    MissingDeploymentIdentity(&'static str),
    #[error("Runtime deployment identity is invalid: {0}")]
    InvalidDeploymentIdentity(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protected_runtime_requires_explicit_exporter_and_safe_endpoint() {
        let mut values = BTreeMap::new();
        assert_eq!(
            TelemetryConfig::from_values(&values, "worker", "v1", "production", true),
            Err(TelemetryConfigError::EndpointRequired)
        );
        values.insert(
            "MUSTERAGENT_OTLP_ENDPOINT".to_owned(),
            "https://collector.internal:4317".to_owned(),
        );
        for (key, value) in [
            ("MUSTERAGENT_CLUSTER_IDENTITY", "cluster-prod-a"),
            ("MUSTERAGENT_DEPLOYMENT_IDENTITY", "runtime-2026-08-04"),
            ("MUSTERAGENT_BUILD_IDENTITY", "sha256:0123456789abcdef"),
        ] {
            values.insert(key.to_owned(), value.to_owned());
        }
        assert!(TelemetryConfig::from_values(&values, "worker", "v1", "production", true).is_ok());
        values.insert(
            "MUSTERAGENT_OTLP_ENDPOINT".to_owned(),
            "https://token@collector.internal:4317/path".to_owned(),
        );
        assert_eq!(
            TelemetryConfig::from_values(&values, "worker", "v1", "production", true),
            Err(TelemetryConfigError::InvalidEndpoint)
        );
    }
}
