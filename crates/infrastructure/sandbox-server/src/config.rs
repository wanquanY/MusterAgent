//! Host configuration for the server sandbox.
//!
//! Everything that is deployment specific (the image, its environment, the
//! container user, host limits and the egress network) is host configuration.
//! The kernel never hardcodes an image or a dependency list.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use ipnet::IpNet;
use musteragent_identity::VersionedKey;
use musteragent_runtime_capability::MAX_BROWSER_LEASE_TTL_MILLIS;
use serde::Deserialize;
use thiserror::Error;

/// Environment keys the executor owns. Host image environment must not set
/// them, because they decide where network traffic may go.
pub const RESERVED_ENV_KEYS: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "all_proxy",
    "HOME",
    "TMPDIR",
];

/// Default interpreter when the image environment does not set `PYTHON_BIN`.
pub const DEFAULT_PYTHON_BIN: &str = "python3";

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServerSandboxConfig {
    pub image: ImageConfig,
    /// Must equal the `runtime_release_ref` of the jobs this provider serves.
    pub runtime_release_ref: String,
    pub resource_profile_ref: String,
    #[serde(default)]
    pub limits: HostLimits,
    /// Numeric `uid:gid` the job runs as. Root is rejected.
    #[serde(default = "default_container_user")]
    pub container_user: String,
    /// Host directory under which one directory per job is created and
    /// removed. Must be shared with the Docker daemon.
    pub work_root: PathBuf,
    /// Policy for explicitly networked jobs. Both profiles use network=none;
    /// networked jobs get a loopback proxy over attached Docker stdio.
    /// None refuses networked jobs; it never grants direct network access.
    #[serde(default)]
    pub egress: Option<EgressNetworkConfig>,
    #[serde(default)]
    pub lease: LeaseConfig,
    #[serde(default = "default_max_concurrent_jobs")]
    pub max_concurrent_jobs: u16,
    /// Extra labels put on every job container (for host accounting).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ImageConfig {
    /// Image reference, ideally pinned by digest.
    pub reference: String,
    /// Environment passed to every job, for example `PYTHON_BIN`,
    /// `UV_PYTHON`, `PYTHONNOUSERSITE`, `MPLCONFIGDIR`.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Interpreter flags placed before the entrypoint.
    #[serde(default = "default_python_args")]
    pub python_args: Vec<String>,
}

impl ImageConfig {
    #[must_use]
    pub fn python_bin(&self) -> &str {
        self.env
            .get("PYTHON_BIN")
            .map_or(DEFAULT_PYTHON_BIN, String::as_str)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostLimits {
    /// CPU quota in thousandths of a CPU.
    pub cpu_millis: u32,
    pub memory_bytes: u64,
    pub pids: u32,
    /// Upper bound applied on top of the job's own wall time.
    pub max_wall_time_millis: u64,
    pub tmpfs_bytes: u64,
    /// Maximum open files inside the container.
    pub nofile: u32,
}

impl Default for HostLimits {
    fn default() -> Self {
        Self {
            cpu_millis: 1_000,
            memory_bytes: 512 * 1024 * 1024,
            pids: 128,
            max_wall_time_millis: 300_000,
            tmpfs_bytes: 64 * 1024 * 1024,
            nofile: 1_024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EgressNetworkConfig {
    /// Policy served over a Docker-attached tunnel; the container has no NIC.
    pub proxy: EgressProxyConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseConfig {
    pub ttl_millis: u64,
    pub poll_interval_millis: u64,
    /// Delay before re-registering after the lease was lost.
    pub reregister_backoff_millis: u64,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            ttl_millis: 30_000,
            poll_interval_millis: 500,
            reregister_backoff_millis: 1_000,
        }
    }
}

impl LeaseConfig {
    /// Renew well before expiry: one third of the TTL.
    #[must_use]
    pub fn renew_interval(&self) -> Duration {
        Duration::from_millis(self.ttl_millis / 3)
    }
}

/// Host egress-proxy configuration.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EgressProxyConfig {
    pub listen: std::net::SocketAddr,
    /// Optional host-operated upstream. Never selected automatically after DNS failure.
    #[serde(default)]
    pub upstream: Option<crate::egress::UpstreamProxy>,
    /// Extra CIDRs that are always refused (in addition to the built-in list).
    #[serde(default)]
    pub deny_cidrs: Vec<String>,
    /// Host names refused before resolution. `example.com` also matches
    /// subdomains.
    #[serde(default)]
    pub deny_hosts: Vec<String>,
    /// Exceptions for documentation/reserved ranges only. Required private,
    /// metadata, loopback, shared, link-local and embedded-address denials
    /// cannot be overridden, including for a host-operated mirror.
    #[serde(default)]
    pub allow_cidrs: Vec<String>,
    /// Destination ports that may be reached. `None` allows any port.
    #[serde(default = "default_allowed_ports")]
    pub allowed_ports: Option<BTreeSet<u16>>,
    #[serde(default = "default_connect_timeout_millis")]
    pub connect_timeout_millis: u64,
}

impl EgressProxyConfig {
    pub fn parsed_deny_cidrs(&self) -> Result<Vec<IpNet>, ConfigError> {
        parse_cidrs(&self.deny_cidrs)
    }

    pub fn parsed_allow_cidrs(&self) -> Result<Vec<IpNet>, ConfigError> {
        parse_cidrs(&self.allow_cidrs)
    }
}

fn parse_cidrs(values: &[String]) -> Result<Vec<IpNet>, ConfigError> {
    values
        .iter()
        .map(|value| {
            value
                .parse::<IpNet>()
                .or_else(|_| value.parse::<std::net::IpAddr>().map(IpNet::from))
                .map_err(|_| ConfigError::InvalidCidr(value.clone()))
        })
        .collect()
}

fn default_container_user() -> String {
    "65534:65534".to_owned()
}

fn default_max_concurrent_jobs() -> u16 {
    4
}

fn default_python_args() -> Vec<String> {
    vec!["-s".to_owned()]
}

#[allow(clippy::unnecessary_wraps)]
fn default_allowed_ports() -> Option<BTreeSet<u16>> {
    Some(BTreeSet::from([80, 443]))
}

fn default_connect_timeout_millis() -> u64 {
    10_000
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConfigError {
    #[error("image reference is empty or malformed")]
    InvalidImage,
    #[error("environment key `{0}` is invalid or reserved")]
    InvalidEnvKey(String),
    #[error("environment value for `{0}` contains a NUL byte")]
    InvalidEnvValue(String),
    #[error("python argument is invalid")]
    InvalidPythonArg,
    #[error("runtime release or resource profile reference is invalid")]
    InvalidReleaseRef,
    #[error("container user must be a non-root numeric uid:gid")]
    InvalidUser,
    #[error("work root must be an absolute path without ':' or ','")]
    InvalidWorkRoot,
    #[error("host limit `{0}` is out of range")]
    InvalidLimit(&'static str),
    #[error("egress network configuration is invalid")]
    InvalidEgress,
    #[error("lease configuration is invalid")]
    InvalidLease,
    #[error("label `{0}` is invalid or reserved")]
    InvalidLabel(String),
    #[error("CIDR `{0}` is invalid")]
    InvalidCidr(String),
}

impl ServerSandboxConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_image_reference(&self.image.reference)?;
        for (key, value) in &self.image.env {
            if !valid_env_key(key) || RESERVED_ENV_KEYS.contains(&key.as_str()) {
                return Err(ConfigError::InvalidEnvKey(key.clone()));
            }
            if value.contains('\0') {
                return Err(ConfigError::InvalidEnvValue(key.clone()));
            }
        }
        if self
            .image
            .python_args
            .iter()
            .any(|arg| !arg.starts_with('-') || arg.contains('\0') || arg.len() > 64)
        {
            return Err(ConfigError::InvalidPythonArg);
        }
        if self.image.python_bin().is_empty() || self.image.python_bin().contains('\0') {
            return Err(ConfigError::InvalidEnvValue("PYTHON_BIN".to_owned()));
        }
        VersionedKey::new(self.runtime_release_ref.as_str())
            .map_err(|_| ConfigError::InvalidReleaseRef)?;
        VersionedKey::new(self.resource_profile_ref.as_str())
            .map_err(|_| ConfigError::InvalidReleaseRef)?;
        validate_user(&self.container_user)?;
        let root = self
            .work_root
            .to_str()
            .ok_or(ConfigError::InvalidWorkRoot)?;
        if !self.work_root.is_absolute() || root.contains(':') || root.contains(',') {
            return Err(ConfigError::InvalidWorkRoot);
        }
        let limits = &self.limits;
        if limits.cpu_millis == 0 || limits.cpu_millis > 64_000 {
            return Err(ConfigError::InvalidLimit("cpu_millis"));
        }
        if limits.memory_bytes < 16 * 1024 * 1024 || i64::try_from(limits.memory_bytes).is_err() {
            return Err(ConfigError::InvalidLimit("memory_bytes"));
        }
        if limits.pids < 8 || limits.pids > 65_536 {
            return Err(ConfigError::InvalidLimit("pids"));
        }
        if limits.max_wall_time_millis == 0 || limits.max_wall_time_millis > 3_600_000 {
            return Err(ConfigError::InvalidLimit("max_wall_time_millis"));
        }
        if limits.tmpfs_bytes == 0 || limits.tmpfs_bytes > limits.memory_bytes {
            return Err(ConfigError::InvalidLimit("tmpfs_bytes"));
        }
        if limits.nofile < 64 || limits.nofile > 1_048_576 {
            return Err(ConfigError::InvalidLimit("nofile"));
        }
        if let Some(egress) = &self.egress {
            crate::egress::EgressProxy::from_config(&egress.proxy)?;
        }
        let lease = &self.lease;
        if lease.ttl_millis < 3_000
            || lease.ttl_millis > MAX_BROWSER_LEASE_TTL_MILLIS
            || lease.poll_interval_millis == 0
            || lease.poll_interval_millis >= lease.ttl_millis / 3
        {
            return Err(ConfigError::InvalidLease);
        }
        if self.max_concurrent_jobs == 0 || self.max_concurrent_jobs > 256 {
            return Err(ConfigError::InvalidLimit("max_concurrent_jobs"));
        }
        for (key, value) in &self.labels {
            if key.is_empty()
                || key.len() > 128
                || key.starts_with(crate::LABEL_PREFIX)
                || value.contains('\0')
            {
                return Err(ConfigError::InvalidLabel(key.clone()));
            }
        }
        Ok(())
    }
}

fn validate_image_reference(value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > 512
        || value.starts_with('-')
        || value
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || !c.is_ascii())
    {
        return Err(ConfigError::InvalidImage);
    }
    Ok(())
}

fn valid_env_key(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && key.len() <= 128
}

fn validate_user(value: &str) -> Result<(), ConfigError> {
    let (uid, gid) = value.split_once(':').ok_or(ConfigError::InvalidUser)?;
    let uid: u32 = uid.parse().map_err(|_| ConfigError::InvalidUser)?;
    let gid: u32 = gid.parse().map_err(|_| ConfigError::InvalidUser)?;
    if uid == 0 || gid == 0 {
        return Err(ConfigError::InvalidUser);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn sample() -> ServerSandboxConfig {
        ServerSandboxConfig {
            image: ImageConfig {
                reference: "python:3.12-slim".to_owned(),
                env: BTreeMap::from([
                    ("PYTHON_BIN".to_owned(), "python3".to_owned()),
                    ("PYTHONNOUSERSITE".to_owned(), "1".to_owned()),
                    ("MPLCONFIGDIR".to_owned(), "/tmp/matplotlib".to_owned()),
                ]),
                python_args: vec!["-s".to_owned()],
            },
            runtime_release_ref: "browser.runtime@1".to_owned(),
            resource_profile_ref: "server.sandbox@1".to_owned(),
            limits: HostLimits::default(),
            container_user: "65534:65534".to_owned(),
            work_root: PathBuf::from("/var/lib/musteragent-sandbox"),
            egress: None,
            lease: LeaseConfig::default(),
            max_concurrent_jobs: 4,
            labels: BTreeMap::new(),
        }
    }

    #[test]
    fn sample_config_is_valid() {
        assert_eq!(sample().validate(), Ok(()));
    }

    #[test]
    fn parses_minimal_toml_like_json_with_defaults() {
        let config: ServerSandboxConfig = serde_json::from_value(serde_json::json!({
            "image": { "reference": "registry.example/sandbox@sha256:abc" },
            "runtime_release_ref": "browser.runtime@1",
            "resource_profile_ref": "server.sandbox@1",
            "work_root": "/srv/sandbox"
        }))
        .expect("config parses");
        assert_eq!(config.validate(), Ok(()));
        assert_eq!(config.container_user, "65534:65534");
        assert_eq!(config.image.python_bin(), DEFAULT_PYTHON_BIN);
        assert_eq!(config.limits, HostLimits::default());
        assert!(config.egress.is_none());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let parsed: Result<ServerSandboxConfig, _> = serde_json::from_value(serde_json::json!({
            "image": { "reference": "x" },
            "runtime_release_ref": "browser.runtime@1",
            "resource_profile_ref": "server.sandbox@1",
            "work_root": "/srv/sandbox",
            "privileged": true
        }));
        assert!(parsed.is_err());
    }

    #[test]
    fn python_bin_comes_from_image_env() {
        let mut config = sample();
        config
            .image
            .env
            .insert("PYTHON_BIN".to_owned(), "/opt/venv/bin/python".to_owned());
        assert_eq!(config.image.python_bin(), "/opt/venv/bin/python");
    }

    #[test]
    fn rejects_bad_images() {
        for image in ["", " python", "python 3", "-v", "pythön"] {
            let mut config = sample();
            config.image.reference = image.to_owned();
            assert_eq!(config.validate(), Err(ConfigError::InvalidImage), "{image}");
        }
    }

    #[test]
    fn rejects_reserved_and_malformed_env() {
        for key in ["HTTP_PROXY", "https_proxy", "HOME", "1ABC", "A-B", ""] {
            let mut config = sample();
            config.image.env.insert(key.to_owned(), "x".to_owned());
            assert_eq!(
                config.validate(),
                Err(ConfigError::InvalidEnvKey(key.to_owned())),
                "{key}"
            );
        }
        let mut config = sample();
        config
            .image
            .env
            .insert("UV_PYTHON".to_owned(), "a\0b".to_owned());
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidEnvValue(_))
        ));
    }

    #[test]
    fn rejects_root_or_symbolic_users() {
        for user in [
            "0:0",
            "0:1000",
            "1000:0",
            "root",
            "nobody:nogroup",
            "1000",
            "",
        ] {
            let mut config = sample();
            config.container_user = user.to_owned();
            assert_eq!(config.validate(), Err(ConfigError::InvalidUser), "{user}");
        }
    }

    #[test]
    fn rejects_unsafe_work_roots() {
        for root in ["relative/dir", "/a:b", "/a,b"] {
            let mut config = sample();
            config.work_root = PathBuf::from(root);
            assert_eq!(
                config.validate(),
                Err(ConfigError::InvalidWorkRoot),
                "{root}"
            );
        }
    }

    #[test]
    fn rejects_out_of_range_limits() {
        let mut config = sample();
        config.limits.cpu_millis = 0;
        assert_eq!(
            config.validate(),
            Err(ConfigError::InvalidLimit("cpu_millis"))
        );
        let mut config = sample();
        config.limits.memory_bytes = 1024;
        assert_eq!(
            config.validate(),
            Err(ConfigError::InvalidLimit("memory_bytes"))
        );
        let mut config = sample();
        config.limits.pids = 1;
        assert_eq!(config.validate(), Err(ConfigError::InvalidLimit("pids")));
        let mut config = sample();
        config.limits.tmpfs_bytes = config.limits.memory_bytes + 1;
        assert_eq!(
            config.validate(),
            Err(ConfigError::InvalidLimit("tmpfs_bytes"))
        );
        let mut config = sample();
        config.max_concurrent_jobs = 0;
        assert_eq!(
            config.validate(),
            Err(ConfigError::InvalidLimit("max_concurrent_jobs"))
        );
    }

    #[test]
    fn lease_ttl_respects_protocol_maximum() {
        let mut config = sample();
        config.lease.ttl_millis = MAX_BROWSER_LEASE_TTL_MILLIS + 1;
        assert_eq!(config.validate(), Err(ConfigError::InvalidLease));
        let mut config = sample();
        config.lease.poll_interval_millis = config.lease.ttl_millis;
        assert_eq!(config.validate(), Err(ConfigError::InvalidLease));
        assert_eq!(
            LeaseConfig::default().renew_interval(),
            Duration::from_millis(10_000)
        );
    }

    #[test]
    fn egress_configuration_rejects_invalid_policy() {
        let mut config = sample();
        config.egress = Some(EgressNetworkConfig {
            proxy: serde_json::from_value(
                serde_json::json!({"listen":"127.0.0.1:0","deny_cidrs":["10.0.0.0/33"]}),
            )
            .expect("config"),
        });
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidCidr(_))
        ));
        let legacy: Result<EgressNetworkConfig, _> = serde_json::from_value(
            serde_json::json!({"internal_network":"bridge","proxy_url":"http://proxy"}),
        );
        assert!(
            legacy.is_err(),
            "a network attachment cannot substitute for the isolated proxy tunnel"
        );
    }

    #[test]
    fn reserved_labels_are_rejected() {
        for key in ["", "io.musteragent.sandbox.job", "io.musteragent.sandbox"] {
            let mut config = sample();
            config.labels.insert(key.to_owned(), "x".to_owned());
            assert!(
                matches!(config.validate(), Err(ConfigError::InvalidLabel(_))),
                "{key}"
            );
        }
    }

    #[test]
    fn proxy_cidrs_parse_networks_and_bare_addresses() {
        let config: EgressProxyConfig = serde_json::from_value(serde_json::json!({
            "listen": "127.0.0.1:3128",
            "deny_cidrs": ["203.0.113.0/24", "198.51.100.7"],
        }))
        .expect("proxy config");
        assert_eq!(config.parsed_deny_cidrs().expect("cidrs").len(), 2);
        assert_eq!(config.allowed_ports, Some(BTreeSet::from([80, 443])));
        let bad = EgressProxyConfig {
            deny_cidrs: vec!["10.0.0.0/33".to_owned()],
            ..config
        };
        assert!(bad.parsed_deny_cidrs().is_err());
    }
}
