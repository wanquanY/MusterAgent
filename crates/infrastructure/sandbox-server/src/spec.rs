//! Pure mapping from a job + host configuration to an isolated container
//! specification. No Docker types here, so the isolation policy is unit
//! tested without a daemon.

use std::collections::BTreeMap;
use std::time::Duration;

use musteragent_capability::{BrowserJobRequest, BrowserSecurityProfile};

use crate::config::ServerSandboxConfig;
use crate::paths::{CONTAINER_INPUT_DIR, CONTAINER_WORK_DIR, JobDirs};
use crate::{LABEL_JOB, LABEL_PREFIX, MAX_BROWSER_JOB_LOG_TAIL_BYTES};

/// Network attachment of a job container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerNetwork {
    /// `--network none`: loopback only.
    None,
    /// Loopback proxy transported over attached Docker stdio; still no NIC.
    EgressProxy,
}

/// Selects offline or proxy-only transport. A missing proxy configuration
/// is refused during preparation/startup; there is no direct-network fallback.
#[must_use]
pub fn network_for_profile(
    profile: BrowserSecurityProfile,
    _config: &ServerSandboxConfig,
) -> ContainerNetwork {
    let allows_network = match profile {
        BrowserSecurityProfile::SafeOffline => false,
        BrowserSecurityProfile::ProxyEgress => true,
    };
    if allows_network {
        ContainerNetwork::EgressProxy
    } else {
        ContainerNetwork::None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindMount {
    pub host: String,
    pub container: String,
    pub read_only: bool,
}

/// Effective per-job limits: the stricter of the job's own limits and the
/// host's limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveLimits {
    pub wall_time: Duration,
    pub nano_cpus: i64,
    pub memory_bytes: i64,
    pub pids: i64,
    pub tmpfs_bytes: u64,
    pub nofile: u32,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub output_bytes: u64,
    pub output_files: u32,
}

impl EffectiveLimits {
    #[must_use]
    pub fn for_job(request: &BrowserJobRequest, config: &ServerSandboxConfig) -> Self {
        let host = config.limits;
        let wall = request
            .limits
            .wall_time_millis
            .get()
            .min(host.max_wall_time_millis);
        Self {
            wall_time: Duration::from_millis(wall),
            nano_cpus: i64::from(host.cpu_millis) * 1_000_000,
            memory_bytes: i64::try_from(host.memory_bytes).unwrap_or(i64::MAX),
            pids: i64::from(host.pids),
            tmpfs_bytes: host.tmpfs_bytes,
            nofile: host.nofile,
            stdout_bytes: request.limits.maximum_stdout_bytes.get(),
            stderr_bytes: request.limits.maximum_stderr_bytes.get(),
            output_bytes: request.limits.maximum_output_bytes.get(),
            output_files: request.limits.maximum_files.get(),
        }
    }

    /// Bytes of each stream retained for the result tail.
    #[must_use]
    pub const fn tail_bytes() -> usize {
        MAX_BROWSER_JOB_LOG_TAIL_BYTES
    }
}

/// Everything needed to create one job container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub cmd: Vec<String>,
    /// Host-selected interpreter used for the isolated supervisor and proxy relay.
    pub python_bin: String,
    pub env: Vec<(String, String)>,
    pub user: String,
    pub working_dir: String,
    pub hostname: String,
    pub labels: BTreeMap<String, String>,
    pub mounts: Vec<BindMount>,
    /// tmpfs mount point → mount options.
    pub tmpfs: BTreeMap<String, String>,
    pub read_only_rootfs: bool,
    pub cap_drop_all: bool,
    pub no_new_privileges: bool,
    pub network: ContainerNetwork,
    pub limits: EffectiveLimits,
}

impl ContainerSpec {
    /// Builds the spec for one Python job whose entrypoint is `entrypoint`
    /// relative to the work directory.
    #[must_use]
    pub fn python_job(
        request: &BrowserJobRequest,
        config: &ServerSandboxConfig,
        dirs: &JobDirs,
        entrypoint: &str,
    ) -> Self {
        let limits = EffectiveLimits::for_job(request, config);
        let network = network_for_profile(request.security_profile, config);
        let job = request.job_id.into_uuid().to_string();
        let mut cmd = vec![config.image.python_bin().to_owned()];
        cmd.extend(config.image.python_args.iter().cloned());
        cmd.push(entrypoint.to_owned());

        let mut env: Vec<(String, String)> = config
            .image
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        env.push(("HOME".to_owned(), "/tmp".to_owned()));
        env.push(("TMPDIR".to_owned(), "/tmp".to_owned()));

        let mut labels = config.labels.clone();
        labels.insert(LABEL_PREFIX.to_owned(), "server".to_owned());
        labels.insert(LABEL_JOB.to_owned(), job.clone());

        let mut tmpfs = BTreeMap::from([(
            "/tmp".to_owned(),
            format!("rw,noexec,nosuid,nodev,size={}", limits.tmpfs_bytes),
        )]);

        let input_bytes: u64 = request
            .inputs
            .iter()
            .map(|i| i.content_ref.byte_length)
            .sum();
        // Filesystem metadata and block rounding have a separate bounded allowance.
        let disk = input_bytes
            .saturating_add(limits.output_bytes)
            .saturating_add(u64::from(limits.output_files).min(10000) * 4096)
            .saturating_add(1024 * 1024)
            .min(config.limits.memory_bytes);
        tmpfs.insert(
            CONTAINER_WORK_DIR.to_owned(),
            format!("rw,nosuid,nodev,size={disk},mode=1777"),
        );

        Self {
            name: format!("musteragent-sandbox-{job}"),
            image: config.image.reference.clone(),
            cmd,
            python_bin: config.image.python_bin().to_owned(),
            env,
            user: config.container_user.clone(),
            working_dir: CONTAINER_WORK_DIR.to_owned(),
            hostname: "sandbox".to_owned(),
            labels,
            mounts: vec![
                BindMount {
                    host: dirs.input.to_string_lossy().into_owned(),
                    container: CONTAINER_INPUT_DIR.to_owned(),
                    read_only: true,
                },
                BindMount {
                    host: dirs.work.to_string_lossy().into_owned(),
                    container: "/sandbox/seed".to_owned(),
                    read_only: true,
                },
            ],
            tmpfs,
            read_only_rootfs: true,
            cap_drop_all: true,
            no_new_privileges: true,
            network: ContainerNetwork::None,
            limits,
        }
        .with_network(network)
    }

    /// Sets the network attachment and the matching proxy environment.
    #[must_use]
    pub fn with_network(mut self, network: ContainerNetwork) -> Self {
        self.env
            .retain(|(key, _)| !key.to_ascii_lowercase().ends_with("_proxy"));
        if let ContainerNetwork::EgressProxy = &network {
            let proxy_url = "http://127.0.0.1:18080";
            for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
                self.env.push((key.to_owned(), proxy_url.to_owned()));
            }
            self.env.push(("NO_PROXY".to_owned(), String::new()));
            self.env.push(("no_proxy".to_owned(), String::new()));
        }
        self.network = network;
        self
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::path::PathBuf;

    use musteragent_capability::{
        BrowserInput, BrowserJobEngine, BrowserJobProgram, BrowserResourceLimits,
    };
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        BrowserJobId, ContentId, ConversationId, InvocationId, VersionedKey,
    };

    use super::*;
    use crate::config::{EgressNetworkConfig, tests::sample};

    fn request(wall_time_millis: u64) -> BrowserJobRequest {
        let source = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/x-python").expect("media"),
            byte_length: 5,
            sha256: Sha256::digest("print"),
        };
        BrowserJobRequest {
            execution_liveness: None,
            job_id: BrowserJobId::generate(),
            invocation_id: InvocationId::generate(),
            conversation_id: ConversationId::generate(),
            engine: BrowserJobEngine::Python,
            runtime_release_ref: VersionedKey::new("browser.runtime@1").expect("release"),
            program: BrowserJobProgram::Python {
                source_ref: source.clone(),
                entrypoint: "main.py".to_owned(),
                environment: None,
                require_prepared_environment: false,
            },
            inputs: vec![BrowserInput {
                path: "main.py".to_owned(),
                content_ref: source,
            }],
            declared_outputs: vec![],
            limits: BrowserResourceLimits {
                wall_time_millis: NonZeroU64::new(wall_time_millis).expect("wall"),
                maximum_input_bytes: NonZeroU64::new(1_000).expect("in"),
                maximum_output_bytes: NonZeroU64::new(2_000).expect("out"),
                maximum_files: NonZeroU32::new(3).expect("files"),
                maximum_stdout_bytes: NonZeroU64::new(4_000).expect("stdout"),
                maximum_stderr_bytes: NonZeroU64::new(5_000).expect("stderr"),
            },
            security_profile: BrowserSecurityProfile::SafeOffline,
        }
    }

    fn dirs() -> JobDirs {
        JobDirs {
            root: PathBuf::from("/srv/sandbox/job"),
            input: PathBuf::from("/srv/sandbox/job/input"),
            work: PathBuf::from("/srv/sandbox/job/work"),
        }
    }

    #[test]
    fn job_limits_are_capped_by_host_limits() {
        let config = sample();
        let short = EffectiveLimits::for_job(&request(1_500), &config);
        assert_eq!(short.wall_time, Duration::from_millis(1_500));
        let long = EffectiveLimits::for_job(&request(10_000_000), &config);
        assert_eq!(
            long.wall_time,
            Duration::from_millis(config.limits.max_wall_time_millis)
        );
        assert_eq!(short.nano_cpus, 1_000_000_000);
        assert_eq!(short.memory_bytes, 512 * 1024 * 1024);
        assert_eq!(short.pids, 128);
        assert_eq!(short.stdout_bytes, 4_000);
        assert_eq!(short.stderr_bytes, 5_000);
        assert_eq!(short.output_bytes, 2_000);
        assert_eq!(short.output_files, 3);
    }

    #[test]
    fn spec_is_locked_down() {
        let config = sample();
        let request = request(1_000);
        let spec = ContainerSpec::python_job(&request, &config, &dirs(), "main.py");
        assert!(spec.read_only_rootfs && spec.cap_drop_all && spec.no_new_privileges);
        assert_eq!(spec.user, "65534:65534");
        assert_eq!(spec.network, ContainerNetwork::None);
        assert_eq!(spec.working_dir, "/sandbox/work");
        assert_eq!(spec.cmd, vec!["python3", "-s", "main.py"]);
        assert_eq!(
            spec.mounts,
            vec![
                BindMount {
                    host: "/srv/sandbox/job/input".to_owned(),
                    container: "/sandbox/input".to_owned(),
                    read_only: true,
                },
                BindMount {
                    host: "/srv/sandbox/job/work".to_owned(),
                    container: "/sandbox/seed".to_owned(),
                    read_only: true,
                },
            ]
        );
        assert_eq!(
            spec.tmpfs.get("/tmp").map(String::as_str),
            Some("rw,noexec,nosuid,nodev,size=67108864")
        );
        assert!(
            !spec
                .env
                .iter()
                .any(|(key, _)| key.to_lowercase().contains("proxy"))
        );
        assert_eq!(
            spec.labels.get(LABEL_JOB),
            Some(&request.job_id.into_uuid().to_string())
        );
        assert_eq!(
            spec.labels.get(LABEL_PREFIX).map(String::as_str),
            Some("server")
        );
    }

    #[test]
    fn offline_profile_never_gets_a_network_even_when_egress_is_configured() {
        let mut config = sample();
        config.egress = Some(EgressNetworkConfig {
            proxy: serde_json::from_value(serde_json::json!({"listen":"127.0.0.1:0"}))
                .expect("proxy config"),
        });
        assert_eq!(
            network_for_profile(BrowserSecurityProfile::SafeOffline, &config),
            ContainerNetwork::None
        );
    }

    #[test]
    fn egress_attachment_sets_proxy_environment() {
        let config = sample();
        let spec = ContainerSpec::python_job(&request(1_000), &config, &dirs(), "main.py")
            .with_network(ContainerNetwork::EgressProxy);
        let env: BTreeMap<_, _> = spec.env.iter().cloned().collect();
        assert_eq!(
            env.get("HTTPS_PROXY").map(String::as_str),
            Some("http://127.0.0.1:18080")
        );
        assert_eq!(
            env.get("http_proxy").map(String::as_str),
            Some("http://127.0.0.1:18080")
        );
        assert_eq!(env.get("NO_PROXY").map(String::as_str), Some(""));
        let back = spec.with_network(ContainerNetwork::None);
        assert!(
            !back
                .env
                .iter()
                .any(|(key, _)| key.to_lowercase().contains("proxy"))
        );
    }

    #[test]
    fn host_labels_cannot_override_job_labels() {
        let mut config = sample();
        config
            .labels
            .insert("purpose".to_owned(), "musteragent-sandbox-test".to_owned());
        let request = request(1_000);
        let spec = ContainerSpec::python_job(&request, &config, &dirs(), "main.py");
        assert_eq!(
            spec.labels.get("purpose").map(String::as_str),
            Some("musteragent-sandbox-test")
        );
        assert_eq!(
            spec.name,
            format!("musteragent-sandbox-{}", request.job_id.into_uuid())
        );
    }
}
