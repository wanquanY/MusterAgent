//! Server-side code sandbox executor.
//!
//! This crate is the [`SandboxExecutor::Server`](musteragent_sandbox::SandboxExecutor)
//! provider for the Browser Job protocol (`musteragent_capability::browser`).
//! It registers with a [`BrowserGatewayStore`](musteragent_runtime_capability::BrowserGatewayStore)
//! like a browser page, leases and claims the same durable jobs, runs each
//! one in a fresh, locked-down Docker container, and seals the same terminal
//! results with the same `ContentRef` inputs and outputs.
//!
//! Layout:
//! * [`config`] — host configuration (image, env, user, limits, egress).
//! * [`spec`] — pure job → container isolation mapping.
//! * [`paths`] — job directories and symlink-safe output collection.
//! * [`runtime`] / [`docker`] — container runtime port and Docker adapter.
//! * [`job`] — materialize inputs, run, collect outputs, build results.
//! * [`provider`] — lease registration, renewal, claiming, cancellation.
//! * [`egress`] — destination filter and forward proxy for networked jobs.
//!
//! Unix only: job directories use symlinks and Unix permissions.
#![forbid(unsafe_code)]

#[cfg(not(unix))]
compile_error!("musteragent-sandbox-server supports Unix hosts only");

pub mod config;
pub mod docker;
pub mod egress;
pub mod job;
pub mod paths;
pub mod provider;
pub mod runtime;
pub mod spec;

/// Label key put on every container and network this crate creates.
pub const LABEL_PREFIX: &str = "io.musteragent.sandbox";
/// Label key carrying the job id of a job container.
pub const LABEL_JOB: &str = "io.musteragent.sandbox.job";

pub use musteragent_capability::{MAX_BROWSER_JOB_LOG_TAIL_BYTES, MAX_BROWSER_JOB_PATH_BYTES};

pub use config::{
    EgressNetworkConfig, EgressProxyConfig, HostLimits, ImageConfig, ServerSandboxConfig,
};
pub use docker::DockerRuntime;
pub use job::JobExecutor;
pub use provider::{
    ProviderBinding, SERVER_PROVIDER_CONTRACTS, ServerProviderRegistration, ServerSandboxProvider,
};
pub use runtime::{ContainerExit, ContainerOutcome, ContainerRuntime, RuntimeError};
pub use spec::{ContainerNetwork, ContainerSpec};

mod docker_outputs;
mod docker_session;

pub mod mcp;
pub use mcp::{McpSandboxLimits, McpSandboxResult, SandboxProcessLauncher};

mod program;
mod workspace;
