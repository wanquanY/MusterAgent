//! PostgreSQL cloud execution composition using kernel-owned ports.
#![forbid(unsafe_code)]
mod context_compactor;
mod provider_io;
mod runner;
pub use context_compactor::PersistedContextCompactor;
pub use musteragent_tool_executor::*;
pub use provider_io::{PostgresWorkerTraceContextSource, RuntimeModelContentResolver};
pub use runner::*;

mod composition;
pub use composition::{
    BuildError, CloudWorkerComposition, SystemRuntimeClock, WorkerHost, WorkerPolicy,
};

mod config;
pub mod workspace_releases;
pub use config::*;
mod manifest;
pub use manifest::capability_manifest;

mod tools;
pub use tools::{ToolServices, kernel_dispatcher, product_dispatchers};

mod file_queries;
mod resource_embeddings;

mod memory_embedding;
pub use memory_embedding::{EmbeddingSettings, HttpMemoryEmbeddingProvider};

pub use resource_embeddings::ResourceEmbeddingWorker;

mod extensions;
pub use extensions::WorkerExtensionConfig;

mod worker;
pub use worker::{WorkerOverrides, run_worker};
