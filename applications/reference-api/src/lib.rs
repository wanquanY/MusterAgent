//! Minimal development reference host for the durable cloud execution kernel.
#![forbid(unsafe_code)]
mod admission;
mod agents;
pub mod config;
mod http;
pub mod identity;
mod snapshot;
mod stream;
mod workspace;
pub use http::{Api, router};
/// Configuration and host-port failure; HTTP responses redact dependency details.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
/// Fallible host operation.
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
#[error("{code}")]
struct Fault {
    status: u16,
    code: &'static str,
}
fn fault(status: u16, code: &'static str) -> Error {
    Box::new(Fault { status, code })
}

#[derive(Debug, thiserror::Error)]
#[error("runtime error: {0:?}")]
struct RuntimeFailure(musteragent_runtime_contract::RuntimeError);
fn runtime_failure(e: musteragent_runtime_contract::RuntimeError) -> Error {
    Box::new(RuntimeFailure(e))
}
