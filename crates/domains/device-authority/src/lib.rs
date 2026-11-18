//! Canonical device execution authority accepted from the host identity owner.
//! Decoding validates binding and byte identity, not signature trust. Hosts must
//! authenticate the original envelope and current membership before admission.
#![forbid(unsafe_code)]
mod atoms;
mod codec;
mod envelope;
mod payload;
mod subject;
pub mod tags;
pub use atoms::*;
pub use codec::*;
pub use envelope::*;
pub use payload::*;
pub use subject::*;
#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("membership JSON or closed schema is invalid")]
    Json(#[from] serde_json::Error),
    #[error("membership bytes are not canonical")]
    NonCanonical,
    #[error("membership object exceeds its byte limit")]
    TooLarge,
    #[error("invalid membership field: {0}")]
    InvalidField(&'static str),
    #[error("inconsistent membership binding: {0}")]
    Binding(&'static str),
}

pub type Result<T> = std::result::Result<T, ContractError>;
