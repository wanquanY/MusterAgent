//! Restore an admission-pinned proof without changing its signed bytes.
use musteragent_execution_grant::{EncodedExecutionGrantProof, ExecutionGrantProofHash};
use musteragent_identity::DottedKey;
use sqlx::{Row, postgres::PgRow};

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoredGrantProofError {
    #[error("execution grant storage unavailable")]
    Database(#[from] sqlx::Error),
    #[error("execution grant proof columns or hash are invalid")]
    Corrupt,
}

pub(crate) fn from_row(
    row: &PgRow,
) -> Result<Option<EncodedExecutionGrantProof>, StoredGrantProofError> {
    use StoredGrantProofError::Corrupt;
    match (
        row.try_get::<Option<String>, _>("execution_grant_proof_schema")?,
        row.try_get::<Option<Vec<u8>>, _>("execution_grant_proof")?,
        row.try_get::<Option<Vec<u8>>, _>("execution_grant_proof_hash")?,
        row.try_get::<Option<Vec<u8>>, _>("execution_grant_claims_hash")?,
    ) {
        (None, None, None, None) => Ok(None),
        (Some(schema), Some(encoded), Some(content_hash), Some(claims_hash)) => {
            EncodedExecutionGrantProof::restore(
                DottedKey::new(schema).map_err(|_| Corrupt)?,
                encoded,
                ExecutionGrantProofHash::from_bytes(content_hash).map_err(|_| Corrupt)?,
                ExecutionGrantProofHash::from_bytes(claims_hash).map_err(|_| Corrupt)?,
            )
            .map(Some)
            .map_err(|_| Corrupt)
        }
        _ => Err(Corrupt),
    }
}
