use std::sync::Arc;

use musteragent_event_envelope::RuntimeHome;
use musteragent_execution_grant::{
    EncodedExecutionGrantProof, ExecutionGrantProofEncoder, SignedExecutionGrant,
};
use musteragent_identity::CorrelationId;
use musteragent_runtime_contract::{ErrorCode, RuntimeError};

use super::RuntimeApplication;
use crate::runtime_error;

impl RuntimeApplication {
    /// Installs the canonical signed-grant proof adapter required by any home
    /// that must preserve authorization lineage beyond the source Run (for
    /// example durable Standing Goal continuations).
    #[must_use]
    pub fn with_execution_grant_proof_encoder(
        mut self,
        encoder: Arc<dyn ExecutionGrantProofEncoder>,
    ) -> Self {
        self.execution_grant_proof_encoder = Some(encoder);
        self
    }

    pub(super) fn execution_grant_proof(
        &self,
        signed: &SignedExecutionGrant,
        correlation_id: CorrelationId,
    ) -> Result<Option<EncodedExecutionGrantProof>, RuntimeError> {
        let Some(encoder) = self.execution_grant_proof_encoder.as_ref() else {
            if !matches!(
                self.home_policy.execution_home(),
                RuntimeHome::Device { .. }
            ) {
                return Ok(None);
            }
            return Err(runtime_error(
                ErrorCode::InternalInvariantViolated,
                correlation_id,
                false,
                "Device Runtime execution grant proof encoder is unavailable",
            ));
        };
        encoder.encode(signed).map(Some).map_err(|_| {
            runtime_error(
                ErrorCode::InternalInvariantViolated,
                correlation_id,
                false,
                "Runtime execution grant proof could not be encoded",
            )
        })
    }
}
