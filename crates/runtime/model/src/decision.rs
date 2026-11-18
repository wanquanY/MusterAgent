//! Non-generative model invocation port and shared transport authority.

use musteragent_model::{CredentialGrantRef, DecisionEvaluation, DecisionResponse, ModelRoute};
use musteragent_run::InvocationContext;
use musteragent_runtime_contract::ApplicationFuture;

use crate::{CanonicalModelRequest, ModelInvocationRouteRole, ProviderError};

/// Only the admission authority needed by first-party transport. It is shared
/// by chat and decision operations without converting decisions into messages.
#[derive(Debug, Clone)]
pub struct ModelRequestAuthority {
    pub invocation: InvocationContext,
    pub route: ModelRoute,
    pub route_role: ModelInvocationRouteRole,
    pub credential_grant_ref: CredentialGrantRef,
}

impl From<&CanonicalModelRequest> for ModelRequestAuthority {
    fn from(value: &CanonicalModelRequest) -> Self {
        Self {
            invocation: value.invocation.clone(),
            route: value.route.clone(),
            route_role: value.route_role,
            credential_grant_ref: value.credential_grant_ref.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DecisionModelRequest {
    pub authority: ModelRequestAuthority,
    pub evaluation: DecisionEvaluation,
}

pub type DecisionModelExecution<'a> = Box<
    dyn musteragent_execution_liveness::ExecutionOperation<
            'a,
            Result<DecisionResponse, DecisionModelFailure>,
        > + 'a,
>;

pub trait DecisionModelProvider: Send + Sync {
    /// Pure construction; adapters retain ownership of one physical request.
    fn execution<'a>(
        &'a self,
        _request: DecisionModelRequest,
        _binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<DecisionModelExecution<'a>, ProviderError> {
        Err(crate::execution::unsupported_model_execution())
    }

    fn evaluate(
        &self,
        request: DecisionModelRequest,
    ) -> ApplicationFuture<'_, Result<DecisionResponse, DecisionModelFailure>>;
}

/// Retains supplier usage even when a paid response fails its decision contract.
#[derive(Debug, Clone)]
pub struct DecisionModelFailure {
    pub error: ProviderError,
    pub usage: Option<musteragent_model::DecisionTokenUsage>,
}
impl From<ProviderError> for DecisionModelFailure {
    fn from(error: ProviderError) -> Self {
        Self { error, usage: None }
    }
}
