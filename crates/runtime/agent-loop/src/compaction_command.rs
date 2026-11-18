//! Data operations of the existing durable Compression invocation store.
use musteragent_content_ref::ContentRef;
use musteragent_context::ContextCompactionRequest;
use musteragent_identity::InvocationId;
use musteragent_runtime_model::{CompleteModelInvocation, ModelInvocation};

#[derive(Debug, Clone, PartialEq)]
pub enum AgentCompactionCommand {
    BeginSupervised {
        request: Box<ContextCompactionRequest>,
        invocation: Box<ModelInvocation>,
    },
    CompleteSupervised(Box<musteragent_runtime_model::SupervisedModelCompletion>),
    /// Historical wire reader: its reply must omit the sealed estimate.
    LatestGeneration(Box<ContextCompactionRequest>),
    /// Explicit support for exact recovery using original observational data.
    LatestGenerationWithSealedEstimate(Box<ContextCompactionRequest>),
    LatestRoleCandidate(Box<ContextCompactionRequest>),
    Load(Box<ContextCompactionRequest>),
    Seal {
        request: Box<ContextCompactionRequest>,
        invocation: Box<ModelInvocation>,
    },
    Dispatch(Box<ContextCompactionRequest>),
    Recover(InvocationId),
    Complete(Box<CompleteModelInvocation>),
    ReadContent {
        invocation_id: InvocationId,
        content: ContentRef,
    },
}
