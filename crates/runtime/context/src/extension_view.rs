//! Narrow, pre-seal context projection port. Durable source records stay immutable.
use crate::ContextPlanError;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{CanonicalMessage, CanonicalModelRequest, ModelText};

/// Only model-visible instructions/messages; authority and tool grants are absent.
#[derive(Debug, Clone)]
pub struct ModelContextView {
    /// System context.
    pub instructions: Vec<ModelText>,
    /// Conversation context.
    pub input: Vec<CanonicalMessage>,
}
/// Host composition stage called on a fresh request before validation/encoding.
/// Observers must never write back into the source ledger.
pub trait ContextViewProjection: Send + Sync {
    /// Project the view using the admitted request as immutable reference.
    fn project<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        request: &'a CanonicalModelRequest,
        view: ModelContextView,
    ) -> ApplicationFuture<'a, Result<ModelContextView, ContextPlanError>>;
}
