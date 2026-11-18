//! Read-only history port and active per-call execution lanes.
use crate::{ExtensionContext, ExtensionError, ToolSample};
use musteragent_identity::{AttemptId, TenantId};
use musteragent_runtime_contract::ApplicationFuture;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

/// One original kernel invocation plus its committed result, if available.
#[derive(Debug, Clone)]
pub struct ToolHistoryEntry {
    /// Original planned arguments. Recorded extension rewrites are applied as a view.
    pub call: ToolSample,
    /// Canonical result content for requested polling tools only; call identity
    /// is excluded from equality. Other tools need no result blob reads.
    pub result: Option<serde_json::Value>,
}
/// Supplied by the existing ledger/projection owner, never by extension facts.
pub trait ToolHistory: Send + Sync {
    /// Whether the original executor already recorded this call's request.
    /// Recovery must reuse its admission and recorded rewrites, without running
    /// mutable hooks again. This is an indexed read, never an extension receipt.
    fn requested<'a>(
        &'a self,
        context: &'a ExtensionContext,
        call_id: &'a str,
    ) -> ApplicationFuture<'a, Result<bool, ExtensionError>>;
    /// Latest `limit` calls in durable invocation/ordinal order, up to and including
    /// `through` if supplied. Include completed calls and calls already requested
    /// through the original tool executor, plus the current `through` candidate.
    /// Exclude later model-planned calls whose preparation has not finished.
    /// Scope strictly to this attempt and bound the read at the storage owner.
    /// Hydrate committed results only for `result_tools`; argument-only patterns
    /// must not pay for content reads or depend on unrelated result availability.
    fn recent<'a>(
        &'a self,
        context: &'a ExtensionContext,
        through: Option<&'a str>,
        limit: usize,
        result_tools: &'a std::collections::BTreeSet<String>,
    ) -> ApplicationFuture<'a, Result<Vec<ToolHistoryEntry>, ExtensionError>>;
}
type CallKey = (TenantId, AttemptId, String);
#[derive(Default)]
pub(crate) struct CallLanes(Mutex<BTreeMap<CallKey, Weak<tokio::sync::Mutex<()>>>>);
impl CallLanes {
    pub(crate) fn lane(
        &self,
        context: &ExtensionContext,
        call: &str,
    ) -> Result<Arc<tokio::sync::Mutex<()>>, ExtensionError> {
        let mut lanes = self
            .0
            .lock()
            .map_err(|_| ExtensionError::Audit("call lanes poisoned".into()))?;
        lanes.retain(|_, lane| lane.strong_count() > 0);
        let key = (
            context.run.tenant_id,
            context.attempt.attempt_id,
            call.into(),
        );
        if let Some(lane) = lanes.get(&key).and_then(Weak::upgrade) {
            return Ok(lane);
        }
        let lane = Arc::new(tokio::sync::Mutex::new(()));
        lanes.insert(key, Arc::downgrade(&lane));
        Ok(lane)
    }
}
