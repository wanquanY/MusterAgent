//! Target-route context compilation for cross-model handoff.
//!
//! The immutable Conversation Ledger remains canonical. This module produces
//! one lossy, target-specific Provider input without mutating source history or
//! teaching wire adapters how to repair conversations.

use musteragent_identity::ConversationSeq;
use musteragent_model::{ModelRoute, SignedModelRouteHash};
use musteragent_runtime_model::{CanonicalMessage, CanonicalRole, validate_tool_exchanges};

use crate::{ContextEntry, ContextPlanError};

/// Safe projection selected for one target Provider request.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum HandoffProjection {
    /// Every Assistant entry has a proven exact target-route origin.
    ExactReplay,
    /// At least one Assistant entry is foreign or has unknown provenance.
    PortableProjection,
    /// Ordinary target-route Context policy summarized an older atomic prefix
    /// of a request whose uncompressed projection contained portable history.
    CompactedHandoff,
}

/// Target-specific request material plus inspectable handoff diagnostics.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderContextCompilation {
    pub projection: HandoffProjection,
    pub messages: Vec<CanonicalMessage>,
    pub exact_assistant_entries: usize,
    pub portable_assistant_entries: usize,
}

impl ProviderContextCompilation {
    #[must_use]
    pub const fn has_portable_history(&self) -> bool {
        self.portable_assistant_entries > 0
    }
}

/// Compile canonical entries for one exact target route.
///
/// Visible content and canonical Tool semantics are portable. Reasoning replay,
/// Provider item identity and Provider-specific Tool replay are retained only
/// when the complete physical source identity matches the target route.
pub(crate) fn compile_provider_context(
    entries: &[&ContextEntry],
    target: &ModelRoute,
    compacted_handoff: bool,
) -> Result<ProviderContextCompilation, ContextPlanError> {
    // An opaque Provider continuation sends only the results produced after
    // its source invocation. Those leading results are paired with calls the
    // Provider already holds, so they are validated as a continuation delta.
    // Any other leading Tool result is still an orphan.
    let continuation_delta = entries
        .first()
        .is_some_and(|first| first.message.role == CanonicalRole::Tool)
        && entries
            .iter()
            .take_while(|entry| entry.message.role == CanonicalRole::Tool)
            .all(|entry| entry.continuation_delta_for.is_some());
    compile_message_sequence(
        entries
            .iter()
            .map(|e| (&e.message, e.assistant_origin.as_ref())),
        target,
        compacted_handoff,
        continuation_delta,
    )
}

pub(crate) fn compile_messages<'a>(
    entries: impl ExactSizeIterator<Item = (&'a CanonicalMessage, Option<&'a crate::AssistantOrigin>)>,
    target: &ModelRoute,
    compacted_handoff: bool,
) -> Result<ProviderContextCompilation, ContextPlanError> {
    compile_message_sequence(entries, target, compacted_handoff, false)
}

fn compile_message_sequence<'a>(
    entries: impl ExactSizeIterator<Item = (&'a CanonicalMessage, Option<&'a crate::AssistantOrigin>)>,
    target: &ModelRoute,
    compacted_handoff: bool,
    continuation_delta: bool,
) -> Result<ProviderContextCompilation, ContextPlanError> {
    let target_hash =
        SignedModelRouteHash::digest(target).map_err(|_| ContextPlanError::invariant())?;
    let mut exact_assistant_entries = 0_usize;
    let mut portable_assistant_entries = 0_usize;
    let mut messages = Vec::with_capacity(entries.len());

    for (original, origin) in entries {
        let mut message = original.clone();
        if message.role == CanonicalRole::Assistant {
            if assistant_origin_matches(origin, target, target_hash) {
                exact_assistant_entries = exact_assistant_entries.saturating_add(1);
            } else {
                portable_assistant_entries = portable_assistant_entries.saturating_add(1);
                remove_route_private_replay(&mut message);
                // Private-only reasoning is not portable. The planner retains
                // the typed continuation event; do not invent Assistant text.
                if message.content.is_empty() && message.tool_calls.is_empty() {
                    continue;
                }
            }
        }
        messages.push(message);
    }

    let projection = if compacted_handoff {
        HandoffProjection::CompactedHandoff
    } else if portable_assistant_entries == 0 {
        HandoffProjection::ExactReplay
    } else {
        HandoffProjection::PortableProjection
    };
    validate_tool_exchanges(&messages, continuation_delta)
        .map_err(|_| ContextPlanError::invariant())?;
    Ok(ProviderContextCompilation {
        projection,
        messages,
        exact_assistant_entries,
        portable_assistant_entries,
    })
}

/// Whether a physical usage measurement belongs to the exact target route.
///
/// Provider-reported usage is only comparable to a rebuilt request when the
/// Assistant completion at the anchor was produced by the same signed route.
/// A model or protocol switch invalidates that physical measurement, so the
/// planner must estimate the complete target projection instead.
pub(crate) fn target_route_owns_usage_anchor(
    entries: &[&ContextEntry],
    source_sequence: ConversationSeq,
    target: &ModelRoute,
) -> Result<bool, ContextPlanError> {
    if target
        .capabilities
        .iter()
        .any(|flag| flag.as_str() == "usage_aggregates_requests")
    {
        return Ok(false);
    }
    let target_hash =
        SignedModelRouteHash::digest(target).map_err(|_| ContextPlanError::invariant())?;
    Ok(entries.iter().any(|entry| {
        entry.source_sequence == source_sequence
            && entry.message.role == CanonicalRole::Assistant
            && assistant_origin_matches(entry.assistant_origin.as_ref(), target, target_hash)
    }))
}

fn assistant_origin_matches(
    origin: Option<&crate::AssistantOrigin>,
    target: &ModelRoute,
    target_hash: SignedModelRouteHash,
) -> bool {
    origin.is_some_and(|origin| {
        origin.model_route_ref == target.route_ref
            && origin.route_identity_hash == target_hash
            && origin.requested_model_key.as_str() == target.resolved_model_key
            && origin.response_model_key.as_str() == target.resolved_model_key
    })
}

fn remove_route_private_replay(message: &mut CanonicalMessage) {
    message.reasoning = None;
    for call in &mut message.tool_calls {
        call.provider_item_ref = None;
        call.provider_replay_ref = None;
    }
}

#[cfg(test)]
mod tests;
