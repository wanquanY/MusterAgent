//! Shared reconstruction of one physical Tool-selecting Assistant turn.
//! Device, Cloud and maintenance profiles preserve the same protocol closure.
use musteragent_content_ref::ContentRef;
use musteragent_identity::InvocationId;
use musteragent_runtime_content::{ContentBatch, ContentStoreError};
use musteragent_runtime_model::{
    CanonicalMessage, CanonicalRole, CanonicalToolCall, ReasoningReplay,
};
use std::{collections::BTreeSet, ops::Range};

pub fn contiguous_model_turns(
    invocations: impl IntoIterator<Item = InvocationId>,
) -> Result<Vec<Range<usize>>, ContentStoreError> {
    let mut seen = BTreeSet::new();
    let mut previous = None;
    let mut groups: Vec<Range<usize>> = Vec::new();
    for (index, invocation) in invocations.into_iter().enumerate() {
        if previous == Some(invocation) {
            groups.last_mut().ok_or(ContentStoreError::Invariant)?.end = index + 1;
        } else {
            if !seen.insert(invocation) {
                return Err(ContentStoreError::Invariant);
            }
            groups.push(index..index + 1);
            previous = Some(invocation);
        }
    }
    Ok(groups)
}

pub fn materialize_tool_turn_assistant_from_batch(
    batch: &ContentBatch,
    response_ref: &ContentRef,
    calls: Vec<CanonicalToolCall>,
    reasoning: Option<ReasoningReplay>,
) -> Result<CanonicalMessage, ContentStoreError> {
    let mut ids = BTreeSet::new();
    if calls.is_empty() || calls.iter().any(|call| !ids.insert(&call.call_id)) {
        return Err(ContentStoreError::Invariant);
    }
    if let Some(replay) = &reasoning {
        if replay.content_ref.media_type.as_str()
            != "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8"
            || replay.content_ref.byte_length == 0
        {
            return Err(ContentStoreError::Invariant);
        }
        // Verify the private closure as current content; never turn its bytes
        // into public Assistant prose or manufacture missing provider state.
        batch.bytes(&replay.content_ref)?;
    }
    for replay in calls
        .iter()
        .filter_map(|call| call.provider_replay_ref.as_ref())
    {
        batch.bytes(replay)?;
    }
    let message = CanonicalMessage {
        role: CanonicalRole::Assistant,
        content: crate::materialize_tool_assistant_text_from_batch(batch, response_ref)?,
        tool_calls: calls,
        tool_call_id: None,
        tool_name: None,
        reasoning,
    };
    message
        .validate()
        .map_err(|_| ContentStoreError::Invariant)?;
    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn physical_turns_are_atomic_and_cannot_reappear_after_a_later_turn() {
        let a = InvocationId::generate();
        let b = InvocationId::generate();
        assert_eq!(
            contiguous_model_turns([a, a, b, b, b]).expect(
                "valid fixture for physical turns are atomic and cannot reappear after a later turn"
            ),
            vec![0..2, 2..5]
        );
        assert!(contiguous_model_turns([a, b, a]).is_err());
        assert!(contiguous_model_turns([]).expect("valid fixture for physical turns are atomic and cannot reappear after a later turn").is_empty());
    }
}
