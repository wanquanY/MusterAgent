use super::*;
use crate::{CompactionCheckpoint, ContextCompactionStep};
use musteragent_runtime_contract::{ErrorCode, OpaqueExternalRef};
use musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V10;

fn request() -> ContextCompactionRequest {
    let mut request = super::tests::request();
    request.slot.compaction_policy_ref =
        musteragent_identity::VersionedKey::new(CONTEXT_COMPACTION_POLICY_V10).unwrap();
    request.slot.plan_fingerprint = request.expected_plan_fingerprint().unwrap();
    request
}
async fn response(
    request: &ContextCompactionRequest,
    text: &str,
    reason: StopReason,
) -> Result<ContextCompactionModelOutput, ContextCompactionModelFailure> {
    let mut events = vec![Ok(ModelStreamEvent::ResponseStarted {
        response_ref: ProviderRequestRef::from_opaque(
            OpaqueExternalRef::new("continuation-test").unwrap(),
        ),
        response_model_key: ModelKey::new("context-test-model").unwrap(),
    })];
    if !text.is_empty() {
        events.push(Ok(ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: ModelText::new(text).unwrap(),
        }));
    }
    events.push(Ok(ModelStreamEvent::Usage(ModelUsage {
        input_tokens_uncached: 100,
        output_tokens: 50,
        total_tokens: Some(150),
        ..ModelUsage::default()
    })));
    events.push(Ok(ModelStreamEvent::ResponseCompleted {
        stop_reason: reason,
    }));
    consume_events(
        request.invocation.invocation_id,
        ModelRouteRef::from_key(request.model.route.route_ref.clone()),
        Box::pin(futures_util::stream::iter(events)),
        Some(Duration::from_secs(1)),
        None,
        &mut CompactionExecutionObservation::default(),
        output_policy(request),
    )
    .await
}
fn step(output: ContextCompactionModelOutput) -> ContextCompactionStep {
    ContextCompactionStep {
        invocation_id: output.invocation_id,
        model_route_ref: output.model_route_ref,
        checkpoint: output.checkpoint,
    }
}

#[tokio::test]
async fn multiple_output_limits_preserve_exact_prefix_and_only_publish_complete_checkpoint() {
    let original = request();
    let mut request = original.clone();
    let pieces = [
        "## Goal\n继续任务。\n## Constraints & Preferences\n- Preserve ",
        "文件🦀 and whitespace.\n## Progress\n### Done\n- Inspected.\n### In Progress\n- Implement",
        " the fix.\n### Blocked\n- None.\n## Key Decisions\n- Keep source truth.\n## Next Steps\n- Verify.\n## Critical Context\n- End.",
    ];
    for (index, piece) in pieces.iter().enumerate() {
        let reason = [
            StopReason::MaxOutputTokens,
            StopReason::TokenLimit,
            StopReason::Stop,
        ][index];
        let output = response(&request, piece, reason).await.unwrap();
        assert_eq!(output.checkpoint.text().as_str(), pieces[..=index].concat());
        assert_eq!(output.usage.total_tokens, Some(150));
        let result = step(output);
        if index < 2 {
            assert!(
                result.clone().into_complete().is_err(),
                "partial text cannot become an authoritative checkpoint"
            );
            let parent = result.invocation_id;
            let next =
                crate::compaction_continuation::next_request(request.clone(), result).unwrap();
            assert_eq!(next.serialized_history, original.serialized_history);
            assert_eq!(next.previous_summary, original.previous_summary);
            assert_ne!(
                next.logical_plan_fingerprint().unwrap(),
                request.logical_plan_fingerprint().unwrap()
            );
            let canonical = canonical_request(&next, next.invocation.clone()).unwrap();
            assert_eq!(canonical.input[1].role, CanonicalRole::Assistant);
            assert_eq!(
                canonical.input[1].content,
                vec![CanonicalContentPart::Text(
                    next.continuation.as_ref().unwrap().prefix.clone()
                )]
            );
            assert_eq!(canonical.invocation.parent_invocation_id, Some(parent));
            assert_eq!(canonical.max_output_tokens, original.max_output_tokens);
            assert_eq!(canonical.reasoning_policy, ReasoningPolicy::Disabled);
            request = next;
        } else {
            assert_eq!(
                result.into_complete().unwrap().summary.as_str(),
                pieces.concat()
            );
        }
    }
}

#[tokio::test]
async fn nonprogress_refusal_and_cancellation_never_authorize_another_generation() {
    let root = request();
    let first = response(
        &root,
        "## Goal\nPreserve source. ",
        StopReason::MaxOutputTokens,
    )
    .await
    .unwrap();
    let next = crate::compaction_continuation::next_request(root, step(first)).unwrap();
    for (text, reason) in [
        ("", StopReason::TokenLimit),
        ("  \n", StopReason::MaxOutputTokens),
        ("## Goal\nPreserve source. ", StopReason::TokenLimit),
        ("denied", StopReason::ContentFilter),
        ("cancelled", StopReason::Cancelled),
    ] {
        let failure = response(&next, text, reason).await.unwrap_err();
        assert_eq!(failure.error.code, ErrorCode::ModelResponseInvalid);
        assert!(!failure.error.retryable);
        assert_eq!(
            failure.usage.unwrap().output_tokens,
            50,
            "actual usage survives rejection"
        );
    }
}

#[test]
fn partial_content_and_continuation_identity_cannot_alias_complete_summary_or_other_source() {
    let root = request();
    let parent = InvocationId::generate();
    let mut next = crate::compaction_continuation::next_request(
        root.clone(),
        ContextCompactionStep {
            invocation_id: parent,
            model_route_ref: ModelRouteRef::from_key(root.model.route.route_ref.clone()),
            checkpoint: CompactionCheckpoint::Incomplete(
                ModelText::new("## Goal\npartial ").unwrap(),
            ),
        },
    )
    .unwrap();
    let exact = next.expected_plan_fingerprint().unwrap();
    next.continuation.as_mut().unwrap().prefix = ModelText::new("## Goal\npartial").unwrap();
    assert_ne!(
        exact,
        next.expected_plan_fingerprint().unwrap(),
        "trailing whitespace is identity"
    );
    assert!(
        next.validate().is_err(),
        "mutated input cannot reuse a sealed request"
    );
    let partial = CompactionCheckpoint::recover(
        crate::ContextPlanContentKind::CompactionPartial
            .media_type()
            .as_str(),
        "## Goal\npartial ".into(),
    )
    .unwrap();
    assert!(matches!(partial, CompactionCheckpoint::Incomplete(_)));
    assert!(CompactionCheckpoint::recover("text/plain", "partial".into()).is_err());
}
