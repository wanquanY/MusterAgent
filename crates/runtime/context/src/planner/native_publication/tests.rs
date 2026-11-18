use super::*;
use crate::planner::tests::{content_ref, model_route, user_entry};
use musteragent_execution_context::PublicDeliveryContract;
use musteragent_identity::DottedKey;
use musteragent_model::{ModelKey, SignedModelRouteHash};

fn assistant(run: RunId) -> ContextEntry {
    let mut value = user_entry(9, "private completed response");
    value.source_run_id = Some(run);
    value.message.role = CanonicalRole::Assistant;
    let route = model_route();
    value.assistant_origin = Some(crate::AssistantOrigin {
        invocation_id: InvocationId::generate(),
        model_route_ref: route.route_ref.clone(),
        route_identity_hash: SignedModelRouteHash::digest(&route).unwrap(),
        requested_model_key: ModelKey::new(route.resolved_model_key.clone()).unwrap(),
        response_model_key: ModelKey::new(route.resolved_model_key).unwrap(),
    });
    value
}

#[test]
fn native_publication_uses_only_latest_proven_current_run_completion() {
    let run = RunId::generate();
    let value = assistant(run);
    let invocation = value.assistant_origin.as_ref().unwrap().invocation_id;
    assert_eq!(source(&[&value], run), Some(invocation));
    assert_eq!(source(&[&value], RunId::generate()), None);
    for role in [CanonicalRole::User, CanonicalRole::Tool] {
        let mut next = user_entry(10, "new source wins");
        next.message.role = role;
        assert_eq!(source(&[&value, &next], run), None);
    }
    let mut unknown = value.clone();
    unknown.assistant_origin = None;
    assert_eq!(source(&[&unknown], run), None);
    let mut unproven = value.clone();
    unproven.source_record_ids.clear();
    assert_eq!(source(&[&unproven], run), None);
    let mut limited = value.clone();
    limited.continuation_delta_for = Some(invocation);
    assert_eq!(source(&[&limited], run), None);
    for event in [
        CanonicalRuntimeEvent::ModelRecovery {
            source_invocation_id: invocation,
        },
        CanonicalRuntimeEvent::OutputLimitContinuation {
            source_invocation_id: invocation,
        },
    ] {
        let mut newer = value.clone();
        newer.runtime_events.push(event);
        assert_eq!(source(&[&value, &newer], run), None);
    }
}

fn contract(value: DialogueOutputContract) -> (PromptSectionSnapshot, MaterializedContent) {
    let bytes = value.prompt().into_bytes();
    let content_ref = content_ref(&bytes);
    (
        PromptSectionSnapshot {
            section_key: DottedKey::new("dialogue.output-contract").unwrap(),
            content_ref: content_ref.clone(),
        },
        MaterializedContent { content_ref, bytes },
    )
}

#[test]
fn native_publication_requires_unique_exact_sealed_native_contract_bytes() {
    let (pin, content) = contract(DialogueOutputContract::NativeToolsV1);
    assert!(
        sealed_native_contract(std::slice::from_ref(&pin), std::slice::from_ref(&content)).unwrap()
    );
    assert!(!sealed_native_contract(&[], std::slice::from_ref(&content)).unwrap());
    assert!(
        sealed_native_contract(&[pin.clone(), pin.clone()], std::slice::from_ref(&content))
            .is_err()
    );
    assert!(sealed_native_contract(std::slice::from_ref(&pin), &[]).is_err());
    let mut changed = content.clone();
    changed.bytes.push(b'x');
    assert!(sealed_native_contract(std::slice::from_ref(&pin), &[changed]).is_err());
    assert!(sealed_native_contract(&[pin], &[content.clone(), content]).is_err());
    for version in [
        PublicDeliveryContract::V1,
        PublicDeliveryContract::V2,
        PublicDeliveryContract::V3,
        PublicDeliveryContract::V4,
        PublicDeliveryContract::V5,
        PublicDeliveryContract::V6,
        PublicDeliveryContract::V7,
    ] {
        let (pin, content) = contract(DialogueOutputContract::Response(version));
        assert!(!sealed_native_contract(&[pin], &[content]).unwrap());
    }
}
