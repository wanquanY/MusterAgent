use super::*;
use musteragent_approval::InteractionStatus;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef};
use musteragent_identity::*;
use serde_json::{Value, json};

fn fixture() -> (Interaction, Value) {
    let invocation = InvocationId::generate();
    let prompt = json!({"schema":"musteragent.tool.approval.prompt.v4", "invocation_id": invocation.to_string(),
        "consequence_fingerprint": Sha256::digest(b"original consequence").to_string(), "session_reusable":true,
        "capability_detail":{"session_reusable":true}});
    let mut interaction = Interaction {
        interaction_id: InteractionId::generate(),
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        activity_id: ActivityId::generate(),
        run_id: RunId::generate(),
        requested_by_invocation_id: invocation,
        kind: InteractionKind::Approval,
        status: InteractionStatus::Pending,
        prompt_ref: ContentRef {
            content_id: ContentId::generate(),
            sha256: Sha256::digest(b""),
            byte_length: 0,
            media_type: MediaType::new("application/json").expect("media"),
        },
        response_schema: SchemaRef {
            schema_key: VersionedKey::new(APPROVAL_RESPONSE_SCHEMA_KEY).expect("key"),
            schema_hash: Sha256::digest(APPROVAL_RESPONSE_SCHEMA),
        },
        response_ref: None,
        expires_at: None,
        requested_at: UtcTimestamp::from_unix_millis(1),
        responded_by: None,
        responded_at: None,
    };
    bind(&mut interaction, &prompt);
    (interaction, prompt)
}
fn bind(interaction: &mut Interaction, prompt: &Value) -> Vec<u8> {
    let bytes = serde_json::to_vec(prompt).expect("prompt");
    interaction.prompt_ref.sha256 = Sha256::digest(&bytes);
    interaction.prompt_ref.byte_length = bytes.len() as u64;
    bytes
}

#[test]
fn session_response_requires_explicit_original_runtime_permission_in_known_versions() {
    let (mut interaction, mut prompt) = fixture();
    for schema in [
        "musteragent.tool.approval.prompt.v4",
        "musteragent.legacy.tool.approval.prompt.v3",
    ] {
        prompt["schema"] = json!(schema);
        let bytes = bind(&mut interaction, &prompt);
        assert_eq!(validate_session_approval(&interaction, &bytes), Ok(()));
    }
    prompt["session_reusable"] = json!(false);
    let bytes = bind(&mut interaction, &prompt);
    assert_eq!(
        validate_session_approval(&interaction, &bytes),
        Err(Error::InvalidResponse)
    );
    assert_eq!(interaction.status, InteractionStatus::Pending);
    assert!(interaction.response_ref.is_none());
}

#[test]
fn provider_detail_missing_flags_foreign_invocations_and_future_contracts_never_confer_reuse() {
    for (field, value) in [
        ("schema", json!("musteragent.tool.approval.prompt.v99")),
        ("invocation_id", json!(InvocationId::generate().to_string())),
        ("consequence_fingerprint", json!("not-a-fingerprint")),
        ("session_reusable", json!("true")),
        ("session_reusable", Value::Null),
    ] {
        let (mut interaction, mut prompt) = fixture();
        prompt[field] = value;
        let bytes = bind(&mut interaction, &prompt);
        assert_eq!(
            validate_session_approval(&interaction, &bytes),
            Err(Error::ContractMismatch)
        );
    }
    let (mut interaction, mut prompt) = fixture();
    prompt
        .as_object_mut()
        .expect("object")
        .remove("session_reusable");
    let bytes = bind(&mut interaction, &prompt);
    assert_eq!(
        validate_session_approval(&interaction, &bytes),
        Err(Error::ContractMismatch)
    );
}

#[test]
fn corruption_and_schema_or_kind_drift_are_rejected_before_the_interaction_closes() {
    for drift in 0..5 {
        let (mut interaction, prompt) = fixture();
        let mut bytes = bind(&mut interaction, &prompt);
        match drift {
            0 => bytes.push(b' '),
            1 => interaction.response_schema.schema_hash = Sha256::digest(b"other"),
            2 => {
                interaction.response_schema.schema_key =
                    VersionedKey::new("interaction.approval.response@2").expect("key")
            }
            3 => interaction.kind = InteractionKind::Confirmation,
            _ => interaction.prompt_ref.media_type = MediaType::new("text/plain").expect("media"),
        }
        assert_eq!(
            validate_session_approval(&interaction, &bytes),
            Err(Error::ContractMismatch)
        );
    }
}

#[test]
fn command_validator_reads_only_the_original_tenant_prompt_before_session_permission() {
    use crate::{InteractionResponseValidator, StrictInteractionResponseValidator};
    use musteragent_approval::{ApprovalDecision, InteractionResponseRef};
    use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
    use musteragent_runtime_contract::ApplicationFuture;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::{Context, Poll, Waker};

    struct Store {
        interaction: Interaction,
        bytes: Vec<u8>,
        reads: AtomicUsize,
        unavailable: bool,
    }
    impl ContentStore for Store {
        fn put(
            &self,
            _: PutContent,
        ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
            panic!("validation cannot write or substitute the original prompt")
        }
        fn read<'a>(
            &'a self,
            tenant: TenantId,
            content: &'a ContentRef,
        ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
            assert_eq!(tenant, self.interaction.tenant_id);
            assert_eq!(*content, self.interaction.prompt_ref);
            self.reads.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if self.unavailable {
                    Err(ContentStoreError::Unavailable)
                } else {
                    Ok(self.bytes.clone())
                }
            })
        }
    }
    for (reusable, unavailable) in [(true, false), (false, false), (true, true)] {
        let (mut interaction, mut prompt) = fixture();
        prompt["session_reusable"] = json!(reusable);
        let bytes = bind(&mut interaction, &prompt);
        let store = Arc::new(Store {
            interaction: interaction.clone(),
            bytes,
            reads: AtomicUsize::new(0),
            unavailable,
        });
        let validator = StrictInteractionResponseValidator::new(store.clone());
        for decision in [ApprovalDecision::Approve, ApprovalDecision::Deny] {
            let response = InteractionResponseRef::Approval(decision);
            assert_eq!(
                validator
                    .validate(&interaction, &response)
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            );
        }
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let response = InteractionResponseRef::Approval(ApprovalDecision::ApproveSession);
        let expected = if unavailable {
            Err(Error::ContentUnavailable)
        } else if reusable {
            Ok(())
        } else {
            Err(Error::InvalidResponse)
        };
        assert_eq!(
            validator
                .validate(&interaction, &response)
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(expected)
        );
        assert_eq!(store.reads.load(Ordering::SeqCst), 1);
        interaction.kind = InteractionKind::Confirmation;
        assert_eq!(
            validator
                .validate(&interaction, &response)
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(Error::InvalidResponse))
        );
        assert_eq!(
            store.reads.load(Ordering::SeqCst),
            1,
            "confirmation cannot confer reuse"
        );
    }
}
