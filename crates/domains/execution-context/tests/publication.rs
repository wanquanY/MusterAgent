#![cfg(test)]

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::{ContentId, InteractionId};

fn message(media: &str, len: u64) -> PublicDeliveryContent {
    PublicDeliveryContent::Message(ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media).unwrap(),
        byte_length: len,
        sha256: Sha256::digest("a useful answer"),
    })
}

#[test]
fn public_message_cannot_embed_executable_markup_or_empty_output() {
    for candidate in [
        message("text/html", 10),
        message("application/json", 10),
        message("text/plain", 0),
    ] {
        assert_eq!(
            candidate.validate(DialoguePublicationPurpose::Reply),
            Err(DialoguePublicationContractError::InvalidMessage)
        );
    }
    for media in [
        "text/markdown",
        "text/markdown;charset=utf-8",
        "text/plain;charset=utf-8",
    ] {
        assert!(
            message(media, 10)
                .validate(DialoguePublicationPurpose::Reply)
                .is_ok()
        );
    }
}

#[test]
fn interaction_is_a_real_owner_reference_for_a_user_decision() {
    let id = InteractionId::generate();
    let value = PublicDeliveryContent::Interaction(id);
    assert_eq!(
        value.validate(DialoguePublicationPurpose::Result),
        Err(DialoguePublicationContractError::PurposeMismatch)
    );
    assert!(
        value
            .validate(DialoguePublicationPurpose::RequiredDecision)
            .is_ok()
    );
    assert_eq!(
        value.public_content(),
        AgentDialogueContent::Interaction { interaction_id: id }
    );
}
