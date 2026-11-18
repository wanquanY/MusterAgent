#![cfg(test)]

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_runtime_contract::{AttachmentRef, DisplayText};

fn attachment() -> DialogueInputAttachment {
    DialogueInputAttachment {
        attachment: AttachmentRef {
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("application/pdf").unwrap(),
                byte_length: 10,
                sha256: Sha256::digest("0123456789"),
            },
            display_name: DisplayText::new("计划.pdf").unwrap(),
        },
        source: DialogueInputAttachmentSource::Uploaded {
            upload_id: "a".repeat(64),
        },
    }
}
#[test]
fn provenance_names_duplicate_identities_and_count_are_validated() {
    let value = attachment();
    assert!(validate_dialogue_input_attachments(&vec![value.clone(); 64]).is_ok());
    assert!(validate_dialogue_input_attachments(&vec![value.clone(); 65]).is_err());
    for upload_id in ["A".repeat(64), "a".repeat(63), "z".repeat(64)] {
        assert!(
            DialogueInputAttachment {
                source: DialogueInputAttachmentSource::Uploaded { upload_id },
                ..value.clone()
            }
            .validate()
            .is_err()
        );
    }
    for name in ["../计划.pdf", "x/计划.pdf", "x\\计划.pdf", ".."] {
        let mut changed = value.clone();
        changed.attachment.display_name = DisplayText::new(name).unwrap();
        assert!(changed.validate().is_err());
    }
    let mut changed = value.clone();
    changed.attachment.content_ref.sha256 = Sha256::digest("other");
    assert!(validate_dialogue_input_attachments(&[value, changed]).is_err());
}
#[test]
fn canonical_bytes_bind_every_field_and_order_without_delimiter_collisions() {
    let a = attachment();
    let b = attachment();
    assert_ne!(
        dialogue_input_attachment_bytes(&[a.clone(), b.clone()]),
        dialogue_input_attachment_bytes(&[b, a.clone()])
    );
    for changed in [
        DialogueInputAttachment {
            source: DialogueInputAttachmentSource::Uploaded {
                upload_id: "b".repeat(64),
            },
            ..a.clone()
        },
        DialogueInputAttachment {
            source: DialogueInputAttachmentSource::AcceptedInput {
                dialogue_id: AgentDialogueId::generate(),
                entry_id: DialogueEntryId::generate(),
                cursor: DialogueCursor { epoch: 1, seq: 1 },
                ordinal: 0,
            },
            ..a.clone()
        },
    ] {
        assert!(changed.validate().is_ok());
        assert_ne!(
            dialogue_input_attachment_bytes(std::slice::from_ref(&a)),
            dialogue_input_attachment_bytes(&[changed])
        );
    }
}
