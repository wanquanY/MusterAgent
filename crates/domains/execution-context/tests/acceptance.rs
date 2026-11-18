#![cfg(test)]

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::*;
fn dialogue() -> AgentDialogue {
    AgentDialogue {
        dialogue_id: AgentDialogueId::generate(),
        agent_id: AgentId::generate(),
        tenant_id: TenantId::generate(),
        owner_id: PrincipalId::generate(),
        head: DialogueCursor { epoch: 1, seq: 10 },
        created_at: UtcTimestamp::from_unix_millis(1),
    }
}
fn intent(d: &AgentDialogue) -> UserIntent {
    UserIntent {
        reply_binding: None,

        input_attachments: vec![],
        intent_id: UserIntentId::generate(),
        command_id: CommandId::generate(),
        dialogue_id: d.dialogue_id,
        tenant_id: d.tenant_id,
        owner_id: d.owner_id,
        source_entry_id: DialogueEntryId::generate(),
        content_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/plain").unwrap(),
            byte_length: 4,
            sha256: Sha256::digest("task"),
        },
        origin: DialogueInputOrigin {
            surface: DialogueInputSurface::Web,
            installation_id: None,
        },
        observed_cursor: DialogueCursor { epoch: 1, seq: 0 },
        reply_to_intent_id: None,
        task_target: None,
        accepted_cursor: d.head.next().unwrap(),
        accepted_at: UtcTimestamp::from_unix_millis(2),
    }
}
#[test]
fn stale_visible_watermark_does_not_block_independent_intents() {
    let mut d = dialogue();
    let a = intent(&d);
    assert!(a.validate_for(&d).is_ok());
    d.head = a.accepted_cursor;
    let b = intent(&d);
    assert!(b.validate_for(&d).is_ok());
    assert_eq!(b.accepted_cursor.seq, a.accepted_cursor.seq + 1);
    assert_eq!(a.public_entry().source_intent_id, Some(a.intent_id));
    assert!(a.public_entry().author_agent_id.is_none());
}
#[test]
fn account_and_dialogue_identity_are_checked() {
    let d = dialogue();
    let mut a = intent(&d);
    a.owner_id = PrincipalId::generate();
    assert_eq!(a.validate_for(&d), Err(AgentDialogueError::ScopeMismatch));
    a.owner_id = d.owner_id;
    a.tenant_id = TenantId::generate();
    assert_eq!(a.validate_for(&d), Err(AgentDialogueError::ScopeMismatch));
    a.tenant_id = d.tenant_id;
    a.dialogue_id = AgentDialogueId::generate();
    assert_eq!(a.validate_for(&d), Err(AgentDialogueError::ScopeMismatch));
}
#[test]
fn cursors_never_cross_epochs_or_accept_future_observation() {
    let d = dialogue();
    let mut a = intent(&d);
    a.observed_cursor.epoch = 2;
    assert_eq!(a.validate_for(&d), Err(AgentDialogueError::EpochMismatch));
    a.observed_cursor = DialogueCursor { epoch: 1, seq: 11 };
    assert_eq!(a.validate_for(&d), Err(AgentDialogueError::FutureCursor));
    assert_eq!(
        DialogueCursor {
            epoch: 1,
            seq: u64::MAX
        }
        .next(),
        Err(AgentDialogueError::SequenceExhausted)
    );
}
#[test]
fn native_origins_require_installation_but_web_does_not() {
    for surface in [DialogueInputSurface::Desktop, DialogueInputSurface::Mobile] {
        assert_eq!(
            DialogueInputOrigin {
                surface: surface.clone(),
                installation_id: None
            }
            .validate(),
            Err(AgentDialogueError::MissingOriginInstallation)
        );
        assert!(
            DialogueInputOrigin {
                surface,
                installation_id: Some(DeviceInstallationId::generate())
            }
            .validate()
            .is_ok()
        );
    }
    assert!(
        DialogueInputOrigin {
            surface: DialogueInputSurface::Web,
            installation_id: None
        }
        .validate()
        .is_ok()
    );
}
