//! Structural/canonical protocol checks. Valid-looking foreign IDs still need
//! original publication/account authorization; the codec never grants that.
use super::*;
use musteragent_content_ref::MediaType;
use prost::Message;
fn reference(label: &str) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").unwrap(),
        byte_length: label.len() as u64,
        sha256: Sha256::digest(label),
    }
}
fn binding() -> DialogueReplyBinding {
    let task = AgentTaskTarget {
        work_item_id: WorkItemId::generate(),
        goal_revision: 7,
        control_revision: 3,
    };
    let run = RunId::generate();
    DialogueReplyBinding {
        target: DialogueReplyTarget {
            entry_id: DialogueEntryId::generate(),
            visible_revision: 9,
        },
        agent_id: AgentId::generate(),
        dialogue_id: AgentDialogueId::generate(),
        cursor: DialogueCursor { epoch: 1, seq: 10 },
        content_ref: reference("exact public result"),
        publication: Some(DialogueReplyPublication {
            delivery_id: CommandId::generate(),
            conversation_id: ConversationId::generate(),
            run_id: run,
            payload_sha256: Sha256::digest("original publication"),
        }),
        subject: DialogueReplySubject::WorkItemResult {
            target: task,
            assignment_epoch: 2,
            producing_run_id: run,
            report_run_id: None,
            report_content_ref: None,
            action_target: AgentTaskActionTarget {
                target: AgentTaskTarget {
                    work_item_id: WorkItemId::generate(),
                    goal_revision: 2,
                    control_revision: 1,
                },
                assignment_epoch: 4,
                chain_root_id: task.work_item_id,
                chain_sequence: 3,
            },
        },
    }
}
fn accepted(b: Option<DialogueReplyBinding>) -> UserIntent {
    UserIntent {
        intent_id: UserIntentId::generate(),
        command_id: CommandId::generate(),
        dialogue_id: b
            .as_ref()
            .map(|b| b.dialogue_id)
            .unwrap_or_else(AgentDialogueId::generate),
        tenant_id: TenantId::generate(),
        owner_id: PrincipalId::generate(),
        source_entry_id: DialogueEntryId::generate(),
        content_ref: reference("new user instruction"),
        input_attachments: vec![],
        origin: DialogueInputOrigin {
            surface: DialogueInputSurface::Web,
            installation_id: None,
        },
        observed_cursor: DialogueCursor { epoch: 1, seq: 10 },
        reply_to_intent_id: None,
        task_target: None,
        reply_binding: b,
        accepted_cursor: DialogueCursor { epoch: 1, seq: 11 },
        accepted_at: UtcTimestamp::from_unix_millis(200),
    }
}
fn work(v: &mut wire::DialogueReplyBinding) -> &mut wire::DialogueReplyWorkItemResult {
    match v.subject.as_mut().unwrap() {
        wire::dialogue_reply_binding::Subject::WorkItemResult(w) => w,
        _ => panic!("task fixture"),
    }
}
#[test]
fn every_reply_subject_roundtrips_canonical_binding_and_accepted_custody() {
    let direct = binding();
    let mut report = direct.clone();
    let report_run = RunId::generate();
    report.publication.as_mut().unwrap().run_id = report_run;
    if let DialogueReplySubject::WorkItemResult {
        report_run_id,
        report_content_ref,
        ..
    } = &mut report.subject
    {
        *report_run_id = Some(report_run);
        *report_content_ref = Some(reference("original task report"));
    }
    let mut goal = direct.clone();
    goal.subject = DialogueReplySubject::GoalResult {
        goal_id: GoalId::generate(),
        binding_revision: 2,
        rules_version: Sha256::digest("frozen rules"),
    };
    let mut output = direct.clone();
    output.subject = DialogueReplySubject::IntentOutput {
        intent_id: UserIntentId::generate(),
    };
    let mut user = output.clone();
    user.publication = None;
    user.subject = DialogueReplySubject::AcceptedInput {
        intent_id: UserIntentId::generate(),
    };
    for b in [direct, report, goal, output, user] {
        b.validate().unwrap();
        let encoded = reply_binding(&b).encode_to_vec();
        let restored =
            decode_reply_binding(wire::DialogueReplyBinding::decode(encoded.as_slice()).unwrap())
                .unwrap();
        assert_eq!(restored, b);
        assert_eq!(restored.canonical_bytes(), b.canonical_bytes());
        assert_eq!(reply_binding(&restored).encode_to_vec(), encoded);
        let input = accepted(Some(b));
        let bytes = intent(&input).encode_to_vec();
        assert_eq!(
            decode_intent(wire::DialogueUserIntent::decode(bytes.as_slice()).unwrap()).unwrap(),
            input
        );
    }
    let old = accepted(None);
    let wire = intent(&old);
    assert!(wire.reply_target.is_none() && wire.reply_binding.is_none());
    assert_eq!(decode_intent(wire).unwrap(), old);
}
#[test]
fn selected_version_and_current_action_are_independent_but_both_committed() {
    let b = binding();
    let selected = b.task_result().unwrap();
    let action = b.task_action().unwrap();
    assert_ne!(selected.work_item_id, action.target.work_item_id);
    let bytes = b.canonical_bytes();
    let wire = reply_binding(&b).encode_to_vec();
    let mut variants = Vec::new();
    let mut v = b.clone();
    v.target.visible_revision += 1;
    variants.push(v);
    let mut v = b.clone();
    v.target.entry_id = DialogueEntryId::generate();
    variants.push(v);
    let mut v = b.clone();
    v.content_ref = reference("another version");
    variants.push(v);
    let mut v = b.clone();
    v.publication.as_mut().unwrap().delivery_id = CommandId::generate();
    variants.push(v);
    let mut v = b.clone();
    v.publication.as_mut().unwrap().payload_sha256 = Sha256::digest("other receipt");
    variants.push(v);
    for field in 0..5 {
        let mut v = b.clone();
        if let DialogueReplySubject::WorkItemResult {
            target,
            assignment_epoch,
            action_target,
            ..
        } = &mut v.subject
        {
            match field {
                0 => target.goal_revision += 1,
                1 => *assignment_epoch += 1,
                2 => action_target.target.control_revision += 1,
                3 => action_target.assignment_epoch += 1,
                _ => action_target.chain_sequence += 1,
            }
        }
        variants.push(v);
    }
    for v in variants {
        v.validate().unwrap();
        assert_ne!(v.canonical_bytes(), bytes);
        assert_ne!(reply_binding(&v).encode_to_vec(), wire);
    }
    assert_eq!(b.task_result(), Some(selected));
    assert_eq!(b.task_action(), Some(action));
}
#[test]
fn broken_subject_publication_report_and_action_shapes_are_rejected() {
    let original = reply_binding(&binding());
    let mut invalid = Vec::new();
    let mut v = original.clone();
    v.target = None;
    invalid.push(v);
    let mut v = original.clone();
    v.target.as_mut().unwrap().visible_revision = 0;
    invalid.push(v);
    let mut v = original.clone();
    v.subject = None;
    invalid.push(v);
    let mut v = original.clone();
    v.publication = None;
    invalid.push(v);
    let mut v = original.clone();
    v.subject = Some(wire::dialogue_reply_binding::Subject::AcceptedInput(
        wire::DialogueReplyAcceptedInput {
            intent_id: Some(wire::UserIntentId {
                value: UserIntentId::generate().to_string(),
            }),
        },
    ));
    invalid.push(v);
    let mut v = original.clone();
    work(&mut v).action_target = None;
    invalid.push(v);
    let mut v = original.clone();
    work(&mut v).action_target.as_mut().unwrap().chain_root_id = "not-a-task-id".into();
    invalid.push(v);
    let mut v = original.clone();
    work(&mut v).action_target.as_mut().unwrap().chain_sequence = 1;
    invalid.push(v);
    let mut v = original.clone();
    work(&mut v)
        .action_target
        .as_mut()
        .unwrap()
        .assignment_epoch = 0;
    invalid.push(v);
    let mut v = original.clone();
    work(&mut v).target.as_mut().unwrap().control_revision = 0;
    invalid.push(v);
    let mut v = original.clone();
    work(&mut v).producing_run_id = Some(wire::RunId {
        value: RunId::generate().to_string(),
    });
    invalid.push(v);
    let mut v = original.clone();
    work(&mut v).report_run_id = Some(wire::RunId {
        value: RunId::generate().to_string(),
    });
    invalid.push(v);
    let mut v = original.clone();
    let w = work(&mut v);
    w.report_content_ref = Some(crate::encode_content_ref(&reference("report")));
    w.report_run_id = Some(wire::RunId {
        value: RunId::generate().to_string(),
    });
    invalid.push(v);
    let mut v = original;
    v.subject = Some(wire::dialogue_reply_binding::Subject::GoalResult(
        wire::DialogueReplyGoalResult {
            goal_id: Some(wire::GoalId {
                value: GoalId::generate().to_string(),
            }),
            binding_revision: 1,
            rules_version: vec![1; 31],
        },
    ));
    invalid.push(v);
    for value in invalid {
        assert_eq!(decode_reply_binding(value), Err(Error::Corrupt));
    }
}
#[test]
fn accepted_selection_mirror_scope_and_cut_must_match_the_frozen_binding() {
    let original = intent(&accepted(Some(binding())));
    let mut invalid = Vec::new();
    let mut v = original.clone();
    v.reply_target = None;
    invalid.push(v);
    let mut v = original.clone();
    v.reply_binding = None;
    invalid.push(v);
    let mut v = original.clone();
    v.reply_target.as_mut().unwrap().visible_revision += 1;
    invalid.push(v);
    let mut v = original.clone();
    v.reply_target.as_mut().unwrap().entry_id = Some(wire::DialogueEntryId {
        value: DialogueEntryId::generate().to_string(),
    });
    invalid.push(v);
    let mut v = original.clone();
    v.reply_binding.as_mut().unwrap().dialogue_id = Some(wire::AgentDialogueId {
        value: AgentDialogueId::generate().to_string(),
    });
    invalid.push(v);
    let mut v = original.clone();
    v.reply_binding
        .as_mut()
        .unwrap()
        .cursor
        .as_mut()
        .unwrap()
        .seq = 11;
    invalid.push(v);
    let mut v = original;
    v.reply_binding
        .as_mut()
        .unwrap()
        .cursor
        .as_mut()
        .unwrap()
        .epoch = 2;
    invalid.push(v);
    for value in invalid {
        assert_eq!(decode_intent(value), Err(Error::Corrupt));
    }
}
