use super::*;
use musteragent_content_ref::MediaType;
use musteragent_identity::{ContentId, PrincipalId, TenantId};

fn unchanged() -> PreparedAgentFileEdit {
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let request = AgentFileEditCommand {
        actor,
        dialogue_id: AgentDialogueId::generate(),
        entry_id: DialogueEntryId::generate(),
        expected_head: ArtifactVersionId::generate(),
        command_id: CommandId::generate(),
        change: AgentFileEditChange::Text("保留正文\r\n".into()),
        now: UtcTimestamp::from_unix_millis(1000),
    };
    let agent_id = AgentId::generate();
    let context = ResourceCommandContext {
        tenant_id: actor.tenant_id,
        actor,
        source: crate::ResourceCommandSource::Account {
            agent_id,
            dialogue_id: request.dialogue_id,
            entry_id: request.entry_id,
        },
        invocation_id: None,
        tool_key: None,
        now: request.now,
    };
    let content = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/markdown").unwrap(),
        byte_length: "保留正文\r\n".len() as u64,
        sha256: Sha256::digest("保留正文\r\n".as_bytes()),
    };
    let target = AgentFileEditTarget {
        context: context.clone(),
        agent_id,
        artifact_id: ArtifactId::generate(),
        file_node_id: ResourceNodeId::generate(),
        head_id: request.expected_head,
        head_version: VersionNumber::new(3).unwrap(),
        head_content: content,
        restore_content: None,
    };
    let key = IdempotencyKey::new(request.command_id.to_string()).unwrap();
    PreparedAgentFileEdit {
        begin: BeginFileEdit {
            context: context.clone(),
            session_id: request.session_id().unwrap(),
            file_node_id: target.file_node_id,
            base_version: target.head_version,
            format_adapter: VersionedKey::new("dialogue.text@1").unwrap(),
            idempotency_key: key.clone(),
            command_fingerprint: request.fingerprint().unwrap(),
            expires_at: UtcTimestamp::from_unix_millis(301000),
        },
        finalize: FinalizeFileEdit {
            context,
            command_id: request.terminal_id().unwrap(),
            session_id: request.session_id().unwrap(),
            artifact_version_id: ArtifactVersionId::from_uuid(derived(
                request.command_id,
                b"agent-edit-version@1",
            ))
            .unwrap(),
            committed_by: ArtifactVersionAuthor::ProductPrincipal(actor.principal_id),
            idempotency_key: key,
            expected_session_revision: ResourceRevision::new(0),
        },
        fingerprint: request.fingerprint().unwrap(),
        request,
        target,
        checkpoint: None,
    }
}

#[test]
fn retry_fingerprint_preserves_command_identity_but_not_clock() {
    let initial = unchanged().request;
    let mut retry = initial.clone();
    retry.now = UtcTimestamp::from_unix_millis(900000);
    assert_eq!(initial.fingerprint().unwrap(), retry.fingerprint().unwrap());
    let mutations: [fn(&mut AgentFileEditCommand); 4] = [
        |r: &mut AgentFileEditCommand| r.entry_id = DialogueEntryId::generate(),
        |r: &mut AgentFileEditCommand| r.actor.principal_id = PrincipalId::generate(),
        |r: &mut AgentFileEditCommand| r.expected_head = ArtifactVersionId::generate(),
        |r: &mut AgentFileEditCommand| {
            r.change = AgentFileEditChange::Restore(ArtifactVersionId::generate())
        },
    ];
    for mutate in mutations {
        let mut changed = initial.clone();
        mutate(&mut changed);
        assert_ne!(
            initial.fingerprint().unwrap(),
            changed.fingerprint().unwrap()
        );
    }
}

#[test]
fn prepared_commit_cannot_substitute_owner_target_author_or_derived_receipt() {
    unchanged().validate().unwrap();
    let mutations: Vec<fn(&mut PreparedAgentFileEdit)> = vec![
        |p| p.target.agent_id = AgentId::generate(),
        |p| p.begin.file_node_id = ResourceNodeId::generate(),
        |p| p.finalize.artifact_version_id = ArtifactVersionId::generate(),
        |p| p.finalize.command_id = ResourceEditTerminalCommandId::generate(),
        |p| {
            p.finalize.committed_by =
                ArtifactVersionAuthor::ProductPrincipal(PrincipalId::generate())
        },
        |p| p.finalize.idempotency_key = IdempotencyKey::new("another-operation").unwrap(),
        |p| p.begin.format_adapter = VersionedKey::new("other.text@1").unwrap(),
        |p| p.target.head_content.byte_length += 1,
        |p| p.target.restore_content = Some(p.target.head_content.clone()),
        |p| p.target.context.invocation_id = Some(musteragent_identity::InvocationId::generate()),
    ];
    for mutate in mutations {
        let mut candidate = unchanged();
        mutate(&mut candidate);
        assert!(candidate.validate().is_err());
    }
}

#[test]
fn unchanged_restore_still_requires_authorized_same_media_source() {
    let mut candidate = unchanged();
    candidate.request.change = AgentFileEditChange::Restore(ArtifactVersionId::generate());
    candidate.fingerprint = candidate.request.fingerprint().unwrap();
    candidate.begin.command_fingerprint = candidate.fingerprint;
    assert!(candidate.validate().is_err());
    candidate.target.restore_content = Some(candidate.target.head_content.clone());
    candidate.validate().unwrap();
    candidate
        .target
        .restore_content
        .as_mut()
        .unwrap()
        .media_type = MediaType::new("image/svg+xml").unwrap();
    assert!(candidate.validate().is_err());
}

#[test]
fn direct_editor_budget_does_not_raise_mime_limits_or_reduce_generator_limits() {
    let csv = MediaType::new("text/csv").unwrap();
    assert_eq!(
        musteragent_artifact::file_capability(&csv).maximum_bytes,
        50 * 1024 * 1024
    );
    assert_eq!(text_edit_budget(&csv).unwrap(), AGENT_FILE_EDIT_MAX_BYTES);
    assert_eq!(
        text_edit_budget(&MediaType::new("application/yaml").unwrap()).unwrap(),
        2 * 1024 * 1024
    );
    for mime in ["image/svg+xml", "image/png", "application/octet-stream"] {
        assert!(text_edit_budget(&MediaType::new(mime).unwrap()).is_err());
    }
    let mut candidate = unchanged();
    candidate.target.head_content.byte_length = AGENT_FILE_EDIT_MAX_BYTES + 1;
    assert!(matches!(
        candidate.validate(),
        Err(FileEditError::FileTooLarge)
    ));
}
