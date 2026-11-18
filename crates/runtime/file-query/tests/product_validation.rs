mod support;
use musteragent_identity::*;
use musteragent_runtime_file_query::*;
use support::Fixture;

fn entry(target: FileQueryTarget) -> FileEntry {
    let (conversation_id, workspace_id, scope, source) = match target {
        FileQueryTarget::Conversation(id) => (
            Some(id.to_string()),
            None,
            FileScope::Conversation,
            FileSource::Artifact {
                artifact_id: ArtifactId::generate().to_string(),
            },
        ),
        FileQueryTarget::Workspace(id) => (
            None,
            Some(id.to_string()),
            FileScope::Project,
            FileSource::Resource {
                node_id: ResourceNodeId::generate().to_string(),
            },
        ),
        FileQueryTarget::OwnedAgent(_) => (
            None,
            None,
            FileScope::OwnedAgent,
            FileSource::Resource {
                node_id: ResourceNodeId::generate().to_string(),
            },
        ),
    };
    FileEntry {
        production: None,
        native_workspace_id: None,
        file_ref: FileReference {
            owned_agent_id: if let FileQueryTarget::OwnedAgent(id) = target {
                Some(id.to_string())
            } else {
                None
            },
            schema: if scope == FileScope::OwnedAgent {
                AGENT_FILE_REFERENCE_SCHEMA
            } else {
                FILE_REFERENCE_SCHEMA
            }
            .into(),
            conversation_id,
            workspace_id,
            scope,
            source,
            revision: "original:1".into(),
        },
        name: "release.md".into(),
        is_directory: false,
        media_type: Some("text/markdown".into()),
        byte_length: Some(4),
        modified_at_ms: 1,
        associated_at_ms: None,
        semantic_kind: None,
        availability: FileAvailability::Available,
    }
}
#[test]
fn read_only_target_does_not_replace_authorized_context_and_preserves_both_target_shapes() {
    let f = Fixture::new();
    let mut context = f.context.clone();
    let target = context.target().expect("target");
    let value = entry(target);
    value.validate_target(target).expect("read-only shape");
    value.validate(&context).expect("context");
    context.authority_revision.clear();
    assert!(value.validate(&context).is_err());
    value.validate_target(target).expect("not authority");
    let workspace = FileQueryTarget::Workspace(WorkspaceId::generate());
    let project = entry(workspace);
    project.validate_target(workspace).expect("project target");
    assert!(project.validate_target(target).is_err());
    assert!(value.validate_target(workspace).is_err());
}
#[test]
fn native_read_shape_never_supplies_the_current_execution_workspace() {
    let f = Fixture::new();
    let mut context = f.context.clone();
    let target = context.target().expect("target");
    let workspace = WorkspaceId::generate();
    context.project_workspace_id = Some(workspace);
    let mut value = entry(target);
    value.file_ref.source = FileSource::NativeCreation {
        invocation_id: InvocationId::generate().to_string(),
        path: "result.md".into(),
    };
    value.native_workspace_id = Some(workspace.to_string());
    value.validate(&context).expect("original workspace");
    value.native_workspace_id = Some(WorkspaceId::generate().to_string());
    value.validate_target(target).expect("shape only");
    assert_eq!(value.validate(&context), Err(FileQueryError::Integrity));
    value.native_workspace_id = None;
    assert!(value.validate_target(target).is_err());
}

#[test]
fn explicit_metadata_refresh_allows_only_revision_change_and_never_weakens_exact_open() {
    for target in [
        FileQueryTarget::Conversation(ConversationId::generate()),
        FileQueryTarget::Workspace(WorkspaceId::generate()),
    ] {
        let original = entry(target);
        let mut current = original.clone();
        current.file_ref.revision = "updated:2".into();
        current
            .validate_refreshed_inspection(target, &original.file_ref)
            .expect("metadata");
        assert_eq!(
            current.validate_inspection(target, &original.file_ref),
            Err(FileQueryError::Integrity)
        );
        let f = Fixture::new();
        let descriptor = FileContentDescriptor {
            file_tree: None,
            resource_ref: None,
            entry: current.clone(),
            artifact_origin: None,
            primary: f.bytes.add("text/plain", b"text"),
            dependencies: vec![],
        };
        assert_eq!(
            descriptor.validate_target(target, &original.file_ref),
            Err(FileQueryError::Integrity)
        );
        for case in 0..7 {
            let mut invalid = current.clone();
            match case {
                0 => invalid.file_ref.revision.clear(),
                1 => {
                    invalid.file_ref.source = FileSource::Resource {
                        node_id: ResourceNodeId::generate().to_string(),
                    }
                }
                2 => {
                    invalid.file_ref.conversation_id = Some(ConversationId::generate().to_string())
                }
                3 => invalid.file_ref.workspace_id = Some(WorkspaceId::generate().to_string()),
                4 => {
                    invalid.file_ref.scope = if invalid.file_ref.scope == FileScope::Project {
                        FileScope::Conversation
                    } else {
                        FileScope::Project
                    }
                }
                5 => invalid.file_ref.schema = "unknown".into(),
                _ => invalid.native_workspace_id = Some(WorkspaceId::generate().to_string()),
            }
            assert!(
                invalid
                    .validate_refreshed_inspection(target, &original.file_ref)
                    .is_err(),
                "case {case}"
            );
        }
    }
}
#[test]
fn page_validation_preserves_scope_coverage_uniqueness_and_cursor_progress() {
    let target = FileQueryTarget::Conversation(ConversationId::generate());
    let value = entry(target);
    let query = ListFiles {
        produced_by: None,
        scope: FileScope::Conversation,
        parent: None,
        cursor: Some("previous".into()),
        limit: 1,
        name_query: None,
    };
    let page = FilePage {
        entries: vec![value],
        next_cursor: Some("next".into()),
        revision: "page:1".into(),
        coverage: FileCoverage::ProjectedConversation,
        retraction_status: None,
    };
    page.validate_target(target, &query).expect("original page");
    for case in 0..4 {
        let mut page = page.clone();
        match case {
            0 => page.next_cursor = query.cursor.clone(),
            1 => page.entries.clear(),
            2 => page.entries.push(page.entries[0].clone()),
            _ => page.coverage = FileCoverage::ProjectChildren,
        }
        assert_eq!(
            page.validate_target(target, &query),
            Err(FileQueryError::Integrity)
        );
    }
    let mut duplicate = page.clone();
    duplicate.entries.push(duplicate.entries[0].clone());
    assert_eq!(
        duplicate.validate_target(
            target,
            &ListFiles {
                limit: 2,
                ..query.clone()
            }
        ),
        Err(FileQueryError::Integrity)
    );
    let mut invalid = page;
    invalid.entries[0].file_ref.revision.clear();
    assert_eq!(
        invalid.validate_target(target, &query),
        Err(FileQueryError::InvalidReference)
    );
}
#[test]
fn descriptor_origin_and_original_content_identity_cannot_be_invented_from_a_filename() {
    let f = Fixture::new();
    let target = f.context.target().expect("target");
    let entry = entry(target);
    let reference = entry.file_ref.clone();
    let primary = f.bytes.add("text/plain", b"text");
    let mut value = FileContentDescriptor {
        file_tree: None,
        resource_ref: None,
        entry,
        artifact_origin: None,
        primary: primary.clone(),
        dependencies: vec![],
    };
    value.validate_target(target, &reference).expect("content");
    value.dependencies.push(primary);
    assert_eq!(
        value.validate_target(target, &reference),
        Err(FileQueryError::Integrity)
    );
    value.dependencies.clear();
    value.artifact_origin = Some(ResolvedArtifactVersion {
        artifact_id: ArtifactId::generate(),
        version: VersionNumber::new(1).expect("version"),
    });
    assert_eq!(
        value.validate_target(target, &reference),
        Err(FileQueryError::Integrity)
    );
}
