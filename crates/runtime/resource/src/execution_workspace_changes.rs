//! Translate a verified complete filesystem result into the original Resource
//! transaction kernel. No copying through model messages or format-specific IO.
use crate::{
    CommitResourceChangeSet, ExecutionWorkspaceSnapshot, ResourceChangeSetMutation as Mutation,
    ResourceCommandContext, ResourceCommandSource, ResourceMutationIdentity, WorkspaceDelta,
    WorkspaceEntryKind, WorkspaceExecutionOutcome, WorkspaceManifest, WorkspaceManifestError,
};
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, IdempotencyKey, ResourceChangeSetId, ResourceNodeId,
    VersionNumber,
};
use musteragent_resource::ResourceName;
use musteragent_run::RunContext;
use std::collections::{BTreeMap, BTreeSet};

pub fn plan_execution_workspace_changes(
    context: ResourceCommandContext,
    run: RunContext,
    snapshot: &ExecutionWorkspaceSnapshot,
    manifest: &WorkspaceManifest,
    delta: &WorkspaceDelta,
    outputs: &BTreeMap<String, ContentRef>,
) -> Result<Option<crate::PreparedResourceMutation>, WorkspaceManifestError> {
    delta.validate_against(manifest, outputs)?;
    let invocation = context.invocation_id.ok_or(WorkspaceManifestError)?;
    if context.source != ResourceCommandSource::ExecutionWorkspace(snapshot.binding)
        || invocation != snapshot.invocation_id
        || context.tenant_id != run.tenant_id
        || context.conversation_id() != Some(run.conversation_id)
        || delta.execution != WorkspaceExecutionOutcome::Succeeded
    {
        return Err(WorkspaceManifestError);
    }
    let all_entries: Vec<_> = snapshot
        .entries
        .iter()
        .chain(snapshot.project.iter().flat_map(|p| p.entries.iter()))
        .collect();
    let mut commands = Vec::new();
    if let Some(command) = plan_scope(
        context.clone(),
        run,
        snapshot.revision,
        &snapshot.entries,
        "work",
        None,
        manifest,
        delta,
        outputs,
        &all_entries,
    )? {
        commands.push(command);
    }
    if let Some(project) = &snapshot.project
        && project.writable
    {
        let mut project_context = context;
        project_context.source = ResourceCommandSource::Conversation {
            conversation_id: snapshot.binding.conversation_id,
            workspace_id: Some(snapshot.binding.project_id.ok_or(WorkspaceManifestError)?),
        };
        if let Some(command) = plan_scope(
            project_context,
            run,
            project.revision,
            &project.entries,
            "project",
            project.root_node_id,
            manifest,
            delta,
            outputs,
            &all_entries,
        )? {
            commands.push(command);
        }
    }
    let mut commands = commands.into_iter();
    let Some(first) = commands.next() else {
        return Ok(None);
    };
    let additional: Vec<_> = commands.collect();
    Ok(Some(if additional.is_empty() {
        crate::PreparedResourceMutation::ChangeSet(Box::new(first))
    } else {
        crate::PreparedResourceMutation::ChangeSets {
            first: Box::new(first),
            additional,
        }
    }))
}

#[allow(clippy::too_many_arguments)]
fn plan_scope(
    context: ResourceCommandContext,
    run: RunContext,
    revision: musteragent_identity::ResourceRevision,
    entries: &[crate::ExecutionWorkspaceSnapshotEntry],
    mount: &str,
    root_node_id: Option<ResourceNodeId>,
    manifest: &WorkspaceManifest,
    delta: &WorkspaceDelta,
    outputs: &BTreeMap<String, ContentRef>,
    all_entries: &[&crate::ExecutionWorkspaceSnapshotEntry],
) -> Result<Option<CommitResourceChangeSet>, WorkspaceManifestError> {
    let invocation = context.invocation_id.ok_or(WorkspaceManifestError)?;
    let prefix = format!("{mount}/");
    let original: BTreeMap<_, _> = entries.iter().map(|e| (e.node_id, e)).collect();
    let original_paths: BTreeMap<_, _> = entries
        .iter()
        .map(|e| (format!("{mount}/{}", e.path), e.node_id))
        .collect();
    let mut final_paths = BTreeMap::new();
    let mut final_entries = BTreeMap::new();
    for entry in delta.entries.iter().filter(|e| e.path.starts_with(&prefix)) {
        let id = match &entry.identity {
            Some(id)
                if id
                    .parse::<ResourceNodeId>()
                    .is_ok_and(|id| original.contains_key(&id)) =>
            {
                id.parse::<ResourceNodeId>()
                    .map_err(|_| WorkspaceManifestError)?
            }
            _ => ResourceNodeId::from_uuid(uuid::Uuid::new_v5(
                &invocation.into_uuid(),
                format!("workspace:node:{}", entry.path).as_bytes(),
            ))
            .map_err(|_| WorkspaceManifestError)?,
        };
        final_paths.insert(entry.path.clone(), id);
        final_entries.insert(id, entry);
    }
    let parent = |path: &str,
                  index: &BTreeMap<String, ResourceNodeId>|
     -> Result<Option<ResourceNodeId>, WorkspaceManifestError> {
        let (prefix, _) = path.rsplit_once('/').ok_or(WorkspaceManifestError)?;
        if prefix == mount {
            Ok(root_node_id)
        } else {
            index
                .get(prefix)
                .copied()
                .map(Some)
                .ok_or(WorkspaceManifestError)
        }
    };
    let name = |path: &str| -> Result<ResourceName, WorkspaceManifestError> {
        ResourceName::new(path.rsplit('/').next().ok_or(WorkspaceManifestError)?)
            .map_err(|_| WorkspaceManifestError)
    };
    let key = |label: &str, id: ResourceNodeId| {
        IdempotencyKey::new(format!("workspace:{label}:{id}")).map_err(|_| WorkspaceManifestError)
    };
    let mut mutations = Vec::new();
    let mut moved = BTreeSet::new();
    let mut occupied: BTreeSet<String> = manifest
        .entries
        .iter()
        .chain(delta.entries.iter())
        .map(|e| e.path.to_lowercase())
        .collect();
    // Evacuate changing nodes to unique, transaction-private root names. This
    // permits swaps, directory cycles and replacement without transient public
    // collisions. Every receipt resolves to the final view after the commit.
    for (id, entry) in &final_entries {
        let Some(old) = original.get(id) else {
            continue;
        };
        let old_path = format!("{mount}/{}", old.path);
        if parent(&old_path, &original_paths)? == parent(&entry.path, &final_paths)?
            && name(&old_path)? == name(&entry.path)?
        {
            continue;
        }
        moved.insert(*id);
        let mut counter = 0;
        let staging = loop {
            let candidate = format!(
                ".mw-{}-{counter}",
                uuid::Uuid::new_v5(&invocation.into_uuid(), id.to_string().as_bytes())
            );
            if occupied.insert(format!("{mount}/{candidate}")) {
                break candidate;
            }
            counter += 1;
        };
        mutations.push(Mutation::Move {
            mutation_key: key("stage", *id)?,
            node_id: *id,
            target_parent_node_id: root_node_id,
            name: Some(ResourceName::new(staging).map_err(|_| WorkspaceManifestError)?),
        });
    }
    let removed: BTreeSet<_> = original
        .keys()
        .filter(|id| !final_entries.contains_key(id))
        .copied()
        .collect();
    for id in &removed {
        let old = original[id];
        // Trashing a directory already handles its removed descendants. Any
        // retained children have been evacuated above before this operation.
        let old_path = format!("{mount}/{}", old.path);
        if parent(&old_path, &original_paths)?.is_some_and(|id| removed.contains(&id)) {
            continue;
        }
        mutations.push(Mutation::Trash {
            mutation_key: key("remove", *id)?,
            node_id: *id,
        });
    }
    let mut ordered: Vec<_> = final_entries.iter().collect();
    ordered.sort_by_key(|(_, entry)| (entry.path.split('/').count(), entry.path.clone()));
    for (id, entry) in ordered {
        let target_parent_node_id = parent(&entry.path, &final_paths)?;
        let filename = name(&entry.path)?;
        if let Some(old) = original.get(id) {
            if moved.contains(id) {
                mutations.push(Mutation::Move {
                    mutation_key: key("place", *id)?,
                    node_id: *id,
                    target_parent_node_id,
                    name: Some(filename.clone()),
                });
            }
            if let Some(content) = outputs.get(&entry.path) {
                let mut file = old.file.clone().ok_or(WorkspaceManifestError)?;
                file.name = filename.as_str().to_owned();
                let version = version(
                    &context,
                    run,
                    *id,
                    file.artifact_id,
                    file.version
                        .get()
                        .checked_add(1)
                        .ok_or(WorkspaceManifestError)?,
                    content.clone(),
                )?;
                mutations.push(Mutation::ReplaceFile {
                    mutation_key: key("write", *id)?,
                    file,
                    version: Box::new(version),
                });
            }
        } else if entry.kind == WorkspaceEntryKind::Directory {
            mutations.push(Mutation::CreateFolder {
                mutation_key: key("create", *id)?,
                node_id: *id,
                parent_node_id: target_parent_node_id,
                sort_key: filename.as_str().into(),
                name: filename,
            });
        } else {
            let content = outputs
                .get(&entry.path)
                .cloned()
                .or_else(|| {
                    entry.identity.as_ref().and_then(|identity| {
                        all_entries
                            .iter()
                            .find(|old| old.node_id.to_string() == *identity)
                            .and_then(|old| old.file.as_ref())
                            .map(|file| file.content_ref.clone())
                    })
                })
                .ok_or(WorkspaceManifestError)?;
            let artifact_id = ArtifactId::from_uuid(uuid::Uuid::new_v5(
                &invocation.into_uuid(),
                format!("workspace:artifact:{id}").as_bytes(),
            ))
            .map_err(|_| WorkspaceManifestError)?;
            let version = version(&context, run, *id, artifact_id, 1, content)?;
            mutations.push(Mutation::CreateFile {
                mutation_key: key("create", *id)?,
                node_id: *id,
                parent_node_id: target_parent_node_id,
                sort_key: filename.as_str().into(),
                name: filename.clone(),
                artifact: Artifact {
                    artifact_id,
                    tenant_id: context.tenant_id,
                    source: ArtifactSource::Conversation(run.conversation_id),
                    origin: ArtifactOrigin::Runtime {
                        activity_id: run.activity_id,
                        created_by_run_id: run.run_id,
                    },
                    kind: ArtifactKind::File,
                    lifecycle: ArtifactLifecycle::Draft,
                    current_version: VersionNumber::new(1).map_err(|_| WorkspaceManifestError)?,
                    title: filename.as_str().into(),
                    created_at: context.now,
                    updated_at: context.now,
                },
                version: Box::new(version),
            });
        }
    }
    if mutations.is_empty() {
        return Ok(None);
    }
    let bytes = serde_json::to_vec(delta).map_err(|_| WorkspaceManifestError)?;
    let command = CommitResourceChangeSet {
        context,
        identity: ResourceMutationIdentity {
            change_set_id: ResourceChangeSetId::from_uuid(uuid::Uuid::new_v5(
                &invocation.into_uuid(),
                format!("workspace:scope:{mount}").as_bytes(),
            ))
            .map_err(|_| WorkspaceManifestError)?,
            idempotency_key: IdempotencyKey::new(format!("workspace:{invocation}:{mount}"))
                .map_err(|_| WorkspaceManifestError)?,
            command_fingerprint: Sha256::digest(bytes),
        },
        expected_revision: revision,
        mutations,
    };
    command.validate().map_err(|_| WorkspaceManifestError)?;
    Ok(Some(command))
}

fn version(
    context: &ResourceCommandContext,
    run: RunContext,
    node: ResourceNodeId,
    artifact_id: ArtifactId,
    number: u64,
    content_ref: ContentRef,
) -> Result<ArtifactVersion, WorkspaceManifestError> {
    let invocation = context.invocation_id.ok_or(WorkspaceManifestError)?;
    Ok(ArtifactVersion {
        artifact_id,
        artifact_version_id: ArtifactVersionId::from_uuid(uuid::Uuid::new_v5(
            &invocation.into_uuid(),
            format!("workspace:version:{node}").as_bytes(),
        ))
        .map_err(|_| WorkspaceManifestError)?,
        version: VersionNumber::new(number).map_err(|_| WorkspaceManifestError)?,
        content_hash: content_ref.sha256,
        content_ref,
        dependency_refs: Vec::new(),
        source_invocation_id: Some(invocation),
        committed_by: ArtifactVersionAuthor::Participant(run.executor_participant_id),
        committed_at: context.now,
    })
}
