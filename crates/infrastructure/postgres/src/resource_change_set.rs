//! PostgreSQL atomic multi-node Resource change-set kernel.

use std::collections::{BTreeMap, BTreeSet};

use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ResourceNodeId, ResourceRevision, VersionNumber,
};
use musteragent_resource::{
    EditSessionState, ResourceName, ResourceNodeKind, ResourceNodeLifecycle,
};
use musteragent_runtime_resource::{
    CommitResourceChangeSet, CreateResourceFolder, FinalizeFileEdit, MoveResourceNode,
    PreparedResourceMutation, RenameResourceNode, ResourceChangeSetMutation,
    ResourceChangeSetMutationReceipt, ResourceChangeSetReceipt, ResourceStoreError,
    RestoreResourceNode, TrashResourceNode, file_edit_terminal_fingerprint,
};
use sqlx::{PgConnection, Row as _};

use crate::resource::{
    InsertNode, OPERATION_CHANGE_SET, PostgresResourceStore,
    apply_prepared_resource_mutation_at_revision, begin_mutation, complete_mutation, encode_scope,
    insert_node, load_node_on, resource_artifact_error, revision_i64,
};

const FOLDER_NODE: i16 = 1;
const FILE_NODE: i16 = 2;
const ACTIVE_NODE: i16 = 1;
const TRASHED_NODE: i16 = 2;
const EDIT_SESSION_OPEN: i16 = 1;
const CHANGE_SET_COMMITTED: i16 = 2;

const MUTATION_CREATE_FOLDER: i16 = 1;
const MUTATION_CREATE_FILE: i16 = 2;
const MUTATION_PUBLISH_EDIT: i16 = 3;
const MUTATION_RENAME: i16 = 4;
const MUTATION_MOVE: i16 = 5;
const MUTATION_TRASH: i16 = 6;
const MUTATION_RESTORE: i16 = 7;
const MUTATION_REPLACE_FILE: i16 = 8;

#[derive(Clone)]
struct GraphNode {
    parent_node_id: Option<ResourceNodeId>,
    original_parent_node_id: Option<ResourceNodeId>,
    name: ResourceName,
    kind: ResourceNodeKind,
    lifecycle: ResourceNodeLifecycle,
    file_artifact_id: Option<ArtifactId>,
}

pub(crate) struct ValidatedMutation {
    node_id: ResourceNodeId,
}

impl PostgresResourceStore {
    pub(crate) async fn commit_change_set_inner(
        &self,
        command: &CommitResourceChangeSet,
    ) -> Result<ResourceChangeSetReceipt, ResourceStoreError> {
        let mut transaction = self.write_resource_unit_of_work().await?;
        let receipt = commit_resource_change_set_on(&mut transaction, command).await?;
        transaction
            .commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(receipt)
    }
}

/// Apply a complete change set inside a caller-owned transaction. This is the
/// single atomic kernel used by both Product RPCs and fenced Agent Tool commits.
pub(crate) async fn commit_resource_change_set_on(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
) -> Result<ResourceChangeSetReceipt, ResourceStoreError> {
    command
        .validate()
        .map_err(|_| ResourceStoreError::Corrupt)?;
    if command.mutations.iter().any(|m| {
        matches!(
            m,
            ResourceChangeSetMutation::PublishEdit { .. }
                | ResourceChangeSetMutation::SetResult {
                    is_result: true,
                    ..
                }
        )
    }) && crate::artifact::task_result::resource::operation_scope(connection, &command.context)
        .await
        .map_err(|error| match error {
            musteragent_runtime_session::DialogueStoreError::Infrastructure => {
                ResourceStoreError::DependencyUnavailable
            }
            musteragent_runtime_session::DialogueStoreError::Corrupt => ResourceStoreError::Corrupt,
            _ => ResourceStoreError::FileUnavailable,
        })?
        .is_some()
    {
        // Both members and their receipts must preserve the atomic semantics of
        // the original change set. A verified candidate cannot share a public write.
        return Err(ResourceStoreError::PrivateTaskResult);
    }
    crate::resource_work_copy::lock_source(connection, command).await?;
    let Some(revisions) = begin_mutation(
        connection,
        &command.context,
        &command.identity,
        command.expected_revision,
        OPERATION_CHANGE_SET,
    )
    .await?
    else {
        return load_duplicate_receipt(connection, command).await;
    };

    let validated = prevalidate(connection, command).await?;
    let mut receipts = Vec::with_capacity(command.mutations.len());
    for (mutation, validated) in command.mutations.iter().zip(validated.iter()) {
        receipts
            .push(apply_mutation(connection, command, mutation, validated, revisions.next).await?);
    }
    // A node can be touched more than once in one set. Every member receipt
    // resolves to the final committed node view so first execution and durable
    // replay are byte-for-byte semantically identical.
    for receipt in &mut receipts {
        receipt.node =
            load_node_on(connection, command.context.tenant_id, receipt.node.node_id).await?;
    }
    for (ordinal, (mutation, receipt)) in command.mutations.iter().zip(receipts.iter()).enumerate()
    {
        persist_result(connection, command, mutation, ordinal, receipt).await?;
    }
    let first_node = receipts
        .first()
        .ok_or(ResourceStoreError::Corrupt)?
        .node
        .node_id;
    complete_mutation(
        connection,
        &command.context,
        &command.identity,
        first_node,
        revisions.next,
    )
    .await?;
    Ok(ResourceChangeSetReceipt {
        scope: command.context.scope(),
        previous_revision: revisions.previous,
        revision: revisions.next,
        mutations: receipts,
        duplicate: false,
    })
}

pub(crate) async fn prevalidate(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
) -> Result<Vec<ValidatedMutation>, ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(command.context.scope());
    let rows = sqlx::query(
        "SELECT node_id, parent_node_id, original_parent_node_id, name, kind,
                lifecycle, file_artifact_id
           FROM resource.nodes
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
          FOR UPDATE",
    )
    .bind(command.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(dependency)?;
    let mut graph = BTreeMap::new();
    for row in rows {
        let node_id = ResourceNodeId::from_uuid(row.try_get("node_id").map_err(corrupt)?)
            .map_err(|_| ResourceStoreError::Corrupt)?;
        graph.insert(
            node_id,
            GraphNode {
                parent_node_id: optional_node(row.try_get("parent_node_id").map_err(corrupt)?)?,
                original_parent_node_id: optional_node(
                    row.try_get("original_parent_node_id").map_err(corrupt)?,
                )?,
                name: ResourceName::new(row.try_get::<String, _>("name").map_err(corrupt)?)
                    .map_err(|_| ResourceStoreError::Corrupt)?,
                kind: decode_kind(row.try_get("kind").map_err(corrupt)?)?,
                lifecycle: decode_lifecycle(row.try_get("lifecycle").map_err(corrupt)?)?,
                file_artifact_id: optional_artifact(
                    row.try_get("file_artifact_id").map_err(corrupt)?,
                )?,
            },
        );
    }

    let mut edit_sessions = BTreeSet::new();
    let mut edit_artifacts = BTreeSet::new();
    let mut validated = Vec::with_capacity(command.mutations.len());
    for mutation in &command.mutations {
        let item = match mutation {
            ResourceChangeSetMutation::CopyWorkTree {
                node_id,
                parent_node_id,
                name,
                ..
            } => {
                ensure_new_node(&graph, *node_id)?;
                ensure_parent(&graph, *parent_node_id)?;
                ensure_name_available(&graph, *parent_node_id, name, None)?;
                ValidatedMutation { node_id: *node_id }
            }
            ResourceChangeSetMutation::SetResult { node_id, .. } => {
                if active_node(&graph, *node_id)?.kind != ResourceNodeKind::File {
                    return Err(ResourceStoreError::NodeUnavailable);
                }
                ValidatedMutation { node_id: *node_id }
            }
            ResourceChangeSetMutation::ReplaceFile { file, version, .. } => {
                let node = active_node(&graph, file.node_id)?;
                if node.kind != ResourceNodeKind::File
                    || node.file_artifact_id != Some(file.artifact_id)
                    || !edit_artifacts.insert(file.artifact_id)
                {
                    return Err(ResourceStoreError::RevisionConflict);
                }
                crate::artifact::authorize_content(connection, command.context.tenant_id, version)
                    .await
                    .map_err(resource_artifact_error)?;
                ValidatedMutation {
                    node_id: file.node_id,
                }
            }
            ResourceChangeSetMutation::CreateFolder {
                node_id,
                parent_node_id,
                name,
                ..
            } => {
                ensure_new_node(&graph, *node_id)?;
                ensure_parent(&graph, *parent_node_id)?;
                ensure_name_available(&graph, *parent_node_id, name, None)?;
                graph.insert(
                    *node_id,
                    GraphNode {
                        parent_node_id: *parent_node_id,
                        original_parent_node_id: None,
                        name: name.clone(),
                        kind: ResourceNodeKind::Folder,
                        lifecycle: ResourceNodeLifecycle::Active,
                        file_artifact_id: None,
                    },
                );
                ValidatedMutation { node_id: *node_id }
            }
            ResourceChangeSetMutation::CreateFile {
                node_id,
                parent_node_id,
                name,
                artifact,
                version,
                ..
            } => {
                ensure_new_node(&graph, *node_id)?;
                ensure_parent(&graph, *parent_node_id)?;
                ensure_name_available(&graph, *parent_node_id, name, None)?;
                if graph
                    .values()
                    .any(|node| node.file_artifact_id == Some(artifact.artifact_id))
                    || artifact_exists(connection, artifact.artifact_id).await?
                {
                    return Err(ResourceStoreError::FileUnavailable);
                }
                crate::artifact::authorize_content(connection, command.context.tenant_id, version)
                    .await
                    .map_err(resource_artifact_error)?;
                graph.insert(
                    *node_id,
                    GraphNode {
                        parent_node_id: *parent_node_id,
                        original_parent_node_id: None,
                        name: name.clone(),
                        kind: ResourceNodeKind::File,
                        lifecycle: ResourceNodeLifecycle::Active,
                        file_artifact_id: Some(artifact.artifact_id),
                    },
                );
                ValidatedMutation { node_id: *node_id }
            }
            ResourceChangeSetMutation::PublishEdit {
                session_id,
                expected_session_revision,
                expected_file_version,
                ..
            } => {
                if !edit_sessions.insert(*session_id) {
                    return Err(ResourceStoreError::RevisionConflict);
                }
                let (node_id, artifact_id) = validate_edit(
                    connection,
                    command,
                    *session_id,
                    *expected_session_revision,
                    *expected_file_version,
                )
                .await?;
                let node = active_node(&graph, node_id)?;
                if node.kind != ResourceNodeKind::File
                    || node.file_artifact_id != Some(artifact_id)
                    || !edit_artifacts.insert(artifact_id)
                {
                    return Err(ResourceStoreError::RevisionConflict);
                }
                ValidatedMutation { node_id }
            }
            ResourceChangeSetMutation::Rename { node_id, name, .. } => {
                let current = active_node(&graph, *node_id)?.clone();
                ensure_name_available(&graph, current.parent_node_id, name, Some(*node_id))?;
                graph.get_mut(node_id).expect("validated node").name = name.clone();
                ValidatedMutation { node_id: *node_id }
            }
            ResourceChangeSetMutation::Move {
                node_id,
                target_parent_node_id,
                name,
                ..
            } => {
                let current = active_node(&graph, *node_id)?.clone();
                ensure_parent(&graph, *target_parent_node_id)?;
                ensure_no_cycle(&graph, *node_id, *target_parent_node_id)?;
                let target_name = name.as_ref().unwrap_or(&current.name);
                ensure_name_available(&graph, *target_parent_node_id, target_name, Some(*node_id))?;
                let node = graph.get_mut(node_id).expect("validated node");
                node.parent_node_id = *target_parent_node_id;
                node.name = target_name.clone();
                ValidatedMutation { node_id: *node_id }
            }
            ResourceChangeSetMutation::Trash { node_id, .. } => {
                active_node(&graph, *node_id)?;
                let descendants = subtree(&graph, *node_id);
                for descendant in descendants {
                    let node = graph.get_mut(&descendant).expect("subtree node");
                    node.lifecycle = ResourceNodeLifecycle::Trashed;
                    if descendant == *node_id {
                        node.original_parent_node_id = node.parent_node_id;
                        node.parent_node_id = None;
                    } else {
                        node.original_parent_node_id = None;
                    }
                }
                ValidatedMutation { node_id: *node_id }
            }
            ResourceChangeSetMutation::Restore {
                node_id,
                target_parent_node_id,
                name,
                ..
            } => {
                let current = graph
                    .get(node_id)
                    .filter(|node| node.lifecycle == ResourceNodeLifecycle::Trashed)
                    .cloned()
                    .ok_or(ResourceStoreError::NodeUnavailable)?;
                let target_parent = target_parent_node_id.or(current.original_parent_node_id);
                ensure_parent(&graph, target_parent)?;
                let target_name = name.as_ref().unwrap_or(&current.name);
                ensure_name_available(&graph, target_parent, target_name, Some(*node_id))?;
                for descendant in subtree(&graph, *node_id) {
                    let node = graph.get_mut(&descendant).expect("subtree node");
                    node.lifecycle = ResourceNodeLifecycle::Active;
                    node.original_parent_node_id = None;
                    if descendant == *node_id {
                        node.parent_node_id = target_parent;
                        node.name = target_name.clone();
                    }
                }
                ValidatedMutation { node_id: *node_id }
            }
        };
        validated.push(item);
    }
    Ok(validated)
}

pub(crate) async fn apply_mutation(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
    mutation: &ResourceChangeSetMutation,
    validated: &ValidatedMutation,
    revision: ResourceRevision,
) -> Result<ResourceChangeSetMutationReceipt, ResourceStoreError> {
    let identity = command.identity.clone();
    let node_receipt = match mutation {
        ResourceChangeSetMutation::CopyWorkTree { .. } => {
            return crate::resource_work_copy::apply(connection, command, mutation, revision).await;
        }
        ResourceChangeSetMutation::SetResult {
            node_id, is_result, ..
        } => {
            sqlx::query("UPDATE resource.nodes SET is_result=$3, revision=$4, updated_at_ms=$5 WHERE tenant_id=$1 AND node_id=$2")
                .bind(command.context.tenant_id.into_uuid())
                .bind(node_id.into_uuid()).bind(*is_result)
                .bind(revision_i64(revision)?).bind(command.context.now.unix_millis())
                .execute(&mut *connection).await.map_err(dependency)?;
            return Ok(ResourceChangeSetMutationReceipt {
                mutation_key: mutation.mutation_key().clone(),
                node: load_node_on(connection, command.context.tenant_id, *node_id).await?,
                artifact_version_id: None,
                file_version: None,
            });
        }
        ResourceChangeSetMutation::ReplaceFile { file, version, .. } => {
            crate::resource_file_replacement::replace_file_on(
                connection,
                &musteragent_runtime_resource::PreparedResourceFileReplacement {
                    context: command.context.clone(),
                    file: file.clone(),
                },
                version,
            )
            .await?;
            crate::artifact::insert_version(connection, version)
                .await
                .map_err(resource_artifact_error)?;
            sqlx::query("UPDATE resource.nodes SET revision=$1,updated_at_ms=$2 WHERE tenant_id=$3 AND node_id=$4")
                .bind(revision_i64(revision)?).bind(command.context.now.unix_millis())
                .bind(command.context.tenant_id.into_uuid()).bind(file.node_id.into_uuid())
                .execute(&mut *connection).await.map_err(dependency)?;
            return Ok(ResourceChangeSetMutationReceipt {
                mutation_key: mutation.mutation_key().clone(),
                node: load_node_on(connection, command.context.tenant_id, file.node_id).await?,
                artifact_version_id: Some(version.artifact_version_id),
                file_version: Some(version.version),
            });
        }
        ResourceChangeSetMutation::CreateFolder {
            node_id,
            parent_node_id,
            name,
            sort_key,
            ..
        } => {
            let prepared = PreparedResourceMutation::CreateFolder(CreateResourceFolder {
                context: command.context.clone(),
                identity,
                expected_revision: command.expected_revision,
                node_id: *node_id,
                parent_node_id: *parent_node_id,
                name: name.clone(),
                sort_key: sort_key.clone(),
            });
            apply_prepared_resource_mutation_at_revision(connection, &prepared, Some(revision))
                .await?
        }
        ResourceChangeSetMutation::CreateFile {
            node_id,
            parent_node_id,
            name,
            sort_key,
            artifact,
            version,
            ..
        } => {
            crate::artifact::insert_artifact(connection, artifact)
                .await
                .map_err(resource_artifact_error)?;
            crate::agent_resource::device::mutation::insert_version(
                connection,
                &command.context,
                *node_id,
                version,
            )
            .await
            .map_err(resource_artifact_error)?;
            insert_node(
                connection,
                InsertNode {
                    context: &command.context,
                    node_id: *node_id,
                    parent_node_id: *parent_node_id,
                    name,
                    kind: ResourceNodeKind::File,
                    file_artifact_id: Some(artifact.artifact_id),
                    sort_key,
                    revision,
                },
            )
            .await?;
            let node = load_node_on(connection, command.context.tenant_id, *node_id).await?;
            return Ok(ResourceChangeSetMutationReceipt {
                mutation_key: mutation.mutation_key().clone(),
                node,
                artifact_version_id: Some(version.artifact_version_id),
                file_version: Some(version.version),
            });
        }
        ResourceChangeSetMutation::PublishEdit {
            mutation_key,
            command_id,
            session_id,
            expected_session_revision,
            artifact_version_id,
            committed_by,
            ..
        } => {
            let terminal = FinalizeFileEdit {
                context: command.context.clone(),
                command_id: *command_id,
                session_id: *session_id,
                artifact_version_id: *artifact_version_id,
                committed_by: musteragent_artifact::ArtifactVersionAuthor::Participant(
                    *committed_by,
                ),
                idempotency_key: mutation_key.clone(),
                expected_session_revision: *expected_session_revision,
            };
            let fingerprint = file_edit_terminal_fingerprint(
                "change-set-publish-edit",
                *command_id,
                *session_id,
                *expected_session_revision,
                Some(*artifact_version_id),
                Some(terminal.committed_by),
            )
            .map_err(edit_error)?;
            let terminal =
                crate::resource_edit::finalize_file_edit_on(connection, &terminal, fingerprint)
                    .await
                    .map_err(edit_error)?;
            if terminal.state != EditSessionState::Committed {
                return Err(ResourceStoreError::RevisionConflict);
            }
            sqlx::query(
                "UPDATE resource.nodes SET revision = $1, updated_at_ms = $2
                  WHERE tenant_id = $3 AND node_id = $4",
            )
            .bind(revision_i64(revision)?)
            .bind(command.context.now.unix_millis())
            .bind(command.context.tenant_id.into_uuid())
            .bind(validated.node_id.into_uuid())
            .execute(&mut *connection)
            .await
            .map_err(dependency)?;
            let node =
                load_node_on(connection, command.context.tenant_id, validated.node_id).await?;
            return Ok(ResourceChangeSetMutationReceipt {
                mutation_key: mutation_key.clone(),
                node,
                artifact_version_id: terminal.artifact_version_id,
                file_version: terminal.file_version,
            });
        }
        ResourceChangeSetMutation::Rename { node_id, name, .. } => {
            let prepared = PreparedResourceMutation::Rename(RenameResourceNode {
                context: command.context.clone(),
                identity,
                expected_revision: command.expected_revision,
                node_id: *node_id,
                name: name.clone(),
            });
            apply_prepared_resource_mutation_at_revision(connection, &prepared, Some(revision))
                .await?
        }
        ResourceChangeSetMutation::Move {
            node_id,
            target_parent_node_id,
            name,
            ..
        } => {
            let prepared = PreparedResourceMutation::Move(MoveResourceNode {
                context: command.context.clone(),
                identity,
                expected_revision: command.expected_revision,
                node_id: *node_id,
                target_parent_node_id: *target_parent_node_id,
                name: name.clone(),
            });
            apply_prepared_resource_mutation_at_revision(connection, &prepared, Some(revision))
                .await?
        }
        ResourceChangeSetMutation::Trash { node_id, .. } => {
            let prepared = PreparedResourceMutation::Trash(TrashResourceNode {
                context: command.context.clone(),
                identity,
                expected_revision: command.expected_revision,
                node_id: *node_id,
            });
            apply_prepared_resource_mutation_at_revision(connection, &prepared, Some(revision))
                .await?
        }
        ResourceChangeSetMutation::Restore {
            node_id,
            target_parent_node_id,
            name,
            ..
        } => {
            let prepared = PreparedResourceMutation::Restore(RestoreResourceNode {
                context: command.context.clone(),
                identity,
                expected_revision: command.expected_revision,
                node_id: *node_id,
                target_parent_node_id: *target_parent_node_id,
                name: name.clone(),
            });
            apply_prepared_resource_mutation_at_revision(connection, &prepared, Some(revision))
                .await?
        }
    };
    Ok(ResourceChangeSetMutationReceipt {
        mutation_key: mutation.mutation_key().clone(),
        node: node_receipt.node,
        artifact_version_id: None,
        file_version: None,
    })
}

async fn persist_result(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
    mutation: &ResourceChangeSetMutation,
    ordinal: usize,
    receipt: &ResourceChangeSetMutationReceipt,
) -> Result<(), ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(command.context.scope());
    let artifact_id = receipt.node.file_artifact_id.or(match mutation {
        ResourceChangeSetMutation::CreateFile { artifact, .. } => Some(artifact.artifact_id),
        _ => None,
    });
    sqlx::query(
        "INSERT INTO resource.change_set_mutations (
             change_set_id, ordinal, tenant_id, scope_kind, scope_id,
             mutation_key, mutation_kind, result_node_id, result_artifact_id,
             result_artifact_version_id, result_file_version
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::numeric)",
    )
    .bind(command.identity.change_set_id.into_uuid())
    .bind(i16::try_from(ordinal).map_err(|_| ResourceStoreError::Corrupt)?)
    .bind(command.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(mutation.mutation_key().as_str())
    .bind(mutation_kind(mutation))
    .bind(receipt.node.node_id.into_uuid())
    .bind(
        receipt
            .artifact_version_id
            .and(artifact_id)
            .map(ArtifactId::into_uuid),
    )
    .bind(
        receipt
            .artifact_version_id
            .map(ArtifactVersionId::into_uuid),
    )
    .bind(
        receipt
            .file_version
            .map(|version| version.get().to_string()),
    )
    .execute(&mut *connection)
    .await
    .map_err(dependency)?;
    Ok(())
}

async fn load_duplicate_receipt(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
) -> Result<ResourceChangeSetReceipt, ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(command.context.scope());
    let header = sqlx::query(
        "SELECT expected_revision, committed_revision
           FROM resource.change_sets
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
            AND change_set_id = $4 AND status = $5",
    )
    .bind(command.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(command.identity.change_set_id.into_uuid())
    .bind(CHANGE_SET_COMMITTED)
    .fetch_one(&mut *connection)
    .await
    .map_err(dependency)?;
    let previous_revision = decode_revision(header.try_get("expected_revision").map_err(corrupt)?)?;
    let revision = decode_revision(header.try_get("committed_revision").map_err(corrupt)?)?;
    let rows = sqlx::query(
        "SELECT receipt.mutation_key, receipt.result_node_id, receipt.result_artifact_version_id,
                receipt.result_file_version::text AS result_file_version, snapshot.*
           FROM resource.change_set_mutations AS receipt
           CROSS JOIN LATERAL jsonb_populate_record(
               NULL::resource.nodes, resource.project_receipt_node(receipt.result_node_snapshot, receipt.legacy_dialogue_snapshot)
           ) AS snapshot
          WHERE receipt.change_set_id = $1 ORDER BY receipt.ordinal",
    )
    .bind(command.identity.change_set_id.into_uuid())
    .fetch_all(&mut *connection)
    .await
    .map_err(dependency)?;
    if rows.len() != command.mutations.len() {
        return Err(ResourceStoreError::Corrupt);
    }
    let mut receipts = Vec::with_capacity(rows.len());
    for (row, mutation) in rows.into_iter().zip(&command.mutations) {
        let mutation_key = musteragent_identity::IdempotencyKey::new(
            row.try_get::<String, _>("mutation_key").map_err(corrupt)?,
        )
        .map_err(|_| ResourceStoreError::Corrupt)?;
        if mutation_key != *mutation.mutation_key() {
            return Err(ResourceStoreError::Corrupt);
        }
        let node_id = ResourceNodeId::from_uuid(row.try_get("result_node_id").map_err(corrupt)?)
            .map_err(|_| ResourceStoreError::Corrupt)?;
        let artifact_version_id = row
            .try_get::<Option<uuid::Uuid>, _>("result_artifact_version_id")
            .map_err(corrupt)?
            .map(ArtifactVersionId::from_uuid)
            .transpose()
            .map_err(|_| ResourceStoreError::Corrupt)?;
        let file_version = row
            .try_get::<Option<String>, _>("result_file_version")
            .map_err(corrupt)?
            .map(decode_version)
            .transpose()?;
        let snapshot = crate::resource::decode_node(row)?;
        if snapshot.node_id != node_id || snapshot.scope != command.context.scope() {
            return Err(ResourceStoreError::Corrupt);
        }
        receipts.push(ResourceChangeSetMutationReceipt {
            mutation_key,
            node: snapshot,
            artifact_version_id,
            file_version,
        });
    }
    Ok(ResourceChangeSetReceipt {
        scope: command.context.scope(),
        previous_revision,
        revision,
        mutations: receipts,
        duplicate: true,
    })
}

async fn validate_edit(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
    session_id: musteragent_identity::ResourceEditSessionId,
    expected_session_revision: ResourceRevision,
    expected_file_version: VersionNumber,
) -> Result<(ResourceNodeId, ArtifactId), ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(command.context.scope());
    let row = sqlx::query(
        "SELECT session.file_node_id, session.file_artifact_id,
                session.base_version::text AS base_version,
                session.session_revision, session.state, session.expires_at_ms,
                artifact.current_version::text AS current_version
           FROM resource.edit_sessions AS session
           JOIN artifact.artifacts AS artifact
             ON artifact.tenant_id = session.tenant_id
            AND artifact.artifact_id = session.file_artifact_id
          WHERE session.tenant_id = $1 AND session.scope_kind = $2
            AND session.scope_id = $3 AND session.session_id = $4
          FOR UPDATE OF session, artifact",
    )
    .bind(command.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(session_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(dependency)?
    .ok_or(ResourceStoreError::NodeUnavailable)?;
    let revision = decode_revision(row.try_get("session_revision").map_err(corrupt)?)?;
    let base_version = decode_version(row.try_get("base_version").map_err(corrupt)?)?;
    let current_version = decode_version(row.try_get("current_version").map_err(corrupt)?)?;
    if revision != expected_session_revision
        || base_version != expected_file_version
        || current_version != expected_file_version
        || row.try_get::<i16, _>("state").map_err(corrupt)? != EDIT_SESSION_OPEN
        || row.try_get::<i64, _>("expires_at_ms").map_err(corrupt)?
            <= command.context.now.unix_millis()
    {
        return Err(ResourceStoreError::RevisionConflict);
    }
    Ok((
        ResourceNodeId::from_uuid(row.try_get("file_node_id").map_err(corrupt)?)
            .map_err(|_| ResourceStoreError::Corrupt)?,
        ArtifactId::from_uuid(row.try_get("file_artifact_id").map_err(corrupt)?)
            .map_err(|_| ResourceStoreError::Corrupt)?,
    ))
}

fn ensure_new_node(
    graph: &BTreeMap<ResourceNodeId, GraphNode>,
    node_id: ResourceNodeId,
) -> Result<(), ResourceStoreError> {
    if graph.contains_key(&node_id) {
        Err(ResourceStoreError::NameConflict)
    } else {
        Ok(())
    }
}

fn ensure_parent(
    graph: &BTreeMap<ResourceNodeId, GraphNode>,
    parent_node_id: Option<ResourceNodeId>,
) -> Result<(), ResourceStoreError> {
    match parent_node_id {
        None => Ok(()),
        Some(parent_node_id) => {
            let parent = active_node(graph, parent_node_id)?;
            if parent.kind == ResourceNodeKind::Folder {
                Ok(())
            } else {
                Err(ResourceStoreError::ParentUnavailable)
            }
        }
    }
}

fn active_node(
    graph: &BTreeMap<ResourceNodeId, GraphNode>,
    node_id: ResourceNodeId,
) -> Result<&GraphNode, ResourceStoreError> {
    graph
        .get(&node_id)
        .filter(|node| node.lifecycle == ResourceNodeLifecycle::Active)
        .ok_or(ResourceStoreError::NodeUnavailable)
}

fn ensure_name_available(
    graph: &BTreeMap<ResourceNodeId, GraphNode>,
    parent_node_id: Option<ResourceNodeId>,
    name: &ResourceName,
    excluding: Option<ResourceNodeId>,
) -> Result<(), ResourceStoreError> {
    let conflicts = graph.iter().any(|(node_id, node)| {
        Some(*node_id) != excluding
            && node.lifecycle == ResourceNodeLifecycle::Active
            && node.parent_node_id == parent_node_id
            && node.name.comparison_key() == name.comparison_key()
    });
    if conflicts {
        Err(ResourceStoreError::NameConflict)
    } else {
        Ok(())
    }
}

fn ensure_no_cycle(
    graph: &BTreeMap<ResourceNodeId, GraphNode>,
    node_id: ResourceNodeId,
    target_parent_node_id: Option<ResourceNodeId>,
) -> Result<(), ResourceStoreError> {
    let mut current = target_parent_node_id;
    let mut visited = BTreeSet::new();
    while let Some(candidate) = current {
        if candidate == node_id || !visited.insert(candidate) {
            return Err(ResourceStoreError::MoveCycle);
        }
        current = graph.get(&candidate).and_then(|node| node.parent_node_id);
    }
    Ok(())
}

fn subtree(
    graph: &BTreeMap<ResourceNodeId, GraphNode>,
    root: ResourceNodeId,
) -> Vec<ResourceNodeId> {
    let mut result = vec![root];
    let mut cursor = 0;
    while cursor < result.len() {
        let parent = result[cursor];
        result.extend(graph.iter().filter_map(|(node_id, node)| {
            (node.parent_node_id == Some(parent)).then_some(*node_id)
        }));
        cursor += 1;
    }
    result
}

async fn artifact_exists(
    connection: &mut PgConnection,
    artifact_id: ArtifactId,
) -> Result<bool, ResourceStoreError> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM artifact.artifacts WHERE artifact_id = $1)")
        .bind(artifact_id.into_uuid())
        .fetch_one(&mut *connection)
        .await
        .map_err(dependency)
}

const fn mutation_kind(mutation: &ResourceChangeSetMutation) -> i16 {
    match mutation {
        ResourceChangeSetMutation::CopyWorkTree { .. } => 10,
        ResourceChangeSetMutation::SetResult { .. } => 9,
        ResourceChangeSetMutation::ReplaceFile { .. } => MUTATION_REPLACE_FILE,
        ResourceChangeSetMutation::CreateFolder { .. } => MUTATION_CREATE_FOLDER,
        ResourceChangeSetMutation::CreateFile { .. } => MUTATION_CREATE_FILE,
        ResourceChangeSetMutation::PublishEdit { .. } => MUTATION_PUBLISH_EDIT,
        ResourceChangeSetMutation::Rename { .. } => MUTATION_RENAME,
        ResourceChangeSetMutation::Move { .. } => MUTATION_MOVE,
        ResourceChangeSetMutation::Trash { .. } => MUTATION_TRASH,
        ResourceChangeSetMutation::Restore { .. } => MUTATION_RESTORE,
    }
}

const fn decode_kind(value: i16) -> Result<ResourceNodeKind, ResourceStoreError> {
    match value {
        FOLDER_NODE => Ok(ResourceNodeKind::Folder),
        FILE_NODE => Ok(ResourceNodeKind::File),
        _ => Err(ResourceStoreError::Corrupt),
    }
}

const fn decode_lifecycle(value: i16) -> Result<ResourceNodeLifecycle, ResourceStoreError> {
    match value {
        ACTIVE_NODE => Ok(ResourceNodeLifecycle::Active),
        TRASHED_NODE => Ok(ResourceNodeLifecycle::Trashed),
        _ => Err(ResourceStoreError::Corrupt),
    }
}

fn optional_node(value: Option<uuid::Uuid>) -> Result<Option<ResourceNodeId>, ResourceStoreError> {
    value
        .map(ResourceNodeId::from_uuid)
        .transpose()
        .map_err(|_| ResourceStoreError::Corrupt)
}

fn optional_artifact(value: Option<uuid::Uuid>) -> Result<Option<ArtifactId>, ResourceStoreError> {
    value
        .map(ArtifactId::from_uuid)
        .transpose()
        .map_err(|_| ResourceStoreError::Corrupt)
}

fn decode_revision(value: i64) -> Result<ResourceRevision, ResourceStoreError> {
    u64::try_from(value)
        .map(ResourceRevision::new)
        .map_err(|_| ResourceStoreError::Corrupt)
}

fn decode_version(value: String) -> Result<VersionNumber, ResourceStoreError> {
    value
        .parse::<u64>()
        .ok()
        .and_then(|value| VersionNumber::new(value).ok())
        .ok_or(ResourceStoreError::Corrupt)
}

fn edit_error(error: musteragent_runtime_resource::FileEditError) -> ResourceStoreError {
    use musteragent_runtime_resource::FileEditError;
    match error {
        FileEditError::SessionConflict | FileEditError::BaseVersionConflict => {
            ResourceStoreError::RevisionConflict
        }
        FileEditError::SessionUnavailable => ResourceStoreError::NodeUnavailable,
        FileEditError::ContentUnavailable | FileEditError::FileUnavailable => {
            ResourceStoreError::FileUnavailable
        }
        FileEditError::DependencyUnavailable => ResourceStoreError::DependencyUnavailable,
        _ => ResourceStoreError::Corrupt,
    }
}

fn dependency(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::DependencyUnavailable
}

fn corrupt(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::Corrupt
}
