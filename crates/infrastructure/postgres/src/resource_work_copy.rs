//! Explicit private-to-project copies reuse the original atomic Resource kernel.
//! No file bytes cross the Product client; Content refs retain their original owner.
use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_identity::*;
use musteragent_resource::{ResourceNodeKind, ResourceScope};
use musteragent_runtime_resource::*;
use sqlx::{PgConnection, Row};
use std::collections::BTreeMap;

const MAX_COPY_ENTRIES: usize = 2048;
const MAX_COPY_BYTES: u64 = 256 * 1024 * 1024;

fn dependency(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::DependencyUnavailable
}
fn corrupt<T>(_: T) -> ResourceStoreError {
    ResourceStoreError::Corrupt
}

/// Lock the original source before the project head, in the same order as an
/// execution commit. A running writer makes this explicit copy unavailable.
pub(crate) async fn lock_source(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
) -> Result<(), ResourceStoreError> {
    let Some(ResourceChangeSetMutation::CopyWorkTree {
        source_workspace_id,
        ..
    }) = command.mutations.first()
    else {
        return Ok(());
    };
    let context = source_context(connection, command, *source_workspace_id).await?;
    if !crate::resource_authority::lock_current_resource_context(connection, &context).await? {
        return Err(ResourceStoreError::RevisionConflict);
    }
    sqlx::query("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=3 AND scope_id=$2 FOR UPDATE")
        .bind(context.tenant_id.into_uuid()).bind(source_workspace_id.into_uuid()).fetch_one(connection).await.map_err(dependency)?;
    Ok(())
}

async fn source_context(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
    source: ExecutionWorkspaceId,
) -> Result<ResourceCommandContext, ResourceStoreError> {
    let context = &command.context;
    if context.invocation_id.is_some() || !matches!(context.scope(), ResourceScope::Project(_)) {
        return Err(ResourceStoreError::NodeUnavailable);
    }
    let cid = context
        .conversation_id()
        .ok_or(ResourceStoreError::NodeUnavailable)?;
    let row = sqlx::query("SELECT w.branch_id,c.home_epoch::text AS home_epoch FROM resource.execution_workspaces w JOIN conversation.conversations c USING(tenant_id,conversation_id) WHERE w.tenant_id=$1 AND w.conversation_id=$2 AND w.execution_workspace_id=$3")
        .bind(context.tenant_id.into_uuid()).bind(cid.into_uuid()).bind(source.into_uuid()).fetch_optional(connection).await.map_err(dependency)?.ok_or(ResourceStoreError::NodeUnavailable)?;
    let binding = ExecutionWorkspaceBinding {
        execution_workspace_id: source,
        conversation_id: cid,
        branch_id: row
            .try_get::<Option<uuid::Uuid>, _>("branch_id")
            .map_err(corrupt)?
            .map(ContextBranchId::from_uuid)
            .transpose()
            .map_err(corrupt)?,
        project_id: context.workspace_id(),
        home_epoch: HomeEpoch::new(
            row.try_get::<String, _>("home_epoch")
                .map_err(corrupt)?
                .parse()
                .map_err(corrupt)?,
        )
        .map_err(corrupt)?,
    };
    Ok(ResourceCommandContext {
        source: ResourceCommandSource::ExecutionWorkspace(binding),
        ..context.clone()
    })
}

pub(crate) async fn apply(
    connection: &mut PgConnection,
    command: &CommitResourceChangeSet,
    mutation: &ResourceChangeSetMutation,
    revision: ResourceRevision,
) -> Result<ResourceChangeSetMutationReceipt, ResourceStoreError> {
    let ResourceChangeSetMutation::CopyWorkTree {
        mutation_key,
        node_id,
        parent_node_id,
        name,
        source_workspace_id,
        source_node_id,
        source_revision,
        committed_by,
    } = mutation
    else {
        return Err(ResourceStoreError::Corrupt);
    };
    let context = source_context(connection, command, *source_workspace_id).await?;
    crate::execution_workspace::authorize_read(connection, &context).await?;
    let current: i64 = sqlx::query_scalar("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=3 AND scope_id=$2")
        .bind(context.tenant_id.into_uuid()).bind(source_workspace_id.into_uuid()).fetch_one(&mut *connection).await.map_err(dependency)?;
    if current < 0 || current as u64 != source_revision.get() {
        return Err(ResourceStoreError::RevisionConflict);
    }
    let sql = format!("WITH RECURSIVE selected(node_id,depth) AS (
        SELECT node_id,0 FROM resource.nodes WHERE tenant_id=$1 AND scope_kind=3 AND scope_id=$3 AND node_id=$4 AND lifecycle=1
        UNION ALL SELECT n.node_id,s.depth+1 FROM resource.nodes n JOIN selected s ON n.parent_node_id=s.node_id
        WHERE n.tenant_id=$1 AND n.scope_kind=3 AND n.scope_id=$3 AND n.lifecycle=1 AND s.depth<32
    ) {} AND node.node_id IN (SELECT node_id FROM selected)
    ORDER BY copy_depth,node.node_id LIMIT $5", crate::resource_file_catalog::ROW_QUERY.replace("SELECT node.*", "SELECT (SELECT depth FROM selected WHERE selected.node_id=node.node_id) AS copy_depth,node.*"));
    let rows = sqlx::query(&sql)
        .bind(context.tenant_id.into_uuid())
        .bind(3_i16)
        .bind(source_workspace_id.into_uuid())
        .bind(source_node_id.into_uuid())
        .bind((MAX_COPY_ENTRIES + 1) as i64)
        .fetch_all(&mut *connection)
        .await
        .map_err(dependency)?;
    if rows.is_empty() {
        return Err(ResourceStoreError::NodeUnavailable);
    }
    if rows.len() > MAX_COPY_ENTRIES {
        return Err(ResourceStoreError::BoundsExceeded);
    }
    if rows
        .iter()
        .any(|row| row.try_get::<i32, _>("copy_depth").unwrap_or(32) >= 32)
    {
        return Err(ResourceStoreError::BoundsExceeded);
    }
    let entries = rows
        .into_iter()
        .map(crate::resource_file_catalog::decode)
        .collect::<Result<Vec<_>, _>>()?;
    let bytes = entries
        .iter()
        .filter_map(|e| e.file.as_ref())
        .try_fold(0_u64, |n, f| n.checked_add(f.content_ref.byte_length))
        .ok_or(ResourceStoreError::BoundsExceeded)?;
    if bytes > MAX_COPY_BYTES {
        return Err(ResourceStoreError::BoundsExceeded);
    }
    let mut ids = BTreeMap::new();
    let mut mutations = Vec::new();
    let mut origins = Vec::new();
    for entry in entries {
        let original = &entry.node;
        let target = if original.node_id == *source_node_id {
            *node_id
        } else {
            ResourceNodeId::generate()
        };
        let parent = if original.node_id == *source_node_id {
            *parent_node_id
        } else {
            Some(
                *ids.get(&original.parent_node_id.ok_or(ResourceStoreError::Corrupt)?)
                    .ok_or(ResourceStoreError::Corrupt)?,
            )
        };
        let target_name = if original.node_id == *source_node_id {
            name.clone()
        } else {
            original.name.clone()
        };
        ids.insert(original.node_id, target);
        let key = IdempotencyKey::new(target.to_string()).map_err(corrupt)?;
        let sort_key = target_name.comparison_key().to_owned();
        let source_version = entry.file.as_ref().map(|f| f.artifact_version_id);
        origins.push((target, original.node_id, source_version));
        mutations.push(match original.kind {
            ResourceNodeKind::Folder => ResourceChangeSetMutation::CreateFolder {
                mutation_key: key,
                node_id: target,
                parent_node_id: parent,
                name: target_name,
                sort_key,
            },
            ResourceNodeKind::File => {
                let file = entry.file.ok_or(ResourceStoreError::FileUnavailable)?;
                let aid = ArtifactId::generate();
                let version = VersionNumber::new(1).map_err(corrupt)?;
                ResourceChangeSetMutation::CreateFile {
                    mutation_key: key,
                    node_id: target,
                    parent_node_id: parent,
                    name: target_name.clone(),
                    sort_key,
                    artifact: Artifact {
                        artifact_id: aid,
                        tenant_id: context.tenant_id,
                        source: ArtifactSource::Conversation(
                            context
                                .conversation_id()
                                .ok_or(ResourceStoreError::Corrupt)?,
                        ),
                        origin: ArtifactOrigin::Product,
                        kind: ArtifactKind::File,
                        lifecycle: ArtifactLifecycle::Draft,
                        current_version: version,
                        title: target_name.as_str().to_owned(),
                        created_at: context.now,
                        updated_at: context.now,
                    },
                    version: Box::new(ArtifactVersion {
                        artifact_version_id: ArtifactVersionId::generate(),
                        artifact_id: aid,
                        version,
                        content_hash: file.content_ref.sha256,
                        content_ref: file.content_ref,
                        dependency_refs: vec![],
                        source_invocation_id: None,
                        committed_by: ArtifactVersionAuthor::Participant(*committed_by),
                        committed_at: context.now,
                    }),
                }
            }
        });
    }
    let expanded = CommitResourceChangeSet {
        mutations,
        ..command.clone()
    };
    // Validate all target names and parents before the first insert. This uses
    // the same kernel as ordinary file operations, without another transaction.
    let prepared = crate::resource_change_set::prevalidate(connection, &expanded).await?;
    let mut root_receipt = None;
    for (item, prepared) in expanded.mutations.iter().zip(prepared.iter()) {
        if matches!(item, ResourceChangeSetMutation::CreateFile { .. }) {
            // The internal expansion has its own bounded count; per-file rules
            // are still the exact rules of the public Resource command.
            let single = CommitResourceChangeSet {
                mutations: vec![item.clone()],
                ..command.clone()
            };
            single.validate().map_err(|_| ResourceStoreError::Corrupt)?;
        }
        let receipt = Box::pin(crate::resource_change_set::apply_mutation(
            connection, &expanded, item, prepared, revision,
        ))
        .await?;
        if receipt.node.node_id == *node_id {
            root_receipt = Some(receipt)
        }
    }
    for (target, source, version) in origins {
        sqlx::query("INSERT INTO resource.copy_origins (target_node_id,source_node_id,source_revision,source_artifact_version_id,change_set_id) VALUES($1,$2,$3,$4,$5)")
            .bind(target.into_uuid()).bind(source.into_uuid()).bind(current).bind(version.map(ArtifactVersionId::into_uuid)).bind(command.identity.change_set_id.into_uuid()).execute(&mut *connection).await.map_err(dependency)?;
    }
    let mut receipt = root_receipt.ok_or(ResourceStoreError::Corrupt)?;
    receipt.mutation_key = mutation_key.clone();
    Ok(receipt)
}
