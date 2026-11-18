mod agent_management;
pub(crate) mod dialogue_device;
pub(crate) mod read;
pub(crate) mod work_item_erasure;
use musteragent_identity::{
    Actor, ArtifactId, ConversationId, PrincipalId, PrincipalKind, ResourceNodeId,
    ResourceRevision, TenantId, UtcTimestamp, VersionedKey, WorkspaceId,
};
use musteragent_resource::{
    ResourceName, ResourceNode, ResourceNodeKind, ResourceNodeLifecycle, ResourceProvenance,
    ResourceScope,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    CreateResourceFile, CreateResourceFolder, LinkResourceFile, ListResourceChildren,
    MoveResourceNode, PreparedResourceMutation, RenameResourceNode, ResourceMutationIdentity,
    ResourceMutationReceipt, ResourceNodePage, ResourceStore, ResourceStoreError,
    RestoreResourceNode, TrashResourceNode,
};
use sqlx::{PgConnection, PgPool, Row as _};

const PROJECT_SCOPE: i16 = 1;
pub(crate) const CONVERSATION_SCOPE: i16 = 2;
const OWNED_AGENT_SCOPE: i16 = 4;
const EXECUTION_WORKSPACE_SCOPE: i16 = 3;
const FOLDER_NODE: i16 = 1;
const FILE_NODE: i16 = 2;
const ACTIVE_NODE: i16 = 1;
const TRASHED_NODE: i16 = 2;
const CHANGE_SET_PENDING: i16 = 1;
const CHANGE_SET_COMMITTED: i16 = 2;
const OPERATION_CREATE_FOLDER: i16 = 1;
const OPERATION_LINK_FILE: i16 = 2;
const OPERATION_RENAME: i16 = 3;
const OPERATION_MOVE: i16 = 4;
const OPERATION_TRASH: i16 = 5;
const OPERATION_RESTORE: i16 = 6;
pub(crate) const OPERATION_CHANGE_SET: i16 = 7;

pub(crate) async fn place_committed_file(
    connection: &mut PgConnection,
    placement: &musteragent_runtime_resource::PreparedResourceFilePlacement,
    artifact_id: ArtifactId,
) -> Result<(), ResourceStoreError> {
    placement
        .validate()
        .map_err(|_| ResourceStoreError::Corrupt)?;
    let context = &placement.context;
    if !crate::resource_authority::lock_current_resource_context(connection, context).await? {
        return Err(ResourceStoreError::RevisionConflict);
    }
    place_file_in_qualified_scope(connection, placement, artifact_id).await
}

pub(crate) async fn place_verified_task_file(
    connection: &mut PgConnection,
    accepted: &crate::artifact::task_result::AcceptedPlacement,
) -> Result<(), ResourceStoreError> {
    let (placement, artifact_id) = accepted.material();
    place_file_in_qualified_scope(connection, placement, artifact_id).await
}

async fn place_file_in_qualified_scope(
    connection: &mut PgConnection,
    placement: &musteragent_runtime_resource::PreparedResourceFilePlacement,
    artifact_id: ArtifactId,
) -> Result<(), ResourceStoreError> {
    let context = &placement.context;
    let scope = context.scope();
    let (scope_kind, scope_id) = encode_scope(scope);
    sqlx::query(
        "INSERT INTO resource.scope_heads (
             tenant_id, scope_kind, scope_id, revision, updated_at_ms
         ) VALUES ($1, $2, $3, 0, $4)
         ON CONFLICT (tenant_id, scope_kind, scope_id) DO NOTHING",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(context.now.unix_millis())
    .execute(&mut *connection)
    .await
    .map_err(dependency)?;
    let current: i64 = sqlx::query_scalar(
        "SELECT revision
           FROM resource.scope_heads
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
          FOR UPDATE",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .fetch_one(&mut *connection)
    .await
    .map_err(dependency)?;
    let previous = decode_revision(current)?;
    if placement
        .expected_revision
        .is_some_and(|expected| expected != previous)
    {
        return Err(ResourceStoreError::RevisionConflict);
    }
    let next = previous.next().map_err(|_| ResourceStoreError::Corrupt)?;
    sqlx::query(
        "INSERT INTO resource.change_sets (
             change_set_id, tenant_id, scope_kind, scope_id, expected_revision,
             committed_revision, result_node_id, idempotency_key, command_fingerprint,
             actor_principal_id, source_invocation_id, operation, status,
             created_at_ms, committed_at_ms
         ) VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7, $8, $9, $10, $11, $12, NULL)",
    )
    .bind(placement.identity.change_set_id.into_uuid())
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(revision_i64(previous)?)
    .bind(placement.identity.idempotency_key.as_str())
    .bind(placement.identity.command_fingerprint.into_bytes().to_vec())
    .bind(context.actor.principal_id.into_uuid())
    .bind(
        context
            .cloud_invocation_id()
            .map(musteragent_identity::InvocationId::into_uuid),
    )
    .bind(OPERATION_LINK_FILE)
    .bind(CHANGE_SET_PENDING)
    .bind(context.now.unix_millis())
    .execute(&mut *connection)
    .await
    .map_err(map_write_error)?;

    let mut parent_node_id = placement.parent_node_id;
    if let Some(parent_id) = parent_node_id {
        let valid_parent = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                   FROM resource.nodes
                  WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
                    AND node_id = $4 AND kind = $5 AND lifecycle = $6
             )",
        )
        .bind(context.tenant_id.into_uuid())
        .bind(scope_kind)
        .bind(scope_id)
        .bind(parent_id.into_uuid())
        .bind(FOLDER_NODE)
        .bind(ACTIVE_NODE)
        .fetch_one(&mut *connection)
        .await
        .map_err(dependency)?;
        if !valid_parent {
            return Err(ResourceStoreError::ParentUnavailable);
        }
    }
    let mut deterministic_path = Vec::new();
    for (index, segment) in placement.path.iter().enumerate() {
        let is_file = index + 1 == placement.path.len();
        if is_file {
            insert_placement_node(
                connection,
                placement,
                placement.node_id,
                parent_node_id,
                segment,
                ResourceNodeKind::File,
                Some(artifact_id),
                &placement.sort_key,
                next,
            )
            .await?;
            continue;
        }
        let existing = sqlx::query(
            "SELECT node_id, kind
               FROM resource.nodes
              WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
                AND parent_node_id IS NOT DISTINCT FROM $4
                AND comparison_name = $5 AND lifecycle = $6",
        )
        .bind(context.tenant_id.into_uuid())
        .bind(scope_kind)
        .bind(scope_id)
        .bind(parent_node_id.map(ResourceNodeId::into_uuid))
        .bind(segment.comparison_key())
        .bind(ACTIVE_NODE)
        .fetch_optional(&mut *connection)
        .await
        .map_err(dependency)?;
        if let Some(existing) = existing {
            if existing.try_get::<i16, _>("kind").map_err(corrupt)? != FOLDER_NODE {
                return Err(ResourceStoreError::NameConflict);
            }
            parent_node_id = Some(
                ResourceNodeId::from_uuid(existing.try_get("node_id").map_err(corrupt)?)
                    .map_err(|_| ResourceStoreError::Corrupt)?,
            );
            continue;
        }
        deterministic_path.extend_from_slice(segment.comparison_key().as_bytes());
        deterministic_path.push(0);
        let folder_id = ResourceNodeId::from_uuid(uuid::Uuid::new_v5(
            &placement.node_id.into_uuid(),
            &deterministic_path,
        ))
        .map_err(|_| ResourceStoreError::Corrupt)?;
        insert_placement_node(
            connection,
            placement,
            folder_id,
            parent_node_id,
            segment,
            ResourceNodeKind::Folder,
            None,
            segment.comparison_key(),
            next,
        )
        .await?;
        parent_node_id = Some(folder_id);
    }

    let updated = sqlx::query(
        "UPDATE resource.scope_heads
            SET revision = $1, updated_at_ms = $2
          WHERE tenant_id = $3 AND scope_kind = $4 AND scope_id = $5
            AND revision = $6",
    )
    .bind(revision_i64(next)?)
    .bind(context.now.unix_millis())
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(revision_i64(previous)?)
    .execute(&mut *connection)
    .await
    .map_err(dependency)?;
    if updated.rows_affected() != 1 {
        return Err(ResourceStoreError::RevisionConflict);
    }
    let completed = sqlx::query(
        "UPDATE resource.change_sets
            SET committed_revision = $1, result_node_id = $2, status = $3, committed_at_ms = $4
          WHERE tenant_id = $5 AND scope_kind = $6 AND scope_id = $7
            AND change_set_id = $8 AND status = $9",
    )
    .bind(revision_i64(next)?)
    .bind(placement.node_id.into_uuid())
    .bind(CHANGE_SET_COMMITTED)
    .bind(context.now.unix_millis())
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(placement.identity.change_set_id.into_uuid())
    .bind(CHANGE_SET_PENDING)
    .execute(&mut *connection)
    .await
    .map_err(dependency)?;
    if completed.rows_affected() != 1 {
        return Err(ResourceStoreError::Corrupt);
    }
    Ok(())
}

/// Applies the canonical tree mutation inside a caller-owned fenced Unit of
/// Work. Product commands and Worker Tool commits share these exact CAS,
/// hierarchy and idempotency invariants; only the outer transaction owner
/// differs.
pub(crate) async fn apply_prepared_resource_mutation(
    connection: &mut PgConnection,
    mutation: &PreparedResourceMutation,
) -> Result<ResourceMutationReceipt, ResourceStoreError> {
    apply_prepared_resource_mutation_at_revision(connection, mutation, None).await
}

/// Reuses the canonical single-node mutation kernel inside a caller-owned
/// multi-node change set. The outer change set owns the scope-head CAS and
/// durable idempotency receipt, so this path changes nodes at one shared
/// revision without advancing the head for each member.
pub(crate) async fn apply_prepared_resource_mutation_at_revision(
    connection: &mut PgConnection,
    mutation: &PreparedResourceMutation,
    committed_revision: Option<ResourceRevision>,
) -> Result<ResourceMutationReceipt, ResourceStoreError> {
    let (context, identity, expected_revision, operation, node_id) = match mutation {
        PreparedResourceMutation::CreateFolder(command) => (
            &command.context,
            &command.identity,
            command.expected_revision,
            OPERATION_CREATE_FOLDER,
            command.node_id,
        ),
        PreparedResourceMutation::LinkFile(command) => (
            &command.context,
            &command.identity,
            command.expected_revision,
            OPERATION_LINK_FILE,
            command.node_id,
        ),
        PreparedResourceMutation::Rename(command) => (
            &command.context,
            &command.identity,
            command.expected_revision,
            OPERATION_RENAME,
            command.node_id,
        ),
        PreparedResourceMutation::Move(command) => (
            &command.context,
            &command.identity,
            command.expected_revision,
            OPERATION_MOVE,
            command.node_id,
        ),
        PreparedResourceMutation::Trash(command) => (
            &command.context,
            &command.identity,
            command.expected_revision,
            OPERATION_TRASH,
            command.node_id,
        ),
        PreparedResourceMutation::Restore(command) => (
            &command.context,
            &command.identity,
            command.expected_revision,
            OPERATION_RESTORE,
            command.node_id,
        ),
        PreparedResourceMutation::ChangeSet(_) | PreparedResourceMutation::ChangeSets { .. } => {
            return Err(ResourceStoreError::Corrupt);
        }
    };
    let scope = context.scope();
    let (revisions, owns_change_set) = if let Some(next) = committed_revision {
        if next.get() != expected_revision.get().saturating_add(1) {
            return Err(ResourceStoreError::Corrupt);
        }
        (
            Revisions {
                previous: expected_revision,
                next,
            },
            false,
        )
    } else {
        let Some(revisions) =
            begin_mutation(connection, context, identity, expected_revision, operation).await?
        else {
            return duplicate_receipt(connection, context, identity).await;
        };
        (revisions, true)
    };

    let mut changed = true;
    match mutation {
        PreparedResourceMutation::CreateFolder(command) => {
            validate_parent(connection, context.tenant_id, scope, command.parent_node_id).await?;
            insert_node(
                connection,
                InsertNode {
                    context,
                    node_id,
                    parent_node_id: command.parent_node_id,
                    name: &command.name,
                    kind: ResourceNodeKind::Folder,
                    file_artifact_id: None,
                    sort_key: &command.sort_key,
                    revision: revisions.next,
                },
            )
            .await?;
        }
        PreparedResourceMutation::LinkFile(command) => {
            validate_parent(connection, context.tenant_id, scope, command.parent_node_id).await?;
            validate_file_artifact(
                connection,
                context.tenant_id,
                scope,
                command.file_artifact_id,
            )
            .await?;
            insert_node(
                connection,
                InsertNode {
                    context,
                    node_id,
                    parent_node_id: command.parent_node_id,
                    name: &command.name,
                    kind: ResourceNodeKind::File,
                    file_artifact_id: Some(command.file_artifact_id),
                    sort_key: &command.sort_key,
                    revision: revisions.next,
                },
            )
            .await?;
        }
        PreparedResourceMutation::Rename(command) => {
            lock_active_node(connection, context.tenant_id, scope, node_id).await?;
            sqlx::query(
                "UPDATE resource.nodes
                    SET name = $1, comparison_name = $2, updated_at_ms = $3, revision = $4,
                        name_source_known=true,name_source_invocation_id=$6,name_source_device_invocation_id=$7,name_erasure_job_id=NULL
                  WHERE node_id = $5",
            )
            .bind(command.name.as_str())
            .bind(command.name.comparison_key())
            .bind(context.now.unix_millis())
            .bind(revision_i64(revisions.next)?)
            .bind(node_id.into_uuid())
            .bind(context.cloud_invocation_id().map(|id|id.into_uuid()))
            .bind(device_name_author(context))
            .execute(&mut *connection)
            .await
            .map_err(map_write_error)?;
        }
        PreparedResourceMutation::Move(command) => {
            lock_active_node(connection, context.tenant_id, scope, node_id).await?;
            let (current_parent, current_name): (Option<uuid::Uuid>, String) = sqlx::query_as(
                "SELECT parent_node_id, name FROM resource.nodes WHERE node_id = $1",
            )
            .bind(node_id.into_uuid())
            .fetch_one(&mut *connection)
            .await
            .map_err(dependency)?;
            let target_parent = command.target_parent_node_id.map(ResourceNodeId::into_uuid);
            let name = command
                .name
                .clone()
                .map(Ok)
                .unwrap_or_else(|| ResourceName::new(current_name.clone()))
                .map_err(|_| ResourceStoreError::Corrupt)?;
            if current_parent == target_parent && current_name == name.as_str() {
                changed = false;
            } else {
                validate_parent(
                    connection,
                    context.tenant_id,
                    scope,
                    command.target_parent_node_id,
                )
                .await?;
                if command.target_parent_node_id.is_some()
                    && move_creates_cycle(
                        connection,
                        context.tenant_id,
                        scope,
                        node_id,
                        command.target_parent_node_id,
                    )
                    .await?
                {
                    return Err(ResourceStoreError::MoveCycle);
                }
                sqlx::query(
                    "UPDATE resource.nodes
                        SET parent_node_id = $1, name = $2, comparison_name = $3,
                            name_source_known=CASE WHEN name<>$2 THEN true ELSE name_source_known END,
                            name_erasure_job_id=CASE WHEN name<>$2 THEN NULL ELSE name_erasure_job_id END,
                            name_source_invocation_id=CASE WHEN name<>$2 THEN $7 ELSE name_source_invocation_id END,
                            name_source_device_invocation_id=CASE WHEN name<>$2 THEN $8 ELSE name_source_device_invocation_id END,
                            updated_at_ms = $4, revision = $5
                      WHERE node_id = $6",
                )
                .bind(command.target_parent_node_id.map(ResourceNodeId::into_uuid))
                .bind(name.as_str())
                .bind(name.comparison_key())
                .bind(context.now.unix_millis())
                .bind(revision_i64(revisions.next)?)
                .bind(node_id.into_uuid())
                .bind(context.cloud_invocation_id().map(|id|id.into_uuid()))
                .bind(device_name_author(context))
                .execute(&mut *connection)
                .await
                .map_err(map_write_error)?;
            }
        }
        PreparedResourceMutation::Trash(_) => {
            let parent: Option<uuid::Uuid> = sqlx::query_scalar(
                "SELECT parent_node_id
                   FROM resource.nodes
                  WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
                    AND node_id = $4 AND lifecycle = $5
                  FOR UPDATE",
            )
            .bind(context.tenant_id.into_uuid())
            .bind(encode_scope(scope).0)
            .bind(encode_scope(scope).1)
            .bind(node_id.into_uuid())
            .bind(ACTIVE_NODE)
            .fetch_optional(&mut *connection)
            .await
            .map_err(dependency)?
            .ok_or(ResourceStoreError::NodeUnavailable)?;
            sqlx::query(
                "WITH RECURSIVE subtree(node_id) AS (
                     SELECT node_id FROM resource.nodes WHERE node_id = $5
                     UNION ALL
                     SELECT child.node_id FROM resource.nodes AS child
                       JOIN subtree AS parent ON child.parent_node_id = parent.node_id
                      WHERE child.tenant_id = $6 AND child.scope_kind = $7
                        AND child.scope_id = $8 AND child.lifecycle = $9
                 )
                 UPDATE resource.nodes
                    SET parent_node_id = CASE WHEN node_id = $5 THEN NULL ELSE parent_node_id END,
                        original_parent_node_id = CASE WHEN node_id = $5 THEN $1 ELSE NULL END,
                        lifecycle = $2, updated_at_ms = $3, revision = $4
                  WHERE node_id IN (SELECT node_id FROM subtree)",
            )
            .bind(parent)
            .bind(TRASHED_NODE)
            .bind(context.now.unix_millis())
            .bind(revision_i64(revisions.next)?)
            .bind(node_id.into_uuid())
            .bind(context.tenant_id.into_uuid())
            .bind(encode_scope(scope).0)
            .bind(encode_scope(scope).1)
            .bind(ACTIVE_NODE)
            .execute(&mut *connection)
            .await
            .map_err(map_write_error)?;
        }
        PreparedResourceMutation::Restore(command) => {
            let row = sqlx::query(
                "SELECT original_parent_node_id, name
                   FROM resource.nodes
                  WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
                    AND node_id = $4 AND lifecycle = $5
                  FOR UPDATE",
            )
            .bind(context.tenant_id.into_uuid())
            .bind(encode_scope(scope).0)
            .bind(encode_scope(scope).1)
            .bind(node_id.into_uuid())
            .bind(TRASHED_NODE)
            .fetch_optional(&mut *connection)
            .await
            .map_err(dependency)?
            .ok_or(ResourceStoreError::NodeUnavailable)?;
            let original_parent: Option<uuid::Uuid> =
                row.try_get("original_parent_node_id").map_err(corrupt)?;
            let target_parent = command
                .target_parent_node_id
                .map(ResourceNodeId::into_uuid)
                .or(original_parent);
            validate_parent(
                connection,
                context.tenant_id,
                scope,
                target_parent
                    .map(ResourceNodeId::from_uuid)
                    .transpose()
                    .map_err(|_| ResourceStoreError::Corrupt)?,
            )
            .await?;
            let current_name: String = row.try_get("name").map_err(corrupt)?;
            let name = command
                .name
                .clone()
                .map(Ok)
                .unwrap_or_else(|| ResourceName::new(current_name))
                .map_err(|_| ResourceStoreError::Corrupt)?;
            sqlx::query(
                "WITH RECURSIVE subtree(node_id) AS (
                     SELECT node_id FROM resource.nodes WHERE node_id = $7
                     UNION ALL
                     SELECT child.node_id FROM resource.nodes AS child
                       JOIN subtree AS parent ON child.parent_node_id = parent.node_id
                      WHERE child.tenant_id = $8 AND child.scope_kind = $9
                        AND child.scope_id = $10 AND child.lifecycle = $11
                 )
                 UPDATE resource.nodes
                    SET parent_node_id = CASE WHEN node_id = $7 THEN $1 ELSE parent_node_id END,
                        original_parent_node_id = NULL,
                        name = CASE WHEN node_id = $7 THEN $2 ELSE name END,
                        name_source_known=CASE WHEN node_id=$7 AND name<>$2 THEN true ELSE name_source_known END,
                        name_erasure_job_id=CASE WHEN node_id=$7 AND name<>$2 THEN NULL ELSE name_erasure_job_id END,
                        name_source_invocation_id=CASE WHEN node_id=$7 AND name<>$2 THEN $12 ELSE name_source_invocation_id END,
                        name_source_device_invocation_id=CASE WHEN node_id=$7 AND name<>$2 THEN $13 ELSE name_source_device_invocation_id END,
                        comparison_name = CASE WHEN node_id = $7 THEN $3 ELSE comparison_name END,
                        lifecycle = $4, updated_at_ms = $5, revision = $6
                  WHERE node_id IN (SELECT node_id FROM subtree)",
            )
            .bind(target_parent)
            .bind(name.as_str())
            .bind(name.comparison_key())
            .bind(ACTIVE_NODE)
            .bind(context.now.unix_millis())
            .bind(revision_i64(revisions.next)?)
            .bind(node_id.into_uuid())
            .bind(context.tenant_id.into_uuid())
            .bind(encode_scope(scope).0)
            .bind(encode_scope(scope).1)
            .bind(TRASHED_NODE)
            .bind(context.cloud_invocation_id().map(|id|id.into_uuid()))
            .bind(device_name_author(context))
            .execute(&mut *connection)
            .await
            .map_err(map_write_error)?;
        }
        PreparedResourceMutation::ChangeSet(_) | PreparedResourceMutation::ChangeSets { .. } => {
            return Err(ResourceStoreError::Corrupt);
        }
    }
    let receipt_revisions = if owns_change_set && !changed {
        complete_noop_mutation(connection, context, identity, node_id, revisions.previous).await?;
        Revisions {
            previous: revisions.previous,
            next: revisions.previous,
        }
    } else {
        if owns_change_set {
            complete_mutation(connection, context, identity, node_id, revisions.next).await?;
        }
        revisions
    };
    let node = load_node_on(connection, context.tenant_id, node_id).await?;
    Ok(receipt(scope, node, receipt_revisions, false))
}

#[allow(clippy::too_many_arguments)]
async fn insert_placement_node(
    connection: &mut PgConnection,
    placement: &musteragent_runtime_resource::PreparedResourceFilePlacement,
    node_id: ResourceNodeId,
    parent_node_id: Option<ResourceNodeId>,
    name: &ResourceName,
    kind: ResourceNodeKind,
    artifact_id: Option<ArtifactId>,
    sort_key: &str,
    revision: ResourceRevision,
) -> Result<(), ResourceStoreError> {
    let context = &placement.context;
    let (scope_kind, scope_id) = encode_scope(context.scope());
    sqlx::query(
        "INSERT INTO resource.nodes (
             node_id, tenant_id, scope_kind, scope_id, parent_node_id,
             original_parent_node_id, name, comparison_name, kind, lifecycle,
             file_artifact_id, sort_key, actor_principal_id, actor_principal_kind,
             source_conversation_id, source_invocation_id, source_tool_key,
             created_at_ms, updated_at_ms, revision, name_source_device_invocation_id
         ) VALUES (
             $1, $2, $3, $4, $5, NULL, $6, $7, $8, $9,
             $10, $11, $12, $13, $14, $15, $16, $17, $17, $18, $19
         )",
    )
    .bind(node_id.into_uuid())
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(parent_node_id.map(ResourceNodeId::into_uuid))
    .bind(name.as_str())
    .bind(name.comparison_key())
    .bind(encode_node_kind(kind))
    .bind(ACTIVE_NODE)
    .bind(artifact_id.map(ArtifactId::into_uuid))
    .bind(sort_key)
    .bind(context.actor.principal_id.into_uuid())
    .bind(encode_principal_kind(context.actor.principal_kind))
    .bind(
        context
            .cloud_conversation_id()
            .map(musteragent_identity::ConversationId::into_uuid),
    )
    .bind(
        context
            .cloud_invocation_id()
            .map(musteragent_identity::InvocationId::into_uuid),
    )
    .bind(context.tool_key.as_ref().map(VersionedKey::as_str))
    .bind(context.now.unix_millis())
    .bind(revision_i64(revision)?)
    .bind(device_name_author(context))
    .execute(&mut *connection)
    .await
    .map_err(map_write_error)?;
    Ok(())
}

#[derive(Clone)]
pub struct PostgresResourceStore {
    pub(crate) pool: PgPool,
    pub(crate) database: crate::PostgresDatabaseRuntime,
}

impl PostgresResourceStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self { pool, database }
    }

    pub(crate) async fn write_resource_unit_of_work(
        &self,
    ) -> Result<crate::GovernedPostgresTransaction<'static>, ResourceStoreError> {
        self.database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)
    }

    async fn list_children_inner(
        &self,
        query: &ListResourceChildren,
    ) -> Result<ResourceNodePage, ResourceStoreError> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Resource,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        crate::resource_file_catalog::authorize(&mut tx, &query.context).await?;
        let page = read::page(
            &mut tx,
            query.context.tenant_id,
            query.context.scope(),
            query.parent_node_id,
            query.include_trashed,
            query.offset,
            query.limit,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(page)
    }

    async fn create_folder_inner(
        &self,
        command: &CreateResourceFolder,
    ) -> Result<ResourceMutationReceipt, ResourceStoreError> {
        let mut transaction = self.write_resource_unit_of_work().await?;
        let receipt = apply_prepared_resource_mutation(
            &mut transaction,
            &PreparedResourceMutation::CreateFolder(command.clone()),
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(receipt)
    }

    async fn link_file_inner(
        &self,
        command: &LinkResourceFile,
    ) -> Result<ResourceMutationReceipt, ResourceStoreError> {
        let mut transaction = self.write_resource_unit_of_work().await?;
        let receipt = apply_prepared_resource_mutation(
            &mut transaction,
            &PreparedResourceMutation::LinkFile(command.clone()),
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(receipt)
    }

    async fn create_file_inner(
        &self,
        command: &CreateResourceFile,
    ) -> Result<ResourceMutationReceipt, ResourceStoreError> {
        let mut transaction = self.write_resource_unit_of_work().await?;
        let scope = command.context.scope();
        let Some(revisions) = begin_mutation(
            &mut transaction,
            &command.context,
            &command.identity,
            command.expected_revision,
            OPERATION_LINK_FILE,
        )
        .await?
        else {
            let receipt =
                duplicate_receipt(&mut transaction, &command.context, &command.identity).await?;
            transaction
                .commit()
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            return Ok(receipt);
        };
        crate::artifact::authorize_content(
            &mut transaction,
            command.context.tenant_id,
            &command.version,
        )
        .await
        .map_err(resource_artifact_error)?;
        crate::artifact::insert_artifact(&mut transaction, &command.artifact)
            .await
            .map_err(resource_artifact_error)?;
        crate::artifact::insert_version(&mut transaction, &command.version)
            .await
            .map_err(resource_artifact_error)?;
        validate_parent(
            &mut transaction,
            command.context.tenant_id,
            scope,
            command.parent_node_id,
        )
        .await?;
        insert_node(
            &mut transaction,
            InsertNode {
                context: &command.context,
                node_id: command.node_id,
                parent_node_id: command.parent_node_id,
                name: &command.name,
                kind: ResourceNodeKind::File,
                file_artifact_id: Some(command.artifact.artifact_id),
                sort_key: &command.sort_key,
                revision: revisions.next,
            },
        )
        .await?;
        complete_mutation(
            &mut transaction,
            &command.context,
            &command.identity,
            command.node_id,
            revisions.next,
        )
        .await?;
        let node =
            load_node_on(&mut transaction, command.context.tenant_id, command.node_id).await?;
        let result = receipt(scope, node, revisions, false);
        transaction
            .commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(result)
    }

    async fn rename_node_inner(
        &self,
        command: &RenameResourceNode,
    ) -> Result<ResourceMutationReceipt, ResourceStoreError> {
        let mut transaction = self.write_resource_unit_of_work().await?;
        let receipt = apply_prepared_resource_mutation(
            &mut transaction,
            &PreparedResourceMutation::Rename(command.clone()),
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(receipt)
    }

    async fn move_node_inner(
        &self,
        command: &MoveResourceNode,
    ) -> Result<ResourceMutationReceipt, ResourceStoreError> {
        let mut transaction = self.write_resource_unit_of_work().await?;
        let receipt = apply_prepared_resource_mutation(
            &mut transaction,
            &PreparedResourceMutation::Move(command.clone()),
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(receipt)
    }

    async fn trash_node_inner(
        &self,
        command: &TrashResourceNode,
    ) -> Result<ResourceMutationReceipt, ResourceStoreError> {
        let mut transaction = self.write_resource_unit_of_work().await?;
        let receipt = apply_prepared_resource_mutation(
            &mut transaction,
            &PreparedResourceMutation::Trash(command.clone()),
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(receipt)
    }

    async fn restore_node_inner(
        &self,
        command: &RestoreResourceNode,
    ) -> Result<ResourceMutationReceipt, ResourceStoreError> {
        let mut transaction = self.write_resource_unit_of_work().await?;
        let receipt = apply_prepared_resource_mutation(
            &mut transaction,
            &PreparedResourceMutation::Restore(command.clone()),
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(receipt)
    }
}

impl ResourceStore for PostgresResourceStore {
    fn list_children<'a>(
        &'a self,
        query: &'a ListResourceChildren,
    ) -> ApplicationFuture<'a, Result<ResourceNodePage, ResourceStoreError>> {
        Box::pin(self.list_children_inner(query))
    }

    fn create_folder<'a>(
        &'a self,
        command: &'a CreateResourceFolder,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>> {
        Box::pin(self.create_folder_inner(command))
    }

    fn link_file<'a>(
        &'a self,
        command: &'a LinkResourceFile,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>> {
        Box::pin(self.link_file_inner(command))
    }

    fn create_file<'a>(
        &'a self,
        command: &'a CreateResourceFile,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>> {
        Box::pin(self.create_file_inner(command))
    }

    fn rename_node<'a>(
        &'a self,
        command: &'a RenameResourceNode,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>> {
        Box::pin(self.rename_node_inner(command))
    }

    fn move_node<'a>(
        &'a self,
        command: &'a MoveResourceNode,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>> {
        Box::pin(self.move_node_inner(command))
    }

    fn trash_node<'a>(
        &'a self,
        command: &'a TrashResourceNode,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>> {
        Box::pin(self.trash_node_inner(command))
    }

    fn restore_node<'a>(
        &'a self,
        command: &'a RestoreResourceNode,
    ) -> ApplicationFuture<'a, Result<ResourceMutationReceipt, ResourceStoreError>> {
        Box::pin(self.restore_node_inner(command))
    }

    fn commit_change_set<'a>(
        &'a self,
        command: &'a musteragent_runtime_resource::CommitResourceChangeSet,
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_runtime_resource::ResourceChangeSetReceipt, ResourceStoreError>,
    > {
        Box::pin(self.commit_change_set_inner(command))
    }
}

pub(crate) fn resource_artifact_error(
    error: crate::artifact::PostgresArtifactError,
) -> ResourceStoreError {
    match error {
        crate::artifact::PostgresArtifactError::ContentUnavailable => {
            ResourceStoreError::FileUnavailable
        }
        crate::artifact::PostgresArtifactError::ArtifactConflict
        | crate::artifact::PostgresArtifactError::VersionConflict => {
            ResourceStoreError::RevisionConflict
        }
        crate::artifact::PostgresArtifactError::Corrupt => ResourceStoreError::Corrupt,
        _ => ResourceStoreError::DependencyUnavailable,
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Revisions {
    pub(crate) previous: ResourceRevision,
    pub(crate) next: ResourceRevision,
}

pub(crate) async fn begin_mutation(
    transaction: &mut PgConnection,
    context: &musteragent_runtime_resource::ResourceCommandContext,
    identity: &ResourceMutationIdentity,
    expected_revision: ResourceRevision,
    operation: i16,
) -> Result<Option<Revisions>, ResourceStoreError> {
    let scope = context.scope();
    let (scope_kind, scope_id) = encode_scope(scope);
    let current_context =
        crate::resource_authority::lock_current_resource_context(transaction, context).await?;
    // A workspace origin has no prior Conversation scope to recover from.
    // Current registered ownership is required even for duplicate receipts.
    if (context.conversation_id().is_none()
        || matches!(
            context.source,
            musteragent_runtime_resource::ResourceCommandSource::ExecutionWorkspace(_)
        ))
        && !current_context
    {
        return Err(ResourceStoreError::RevisionConflict);
    }
    if let Some(row) = sqlx::query(
        "SELECT change_set_id, expected_revision, command_fingerprint, operation, status
           FROM resource.change_sets
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
            AND idempotency_key = $4",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(identity.idempotency_key.as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(dependency)?
    {
        let same = row
            .try_get::<uuid::Uuid, _>("change_set_id")
            .map_err(corrupt)?
            == identity.change_set_id.into_uuid()
            && row
                .try_get::<i64, _>("expected_revision")
                .map_err(corrupt)?
                == revision_i64(expected_revision)?
            && row
                .try_get::<Vec<u8>, _>("command_fingerprint")
                .map_err(corrupt)?
                == identity.command_fingerprint.into_bytes()
            && row.try_get::<i16, _>("operation").map_err(corrupt)? == operation
            && row.try_get::<i16, _>("status").map_err(corrupt)? == CHANGE_SET_COMMITTED;
        return if same {
            Ok(None)
        } else {
            Err(ResourceStoreError::RevisionConflict)
        };
    }
    if !current_context {
        return Err(ResourceStoreError::RevisionConflict);
    }
    sqlx::query(
        "INSERT INTO resource.scope_heads (
             tenant_id, scope_kind, scope_id, revision, updated_at_ms
         ) VALUES ($1, $2, $3, 0, $4)
         ON CONFLICT (tenant_id, scope_kind, scope_id) DO NOTHING",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(context.now.unix_millis())
    .execute(&mut *transaction)
    .await
    .map_err(dependency)?;
    let current: i64 = sqlx::query_scalar(
        "SELECT revision
           FROM resource.scope_heads
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
          FOR UPDATE",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .fetch_one(&mut *transaction)
    .await
    .map_err(dependency)?;
    let previous = decode_revision(current)?;
    if previous != expected_revision {
        return Err(ResourceStoreError::RevisionConflict);
    }
    let next = previous.next().map_err(|_| ResourceStoreError::Corrupt)?;
    sqlx::query(
        "INSERT INTO resource.change_sets (
             change_set_id, tenant_id, scope_kind, scope_id, expected_revision,
             committed_revision, result_node_id, idempotency_key, command_fingerprint,
             actor_principal_id, source_invocation_id, operation, status,
             created_at_ms, committed_at_ms
         ) VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7, $8, $9, $10, $11, $12, NULL)",
    )
    .bind(identity.change_set_id.into_uuid())
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(revision_i64(expected_revision)?)
    .bind(identity.idempotency_key.as_str())
    .bind(identity.command_fingerprint.into_bytes().to_vec())
    .bind(context.actor.principal_id.into_uuid())
    .bind(
        context
            .cloud_invocation_id()
            .map(musteragent_identity::InvocationId::into_uuid),
    )
    .bind(operation)
    .bind(CHANGE_SET_PENDING)
    .bind(context.now.unix_millis())
    .execute(&mut *transaction)
    .await
    .map_err(map_write_error)?;
    Ok(Some(Revisions { previous, next }))
}

pub(crate) async fn complete_mutation(
    transaction: &mut PgConnection,
    context: &musteragent_runtime_resource::ResourceCommandContext,
    identity: &ResourceMutationIdentity,
    node_id: ResourceNodeId,
    revision: ResourceRevision,
) -> Result<(), ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(context.scope());
    let head = sqlx::query(
        "UPDATE resource.scope_heads
            SET revision = $1, updated_at_ms = $2
          WHERE tenant_id = $3 AND scope_kind = $4 AND scope_id = $5
            AND revision = $6",
    )
    .bind(revision_i64(revision)?)
    .bind(context.now.unix_millis())
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(revision_i64(ResourceRevision::new(revision.get() - 1))?)
    .execute(&mut *transaction)
    .await
    .map_err(dependency)?;
    if head.rows_affected() != 1 {
        return Err(ResourceStoreError::RevisionConflict);
    }
    complete_noop_mutation(transaction, context, identity, node_id, revision).await
}

async fn complete_noop_mutation(
    transaction: &mut PgConnection,
    context: &musteragent_runtime_resource::ResourceCommandContext,
    identity: &ResourceMutationIdentity,
    node_id: ResourceNodeId,
    revision: ResourceRevision,
) -> Result<(), ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(context.scope());
    let change_set = sqlx::query(
        "UPDATE resource.change_sets
            SET committed_revision = $1, result_node_id = $2, status = $3, committed_at_ms = $4
          WHERE tenant_id = $5 AND scope_kind = $6 AND scope_id = $7
            AND change_set_id = $8 AND status = $9",
    )
    .bind(revision_i64(revision)?)
    .bind(node_id.into_uuid())
    .bind(CHANGE_SET_COMMITTED)
    .bind(context.now.unix_millis())
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(identity.change_set_id.into_uuid())
    .bind(CHANGE_SET_PENDING)
    .execute(&mut *transaction)
    .await
    .map_err(dependency)?;
    if change_set.rows_affected() != 1 {
        return Err(ResourceStoreError::Corrupt);
    }
    Ok(())
}

pub(crate) async fn duplicate_receipt(
    transaction: &mut PgConnection,
    context: &musteragent_runtime_resource::ResourceCommandContext,
    identity: &ResourceMutationIdentity,
) -> Result<ResourceMutationReceipt, ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(context.scope());
    let row = sqlx::query(
        "SELECT expected_revision, committed_revision, result_node_id
           FROM resource.change_sets
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
            AND change_set_id = $4 AND status = $5",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(identity.change_set_id.into_uuid())
    .bind(CHANGE_SET_COMMITTED)
    .fetch_one(&mut *transaction)
    .await
    .map_err(dependency)?;
    let node_id = ResourceNodeId::from_uuid(
        row.try_get::<uuid::Uuid, _>("result_node_id")
            .map_err(corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)?;
    let node = sqlx::query(
        "SELECT snapshot.*
           FROM resource.change_sets AS receipt
           CROSS JOIN LATERAL jsonb_populate_record(
               NULL::resource.nodes, resource.project_receipt_node(receipt.result_node_snapshot, receipt.legacy_dialogue_snapshot)
           ) AS snapshot
          WHERE receipt.tenant_id = $1 AND receipt.change_set_id = $2",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(identity.change_set_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await
    .map_err(dependency)
    .and_then(decode_node)?;
    if node.node_id != node_id || node.scope != context.scope() {
        return Err(ResourceStoreError::Corrupt);
    }
    let previous = decode_revision(row.try_get("expected_revision").map_err(corrupt)?)?;
    let revision = decode_revision(row.try_get("committed_revision").map_err(corrupt)?)?;
    Ok(ResourceMutationReceipt {
        scope: context.scope(),
        node,
        previous_revision: previous,
        revision,
        duplicate: true,
    })
}

pub(crate) struct InsertNode<'a> {
    pub(crate) context: &'a musteragent_runtime_resource::ResourceCommandContext,
    pub(crate) node_id: ResourceNodeId,
    pub(crate) parent_node_id: Option<ResourceNodeId>,
    pub(crate) name: &'a ResourceName,
    pub(crate) kind: ResourceNodeKind,
    pub(crate) file_artifact_id: Option<ArtifactId>,
    pub(crate) sort_key: &'a str,
    pub(crate) revision: ResourceRevision,
}

pub(crate) async fn insert_node(
    transaction: &mut PgConnection,
    node: InsertNode<'_>,
) -> Result<(), ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(node.context.scope());
    sqlx::query(
        "INSERT INTO resource.nodes (
             node_id, tenant_id, scope_kind, scope_id, parent_node_id,
             original_parent_node_id, name, comparison_name, kind, lifecycle,
             file_artifact_id, sort_key, actor_principal_id, actor_principal_kind,
             source_conversation_id, source_invocation_id, source_tool_key,
             created_at_ms, updated_at_ms, revision, name_source_device_invocation_id
         ) VALUES (
             $1, $2, $3, $4, $5, NULL, $6, $7, $8, $9,
             $10, $11, $12, $13, $14, $15, $16, $17, $17, $18, $19
         )",
    )
    .bind(node.node_id.into_uuid())
    .bind(node.context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(node.parent_node_id.map(ResourceNodeId::into_uuid))
    .bind(node.name.as_str())
    .bind(node.name.comparison_key())
    .bind(encode_node_kind(node.kind))
    .bind(ACTIVE_NODE)
    .bind(node.file_artifact_id.map(ArtifactId::into_uuid))
    .bind(node.sort_key)
    .bind(node.context.actor.principal_id.into_uuid())
    .bind(encode_principal_kind(node.context.actor.principal_kind))
    .bind(
        node.context
            .cloud_conversation_id()
            .map(musteragent_identity::ConversationId::into_uuid),
    )
    .bind(
        node.context
            .cloud_invocation_id()
            .map(musteragent_identity::InvocationId::into_uuid),
    )
    .bind(node.context.tool_key.as_ref().map(VersionedKey::as_str))
    .bind(node.context.now.unix_millis())
    .bind(revision_i64(node.revision)?)
    .bind(device_name_author(node.context))
    .execute(&mut *transaction)
    .await
    .map_err(map_write_error)?;
    Ok(())
}

pub(crate) async fn validate_parent(
    transaction: &mut PgConnection,
    tenant_id: TenantId,
    scope: ResourceScope,
    parent_node_id: Option<ResourceNodeId>,
) -> Result<(), ResourceStoreError> {
    let Some(parent_node_id) = parent_node_id else {
        return Ok(());
    };
    let (scope_kind, scope_id) = encode_scope(scope);
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resource.nodes
              WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
                AND node_id = $4 AND kind = $5 AND lifecycle = $6
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(parent_node_id.into_uuid())
    .bind(FOLDER_NODE)
    .bind(ACTIVE_NODE)
    .fetch_one(&mut *transaction)
    .await
    .map_err(dependency)?;
    if valid {
        Ok(())
    } else {
        Err(ResourceStoreError::ParentUnavailable)
    }
}

async fn validate_file_artifact(
    transaction: &mut PgConnection,
    tenant_id: TenantId,
    scope: ResourceScope,
    artifact_id: ArtifactId,
) -> Result<(), ResourceStoreError> {
    let valid: bool = match scope {
        ResourceScope::OwnedAgent(agent_id) => sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM artifact.artifacts a
             JOIN agent.dialogue_sessions s ON s.tenant_id=a.tenant_id AND s.conversation_id=a.conversation_id
             JOIN agent.owned_agents agent ON agent.agent_id=s.agent_id
             WHERE a.tenant_id=$1 AND a.artifact_id=$2 AND s.agent_id=$3 AND s.purpose IN ('intent_coordination','work_item_execution','work_item_report')
               AND EXISTS(SELECT 1 FROM resource.file_versions WHERE artifact_id=a.artifact_id)
               AND NOT EXISTS(SELECT 1 FROM resource.nodes WHERE file_artifact_id=a.artifact_id))")
            .bind(tenant_id.into_uuid()).bind(artifact_id.into_uuid()).bind(agent_id.into_uuid())
            .fetch_one(&mut *transaction).await.map_err(dependency)?,
        ResourceScope::ExecutionWorkspace(id) => sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM artifact.artifacts AS a
                JOIN resource.execution_workspaces AS w
                  ON w.tenant_id = a.tenant_id AND w.conversation_id = a.conversation_id
                WHERE a.tenant_id = $1 AND a.artifact_id = $2 AND w.execution_workspace_id = $3
                  AND EXISTS (SELECT 1 FROM resource.file_versions WHERE artifact_id = a.artifact_id)
                  AND NOT EXISTS (SELECT 1 FROM resource.nodes WHERE file_artifact_id = a.artifact_id)
            )",
        )
        .bind(tenant_id.into_uuid())
        .bind(artifact_id.into_uuid())
        .bind(id.into_uuid())
        .fetch_one(&mut *transaction)
        .await
        .map_err(dependency)?,
        ResourceScope::Project(workspace_id) => sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                   FROM artifact.artifacts AS artifact_row
                   LEFT JOIN conversation.conversations AS conversation_row
                     ON conversation_row.tenant_id = artifact_row.tenant_id
                    AND conversation_row.conversation_id = artifact_row.conversation_id
                  WHERE artifact_row.tenant_id = $1
                    AND artifact_row.artifact_id = $2
                    AND EXISTS (SELECT 1 FROM resource.file_versions WHERE artifact_id = artifact_row.artifact_id)
                    AND COALESCE(artifact_row.workspace_id, conversation_row.workspace_id) = $3
                    AND NOT EXISTS (
                        SELECT 1 FROM resource.nodes WHERE file_artifact_id = $2
                    )
             )",
        )
        .bind(tenant_id.into_uuid())
        .bind(artifact_id.into_uuid())
        .bind(workspace_id.into_uuid())
        .fetch_one(&mut *transaction)
        .await
        .map_err(dependency)?,
        ResourceScope::Conversation(conversation_id) => sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM artifact.artifacts
                  WHERE tenant_id = $1 AND artifact_id = $2 AND EXISTS (SELECT 1 FROM resource.file_versions WHERE artifact_id = $2)
                    AND conversation_id = $3
                    AND NOT EXISTS (
                        SELECT 1 FROM resource.nodes WHERE file_artifact_id = $2
                    )
             )",
        )
        .bind(tenant_id.into_uuid())
        .bind(artifact_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_one(&mut *transaction)
        .await
        .map_err(dependency)?,
    };
    if valid {
        Ok(())
    } else {
        Err(ResourceStoreError::FileUnavailable)
    }
}

pub(crate) async fn lock_active_node(
    transaction: &mut PgConnection,
    tenant_id: TenantId,
    scope: ResourceScope,
    node_id: ResourceNodeId,
) -> Result<(), ResourceStoreError> {
    let (scope_kind, scope_id) = encode_scope(scope);
    sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT node_id FROM resource.nodes
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
            AND node_id = $4 AND lifecycle = $5
          FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(node_id.into_uuid())
    .bind(ACTIVE_NODE)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(dependency)?
    .ok_or(ResourceStoreError::NodeUnavailable)
    .map(|_| ())
}

pub(crate) async fn move_creates_cycle(
    transaction: &mut PgConnection,
    tenant_id: TenantId,
    scope: ResourceScope,
    node_id: ResourceNodeId,
    target: Option<ResourceNodeId>,
) -> Result<bool, ResourceStoreError> {
    let Some(target) = target else {
        return Ok(false);
    };
    let (scope_kind, scope_id) = encode_scope(scope);
    sqlx::query_scalar(
        "WITH RECURSIVE descendants(node_id) AS (
             SELECT node_id FROM resource.nodes
              WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
                AND parent_node_id = $4 AND lifecycle = $5
             UNION ALL
             SELECT child.node_id
               FROM resource.nodes AS child
               JOIN descendants AS parent ON child.parent_node_id = parent.node_id
              WHERE child.tenant_id = $1 AND child.scope_kind = $2
                AND child.scope_id = $3 AND child.lifecycle = $5
         )
         SELECT $6 = $4 OR EXISTS (SELECT 1 FROM descendants WHERE node_id = $6)",
    )
    .bind(tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(node_id.into_uuid())
    .bind(ACTIVE_NODE)
    .bind(target.into_uuid())
    .fetch_one(&mut *transaction)
    .await
    .map_err(dependency)
}

pub(crate) fn decode_node(row: sqlx::postgres::PgRow) -> Result<ResourceNode, ResourceStoreError> {
    let tenant_id = TenantId::from_uuid(row.try_get("tenant_id").map_err(corrupt)?)
        .map_err(|_| ResourceStoreError::Corrupt)?;
    let scope = decode_scope(
        row.try_get("scope_kind").map_err(corrupt)?,
        row.try_get("scope_id").map_err(corrupt)?,
    )?;
    let name = ResourceName::new(row.try_get::<String, _>("name").map_err(corrupt)?)
        .map_err(|_| ResourceStoreError::Corrupt)?;
    if name.comparison_key()
        != row
            .try_get::<String, _>("comparison_name")
            .map_err(corrupt)?
    {
        return Err(ResourceStoreError::Corrupt);
    }
    let node = ResourceNode {
        is_result: row
            .try_get::<Option<bool>, _>("is_result")
            .map_err(corrupt)?
            .unwrap_or(false),
        node_id: ResourceNodeId::from_uuid(row.try_get("node_id").map_err(corrupt)?)
            .map_err(|_| ResourceStoreError::Corrupt)?,
        tenant_id,
        scope,
        parent_node_id: optional_node_id(row.try_get("parent_node_id").map_err(corrupt)?)?,
        original_parent_node_id: optional_node_id(
            row.try_get("original_parent_node_id").map_err(corrupt)?,
        )?,
        name,
        kind: decode_node_kind(row.try_get("kind").map_err(corrupt)?)?,
        lifecycle: decode_lifecycle(row.try_get("lifecycle").map_err(corrupt)?)?,
        file_artifact_id: row
            .try_get::<Option<uuid::Uuid>, _>("file_artifact_id")
            .map_err(corrupt)?
            .map(ArtifactId::from_uuid)
            .transpose()
            .map_err(|_| ResourceStoreError::Corrupt)?,
        sort_key: row.try_get("sort_key").map_err(corrupt)?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(corrupt)?),
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms").map_err(corrupt)?),
        revision: decode_revision(row.try_get("revision").map_err(corrupt)?)?,
        provenance: ResourceProvenance {
            actor: Actor {
                tenant_id,
                principal_id: PrincipalId::from_uuid(
                    row.try_get("actor_principal_id").map_err(corrupt)?,
                )
                .map_err(|_| ResourceStoreError::Corrupt)?,
                principal_kind: decode_principal_kind(
                    row.try_get("actor_principal_kind").map_err(corrupt)?,
                )?,
            },
            conversation_id: row
                .try_get::<Option<uuid::Uuid>, _>("source_conversation_id")
                .map_err(corrupt)?
                .map(ConversationId::from_uuid)
                .transpose()
                .map_err(|_| ResourceStoreError::Corrupt)?,
            invocation_id: row
                .try_get::<Option<uuid::Uuid>, _>("source_invocation_id")
                .map_err(corrupt)?
                .map(musteragent_identity::InvocationId::from_uuid)
                .transpose()
                .map_err(|_| ResourceStoreError::Corrupt)?,
            tool_key: row
                .try_get::<Option<String>, _>("source_tool_key")
                .map_err(corrupt)?
                .map(VersionedKey::new)
                .transpose()
                .map_err(|_| ResourceStoreError::Corrupt)?,
        },
    };
    node.validate().map_err(|_| ResourceStoreError::Corrupt)?;
    Ok(node)
}

pub(crate) async fn load_node_on(
    connection: &mut PgConnection,
    tenant_id: TenantId,
    node_id: ResourceNodeId,
) -> Result<ResourceNode, ResourceStoreError> {
    sqlx::query(
        "SELECT node_id, tenant_id, scope_kind, scope_id, parent_node_id,
                original_parent_node_id, name, comparison_name, kind, lifecycle,
                file_artifact_id, sort_key, actor_principal_id, actor_principal_kind,
                source_conversation_id, source_invocation_id, source_tool_key,
                created_at_ms, updated_at_ms, revision, is_result
           FROM resource.nodes
          WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(node_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(dependency)?
    .map(decode_node)
    .transpose()?
    .ok_or(ResourceStoreError::NodeUnavailable)
}

pub(crate) async fn load_node_from_pool(
    pool: &PgPool,
    tenant_id: TenantId,
    node_id: ResourceNodeId,
) -> Result<ResourceNode, ResourceStoreError> {
    sqlx::query(
        "SELECT node_id, tenant_id, scope_kind, scope_id, parent_node_id,
                original_parent_node_id, name, comparison_name, kind, lifecycle,
                file_artifact_id, sort_key, actor_principal_id, actor_principal_kind,
                source_conversation_id, source_invocation_id, source_tool_key,
                created_at_ms, updated_at_ms, revision, is_result
           FROM resource.nodes
          WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(node_id.into_uuid())
    .fetch_optional(pool)
    .await
    .map_err(dependency)?
    .map(decode_node)
    .transpose()?
    .ok_or(ResourceStoreError::NodeUnavailable)
}

fn receipt(
    scope: ResourceScope,
    node: ResourceNode,
    revisions: Revisions,
    duplicate: bool,
) -> ResourceMutationReceipt {
    ResourceMutationReceipt {
        scope,
        node,
        previous_revision: revisions.previous,
        revision: revisions.next,
        duplicate,
    }
}

pub(crate) fn encode_scope(scope: ResourceScope) -> (i16, uuid::Uuid) {
    match scope {
        ResourceScope::ExecutionWorkspace(id) => (EXECUTION_WORKSPACE_SCOPE, id.into_uuid()),
        ResourceScope::Project(id) => (PROJECT_SCOPE, id.into_uuid()),
        ResourceScope::Conversation(id) => (CONVERSATION_SCOPE, id.into_uuid()),
        ResourceScope::OwnedAgent(id) => (OWNED_AGENT_SCOPE, id.into_uuid()),
    }
}

pub(crate) fn decode_scope(kind: i16, id: uuid::Uuid) -> Result<ResourceScope, ResourceStoreError> {
    match kind {
        EXECUTION_WORKSPACE_SCOPE => musteragent_identity::ExecutionWorkspaceId::from_uuid(id)
            .map(ResourceScope::ExecutionWorkspace)
            .map_err(|_| ResourceStoreError::Corrupt),
        PROJECT_SCOPE => WorkspaceId::from_uuid(id)
            .map(ResourceScope::Project)
            .map_err(|_| ResourceStoreError::Corrupt),
        OWNED_AGENT_SCOPE => musteragent_identity::AgentId::from_uuid(id)
            .map(ResourceScope::OwnedAgent)
            .map_err(|_| ResourceStoreError::Corrupt),
        CONVERSATION_SCOPE => ConversationId::from_uuid(id)
            .map(ResourceScope::Conversation)
            .map_err(|_| ResourceStoreError::Corrupt),
        _ => Err(ResourceStoreError::Corrupt),
    }
}

const fn encode_node_kind(kind: ResourceNodeKind) -> i16 {
    match kind {
        ResourceNodeKind::Folder => FOLDER_NODE,
        ResourceNodeKind::File => FILE_NODE,
    }
}

const fn decode_node_kind(kind: i16) -> Result<ResourceNodeKind, ResourceStoreError> {
    match kind {
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

const fn encode_principal_kind(kind: PrincipalKind) -> i16 {
    match kind {
        PrincipalKind::User => 1,
        PrincipalKind::Service => 2,
        PrincipalKind::System => 3,
    }
}

const fn decode_principal_kind(value: i16) -> Result<PrincipalKind, ResourceStoreError> {
    match value {
        1 => Ok(PrincipalKind::User),
        2 => Ok(PrincipalKind::Service),
        3 => Ok(PrincipalKind::System),
        _ => Err(ResourceStoreError::Corrupt),
    }
}

fn optional_node_id(
    value: Option<uuid::Uuid>,
) -> Result<Option<ResourceNodeId>, ResourceStoreError> {
    value
        .map(ResourceNodeId::from_uuid)
        .transpose()
        .map_err(|_| ResourceStoreError::Corrupt)
}

pub(crate) fn decode_revision(value: i64) -> Result<ResourceRevision, ResourceStoreError> {
    u64::try_from(value)
        .map(ResourceRevision::new)
        .map_err(|_| ResourceStoreError::Corrupt)
}

pub(crate) fn revision_i64(value: ResourceRevision) -> Result<i64, ResourceStoreError> {
    i64::try_from(value.get()).map_err(|_| ResourceStoreError::Corrupt)
}

fn map_write_error(error: sqlx::Error) -> ResourceStoreError {
    if let sqlx::Error::Database(database) = &error
        && database.code().as_deref() == Some("23505")
    {
        return match database.constraint() {
            Some("resource_nodes_active_sibling_name_idx") => ResourceStoreError::NameConflict,
            Some("nodes_file_artifact_id_key") => ResourceStoreError::FileUnavailable,
            _ => ResourceStoreError::RevisionConflict,
        };
    }
    dependency(error)
}

fn dependency(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::DependencyUnavailable
}

fn corrupt(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::Corrupt
}

fn device_name_author(
    context: &musteragent_runtime_resource::ResourceCommandContext,
) -> Option<uuid::Uuid> {
    matches!(
        context.source,
        musteragent_runtime_resource::ResourceCommandSource::DeviceExecution { .. }
    )
    .then(|| context.invocation_id.map(|id| id.into_uuid()))
    .flatten()
}
