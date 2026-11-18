use musteragent_content_ref::Sha256;
use musteragent_identity::{ContextBranchId, InvocationId, ResourceNodeId, ResourceRevision};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ExecutionWorkspacePreparationError as Error, ExecutionWorkspacePreparationStore,
    ExecutionWorkspaceSnapshot, ExecutionWorkspaceSnapshotEntry, PrepareExecutionWorkspace,
    ReadExecutionWorkspaceTree, ResourceCommandContext, ResourceCommandSource, ResourceStoreError,
};
use sqlx::{PgConnection, Row};

impl ExecutionWorkspacePreparationStore for crate::PostgresResourceStore {
    fn prepare_execution(
        &self,
        request: PrepareExecutionWorkspace,
    ) -> ApplicationFuture<'_, Result<ExecutionWorkspaceSnapshot, Error>> {
        Box::pin(prepare(self, request))
    }
}

async fn prepare(
    store: &crate::PostgresResourceStore,
    mut request: PrepareExecutionWorkspace,
) -> Result<ExecutionWorkspaceSnapshot, Error> {
    request.context.validate().map_err(|_| Error::Unavailable)?;
    let invocation = request.context.invocation_id.ok_or(Error::Unavailable)?;
    // Original commit lane supplies ordering, server-clock lease checks and
    // the Attempt fence. Never create a separate workspace timeout/lease.
    let (mut tx, state) = crate::commit_lane::acquire(&store.database, &request.attempt)
        .await
        .map_err(|error| match error {
            crate::commit_lane::CommitLaneError::StaleFence => Error::Unavailable,
            _ => Error::DependencyUnavailable,
        })?;
    if state.tenant_id != request.context.tenant_id
        || Some(state.conversation_id) != request.context.conversation_id()
    {
        return Err(Error::Unavailable);
    }
    // Admission's immutable Ledger record owns the Run's branch. Never infer
    // it from the currently selected UI branch, including first preparation.
    let row = sqlx::query("SELECT admitted.context_branch_id, i.status AS invocation_status FROM runtime.runs r
         JOIN event_store.ledger_records admitted ON admitted.tenant_id=r.tenant_id
           AND admitted.conversation_id=r.conversation_id AND admitted.conversation_seq=r.admitted_sequence
         JOIN tool.tool_invocations i ON i.tenant_id=r.tenant_id AND i.run_id=r.run_id
         WHERE r.run_id=$1 AND r.status=2 AND r.cancel_requested_at_ms IS NULL
           AND i.invocation_id=$2 AND ((i.status=3 AND i.execution_attempt_id=$3) OR (i.status=6 AND i.retry_contract=3))")
        .bind(request.attempt.run_id.into_uuid()).bind(invocation.into_uuid())
        .bind(request.attempt.attempt_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(dependency)?
        .ok_or(Error::Unavailable)?;
    let recovering = row
        .try_get::<i16, _>("invocation_status")
        .map_err(corrupt)?
        == 6;
    let branch = row
        .try_get::<Option<uuid::Uuid>, _>("context_branch_id")
        .map_err(corrupt)?
        .map(ContextBranchId::from_uuid)
        .transpose()
        .map_err(corrupt)?;
    let binding = crate::execution_workspace::ensure_on(
        &mut tx,
        musteragent_runtime_resource::EnsureExecutionWorkspace {
            actor: request.context.actor,
            conversation_id: state.conversation_id,
            expected_branch_id: branch,
            expected_home_epoch: state.home_epoch,
            now: request.context.now,
        },
    )
    .await
    .map_err(|error| match error {
        musteragent_runtime_resource::ExecutionWorkspaceError::DependencyUnavailable => {
            Error::DependencyUnavailable
        }
        musteragent_runtime_resource::ExecutionWorkspaceError::Corrupt => Error::Corrupt,
        _ => Error::Unavailable,
    })?
    .binding;
    match request.context.source {
        ResourceCommandSource::ExecutionWorkspace(expected) if expected == binding => {}
        ResourceCommandSource::Conversation { workspace_id, .. }
            if workspace_id == binding.project_id => {}
        _ => return Err(Error::Unavailable),
    }
    request.context.source = ResourceCommandSource::ExecutionWorkspace(binding);
    let tree_request = ReadExecutionWorkspaceTree {
        context: request.context.clone(),
        maximum_entries: request.maximum_entries,
        maximum_total_bytes: request.maximum_total_bytes,
    };
    tree_request.binding().map_err(resource_error)?;
    if !writer_available(&mut tx, &request.context)
        .await
        .map_err(dependency)?
    {
        return Err(Error::WriterBusy);
    }
    if let Some(snapshot) = read_snapshot(&mut tx, &request).await? {
        if snapshot.request_fingerprint != request.request_fingerprint {
            return Err(Error::RequestConflict);
        }
        if snapshot.entries.len() + snapshot.project.as_ref().map_or(0, |p| p.entries.len())
            > request.maximum_entries
            || snapshot.total_bytes > request.maximum_total_bytes
        {
            return Err(Error::BoundsExceeded);
        }
        claim_writer(
            &mut tx,
            binding.execution_workspace_id.into_uuid(),
            invocation,
        )
        .await?;
        tx.commit().await.map_err(dependency)?;
        return Ok(snapshot);
    }
    // An indeterminate invocation may recover only its frozen baseline.
    // Absence is not permission to prepare and execute the program again.
    if recovering {
        return Err(Error::Unavailable);
    }
    // All Resource mutations acquire this head after the original authority
    // lock. A snapshot cannot mix directory metadata and different file bases.
    sqlx::query("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=3 AND scope_id=$2 FOR UPDATE")
        .bind(request.context.tenant_id.into_uuid()).bind(binding.execution_workspace_id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(dependency)?;
    let tree = crate::execution_workspace_tree::read_tree_on(&mut tx, &tree_request)
        .await
        .map_err(resource_error)?;
    let environment = match request.environment {
        musteragent_runtime_resource::ExecutionWorkspaceEnvironmentChange::Select(id) => {
            Some(id.into_uuid())
        }
        _ => None,
    };
    let keep_environment = request.environment
        == musteragent_runtime_resource::ExecutionWorkspaceEnvironmentChange::Keep;
    if let Some(reference) = &request.selection_ref {
        let selected = read_selection(
            &mut tx,
            request.context.tenant_id,
            Some(reference.content_id.into_uuid()),
        )
        .await?;
        if selected.as_ref() != Some(reference) {
            return Err(Error::Unavailable);
        }
    }
    let settings = sqlx::query("INSERT INTO resource.execution_workspace_settings(execution_workspace_id,cwd,environment_ref,selection_content_id)
        VALUES($1,COALESCE($2,'work'),$3,$5) ON CONFLICT(execution_workspace_id) DO UPDATE
        SET cwd=COALESCE($2,execution_workspace_settings.cwd),
          environment_ref=CASE WHEN $4 THEN execution_workspace_settings.environment_ref ELSE $3 END,
          selection_content_id=COALESCE($5,execution_workspace_settings.selection_content_id)
        RETURNING cwd,environment_ref,selection_content_id")
        .bind(binding.execution_workspace_id.into_uuid()).bind(request.cwd.as_deref()).bind(environment).bind(keep_environment).bind(request.selection_ref.as_ref().map(|r|r.content_id.into_uuid()))
        .fetch_one(&mut *tx).await.map_err(dependency)?;
    let cwd: String = settings.try_get("cwd").map_err(corrupt)?;
    let environment: Option<uuid::Uuid> = settings.try_get("environment_ref").map_err(corrupt)?;
    let selection_content_id: Option<uuid::Uuid> =
        settings.try_get("selection_content_id").map_err(corrupt)?;
    let project = crate::execution_workspace_project::prepare(
        &mut tx,
        &request,
        tree.entries.len(),
        tree.total_bytes,
    )
    .await?;
    let project_entries = project
        .as_ref()
        .map(|p| p.entries.as_slice())
        .unwrap_or_default();
    let total_bytes = tree.total_bytes
        + project_entries
            .iter()
            .filter_map(|e| e.file.as_ref())
            .map(|f| f.content_ref.byte_length)
            .sum::<u64>();
    sqlx::query("INSERT INTO resource.execution_workspace_preparations
        (invocation_id,tenant_id,execution_workspace_id,request_hash,base_revision,total_bytes,entry_count,created_at_ms,cwd,environment_ref,selection_content_id,project_root_node_id,project_revision,project_writable)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
        .bind(invocation.into_uuid()).bind(request.context.tenant_id.into_uuid())
        .bind(binding.execution_workspace_id.into_uuid()).bind(request.request_fingerprint.into_bytes().to_vec())
        .bind(i64::try_from(tree.revision.get()).map_err(corrupt)?)
        .bind(i64::try_from(total_bytes).map_err(corrupt)?)
        .bind((tree.entries.len()+project_entries.len()) as i32).bind(request.context.now.unix_millis())
        .bind(cwd).bind(environment).bind(selection_content_id)
        .bind(project.as_ref().and_then(|p|p.root_node_id).map(|id|id.into_uuid()))
        .bind(project.as_ref().map(|p|i64::try_from(p.revision.get())).transpose().map_err(corrupt)?)
        .bind(project.as_ref().is_some_and(|p|p.writable))
        .execute(&mut *tx).await.map_err(dependency)?;
    let private_entries = tree
        .entries
        .into_iter()
        .map(|e| ExecutionWorkspaceSnapshotEntry {
            node_id: e.resource.node.node_id,
            path: e.path,
            file: e.resource.file,
        })
        .collect::<Vec<_>>();
    for (kind, entries) in [
        (0_i16, private_entries.as_slice()),
        (1_i16, project_entries),
    ] {
        for entry in entries {
            let file = entry.file.as_ref();
            sqlx::query("INSERT INTO resource.execution_workspace_snapshot_entries
                (invocation_id,node_id,path,artifact_id,artifact_version_id,version,content_id,mount_kind)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                .bind(invocation.into_uuid()).bind(entry.node_id.into_uuid()).bind(&entry.path)
                .bind(file.map(|f|f.artifact_id.into_uuid())).bind(file.map(|f|f.artifact_version_id.into_uuid()))
                .bind(file.map(|f|i64::try_from(f.version.get())).transpose().map_err(corrupt)?)
                .bind(file.map(|f|f.content_ref.content_id.into_uuid())).bind(kind)
                .execute(&mut *tx).await.map_err(dependency)?;
        }
    }
    claim_writer(
        &mut tx,
        binding.execution_workspace_id.into_uuid(),
        invocation,
    )
    .await?;
    let snapshot = read_snapshot(&mut tx, &request)
        .await?
        .ok_or(Error::Corrupt)?;
    musteragent_runtime_resource::WorkspaceManifest::from_snapshot(&snapshot, snapshot.cwd.clone())
        .map_err(|_| Error::Unavailable)?;
    tx.commit().await.map_err(dependency)?;
    Ok(snapshot)
}

async fn claim_writer(
    connection: &mut PgConnection,
    workspace: uuid::Uuid,
    invocation: InvocationId,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO resource.execution_workspace_writers(execution_workspace_id,invocation_id)
        VALUES($1,$2) ON CONFLICT(execution_workspace_id) DO UPDATE SET invocation_id=EXCLUDED.invocation_id")
        .bind(workspace).bind(invocation.into_uuid()).execute(connection).await.map_err(dependency)?;
    Ok(())
}

/// The conversation stream is already locked by the caller. Qualification
/// derives from original Tool/Run terminal state, never a local wall-clock TTL.
/// Failed and cancelled candidates remain pinned independently of writer reuse.
pub(crate) async fn writer_available(
    connection: &mut PgConnection,
    context: &ResourceCommandContext,
) -> Result<bool, sqlx::Error> {
    let ResourceCommandSource::ExecutionWorkspace(binding) = context.source else {
        return Ok(true);
    };
    sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM resource.execution_workspace_writers w
        JOIN tool.tool_invocations i ON i.invocation_id=w.invocation_id
        JOIN runtime.runs r ON r.run_id=i.run_id
        WHERE w.execution_workspace_id=$1 AND w.invocation_id IS DISTINCT FROM $2
          AND i.status IN (1,2,3,6) AND r.status IN (1,2,3,4))",
    )
    .bind(binding.execution_workspace_id.into_uuid())
    .bind(context.invocation_id.map(InvocationId::into_uuid))
    .fetch_one(connection)
    .await
}

async fn read_snapshot(
    connection: &mut PgConnection,
    request: &PrepareExecutionWorkspace,
) -> Result<Option<ExecutionWorkspaceSnapshot>, Error> {
    let ResourceCommandSource::ExecutionWorkspace(binding) = request.context.source else {
        return Err(Error::Unavailable);
    };
    let invocation_id = request.context.invocation_id.ok_or(Error::Unavailable)?;
    let Some(row) = sqlx::query("SELECT request_hash,base_revision,total_bytes,entry_count,cwd,environment_ref,selection_content_id,project_root_node_id,project_revision,project_writable
        FROM resource.execution_workspace_preparations WHERE invocation_id=$1 AND tenant_id=$2 AND execution_workspace_id=$3")
        .bind(invocation_id.into_uuid()).bind(request.context.tenant_id.into_uuid()).bind(binding.execution_workspace_id.into_uuid())
        .fetch_optional(&mut *connection).await.map_err(dependency)? else { return Ok(None); };
    let request_fingerprint =
        Sha256::from_bytes(row.try_get::<Vec<u8>, _>("request_hash").map_err(corrupt)?)
            .map_err(corrupt)?;
    let revision = ResourceRevision::new(
        u64::try_from(row.try_get::<i64, _>("base_revision").map_err(corrupt)?).map_err(corrupt)?,
    );
    let total_bytes =
        u64::try_from(row.try_get::<i64, _>("total_bytes").map_err(corrupt)?).map_err(corrupt)?;
    let count: i32 = row.try_get("entry_count").map_err(corrupt)?;
    let cwd = row.try_get("cwd").map_err(corrupt)?;
    let environment_ref = row
        .try_get::<Option<uuid::Uuid>, _>("environment_ref")
        .map_err(corrupt)?
        .map(musteragent_identity::BrowserJobId::from_uuid)
        .transpose()
        .map_err(corrupt)?;
    let selection_ref = read_selection(
        connection,
        request.context.tenant_id,
        row.try_get("selection_content_id").map_err(corrupt)?,
    )
    .await?;
    let project_revision = row
        .try_get::<Option<i64>, _>("project_revision")
        .map_err(corrupt)?;
    let mut project = project_revision
        .map(|revision| {
            Ok::<_, Error>(
                musteragent_runtime_resource::ExecutionWorkspaceProjectSnapshot {
                    root_node_id: row
                        .try_get::<Option<uuid::Uuid>, _>("project_root_node_id")
                        .map_err(corrupt)?
                        .map(ResourceNodeId::from_uuid)
                        .transpose()
                        .map_err(corrupt)?,
                    writable: row.try_get("project_writable").map_err(corrupt)?,
                    revision: crate::resource::decode_revision(revision).map_err(corrupt)?,
                    entries: Vec::new(),
                },
            )
        })
        .transpose()?;
    let rows = sqlx::query("SELECT e.node_id,e.path,e.mount_kind,e.artifact_id AS file_artifact_id,e.artifact_version_id,
        e.version::text AS version,regexp_replace(e.path,'^.*/','') AS name,
        c.content_id,c.content_hash,c.media_type,c.byte_length::text AS byte_length
        FROM resource.execution_workspace_snapshot_entries e
        LEFT JOIN artifact.content_refs c ON c.content_id=e.content_id AND c.tenant_id=$2
        WHERE e.invocation_id=$1 ORDER BY e.path")
        .bind(invocation_id.into_uuid()).bind(request.context.tenant_id.into_uuid())
        .fetch_all(connection).await.map_err(dependency)?;
    if rows.len() != usize::try_from(count).map_err(corrupt)? {
        return Err(Error::Corrupt);
    }
    let mut entries = Vec::new();
    for row in rows {
        let node_id =
            ResourceNodeId::from_uuid(row.try_get("node_id").map_err(corrupt)?).map_err(corrupt)?;
        let file = if row
            .try_get::<Option<uuid::Uuid>, _>("file_artifact_id")
            .map_err(corrupt)?
            .is_some()
        {
            Some(
                crate::resource_read::decode_project_file_version(&row, node_id)
                    .map_err(resource_error)?,
            )
        } else {
            None
        };
        let entry = ExecutionWorkspaceSnapshotEntry {
            node_id,
            path: row.try_get("path").map_err(corrupt)?,
            file,
        };
        match row.try_get::<i16, _>("mount_kind").map_err(corrupt)? {
            0 => entries.push(entry),
            1 => project.as_mut().ok_or(Error::Corrupt)?.entries.push(entry),
            _ => return Err(Error::Corrupt),
        }
    }
    if entries
        .iter()
        .chain(project.iter().flat_map(|p| p.entries.iter()))
        .filter_map(|e| e.file.as_ref())
        .try_fold(0_u64, |sum, f| sum.checked_add(f.content_ref.byte_length))
        != Some(total_bytes)
    {
        return Err(Error::Corrupt);
    }
    Ok(Some(ExecutionWorkspaceSnapshot {
        binding,
        invocation_id,
        request_fingerprint,
        revision,
        total_bytes,
        entries,
        cwd,
        environment_ref,
        selection_ref,
        project,
    }))
}

fn dependency<T>(_: T) -> Error {
    Error::DependencyUnavailable
}
fn corrupt<T>(_: T) -> Error {
    Error::Corrupt
}
fn resource_error(error: ResourceStoreError) -> Error {
    match error {
        ResourceStoreError::BoundsExceeded => Error::BoundsExceeded,
        ResourceStoreError::DependencyUnavailable => Error::DependencyUnavailable,
        ResourceStoreError::Corrupt => Error::Corrupt,
        _ => Error::Unavailable,
    }
}

async fn read_selection(
    connection: &mut PgConnection,
    tenant: musteragent_identity::TenantId,
    id: Option<uuid::Uuid>,
) -> Result<Option<musteragent_content_ref::ContentRef>, Error> {
    let Some(id) = id else { return Ok(None) };
    let row=sqlx::query("SELECT media_type,byte_length::text AS byte_length,content_hash FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2")
        .bind(tenant.into_uuid()).bind(id).fetch_optional(connection).await.map_err(dependency)?.ok_or(Error::Unavailable)?;
    crate::content::decode_content_ref(
        musteragent_identity::ContentId::from_uuid(id).map_err(corrupt)?,
        &row,
    )
    .map(Some)
    .map_err(corrupt)
}
