//! Message retraction enters the original Resource mutation kernel, in the same
//! transaction as the Ledger fact. It never deletes immutable Artifact bytes.
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    ArtifactId, IdempotencyKey, ResourceChangeSetId, ResourceNodeId, ResourceRevision, WorkspaceId,
};
use musteragent_runtime_resource::{
    PreparedResourceMutation, ResourceCommandContext, ResourceCommandSource,
    ResourceMutationIdentity, ResourceStoreError, TrashResourceNode,
};
use musteragent_runtime_session::{
    RetractMessageCommit, file_retraction::ManagedResourceRetractionPlan,
};
use sqlx::{PgConnection, Row};

fn dependency(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::DependencyUnavailable
}
fn corrupt(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::Corrupt
}

pub(crate) async fn retract_resource_files_on(
    connection: &mut PgConnection,
    commit: &RetractMessageCommit,
) -> Result<(), ResourceStoreError> {
    let Some(plan) = &commit.managed_resources else {
        return Ok(());
    };
    if !plan.validate() {
        return Err(ResourceStoreError::Corrupt);
    }
    if plan.invocation_ids.is_empty() {
        return Ok(());
    }
    let tenant = commit.actor.tenant_id.into_uuid();
    let conversation = commit.append.conversation_id.into_uuid();
    // Conversation ownership is already locked by retract_inner. Resolve its
    // present placement, never a client path or remembered workspace identifier.
    let workspace: Option<uuid::Uuid> = sqlx::query_scalar("SELECT workspace_id FROM conversation.conversations WHERE tenant_id=$1 AND conversation_id=$2")
        .bind(tenant).bind(conversation).fetch_one(&mut *connection).await.map_err(dependency)?;
    let context = ResourceCommandContext {
        tenant_id: commit.actor.tenant_id,
        actor: commit.actor,
        source: ResourceCommandSource::Conversation {
            conversation_id: commit.append.conversation_id,
            workspace_id: workspace
                .map(WorkspaceId::from_uuid)
                .transpose()
                .map_err(|_| ResourceStoreError::Corrupt)?,
        },
        invocation_id: None,
        tool_key: None,
        now: commit.retracted_at,
    };
    let invocations = plan
        .invocation_ids
        .iter()
        .map(|id| id.into_uuid())
        .collect::<Vec<_>>();
    let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM tool.tool_invocations WHERE tenant_id=$1 AND conversation_id=$2 AND invocation_id=ANY($3)")
        .bind(tenant).bind(conversation).bind(&invocations).fetch_one(&mut *connection).await.map_err(dependency)?;
    if usize::try_from(actual).ok() != Some(plan.invocation_ids.len()) {
        return Err(ResourceStoreError::Corrupt);
    }
    let (scope_kind, scope_id) = crate::resource::encode_scope(context.scope());
    // Same scope lock as every Resource command. No Resource node can be edited,
    // renamed, moved or deleted between validation and the original Trash call.
    let revision: Option<i64> = sqlx::query_scalar("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 FOR UPDATE")
        .bind(tenant).bind(scope_kind).bind(scope_id).fetch_optional(&mut *connection).await.map_err(dependency)?;
    let mut revision = revision.unwrap_or(0);
    let nodes = sqlx::query("SELECT node_id, file_artifact_id, source_invocation_id, scope_kind, scope_id, lifecycle, revision FROM resource.nodes WHERE tenant_id=$1 AND source_conversation_id=$2 AND source_invocation_id=ANY($3) AND kind=2 ORDER BY node_id LIMIT $4 FOR UPDATE")
        .bind(tenant).bind(conversation).bind(&invocations).bind((ManagedResourceRetractionPlan::MAX_ITEMS + 1) as i64)
        .fetch_all(&mut *connection).await.map_err(dependency)?;
    if nodes.len() > ManagedResourceRetractionPlan::MAX_ITEMS {
        return Err(ResourceStoreError::Corrupt);
    }
    let record = commit
        .append
        .records
        .first()
        .ok_or(ResourceStoreError::Corrupt)?
        .record_id;
    for row in nodes {
        let node = ResourceNodeId::from_uuid(row.try_get("node_id").map_err(corrupt)?)
            .map_err(|_| ResourceStoreError::Corrupt)?;
        let artifact = ArtifactId::from_uuid(row.try_get("file_artifact_id").map_err(corrupt)?)
            .map_err(|_| ResourceStoreError::Corrupt)?;
        let invocation: uuid::Uuid = row.try_get("source_invocation_id").map_err(corrupt)?;
        let mut disposition = 3_i16;
        let reason;
        let mut change_set = None;
        if row.try_get::<i16, _>("scope_kind").map_err(corrupt)? != scope_kind
            || row.try_get::<uuid::Uuid, _>("scope_id").map_err(corrupt)? != scope_id
        {
            reason = "scope_changed";
        } else if plan.retained_artifact_ids.contains(&artifact) {
            reason = "effective_reference";
            disposition = 2;
        } else if row.try_get::<i16, _>("lifecycle").map_err(corrupt)? != 1 {
            reason = "already_removed";
            disposition = 2;
        } else {
            let creation_revision: Option<i64> = sqlx::query_scalar("SELECT min(change.committed_revision) FROM resource.change_sets AS change WHERE change.tenant_id=$1 AND change.scope_kind=$2 AND change.scope_id=$3 AND change.source_invocation_id=$4 AND change.status=2 AND ((change.operation=2 AND change.result_node_id=$5) OR (change.operation=7 AND EXISTS(SELECT 1 FROM resource.change_set_mutations AS mutation WHERE mutation.change_set_id=change.change_set_id AND mutation.result_node_id=$5 AND mutation.mutation_kind=2)))")
                .bind(tenant).bind(scope_kind).bind(scope_id).bind(invocation).bind(node.into_uuid()).fetch_one(&mut *connection).await.map_err(dependency)?;
            if creation_revision.is_none()
                || creation_revision != Some(row.try_get::<i64, _>("revision").map_err(corrupt)?)
            {
                reason = "node_changed";
            } else {
                let original = sqlx::query("SELECT artifact.current_version::text AS version, artifact.lifecycle, first.source_invocation_id, artifact.created_by_run_id=invocation.run_id AS same_run FROM artifact.artifacts AS artifact JOIN artifact.artifact_versions AS first ON first.artifact_id=artifact.artifact_id AND first.version=1 JOIN tool.tool_invocations AS invocation ON invocation.invocation_id=$3 AND invocation.tenant_id=$1 AND invocation.conversation_id=$4 WHERE artifact.tenant_id=$1 AND artifact.artifact_id=$2 AND artifact.conversation_id=$4 FOR UPDATE OF artifact")
                    .bind(tenant).bind(artifact.into_uuid()).bind(invocation).bind(conversation).fetch_optional(&mut *connection).await.map_err(dependency)?;
                let valid_source = original.as_ref().is_some_and(|row| {
                    row.try_get::<Option<uuid::Uuid>, _>("source_invocation_id")
                        .ok()
                        .flatten()
                        == Some(invocation)
                        && row.try_get::<Option<bool>, _>("same_run").ok().flatten() == Some(true)
                });
                let valid_version = original.as_ref().is_some_and(|row| {
                    row.try_get::<String, _>("version").is_ok_and(|v| v == "1")
                        && row.try_get::<i16, _>("lifecycle").is_ok_and(|v| v != 3)
                });
                let editing: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource.edit_sessions WHERE tenant_id=$1 AND file_node_id=$2 AND state IN (1,2))")
                    .bind(tenant).bind(node.into_uuid()).fetch_one(&mut *connection).await.map_err(dependency)?;
                if !valid_source {
                    reason = "source_changed";
                } else if !valid_version {
                    reason = "version_changed";
                } else if editing {
                    reason = "edit_in_progress";
                } else {
                    let key = format!("resource-file-retraction:{record}:{node}");
                    let id = ResourceChangeSetId::from_uuid(uuid::Uuid::new_v5(
                        &uuid::Uuid::NAMESPACE_OID,
                        key.as_bytes(),
                    ))
                    .map_err(|_| ResourceStoreError::Corrupt)?;
                    let result = crate::resource::apply_prepared_resource_mutation(
                        connection,
                        &PreparedResourceMutation::Trash(TrashResourceNode {
                            context: context.clone(),
                            identity: ResourceMutationIdentity {
                                change_set_id: id,
                                idempotency_key: IdempotencyKey::new(key.clone())
                                    .map_err(|_| ResourceStoreError::Corrupt)?,
                                command_fingerprint: Sha256::digest(key.as_bytes()),
                            },
                            expected_revision: ResourceRevision::new(
                                u64::try_from(revision).map_err(|_| ResourceStoreError::Corrupt)?,
                            ),
                            node_id: node,
                        }),
                    )
                    .await?;
                    revision = crate::resource::revision_i64(result.revision)?;
                    change_set = Some(id.into_uuid());
                    disposition = 1;
                    reason = "creation_retracted";
                }
            }
        }
        sqlx::query("INSERT INTO resource.file_retraction_outcomes(retraction_record_id,node_id,tenant_id,conversation_id,source_invocation_id,disposition,reason,change_set_id,recorded_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(record.into_uuid()).bind(node.into_uuid()).bind(tenant).bind(conversation).bind(invocation)
            .bind(disposition).bind(reason).bind(change_set).bind(commit.retracted_at.unix_millis()).execute(&mut *connection).await.map_err(dependency)?;
    }
    Ok(())
}

impl musteragent_runtime_file_query::FileRetractionStatusSource for crate::PostgresResourceStore {
    fn retraction_status<'a>(
        &'a self,
        context: &'a musteragent_runtime_file_query::FileQueryContext,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<
            Option<musteragent_runtime_file_query::FileRetractionStatus>,
            musteragent_runtime_file_query::FileQueryError,
        >,
    > {
        use musteragent_runtime_file_query::{FileQueryError, FileRetractionStatus};
        Box::pin(async move {
            if context.content_allowlist.is_some() {
                return Ok(None);
            }
            let conversation = context.require_conversation()?;
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| FileQueryError::Unavailable)?;
            let allowed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM conversation.conversations AS conversation JOIN conversation.participants AS viewer ON viewer.tenant_id=conversation.tenant_id AND viewer.conversation_id=conversation.conversation_id AND viewer.kind=1 AND viewer.principal_ref=$3 AND viewer.left_at_ms IS NULL WHERE conversation.tenant_id=$1 AND conversation.conversation_id=$2 AND conversation.workspace_id IS NOT DISTINCT FROM $4 AND conversation.lifecycle IN(1,2) AND conversation.control_home_kind=1)")
                .bind(context.actor.tenant_id.into_uuid()).bind(conversation.into_uuid()).bind(context.actor.principal_id.into_uuid())
                .bind(context.project_workspace_id.map(|id| id.into_uuid())).fetch_one(&mut *tx).await.map_err(|_| FileQueryError::Unavailable)?;
            if !allowed {
                return Err(FileQueryError::Denied);
            }
            let row = sqlx::query("SELECT count(*) FILTER (WHERE disposition=1) AS applied, count(*) FILTER (WHERE disposition=2) AS retained, count(*) FILTER (WHERE disposition=3) AS conflict FROM resource.file_retraction_outcomes WHERE tenant_id=$1 AND conversation_id=$2")
                .bind(context.actor.tenant_id.into_uuid()).bind(conversation.into_uuid()).fetch_one(&mut *tx).await.map_err(|_| FileQueryError::Unavailable)?;
            let count = |key| {
                row.try_get::<i64, _>(key)
                    .map_err(|_| FileQueryError::Integrity)
                    .and_then(|v| u64::try_from(v).map_err(|_| FileQueryError::Integrity))
            };
            let status = FileRetractionStatus {
                pending: 0,
                applied: count("applied")?,
                retained: count("retained")?,
                conflict: count("conflict")?,
                indeterminate: 0,
            };
            tx.commit().await.map_err(|_| FileQueryError::Unavailable)?;
            Ok(Some(status))
        })
    }
}
