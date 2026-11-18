use super::*;
use musteragent_identity::WorkspaceId;
use musteragent_projection::{
    ListWorkspaceCatalogActivity, WorkspaceCatalogActivity, WorkspaceCatalogActivityBatch,
    WorkspaceCatalogActivitySource,
};

impl PostgresConversationCatalogStore {
    async fn read_workspace_activity(
        &self,
        query: ListWorkspaceCatalogActivity,
    ) -> Result<WorkspaceCatalogActivityBatch, PostgresConversationCatalogError> {
        query
            .validate()
            .map_err(|_| PostgresConversationCatalogError::InvalidQuery)?;
        let mut transaction = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(PostgresConversationCatalogError::Transaction)?;
        let snapshot: String = sqlx::query_scalar(
            "SELECT COALESCE((SELECT current_position FROM projection.conversation_catalog_clocks WHERE tenant_id=$1),0)::text"
        ).bind(query.actor.tenant_id.into_uuid()).fetch_one(transaction.as_mut()).await?;
        let ids: Vec<_> = query
            .workspace_ids
            .iter()
            .map(|id| id.into_uuid())
            .collect();
        let excluded: Vec<_> = query
            .excluded_conversation_ids
            .iter()
            .map(|id| id.into_uuid())
            .collect();
        let rows = sqlx::query(
            "WITH requested AS (SELECT workspace_id, ordinal FROM unnest($3::uuid[]) WITH ORDINALITY AS ids(workspace_id,ordinal)),
             visible AS MATERIALIZED (
               SELECT c.conversation_id,c.workspace_id,
                 (p.conversation_id IS NULL OR p.home_epoch<>c.home_epoch OR p.lifecycle<>c.lifecycle OR EXISTS(
                    SELECT 1 FROM projection.conversation_catalog_bootstrap_receipts b
                    WHERE b.tenant_id=c.tenant_id AND b.conversation_id=c.conversation_id)) AS pending
               FROM conversation.conversations c
               LEFT JOIN projection.conversation_catalog p ON p.tenant_id=c.tenant_id AND p.conversation_id=c.conversation_id
               WHERE c.tenant_id=$1 AND c.lifecycle=1 AND c.workspace_id=ANY($3::uuid[])
               AND NOT (c.conversation_id=ANY($4::uuid[]))
               AND EXISTS(SELECT 1 FROM conversation.participants v WHERE v.tenant_id=c.tenant_id
                 AND v.conversation_id=c.conversation_id AND v.kind=1 AND v.principal_ref=$2 AND v.left_at_ms IS NULL)
             ), totals AS (
               SELECT v.workspace_id, count(*) FILTER(WHERE v.pending) AS pending,
                 sum(CASE WHEN v.pending THEN 0 ELSE executions.total END)::bigint AS executions,
                 sum(CASE WHEN v.pending THEN 0 ELSE actions.total END)::bigint AS actions,
                 sum(CASE WHEN v.pending THEN 0 ELSE unread.total END)::bigint AS unread
               FROM visible v
               LEFT JOIN projection.conversation_read_receipts r ON r.tenant_id=$1
                 AND r.conversation_id=v.conversation_id AND r.principal_id=$2
               LEFT JOIN LATERAL (SELECT count(*) AS total FROM projection.conversation_catalog_executions e
                 WHERE NOT v.pending AND e.tenant_id=$1 AND e.conversation_id=v.conversation_id AND e.status IN (1,2,3,4)) executions ON true
               LEFT JOIN LATERAL (SELECT count(*) AS total FROM projection.conversation_catalog_actions a
                 WHERE NOT v.pending AND a.tenant_id=$1 AND a.conversation_id=v.conversation_id) actions ON true
               LEFT JOIN LATERAL (SELECT count(*) AS total FROM projection.conversation_catalog_attention_deliveries d
                 WHERE NOT v.pending AND d.tenant_id=$1 AND d.conversation_id=v.conversation_id
                 AND d.principal_id=$2 AND d.attention_revision>COALESCE(r.seen_through_revision,0)) unread ON true
               GROUP BY v.workspace_id
             ), outcomes AS (
               SELECT v.workspace_id, a.outcome_kind,
                 row_number() OVER (PARTITION BY v.workspace_id ORDER BY a.activity_position DESC, v.conversation_id DESC, a.attention_revision DESC) AS rank
               FROM visible v
               JOIN projection.conversation_catalog_attention_deliveries d ON d.tenant_id=$1 AND d.conversation_id=v.conversation_id AND d.principal_id=$2
               JOIN projection.conversation_catalog_attention a ON a.tenant_id=d.tenant_id AND a.conversation_id=d.conversation_id AND a.attention_revision=d.attention_revision
               LEFT JOIN projection.conversation_read_receipts r ON r.tenant_id=d.tenant_id AND r.conversation_id=d.conversation_id AND r.principal_id=d.principal_id
               WHERE NOT v.pending AND a.outcome_kind IS NOT NULL AND d.attention_revision>COALESCE(r.seen_through_revision,0)
             )
             SELECT q.workspace_id,COALESCE(t.pending,0)::text pending,COALESCE(t.executions,0)::text executions,
               COALESCE(t.actions,0)::text actions,COALESCE(t.unread,0)::text unread,o.outcome_kind
             FROM requested q LEFT JOIN totals t USING(workspace_id) LEFT JOIN outcomes o ON o.workspace_id=q.workspace_id AND o.rank=1 ORDER BY q.ordinal"
        ).bind(query.actor.tenant_id.into_uuid()).bind(query.actor.principal_id.into_uuid())
          .bind(ids).bind(excluded).fetch_all(transaction.as_mut()).await?;
        let workspaces = rows
            .into_iter()
            .map(|row| {
                Ok(WorkspaceCatalogActivity {
                    workspace_id: WorkspaceId::from_uuid(row.try_get("workspace_id")?)
                        .map_err(|_| PostgresConversationCatalogError::CorruptProjection)?,
                    active_executions: parse_u64(row.try_get("executions")?)?,
                    action_required: parse_u64(row.try_get("actions")?)?,
                    unread: parse_u64(row.try_get("unread")?)?,
                    latest_unread_outcome: row
                        .try_get::<Option<i16>, _>("outcome_kind")?
                        .map(|code| {
                            ExecutionOutcomeKind::from_storage_code(code)
                                .ok_or(PostgresConversationCatalogError::CorruptProjection)
                        })
                        .transpose()?,
                    pending_projections: parse_u64(row.try_get("pending")?)?,
                })
            })
            .collect::<Result<Vec<_>, PostgresConversationCatalogError>>()?;
        transaction.commit().await?;
        Ok(WorkspaceCatalogActivityBatch {
            snapshot_position: CatalogActivityPosition::new(parse_u64(snapshot)?),
            workspaces,
        })
    }
}
impl WorkspaceCatalogActivitySource for PostgresConversationCatalogStore {
    fn workspace_activity(
        &self,
        query: ListWorkspaceCatalogActivity,
    ) -> ApplicationFuture<'_, Result<WorkspaceCatalogActivityBatch, ConversationCatalogSourceError>>
    {
        Box::pin(async move {
            self.read_workspace_activity(query)
                .await
                .map_err(map_source_error)
        })
    }
}

#[cfg(test)]
#[path = "workspace_activity_tests.rs"]
mod tests;
