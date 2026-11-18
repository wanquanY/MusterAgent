//! Read-only pending index. It shares the exact visibility and pending predicates
//! with original Interaction control; no second task/approval lifecycle is stored.
use super::*;
use musteragent_identity::AgentId;
use musteragent_runtime_session::{
    DialoguePendingInteraction, DialoguePendingInteractionPage, DialoguePendingInteractionQuery,
};

pub(super) const FROM: &str = "FROM agent.agent_dialogue_entries e
        JOIN agent.dialogue_publications publication ON publication.entry_id=e.entry_id
        JOIN approval.interactions i ON i.tenant_id=e.tenant_id AND i.interaction_id=e.interaction_id
          AND i.run_id=publication.source_run_id
        JOIN agent.dialogue_sessions s ON s.tenant_id=i.tenant_id AND s.conversation_id=i.conversation_id
          AND s.dialogue_id=e.dialogue_id AND s.agent_id=e.author_agent_id AND s.owner_id=e.owner_id
        JOIN agent.owned_agents a ON a.agent_id=s.agent_id AND a.dialogue_id=e.dialogue_id
        JOIN agent.owner_spaces space ON space.space_id=a.space_id AND space.tenant_id=e.tenant_id AND space.owner_id=e.owner_id
        JOIN agent.owned_agent_configurations cfg ON cfg.agent_id=a.agent_id
        JOIN runtime.runs r ON r.tenant_id=i.tenant_id AND r.run_id=i.run_id AND r.conversation_id=i.conversation_id
        JOIN conversation.conversations c ON c.tenant_id=s.tenant_id AND c.conversation_id=s.conversation_id
          AND c.created_by=s.owner_id AND c.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report') AND c.kind=4 AND c.workspace_id IS NULL
        JOIN artifact.content_refs p ON p.content_id=i.prompt_content_id";
pub(super) const VISIBLE: &str = "e.tenant_id=$1 AND e.owner_id=$2
    AND e.kind='interaction' AND a.status='active' AND c.lifecycle=1
    AND space.product='musteragent'";
pub(super) const PENDING: &str = "i.status=1
    AND (i.expires_at_ms IS NULL OR i.expires_at_ms > floor(extract(epoch FROM statement_timestamp()) * 1000)::bigint)
    AND s.permission_revision=cfg.permission_revision
    AND r.status=3 AND r.cancel_requested_at_ms IS NULL";

impl PostgresInteractionApplicationStore {
    pub(super) async fn pending_dialogue(
        &self,
        actor: Actor,
        query: DialoguePendingInteractionQuery,
        correlation: CorrelationId,
    ) -> Result<DialoguePendingInteractionPage, RuntimeError> {
        if actor.principal_kind != PrincipalKind::User
            || !(1..=50).contains(&query.limit)
            || (query.agent_id.is_some() && query.exclude_agent_id.is_some())
        {
            return Err(map_interaction(
                PostgresInteractionError::ActorForbidden,
                correlation,
            ));
        }
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Interaction,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| dependency(correlation))?;
        // One read snapshot for the anchor and page. No account/interaction writes,
        // participant enrollment, read receipt, or command admission.
        let agent = query.agent_id.map(|id| id.into_uuid());
        let excluded = query.exclude_agent_id.map(|id| id.into_uuid());
        if let Some(agent) = agent.or(excluded) {
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.product='musteragent')")
                .bind(agent).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
                .fetch_one(&mut *tx).await.map_err(|_|dependency(correlation))?;
            if !exists {
                return Err(map_interaction(
                    PostgresInteractionError::ActorForbidden,
                    correlation,
                ));
            }
        }
        let before: Option<(i64, uuid::Uuid)> = if let Some(entry) = query.before_entry_id {
            // Resolved anchors remain valid. Erasure/revocation never confers
            // authority, and anchors from another account or Agent are rejected.
            Some(sqlx::query_as("SELECT e.created_at_ms,e.entry_id FROM agent.agent_dialogue_entries e JOIN agent.agent_dialogues d USING(dialogue_id) JOIN agent.owned_agents a ON a.agent_id=d.agent_id JOIN agent.owner_spaces s ON s.space_id=a.space_id WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.entry_id=$3 AND s.tenant_id=e.tenant_id AND s.owner_id=e.owner_id AND s.product='musteragent' AND ($4::uuid IS NULL OR a.agent_id=$4) AND ($5::uuid IS NULL OR a.agent_id<>$5)")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(entry.into_uuid()).bind(agent).bind(excluded)
                .fetch_optional(&mut *tx).await.map_err(|_|dependency(correlation))?
                .ok_or_else(||map_interaction(PostgresInteractionError::ActorForbidden,correlation))?)
        } else {
            None
        };
        let sql = format!(
            "SELECT * FROM (SELECT e.*,a.agent_id AS pending_agent_id {FROM} WHERE {VISIBLE} AND {PENDING}
            UNION ALL SELECT e.*,a.agent_id AS pending_agent_id FROM agent.agent_dialogue_entries e
            JOIN agent.dialogue_publications p ON p.entry_id=e.entry_id AND p.execution_home='device'
            JOIN agent.native_dialogue_interaction_projections i ON i.interaction_id=e.interaction_id AND i.run_id=p.source_run_id AND i.tenant_id=e.tenant_id AND i.owner_id=e.owner_id
            JOIN agent.owned_agents a ON a.agent_id=i.agent_id AND a.dialogue_id=e.dialogue_id
            JOIN agent.owner_spaces s ON s.space_id=a.space_id AND s.tenant_id=e.tenant_id AND s.owner_id=e.owner_id AND s.product='musteragent'
            JOIN agent.owned_agent_configurations cfg ON cfg.agent_id=a.agent_id AND cfg.permission_revision=p.permission_revision
            WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.kind='interaction' AND a.status='active' AND i.status=1 AND i.control_open
            AND (i.expires_at_ms IS NULL OR i.expires_at_ms>floor(extract(epoch FROM statement_timestamp())*1000)::bigint)) pending
            WHERE ($3::uuid IS NULL OR pending_agent_id=$3) AND ($4::bigint IS NULL OR (created_at_ms,entry_id)<($4,$5::uuid)) AND ($7::uuid IS NULL OR pending_agent_id<>$7)
            ORDER BY created_at_ms DESC,entry_id DESC LIMIT $6"
        );
        let rows = sqlx::query(&sql)
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .bind(agent)
            .bind(before.map(|v| v.0))
            .bind(before.map(|v| v.1))
            .bind(i64::from(query.limit) + 1)
            .bind(excluded)
            .fetch_all(&mut *tx)
            .await
            .map_err(|_| dependency(correlation))?;
        let more = rows.len() > query.limit as usize;
        let mut items = Vec::with_capacity(query.limit as usize);
        for row in rows.iter().take(query.limit as usize) {
            items.push(DialoguePendingInteraction {
                agent_id: AgentId::from_uuid(
                    row.try_get("pending_agent_id")
                        .map_err(|_| corrupt(correlation))?,
                )
                .map_err(|_| corrupt(correlation))?,
                entry: crate::agent_dialogue::entry_row(row).map_err(|_| corrupt(correlation))?,
            });
        }
        let next_before_entry_id = if more {
            items.last().map(|v| v.entry.entry_id)
        } else {
            None
        };
        tx.commit().await.map_err(|_| dependency(correlation))?;
        Ok(DialoguePendingInteractionPage {
            items,
            next_before_entry_id,
        })
    }
}
