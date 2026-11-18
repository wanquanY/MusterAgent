//! Sealed Dialogue execution file reads. Public Conversation/Run read owners
//! remain purpose-filtered; no Product caller can nominate an Agent here.
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::RuntimeHome;
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_contract::{
    ApplicationFuture, execution_liveness::InvocationExecutionTiming,
};
use musteragent_runtime_file_query::{
    AgentFileQuerySource, FileQueryAccess, FileQueryAuthority, FileQueryContext,
    FileQueryError as Error,
};
mod inputs;
mod outputs;
mod work_item_inputs;
pub use inputs::PostgresAgentInputFileSource;
pub(crate) use inputs::{
    accepted_intent_view, direct_view as direct_input_view, list_authorized, resolve_authorized,
};
pub use outputs::PostgresAgentOutputFileSource;
pub(crate) use outputs::{
    list_authorized as list_outputs_authorized, resolve_authorized as resolve_output_authorized,
};
use sqlx::{PgPool, Row};
use std::sync::Arc;

pub struct PostgresAgentFileQueryAuthority {
    pool: PgPool,
    standard: Arc<dyn FileQueryAuthority>,
    clock: Arc<dyn RuntimeClock>,
}
impl PostgresAgentFileQueryAuthority {
    pub fn new(
        pool: PgPool,
        standard: Arc<dyn FileQueryAuthority>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            pool,
            standard,
            clock,
        }
    }
    async fn resolve_inner(&self, access: &FileQueryAccess) -> Result<FileQueryContext, Error> {
        let Some(conversation) = access.conversation_id else {
            return self.standard.resolve(access).await;
        };
        let mut connection = self.pool.acquire().await.map_err(|_| Error::Unavailable)?;
        let purpose = sqlx::query_scalar::<_,String>("SELECT purpose FROM conversation.conversations WHERE tenant_id=$1 AND conversation_id=$2")
            .bind(access.actor.tenant_id.into_uuid()).bind(conversation.into_uuid()).fetch_optional(&mut *connection).await.map_err(|_| Error::Unavailable)?;
        if purpose.as_deref() == Some("standard") {
            drop(connection);
            return self.standard.resolve(access).await;
        }
        if !matches!(
            purpose.as_deref(),
            Some(
                "dialogue_intent_coordination"
                    | "agent_work_item_execution"
                    | "agent_work_item_report"
            )
        ) || access.workspace_id.is_some()
        {
            return Err(Error::Denied);
        }
        let run = access.run.as_ref().ok_or(Error::Denied)?;
        let invocation = run.invocation_id.ok_or(Error::Denied)?;
        let now = self.clock.now();
        let expired = match &run.timing {
            InvocationExecutionTiming::LegacyFixed { deadline } => *deadline <= now,
            InvocationExecutionTiming::ActivityV1(value) => value
                .business_deadline
                .as_ref()
                .is_some_and(|deadline| deadline.at <= now),
        };
        if expired
            || run.context.conversation_id != conversation
            || run.context.tenant_id != access.actor.tenant_id
            || run.context.control_home != RuntimeHome::Cloud
            || run.context.execution_home != RuntimeHome::Cloud
            || run.attempt.run_id != run.context.run_id
        {
            return Err(Error::Denied);
        }
        let (agent_id, tool_key) = crate::agent_resource::resolve_execution(
            &mut connection,
            access.actor,
            conversation,
            invocation,
        )
        .await
        .map_err(|_| Error::Unavailable)?
        .ok_or(Error::Denied)?;
        if !matches!(
            tool_key.as_str(),
            "files.list@3"
                | "files.search@3"
                | "files.inspect@3"
                | "files.read@4"
                | "files.list@4"
                | "files.list@5"
                | "files.search@4"
                | "files.inspect@4"
                | "files.inspect@5"
                | "files.read@5"
                | "data.python.run@9"
                | "data.python.run@12"
                | "files.list@6"
                | "files.inspect@6"
                | "files.read@6"
                | "files.search@5"
        ) {
            return Err(Error::Denied);
        }
        let row = sqlx::query("SELECT r.conversation_id,r.activity_id,r.activity_kind,r.executor_participant_id,
            r.requested_by_participant_id,r.parent_run_id,r.delegation_id,r.execution_scope_kind,
            r.workspace_id,r.device_id,r.control_home_kind,r.control_device_id,r.execution_home_kind,
            r.execution_device_id,r.home_epoch::text AS home_epoch,r.runtime_snapshot_id,
            s.permission_revision::text AS permission_revision
            FROM runtime.runs r
            JOIN runtime.attempts a ON a.tenant_id=r.tenant_id AND a.run_id=r.run_id
            JOIN runtime.leases l ON l.tenant_id=r.tenant_id AND l.run_id=r.run_id AND l.attempt_id=a.attempt_id
            JOIN tool.tool_invocations inv ON inv.tenant_id=r.tenant_id AND inv.run_id=r.run_id AND inv.conversation_id=r.conversation_id
            JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
            JOIN conversation.conversations c ON c.tenant_id=r.tenant_id AND c.conversation_id=r.conversation_id AND c.home_epoch=r.home_epoch
            WHERE r.tenant_id=$1 AND r.run_id=$2 AND r.conversation_id=$3 AND inv.invocation_id=$4
            AND a.attempt_id=$5 AND a.attempt_number=$6 AND a.status=1
            AND l.worker_id=$7 AND l.lease_id=$8 AND l.fencing_token=$9::bigint
            AND l.expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint
            AND r.status=2 AND r.cancel_requested_at_ms IS NULL")
            .bind(access.actor.tenant_id.into_uuid()).bind(run.context.run_id.into_uuid()).bind(conversation.into_uuid()).bind(invocation.into_uuid())
            .bind(run.attempt.attempt_id.into_uuid()).bind(i32::try_from(run.attempt.attempt_number.get()).map_err(|_| Error::Integrity)?)
            .bind(run.attempt.worker_id.as_str()).bind(run.attempt.lease_id.into_uuid()).bind(run.attempt.fencing_token.get().to_string())
            .fetch_optional(&mut *connection).await.map_err(|_| Error::Unavailable)?.ok_or(Error::Denied)?;
        let current = crate::tool_material::decode_run_context_for_skill_discovery(
            access.actor.tenant_id,
            run.context.run_id,
            &row,
        )
        .ok_or(Error::Integrity)?;
        if current != run.context {
            return Err(Error::Denied);
        }
        let permission_revision: String = row
            .try_get("permission_revision")
            .map_err(|_| Error::Integrity)?;
        let input_view =
            inputs::frozen_view(&mut connection, access.actor, run.context.run_id, agent_id)
                .await?;
        let authority = (
            access.actor.tenant_id.to_string(),
            access.actor.principal_id.to_string(),
            agent_id.to_string(),
            permission_revision,
        );
        // A text-only resource scope preserves the original authority bytes.
        // Only the new composite scope commits its exact accepted input cut.
        let revision = if input_view.has_inputs {
            serde_json::to_vec(&(authority, input_view.cut.source_chain_sha256.to_string()))
        } else {
            serde_json::to_vec(&authority)
        }
        .map_err(|_| Error::Integrity)?;
        let context = FileQueryContext {
            dialogue_source: Some(AgentFileQuerySource {
                input_view: Some(input_view),
                agent_id,
                invocation_id: invocation,
                tool_key,
            }),
            actor: access.actor,
            conversation_id: Some(conversation),
            now,
            authority_revision: Sha256::digest(revision).to_string(),
            content_allowlist: None,
            project_workspace_id: None,
        };
        context.validate()?;
        Ok(context)
    }
}
impl FileQueryAuthority for PostgresAgentFileQueryAuthority {
    fn resolve<'a>(
        &'a self,
        access: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, Error>> {
        Box::pin(self.resolve_inner(access))
    }
}
