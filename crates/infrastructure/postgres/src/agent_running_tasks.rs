//! Account projection of one Agent's current executions. Membership, state and
//! the request excerpt come from the execution directory; the plan and the Run
//! version come from their original owners, all in one repeatable read.
use crate::dialogue_execution_activity::directory::{self, QueryScope};
use crate::{PostgresDatabaseRuntime, PostgresTransactionIsolation};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use musteragent_run::{ExecutionPlanDocument, PlanStepStatus};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use sqlx::{PgConnection, Row};
use std::collections::HashMap;
use std::sync::Arc;
type Error = DialogueStoreError;

#[derive(Clone)]
pub struct PostgresAgentRunningTasksQuery {
    database: PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
}
impl PostgresAgentRunningTasksQuery {
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
        }
    }

    async fn read_inner(&self, r: ReadAgentRunningTasks) -> Result<AgentRunningTasksPage, Error> {
        if r.actor.principal_kind != PrincipalKind::User {
            return Err(Error::Unavailable);
        }
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| Error::Infrastructure)?;
        // The directory requalifies this same identity; resolving the dialogue
        // here only names the Agent's single public dialogue.
        let dialogue: uuid::Uuid = sqlx::query_scalar(
            "SELECT a.dialogue_id FROM agent.owned_agents a
             JOIN agent.owner_spaces s USING(space_id)
             WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3
               AND a.status='active' AND s.product='musteragent'",
        )
        .bind(r.agent_id.into_uuid())
        .bind(r.actor.tenant_id.into_uuid())
        .bind(r.actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(Error::Unavailable)?;
        let dialogue = AgentDialogueId::from_uuid(dialogue).map_err(|_| Error::Corrupt)?;
        let mut observed_at_ms = 0;
        let mut entries = Vec::new();
        for (state, limit) in [
            (
                DialogueExecutionDirectoryFilter::Active,
                MAX_AGENT_RUNNING_TASKS,
            ),
            (
                DialogueExecutionDirectoryFilter::Terminal,
                MAX_AGENT_RECENT_TASKS,
            ),
        ] {
            let page = directory::list(
                &mut tx,
                self.content.as_ref(),
                crate::dialogue_execution_activity::directory::ExecutionDirectoryOwner {
                    actor: r.actor,
                    agent: r.agent_id,
                    dialogue,
                },
                DialogueExecutionDirectoryQuery {
                    cursor: None,
                    limit,
                    state,
                },
                DialogueExecutionDirectoryContract::IntentAndTaskV1,
                QueryScope::Dialogue,
            )
            .await?;
            observed_at_ms = observed_at_ms.max(page.observed_at_ms);
            entries.extend(page.entries);
        }
        let run_ids = entries
            .iter()
            .map(|entry| entry.run_id.into_uuid())
            .collect::<Vec<_>>();
        let runs = run_facts(&mut tx, r.actor.tenant_id, &run_ids).await?;
        let plans = plan_refs(&mut tx, r.actor.tenant_id, &run_ids).await?;
        let control_revision: i64 = sqlx::query_scalar(
            "SELECT coalesce(max(activity_revision),0) FROM agent.dialogue_execution_control_snapshots
             WHERE agent_id=$1 AND tenant_id=$2 AND owner_id=$3",
        )
        .bind(r.agent_id.into_uuid())
        .bind(r.actor.tenant_id.into_uuid())
        .bind(r.actor.principal_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;

        let mut tasks = Vec::with_capacity(entries.len());
        for entry in entries {
            let run = runs.get(&entry.run_id.into_uuid()).ok_or(Error::Corrupt)?;
            let terminal = entry.activity.state == DialogueActivityState::Terminal;
            if terminal
                && run.finished_at_ms.is_none_or(|ended| {
                    observed_at_ms.saturating_sub(ended) > AGENT_RUNNING_TASK_RECENT_WINDOW_MS
                })
            {
                continue;
            }
            // A plan is progress detail. One unreadable plan document must not
            // hide the task or the rest of the list.
            let plan = match plans.get(&entry.run_id.into_uuid()) {
                Some(reference) if !terminal => self
                    .read_plan(r.actor.tenant_id, reference)
                    .await
                    .unwrap_or_default(),
                _ => Vec::new(),
            };
            tasks.push(AgentRunningTask {
                run_id: entry.run_id,
                agent_id: entry.agent_id,
                dialogue_id: entry.dialogue_id,
                intent_id: entry.source.intent_id(),
                revision: run.version,
                title: entry
                    .request
                    .as_ref()
                    .and_then(|request| agent_running_task_title(&request.text)),
                title_source: AgentRunningTaskTitleSource::Request,
                activity: entry.activity,
                stop_requested: run.cancel_requested && !terminal,
                plan,
                notice: entry.delivery.terminal_notice.filter(|_| terminal),
                started_at_ms: entry.admitted_at_ms,
                ended_at_ms: run.finished_at_ms.filter(|_| terminal),
            });
        }
        Ok(AgentRunningTasksPage {
            tasks,
            observed_at_ms,
            control_revision: u64::try_from(control_revision).map_err(|_| Error::Corrupt)?,
        })
    }

    async fn read_plan(
        &self,
        tenant: TenantId,
        reference: &ContentRef,
    ) -> Result<Vec<AgentRunningTaskStep>, Error> {
        let bytes = self
            .content
            .read(tenant, reference)
            .await
            .map_err(|_| Error::Infrastructure)?;
        let document: ExecutionPlanDocument =
            serde_json::from_slice(&bytes).map_err(|_| Error::Corrupt)?;
        document.validate().map_err(|_| Error::Corrupt)?;
        Ok(document
            .steps
            .into_iter()
            .filter_map(|step| {
                let status = match step.status {
                    PlanStepStatus::Pending => AgentRunningTaskStepStatus::Pending,
                    PlanStepStatus::InProgress => AgentRunningTaskStepStatus::InProgress,
                    PlanStepStatus::Completed => AgentRunningTaskStepStatus::Completed,
                    // A step the Agent dropped is no longer part of its plan.
                    PlanStepStatus::Cancelled => return None,
                };
                Some(AgentRunningTaskStep {
                    content: step.content,
                    status,
                })
            })
            .collect())
    }
}

impl AgentRunningTasksQuery for PostgresAgentRunningTasksQuery {
    fn read(
        &self,
        request: ReadAgentRunningTasks,
    ) -> ApplicationFuture<'_, Result<AgentRunningTasksPage, Error>> {
        Box::pin(self.read_inner(request))
    }
}

struct RunFacts {
    version: u64,
    finished_at_ms: Option<i64>,
    cancel_requested: bool,
}

/// Cloud executions are Runs in this database. A Native execution runs on the
/// user's device and is known here only through its admission, activity
/// observations and terminal receipt, so its facts come from those owners.
async fn run_facts(
    tx: &mut PgConnection,
    tenant: TenantId,
    run_ids: &[uuid::Uuid],
) -> Result<HashMap<uuid::Uuid, RunFacts>, Error> {
    let rows = sqlx::query(
        "SELECT run_id,run_version::text AS version,finished_at_ms,
                cancel_requested_at_ms IS NOT NULL AS cancel_requested
         FROM runtime.runs WHERE tenant_id=$1 AND run_id=ANY($2)
         UNION ALL
         SELECT ids.run_id,(coalesce(o.run_sequence,0)+1)::text,
                coalesce(t.committed_at_ms,w.committed_at_ms,
                         CASE WHEN o.outcome IS NOT NULL THEN o.observed_at_ms END),
                false
         FROM unnest($2::uuid[]) AS ids(run_id)
         LEFT JOIN agent.native_execution_activity o ON o.run_id=ids.run_id AND o.tenant_id=$1
         LEFT JOIN agent.native_dialogue_terminals t ON t.run_id=ids.run_id AND t.tenant_id=$1
         LEFT JOIN collaboration.work_item_device_terminals w ON w.run_id=ids.run_id
         WHERE NOT EXISTS(SELECT 1 FROM runtime.runs r WHERE r.tenant_id=$1 AND r.run_id=ids.run_id)",
    )
    .bind(tenant.into_uuid())
    .bind(run_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(db)?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("run_id").map_err(db)?,
                RunFacts {
                    version: row
                        .try_get::<String, _>("version")
                        .map_err(db)?
                        .parse()
                        .map_err(|_| Error::Corrupt)?,
                    finished_at_ms: row.try_get("finished_at_ms").map_err(db)?,
                    cancel_requested: row.try_get("cancel_requested").map_err(db)?,
                },
            ))
        })
        .collect()
}

async fn plan_refs(
    tx: &mut PgConnection,
    tenant: TenantId,
    run_ids: &[uuid::Uuid],
) -> Result<HashMap<uuid::Uuid, ContentRef>, Error> {
    let rows = sqlx::query(
        "SELECT run_id,plan_content_id,plan_media_type,plan_byte_length::text AS plan_byte_length,plan_hash
         FROM runtime.execution_plans WHERE tenant_id=$1 AND run_id=ANY($2)",
    )
    .bind(tenant.into_uuid())
    .bind(run_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(db)?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("run_id").map_err(db)?,
                ContentRef {
                    content_id: ContentId::from_uuid(row.try_get("plan_content_id").map_err(db)?)
                        .map_err(|_| Error::Corrupt)?,
                    media_type: MediaType::new(
                        row.try_get::<String, _>("plan_media_type").map_err(db)?,
                    )
                    .map_err(|_| Error::Corrupt)?,
                    byte_length: row
                        .try_get::<String, _>("plan_byte_length")
                        .map_err(db)?
                        .parse()
                        .map_err(|_| Error::Corrupt)?,
                    sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("plan_hash").map_err(db)?)
                        .map_err(|_| Error::Corrupt)?,
                },
            ))
        })
        .collect()
}

fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}
