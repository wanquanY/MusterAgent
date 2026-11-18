//! Recoverable P5 observation of immutable child lifecycle facts. Hook effects
//! use the event identity as their idempotency scope; failed observations retry.
use super::*;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_extension::{ExtensionContext, ExtensionHost, LifecycleEvent};
impl PostgresCloudDelegation {
    pub async fn dispatch_lifecycle(
        &self,
        host: &Arc<ExtensionHost>,
        workspace: &std::path::Path,
        now: UtcTimestamp,
    ) -> Result<(), Error> {
        if !host.has_extensions() {
            return Ok(());
        }
        let candidates:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT e.event_id FROM runtime.delegation_events e LEFT JOIN runtime.delegation_hook_deliveries h ON h.event_id=e.event_id WHERE e.kind IN ('subagent_spawned','subagent_ended') AND (h.event_id IS NULL OR (h.completed_at_ms IS NULL AND h.lease_until_ms<=$1)) ORDER BY e.occurred_at_ms,e.event_id LIMIT 32")
            .bind(now.unix_millis()).fetch_all(self.database.pool()).await.map_err(db)?;
        for id in candidates {
            let claim = uuid::Uuid::now_v7();
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Projection)
                .await
                .map_err(|_| Error::Unavailable)?;
            let claimed=sqlx::query("INSERT INTO runtime.delegation_hook_deliveries(event_id,claim_id,lease_until_ms) VALUES($1,$2,$3) ON CONFLICT(event_id) DO UPDATE SET claim_id=EXCLUDED.claim_id,lease_until_ms=EXCLUDED.lease_until_ms WHERE delegation_hook_deliveries.completed_at_ms IS NULL AND delegation_hook_deliveries.lease_until_ms<=$4")
                .bind(id).bind(claim).bind(now.unix_millis()+30_000).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?.rows_affected();
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            if claimed == 0 {
                continue;
            }
            let row=sqlx::query("SELECT r.*,a.attempt_id,a.attempt_number,a.worker_id,a.lease_id,a.fencing_token,a.lease_expires_at_ms,e.kind AS event_kind,e.payload,d.child_run_id FROM runtime.delegation_events e JOIN runtime.delegations d ON d.delegation_id=e.delegation_id JOIN runtime.runs r ON r.run_id=d.parent_run_id JOIN LATERAL(SELECT * FROM runtime.attempts WHERE run_id=r.run_id ORDER BY attempt_number DESC LIMIT 1)a ON TRUE WHERE e.event_id=$1")
                .bind(id).fetch_one(self.database.pool()).await.map_err(db)?;
            let context = scope(&row, workspace, id)?;
            let child = RunId::from_uuid(row.try_get("child_run_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?;
            let event = match row.try_get::<String, _>("event_kind").map_err(db)?.as_str() {
                "subagent_spawned" => LifecycleEvent::SubagentSpawned(child),
                "subagent_ended" => LifecycleEvent::SubagentEnded {
                    run_id: child,
                    report: row.try_get("payload").map_err(db)?,
                },
                _ => return Err(Error::Corrupt),
            };
            let result = host.lifecycle(&context, event).await;
            let succeeded = result.as_ref().is_ok_and(|failures| failures.is_empty());
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Projection)
                .await
                .map_err(|_| Error::Unavailable)?;
            sqlx::query("UPDATE runtime.delegation_hook_deliveries SET completed_at_ms=$3,lease_until_ms=$4 WHERE event_id=$1 AND claim_id=$2 AND completed_at_ms IS NULL")
                .bind(id).bind(claim).bind(succeeded.then_some(now.unix_millis())).bind(now.unix_millis()+1000).execute(&mut *tx).await.map_err(db)?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            if !succeeded {
                return Err(Error::Unavailable);
            }
        }
        Ok(())
    }
}
fn scope(
    row: &sqlx::postgres::PgRow,
    workspace: &std::path::Path,
    event: uuid::Uuid,
) -> Result<ExtensionContext, Error> {
    macro_rules! id {
        ($name:literal,$ty:ident) => {
            $ty::from_uuid(row.try_get($name).map_err(db)?).map_err(|_| Error::Corrupt)?
        };
    }
    let tenant_id = id!("tenant_id", TenantId);
    let conversation_id = id!("conversation_id", ConversationId);
    let run_id = id!("run_id", RunId);
    let workspace_id = row
        .try_get::<Option<uuid::Uuid>, _>("workspace_id")
        .map_err(db)?
        .map(WorkspaceId::from_uuid)
        .transpose()
        .map_err(|_| Error::Corrupt)?;
    let activity_kind = match row.try_get::<i16, _>("activity_kind").map_err(db)? {
        1 => musteragent_activity::ActivityKind::Chat,
        2 => musteragent_activity::ActivityKind::Coordination,
        3 => musteragent_activity::ActivityKind::Automation,
        4 => musteragent_activity::ActivityKind::Delegation,
        5 => musteragent_activity::ActivityKind::Maintenance,
        6 => musteragent_activity::ActivityKind::Evaluation,
        _ => return Err(Error::Corrupt),
    };
    let run = musteragent_run::RunContext {
        tenant_id,
        conversation_id,
        run_id,
        activity_id: id!("activity_id", ActivityId),
        activity_kind,
        executor_participant_id: id!("executor_participant_id", ParticipantId),
        requested_by_participant_id: id!("requested_by_participant_id", ParticipantId),
        parent_run_id: row
            .try_get::<Option<uuid::Uuid>, _>("parent_run_id")
            .map_err(db)?
            .map(RunId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        delegation_id: row
            .try_get::<Option<uuid::Uuid>, _>("delegation_id")
            .map_err(db)?
            .map(DelegationId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        execution_scope: workspace_id
            .map_or(ExecutionScope::Tenant { tenant_id }, |workspace_id| {
                ExecutionScope::Workspace { workspace_id }
            }),
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(
            u64::try_from(row.try_get::<i64, _>("home_epoch").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
        )
        .map_err(|_| Error::Corrupt)?,
        runtime_snapshot_id: id!("runtime_snapshot_id", RuntimeSnapshotId),
    };
    let attempt = AttemptContext {
        lease_expires_at: UtcTimestamp::from_unix_millis(
            row.try_get("lease_expires_at_ms").map_err(db)?,
        ),
        run_id,
        attempt_id: id!("attempt_id", AttemptId),
        attempt_number: AttemptNumber::new(
            u32::try_from(row.try_get::<i32, _>("attempt_number").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
        )
        .map_err(|_| Error::Corrupt)?,
        worker_id: musteragent_run::WorkerId::new(
            row.try_get::<String, _>("worker_id").map_err(db)?,
        )
        .map_err(|_| Error::Corrupt)?,
        lease_id: id!("lease_id", LeaseId),
        fencing_token: FencingToken::new(
            u64::try_from(row.try_get::<i64, _>("fencing_token").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
        )
        .map_err(|_| Error::Corrupt)?,
    };
    Ok(ExtensionContext {
        workspace_dir: workspace
            .join(tenant_id.to_string())
            .join(conversation_id.to_string()),
        session_key: format!("delegation-event:{event}"),
        run,
        attempt,
        channel: None,
        sandboxed: false,
        ingress: None,
    })
}
