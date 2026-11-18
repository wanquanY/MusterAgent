//! Recoverable original WorkItem command obligations. The queue never writes
//! Runtime status: it calls the original control owner, then proves quiescence.
use super::*;
use musteragent_content_ref::MediaType;
use musteragent_runtime_content::{ContentStore, PutContent};
use std::sync::Arc;

#[derive(Clone)]
pub struct PostgresWorkItemControlStore {
    executions: PostgresWorkItemExecutionStore,
    content: Arc<dyn ContentStore>,
}
impl PostgresWorkItemControlStore {
    pub fn new(executions: PostgresWorkItemExecutionStore, content: Arc<dyn ContentStore>) -> Self {
        Self {
            executions,
            content,
        }
    }
    async fn claim_inner(
        &self,
        owner: uuid::Uuid,
    ) -> Result<Option<WorkItemControlClaim>, DialogueStoreError> {
        if owner.is_nil() {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let row=sqlx::query("WITH clock AS MATERIALIZED(SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint ms), candidate AS (
   SELECT q.event_seq FROM collaboration.work_item_outbox q JOIN collaboration.work_item_commands c ON c.work_item_id=q.work_item_id AND c.state_revision=q.state_revision,clock
   WHERE q.event_kind IN ('control_requested','goal_amended') AND c.operation IN ('cancel','amend') AND q.delivered_at_ms IS NULL AND q.available_at_ms<=clock.ms AND (q.claim_owner IS NULL OR q.lease_expires_at_ms<=clock.ms)
   ORDER BY q.available_at_ms,q.event_seq LIMIT 1 FOR UPDATE OF q SKIP LOCKED), claimed AS (
    UPDATE collaboration.work_item_outbox q SET claim_owner=$1,claim_generation=q.claim_generation+1,attempt_count=q.attempt_count+1,lease_expires_at_ms=clock.ms+120000 FROM candidate,clock WHERE q.event_seq=candidate.event_seq RETURNING q.*)
   SELECT q.*,c.command_id,c.operation,t.tenant_id,t.owner_id,t.space_id,t.agent_id,r.goal_revision,r.control_revision,
    (t.erasure_job_id IS NOT NULL OR current.goal_revision<>r.goal_revision OR current.control_revision<>r.control_revision OR current.status<>'blocked') AS obsolete
   FROM claimed q JOIN collaboration.work_item_commands c ON c.work_item_id=q.work_item_id AND c.state_revision=q.state_revision
   JOIN collaboration.work_items t ON t.work_item_id=q.work_item_id JOIN collaboration.work_item_revisions r ON r.work_item_id=q.work_item_id AND r.state_revision=q.state_revision
   JOIN collaboration.work_item_revisions current ON current.work_item_id=t.work_item_id AND current.state_revision=t.state_revision")
   .bind(owner).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else { return Ok(None) };
        macro_rules! id {
            ($ty:ty,$name:literal) => {
                <$ty>::from_uuid(row.try_get($name).map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?
            };
        }
        let actor = Actor {
            tenant_id: id!(TenantId, "tenant_id"),
            principal_id: id!(PrincipalId, "owner_id"),
            principal_kind: PrincipalKind::User,
        };
        let mut claim = WorkItemControlClaim {
            event_seq: unsigned(row.try_get("event_seq").map_err(db)?)?,
            owner,
            generation: unsigned(row.try_get("claim_generation").map_err(db)?)?,
            command_id: id!(CommandId, "command_id"),
            actor,
            scope: OwnedAgentWorkItemScope {
                owner_id: actor.principal_id,
                space_id: id!(OwnerSpaceId, "space_id"),
                agent_id: id!(AgentId, "agent_id"),
            },
            work_item_id: id!(WorkItemId, "work_item_id"),
            goal: WorkItemGoalRevision::new(unsigned(row.try_get("goal_revision").map_err(db)?)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            control: WorkItemControlRevision::new(unsigned(
                row.try_get("control_revision").map_err(db)?,
            )?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
            operation: match row.try_get::<&str, _>("operation").map_err(db)? {
                "cancel" => WorkItemControlOperation::Cancel,
                "amend" => WorkItemControlOperation::Amend,
                _ => return Err(DialogueStoreError::Corrupt),
            },
            runs: Vec::new(),
            obsolete: row.try_get("obsolete").map_err(db)?,
        };
        if !claim.obsolete {
            claim.runs = active_runs(&mut tx, &claim).await?;
        }
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(Some(claim))
    }
    async fn settle_inner(
        &self,
        c: &WorkItemControlClaim,
    ) -> Result<WorkItemControlProgress, DialogueStoreError> {
        // Preparing a new partner-facing message is reclaimable until the original
        // reservation transaction pins it. It is never used if authority changed.
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let old = crate::work_item_authority::load(&mut tx, c.actor, c.scope, c.work_item_id, None)
            .await
            .map_err(authority_error)?
            .ok_or(DialogueStoreError::Unavailable)?;
        let target:Option<uuid::Uuid>=sqlx::query_scalar("SELECT executing_agent_id FROM collaboration.work_item_execution_reservations WHERE work_item_id=$1 ORDER BY assignment_epoch DESC LIMIT 1").bind(c.work_item_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        let target = target
            .map(AgentId::from_uuid)
            .transpose()
            .map_err(|_| DialogueStoreError::Corrupt)?
            .unwrap_or(c.scope.agent_id);
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let partner_input = if !c.obsolete
            && c.operation == WorkItemControlOperation::Amend
            && target != c.scope.agent_id
        {
            let bytes = self
                .content
                .read(c.actor.tenant_id, &old.goal_content_ref)
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            let spec = musteragent_runtime_session::decode_work_item_goal(&old, &bytes)
                .map_err(authority_error)?;
            let id = ContentId::from_uuid(uuid::Uuid::new_v5(
                &c.command_id.into_uuid(),
                b"work-item-amend-partner-input@1",
            ))
            .map_err(|_| DialogueStoreError::Corrupt)?;
            Some(
                self.content
                    .put_reclaimable_at(
                        id,
                        PutContent {
                            tenant_id: c.actor.tenant_id,
                            media_type: MediaType::new("text/plain")
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                            bytes: work_item_partner_input_bytes(&spec),
                        },
                    )
                    .await
                    .map_err(|_| DialogueStoreError::Infrastructure)?,
            )
        } else {
            None
        };
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        preparation::lock_agents(&mut tx, c.actor, c.scope, target).await?;
        sqlx::query(
            "SELECT work_item_id FROM collaboration.work_items WHERE work_item_id=$1 FOR UPDATE",
        )
        .bind(c.work_item_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_outbox q JOIN collaboration.work_item_commands command ON command.work_item_id=q.work_item_id AND command.state_revision=q.state_revision WHERE q.event_seq=$1 AND q.claim_owner=$2 AND q.claim_generation=$3 AND q.delivered_at_ms IS NULL AND q.lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint AND command.command_id=$4 AND command.tenant_id=$5 AND command.owner_id=$6)")
   .bind(integer(c.event_seq)?).bind(c.owner).bind(integer(c.generation)?).bind(c.command_id.into_uuid()).bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if !valid {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let mut current =
            crate::work_item_authority::load(&mut tx, c.actor, c.scope, c.work_item_id, None)
                .await
                .map_err(authority_error)?
                .ok_or(DialogueStoreError::Unavailable)?;
        let fenced: bool = sqlx::query_scalar(
            "SELECT erasure_job_id IS NOT NULL FROM collaboration.work_items WHERE work_item_id=$1",
        )
        .bind(c.work_item_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if fenced
            || current.core.revisions.goal != c.goal
            || current.core.revisions.control != c.control
            || current.core.status != WorkItemStatus::Blocked
        {
            acknowledge(&mut tx, c, Some("superseded")).await?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            return Ok(WorkItemControlProgress::Superseded);
        }
        if !active_runs(&mut tx, c).await?.is_empty() {
            return Ok(WorkItemControlProgress::Deferred);
        }
        let pending_lease:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id JOIN runtime.leases l ON l.run_id=r.run_id WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.work_item_id=$3 AND s.subject_kind IN ('work_item','work_item_report'))")
   .bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid()).bind(c.work_item_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        let pending_device:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_device_custody d JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch) LEFT JOIN collaboration.work_item_device_terminals terminal ON terminal.run_id=d.run_id WHERE x.work_item_id=$1 AND x.tenant_id=$2 AND x.owner_id=$3 AND terminal.run_id IS NULL AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_device_stops stop WHERE stop.run_id=d.run_id))")
            .bind(c.work_item_id.into_uuid()).bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid())
            .fetch_one(&mut *tx).await.map_err(db)?;
        if pending_lease || pending_device {
            return Ok(WorkItemControlProgress::Deferred);
        }
        let at = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        match c.operation {
            WorkItemControlOperation::Cancel => {
                current.core = current
                    .core
                    .cancellation_settled(c.goal, c.control)
                    .map_err(|_| DialogueStoreError::RevisionConflict)?;
                current.updated_at = at;
                crate::work_item_authority::commands::revision_and_outbox(
                    &mut tx,
                    &current,
                    "cancelled",
                )
                .await
                .map_err(authority_error)?;
                sqlx::query(
                    "UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1",
                )
                .bind(c.work_item_id.into_uuid())
                .bind(integer(current.core.revisions.state.get())?)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
            WorkItemControlOperation::Amend => {
                // A new goal is not permission to repeat a previously dispatched write.
                // Such runs must first use the existing effect reconciliation/recovery owner.
                // A terminal rejected atomic Task command with no committed command
                // is proven effect-free; its mutation and Tool success share one commit.
                // Discovery only changes this Run's prompt/disclosed catalog. Its
                // committed Tool receipt is closure, even for an already-disclosed view.
                // This exception never applies to an external provider write.
                let effects:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id JOIN tool.tool_invocations i ON i.tenant_id=r.tenant_id AND i.run_id=r.run_id WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.work_item_id=$3 AND s.subject_kind IN ('work_item','work_item_report') AND i.effect<>1 AND i.status NOT IN (1,2)
      AND NOT ((i.status=4 AND (
       (i.tool_ref IN ('dialogue.publish@1','artifacts.deliver@1','artifacts.deliver@2') AND EXISTS(SELECT 1 FROM agent.dialogue_publications p WHERE p.tenant_id=i.tenant_id AND p.source_run_id=i.run_id AND p.source_tool_invocation_id=i.invocation_id))
       OR (i.tool_ref='tasks.submit_result@1' AND EXISTS(SELECT 1 FROM collaboration.work_item_result_submissions sub WHERE sub.tenant_id=i.tenant_id AND sub.producing_run_id=i.run_id AND sub.submission_invocation_id=i.invocation_id AND sub.request_sha256=i.request_hash AND sub.receipt_content_id=i.result_content_id))
       OR (i.tool_ref IN ('skills.view@1','tool.catalog.activate@1','tool.catalog.activate@2') AND i.result_content_id IS NOT NULL)
       OR (i.tool_ref IN ('tasks.amend@1','tasks.amend@2','tasks.control@1','tasks.verify@1','tasks.verify@2','tasks.verify@3') AND EXISTS(SELECT 1 FROM collaboration.work_item_commands command WHERE command.tenant_id=i.tenant_id AND command.work_item_id=s.work_item_id AND command.source_invocation_id=i.invocation_id AND command.source_run_id=i.run_id))
      )) OR (i.status IN(5,7) AND i.tool_ref='tasks.submit_result@1' AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_result_submissions sub WHERE sub.submission_invocation_id=i.invocation_id))
      OR (i.status IN (5,7) AND i.tool_ref IN ('tasks.amend@1','tasks.amend@2','tasks.control@1','tasks.verify@1','tasks.verify@2','tasks.verify@3')
       AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_commands command WHERE command.tenant_id=i.tenant_id AND command.source_invocation_id=i.invocation_id AND command.source_run_id=i.run_id))))")
      .bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid()).bind(c.work_item_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
                if effects {
                    sqlx::query("UPDATE collaboration.work_item_outbox SET failure_code='original_effect_reconciliation_required' WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL")
      .bind(integer(c.event_seq)?).bind(c.owner).bind(integer(c.generation)?).execute(&mut *tx).await.map_err(db)?;
                    tx.commit()
                        .await
                        .map_err(crate::dialogue_codec::transaction)?;
                    return Ok(WorkItemControlProgress::Deferred);
                }
                if current.goal_content_ref != old.goal_content_ref {
                    return Err(DialogueStoreError::RevisionConflict);
                }
                let cause=sqlx::query("SELECT command.source_run_id,command.source_invocation_id,command.source_snapshot_id,
        COALESCE(s.source_intent_id,native.intent_id) AS source_intent_id,COALESCE(s.dialogue_id,native.dialogue_id) AS dialogue_id
        FROM collaboration.work_item_commands command
        LEFT JOIN runtime.runs r ON command.source_home='cloud' AND r.tenant_id=command.tenant_id AND r.run_id=command.source_run_id
        LEFT JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
        LEFT JOIN resource.device_commands native ON command.source_home='device' AND native.invocation_id=command.source_invocation_id AND native.run_id=command.source_run_id AND native.tenant_id=command.tenant_id AND native.owner_id=command.owner_id
        WHERE command.command_id=$1 AND command.tenant_id=$2 AND command.owner_id=$3
          AND ((s.agent_id=$4 AND (s.subject_kind='intent' OR (s.subject_kind='work_item_report' AND s.work_item_id=command.work_item_id)))
            OR (native.agent_id=$4 AND native.admission_kind='intent' AND native.outcome='succeeded'))")
      .bind(c.command_id.into_uuid()).bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid()).bind(c.scope.agent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
                macro_rules! id {
                    ($ty:ty,$name:literal) => {
                        <$ty>::from_uuid(cause.try_get($name).map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?
                    };
                }
                let source = WorkItemStartSource {
                    actor: c.actor,
                    scope: c.scope,
                    source_dialogue_id: id!(AgentDialogueId, "dialogue_id"),
                    source_intent_id: id!(UserIntentId, "source_intent_id"),
                    source_run_id: id!(RunId, "source_run_id"),
                    source_invocation_id: id!(InvocationId, "source_invocation_id"),
                    source_snapshot_id: id!(RuntimeSnapshotId, "source_snapshot_id"),
                };
                start_source::retained_authority(&mut tx, &source, c.work_item_id).await?;
                start_reservation::reserve_existing(
                    &mut tx,
                    current,
                    &source,
                    target,
                    partner_input.as_ref(),
                    at,
                )
                .await?;
            }
        }
        acknowledge(&mut tx, c, None).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(WorkItemControlProgress::Settled)
    }
}
impl WorkItemControlStore for PostgresWorkItemControlStore {
    fn claim(
        &self,
        owner: uuid::Uuid,
    ) -> ApplicationFuture<'_, Result<Option<WorkItemControlClaim>, DialogueStoreError>> {
        Box::pin(self.claim_inner(owner))
    }
    fn settle<'a>(
        &'a self,
        c: &'a WorkItemControlClaim,
    ) -> ApplicationFuture<'a, Result<WorkItemControlProgress, DialogueStoreError>> {
        Box::pin(self.settle_inner(c))
    }
    fn defer<'a>(
        &'a self,
        c: &'a WorkItemControlClaim,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>> {
        Box::pin(async move {
            sqlx::query("UPDATE collaboration.work_item_outbox SET claim_owner=NULL,lease_expires_at_ms=NULL,available_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+1000,failure_code=COALESCE(failure_code,'awaiting_original_run_quiescence') WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL").bind(integer(c.event_seq)?).bind(c.owner).bind(integer(c.generation)?).execute(self.executions.admission.ledger.pool()).await.map_err(db)?;
            Ok(())
        })
    }
}
async fn active_runs(
    tx: &mut PgConnection,
    c: &WorkItemControlClaim,
) -> Result<Vec<WorkItemControlRun>, DialogueStoreError> {
    let rows=sqlx::query("SELECT r.run_id,r.conversation_id,r.home_epoch::text AS home_epoch FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.work_item_id=$3 AND s.subject_kind IN ('work_item','work_item_report') AND r.status NOT IN (5,6,7) ORDER BY r.run_id LIMIT 1001")
  .bind(c.actor.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid()).bind(c.work_item_id.into_uuid()).fetch_all(&mut *tx).await.map_err(db)?;
    if rows.len() > 1000 {
        return Err(DialogueStoreError::Unavailable);
    }
    rows.into_iter()
        .map(|r| {
            Ok(WorkItemControlRun {
                run_id: RunId::from_uuid(r.try_get("run_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                conversation_id: ConversationId::from_uuid(
                    r.try_get("conversation_id").map_err(db)?,
                )
                .map_err(|_| DialogueStoreError::Corrupt)?,
                home_epoch: HomeEpoch::new(
                    r.try_get::<&str, _>("home_epoch")
                        .map_err(db)?
                        .parse()
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                )
                .map_err(|_| DialogueStoreError::Corrupt)?,
            })
        })
        .collect()
}
async fn acknowledge(
    tx: &mut PgConnection,
    c: &WorkItemControlClaim,
    reason: Option<&str>,
) -> Result<(), DialogueStoreError> {
    let rows=sqlx::query("UPDATE collaboration.work_item_outbox SET delivered_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint,claim_owner=NULL,lease_expires_at_ms=NULL,failure_code=$4 WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint").bind(integer(c.event_seq)?).bind(c.owner).bind(integer(c.generation)?).bind(reason).execute(&mut *tx).await.map_err(db)?.rows_affected();
    if rows != 1 {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}
