//! The requester can use a returned result or send a change to its original executor.
//! Either decision commits with the invoking tool. A later report has its own decision.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_session::WorkItemAuthorityRecord;
use sqlx::postgres::PgRow;

struct Returned {
    task: WorkItemAuthorityRecord,
    origin: WorkItemStartSource,
    executor: AgentId,
}
async fn returned(
    tx: &mut PgConnection,
    tenant: TenantId,
    report: RunId,
    allow_verified_feedback: bool,
) -> Result<Option<Returned>, DialogueStoreError> {
    let row = sqlx::query("SELECT x.*,r.goal_revision AS returned_goal,r.control_revision AS returned_control,r.assignment_epoch AS returned_epoch,
        t.source_intent_id,t.source_dialogue_id FROM collaboration.work_item_execution_reports r
        JOIN collaboration.work_item_execution_reservations x ON x.run_id=r.producing_run_id AND (x.requester_uses_result OR ($3 AND x.result_protocol='verified_result_v1'))
        JOIN collaboration.work_items t ON t.work_item_id=r.work_item_id AND t.tenant_id=r.tenant_id AND t.owner_id=r.owner_id
        WHERE r.report_run_id=$1 AND r.tenant_id=$2 AND t.erasure_job_id IS NULL FOR UPDATE OF t")
        .bind(report.into_uuid()).bind(tenant.into_uuid()).bind(allow_verified_feedback).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    macro_rules! id {
        ($ty:ty,$key:literal) => {
            <$ty>::from_uuid(row.try_get($key).map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?
        };
    }
    let actor = Actor {
        tenant_id: tenant,
        principal_id: id!(PrincipalId, "owner_id"),
        principal_kind: PrincipalKind::User,
    };
    let scope = OwnedAgentWorkItemScope {
        agent_id: id!(AgentId, "requesting_agent_id"),
        owner_id: actor.principal_id,
        space_id: id!(OwnerSpaceId, "space_id"),
    };
    let task =
        crate::work_item_authority::load(tx, actor, scope, id!(WorkItemId, "work_item_id"), None)
            .await
            .map_err(authority_error)?
            .ok_or(DialogueStoreError::Inactive)?;
    if Some(number(&row, "returned_epoch")?) != task.core.assignment_epoch.map(|e| e.get())
        || number(&row, "returned_goal")? != task.core.revisions.goal.get()
        || number(&row, "returned_control")? != task.core.revisions.control.get()
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(Some(Returned {
        task,
        executor: id!(AgentId, "executing_agent_id"),
        origin: WorkItemStartSource {
            actor,
            scope,
            source_intent_id: id!(UserIntentId, "source_intent_id"),
            source_dialogue_id: id!(AgentDialogueId, "source_dialogue_id"),
            source_run_id: id!(RunId, "source_run_id"),
            source_snapshot_id: id!(RuntimeSnapshotId, "source_snapshot_id"),
            source_invocation_id: id!(InvocationId, "source_invocation_id"),
        },
    }))
}
fn number(row: &PgRow, field: &str) -> Result<u64, DialogueStoreError> {
    unsigned(row.try_get(field).map_err(db)?)
}

pub(crate) async fn publication(
    tx: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    invocation: InvocationId,
    purpose: &str,
    at: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    if !matches!(purpose, "reply" | "result" | "blocked") {
        return Ok(());
    }
    let Some(Returned { mut task, .. }) = returned(tx, tenant, run, false).await? else {
        return Ok(());
    };
    let recorded = sqlx::query(
        "SELECT next_run_id FROM collaboration.work_item_requester_actions WHERE report_run_id=$1",
    )
    .bind(run.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    if let Some(row) = recorded {
        return if row
            .try_get::<Option<uuid::Uuid>, _>("next_run_id")
            .map_err(db)?
            .is_none()
        {
            Ok(())
        } else {
            Err(DialogueStoreError::RevisionConflict)
        };
    }
    if task.core.status == WorkItemStatus::Verifying {
        task.core = task
            .core
            .assess(
                task.core.revisions.goal,
                task.core.revisions.control,
                task.core
                    .assignment_epoch
                    .ok_or(DialogueStoreError::Corrupt)?,
                purpose != "blocked",
                false,
            )
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        task.updated_at = at;
        crate::work_item_authority::commands::revision_and_outbox(
            tx,
            &task,
            "verification_recorded",
        )
        .await
        .map_err(authority_error)?;
        sqlx::query("UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1")
            .bind(task.core.work_item_id.into_uuid())
            .bind(integer(task.core.revisions.state.get())?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
    }
    sqlx::query("INSERT INTO collaboration.work_item_requester_actions(report_run_id,invocation_id) VALUES($1,$2)").bind(run.into_uuid()).bind(invocation.into_uuid()).execute(tx).await.map_err(db)?;
    Ok(())
}

impl PostgresWorkItemStartStore {
    pub(super) async fn feedback(
        &self,
        attempt: &AttemptContext,
        c: CommitWorkItemFeedback,
    ) -> Result<(), DialogueStoreError> {
        let i = &c.invocation;
        let changes = WorkItemFeedback::parse(&c.request_bytes)?;
        if attempt.run_id != i.run_context.run_id
            || Sha256::digest(&c.request_bytes) != i.request_ref.sha256
            || c.partner_message.sha256 != Sha256::digest(changes.feedback.as_bytes())
            || c.result.sha256 != Sha256::digest(TASK_FEEDBACK_RECEIPT)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        {
            let mut connection = self.ledger.pool().acquire().await.map_err(db)?;
            if feedback_replayed(&mut connection, &c).await? {
                return Ok(());
            }
        }
        let (mut tx, lane) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        if feedback_replayed(&mut tx, &c).await? {
            return Ok(());
        }
        if lane.tenant_id != i.actor.tenant_id
            || lane.conversation_id != i.run_context.conversation_id
            || lane.activity_id != i.run_context.activity_id
            || lane.home_epoch != i.run_context.home_epoch
        {
            return Err(DialogueStoreError::Unavailable);
        }
        start::fence(&mut tx, attempt, lane).await?;
        crate::resource_commit::lock_invocation(
            &mut tx,
            i.actor.tenant_id,
            i.invocation_id,
            attempt,
        )
        .await
        .map_err(|_| DialogueStoreError::RevisionConflict)?;
        let returned = returned(&mut tx, i.actor.tenant_id, i.run_context.run_id, true)
            .await?
            .ok_or(DialogueStoreError::Unavailable)?;
        let source = start_source::load_for(
            &mut tx,
            i,
            "tasks.feedback@1",
            false,
            Some(returned.task.core.work_item_id),
        )
        .await?;
        if source.actor != returned.origin.actor
            || source.scope != returned.origin.scope
            || source.source_dialogue_id != returned.origin.source_dialogue_id
        {
            return Err(DialogueStoreError::Unavailable);
        }
        super::result_verification::require_terminal_tool_batch(&mut tx, i).await?;
        lock_ready_content_reference(&mut tx, i.actor.tenant_id, &i.request_ref).await?;
        let partner_message =
            (returned.executor != source.scope.agent_id).then_some(&c.partner_message);
        let next = start_reservation::reserve_assignment(
            &mut tx,
            returned.task,
            &returned.origin,
            returned.executor,
            partner_message,
            c.at,
            true,
        )
        .await?;
        let key = WorkItemExecutionKey {
            actor: next.actor,
            scope: source.scope,
            work_item_id: next.work_item_id,
            goal_revision: next.goal_revision,
            control_revision: next.control_revision,
            assignment_epoch: next.assignment_epoch,
            report_from_run_id: None,
            executing_agent_id: returned.executor,
        };
        sqlx::query("INSERT INTO collaboration.work_item_requester_actions(report_run_id,invocation_id,next_run_id,feedback_request_ref) VALUES($1,$2,$3,$4)")
            .bind(i.run_context.run_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(key.identities().run_id()?.into_uuid()).bind(content_bytes(&i.request_ref)).execute(&mut *tx).await.map_err(db)?;
        let result = CapabilityResult {
            result_hash: c.result.sha256,
            result_ref: c.result,
            result_schema: c.result_schema,
            provider_request_ref: None,
        };
        crate::artifact::authorize_result_content(&mut tx, i.actor.tenant_id, &result)
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?;
        self.finish_tool(tx, lane, attempt, i, &result, c.at).await
    }
}

async fn feedback_replayed(
    tx: &mut PgConnection,
    c: &CommitWorkItemFeedback,
) -> Result<bool, DialogueStoreError> {
    let i = &c.invocation;
    sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_requester_actions a JOIN tool.tool_invocations t ON t.invocation_id=a.invocation_id
            JOIN collaboration.work_item_execution_reports r USING(report_run_id) JOIN collaboration.work_items w ON w.work_item_id=r.work_item_id
            WHERE a.report_run_id=$1 AND a.invocation_id=$2 AND a.next_run_id IS NOT NULL AND t.status=4 AND t.tool_ref='tasks.feedback@1'
            AND t.request_content_id=$3 AND t.result_content_id=$4 AND t.tenant_id=$5 AND w.owner_id=$6 AND t.tool_call_id=$7 AND t.request_hash=$8 AND w.erasure_job_id IS NULL)")
            .bind(i.run_context.run_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(i.request_ref.content_id.into_uuid()).bind(c.result.content_id.into_uuid())
            .bind(i.actor.tenant_id.into_uuid()).bind(i.actor.principal_id.into_uuid()).bind(i.tool_call_id.as_str()).bind(i.request_ref.sha256.into_bytes().to_vec()).fetch_one(tx).await.map_err(db)
}

/// Load only the preceding delivery for the active assignment of this Task.
pub(crate) async fn load_feedback(
    tx: &mut PgConnection,
    tenant: TenantId,
    executor_run: RunId,
) -> Result<Option<WorkItemFeedbackContext>, DialogueStoreError> {
    let row = sqlx::query("SELECT r.report_content_ref, a.feedback_request_ref,
        ARRAY(SELECT item.content_ref FROM collaboration.work_item_result_items item
              WHERE item.result_id=r.private_result_id AND item.artifact_id IS NOT NULL ORDER BY item.ordinal) AS files
        FROM collaboration.work_item_requester_actions a
        JOIN collaboration.work_item_execution_reports r USING(report_run_id)
        JOIN collaboration.work_item_execution_reservations x ON x.run_id=a.next_run_id AND x.requester_uses_result
          AND (x.tenant_id,x.owner_id,x.work_item_id,x.executing_agent_id)=(r.tenant_id,r.owner_id,r.work_item_id,r.producing_agent_id)
          AND (x.goal_revision,x.control_revision,x.assignment_epoch)=(r.goal_revision,r.control_revision,r.assignment_epoch+1)
        JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id AND t.erasure_job_id IS NULL
        JOIN collaboration.work_item_revisions v ON v.work_item_id=t.work_item_id AND v.state_revision=t.state_revision
          AND (v.goal_revision,v.control_revision,v.assignment_epoch)=(x.goal_revision,x.control_revision,x.assignment_epoch)
        WHERE x.tenant_id=$1 AND x.run_id=$2")
        .bind(tenant.into_uuid()).bind(executor_run.into_uuid()).fetch_optional(tx).await.map_err(db)?;
    row.map(|r| {
        Ok(WorkItemFeedbackContext {
            previous_result: content_from_bytes(r.try_get("report_content_ref").map_err(db)?)?,
            request: content_from_bytes(r.try_get("feedback_request_ref").map_err(db)?)?,
            files: r
                .try_get::<Vec<Vec<u8>>, _>("files")
                .map_err(db)?
                .into_iter()
                .map(content_from_bytes)
                .collect::<Result<_, _>>()?,
        })
    })
    .transpose()
}
