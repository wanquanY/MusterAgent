//! Read-only predecessor selection. Both reads and inheritance commits use the
//! same transaction-scoped membership, root ownership and branch proof.
use super::*;
use crate::projection_branch::ProjectionBranchRoute;
use musteragent_identity::{ConversationSeq, LedgerOffset};
use musteragent_run::RunContext;

pub(crate) async fn predecessor(
    tx: &mut sqlx::PgConnection,
    context: RunContext,
    source: Option<RunId>,
) -> Result<Option<ExecutionPlan>, PlanError> {
    if context.activity_kind != musteragent_activity::ActivityKind::Chat
        || context.delegation_id.is_some()
    {
        return Ok(None);
    }
    let head: Option<String> = sqlx::query_scalar(
        "SELECT stream.last_sequence::text FROM runtime.runs AS target
         JOIN conversation.participants AS member ON member.tenant_id = target.tenant_id
           AND member.conversation_id = target.conversation_id
           AND member.participant_id = target.requested_by_participant_id AND member.left_at_ms IS NULL
         JOIN conversation.conversations AS conversation ON conversation.tenant_id = target.tenant_id
           AND conversation.conversation_id = target.conversation_id AND conversation.lifecycle = 1
         JOIN event_store.ledger_streams AS stream ON stream.tenant_id = target.tenant_id
           AND stream.conversation_id = target.conversation_id
         WHERE target.tenant_id = $1 AND target.conversation_id = $2 AND target.run_id = $3
           AND target.requested_by_participant_id = $4 AND target.executor_participant_id = $5
           AND target.activity_kind = 1 AND target.delegation_id IS NULL",
    ).bind(context.tenant_id.into_uuid()).bind(context.conversation_id.into_uuid())
     .bind(context.run_id.into_uuid()).bind(context.requested_by_participant_id.into_uuid())
     .bind(context.executor_participant_id.into_uuid()).fetch_optional(&mut *tx).await?;
    let head = head.ok_or(PlanError::InvalidReference)?;
    let head = LedgerOffset::new(parse_u64(head)?);
    let active: Option<uuid::Uuid> = sqlx::query_scalar("SELECT active_branch_id FROM conversation.context_branch_state WHERE tenant_id=$1 AND conversation_id=$2")
        .bind(context.tenant_id.into_uuid()).bind(context.conversation_id.into_uuid()).fetch_optional(&mut *tx).await?;
    let active = active
        .map(musteragent_identity::ContextBranchId::from_uuid)
        .transpose()
        .map_err(|_| PlanError::Corrupt)?;
    let branch =
        ProjectionBranchRoute::load(tx, context.tenant_id, context.conversation_id, head, active)
            .await
            .map_err(|_| PlanError::Corrupt)?;
    let retracted = branch
        .retracted_runs(tx, context.tenant_id, context.conversation_id)
        .await
        .map_err(|_| PlanError::Corrupt)?;
    if retracted.contains(&context.run_id) {
        return Err(PlanError::InvalidReference);
    }
    let admission = sqlx::query("SELECT conversation_seq::text AS conversation_seq, context_branch_id FROM event_store.ledger_records WHERE tenant_id=$1 AND conversation_id=$2 AND run_id=$3 AND run_seq=1")
        .bind(context.tenant_id.into_uuid()).bind(context.conversation_id.into_uuid())
        .bind(context.run_id.into_uuid()).fetch_optional(&mut *tx).await?
        .ok_or(PlanError::InvalidReference)?;
    let admission_seq = ConversationSeq::new(parse_u64(admission.try_get("conversation_seq")?)?)
        .map_err(|_| PlanError::Corrupt)?;
    let admission_branch = admission
        .try_get::<Option<uuid::Uuid>, _>("context_branch_id")?
        .map(musteragent_identity::ContextBranchId::from_uuid)
        .transpose()
        .map_err(|_| PlanError::Corrupt)?;
    if !branch.includes(admission_branch, admission_seq) {
        return Err(PlanError::InvalidReference);
    }
    let mut before = u64::MAX;
    loop {
        let rows = sqlx::query(
            "SELECT plan.run_id, record.conversation_seq::text AS conversation_seq, record.context_branch_id,
                    admission.conversation_seq::text AS admission_seq, admission.context_branch_id AS admission_branch
             FROM runtime.execution_plans AS plan
             JOIN runtime.runs AS owner ON owner.tenant_id = plan.tenant_id AND owner.run_id = plan.run_id
             JOIN event_store.ledger_records AS record ON record.tenant_id = plan.tenant_id AND record.record_id = plan.record_id
             JOIN event_store.ledger_records AS admission ON admission.tenant_id = owner.tenant_id
               AND admission.conversation_id = owner.conversation_id AND admission.run_id = owner.run_id AND admission.run_seq = 1
             JOIN event_store.ledger_records AS target ON target.tenant_id = owner.tenant_id
               AND target.conversation_id = owner.conversation_id AND target.run_id = $1 AND target.run_seq = 1
             JOIN artifact.content_refs AS body ON body.tenant_id = plan.tenant_id AND body.content_id = plan.plan_content_id
               AND body.state = 2
             WHERE plan.tenant_id = $2 AND owner.conversation_id = $3
               AND owner.activity_kind = 1 AND owner.delegation_id IS NULL AND owner.status BETWEEN 5 AND 8
               AND admission.conversation_seq < target.conversation_seq
               AND record.conversation_id = owner.conversation_id AND record.run_id = owner.run_id
               AND record.conversation_seq < $4::numeric AND record.conversation_seq <= $5::numeric
               AND ($6::uuid IS NULL OR plan.run_id = $6)
             ORDER BY record.conversation_seq DESC LIMIT 64",
        ).bind(context.run_id.into_uuid()).bind(context.tenant_id.into_uuid())
         .bind(context.conversation_id.into_uuid()).bind(before.to_string()).bind(head.get().to_string())
         .bind(source.map(|run| run.into_uuid())).fetch_all(&mut *tx).await?;
        for row in &rows {
            before = parse_u64(row.try_get("conversation_seq")?)?;
            let sequence = ConversationSeq::new(before).map_err(|_| PlanError::Corrupt)?;
            let admission = ConversationSeq::new(parse_u64(row.try_get("admission_seq")?)?)
                .map_err(|_| PlanError::Corrupt)?;
            let branch_id = row
                .try_get::<Option<uuid::Uuid>, _>("context_branch_id")?
                .map(musteragent_identity::ContextBranchId::from_uuid)
                .transpose()
                .map_err(|_| PlanError::Corrupt)?;
            let admission_branch = row
                .try_get::<Option<uuid::Uuid>, _>("admission_branch")?
                .map(musteragent_identity::ContextBranchId::from_uuid)
                .transpose()
                .map_err(|_| PlanError::Corrupt)?;
            if branch.includes(branch_id, sequence) && branch.includes(admission_branch, admission)
            {
                let run_id =
                    RunId::from_uuid(row.try_get("run_id")?).map_err(|_| PlanError::Corrupt)?;
                if retracted.contains(&run_id) {
                    continue;
                }
                return load_plan(&mut *tx, context.tenant_id, run_id, false).await;
            }
        }
        if rows.len() < 64 {
            return Ok(None);
        }
    }
}
