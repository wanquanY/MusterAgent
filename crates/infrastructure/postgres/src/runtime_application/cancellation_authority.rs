//! Shared original Run cancellation read; callers separately prove Task scope.
use super::*;
#[derive(Clone, Copy)]
pub(super) enum Scope {
    Conversation,
    WorkItem,
    DialogueIntent,
}

async fn load_one(
    pool: &PgPool,
    actor: Actor,
    run_id: RunId,
    correlation_id: CorrelationId,
    scope: Scope,
) -> Result<CancelRunAuthority, RuntimeError> {
    let row = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    run.status, run.wait_reason, run.wait_error_code, run.wait_interaction_id,
                    CASE
                        WHEN EXISTS(SELECT 1 FROM tool.tool_invocations t WHERE t.tenant_id=run.tenant_id AND t.run_id=run.run_id AND t.invocation_id=run.wait_invocation_id AND t.status=6) THEN NULL
                        WHEN run.wait_interaction_id IS NULL
                            THEN run.wait_invocation_id
                        WHEN pending_interaction.requested_by_invocation_id
                                = run.wait_invocation_id
                             AND pending_tool.invocation_id IS NOT NULL
                            THEN run.wait_invocation_id
                        ELSE NULL
                    END AS pending_tool_invocation_id,
                    (SELECT t.invocation_id FROM tool.tool_invocations t WHERE t.tenant_id=run.tenant_id AND t.run_id=run.run_id AND t.invocation_id=run.wait_invocation_id AND t.status=6) AS unresolved_tool_invocation_id,
                    stream.last_sequence::text AS conversation_offset,
                    viewer.participant_id,
                    EXISTS (
                        SELECT 1 FROM runtime.leases
                        WHERE leases.tenant_id = run.tenant_id
                          AND leases.run_id = run.run_id
                    ) AS has_lease,
                    (
                        SELECT attempts.attempt_id
                        FROM runtime.attempts
                        WHERE attempts.tenant_id = run.tenant_id
                          AND attempts.run_id = run.run_id
                        ORDER BY attempts.attempt_number DESC
                        LIMIT 1
                    ) AS last_attempt_id
             FROM runtime.runs AS run
             JOIN conversation.conversations AS public_conversation ON public_conversation.tenant_id=run.tenant_id AND public_conversation.conversation_id=run.conversation_id AND (($4 AND public_conversation.purpose IN ('agent_work_item_execution','agent_work_item_report')) OR ($5 AND public_conversation.purpose='dialogue_intent_coordination') OR (NOT $4 AND NOT $5 AND public_conversation.purpose='standard'))
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             JOIN conversation.participants AS viewer
               ON viewer.tenant_id = run.tenant_id
              AND viewer.conversation_id = run.conversation_id
              AND ((viewer.kind = 1 AND viewer.principal_ref = $2)
                   OR ($4 AND viewer.kind=3 AND viewer.participant_id=run.requested_by_participant_id
                       AND viewer.service_ref IN ('musteragent.work-item@1','musteragent.work-item-report@1')))
              AND viewer.left_at_ms IS NULL
             LEFT JOIN approval.interactions AS pending_interaction
               ON pending_interaction.tenant_id = run.tenant_id
              AND pending_interaction.conversation_id = run.conversation_id
              AND pending_interaction.run_id = run.run_id
              AND pending_interaction.interaction_id = run.wait_interaction_id
              AND pending_interaction.status = 1
             LEFT JOIN tool.tool_invocations AS pending_tool
               ON pending_tool.tenant_id = run.tenant_id
              AND pending_tool.conversation_id = run.conversation_id
              AND pending_tool.run_id = run.run_id
              AND pending_tool.invocation_id = run.wait_invocation_id
              AND pending_tool.interaction_id = run.wait_interaction_id
              AND pending_tool.status IN (2, 3, 6)
             WHERE run.tenant_id = $1
               AND run.run_id = $3
               AND run.control_home_kind = 1
               AND run.control_device_id IS NULL
               AND run.execution_home_kind = 1
               AND run.execution_device_id IS NULL",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(matches!(scope,Scope::WorkItem))
        .bind(matches!(scope,Scope::DialogueIntent))
        .fetch_optional(pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .ok_or_else(|| not_found(ErrorCode::RunNotFound, correlation_id))?;
    let status: i16 = row
        .try_get("status")
        .map_err(|_| dependency(correlation_id))?;
    let has_lease: bool = row
        .try_get("has_lease")
        .map_err(|_| dependency(correlation_id))?;
    let mode = match (status, has_lease) {
        (2, true) => CancelMode::RequestActiveAttempt,
        (1 | 3, false) => CancelMode::CancelWithoutAttempt,
        // Cancellation is monotonic. An exact replay remains a Ledger
        // duplicate, while a newly issued stop against an already
        // cancelling Run is accepted as another auditable request without
        // resetting the original cancellation timestamp.
        (4, _) => CancelMode::RequestActiveAttempt,
        (5..=8, _) => CancelMode::CancelWithoutAttempt,
        _ => return Err(corrupt(correlation_id)),
    };
    let pending_interaction_id = optional_id(
        row.try_get("wait_interaction_id")
            .map_err(|_| dependency(correlation_id))?,
        correlation_id,
    )?;
    let pending_tool_invocation_id = optional_id(
        row.try_get("pending_tool_invocation_id")
            .map_err(|_| dependency(correlation_id))?,
        correlation_id,
    )?;
    let wait_reason: Option<i16> = row
        .try_get("wait_reason")
        .map_err(|_| dependency(correlation_id))?;
    if support::invalid_wait_shape(
        mode,
        wait_reason,
        pending_interaction_id.is_some(),
        pending_tool_invocation_id.is_some(),
    ) {
        return Err(corrupt(correlation_id));
    }
    Ok(CancelRunAuthority {
        tenant_id: actor.tenant_id,
        conversation_id: id(
            row.try_get("conversation_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?,
        home_epoch: HomeEpoch::new(parse_u64(
            row.try_get("home_epoch")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| corrupt(correlation_id))?,
        current_offset: LedgerOffset::new(parse_u64(
            row.try_get("conversation_offset")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?),
        activity_id: id(
            row.try_get("activity_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?,
        run_id,
        last_run_sequence: musteragent_identity::RunSeq::new(parse_u64(
            row.try_get("last_run_sequence")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| corrupt(correlation_id))?,
        requested_by_participant_id: id(
            row.try_get("participant_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?,
        last_attempt_id: optional_id(
            row.try_get("last_attempt_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?,
        mode,
        pending_interaction_id,
        pending_tool_invocation_id,
        unresolved_tool_invocation_id: optional_id(
            row.try_get("unresolved_tool_invocation_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?,
        descendants: Vec::new(),
    })
}

/// Freeze the transitive graph; the write owner rechecks the same set under
/// the conversation lane before appending any cancellation facts.
pub(super) async fn load(
    pool: &PgPool,
    actor: Actor,
    run_id: RunId,
    correlation_id: CorrelationId,
    scope: Scope,
) -> Result<CancelRunAuthority, RuntimeError> {
    let mut authority = load_one(pool, actor, run_id, correlation_id, scope).await?;
    let children = crate::execution::cascade::descendants(
        pool,
        actor.tenant_id,
        authority.conversation_id,
        run_id,
    )
    .await
    .map_err(|_| dependency(correlation_id))?;
    for child in children {
        let child = RunId::from_uuid(child).map_err(|_| corrupt(correlation_id))?;
        let a = load_one(pool, actor, child, correlation_id, scope).await?;
        if a.current_offset != authority.current_offset {
            return Err(dependency(correlation_id));
        }
        authority
            .descendants
            .push(musteragent_ledger::CascadeCancelTarget {
                run_id: child,
                activity_id: a.activity_id,
                expected_last_run_sequence: a.last_run_sequence,
                mode: if a.mode == CancelMode::CancelWithoutAttempt {
                    CancelMode::QueueCancellation
                } else {
                    a.mode
                },
                pending_interaction_id: a.pending_interaction_id,
                pending_tool_invocation_id: a.pending_tool_invocation_id,
                unresolved_tool_invocation_id: a.unresolved_tool_invocation_id,
            });
    }
    Ok(authority)
}
