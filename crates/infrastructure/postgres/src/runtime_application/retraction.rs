use std::collections::BTreeSet;

use musteragent_identity::{
    Actor, ConversationId, CorrelationId, HomeEpoch, LedgerOffset, MessageId,
};
use musteragent_ledger::AppendReceipt;
use musteragent_runtime_contract::{ErrorCode, RuntimeError};
use musteragent_runtime_session::{
    RetractMessageAuthority, RetractMessageCommit, RetractMessageTarget, runtime_error,
};
use sqlx::Row as _;

use super::{
    BeginAppend, PostgresRuntimeApplicationStore, corrupt, dependency, id, map_ledger, optional_id,
    parse_u64,
};

const RETRACTABLE_SUFFIX_SQL: &str = r#"
WITH RECURSIVE branch_lineage AS (
    SELECT branch.branch_id,
           branch.parent_branch_id,
           stream.last_sequence AS maximum_sequence,
           branch.fork_from_sequence
    FROM event_store.ledger_streams AS stream
    JOIN conversation.context_branch_state AS state
      ON state.tenant_id = stream.tenant_id
     AND state.conversation_id = stream.conversation_id
    JOIN conversation.context_branches AS branch
      ON branch.tenant_id = state.tenant_id
     AND branch.conversation_id = state.conversation_id
     AND branch.branch_id = state.active_branch_id
    WHERE stream.tenant_id = $1 AND stream.conversation_id = $2

    UNION ALL

    SELECT parent.branch_id,
           parent.parent_branch_id,
           LEAST(child.maximum_sequence, child.fork_from_sequence) AS maximum_sequence,
           parent.fork_from_sequence
    FROM branch_lineage AS child
    JOIN conversation.context_branches AS parent
      ON parent.tenant_id = $1
     AND parent.conversation_id = $2
     AND parent.branch_id = child.parent_branch_id
),
root_limit AS (
    SELECT CASE
        WHEN state.active_branch_id IS NULL THEN stream.last_sequence
        ELSE (
            SELECT LEAST(lineage.maximum_sequence, lineage.fork_from_sequence)
            FROM branch_lineage AS lineage
            WHERE lineage.parent_branch_id IS NULL
        )
    END AS maximum_sequence
    FROM event_store.ledger_streams AS stream
    LEFT JOIN conversation.context_branch_state AS state
      ON state.tenant_id = stream.tenant_id
     AND state.conversation_id = stream.conversation_id
    WHERE stream.tenant_id = $1 AND stream.conversation_id = $2
)
SELECT suffix.message_id,
       suffix.run_id,
       suffix.committed_sequence::text AS committed_sequence
FROM conversation.messages AS suffix
JOIN conversation.messages AS target
  ON target.tenant_id = suffix.tenant_id
 AND target.conversation_id = suffix.conversation_id
 AND target.message_id = $3
 AND target.role = 1
JOIN event_store.ledger_records AS accepted
  ON accepted.tenant_id = suffix.tenant_id
 AND accepted.conversation_id = suffix.conversation_id
 AND accepted.conversation_seq = suffix.committed_sequence
WHERE suffix.tenant_id = $1
  AND suffix.conversation_id = $2
  AND suffix.committed_sequence >= target.committed_sequence
  AND (
      (
          accepted.context_branch_id IS NULL
          AND accepted.conversation_seq <= (SELECT maximum_sequence FROM root_limit)
      )
      OR EXISTS (
          SELECT 1 FROM branch_lineage AS lineage
          WHERE lineage.branch_id = accepted.context_branch_id
            AND accepted.conversation_seq <= lineage.maximum_sequence
      )
  )
  AND NOT EXISTS (
      SELECT 1
      FROM conversation.message_retractions AS retraction
      JOIN event_store.ledger_records AS retraction_record
        ON retraction_record.record_id = retraction.retraction_record_id
      WHERE retraction.tenant_id = suffix.tenant_id
        AND retraction.conversation_id = suffix.conversation_id
        AND retraction.message_id = suffix.message_id
        AND (
            (
                retraction_record.context_branch_id IS NULL
                AND retraction_record.conversation_seq <= (
                    SELECT maximum_sequence FROM root_limit
                )
            )
            OR EXISTS (
                SELECT 1 FROM branch_lineage AS lineage
                WHERE lineage.branch_id = retraction_record.context_branch_id
                  AND retraction_record.conversation_seq <= lineage.maximum_sequence
            )
        )
  )
  AND NOT (
      suffix.lifecycle = 3
      AND NOT EXISTS (
          SELECT 1 FROM conversation.message_retractions AS historical
          WHERE historical.tenant_id = suffix.tenant_id
            AND historical.conversation_id = suffix.conversation_id
            AND historical.message_id = suffix.message_id
      )
  )
ORDER BY suffix.committed_sequence, suffix.message_id
"#;

const REACHABLE_PROJECTED_ASSISTANTS_SQL: &str = r#"
WITH RECURSIVE branch_lineage AS (
    SELECT branch.branch_id,
           branch.parent_branch_id,
           stream.last_sequence AS maximum_sequence,
           branch.fork_from_sequence
    FROM event_store.ledger_streams AS stream
    JOIN conversation.context_branch_state AS state
      ON state.tenant_id = stream.tenant_id
     AND state.conversation_id = stream.conversation_id
    JOIN conversation.context_branches AS branch
      ON branch.tenant_id = state.tenant_id
     AND branch.conversation_id = state.conversation_id
     AND branch.branch_id = state.active_branch_id
    WHERE stream.tenant_id = $1 AND stream.conversation_id = $2

    UNION ALL

    SELECT parent.branch_id,
           parent.parent_branch_id,
           LEAST(child.maximum_sequence, child.fork_from_sequence) AS maximum_sequence,
           parent.fork_from_sequence
    FROM branch_lineage AS child
    JOIN conversation.context_branches AS parent
      ON parent.tenant_id = $1
     AND parent.conversation_id = $2
     AND parent.branch_id = child.parent_branch_id
),
root_limit AS (
    SELECT CASE
        WHEN state.active_branch_id IS NULL THEN stream.last_sequence
        ELSE (
            SELECT LEAST(lineage.maximum_sequence, lineage.fork_from_sequence)
            FROM branch_lineage AS lineage
            WHERE lineage.parent_branch_id IS NULL
        )
    END AS maximum_sequence
    FROM event_store.ledger_streams AS stream
    LEFT JOIN conversation.context_branch_state AS state
      ON state.tenant_id = stream.tenant_id
     AND state.conversation_id = stream.conversation_id
    WHERE stream.tenant_id = $1 AND stream.conversation_id = $2
)
-- A multi-step Agent loop persists one invocation row per model turn while all
-- turns contribute to one logical Assistant message. Retraction cardinality is
-- therefore message/run, never invocation.
SELECT invocation.assistant_message_id,
       invocation.run_id,
       MIN(invocation.started_conversation_seq)::text AS started_sequence
FROM runtime.model_invocations AS invocation
JOIN event_store.ledger_records AS started_record
  ON started_record.tenant_id = invocation.tenant_id
 AND started_record.conversation_id = invocation.conversation_id
 AND started_record.conversation_seq = invocation.started_conversation_seq
WHERE invocation.tenant_id = $1
  AND invocation.conversation_id = $2
  AND invocation.run_id = ANY($3)
  AND invocation.assistant_message_id IS NOT NULL
  AND (
      (
          started_record.context_branch_id IS NULL
          AND started_record.conversation_seq <= (SELECT maximum_sequence FROM root_limit)
      )
      OR EXISTS (
          SELECT 1 FROM branch_lineage AS lineage
          WHERE lineage.branch_id = started_record.context_branch_id
            AND started_record.conversation_seq <= lineage.maximum_sequence
      )
  )
GROUP BY invocation.assistant_message_id, invocation.run_id
ORDER BY MIN(invocation.started_conversation_seq),
         invocation.assistant_message_id,
         invocation.run_id
"#;

impl PostgresRuntimeApplicationStore {
    pub(super) async fn authorize_retract_inner(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        message_id: MessageId,
        correlation_id: CorrelationId,
    ) -> Result<RetractMessageAuthority, RuntimeError> {
        let row = sqlx::query(
            "SELECT conversation.lifecycle,
                    conversation.home_epoch::text AS home_epoch,
                    stream.last_sequence::text AS conversation_offset,
                    viewer.participant_id
             FROM conversation.conversations AS conversation
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = conversation.tenant_id
              AND stream.conversation_id = conversation.conversation_id
             JOIN conversation.participants AS viewer
               ON viewer.tenant_id = conversation.tenant_id
              AND viewer.conversation_id = conversation.conversation_id
              AND viewer.kind = 1
              AND viewer.principal_ref = $3
              AND viewer.left_at_ms IS NULL
             JOIN conversation.messages AS target
               ON target.tenant_id = conversation.tenant_id
              AND target.conversation_id = conversation.conversation_id
              AND target.message_id = $4
              AND target.participant_id = viewer.participant_id
              AND target.role = 1
             WHERE conversation.tenant_id = $1
               AND conversation.conversation_id = $2
               AND conversation.control_home_kind = 1
               AND conversation.control_device_id IS NULL",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(message_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Message is not a retractable user message",
            )
        })?;
        if row
            .try_get::<i16, _>("lifecycle")
            .map_err(|_| dependency(correlation_id))?
            != 1
        {
            return Err(runtime_error(
                ErrorCode::ConversationArchived,
                correlation_id,
                false,
                "Conversation is archived",
            ));
        }
        let persisted_rows = sqlx::query(RETRACTABLE_SUFFIX_SQL)
            .bind(actor.tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .bind(message_id.into_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(|_| dependency(correlation_id))?;

        let mut ordered = Vec::with_capacity(persisted_rows.len());
        let mut persisted_ids = BTreeSet::new();
        let mut affected_runs = BTreeSet::new();
        for persisted in persisted_rows {
            let persisted_message_id = id(
                persisted
                    .try_get("message_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?;
            let source_run_id = optional_id(
                persisted
                    .try_get("run_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?;
            let sequence = parse_u64(
                persisted
                    .try_get("committed_sequence")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?;
            persisted_ids.insert(persisted_message_id);
            if let Some(run_id) = source_run_id {
                affected_runs.insert(run_id);
            }
            ordered.push((
                sequence,
                0_u8,
                RetractMessageTarget {
                    message_id: persisted_message_id,
                    source_run_id,
                    persisted_message: true,
                },
            ));
        }
        if !affected_runs.is_empty() {
            let run_ids = affected_runs
                .into_iter()
                .map(|value| value.into_uuid())
                .collect::<Vec<_>>();
            let projected_rows = sqlx::query(REACHABLE_PROJECTED_ASSISTANTS_SQL)
                .bind(actor.tenant_id.into_uuid())
                .bind(conversation_id.into_uuid())
                .bind(run_ids)
                .fetch_all(&self.pool)
                .await
                .inspect_err(|error| {
                    tracing::warn!(
                        correlation_id = %correlation_id,
                        error = %error,
                        "failed to resolve projected Assistant retraction targets"
                    );
                })
                .map_err(|_| dependency(correlation_id))?;
            for projected in projected_rows {
                let projected_message_id = id(
                    projected
                        .try_get("assistant_message_id")
                        .map_err(|_| dependency(correlation_id))?,
                    correlation_id,
                )?;
                if persisted_ids.contains(&projected_message_id) {
                    continue;
                }
                ordered.push((
                    parse_u64(
                        projected
                            .try_get("started_sequence")
                            .map_err(|_| dependency(correlation_id))?,
                        correlation_id,
                    )?,
                    1,
                    RetractMessageTarget {
                        message_id: projected_message_id,
                        source_run_id: Some(id(
                            projected
                                .try_get("run_id")
                                .map_err(|_| dependency(correlation_id))?,
                            correlation_id,
                        )?),
                        persisted_message: false,
                    },
                ));
            }
        }
        ordered.sort_by_key(|(sequence, kind, target)| {
            (*sequence, *kind, target.message_id.into_uuid())
        });
        let targets = ordered
            .into_iter()
            .map(|(_, _, target)| target)
            .collect::<Vec<_>>();
        if targets.first().map(|target| target.message_id) != Some(message_id) {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Message is not reachable and active on the current Context branch",
            ));
        }
        self.ensure_no_active_run(actor, conversation_id, correlation_id)
            .await?;
        Ok(RetractMessageAuthority {
            tenant_id: actor.tenant_id,
            conversation_id,
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
            requested_by_participant_id: id(
                row.try_get("participant_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
            targets,
        })
    }

    pub(super) async fn ensure_no_active_run(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: CorrelationId,
    ) -> Result<(), RuntimeError> {
        let active_run_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM runtime.runs
                 WHERE tenant_id = $1 AND conversation_id = $2
                   AND status IN (1, 2, 3, 4)
             )",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?;
        if active_run_exists {
            return Err(runtime_error(
                ErrorCode::RunTransitionInvalid,
                correlation_id,
                false,
                "Conversation has an active Run",
            ));
        }
        Ok(())
    }

    pub(super) async fn retract_inner(
        &self,
        correlation_id: CorrelationId,
        commit: RetractMessageCommit,
    ) -> Result<AppendReceipt, RuntimeError> {
        validate_retract(&commit).map_err(|code| {
            runtime_error(
                code,
                correlation_id,
                false,
                "Retract Message commit is invalid",
            )
        })?;
        let mut prepared = match self.ledger.begin_append(&commit.append).await {
            Ok(BeginAppend::Duplicate(receipt)) => return Ok(receipt),
            Ok(BeginAppend::Ready(prepared)) => prepared,
            Err(error) => return Err(map_ledger(error, correlation_id)),
        };
        let lifecycle: Option<i16> = sqlx::query_scalar(
            "SELECT lifecycle
             FROM conversation.conversations
             WHERE tenant_id = $1 AND conversation_id = $2
               AND home_epoch = $3::bigint
             FOR UPDATE",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.append.expected_home_epoch.get().to_string())
        .fetch_optional(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        match lifecycle {
            Some(1) => {}
            Some(2) => {
                return Err(runtime_error(
                    ErrorCode::ConversationArchived,
                    correlation_id,
                    false,
                    "Conversation is archived",
                ));
            }
            _ => {
                return Err(runtime_error(
                    ErrorCode::ConversationEpochStale,
                    correlation_id,
                    false,
                    "Conversation ownership epoch is stale",
                ));
            }
        }
        let active_runs = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT run_id
             FROM runtime.runs
             WHERE tenant_id = $1 AND conversation_id = $2
               AND status IN (1, 2, 3, 4)
             FOR UPDATE",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .fetch_all(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        if !active_runs.is_empty() {
            return Err(runtime_error(
                ErrorCode::RunTransitionInvalid,
                correlation_id,
                false,
                "Conversation has an active Run",
            ));
        }
        let persisted_ids = sqlx::query_scalar::<_, uuid::Uuid>(RETRACTABLE_SUFFIX_SQL)
            .bind(commit.actor.tenant_id.into_uuid())
            .bind(commit.append.conversation_id.into_uuid())
            .bind(commit.targets[0].message_id.into_uuid())
            .fetch_all(&mut *prepared.transaction_mut())
            .await
            .map_err(|_| dependency(correlation_id))?;
        let expected_ids = commit
            .targets
            .iter()
            .filter(|target| target.persisted_message)
            .map(|target| target.message_id.into_uuid())
            .collect::<Vec<_>>();
        if persisted_ids != expected_ids {
            return Err(runtime_error(
                ErrorCode::ConversationSeqConflict,
                correlation_id,
                false,
                "Conversation changed before message retraction",
            ));
        }
        let active_branch_id = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT active_branch_id
             FROM conversation.context_branch_state
             WHERE tenant_id = $1 AND conversation_id = $2",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .fetch_optional(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        let retraction_rows = commit
            .targets
            .iter()
            .zip(&commit.append.records)
            .zip(prepared.sequences())
            .filter_map(|((target, record), sequence)| {
                target.persisted_message.then_some((
                    record.record_id.into_uuid(),
                    target.message_id.into_uuid(),
                    sequence.get().to_string(),
                ))
            })
            .collect::<Vec<_>>();
        sqlx::query(
            "UPDATE conversation.messages
             SET lifecycle = 3, retracted_at_ms = $3
             WHERE tenant_id = $1 AND message_id = ANY($2)",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(expected_ids)
        .bind(commit.retracted_at.unix_millis())
        .execute(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        sqlx::query(
            "UPDATE conversation.conversations
             SET updated_at_ms = $3
             WHERE tenant_id = $1 AND conversation_id = $2",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.retracted_at.unix_millis())
        .execute(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        let mut staged = prepared
            .stage(&commit.append)
            .await
            .map_err(|error| map_ledger(error, correlation_id))?;
        for (record_id, message_id, sequence) in retraction_rows {
            sqlx::query(
                "INSERT INTO conversation.message_retractions (
                     retraction_record_id, tenant_id, conversation_id, message_id,
                     context_branch_id, retracted_sequence, retracted_at_ms
                 ) VALUES ($1, $2, $3, $4, $5, $6::bigint, $7)",
            )
            .bind(record_id)
            .bind(commit.actor.tenant_id.into_uuid())
            .bind(commit.append.conversation_id.into_uuid())
            .bind(message_id)
            .bind(active_branch_id)
            .bind(sequence)
            .bind(commit.retracted_at.unix_millis())
            .execute(&mut *staged.transaction_mut())
            .await
            .map_err(|_| dependency(correlation_id))?;
        }
        crate::resource_retraction::retract_resource_files_on(staged.transaction_mut(), &commit)
            .await
            .map_err(|_| dependency(correlation_id))?;
        staged
            .commit()
            .await
            .map_err(|error| map_ledger(error, correlation_id))
    }
}

fn validate_retract(commit: &RetractMessageCommit) -> Result<(), ErrorCode> {
    if commit.actor.tenant_id != commit.append.tenant_id
        || !commit.append.outbox.is_empty()
        || commit
            .managed_resources
            .as_ref()
            .is_some_and(|plan| !plan.validate())
        || commit.targets.is_empty()
        || !commit.targets[0].persisted_message
        || commit.append.records.len() != commit.targets.len()
        || commit
            .targets
            .iter()
            .any(|target| !target.persisted_message && target.source_run_id.is_none())
        || commit
            .targets
            .iter()
            .map(|target| target.message_id)
            .collect::<BTreeSet<_>>()
            .len()
            != commit.targets.len()
    {
        return Err(ErrorCode::InvalidCommand);
    }
    Ok(())
}
