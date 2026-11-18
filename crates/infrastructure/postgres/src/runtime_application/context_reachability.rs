use musteragent_identity::{
    ContextBranchId, ConversationId, HomeEpoch, LedgerOffset, RunId, TenantId,
};
use sqlx::PgPool;
use sqlx::postgres::PgRow;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) struct ReachableContextPoint {
    pub(super) lineage_head_branch_id: Option<ContextBranchId>,
    pub(super) through_offset: LedgerOffset,
}

const PRIOR_DISCOVERY_SOURCE_SQL: &str = r#"
WITH RECURSIVE branch_lineage AS (
    SELECT branch.branch_id,
           branch.parent_branch_id,
           $5::bigint AS maximum_sequence,
           branch.fork_from_sequence
    FROM conversation.context_branches AS branch
    WHERE branch.tenant_id = $1
      AND branch.conversation_id = $2
      AND branch.branch_id = $4::uuid

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
        WHEN $4::uuid IS NULL THEN $5::bigint
        ELSE (
            SELECT LEAST(lineage.maximum_sequence, lineage.fork_from_sequence)
            FROM branch_lineage AS lineage
            WHERE lineage.parent_branch_id IS NULL
        )
    END AS maximum_sequence
)
SELECT run.run_id, admitted.conversation_seq::text AS admitted_sequence,
       snapshot.encoded_snapshot, snapshot.content_hash
FROM runtime.runs AS run
JOIN event_store.ledger_records AS admitted
  ON admitted.tenant_id = run.tenant_id
 AND admitted.conversation_id = run.conversation_id
 AND admitted.conversation_seq = run.admitted_sequence
 AND admitted.home_epoch = run.home_epoch
JOIN capability.runtime_snapshots AS snapshot
  ON snapshot.tenant_id = run.tenant_id
 AND snapshot.run_id = run.run_id
WHERE run.tenant_id = $1
  AND run.conversation_id = $2
  AND run.home_epoch = $3::bigint
  AND run.status IN (5, 6, 7, 8)
  AND ($6::bigint IS NULL OR admitted.conversation_seq < $6::bigint)
  AND (
      (
          admitted.context_branch_id IS NULL
          AND admitted.conversation_seq <= (SELECT maximum_sequence FROM root_limit)
      )
      OR EXISTS (
          SELECT 1 FROM branch_lineage AS lineage
          WHERE lineage.branch_id = admitted.context_branch_id
            AND admitted.conversation_seq <= lineage.maximum_sequence
      )
  )
  AND NOT EXISTS (
      SELECT 1
      FROM conversation.messages AS message
      JOIN conversation.message_retractions AS retraction
        ON retraction.tenant_id = message.tenant_id
       AND retraction.conversation_id = message.conversation_id
       AND retraction.message_id = message.message_id
      JOIN event_store.ledger_records AS retraction_record
        ON retraction_record.record_id = retraction.retraction_record_id
       AND retraction_record.tenant_id = message.tenant_id
       AND retraction_record.conversation_id = message.conversation_id
       AND retraction_record.home_epoch = run.home_epoch
      WHERE message.tenant_id = run.tenant_id
        AND message.conversation_id = run.conversation_id
        AND message.run_id = run.run_id
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
  AND NOT EXISTS (
      SELECT 1
      FROM conversation.messages AS legacy_retracted
      WHERE legacy_retracted.tenant_id = run.tenant_id
        AND legacy_retracted.conversation_id = run.conversation_id
        AND legacy_retracted.run_id = run.run_id
        AND legacy_retracted.lifecycle = 3
        AND NOT EXISTS (
            SELECT 1
            FROM conversation.message_retractions AS historical
            WHERE historical.tenant_id = legacy_retracted.tenant_id
              AND historical.conversation_id = legacy_retracted.conversation_id
              AND historical.message_id = legacy_retracted.message_id
        )
  )
ORDER BY run.admitted_sequence DESC, run.run_id DESC
LIMIT 1
"#;

const SOURCE_RUN_ROW_SQL: &str = r#"
SELECT admitted.conversation_seq::text AS admitted_sequence,
       snapshot.encoded_snapshot, snapshot.content_hash
FROM runtime.runs AS run
JOIN event_store.ledger_records AS admitted
  ON admitted.tenant_id = run.tenant_id
 AND admitted.conversation_id = run.conversation_id
 AND admitted.conversation_seq = run.admitted_sequence
 AND admitted.home_epoch = run.home_epoch
JOIN capability.runtime_snapshots AS snapshot
  ON snapshot.tenant_id = run.tenant_id
 AND snapshot.run_id = run.run_id
WHERE run.tenant_id = $1
  AND run.conversation_id = $2
  AND run.home_epoch = $3::bigint
  AND run.run_id = $4
  AND run.status IN (5, 6, 7, 8)
"#;

const REACHABLE_TOOL_EXPOSURES_SQL: &str = r#"
WITH RECURSIVE branch_lineage AS (
    SELECT branch.branch_id,
           branch.parent_branch_id,
           $6::bigint AS maximum_sequence,
           branch.fork_from_sequence
    FROM conversation.context_branches AS branch
    WHERE branch.tenant_id = $1
      AND branch.conversation_id = $2
      AND branch.branch_id = $5::uuid

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
        WHEN $5::uuid IS NULL THEN $6::bigint
        ELSE (
            SELECT LEAST(lineage.maximum_sequence, lineage.fork_from_sequence)
            FROM branch_lineage AS lineage
            WHERE lineage.parent_branch_id IS NULL
        )
    END AS maximum_sequence
)
SELECT exposure.tool_key, exposure.schema_hash, exposure.ordinal
FROM runtime.tool_exposures AS exposure
JOIN event_store.ledger_records AS record
  ON record.record_id = exposure.record_id
 AND record.tenant_id = exposure.tenant_id
 AND record.conversation_id = $2
 AND record.run_id = exposure.run_id
 AND record.home_epoch = $3::bigint
WHERE exposure.tenant_id = $1
  AND exposure.run_id = $4
  AND (
      (
          record.context_branch_id IS NULL
          AND record.conversation_seq <= (SELECT maximum_sequence FROM root_limit)
      )
      OR EXISTS (
          SELECT 1 FROM branch_lineage AS lineage
          WHERE lineage.branch_id = record.context_branch_id
            AND record.conversation_seq <= lineage.maximum_sequence
      )
  )
ORDER BY exposure.ordinal
"#;

pub(super) async fn load_prior_discovery_source(
    pool: &PgPool,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    reachable_at: ReachableContextPoint,
    legacy_activation_before: Option<LedgerOffset>,
) -> Result<Option<PgRow>, sqlx::Error> {
    sqlx::query(PRIOR_DISCOVERY_SOURCE_SQL)
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(home_epoch.get().to_string())
        .bind(
            reachable_at
                .lineage_head_branch_id
                .map(ContextBranchId::into_uuid),
        )
        .bind(reachable_at.through_offset.get().to_string())
        .bind(legacy_activation_before.map(|value| value.get().to_string()))
        .fetch_optional(pool)
        .await
}

pub(super) async fn load_source_run(
    pool: &PgPool,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    source_run_id: RunId,
) -> Result<Option<PgRow>, sqlx::Error> {
    sqlx::query(SOURCE_RUN_ROW_SQL)
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(home_epoch.get().to_string())
        .bind(source_run_id.into_uuid())
        .fetch_optional(pool)
        .await
}

pub(super) async fn load_reachable_tool_exposures(
    pool: &PgPool,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    source_run_id: RunId,
    reachable_at: ReachableContextPoint,
) -> Result<Vec<PgRow>, sqlx::Error> {
    sqlx::query(REACHABLE_TOOL_EXPOSURES_SQL)
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(home_epoch.get().to_string())
        .bind(source_run_id.into_uuid())
        .bind(
            reachable_at
                .lineage_head_branch_id
                .map(ContextBranchId::into_uuid),
        )
        .bind(reachable_at.through_offset.get().to_string())
        .fetch_all(pool)
        .await
}
