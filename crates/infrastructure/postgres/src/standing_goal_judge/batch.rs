//! Carry receipts only through the original predecessor chain of one input batch.
use super::*;
impl PostgresStandingGoalJudgeRunSource {
    pub(super) async fn evidence_batch(
        &self,
        tenant: TenantId,
        run: RunId,
    ) -> Result<(Vec<uuid::Uuid>, Vec<ContentRef>), StandingGoalJudgeRunSourceError> {
        let rows = sqlx::query("WITH RECURSIVE chain AS (
            SELECT r.*,0 AS depth FROM runtime.runs r WHERE r.tenant_id=$1 AND r.run_id=$2
            UNION ALL SELECT p.*,c.depth+1 FROM chain c JOIN runtime.runs p
              ON p.tenant_id=c.tenant_id AND p.conversation_id=c.conversation_id AND p.run_id=c.parent_run_id
            WHERE c.depth<99 AND p.activity_kind=3 AND p.delegation_id IS NULL
              AND p.erasure_job_id IS NULL AND p.status IN (5,6,7,8)

        ) SELECT c.run_id,c.depth,rules.rules_version,rules.input_first,rules.input_last,e.encoded_event,l.payload_hash
          FROM chain c LEFT JOIN runtime.goal_run_processing_rules rules ON rules.tenant_id=c.tenant_id AND rules.run_id=c.run_id
          LEFT JOIN event_store.ledger_records l ON l.tenant_id=c.tenant_id AND l.conversation_id=c.conversation_id
            AND l.activity_id=c.activity_id AND l.run_id IS NULL AND l.record_kind=1
          LEFT JOIN event_store.domain_events e ON e.record_id=l.record_id
          ORDER BY c.depth")
            .bind(tenant.into_uuid()).bind(run.into_uuid()).fetch_all(&self.pool).await.map_err(unavailable)?;
        let mut runs = vec![run.into_uuid()];
        let mut contexts = std::collections::BTreeMap::new();
        for row in rows {
            let depth: i32 = row.try_get("depth").map_err(unavailable)?;
            let Some(bytes) = row
                .try_get::<Option<Vec<u8>>, _>("encoded_event")
                .map_err(unavailable)?
            else {
                continue;
            };
            let hash: Vec<u8> = row.try_get("payload_hash").map_err(unavailable)?;
            // Activity admission also writes other DomainEvents without a Run
            // ID. Only its original Goal context establishes the batch identity.
            let Some(batch) =
                musteragent_persistence_codec::goal_evidence_batch::decode(&bytes, &hash)
                    .map_err(|_| corrupt())?
            else {
                continue;
            };
            let rules: Option<Vec<u8>> = row.try_get("rules_version").map_err(unavailable)?;
            let window: (Option<i64>, Option<i64>) = (
                row.try_get("input_first").map_err(unavailable)?,
                row.try_get("input_last").map_err(unavailable)?,
            );
            let previous: uuid::Uuid = row.try_get("run_id").map_err(unavailable)?;
            if contexts
                .insert(depth, (previous, batch, rules, window))
                .is_some()
            {
                return Err(corrupt());
            }
        }
        let mut signature = None;
        let mut inputs = Vec::new();
        for (expected, (depth, (previous, batch, rules, window))) in
            contexts.into_iter().enumerate()
        {
            if depth != expected as i32 {
                break;
            }
            if depth == 0 {
                signature = Some((batch.fingerprint, rules, window));
                inputs = batch.inputs;
            } else if window.0.is_some()
                && signature.as_ref() == Some(&(batch.fingerprint, rules, window))
            {
                if runs.contains(&previous) {
                    return Err(corrupt());
                }
                runs.push(previous);
            } else {
                break;
            }
        }
        Ok((runs, inputs))
    }
}
