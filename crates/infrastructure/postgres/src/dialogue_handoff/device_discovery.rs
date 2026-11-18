//! Original accepted-input discovery. Reads never claim a queue row or create
//! a grant. Inactive sources cannot starve later independent accepted inputs.
use super::*;
use musteragent_runtime_session::{
    DIALOGUE_DEVICE_DISCOVERY_LIMIT, DeviceDiscoveryPage, OriginDeviceExecutionTarget,
};

impl PostgresDialogueIntentHandoffStore {
    pub(super) async fn discover_device_inner(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        after: Option<UserIntentId>,
    ) -> Result<DeviceDiscoveryPage, DialogueStoreError> {
        target.require_actor(actor)?;
        if musteragent_runtime_snapshot::dialogue_device_discovery_policy_for_current_release()
            .is_none()
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let mut tx = self
            .database
            .read_unit_of_work(
                DbCommitKind::Admission,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Acquire, e))?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        target.require_current_at(now)?;
        // The current signed Native membership is verified by Backend. Here the
        // exact original account, active public scope and accepted installation
        // are checked independently. A live queue lease may have a local receipt
        // whose ACK was lost, so it remains discoverable until that ACK commits.
        let rows = sqlx::query(
            "SELECT q.intent_id,r.canonical_refusal,r.refusal_sha256, (r.intent_id IS NULL AND q.available_at_ms <= $5 AND (q.status='pending' OR q.lease_expires_at_ms <= $5)
               AND NOT EXISTS(SELECT 1 FROM agent.device_preparation_observations o
                 JOIN agent.device_admissions c USING(intent_id)
                 WHERE o.intent_id=q.intent_id AND o.observation=3 AND o.source_digest=c.source_digest
                   AND o.client_release_sha256=$8)) AS prepare_allowed FROM agent.dialogue_intent_admissions q
             JOIN agent.dialogue_user_intents i USING(intent_id)
             JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id
                AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id
             JOIN agent.owned_agents a ON a.agent_id=d.agent_id
             LEFT JOIN agent.device_source_refusals r ON r.intent_id=q.intent_id
             JOIN agent.owner_spaces s ON s.space_id=a.space_id
                AND s.tenant_id=i.tenant_id AND s.owner_id=i.owner_id
             WHERE i.tenant_id=$1 AND i.owner_id=$2
                AND q.execution_target='origin_device'
                AND q.target_device_id=$3 AND q.target_installation_id=$4
                AND q.status IN ('pending','claimed') AND (q.available_at_ms <= $5 OR EXISTS(SELECT 1 FROM agent.device_admissions c WHERE c.intent_id=q.intent_id))
                AND ($6::uuid IS NULL OR q.intent_id > $6)
                AND NOT EXISTS(SELECT 1 FROM agent.dialogue_admission_outcomes o WHERE o.intent_id=q.intent_id)
                AND NOT EXISTS(SELECT 1 FROM agent.device_admissions r
                    WHERE r.intent_id=q.intent_id AND r.admitted_at_ms IS NOT NULL)
             ORDER BY q.intent_id LIMIT $7")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
            .bind(target.device_id().into_uuid()).bind(target.installation_id().into_uuid())
            .bind(now.unix_millis()).bind(after.map(UserIntentId::into_uuid))
            .bind((DIALOGUE_DEVICE_DISCOVERY_LIMIT + 1) as i64)
            .bind(target.client_release_sha256())
            .fetch_all(&mut *tx).await.map_err(db)?;
        let more = rows.len() > DIALOGUE_DEVICE_DISCOVERY_LIMIT;
        let prepare_allowed = rows
            .iter()
            .take(DIALOGUE_DEVICE_DISCOVERY_LIMIT)
            .map(|row| row.try_get("prepare_allowed").map_err(db))
            .collect::<Result<Vec<bool>, _>>()?;
        let source_refusals = rows
            .iter()
            .take(DIALOGUE_DEVICE_DISCOVERY_LIMIT)
            .filter(|row| {
                row.try_get::<Option<Vec<u8>>, _>("canonical_refusal")
                    .map_or(true, |v| v.is_some())
            })
            .map(super::source_refusal::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let intents: Vec<_> = rows
            .into_iter()
            .take(DIALOGUE_DEVICE_DISCOVERY_LIMIT)
            .map(|row| {
                UserIntentId::from_uuid(row.try_get("intent_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)
            })
            .collect::<Result<_, _>>()?;
        let next_after = if more { intents.last().copied() } else { None };
        tx.commit()
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
        let page = DeviceDiscoveryPage {
            intents,
            next_after,
            prepare_allowed,
            source_refusals,
        };
        page.validate_refusals()?;
        for refusal in &page.source_refusals {
            if refusal.target.actor != actor
                || refusal.target.device_id != target.device_id()
                || refusal.target.installation_id != target.installation_id()
            {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        Ok(page)
    }
}
