//! Exact immutable observation receipts share the original custody transaction.
//! Contract blocks are nonterminal: a later original SQLite receipt always wins.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_runtime_session::{DeviceAdmissionObservation, DeviceAttemptObservation};

fn kind(value: DeviceAttemptObservation) -> i16 {
    match value {
        DeviceAttemptObservation::AuthorizationCheckFailed => 1,
        DeviceAttemptObservation::DependencyUnavailable => 2,
        DeviceAttemptObservation::RuntimeContractRejected => 3,
    }
}
fn fingerprint(value: DeviceAdmissionObservation, release: &str) -> Sha256 {
    let c = value.claim;
    let mut bytes = b"musteragent.native-preparation-observation.v1\0".to_vec();
    for id in [
        c.claim.actor.tenant_id.into_uuid(),
        c.claim.actor.principal_id.into_uuid(),
        c.claim.intent_id.into_uuid(),
        c.device_id.into_uuid(),
        c.installation_id.into_uuid(),
        c.claim.owner,
    ] {
        bytes.extend_from_slice(id.as_bytes());
    }
    bytes.extend_from_slice(&c.claim.generation.to_be_bytes());
    bytes.extend_from_slice(&c.claim.attempt_count.to_be_bytes());
    bytes.extend_from_slice(&c.claim.lease_expires_at.unix_millis().to_be_bytes());
    bytes.extend_from_slice(&value.source_digest.into_bytes());
    bytes.push(u8::from(value.expected_grant_hash.is_some()));
    if let Some(hash) = value.expected_grant_hash {
        bytes.extend_from_slice(&hash.into_bytes());
    }
    bytes.extend_from_slice(&kind(value.observation).to_be_bytes());
    bytes.extend_from_slice(release.as_bytes());
    Sha256::digest(bytes)
}
impl PostgresDialogueIntentHandoffStore {
    pub(in crate::dialogue_handoff) async fn record_device_observation_inner(
        &self,
        target: &OriginDeviceExecutionTarget,
        value: DeviceAdmissionObservation,
    ) -> Result<bool, DialogueStoreError> {
        let claim = value.claim.claim;
        target.require_actor(claim.actor)?;
        if target.device_id() != value.claim.device_id
            || target.installation_id() != value.claim.installation_id
            || (value.observation == DeviceAttemptObservation::RuntimeContractRejected
                && value.expected_grant_hash.is_none())
        {
            return Err(DialogueStoreError::Unavailable);
        }
        let digest = fingerprint(value, target.client_release_sha256());
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Acquire, e))?;
        lock_account(&mut tx, claim.actor).await?;
        // Account/input ownership is checked even for receipt replay. No lease,
        // grant or progress is rewritten by a replay after a successor exists.
        super::super::termination::lock_input(&mut tx, claim).await?;
        let at = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        target.require_current_at(at)?;
        let existing: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT request_sha256 FROM agent.device_preparation_observations WHERE intent_id=$1 AND claim_generation=$2")
            .bind(claim.intent_id.into_uuid()).bind(integer(claim.generation)?)
            .fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(existing) = existing {
            return if existing == digest.into_bytes() {
                Ok(true)
            } else {
                Err(DialogueStoreError::IdempotencyConflict)
            };
        }
        crate::dialogue_execution::lock_unresolved_intent(&mut tx, claim.actor, claim.intent_id)
            .await?;
        if !super::super::device::lock_device_claim(&mut tx, value.claim).await? {
            return Ok(false);
        }
        let row = sqlx::query("SELECT source_digest,admitted_at_ms FROM agent.device_admissions WHERE intent_id=$1 FOR UPDATE")
            .bind(claim.intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
            .ok_or(DialogueStoreError::Unavailable)?;
        let current: Option<Vec<u8>> = sqlx::query_scalar("SELECT v.signed_grant_hash FROM agent.device_grant_heads h JOIN agent.device_grant_versions v ON v.intent_id=h.intent_id AND v.revision=h.current_revision WHERE h.intent_id=$1")
            .bind(claim.intent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        if row
            .try_get::<Option<i64>, _>("admitted_at_ms")
            .map_err(db)?
            .is_some()
            || row.try_get::<Vec<u8>, _>("source_digest").map_err(db)?
                != value.source_digest.into_bytes()
            || current != value.expected_grant_hash.map(|v| v.into_bytes().to_vec())
        {
            return Ok(false);
        }
        let reason = match value.observation {
            DeviceAttemptObservation::AuthorizationCheckFailed => Blocker::AuthorizationCheckFailed,
            DeviceAttemptObservation::DependencyUnavailable => Blocker::DependencyUnavailable,
            DeviceAttemptObservation::RuntimeContractRejected => Blocker::RuntimeContractRejected,
        };
        if reason == Blocker::RuntimeContractRejected {
            refresh(&mut tx, claim.actor, claim.intent_id, Some(reason), false).await?;
            // Eligibility is source + qualified release, not elapsed time. Keep
            // the original queue and local-receipt discovery path intact.
            sqlx::query("UPDATE agent.dialogue_intent_admissions SET status='pending',claim_owner=NULL,lease_expires_at_ms=NULL,last_error_code='invalid_contract',available_at_ms=$4 WHERE intent_id=$1 AND status='claimed' AND claim_owner=$2 AND claim_generation=$3")
                .bind(claim.intent_id.into_uuid()).bind(claim.owner).bind(integer(claim.generation)?)
                .bind(at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        } else {
            defer(&mut tx, value.claim, reason).await?;
        }
        // A lock wait may cross the original lease boundary. Never release a
        // successor or persist a new observation using expired authority.
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        target.require_current_at(now)?;
        if now >= claim.lease_expires_at {
            return Ok(false);
        }
        sqlx::query("INSERT INTO agent.device_preparation_observations(intent_id,claim_generation,request_sha256,source_digest,client_release_sha256,observation,recorded_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7)")
            .bind(claim.intent_id.into_uuid()).bind(integer(claim.generation)?)
            .bind(digest.into_bytes().to_vec()).bind(value.source_digest.into_bytes().to_vec())
            .bind(target.client_release_sha256()).bind(kind(value.observation)).bind(now.unix_millis())
            .execute(&mut *tx).await.map_err(db)?;
        tx.commit()
            .await
            .map_err(|e| transaction_error(DbTransactionPhase::Commit, e))?;
        Ok(true)
    }
}
