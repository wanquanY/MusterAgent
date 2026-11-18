//! Native pickup of the original durable queue. The authenticated Backend
//! proves current membership; SQL proves this exact accepted destination.
use super::*;
use musteragent_runtime_session::{
    DeviceGrantCustody, DeviceHandoffStore, DeviceIntentClaim, DeviceSourceCustody,
    OriginDeviceExecutionTarget,
};

/// Source, proof and Memory reads all bind the same exact queue lease. A caller
/// cannot prolong it or change its attempt merely by retaining an owner UUID.
pub(super) async fn lock_device_claim(
    tx: &mut crate::GovernedPostgresTransaction<'_>,
    requested: DeviceIntentClaim,
) -> Result<bool, DialogueStoreError> {
    let claim = requested.claim;
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_intent_admissions WHERE intent_id=$1 AND execution_target='origin_device' AND target_device_id=$2 AND target_installation_id=$3 AND status='claimed' AND claim_owner=$4 AND claim_generation=$5 AND attempt_count=$6 AND lease_expires_at_ms=$7 AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint FOR UPDATE)")
        .bind(claim.intent_id.into_uuid()).bind(requested.device_id.into_uuid()).bind(requested.installation_id.into_uuid())
        .bind(claim.owner).bind(integer(claim.generation)?).bind(integer(claim.attempt_count)?).bind(claim.lease_expires_at.unix_millis())
        .fetch_one(&mut **tx).await.map_err(db)
}

impl DeviceHandoffStore for PostgresDialogueIntentHandoffStore {
    fn record_source_not_admitted<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        receipt: &'a musteragent_runtime_session::DeviceSourceNotAdmittedReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        Box::pin(self.record_source_not_admitted_inner(actor, target, receipt))
    }

    fn record_device_observation<'a>(
        &'a self,
        target: &'a OriginDeviceExecutionTarget,
        value: musteragent_runtime_session::DeviceAdmissionObservation,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>> {
        Box::pin(self.record_device_observation_inner(target, value))
    }

    fn discover_device<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        after: Option<UserIntentId>,
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_runtime_session::DeviceDiscoveryPage, DialogueStoreError>,
    > {
        Box::pin(self.discover_device_inner(actor, target, after))
    }

    fn acknowledge_device_admission<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        receipt: &'a musteragent_runtime_session::DeviceDeliveryBinding,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>> {
        Box::pin(self.acknowledge_device_admission_inner(actor, target, receipt))
    }
    fn claim_device(
        &self,
        actor: Actor,
        target: OriginDeviceExecutionTarget,
        intent: UserIntentId,
        owner: uuid::Uuid,
        lease: Duration,
    ) -> ApplicationFuture<'_, Result<Option<DeviceIntentClaim>, DialogueStoreError>> {
        Box::pin(async move {
            target.require_actor(actor)?;
            if owner.is_nil()
                || lease < Duration::from_secs(1)
                || lease > Duration::from_secs(300)
                || musteragent_runtime_snapshot::dialogue_device_discovery_policy_for_current_release().is_none()
            {
                return Err(DialogueStoreError::Unavailable);
            }
            // Current authority is observed by the original Backend membership
            // owner; database time additionally prevents stale pickup material.
            let now: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
            target.require_current_at(UtcTimestamp::from_unix_millis(now))?;
            let row = sqlx::query("WITH clock AS MATERIALIZED (SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint AS ms), candidate AS (SELECT q.intent_id FROM agent.dialogue_intent_admissions q JOIN agent.dialogue_user_intents i USING(intent_id),clock WHERE q.intent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3 AND q.execution_target='origin_device' AND q.target_device_id=$4 AND q.target_installation_id=$5 AND q.available_at_ms<=clock.ms AND NOT EXISTS(SELECT 1 FROM agent.device_source_refusals r WHERE r.intent_id=q.intent_id) AND NOT EXISTS(SELECT 1 FROM agent.device_preparation_observations o JOIN agent.device_admissions c USING(intent_id) WHERE o.intent_id=q.intent_id AND o.observation=3 AND o.source_digest=c.source_digest AND o.client_release_sha256=$8) AND ((q.status='pending' AND q.available_at_ms<=clock.ms) OR (q.status='claimed' AND (q.lease_expires_at_ms<=clock.ms OR q.claim_owner=$6))) FOR UPDATE OF q SKIP LOCKED), claimed AS (UPDATE agent.dialogue_intent_admissions q SET status='claimed',claim_owner=$6,claim_generation=CASE WHEN q.status='claimed' AND q.claim_owner=$6 AND q.lease_expires_at_ms>clock.ms THEN q.claim_generation ELSE q.claim_generation+1 END,attempt_count=CASE WHEN q.status='claimed' AND q.claim_owner=$6 AND q.lease_expires_at_ms>clock.ms THEN q.attempt_count ELSE q.attempt_count+1 END,lease_expires_at_ms=CASE WHEN q.status='claimed' AND q.claim_owner=$6 AND q.lease_expires_at_ms>clock.ms THEN q.lease_expires_at_ms ELSE clock.ms+$7 END FROM candidate,clock WHERE q.intent_id=candidate.intent_id RETURNING q.*) SELECT claim_generation,attempt_count,lease_expires_at_ms FROM claimed")
                .bind(intent.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
                .bind(target.device_id().into_uuid()).bind(target.installation_id().into_uuid())
                .bind(owner).bind(i64::try_from(lease.as_millis()).map_err(|_| DialogueStoreError::Corrupt)?)
                .bind(target.client_release_sha256())
                .fetch_optional(&self.pool).await.map_err(db)?;
            row.map(|row| {
                Ok(DeviceIntentClaim {
                    claim: DialogueIntentClaim {
                        actor,
                        intent_id: intent,
                        owner,
                        generation: unsigned(row.try_get("claim_generation").map_err(db)?)?,
                        attempt_count: unsigned(row.try_get("attempt_count").map_err(db)?)?,
                        lease_expires_at: UtcTimestamp::from_unix_millis(
                            row.try_get("lease_expires_at_ms").map_err(db)?,
                        ),
                    },
                    device_id: target.device_id(),
                    installation_id: target.installation_id(),
                })
            })
            .transpose()
        })
    }
    fn capture_device_source(
        &self,
        claim: DeviceIntentClaim,
    ) -> ApplicationFuture<'_, Result<DeviceSourceCustody, DialogueStoreError>> {
        Box::pin(self.capture_device_source_inner(claim, None))
    }
    fn capture_device_source_in_workspace(
        &self,
        claim: DeviceIntentClaim,
        chosen: Option<WorkspaceId>,
    ) -> ApplicationFuture<'_, Result<DeviceSourceCustody, DialogueStoreError>> {
        Box::pin(self.capture_device_source_inner(claim, chosen))
    }
    fn bind_device_grant(
        &self,
        claim: DeviceIntentClaim,
        signed: musteragent_execution_grant::SignedExecutionGrant,
    ) -> ApplicationFuture<'_, Result<DeviceGrantCustody, DialogueStoreError>> {
        Box::pin(async move {
            self.device_grant(claim, Some(signed))
                .await?
                .ok_or(DialogueStoreError::Corrupt)
        })
    }
    fn reauthorize_device_grant<'a>(
        &'a self,
        claim: DeviceIntentClaim,
        target: &'a OriginDeviceExecutionTarget,
        expected_grant_hash: musteragent_content_ref::Sha256,
        source_digest: musteragent_content_ref::Sha256,
        signed: musteragent_execution_grant::SignedExecutionGrant,
    ) -> ApplicationFuture<'a, Result<DeviceGrantCustody, DialogueStoreError>> {
        Box::pin(self.reauthorize_device_grant_inner(
            claim,
            target,
            expected_grant_hash,
            source_digest,
            signed,
        ))
    }
    fn read_device_grant(
        &self,
        claim: DeviceIntentClaim,
    ) -> ApplicationFuture<'_, Result<Option<DeviceGrantCustody>, DialogueStoreError>> {
        Box::pin(self.device_grant(claim, None))
    }
}
