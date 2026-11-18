//! Native root availability is a bounded retry observation on the original
//! reservation. It cannot fabricate terminal or replace a newer claim/receipt.
use super::*;
impl PostgresWorkItemDeviceExecutionStore {
    pub(super) async fn defer_workspace_inner(
        &self,
        authority: &DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        digest: Sha256,
    ) -> Result<bool, DialogueStoreError> {
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        if claim.request.actor != authority.actor() || digest.into_bytes() == [0; 32] {
            return Err(DialogueStoreError::Unavailable);
        }
        // A delayed observation after successful ACK is a stale claim, not a
        // request to prepare the already Active Task again. This read does not
        // mutate or reorder locks; the original claim CAS below still decides.
        let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_outbox q JOIN collaboration.work_items w ON w.work_item_id=q.work_item_id WHERE q.event_seq=$1 AND q.work_item_id=$2 AND q.claim_owner=$3 AND q.claim_generation=$4 AND q.delivered_at_ms IS NULL AND w.tenant_id=$5 AND w.owner_id=$6)")
            .bind(integer(claim.event_seq)?).bind(claim.request.work_item_id.into_uuid()).bind(claim.owner).bind(integer(claim.generation)?).bind(claim.request.actor.tenant_id.into_uuid()).bind(claim.request.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if !pending {
            return Ok(false);
        }
        let source = preparation::load(
            &mut tx,
            claim.request,
            true,
            self.executions.admission.snapshots.as_ref(),
        )
        .await?;
        let WorkItemExecutionTarget::RemoteDevice(target) = &source.target else {
            return Err(DialogueStoreError::Unavailable);
        };
        // Reporting an unavailable directory needs authenticated retained host
        // continuity, not renewed permission to execute a Tool there.
        device_target::require_authority(
            self.targets.as_ref(),
            authority,
            claim.request.actor,
            target,
            &mut tx,
        )
        .await?;
        match claim::lock_claim(&mut tx, claim).await {
            Ok(()) => {}
            Err(DialogueStoreError::RevisionConflict) => return Ok(false),
            Err(e) => return Err(e),
        }
        // Device custody inherits account ownership from its exact original
        // reservation; the custody table deliberately has no tenant/owner copy.
        let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_device_custody d JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch) WHERE d.run_id=$1 AND x.run_id=d.run_id AND x.tenant_id=$2 AND x.owner_id=$3 AND d.source_digest=$4 AND d.admitted_at_ms IS NULL AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_device_terminals t WHERE t.run_id=d.run_id))")
            .bind(source.identities().run_id()?.into_uuid()).bind(claim.request.actor.tenant_id.into_uuid()).bind(claim.request.actor.principal_id.into_uuid()).bind(digest.into_bytes().as_slice()).fetch_one(&mut *tx).await.map_err(db)?;
        if !exact {
            return Err(DialogueStoreError::Unavailable);
        }
        let delay = 2u64
            .saturating_pow(claim.attempt_count.min(8) as u32)
            .min(300);
        let changed=sqlx::query("UPDATE collaboration.work_item_outbox SET claim_owner=NULL,lease_expires_at_ms=NULL,failure_code='workspace_unavailable',available_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+$4 WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL")
            .bind(integer(claim.event_seq)?).bind(claim.owner).bind(integer(claim.generation)?).bind(integer(delay*1000)?).execute(&mut *tx).await.map_err(db)?.rows_affected();
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(changed == 1)
    }
}
