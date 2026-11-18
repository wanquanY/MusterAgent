//! Revocation-time closure of Device Tasks whose original membership lineage
//! has permanently ended. Pickup-time settlement (`claim.rs`) needs the Device
//! to sign in again; this sweep covers a Device that never does. It settles
//! only on a definite "ended" answer from the original membership Owner, under
//! its own claim, and only while the stored target is exactly the judged one.
use super::*;
use std::{collections::HashMap, num::NonZeroU64, sync::Mutex};

const SETTLE_LEASE_MS: i64 = 60_000;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct WorkItemDeviceLineageSweepReport {
    pub inspected: usize,
    pub settled: usize,
    /// The pass reached the end of the queue and starts from the beginning next.
    pub wrapped: bool,
}

pub struct PostgresWorkItemDeviceLineageSweep {
    executions: PostgresWorkItemExecutionStore,
    targets: Arc<dyn WorkItemDeviceTargetAuthority>,
    after_event_seq: Mutex<i64>,
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    event_seq: i64,
    work_item_id: WorkItemId,
    actor: Actor,
    destination: DeviceDestination,
}

impl PostgresWorkItemDeviceLineageSweep {
    pub fn new(
        executions: PostgresWorkItemExecutionStore,
        targets: Arc<dyn WorkItemDeviceTargetAuthority>,
    ) -> Self {
        Self {
            executions,
            targets,
            after_event_seq: Mutex::new(0),
        }
    }

    /// Inspects at most `batch` ready Device obligations after the cursor.
    /// Ordinary errors and unavailability never settle anything.
    pub async fn sweep_once(
        &self,
        owner: uuid::Uuid,
        batch: u32,
    ) -> Result<WorkItemDeviceLineageSweepReport, DialogueStoreError> {
        if owner.is_nil() || !(1..=256).contains(&batch) {
            return Err(DialogueStoreError::Corrupt);
        }
        let after = *self
            .after_event_seq
            .lock()
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let candidates = self.candidates(after, batch).await?;
        let wrapped = candidates.len() < batch as usize;
        let next = if wrapped {
            0
        } else {
            candidates.last().map_or(0, |c| c.event_seq)
        };
        let mut report = WorkItemDeviceLineageSweepReport {
            wrapped,
            ..Default::default()
        };
        let mut answers = HashMap::new();
        for candidate in candidates {
            report.inspected += 1;
            let d = candidate.destination;
            let key = (
                candidate.actor.tenant_id.into_uuid(),
                candidate.actor.principal_id.into_uuid(),
                d.device_id.into_uuid(),
                d.installation_id.into_uuid(),
                d.membership_id,
                d.membership_epoch,
            );
            let lineage = match answers.get(&key) {
                Some(lineage) => *lineage,
                None => {
                    let lineage = inspect(
                        self.targets.as_ref(),
                        candidate.actor,
                        candidate.destination,
                    )
                    .await;
                    let lineage = match lineage {
                        Ok(lineage) => lineage,
                        Err(error) => {
                            tracing::debug!(
                                ?error,
                                "Device membership lineage is unavailable; Task retained"
                            );
                            continue;
                        }
                    };
                    answers.insert(key, lineage);
                    lineage
                }
            };
            if lineage != WorkItemDeviceMembershipLineage::Ended {
                continue;
            }
            match self.settle(owner, candidate).await {
                Ok(true) => {
                    report.settled += 1;
                    tracing::info!(
                        work_item_id = %candidate.work_item_id.into_uuid(),
                        "Task device target membership lineage ended; execution rejected"
                    );
                }
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    ?error,
                    work_item_id = %candidate.work_item_id.into_uuid(),
                    "Ended device membership settlement deferred"
                ),
            }
        }
        *self
            .after_event_seq
            .lock()
            .map_err(|_| DialogueStoreError::Infrastructure)? = next;
        Ok(report)
    }

    async fn candidates(
        &self,
        after: i64,
        batch: u32,
    ) -> Result<Vec<Candidate>, DialogueStoreError> {
        let rows = sqlx::query("SELECT q.event_seq,q.work_item_id,t.tenant_id,t.owner_id,d.device_id,d.installation_id,d.membership_id,d.membership_epoch
 FROM collaboration.work_item_outbox q
 JOIN collaboration.work_items t USING(work_item_id)
 JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision
 JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=t.work_item_id AND x.goal_revision=r.goal_revision AND x.control_revision=r.control_revision AND x.assignment_epoch=r.assignment_epoch
 JOIN collaboration.work_item_device_targets d ON d.work_item_id=x.work_item_id AND d.goal_revision=x.goal_revision AND d.control_revision=x.control_revision AND d.assignment_epoch=x.assignment_epoch
 WHERE q.event_kind='execution_requested' AND q.delivered_at_ms IS NULL AND q.state_revision=x.accepted_state_revision
 AND r.status='ready' AND t.erasure_job_id IS NULL AND q.event_seq>$1
 ORDER BY q.event_seq LIMIT $2")
            .bind(after)
            .bind(i64::from(batch))
            .fetch_all(self.executions.admission.ledger.pool())
            .await
            .map_err(db)?;
        rows.into_iter()
            .map(|row| {
                Ok(Candidate {
                    event_seq: row.try_get("event_seq").map_err(db)?,
                    work_item_id: WorkItemId::from_uuid(row.try_get("work_item_id").map_err(db)?)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    actor: Actor {
                        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        principal_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        principal_kind: PrincipalKind::User,
                    },
                    destination: DeviceDestination {
                        device_id: DeviceId::from_uuid(row.try_get("device_id").map_err(db)?)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        installation_id: DeviceInstallationId::from_uuid(
                            row.try_get("installation_id").map_err(db)?,
                        )
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                        membership_id: row.try_get("membership_id").map_err(db)?,
                        membership_epoch: NonZeroU64::new(unsigned(
                            row.try_get("membership_epoch").map_err(db)?,
                        )?)
                        .ok_or(DialogueStoreError::Corrupt)?,
                    },
                })
            })
            .collect()
    }

    /// Claims the exact obligation with this sweep's own owner, then settles it
    /// through the shared routine, which rechecks the reloaded stored target.
    async fn settle(&self, owner: uuid::Uuid, c: Candidate) -> Result<bool, DialogueStoreError> {
        let Some((claim, preparation)) = self.claim(owner, c).await? else {
            return Ok(false);
        };
        let judged = c.destination;
        let settled = settle_ended(&self.executions, claim, preparation, judged).await;
        if !matches!(settled, Ok(true)) {
            self.release(claim).await?;
        }
        settled
    }

    async fn claim(
        &self,
        owner: uuid::Uuid,
        c: Candidate,
    ) -> Result<Option<(WorkItemExecutionClaim, WorkItemExecutionPreparation)>, DialogueStoreError>
    {
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let row = sqlx::query("WITH clock AS MATERIALIZED(SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint ms), claimed AS (
 UPDATE collaboration.work_item_outbox q SET claim_owner=$2,claim_generation=q.claim_generation+1,attempt_count=q.attempt_count+1,lease_expires_at_ms=clock.ms+$3
 FROM clock WHERE q.event_seq=$1 AND q.work_item_id=$4 AND q.event_kind='execution_requested' AND q.delivered_at_ms IS NULL
 AND (q.claim_owner IS NULL OR q.lease_expires_at_ms<=clock.ms)
 AND EXISTS(SELECT 1 FROM collaboration.work_items t
  JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision
  JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=t.work_item_id AND x.goal_revision=r.goal_revision AND x.control_revision=r.control_revision AND x.assignment_epoch=r.assignment_epoch
  JOIN collaboration.work_item_device_targets d ON d.work_item_id=x.work_item_id AND d.goal_revision=x.goal_revision AND d.control_revision=x.control_revision AND d.assignment_epoch=x.assignment_epoch
  WHERE t.work_item_id=q.work_item_id AND x.accepted_state_revision=q.state_revision AND r.status='ready' AND t.erasure_job_id IS NULL
  AND t.tenant_id=$5 AND t.owner_id=$6 AND d.device_id=$7 AND d.installation_id=$8 AND d.membership_id=$9 AND d.membership_epoch=$10)
 RETURNING q.*)
 SELECT c.*,r.goal_revision,r.control_revision,r.assignment_epoch FROM claimed c JOIN collaboration.work_item_revisions r ON r.work_item_id=c.work_item_id AND r.state_revision=c.state_revision")
            .bind(c.event_seq)
            .bind(owner)
            .bind(SETTLE_LEASE_MS)
            .bind(c.work_item_id.into_uuid())
            .bind(c.actor.tenant_id.into_uuid())
            .bind(c.actor.principal_id.into_uuid())
            .bind(c.destination.device_id.into_uuid())
            .bind(c.destination.installation_id.into_uuid())
            .bind(c.destination.membership_id)
            .bind(integer(c.destination.membership_epoch.get())?)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let claim = super::claim::decode_claim(&row, c.actor, owner)?;
        let preparation = preparation::load(
            &mut tx,
            claim.request,
            false,
            self.executions.admission.snapshots.as_ref(),
        )
        .await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(Some((claim, preparation)))
    }

    /// Returns an unsettled claim without moving the obligation's due time.
    async fn release(&self, claim: WorkItemExecutionClaim) -> Result<(), DialogueStoreError> {
        sqlx::query("UPDATE collaboration.work_item_outbox SET claim_owner=NULL,lease_expires_at_ms=NULL WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL")
            .bind(integer(claim.event_seq)?)
            .bind(claim.owner)
            .bind(integer(claim.generation)?)
            .execute(self.executions.admission.ledger.pool())
            .await
            .map_err(db)?;
        Ok(())
    }
}

/// Treat a stalled membership owner as unavailable so the scan can reach other targets.
pub(super) async fn inspect(
    targets: &dyn WorkItemDeviceTargetAuthority,
    actor: Actor,
    destination: DeviceDestination,
) -> Result<WorkItemDeviceMembershipLineage, DialogueStoreError> {
    tokio::time::timeout(
        Duration::from_secs(5),
        targets.inspect_membership_lineage(actor, destination),
    )
    .await
    .map_err(|_| DialogueStoreError::Unavailable)?
}

/// The reloaded stored target must be exactly the destination the Owner judged.
fn stored_target_is(target: &WorkItemExecutionTarget, judged: DeviceDestination) -> bool {
    matches!(target, WorkItemExecutionTarget::RemoteDevice(target)
        if target.membership_id == judged.membership_id
            && target.membership_epoch == judged.membership_epoch.get()
            && target.device_id == judged.device_id
            && target.installation_id == judged.installation_id)
}

pub(super) async fn settle_ended(
    executions: &PostgresWorkItemExecutionStore,
    claim: WorkItemExecutionClaim,
    p: WorkItemExecutionPreparation,
    destination: DeviceDestination,
) -> Result<bool, DialogueStoreError> {
    if p.key.request() != claim.request {
        return Err(DialogueStoreError::Corrupt);
    }
    let mut tx = executions
        .admission
        .ledger
        .database()
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
        .await
        .map_err(crate::dialogue_codec::transaction)?;
    preparation::lock_agents(&mut tx, p.key.actor, p.key.scope, p.key.executing_agent_id).await?;
    super::claim::lock_claim(&mut tx, claim).await?;
    if lookup(&mut tx, claim.request).await?.is_some() {
        return Ok(false);
    }
    let current = preparation::load(
        &mut tx,
        claim.request,
        false,
        executions.admission.snapshots.as_ref(),
    )
    .await?;
    if current != p {
        return Err(DialogueStoreError::RevisionConflict);
    }
    if current.actor() != claim.request.actor || !stored_target_is(&current.target, destination) {
        return Err(DialogueStoreError::Corrupt);
    }
    let now = crate::ledger::database_now_ms(&mut tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    // lock_claim accepts only the ready execution request at these revisions.
    super::super::handoff::reject_execution(&mut tx, &current, now).await?;
    let changed=sqlx::query("UPDATE collaboration.work_item_outbox SET delivered_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint,failure_code='device_membership_ended',claim_owner=NULL,lease_expires_at_ms=NULL WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
   .bind(integer(claim.event_seq)?).bind(claim.owner).bind(integer(claim.generation)?).execute(&mut *tx).await.map_err(db)?.rows_affected()==1;
    if !changed {
        return Ok(false);
    }
    tx.commit()
        .await
        .map_err(crate::dialogue_codec::transaction)?;
    Ok(true)
}
