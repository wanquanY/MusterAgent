use super::*;
impl PostgresWorkItemDeviceExecutionStore {
    pub(super) async fn claim_inner(
        &self,
        authority: &DeviceHostAuthority,
        owner: uuid::Uuid,
        lease: Duration,
    ) -> Result<Option<WorkItemExecutionClaim>, DialogueStoreError> {
        if owner.is_nil() || !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&lease) {
            return Err(DialogueStoreError::Corrupt);
        }
        let actor = authority.actor();
        let destination = authority.destination();
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        let row=sqlx::query("WITH clock AS MATERIALIZED(SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint ms), candidate AS (
 SELECT q.event_seq FROM collaboration.work_item_outbox q
 JOIN collaboration.work_items t USING(work_item_id)
 JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision
 JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=t.work_item_id AND x.goal_revision=r.goal_revision AND x.control_revision=r.control_revision AND x.assignment_epoch=r.assignment_epoch
 JOIN collaboration.work_item_device_targets d ON d.work_item_id=x.work_item_id AND d.goal_revision=x.goal_revision AND d.control_revision=x.control_revision AND d.assignment_epoch=x.assignment_epoch,clock
 WHERE q.event_kind='execution_requested' AND q.state_revision=x.accepted_state_revision AND q.delivered_at_ms IS NULL
 AND q.available_at_ms<=clock.ms AND (q.claim_owner IS NULL OR q.lease_expires_at_ms<=clock.ms OR q.claim_owner=$1)
 AND t.tenant_id=$3 AND t.owner_id=$4 AND t.erasure_job_id IS NULL AND r.status='ready'
 AND d.device_id=$5 AND d.installation_id=$6 AND d.membership_id=$7
 AND NOT EXISTS(SELECT 1 FROM collaboration.work_items dep JOIN collaboration.work_item_revisions dr ON dr.work_item_id=dep.work_item_id AND dr.state_revision=dep.state_revision WHERE dep.work_item_id=ANY(r.dependencies) AND (dr.status<>'completed' OR dep.erasure_job_id IS NOT NULL))
 ORDER BY q.available_at_ms,q.event_seq LIMIT 1 FOR UPDATE OF q SKIP LOCKED), claimed AS (
 UPDATE collaboration.work_item_outbox q SET claim_owner=$1,
 claim_generation=CASE WHEN q.claim_owner=$1 AND q.lease_expires_at_ms>clock.ms THEN q.claim_generation ELSE q.claim_generation+1 END,
 attempt_count=CASE WHEN q.claim_owner=$1 AND q.lease_expires_at_ms>clock.ms THEN q.attempt_count ELSE q.attempt_count+1 END,
 lease_expires_at_ms=CASE WHEN q.claim_owner=$1 AND q.lease_expires_at_ms>clock.ms THEN q.lease_expires_at_ms ELSE clock.ms+$2 END
 FROM candidate,clock WHERE q.event_seq=candidate.event_seq RETURNING q.*)
 SELECT c.*,r.goal_revision,r.control_revision,r.assignment_epoch FROM claimed c JOIN collaboration.work_item_revisions r ON r.work_item_id=c.work_item_id AND r.state_revision=c.state_revision")
            .bind(owner).bind(i64::try_from(lease.as_millis()).map_err(|_|DialogueStoreError::Corrupt)?)
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
            .bind(destination.device_id.into_uuid()).bind(destination.installation_id.into_uuid())
            .bind(destination.membership_id)
            .fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else { return Ok(None) };
        let claim = WorkItemExecutionClaim {
            request: WorkItemExecutionRequest {
                actor,
                work_item_id: WorkItemId::from_uuid(row.try_get("work_item_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                goal_revision: WorkItemGoalRevision::new(unsigned(
                    row.try_get("goal_revision").map_err(db)?,
                )?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
                control_revision: WorkItemControlRevision::new(unsigned(
                    row.try_get("control_revision").map_err(db)?,
                )?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
                assignment_epoch: AssignmentEpoch::new(unsigned(
                    row.try_get("assignment_epoch").map_err(db)?,
                )?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
                report_from_run_id: None,
            },
            event_seq: unsigned(row.try_get("event_seq").map_err(db)?)?,
            owner,
            generation: unsigned(row.try_get("claim_generation").map_err(db)?)?,
            attempt_count: unsigned(row.try_get("attempt_count").map_err(db)?)?,
            lease_expires_at: UtcTimestamp::from_unix_millis(
                row.try_get("lease_expires_at_ms").map_err(db)?,
            ),
        };
        // No target migration or newly generated Intent: revalidate the original
        // reservation against the currently authenticated membership/consent.
        let validated = async {
            let source = preparation::load(
                &mut tx,
                claim.request,
                false,
                self.executions.admission.snapshots.as_ref(),
            )
            .await?;
            self.require_current_target(authority, &source, &mut tx)
                .await
        }
        .await;
        if let Err(error) = validated {
            // Failure is not an ACK. Retain this obligation but move its
            // availability atomically, so an old revoked source cannot starve
            // other Tasks owned by this exact authenticated destination.
            authority
                .require_current_at(now(&mut tx).await?)
                .map_err(|_| DialogueStoreError::Unavailable)?;
            let failure = match error {
                DialogueStoreError::PermissionReduced
                | DialogueStoreError::Unavailable
                | DialogueStoreError::Inactive => WorkItemHandoffFailure::PolicyDenied,
                DialogueStoreError::RevisionConflict | DialogueStoreError::IdempotencyConflict => {
                    WorkItemHandoffFailure::PreparationChanged
                }
                DialogueStoreError::Corrupt => WorkItemHandoffFailure::InvalidContract,
                DialogueStoreError::Infrastructure => WorkItemHandoffFailure::DependencyUnavailable,
            };
            sqlx::query("UPDATE collaboration.work_item_outbox SET claim_owner=NULL,lease_expires_at_ms=NULL,failure_code=$4,available_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+5000 WHERE event_seq=$1 AND claim_owner=$2 AND claim_generation=$3 AND delivered_at_ms IS NULL")
                .bind(integer(claim.event_seq)?).bind(claim.owner).bind(integer(claim.generation)?).bind(failure.as_str()).execute(&mut *tx).await.map_err(db)?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            return Err(error);
        }
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(Some(claim))
    }
    pub(super) async fn prepare_inner(
        &self,
        authority: &DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        chosen: Option<WorkspaceId>,
    ) -> Result<WorkItemDeviceExecutionPlacement, DialogueStoreError> {
        self.prepare_placement_inner(authority, claim, Some(chosen))
            .await?
            .ok_or(DialogueStoreError::Corrupt)
    }
    pub(super) async fn prepare_placement_inner(
        &self,
        authority: &DeviceHostAuthority,
        claim: WorkItemExecutionClaim,
        chosen: Option<Option<WorkspaceId>>,
    ) -> Result<Option<WorkItemDeviceExecutionPlacement>, DialogueStoreError> {
        let mut tx = self
            .executions
            .admission
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let source = preparation::load(
            &mut tx,
            claim.request,
            true,
            self.executions.admission.snapshots.as_ref(),
        )
        .await?;
        lock_claim(&mut tx, claim).await?;
        self.require_current_target(authority, &source, &mut tx)
            .await?;
        let placement = match chosen {
            Some(chosen) => Some(super::placement::capture(&mut tx, &source, chosen).await?),
            None => super::placement::retained(&mut tx, &source).await?,
        };
        let Some(device_workspace_id) = placement else {
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            return Ok(None);
        };
        lock_claim(&mut tx, claim).await?;
        authority
            .require_current_at(now(&mut tx).await?)
            .map_err(|_| DialogueStoreError::Unavailable)?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(Some(WorkItemDeviceExecutionPlacement {
            preparation: source,
            device_workspace_id,
        }))
    }
}
pub(super) async fn lock_claim(
    tx: &mut PgConnection,
    c: WorkItemExecutionClaim,
) -> Result<(), DialogueStoreError> {
    if c.owner.is_nil() || c.generation == 0 || c.request.report_from_run_id.is_some() {
        return Err(DialogueStoreError::Unavailable);
    }
    let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_outbox q JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=q.work_item_id AND x.accepted_state_revision=q.state_revision JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision WHERE q.event_seq=$1 AND q.work_item_id=$2 AND q.claim_owner=$3 AND q.claim_generation=$4 AND q.attempt_count=$5 AND q.lease_expires_at_ms=$6 AND q.lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint AND q.delivered_at_ms IS NULL AND q.event_kind='execution_requested' AND x.tenant_id=$7 AND x.owner_id=$8 AND x.goal_revision=$9 AND x.control_revision=$10 AND x.assignment_epoch=$11 AND r.goal_revision=x.goal_revision AND r.control_revision=x.control_revision AND r.assignment_epoch=x.assignment_epoch AND r.status='ready' AND t.erasure_job_id IS NULL FOR UPDATE OF q)")
        .bind(integer(c.event_seq)?).bind(c.request.work_item_id.into_uuid()).bind(c.owner).bind(integer(c.generation)?)
        .bind(integer(c.attempt_count)?).bind(c.lease_expires_at.unix_millis()).bind(c.request.actor.tenant_id.into_uuid())
        .bind(c.request.actor.principal_id.into_uuid()).bind(integer(c.request.goal_revision.get())?)
        .bind(integer(c.request.control_revision.get())?).bind(integer(c.request.assignment_epoch.get())?)
        .fetch_one(tx).await.map_err(db)?;
    if !valid {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}
