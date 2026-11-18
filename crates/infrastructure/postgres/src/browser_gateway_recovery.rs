//! Candidate delivery recovery uses the original Browser Job and original Tool.
//! The new lease cannot read an unrelated account or start the old program.
use super::*;
use musteragent_content_ref::Sha256;

impl PostgresBrowserGatewayStore {
    pub(super) async fn candidate_settlement_inner(
        &self,
        proof: BrowserLeaseProof,
        job_id: BrowserJobId,
    ) -> Result<
        musteragent_runtime_capability::BrowserCandidateSettlement,
        BrowserProviderRuntimeError,
    > {
        use musteragent_runtime_capability::BrowserCandidateSettlement as Settlement;
        let status=sqlx::query_scalar::<_, i16>("SELECT t.status FROM capability.browser_jobs j
            JOIN tool.tool_invocations t ON t.tenant_id=j.tenant_id AND t.invocation_id=j.invocation_id
            WHERE j.tenant_id=$1 AND j.principal_id=$2 AND j.job_id=$3")
            .bind(proof.tenant_id.into_uuid()).bind(proof.principal_id.into_uuid()).bind(job_id.into_uuid())
            .fetch_optional(&self.pool).await.map_err(map_database)?;
        Ok(match status {
            Some(4) => Settlement::Committed,
            Some(5 | 7) => Settlement::NotCommitted,
            _ => Settlement::Pending,
        })
    }

    pub(super) async fn recover_candidate_inner(
        &self,
        proof: BrowserLeaseProof,
        job_id: BrowserJobId,
        request_hash: Sha256,
    ) -> Result<Option<DurableBrowserJob>, BrowserProviderRuntimeError> {
        let mut tx = self.write_unit_of_work().await?;
        let slot = sqlx::query(
            "SELECT *, floor(extract(epoch FROM clock_timestamp())*1000)::bigint AS now_ms
            FROM capability.browser_provider_slots WHERE tenant_id=$1 AND principal_id=$2
            AND browser_session_id=$3 AND page_instance_id=$4 AND active_lease_id=$5
            AND provider_id=$6 AND last_epoch=$7 AND last_fence=$8
            AND expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint FOR UPDATE",
        )
        .bind(proof.tenant_id.into_uuid())
        .bind(proof.principal_id.into_uuid())
        .bind(proof.browser_session_id.into_uuid())
        .bind(proof.page_instance_id.into_uuid())
        .bind(proof.lease_id.into_uuid())
        .bind(proof.provider_id.into_uuid())
        .bind(bounded_i64(proof.epoch.get())?)
        .bind(bounded_i64(proof.fence.get())?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_database)?
        .ok_or(BrowserProviderRuntimeError::StaleLease)?;
        let row=sqlx::query("SELECT * FROM capability.browser_jobs WHERE tenant_id=$1 AND principal_id=$2 AND job_id=$3 FOR UPDATE")
            .bind(proof.tenant_id.into_uuid()).bind(proof.principal_id.into_uuid()).bind(job_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(map_database)?.ok_or(BrowserProviderRuntimeError::JobNotFound)?;
        let original: Vec<u8> = row.try_get("request_proto").map_err(map_database)?;
        if Sha256::digest(&original) != request_hash {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        let mut job = decode_durable_job(&row)?;
        let lease = lease_from_active_row(proof, &slot)?;
        if lease.runtime_release_ref != job.delivery.request.runtime_release_ref {
            return Err(BrowserProviderRuntimeError::RuntimeReleaseMismatch);
        }
        if !lease.capabilities.contains(&job.delivery.capability) {
            return Err(BrowserProviderRuntimeError::CapabilityNotAuthorized);
        }
        // Current conversation membership and current managed Home are required
        // even for receipt recovery. Recovery is not a resurrection grant.
        let allowed=sqlx::query_scalar::<_,bool>("SELECT true FROM conversation.conversations c JOIN conversation.participants p
            ON p.tenant_id=c.tenant_id AND p.conversation_id=c.conversation_id
            WHERE c.tenant_id=$1 AND c.conversation_id=$2 AND c.control_home_kind=1 AND c.lifecycle IN(1,2)
            AND p.kind=1 AND p.principal_ref=$3 AND p.left_at_ms IS NULL FOR SHARE OF c,p")
            .bind(proof.tenant_id.into_uuid()).bind(job.delivery.request.conversation_id.into_uuid()).bind(proof.principal_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(map_database)?.unwrap_or(false);
        if !allowed {
            return Err(BrowserProviderRuntimeError::JobNotFound);
        }
        if let musteragent_capability::BrowserJobProgram::PythonWorkspace {
            execution_workspace_id,
            ..
        } = &job.delivery.request.program
        {
            let current=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM resource.execution_workspaces w
                LEFT JOIN conversation.context_branch_state b ON b.tenant_id=w.tenant_id AND b.conversation_id=w.conversation_id
                WHERE w.tenant_id=$1 AND w.conversation_id=$2 AND w.execution_workspace_id=$3
                AND w.branch_id IS NOT DISTINCT FROM b.active_branch_id)")
                .bind(proof.tenant_id.into_uuid()).bind(job.delivery.request.conversation_id.into_uuid()).bind(execution_workspace_id.into_uuid())
                .fetch_one(&mut *tx).await.map_err(map_database)?;
            if !current {
                return Err(BrowserProviderRuntimeError::JobNotFound);
            }
        }
        if job.delivery.state == BrowserJobDeliveryState::Terminal {
            // Observation only: keep the original durable receipt's lease.
            return Ok(Some(job));
        }
        if row_matches_proof(&row, proof)? {
            return Ok(Some(job));
        }
        let old = job.delivery.lease;
        let owner_active = sqlx::query_scalar::<_, bool>(
            "SELECT true FROM capability.browser_provider_slots
            WHERE tenant_id=$1 AND principal_id=$2 AND browser_session_id=$3 AND page_instance_id=$4
            AND active_lease_id=$5 AND last_epoch=$6 AND last_fence=$7
            AND expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint FOR SHARE",
        )
        .bind(old.tenant_id.into_uuid())
        .bind(old.principal_id.into_uuid())
        .bind(old.browser_session_id.into_uuid())
        .bind(old.page_instance_id.into_uuid())
        .bind(old.lease_id.into_uuid())
        .bind(bounded_i64(old.epoch.get())?)
        .bind(bounded_i64(old.fence.get())?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_database)?
        .unwrap_or(false);
        if owner_active {
            return Ok(None);
        }
        let sequence = slot
            .try_get::<i64, _>("last_delivery_sequence")
            .map_err(map_database)?
            .checked_add(1)
            .ok_or(BrowserProviderRuntimeError::SequenceExhausted)?;
        let now = slot.try_get::<i64, _>("now_ms").map_err(map_database)?;
        sqlx::query("UPDATE capability.browser_provider_slots SET last_delivery_sequence=$2 WHERE active_lease_id=$1")
            .bind(proof.lease_id.into_uuid()).bind(sequence).execute(&mut *tx).await.map_err(map_database)?;
        sqlx::query("UPDATE capability.browser_jobs SET lease_id=$2,browser_session_id=$3,page_instance_id=$4,provider_id=$5,
            lease_epoch=$6,lease_fence=$7,delivery_sequence=$8,last_provider_sequence=0,updated_at_ms=$9,
            state=CASE WHEN state=4 THEN 4 ELSE 3 END WHERE job_id=$1")
            .bind(job_id.into_uuid()).bind(proof.lease_id.into_uuid()).bind(proof.browser_session_id.into_uuid())
            .bind(proof.page_instance_id.into_uuid()).bind(proof.provider_id.into_uuid())
            .bind(bounded_i64(proof.epoch.get())?).bind(bounded_i64(proof.fence.get())?).bind(sequence).bind(now)
            .execute(&mut *tx).await.map_err(map_database)?;
        tx.commit()
            .await
            .map_err(|_| BrowserProviderRuntimeError::DurabilityUnavailable)?;
        job.delivery.lease = proof;
        job.delivery_sequence = decode_positive_sequence(sequence)?;
        job.delivery.last_provider_sequence = 0;
        job.updated_at = timestamp(now);
        if job.delivery.state != BrowserJobDeliveryState::CancelRequested {
            job.delivery.state = BrowserJobDeliveryState::Running;
        }
        Ok(Some(job))
    }
}
