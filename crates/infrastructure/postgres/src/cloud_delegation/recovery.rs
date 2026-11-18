//! Reconciliation is durable and independent of the worker that spawned a
//! child. It observes original Run terminals; it never re-spawns a lost child.
use super::*;
impl PostgresCloudDelegation {
    /// Bounded reconciliation called by every cloud worker. Conversation lanes
    /// serialize this with admission, parent completion, cancellation and plans.
    pub async fn tick(&self, now: UtcTimestamp) -> Result<(), Error> {
        self.cascade_stops(now).await?;
        let children:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT d.child_run_id FROM runtime.delegations d JOIN runtime.runs r ON r.run_id=d.child_run_id WHERE d.status=1 AND r.status IN (5,6,7,8) ORDER BY r.finished_at_ms,d.delegation_id LIMIT 32")
            .fetch_all(self.database.pool()).await.map_err(db)?;
        for child in children {
            self.deliver(RunId::from_uuid(child).map_err(|_| Error::Corrupt)?, now)
                .await?;
        }
        Ok(())
    }
    async fn cascade_stops(&self, now: UtcTimestamp) -> Result<(), Error> {
        // Walk upward only from live work. Historical terminal Runs must not
        // become an ever-growing recursive scan on every reconciliation tick.
        let rows = sqlx::query(
            "WITH RECURSIVE ancestry AS (
            SELECT run_id AS target,run_id,parent_run_id,status,cancel_requested_at_ms
              FROM runtime.runs WHERE status IN (1,2,3,4) AND cancel_requested_at_ms IS NULL
            UNION ALL
            SELECT a.target,r.run_id,r.parent_run_id,r.status,r.cancel_requested_at_ms
              FROM runtime.runs r JOIN ancestry a ON r.run_id=a.parent_run_id
        ), stopped AS (
            SELECT DISTINCT target FROM ancestry a WHERE a.cancel_requested_at_ms IS NOT NULL
              OR a.status IN (6,7,8)
              OR EXISTS(SELECT 1 FROM runtime.delegation_stop_requests s WHERE s.run_id=a.run_id)
        ) SELECT r.run_id,r.tenant_id,c.created_by FROM runtime.runs r
          JOIN stopped s ON s.target=r.run_id
          JOIN conversation.conversations c ON c.conversation_id=r.conversation_id
          ORDER BY r.admitted_at_ms,r.run_id LIMIT 64",
        )
        .fetch_all(self.database.pool())
        .await
        .map_err(db)?;
        for row in rows {
            let run: uuid::Uuid = row.try_get("run_id").map_err(db)?;
            let actor = Actor {
                tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                principal_id: PrincipalId::from_uuid(row.try_get("created_by").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                principal_kind: PrincipalKind::User,
            };
            crate::cloud_cancellation::cancel(
                &self.database,
                self.encoder.as_ref(),
                actor,
                RunId::from_uuid(run).map_err(|_| Error::Corrupt)?,
                uuid::Uuid::new_v5(&run, b"parent-cascade-stop"),
                musteragent_runtime_contract::CancelReason::ParentCancelled,
                now,
            )
            .await
            .map_err(source_error)?;
        }
        Ok(())
    }
    async fn deliver(&self, child: RunId, now: UtcTimestamp) -> Result<(), Error> {
        let mut tx = crate::commit_lane::acquire_for_run(&self.database, child)
            .await
            .map_err(|_| Error::Unavailable)?
            .ok_or(Error::NotFound)?;
        let Some(row)=sqlx::query("SELECT d.delegation_id,d.parent_run_id,d.tenant_id,d.conversation_id,r.status,r.terminal_error FROM runtime.delegations d JOIN runtime.runs r ON r.run_id=d.child_run_id WHERE d.child_run_id=$1 AND d.status=1 AND r.status IN (5,6,7,8) FOR UPDATE OF d")
            .bind(child.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)? else{return Ok(());};
        let tenant = TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?;
        let conversation = ConversationId::from_uuid(row.try_get("conversation_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?;
        let delegation: uuid::Uuid = row.try_get("delegation_id").map_err(db)?;
        let parent: uuid::Uuid = row.try_get("parent_run_id").map_err(db)?;
        let run_status: i16 = row.try_get("status").map_err(db)?;
        let responses=sqlx::query("SELECT c.content_id,c.media_type,c.byte_length::bigint AS byte_length,c.content_hash FROM runtime.model_invocations i JOIN artifact.content_refs c ON c.content_id=i.response_content_id WHERE i.run_id=$1 AND i.status=2 AND c.byte_length>0 ORDER BY i.started_at_ms,i.invocation_id")
            .bind(child.into_uuid()).fetch_all(&mut *tx).await.map_err(db)?;
        let mut results = Vec::new();
        for response in responses {
            let reference = controls::reference(&response)?;
            let bytes = self
                .content
                .read(tenant, &reference)
                .await
                .map_err(|_| Error::Unavailable)?;
            verify_content(&reference, &bytes).map_err(|_| Error::Corrupt)?;
            results.push(serde_json::json!({"content_id":reference.content_id.to_string(),"text":std::str::from_utf8(&bytes).map_err(|_|Error::Corrupt)?}));
        }
        let report = serde_json::json!({"handle":delegation,"parent_run_id":parent,"child_run_id":child.to_string(),"status":controls::status(run_status)?,"results":results,"terminal_error":row.try_get::<Option<Vec<u8>>,_>("terminal_error").map_err(db)?});
        let bytes = serde_json::to_vec(&report).map_err(|_| Error::Corrupt)?;
        let reference = self
            .put(
                tenant,
                delegation,
                b"terminal-report",
                "application/json",
                &bytes,
            )
            .await?;
        crate::admission::authorize_published_content_ref(&mut tx, tenant, &reference)
            .await
            .map_err(|_| Error::Corrupt)?;
        let status = match run_status {
            5 => 2i16,
            6 => 3,
            7 => 4,
            8 => 5,
            _ => return Err(Error::Corrupt),
        };
        sqlx::query("UPDATE runtime.delegations SET status=$2,report_content_id=$3,report_media_type=$4,report_byte_length=$5::bigint,report_hash=$6,resolved_at_ms=$7 WHERE delegation_id=$1 AND status=1")
            .bind(delegation).bind(status).bind(reference.content_id.into_uuid()).bind(reference.media_type.as_str()).bind(reference.byte_length.to_string()).bind(reference.sha256.into_bytes().as_slice()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO runtime.delegation_deliveries(delivery_id,delegation_id,tenant_id,parent_run_id,created_at_ms) VALUES($1,$2,$3,$4,$5)")
            .bind(uuid::Uuid::new_v5(&delegation,b"delivery")).bind(delegation).bind(tenant.into_uuid()).bind(parent).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        // Parent already waiting: the original scheduler creates its next
        // Attempt. If still running, its completion gate sees this inbox.
        sqlx::query("UPDATE runtime.runs SET status=1,wait_reason=NULL,next_attempt_trigger=6,run_version=run_version+1 WHERE run_id=$1 AND status=3 AND wait_reason=9 AND cancel_requested_at_ms IS NULL")
            .bind(parent).execute(&mut *tx).await.map_err(db)?;
        event_raw(
            &mut tx,
            tenant,
            conversation,
            delegation,
            uuid::Uuid::new_v5(&delegation, b"ended"),
            "subagent_ended",
            report,
            now,
        )
        .await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(())
    }
}
