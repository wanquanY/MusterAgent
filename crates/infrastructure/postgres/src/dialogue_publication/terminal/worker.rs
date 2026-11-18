//! One server listener wakes durable work. Reconnect/startup always query the
//! backlog; timers are only for persisted delivery retries, never client polling.
use super::*;
use sqlx::postgres::PgListener;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
const CHANNEL: &str = "musteragent_terminal_delivery";
impl PostgresPublicTerminalDeliveryStore {
    pub async fn connect(&self) -> Result<PgListener, DialoguePublicationError> {
        let mut listener = PgListener::connect_with(&self.pool).await.map_err(db)?;
        listener.listen(CHANNEL).await.map_err(db)?;
        Ok(listener)
    }
    pub async fn run(
        &self,
        mut listener: PgListener,
        cancellation: CancellationToken,
    ) -> Result<(), DialoguePublicationError> {
        // Existing real failures are adopted by verifying their original Ledger
        // facts. No placeholder message or synthetic terminal transition is made.
        loop {
            let count = tokio::select! { biased; ()=cancellation.cancelled()=>return Ok(()), value=self.recover_legacy_failures(16)=>value? };
            if count < 16 {
                break;
            }
        }
        loop {
            let work: Vec<(uuid::Uuid,uuid::Uuid)> = sqlx::query_as("SELECT tenant_id,run_id FROM agent.dialogue_terminal_deliveries WHERE state='pending' AND available_at_ms<=floor(extract(epoch FROM clock_timestamp())*1000)::bigint ORDER BY available_at_ms,created_at_ms,run_id LIMIT 16")
                .fetch_all(&self.pool).await.map_err(db)?;
            for (tenant, run) in &work {
                let tenant_id =
                    TenantId::from_uuid(*tenant).map_err(|_| DialogueStoreError::Corrupt)?;
                let run_id = RunId::from_uuid(*run).map_err(|_| DialogueStoreError::Corrupt)?;
                let result = tokio::select! {biased;()=cancellation.cancelled()=>return Ok(()), result=tokio::time::timeout(Duration::from_secs(30),self.publish_terminal(tenant_id,run_id))=>result};
                if !matches!(result, Ok(Ok(_))) {
                    // A crash or failure never acknowledges the obligation. The
                    // delay belongs to output storage only, not Run reexecution.
                    sqlx::query("UPDATE agent.dialogue_terminal_deliveries SET attempts=least(attempts::bigint+1,2147483647)::integer,available_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+least(60000,1000*(1::bigint<<least(attempts,6))) WHERE tenant_id=$1 AND run_id=$2 AND state='pending'")
                        .bind(*tenant).bind(*run).execute(&self.pool).await.map_err(db)?;
                    tracing::warn!(
                        "Dialogue terminal delivery remains pending; original execution is not retried"
                    );
                }
            }
            if work.len() == 16 {
                continue;
            }
            let wait: Option<i64> = sqlx::query_scalar("SELECT min(greatest(0,available_at_ms-floor(extract(epoch FROM clock_timestamp())*1000)::bigint)) FROM agent.dialogue_terminal_deliveries WHERE state='pending'")
                .fetch_one(&self.pool).await.map_err(db)?;
            tokio::select! {
                biased;
                ()=cancellation.cancelled()=>return Ok(()),
                ()=async { if let Some(ms)=wait { tokio::time::sleep(Duration::from_millis(ms as u64)).await } else { std::future::pending::<()>().await } }=>{},
                notification=listener.try_recv()=>{
                    if notification.is_err() {
                        // LISTEN is restored before re-reading; lost hints are harmless.
                        loop {
                            tokio::select! {biased;()=cancellation.cancelled()=>return Ok(()),()=tokio::time::sleep(Duration::from_millis(200))=>{}}
                            match self.connect().await { Ok(value)=>{listener=value;break},Err(_)=>continue }
                        }
                    }
                }
            }
        }
    }

    /// Upgrade recovery is bounded and idempotent. It accepts only a genuine
    /// final Failed event from the existing Run, never a status-only SQL row.
    pub async fn recover_legacy_failures(
        &self,
        limit: u32,
    ) -> Result<usize, DialoguePublicationError> {
        if !(1..=64).contains(&limit) {
            return Err(DialogueStoreError::Corrupt.into());
        }
        let rows:Vec<(uuid::Uuid,uuid::Uuid,uuid::Uuid)>=sqlx::query_as("SELECT r.tenant_id,r.run_id,r.conversation_id FROM runtime.runs r JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id LEFT JOIN agent.dialogue_terminal_deliveries o ON o.tenant_id=r.tenant_id AND o.run_id=r.run_id WHERE r.status=6 AND o.run_id IS NULL ORDER BY r.finished_at_ms,r.run_id LIMIT $1")
            .bind(i64::from(limit)).fetch_all(&self.pool).await.map_err(db)?;
        for (tenant, run, conversation) in &rows {
            let mut tx = self
                .database
                .write_unit_of_work(DbCommitKind::Resource)
                .await
                .map_err(|error| transaction_error(DbTransactionPhase::Acquire, error))?;
            sqlx::query("SELECT conversation_id FROM event_store.ledger_streams WHERE tenant_id=$1 AND conversation_id=$2 FOR UPDATE").bind(*tenant).bind(*conversation).fetch_one(&mut *tx).await.map_err(db)?;
            let state=sqlx::query("SELECT r.last_run_sequence::text AS seq,r.status,c.iteration FROM runtime.runs r JOIN runtime.loop_checkpoints c USING(run_id) WHERE r.tenant_id=$1 AND r.run_id=$2 FOR UPDATE OF r")
                .bind(*tenant).bind(*run).fetch_one(&mut *tx).await.map_err(db)?;
            if state.try_get::<i16, _>("status").map_err(db)? != 6 {
                continue;
            }
            let records=sqlx::query("SELECT l.record_id,l.payload_hash,e.payload_hash AS event_hash,e.encoded_event,l.occurred_at_ms FROM event_store.ledger_records l JOIN event_store.domain_events e USING(record_id) WHERE l.tenant_id=$1 AND l.run_id=$2 AND l.conversation_id=$3 AND l.run_seq>greatest(0,$4::numeric-256) AND l.run_seq<=$4::numeric ORDER BY l.run_seq DESC LIMIT 256")
                .bind(*tenant).bind(*run).bind(*conversation).bind(state.try_get::<String,_>("seq").map_err(db)?).fetch_all(&mut *tx).await.map_err(db)?;
            let mut found = None;
            for record in records {
                let bytes: Vec<u8> = record.try_get("encoded_event").map_err(db)?;
                let hash = Sha256::digest(&bytes);
                if record.try_get::<Vec<u8>, _>("payload_hash").map_err(db)? != hash.into_bytes()
                    || record.try_get::<Vec<u8>, _>("event_hash").map_err(db)? != hash.into_bytes()
                {
                    return Err(DialogueStoreError::Corrupt.into());
                }
                let payload = self
                    .decoder
                    .decode(
                        musteragent_event_envelope::LedgerRecordKind::DomainEvent,
                        &bytes,
                    )
                    .map_err(|_| DialogueStoreError::Corrupt)?;
                if matches!(payload,musteragent_ledger::LedgerPayload::DomainEvent(musteragent_ledger::DomainEvent::Run(musteragent_run::RunEvent::Failed {ref error,..})) if !error.retryable)
                {
                    if found.is_some() {
                        return Err(DialogueStoreError::Corrupt.into());
                    }
                    found = Some((record, hash));
                }
            }
            let (record, hash) = found.ok_or(DialogueStoreError::Corrupt)?;
            let record_id: uuid::Uuid = record.try_get("record_id").map_err(db)?;
            sqlx::query("INSERT INTO agent.dialogue_terminal_deliveries(tenant_id,run_id,conversation_id,source_record_id,source_sha256,terminal_run_seq,terminal_iteration,delivery_id,available_at_ms,created_at_ms) VALUES($1,$2,$3,$4,$5,$6::numeric,$7,$8,$9,$9) ON CONFLICT(tenant_id,run_id) DO NOTHING")
                .bind(*tenant).bind(*run).bind(*conversation).bind(record_id).bind(hash.into_bytes().to_vec()).bind(state.try_get::<String,_>("seq").map_err(db)?)
                .bind(state.try_get::<i32,_>("iteration").map_err(db)?).bind(uuid::Uuid::new_v5(&record_id,b"public-terminal-delivery@1")).bind(record.try_get::<i64,_>("occurred_at_ms").map_err(db)?)
                .execute(&mut *tx).await.map_err(db)?;
            tx.commit()
                .await
                .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
        }
        Ok(rows.len())
    }
}
