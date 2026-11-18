//! One projection listener. Notifications are hints; committed dirty rows are
//! reread after startup/reconnect. There is no per-client execution polling.
use super::*;
use sqlx::postgres::PgListener;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
const CHANNEL: &str = "musteragent_execution_activity";
impl PostgresDialogueActivityProjection {
    pub async fn connect(&self) -> Result<PgListener, DialogueStoreError> {
        let mut listener = PgListener::connect_with(self.database.pool())
            .await
            .map_err(db)?;
        listener.listen(CHANNEL).await.map_err(db)?;
        Ok(listener)
    }
    pub async fn run(
        &self,
        mut listener: PgListener,
        cancel: CancellationToken,
    ) -> Result<(), DialogueStoreError> {
        let mut after: Option<uuid::Uuid> = None;
        let mut failed = false;
        loop {
            sqlx::query("INSERT INTO agent.dialogue_activity_dirty(agent_id) SELECT agent_id FROM agent.dialogue_execution_activity WHERE next_refresh_at_ms<=floor(extract(epoch FROM clock_timestamp())*1000)::bigint ON CONFLICT(agent_id) DO NOTHING").execute(self.database.pool()).await.map_err(db)?;
            let agents:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT agent_id FROM agent.dialogue_activity_dirty WHERE ($1::uuid IS NULL OR agent_id>$1) ORDER BY agent_id LIMIT 64").bind(after).fetch_all(self.database.pool()).await.map_err(db)?;
            for agent in &agents {
                let result = tokio::select! {biased;()=cancel.cancelled()=>return Ok(()),v=self.refresh(AgentId::from_uuid(*agent).map_err(|_|DialogueStoreError::Corrupt)?)=>v};
                if result.is_err() {
                    failed = true;
                    tracing::warn!(
                        "Dialogue activity projection pending; original execution is unchanged"
                    );
                }
            }
            // Advance even past a rejected row: a corrupt first page must not
            // starve other Agents. Failures are revisited on the next pass.
            if agents.len() == 64 {
                after = agents.last().copied();
                continue;
            }
            after = None;
            let until:Option<i64>=sqlx::query_scalar("SELECT min(next_refresh_at_ms)-floor(extract(epoch FROM clock_timestamp())*1000)::bigint FROM agent.dialogue_execution_activity WHERE next_refresh_at_ms IS NOT NULL").fetch_one(self.database.pool()).await.map_err(db)?;
            let delay = if failed {
                Some(Duration::from_secs(1))
            } else {
                until.map(|ms| Duration::from_millis(ms.max(1) as u64))
            };
            tokio::select! {biased;
                ()=cancel.cancelled()=>return Ok(()),
                ()=async{if let Some(delay)=delay {tokio::time::sleep(delay).await}else{std::future::pending::<()>().await}}=>{},
                notification=listener.try_recv()=>{
                    if notification.is_err(){
                        loop {tokio::select!{biased;()=cancel.cancelled()=>return Ok(()),()=tokio::time::sleep(Duration::from_millis(200))=>{}}
                            if let Ok(value)=self.connect().await{listener=value;break;}
                        }
                    }
                }
            }
            failed = false;
        }
    }
}
