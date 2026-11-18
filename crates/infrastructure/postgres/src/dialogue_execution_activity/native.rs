//! Qualified metadata from the original Home Run. It cannot admit work, settle
//! a Tool/Task or publish a message. Terminal receipts remain stronger facts.
use super::*;
use crate::agent_resource::device_custody::{self, DeviceCustody};
use musteragent_runtime_contract::ApplicationFuture;
use std::sync::Arc;

#[derive(Clone)]
pub struct PostgresNativeDialogueActivityStore {
    database: crate::PostgresDatabaseRuntime,
    hosts: Arc<dyn WorkItemDeviceTargetAuthority>,
}
impl PostgresNativeDialogueActivityStore {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        hosts: Arc<dyn WorkItemDeviceTargetAuthority>,
    ) -> Self {
        Self {
            database: database.into(),
            hosts,
        }
    }
    async fn observe_inner(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        value: &NativeDialogueActivityObservation,
    ) -> Result<NativeDialogueActivityAcknowledgement, DialogueStoreError> {
        value.validate()?;
        target.require_actor(actor)?;
        let hash = codec::observation_hash(value).map_err(|_| DialogueStoreError::Corrupt)?;
        let authority = DeviceHostAuthority::from_trusted_account_transport(
            actor,
            target.membership_authority(),
        )
        .map_err(|_| DialogueStoreError::Unavailable)?;
        let original = {
            let mut conn = self.database.pool().acquire().await.map_err(db)?;
            device_custody::load_local(&mut conn, actor, target, &value.binding, false)
                .await
                .map_err(custody_error)?
        };
        let selection = match &original {
            DeviceCustody::Intent(_) => WorkItemDeviceSelection {
                membership_id: authority.destination().membership_id,
                expected_membership_epoch: authority.destination().membership_epoch.get(),
            },
            DeviceCustody::WorkItem(v) => WorkItemDeviceSelection {
                membership_id: v.target().membership_id,
                expected_membership_epoch: v.target().membership_epoch,
            },
        };
        // Original current/retained host owner, without SQL/Auth locks across IO.
        // Reporting facts does not renew consent or authorize an old Tool.
        let qualified = self
            .hosts
            .qualify_retained_host(&authority, selection)
            .await?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Projection)
            .await
            .map_err(transaction)?;
        let visible:Option<uuid::Uuid>=sqlx::query_scalar("SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.product='musteragent' AND a.agent_id=$3 AND a.status<>'deleted' FOR UPDATE OF s")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(original.agent_id().into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        visible.ok_or(DialogueStoreError::Unavailable)?;
        let current = device_custody::load_local(&mut tx, actor, target, &value.binding, false)
            .await
            .map_err(custody_error)?;
        if current != original {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        qualified.require_selection(&authority, selection, now)?;
        let previous=sqlx::query("SELECT run_sequence::text AS sequence,checkpoint_version::text AS checkpoint_version,observation_sha256,state,CASE WHEN retry_observation_sha256=observation_sha256 THEN scheduled_retry_at_ms END AS scheduled_retry_at_ms,COALESCE(permission_observation_sha256=observation_sha256 AND permission_wait,false) AS permission_wait FROM agent.native_execution_activity WHERE run_id=$1 FOR UPDATE")
            .bind(value.binding.run_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        let mut update = true;
        let mut renew = false;
        if let Some(old) = previous {
            let sequence = old
                .try_get::<String, _>("sequence")
                .map_err(db)?
                .parse::<u64>()
                .map_err(|_| DialogueStoreError::Corrupt)?;
            let checkpoint = old
                .try_get::<String, _>("checkpoint_version")
                .map_err(db)?
                .parse::<u64>()
                .map_err(|_| DialogueStoreError::Corrupt)?;
            let previous_version = (sequence, checkpoint);
            let incoming_version = (value.run_sequence, value.checkpoint_version);
            let oldhash: Vec<u8> = old.try_get("observation_sha256").map_err(db)?;
            if previous_version >= incoming_version {
                update = false;
                renew = previous_version == incoming_version;
                if renew && oldhash != hash.into_bytes() {
                    // Rolling upgrades may add only the scheduler datum to an
                    // otherwise byte-identical observation. Never downgrade it.
                    let old_retry: Option<i64> =
                        old.try_get("scheduled_retry_at_ms").map_err(db)?;
                    let old_permission: bool = old.try_get("permission_wait").map_err(db)?;
                    let old_detail = old_retry.is_some() || old_permission;
                    let new_detail = value.scheduled_retry_at.is_some() || value.permission_wait;
                    let mut equivalent = value.clone();
                    equivalent.permission_wait = old_permission;
                    equivalent.scheduled_retry_at = old_retry.map(UtcTimestamp::from_unix_millis);
                    let equivalent_hash = codec::observation_hash(&equivalent)
                        .map_err(|_| DialogueStoreError::Corrupt)?;
                    if oldhash != equivalent_hash.into_bytes() || old_detail == new_detail {
                        return Err(DialogueStoreError::IdempotencyConflict);
                    }
                    update = !old_detail;
                    renew = !update;
                }
            } else if old.try_get::<i16, _>("state").map_err(db)?
                == DialogueActivityState::Terminal as i16
            {
                return Err(DialogueStoreError::RevisionConflict);
            }
        }
        if update {
            sqlx::query("INSERT INTO agent.native_execution_activity(run_id,tenant_id,owner_id,agent_id,dialogue_id,observation_sha256,run_sequence,state,step,outcome,observed_at_ms,checkpoint_version,scheduled_retry_at_ms,retry_observation_sha256,permission_wait,permission_observation_sha256) VALUES($1,$2,$3,$4,$5,$6,$7::numeric,$8,$9,$10,$11,$12::numeric,$13,$6,$14,$6) ON CONFLICT(run_id) DO UPDATE SET observation_sha256=EXCLUDED.observation_sha256,run_sequence=EXCLUDED.run_sequence,state=EXCLUDED.state,step=EXCLUDED.step,outcome=EXCLUDED.outcome,observed_at_ms=EXCLUDED.observed_at_ms,checkpoint_version=EXCLUDED.checkpoint_version,scheduled_retry_at_ms=EXCLUDED.scheduled_retry_at_ms,retry_observation_sha256=EXCLUDED.retry_observation_sha256,permission_wait=EXCLUDED.permission_wait,permission_observation_sha256=EXCLUDED.permission_observation_sha256")
                .bind(value.binding.run_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(original.agent_id().into_uuid()).bind(original.dialogue_id().into_uuid()).bind(hash.into_bytes().to_vec()).bind(value.run_sequence.to_string()).bind(value.activity.state as i16).bind(value.activity.step.map(|v|v as i16)).bind(value.activity.outcome.map(|v|v as i16)).bind(now.unix_millis()).bind(value.checkpoint_version.to_string()).bind(value.scheduled_retry_at.map(|t| t.unix_millis())).bind(value.permission_wait).execute(&mut *tx).await.map_err(db)?;
        }
        if renew {
            sqlx::query("UPDATE agent.native_execution_activity SET observed_at_ms=$2 WHERE run_id=$1 AND state IN(2,3)").bind(value.binding.run_id.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(transaction)?;
        Ok(NativeDialogueActivityAcknowledgement {
            binding: value.binding.clone(),
            run_sequence: value.run_sequence,
            checkpoint_version: value.checkpoint_version,
            observation_sha256: hash,
        })
    }
}
impl NativeDialogueActivityStore for PostgresNativeDialogueActivityStore {
    fn observe<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        value: &'a NativeDialogueActivityObservation,
    ) -> ApplicationFuture<'a, Result<NativeDialogueActivityAcknowledgement, DialogueStoreError>>
    {
        Box::pin(self.observe_inner(actor, target, value))
    }
}
fn custody_error(v: DeviceResourceError) -> DialogueStoreError {
    match v {
        DeviceResourceError::Corrupt => DialogueStoreError::Corrupt,
        DeviceResourceError::Infrastructure => DialogueStoreError::Infrastructure,
        _ => DialogueStoreError::Unavailable,
    }
}
