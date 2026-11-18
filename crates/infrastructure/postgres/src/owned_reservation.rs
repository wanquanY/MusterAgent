use crate::dialogue_codec::{db, require_user};
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{
    DialogueStoreError, OwnerDefaultReservation, OwnerDefaultReservationStore,
    ReserveDialogueDefault,
};
use sqlx::Row;
#[derive(Clone)]
pub struct PostgresOwnerDefaultReservationStore {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresOwnerDefaultReservationStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self { database }
    }
}
impl OwnerDefaultReservationStore for PostgresOwnerDefaultReservationStore {
    fn reserve_default(
        &self,
        request: ReserveDialogueDefault,
    ) -> ApplicationFuture<'_, Result<OwnerDefaultReservation, DialogueStoreError>> {
        Box::pin(async move {
            let actor = request.actor;
            require_user(actor)?;
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            sqlx::query(
                "INSERT INTO agent.owner_default_reservations
                 (tenant_id,owner_id,command_id,agent_id,agent_version_id,space_id,
                  dialogue_id,template_ref,prepared_at_ms,template_hash)
                 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                 ON CONFLICT(tenant_id,owner_id) DO NOTHING",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .bind(request.command_id.into_uuid())
            .bind(AgentId::generate().into_uuid())
            .bind(AgentVersionId::generate().into_uuid())
            .bind(OwnerSpaceId::generate().into_uuid())
            .bind(AgentDialogueId::generate().into_uuid())
            .bind(request.template_ref.as_str())
            .bind(request.requested_at.unix_millis())
            .bind(request.template_hash.into_bytes().to_vec())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            let row = sqlx::query(
                "SELECT * FROM agent.owner_default_reservations
                 WHERE tenant_id=$1 AND owner_id=$2",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
            let result = OwnerDefaultReservation {
                actor,
                command_id: CommandId::from_uuid(row.try_get("command_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                agent_version_id: AgentVersionId::from_uuid(
                    row.try_get("agent_version_id").map_err(db)?,
                )
                .map_err(|_| DialogueStoreError::Corrupt)?,
                space_id: OwnerSpaceId::from_uuid(row.try_get("space_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                dialogue_id: AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                template_ref: VersionedKey::new(
                    row.try_get::<String, _>("template_ref").map_err(db)?,
                )
                .map_err(|_| DialogueStoreError::Corrupt)?,
                template_hash: Sha256::from_bytes(
                    row.try_get::<Option<Vec<u8>>, _>("template_hash")
                        .map_err(db)?
                        .ok_or(DialogueStoreError::Corrupt)?,
                )
                .map_err(|_| DialogueStoreError::Corrupt)?,
                prepared_at: UtcTimestamp::from_unix_millis(
                    row.try_get("prepared_at_ms").map_err(db)?,
                ),
            };
            result.validate_for(actor)?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(result)
        })
    }
}
