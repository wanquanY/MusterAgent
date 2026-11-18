//! Separate per-request control authority from the authenticated polling device.
//! A slow membership owner must never hold a pool slot or a row lock.
use super::*;

impl PostgresNativeDialogueInteractionStore {
    pub(super) async fn deliver_queued_command(
        &self,
        actor: Actor,
        transport: &OriginDeviceExecutionTarget,
        row: &PgRow,
    ) -> Result<NativeDialogueInteractionCommand, RuntimeError> {
        let command = decode_command(
            &row.try_get::<Vec<u8>, _>("command_bytes")
                .map_err(database)?,
        )?;
        let current = {
            let mut conn = self.database.pool().acquire().await.map_err(database)?;
            entry_row(
                &mut conn,
                actor,
                command.target.dialogue_id,
                command.target.entry_id,
                false,
            )
            .await?
            .ok_or_else(|| fail(ErrorCode::PolicyDenied))?
        };
        let p = row_projection(&current)?;
        p.validate().map_err(fail)?;
        if p.admission != command.admission
            || p.interaction.interaction_id != command.interaction_id
        {
            return Err(fail(ErrorCode::ContentIntegrityFailed));
        }
        let live = p.interaction.status == InteractionStatus::Pending;
        let correlation = CorrelationId::generate();
        let control = if live {
            Some(
                self.qualify_control(actor, row_id(&current, "agent_id")?, &p, correlation)
                    .await?,
            )
        } else {
            None
        };
        // Only byte-identical signed host credentials share the same proof.
        // Retained delivery of a closed request may recover its original ACK.
        let requester = match &control {
            Some(control) if &control.authority.target == transport => None,
            _ => Some(
                self.qualify_subject(actor, transport, &p, live, correlation)
                    .await?,
            ),
        };
        let mut conn = self.database.pool().acquire().await.map_err(database)?;
        let current = entry_row(
            &mut conn,
            actor,
            command.target.dialogue_id,
            command.target.entry_id,
            false,
        )
        .await?
        .ok_or_else(|| fail(ErrorCode::PolicyDenied))?;
        if row_projection(&current)?.fingerprint() != p.fingerprint() {
            // Re-read from the fresh state. A transition from pending to closed
            // can still recover its original receipt on the next device scan.
            return Err(fail(ErrorCode::ConversationSeqConflict));
        }
        if let Some(requester) = requester {
            requester.verify(&mut conn, actor, &current, &p).await?;
        }
        if let Some(control) = control {
            control.verify(&mut conn, actor, &current, &p).await?;
            if !p
                .actions(
                    crate::ledger::database_now_ms(&mut conn)
                        .await
                        .map_err(database)?,
                    control.authority.remote_tool_approval,
                )
                .contains(&command.response.action())
            {
                return Err(fail(ErrorCode::PolicyDenied));
            }
        }
        Ok(command)
    }
}
